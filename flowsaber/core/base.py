import functools
import inspect
import traceback
from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Optional, Union, List, TYPE_CHECKING, Callable, Any, Mapping

from makefun import with_signature
from pydantic import BaseModel

import flowsaber
from flowsaber.core.channel import Consumer, Output
from flowsaber.utility.context import Context, merge_dicts

if TYPE_CHECKING:
    import asyncio


def enter_context(method: Callable[..., Any]) -> Any:
    """A decorator runs the wrapped method within a new context composed of self.context and kwargs' context.

    Parameters
    ----------
    method

    Returns
    -------

    """

    @functools.wraps(method)
    def _enter_context(self, *args, **kwargs) -> Any:
        # we clear the package default context
        import flowsaber
        with flowsaber.context():
            with flowsaber.context(self.context):
                flowsaber.context.update(kwargs.get('context', {}))
                return method(self, *args, **kwargs)

    return _enter_context


def aenter_context(method: Callable[..., Any]) -> Any:
    """A decorator runs the wrapped method within a new context composed of self.context and kwargs' context.

    Parameters
    ----------
    method

    Returns
    -------

    """

    @functools.wraps(method)
    async def _aenter_context(self, *args, **kwargs) -> Any:
        import flowsaber
        with flowsaber.context():
            with flowsaber.context(self.context):
                flowsaber.context.update(kwargs.get('context', {}))
                return await method(self, *args, **kwargs)

    return _aenter_context


class ComponentExecuteError(RuntimeError):
    def __init__(self, *args, futures=None, trace_back=None):
        super().__init__(*args)
        self.futures = futures or []
        self.trace_back = trace_back


class ComponentCallError(RuntimeError):
    def __init__(self, *args, trace_back=None):
        super().__init__(*args)
        self.trace_back = trace_back


class ComponentMeta(type):
    PAIR_ARG_NAME = 'FUNC_PAIRS'

    def __new__(mcs, class_name, bases, class_dict):

        class_name, bases, class_dict = mcs.copy_method_sig(class_name, bases, class_dict)
        class_name, bases, class_dict = mcs.update_default_config(class_name, bases, class_dict)

        return super().__new__(mcs, class_name, bases, class_dict)

    @classmethod
    def copy_method_sig(mcs, class_name, bases, class_dict):
        """Used for automatically copy signature of a method to another method.
        Class must define 'FUNC_PAIRS' to indicate the src method and the target method

        FUNC_PAIRS:
            [src_method_name, target_method_name]
            [src_method_name, target_method_name, is_call_boolean]: signature of int will change to Channel[int]
        """

        # 1. handle copying method signature
        func_pairs = class_dict.get(mcs.PAIR_ARG_NAME, [])
        for base_cls in bases:
            base_func_pairs = getattr(base_cls, mcs.PAIR_ARG_NAME, [])
            func_pairs += base_func_pairs

        def copy_sig():
            src = class_dict.get(src_fn) or next(getattr(c, src_fn) for c in bases)
            tgt = class_dict.get(tgt_fn) or next(getattr(c, tgt_fn) for c in bases)
            while hasattr(src, '__inner_func__'):
                src = src.__inner_func__
            while hasattr(tgt, '__inner_func__'):
                tgt = tgt.__inner_func__
            src_sigs = inspect.signature(src)
            tgt_sigs = inspect.signature(tgt)
            # handle param signatures, this is used for run -> __call__
            if len(options) and options[0]:
                src_sig_params = list(src_sigs.parameters.values())
                for i, param in enumerate(src_sig_params):
                    if param.annotation is not inspect.Signature.empty:
                        src_sig_params[i] = param.replace(annotation=f"Channel[{param.annotation}]")
                src_sigs = inspect.Signature(src_sig_params, return_annotation=src_sigs.return_annotation)
            # handle return annotation, if tgt already has return annotation, keep it
            if tgt_sigs.return_annotation is not inspect.Signature.empty:
                src_sig_params = list(src_sigs.parameters.values())
                src_sigs = inspect.Signature(src_sig_params, return_annotation=tgt_sigs.return_annotation)

            @with_signature(src_sigs, func_name=tgt.__name__, qualname=tgt.__qualname__, doc=src.__doc__)
            def new_tgt_fn(*args, **kwargs):
                return tgt(*args, **kwargs)

            # used for source the real func
            new_tgt_fn.__source_func__ = src
            new_tgt_fn.__inner_func__ = tgt
            return new_tgt_fn

        if func_pairs:
            assert all(len(item) >= 2 for item in func_pairs)
            for src_fn, tgt_fn, *options in func_pairs:
                if src_fn == tgt_fn:
                    raise ValueError(f"src {src_fn} and tgt {tgt_fn} can not be the same.")
                new_tgt_fn = copy_sig()
                class_dict[tgt_fn] = new_tgt_fn

        return class_name, bases, class_dict

    @classmethod
    def update_default_config(mcs, class_name, bases, class_dict):
        """Automatically merge and update class-scoped `default_config` dict from parent class.
        Parameters
        ----------
        class_name
        bases
        class_dict

        Returns
        -------

        """
        # 2. handle default_config update
        from copy import deepcopy
        config_name = "default_config"
        default_config: dict = deepcopy(getattr(bases[0], config_name, {}))
        class_dict[config_name] = merge_dicts(default_config, class_dict.get(config_name, {}))

        return class_name, bases, class_dict


class Component(object, metaclass=ComponentMeta):
    """Base class of Flow and Task
    """

    class State(Enum):
        CREATED = 1
        INITIALIZED = 2
        EXECUTED = 3

    CREATED = State.CREATED
    INITIALIZED = State.INITIALIZED
    EXECUTED = State.EXECUTED

    default_config = {
        'id': None,
        'name': None,
        'full_name': None,
        'labels': [],
        'workdir': '',
        'log_stdout': True,
        'log_stderr': True,
    }

    def __init__(self, **kwargs):
        self.rest_kwargs = kwargs
        self.state: Component.State = self.CREATED
        self.context: Optional[dict] = None

        self._input_args: Optional[tuple] = None
        self._input_kwargs: Optional[dict] = None
        self._input: Optional[Consumer] = None
        self._output: Optional[Output] = None

    @property
    def input(self) -> Optional[Consumer]:
        return self._input

    @input.setter
    def input(self, value: Consumer):
        self._input = value

    @property
    def output(self) -> Optional[Output]:
        return self._output

    @output.setter
    def output(self, value: Output):
        self._output = value

    @property
    def config_name(self) -> str:
        raise NotImplementedError

    @property
    def config_dict(self) -> dict:
        if self.context is None:
            return {}
        else:
            return self.context[self.config_name]

    @property
    def config(self) -> Context:
        """return a non-editable context"""
        return Context(self.config_dict)

    @property
    def initialized(self):
        return self.state != Component.State.CREATED

    def __str__(self):
        name = None
        if self.initialized:
            name = self.config_dict.get('name')
        return name or f"{type(self).__name__}[{id(self)}]"

    def __repr__(self):
        full_name = None
        if self.initialized:
            full_name = self.config_dict.get("full_name")
        return full_name or str(self)

    def get_full_name(self) -> str:
        """Generate a name like flow1.name|flow2.name|flow3.name|cur_task
        """
        up_flow_names = '|'.join(flow.config_dict['name'] for flow in flowsaber.context.flow_stack)
        if up_flow_names:
            up_flow_names += '|'
        return f"{up_flow_names}{type(self).__name__}[{id(self)}]"

    def __call__(self, *args, **kwargs) -> Union[Output, 'Component']:
        """ This is where the flow/task build dependency graph

        Parameters
        ----------
        args
        kwargs

        Returns
        -------

        """
        # TODO using type annotation, it's possible to check type before running flow
        try:
            from copy import copy
            new = copy(self)
            new.call_initialize(*args, **kwargs)
            build_output = new.call_build(*args, **kwargs)
            return build_output
        except BaseException as e:
            tb = traceback.format_exc()
            raise ComponentCallError(str(e), trace_back=tb) from e

    def __copy__(self):
        cls = type(self)
        new = cls.__new__(cls)
        for k, v in self.__dict__.items():
            new.__dict__[k] = None if k.startswith('_') else deepcopy(v)
        return new

    def call_initialize(self, *args, **kwargs):
        """Copy a new one and initialize some attributes.
        Parameters
        ----------
        args
        kwargs

        Returns
        -------

        """
        # copy a new one and initialize the context
        self.state = self.INITIALIZED
        self.initialize_context()

    def call_build(self, *args, **kwargs) -> Union[Output, 'Component']:
        raise NotImplementedError

    def initialize_context(self):
        """Called by call_initialize, merge and update self.config dict of self.context from different sources.
        """
        # may copied from a task already has context
        self.context = self.context or {}
        config_name = self.config_name
        up_workdir = flowsaber.context.get("up_" + config_name, {}).get('workdir', '')
        # update config in four steps
        default_config = self.default_config
        global_default_config = getattr(flowsaber.context, f'default_{config_name}', {})
        kwargs_config = self.rest_kwargs
        tmp_config = flowsaber.context.get(config_name, {})
        # initialize flow/task's context
        with flowsaber.context() as context:
            # 1: use class default config_dict
            context.update({config_name: default_config})
            # 2: use global default config_dict
            context.update({config_name: global_default_config})
            # 3. use kwargs settled config_dict
            context.update({config_name: kwargs_config})
            # 4. use user temporally settled config_dict
            context.update({config_name: tmp_config})
            context_dict = context.to_dict()
        self.context = merge_dicts(self.context, context_dict)

        # set up id, name, full_name
        if not self.config_dict.get('id'):
            self.config_dict['id'] = flowsaber.context.random_id
        if not self.config_dict.get('name'):
            self.config_dict['name'] = str(self)
        if not self.config_dict.get('full_name'):
            self.config_dict['full_name'] = self.get_full_name()
        # record mro info for future use
        self.config_dict['bases'] = [
            f"{base}|{id(base)}" for base in type(self).__mro__
        ]

        # set up workdir build from the hierarchy of flows/tasks
        # flow's workdir and task's workdir is handled separately
        workdir = self.config_dict['workdir']
        workdir = Path(workdir).expanduser()  # do not resolve
        if workdir.is_absolute():
            workdir = str(workdir)
        else:
            workdir = str(Path(up_workdir, workdir))
        self.config_dict['workdir'] = workdir

    def initialize_input(self, *args, **kwargs):
        self._input_args = args
        self._input_kwargs = kwargs

    # TODO should we use context manager
    @aenter_context
    async def start(self, **kwargs):
        """Start running the Flow/Task in the context of self.context, before setting the context,
        self.context will be merged/updated from kwargs.get('context', {})
        Parameters
        ----------
        kwargs

        Returns
        -------

        """
        # update context
        back_context = deepcopy(self.context)
        self.context = merge_dicts(self.context, kwargs.get('context', {}))
        try:
            res = await self.start_execute(**kwargs)
            return res
        except BaseException as e:
            tb = traceback.format_exc()
            raise ComponentExecuteError(str(e), trace_back=tb) from e
        finally:
            await self.end_execute()
            self.context = back_context

    async def start_execute(self, **kwargs):
        if self.state == self.CREATED:
            raise ValueError("The Task/Flow object is not initialized, "
                             "please use task()/flow() to initialize it.")
        elif self.state == self.EXECUTED:
            raise ValueError("The Task/Flow has already been executed once before.")

        self.state = self.EXECUTED

    async def end_execute(self, *args, **kwargs):
        self.clean()

    def clean(self):
        pass

    def check_future_exceptions(self, futures: List['asyncio.Future']):
        # wrap into a ComponentExecuteError with record of all futures
        first_exception = next((fut.exception() for fut in futures if fut.exception()), None)
        if first_exception:
            raise ComponentExecuteError(str(first_exception), futures=futures) from first_exception

    def serialize(self) -> BaseModel:
        raise NotImplementedError

    def dict(self):
        return self.serialize().dict()

    @classmethod
    def input_signature(cls) -> Mapping[str, inspect.Parameter]:
        return inspect.signature(cls.__call__).parameters
