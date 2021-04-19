import inspect
from enum import Enum
from pathlib import Path
from typing import Optional, Union, List, Sequence

from makefun import with_signature
from pydantic import BaseModel

import flowsaber
from flowsaber.core.channel import Consumer, Channel
from flowsaber.utility.logtool import get_logger
from .utils.context import Context

logger = get_logger(__name__)

"""
flow_config: {}
task_config: {}

flow_id
flow_name
flow_full_name
flow_labels

task_id
task_name
task_full_name
task_labels

flowrun_id
flowrun_name

taskrun_id
"""

EMPTY_ANNOTATION = inspect._empty


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

        if func_pairs:
            assert all(len(item) == 2 for item in func_pairs)
            for src_fn, tgt_fn, *options in func_pairs:
                if src_fn == tgt_fn:
                    raise ValueError(f"src {src_fn} and tgt {tgt_fn} can not be the same.")
                src = class_dict.get(src_fn) or next(getattr(c, src_fn) for c in bases)
                tgt = class_dict.get(tgt_fn) or next(getattr(c, tgt_fn) for c in bases)
                src_sigs = inspect.signature(src)
                tgt_sigs = inspect.signature(tgt)
                # handle param signatures, this is used for run -> __call__
                if len(options) and options[0]:
                    src_sig_params = list(src_sigs.parameters.values())
                    for i, param in enumerate(src_sig_params):
                        if param.annotation is not EMPTY_ANNOTATION:
                            src_sig_params[i] = param.replace(annotation=f"Channel[{param.annotation}]")
                    src_sigs = inspect.Signature(src_sig_params, return_annotation=src_sigs.return_annotation)
                # handle return annotation, if tgt already has return annotation, keep it
                if tgt_sigs.return_annotation is not EMPTY_ANNOTATION:
                    src_sigs.return_annotation = tgt_sigs.return_annotation

                @with_signature(src_sigs, func_name=tgt.__name__, qualname=tgt.__qualname__, doc=src.__doc__)
                def new_tgt_fn(*args, **kwargs):
                    return tgt(*args, **kwargs)

                # used for source the real func
                new_tgt_fn.__source_func__ = src
                class_dict[tgt_fn] = new_tgt_fn

        return class_name, bases, class_dict

    @classmethod
    def update_default_config(mcs, class_name, bases, class_dict):
        # 2. handle default_config update
        from copy import deepcopy
        default_config: dict = deepcopy(getattr(bases[0], 'default_config', {}))
        default_config.update(getattr(class_dict, 'default_config', {}))
        class_dict['default_config'] = default_config

        return class_name, bases, class_dict


class Component(object, metaclass=ComponentMeta):
    class State(Enum):
        CREATED = 1
        INITIALIZED = 2
        EXECUTED = 3

    default_config = {
        'id': None,
        'name': None,
        'full_name': None,
        'labels': [],
        'workdir': ''
    }

    def __init__(self, **kwargs):
        self.state: Component.State = Component.State.CREATED
        self.input_args = Optional[tuple] = None
        self.input_kwargs = Optional[dict] = None
        self.input = Optional[Consumer] = None
        self.output = Optional[Union[Sequence[Channel], Channel]] = None
        self.context: Optional[dict] = None
        self.rest_kwargs = kwargs

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
        return Context(self.config, write=False)

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

    def _get_full_name(self) -> str:
        """Generate a name like flow1.name|flow2.name|flow3.name|cur_task
        """
        up_flow_names = '|'.join(flow.config['name'] for flow in flowsaber.context.flow_stack)
        if up_flow_names:
            up_flow_names += '|'
        return f"{up_flow_names}{type(self).__name__}[{id(self)}]"

    def __call__(self, *args, **kwargs) -> Union[List[Channel], Channel, None]:
        raise NotImplementedError

    def __copy__(self):
        from copy import copy, deepcopy
        new = copy(self)
        new.call_initialized = False
        for attr in ['context', 'rest_kwargs']:
            setattr(new, attr, deepcopy(getattr(self, attr)))
        for attr in ['input_args', 'input_kwargs', 'input', 'output']:
            setattr(new, attr, None)
        return new

    def call_initialize(self, *args, **kwargs):
        from copy import copy
        new = copy(self)
        new.call_initialized = True
        new.initialize_context()
        return new

    def initialize_context(self):
        self.context = {}
        config_name = self.config_name
        tmp_config = flowsaber.context.get(config_name, {})
        global_default_config = getattr(flowsaber.context, f'default_{config_name}', {})
        pre_workdir = flowsaber.context.get(config_name, 'workdir', '')
        # update config
        # 1: use global default config_dict
        with flowsaber.context({config_name: global_default_config}):
            # 1: use class default config_dict
            with flowsaber.context({config_name: self.default_config}):
                # 2. use kwargs settled config_dict
                with flowsaber.context({config_name: self.rest_kwargs}):
                    # 3. use user temporally settled config_dict
                    with flowsaber.context({config_name: tmp_config}) as context:
                        context_dict = context.to_dict()
        self.context.update(context_dict)
        # set up id, name, full_name
        if not self.config_dict.get('id'):
            self.config_dict['id'] = flowsaber.context.random_id
        if not self.config_dict.get('name'):
            self.config_dict['name'] = str(self)
        if not self.config_dict.get('full_name'):
            self.config_dict['full_name'] = self._get_full_name()

        # set up workdir build from the hierarchy of flows/tasks
        workdir = self.config_dict['workdir']
        workdir = Path(workdir).expanduser().resolve()
        if workdir.is_absolute():
            workdir = str(workdir)
        else:
            workdir = str(Path(pre_workdir, workdir))
        self.config_dict['workdir'] = workdir

    def initialize_input(self, *args, **kwargs):
        self.input_args = args
        self.input_kwargs = kwargs

    async def execute(self, **kwargs):
        raise NotImplementedError

    async def pre_execute_check(self):
        if not self.initialized:
            raise ValueError("The Task/Flow object is not initialized, "
                             "please use task()/flow() to initialize it.")

    def clean(self):
        pass

    def serialize(self) -> BaseModel:
        raise NotImplementedError

    def dict(self):
        return self.serialize().dict()
