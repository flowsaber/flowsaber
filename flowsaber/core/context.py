"""
Some functions come from prefect.utilities.context
"""
import asyncio
import contextlib
import contextvars
import inspect
import logging
import uuid
from collections import UserDict, defaultdict
from collections.abc import MutableMapping
from typing import Any, Iterable, Iterator, Union, cast

from flowsaber.core.utility.cache import Cache, get_cache
from flowsaber.core.utility.executor import Executor, get_executor
from flowsaber.utility.logging import get_logger

DictLike = Union[dict, "DotBase"]


def merge_dicts(d1: DictLike, d2: DictLike) -> DictLike:
    """
    Updates `d1` from `d2` by replacing each `(k, v1)` pair in `d1` with the
    corresponding `(k, v2)` pair in `d2`.

    If the _output of each pair is itself a dict, then the _output is updated
    recursively.

    Args:
        - d1 (MutableMapping): A dictionary to be replaced
        - d2 (MutableMapping): A dictionary used for replacement

    Returns:
        - A `MutableMapping` with the two dictionary contents merged
    """

    new_dict = d1.copy()

    for k, v in d2.items():
        if isinstance(new_dict.get(k), MutableMapping) and isinstance(
                v, MutableMapping
        ):
            new_dict[k] = merge_dicts(new_dict[k], d2[k])
        else:
            new_dict[k] = d2[k]
    return new_dict


def as_nested_dict(
        obj: Union[DictLike, Iterable[DictLike]],
        dct_class: type = dict, **kwargs) -> Union[DictLike, Iterable[DictLike]]:
    """
    Given a obj formatted as a dictionary, transforms it (and any nested dictionaries)
    into the provided dct_class

    Args:
        - obj (Any): An object that is formatted as a `dict`
        - dct_class (type): the `dict` class to use (defaults to DotDict)

    Returns:
        - A `dict_class` representation of the object passed in
    ```
    """
    if isinstance(obj, (list, tuple, set)):
        return type(obj)([as_nested_dict(d, dct_class) for d in obj])
    elif isinstance(obj, dct_class):
        for k, v in obj.items():
            obj[k] = as_nested_dict(v, dct_class)
    elif isinstance(obj, MutableMapping):
        # DotDicts could have keys that shadow `update` and `items`, so we
        # take care to avoid accessing those keys here
        return dct_class(
            {
                k: as_nested_dict(v, dct_class)
                for k, v in obj.items()
            },
            **kwargs
        )
    return obj


class DotBase(UserDict):
    PASS_ARGS = ['data']

    def __getattr__(self, item):
        return self[item]

    def __delattr__(self, item):
        del self[item]

    def __setattr__(self, key, value):
        if key.startswith('_') or key in self.PASS_ARGS:
            # __setattr__ prevail property.__set__
            if key in type(self).__dict__:
                object.__setattr__(self, key, value)
            else:
                self.__dict__[key] = value
        else:
            self[key] = value

    def to_dict(self) -> dict:
        """
        Converts current `DotDict` (and any `DotDict`s contained within)
        to an appropriate nested dictionary.
        """
        # mypy cast
        return cast(dict, as_nested_dict(self, dct_class=dict))


class DotDict(DotBase):

    def __init__(self, *args, **kwargs: Any):
        # a DotDict could have a task_hash that shadows `update`
        super().__init__()
        # can not call super().__init__() as super().__init__() will again call self.update
        super().update(*args, **kwargs)

    def update(self, *args, **kwargs) -> None:
        dot_dict = dict(*args, **kwargs)
        # convert all dict into DotDict
        dot_dict = as_nested_dict(dot_dict, type(self))
        super().update(dot_dict)

    def __repr__(self):
        return f"{type(self).__name__}({self})"

    def __str__(self):
        return str(self.data)


class MergingDotDict(DotDict):
    def update(self, *args, **kwargs) -> None:
        # update dict recursively
        merged_dict = merge_dicts(self.data, dict(*args, **kwargs))
        super().update(merged_dict)


class Context(DotBase):
    """
    A coroutine safe context store


    Args:
        - *data (Any): arguments to provide to the `DotDict` constructor (e.g.,
            an initial dictionary)
        - **kwargs (Any): any task_hash / _output pairs to initialize this context with
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.__data = contextvars.ContextVar(f"data-{id(self)}")
        self.__info = DotDict()
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return f"{type(self).__name__}({self.data})"

    def update(self, *args, **kwargs) -> None:
        self.data.update(*args, **kwargs)

    @property
    def data(self) -> MergingDotDict:
        return self.__data.get()

    @data.setter
    def data(self, dic: DictLike):
        # this is the main point
        if not isinstance(dic, MergingDotDict):
            dic = MergingDotDict(dic)
        self.__data.set(dic)

    @property
    def random_id(self) -> str:
        return str(uuid.uuid4())

    @property
    def flow_stack(self):
        return self.__info.setdefault('__flow_stack', [])

    @property
    def top_flow(self):
        return self.flow_stack[0] if self.flow_stack else None

    @property
    def up_flow(self):
        return self.flow_stack[-1] if self.flow_stack else None

    def __getstate__(self) -> None:
        """
        Because we dynamically update context during runs, we don't ever want to pickle
        or "freeze" the contents of context.  Consequently it should always be accessed
        as an attribute of the prefect module.
        """
        raise TypeError(
            "Pickling context objects is explicitly not supported. You should always "
            "access context as an attribute of the `flowsaber` module, as in `flowsaber.context`"
        )

    @contextlib.contextmanager
    def __call__(self, *args: MutableMapping, **kwargs: Any) -> Iterator["Context"]:
        """
        coroutine should use this context manager to update local context
        A context manager for setting / resetting the context context

        Example:
            from context import context
            with context(dict(a=1, b=2), c=3):
                print(context.a) # 1
        """
        # Avoid creating new `Context` object, copy as `dict` instead.
        prev_context = self.to_dict()
        try:
            new_context = MergingDotDict(prev_context)
            new_context.update(*args, **kwargs)
            self.data = new_context
            yield self
        finally:
            self.data = prev_context


class FlowSaberContext(Context):
    # utility instance based on the current context
    @property
    def cache(self, ) -> Cache:
        caches = self.__info.setdefault('__caches', {})
        assert 'task_workdir' in self, "Can not find 'task_workdir' in context. You are not within a task."
        task_workdir = self.task_workdir
        if task_workdir not in caches:
            caches[task_workdir] = get_cache(cache=self.config_dict['cache'])
        return caches[task_workdir]

    @property
    def run_lock(self) -> asyncio.Lock:
        locks = self.__info.setdefault('__run_locks', default=defaultdict(asyncio.Lock))
        assert 'run_workdir' in self, "Can not find 'run_workdir' in context. You are not within a task."
        run_workdir = self.run_workdir
        return locks[run_workdir]

    @property
    def logger(self) -> logging.Logger:
        # find callee
        callee_frame = inspect.currentframe().f_back
        callee_module_name = callee_frame.f_globals['__name__']
        # find running info
        run_infos = [self.get(attr, 'NULL') for attr in
                     ['agent_id', 'flow_id', 'flowrun_id', 'task_id', 'taskrun_id']]
        run_name = '.'.join(run_infos)

        logger_name = f"{callee_module_name}.{run_name}".rstrip('.')

        return get_logger(logger_name)

    @property
    def executor(self) -> Executor:
        assert 'executor_type' in self, "Can not find 'executor_type in context. You are not within a task."
        executors = self.__info.setdefault('__executors', {})
        executor_type = self.executor_type
        if executor_type not in executors:
            raise RuntimeWarning(f"The executor: {executor_type} not found. fall back to local")
        return executors[executor_type]

    async def __aenter__(self):
        executors = self.__info.setdefault('__executors', {})
        for executor_config in self.executors:
            executor_type = executor_config['executor_type']
            executors[executor_type] = get_executor(**executor_config)
        for executor in executors:
            await executor.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        executors = self.__info.setdefault('__executors', {})
        # TODO, how to pass __aexit__ parameters?
        for executor in executors:
            await executor.__aexit__(exc_type, exc_val, exc_tb)
        executors.clear()


context = FlowSaberContext()

context.update({
    'default_flow_config': {
        'test__': {
            'test__': [1, 2, 3]
        }
    },
    'default_task_config': {
        'test__': {
            'test__': [1, 2, 3]
        }
    },
    'logging': {
        'format': "[%(levelname)6s:%(filename)10s:%(lineno)3s-%(funcName)15s()] %(message)s",
        'datefmt': "%Y-%m-%d %H:%M:%S%z",
        'level': 0,
        'buffer_size': 10,
        'context_attrs': None,
    },
    'executors': [
        {
            'executor_type': 'local'
        },
        {
            'executor_type': 'dask',
            'address': None,
            'cluster_class': None,
            'cluster_kwargs': None,
            'adapt_kwargs': None,
            'client_kwargs': None,
            'debug': False
        }
    ]
})
