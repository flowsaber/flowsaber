"""
Some functions come from prefect.utilities.context
"""
import contextlib
import contextvars
from collections import UserDict
from collections.abc import MutableMapping
from copy import deepcopy
from typing import Any, Iterable, Iterator, Union, cast

from flowsaber.core.default_context import DEFAULT_CONTEXT

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
            if hasattr(type(self), key):
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
    """To make all levels of dict to be type(self), must use update to initialize
    """

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
        # The magic for ensuring coroutine safe
        self._data: contextvars.ContextVar = contextvars.ContextVar(f"data-{id(self)}")
        self._info: DotDict = DotDict()
        # the initial value of self._data is settled in here, within init will call self.data = dict(*args, **kwargs)
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return f"{type(self).__name__}({self.data})"

    def update(self, *args, **kwargs) -> None:
        self.data.update(*args, **kwargs)

    @property
    def data(self) -> MergingDotDict:
        try:
            return self._data.get()
        except LookupError:
            # this happens in thread, context var is not valid in new thread
            # dask uses thread to execute task
            # TODO Any other way?
            self.data = DEFAULT_CONTEXT
            return self._data.get()

    @data.setter
    def data(self, dic: DictLike):
        # this is the main point
        if not isinstance(dic, MergingDotDict):
            self._data.set(MergingDotDict())
            self.update(dic)
        else:
            self._data.set(dic)
        self._thread_backup = self._data.get()

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
        update_dict = dict(*args, **kwargs)
        prev_context = self.to_dict()
        try:
            new_context = merge_dicts(deepcopy(prev_context), update_dict)
            self.data = new_context
            yield self
        finally:
            self.data = prev_context
