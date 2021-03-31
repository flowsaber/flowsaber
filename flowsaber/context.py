"""
Modified from prefect.utilities.context
"""

import contextlib
from collections.abc import MutableMapping
from typing import Any, Iterable, Iterator, Union, cast

DictLike = Union[dict, "DotDict"]

__all__ = ['Context', 'context', 'config']


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


class DotDict(MutableMapping):
    """
    A `dict` that also supports attribute ("dot") access. Think of this as an extension
    to the standard python `dict` object.  **Note**: while any hashable object can be added to
    a `DotDict`, _only_ valid Python identifiers can be accessed with the dot syntax; this excludes
    strings which begin in numbers, special characters, or double underscores.

    Args:
        - init_dict (dict, optional): dictionary to initialize the `DotDict`
        with
        - **kwargs (optional): task_hash, _output pairs with which to initialize the
        `DotDict`

    Example:
        ```python
        dotdict = DotDict({'a': 34}, b=56, c=set())
        dotdict.a # 34
        dotdict['b'] # 56
        dotdict.c # set()
        ```
    """

    def __init__(self, init_dict: DictLike = None, **kwargs: Any):
        # a DotDict could have a task_hash that shadows `update`
        if init_dict:
            super().update(init_dict)
        super().update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        """
        This method is defined for MyPy, which otherwise tries to type
        the inherited `.get()` method incorrectly.

        Args:
            - task_hash (str): the task_hash to retrieve
            - default (Any): a default _output to return if the task_hash is not found

        Returns:
            - Any: the _output of the task_hash, or the default _output if the task_hash is not found
        """
        return super().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.__dict__[key]  # __dict__ expects string keys

    def __setitem__(self, key: str, value: Any) -> None:
        self.__dict__[key] = value

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict__.keys())

    def __delitem__(self, key: str) -> None:
        del self.__dict__[key]

    def __len__(self) -> int:
        return len(self.__dict__)

    def __repr__(self) -> str:
        if len(self) > 0:
            return "<{}: {}>".format(
                type(self).__name__, ", ".join(sorted(repr(k) for k in self.keys()))
            )
        else:
            return "<{}>".format(type(self).__name__)

    def copy(self) -> "DotDict":
        """Creates and returns a shallow copy of the current DotDict"""
        return type(self)(self.__dict__.copy())

    def to_dict(self) -> dict:
        """
        Converts current `DotDict` (and any `DotDict`s contained within)
        to an appropriate nested dictionary.
        """
        # mypy cast
        return cast(dict, as_nested_dict(self, dct_class=dict))


def as_nested_dict(
        obj: Union[DictLike, Iterable[DictLike]],
        dct_class: type = DotDict) -> Union[DictLike, Iterable[DictLike]]:
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

    elif isinstance(obj, (dict, DotDict)):
        # DotDicts could have keys that shadow `update` and `items`, so we
        # take care to avoid accessing those keys here
        return dct_class(
            {
                k: as_nested_dict(v, dct_class)
                for k, v in getattr(obj, "__dict__", obj).items()
            }
        )
    return obj


class Context(DotDict):
    """
    A thread safe context store for Prefect _input.

    The `Context` is a `DotDict` subclass, and can be instantiated the same way.

    Args:
        - *run_args (Any): arguments to provide to the `DotDict` constructor (e.g.,
            an initial dictionary)
        - **kwargs (Any): any task_hash / _output pairs to initialize this context with
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # TODO merge with config
        init = {}
        # Overwrite with explicit run_args
        init.update(dict(*args, **kwargs))
        super().__init__(init)
        self.__flow_stack = []
        self.__env_tasks = {}
        self.__dict__['_flow_stack'] = []

    @property
    def env_tasks(self):
        return self.__env_tasks

    @property
    def flow_stack(self):
        return self.__flow_stack

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
            "access context as an attribute of the `prefect` module, as in `prefect.context`"
        )

    def __repr__(self) -> str:
        return "<Context>"

    @contextlib.contextmanager
    def __call__(self, *args: MutableMapping, **kwargs: Any) -> Iterator["Context"]:
        """
        A context manager for setting / resetting the context context

        Example:
            from context import context
            with context(dict(a=1, b=2), c=3):
                print(context.a) # 1
        """
        # Avoid creating new `Context` object, copy as `dict` instead.
        previous_context = self.__dict__.copy()
        try:
            new_context = dict(*args, **kwargs)
            self.update(new_context)  # type: ignore
            yield self
        finally:
            self.clear()
            self.update(previous_context)


context = Context()

config = Context()
