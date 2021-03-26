import builtins
import inspect
import os
import types
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Sequence
from typing import Union, Callable, Tuple

from makefun import with_signature

from pyflow.core.channel import Channel, End
from pyflow.utility.doctool import NumpyDocInheritor

TaskOutput = Union[Sequence[Channel], Channel, None]
Data = Union[tuple, End]


def get_sig_param(sig, param_type) -> tuple:
    return tuple(p for p in sig.parameters.values()
                 if p.kind == param_type)


def _a(*args: Union[object, Channel]):
    pass


def _c(self):
    pass


def _b() -> TaskOutput:
    pass


ARGS_SIG = list(inspect.signature(_a).parameters.values())[0]
SELF_SIG = list(inspect.signature(_c).parameters.values())[0]
OUTPUT_ANNOTATION = inspect.signature(_b).return_annotation


def class_to_func(cls: type):
    """
    wrap:

        class A():
            def __init__(self, a: int, b: str = "x", **kwargs):
                pass
    into a function:
        def a(*run_args, a: int, b: str = "x", **kwargs):
            return A(a=a, b=b, **kwargs)(*run_args)
    """
    assert isinstance(cls, type), "The consumer argument must be a class"
    # Get signature of cls.__init__ except for self
    fn = types.MethodType(cls.__init__, object)
    fn_name = cls.__name__.lower()
    # check name collide with builtins
    if fn_name in dir(builtins):
        fn_name += "_by"
    # replace POSITIONAL_OR_KEYWORD to KEYWORD_ONLY
    # append *run_args VAR_POSITIONAL at the front
    sigs = list(inspect.signature(fn).parameters.values())
    for i, sig in enumerate(sigs):
        if sig.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError("The consumer cls.__init__ should not have *run_args: VAR_POSITIONAL parameter.")
        elif sig.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            sigs[i] = sig.replace(kind=inspect.Parameter.KEYWORD_ONLY)
    sigs.insert(0, ARGS_SIG)
    sigs = inspect.Signature(sigs, return_annotation=OUTPUT_ANNOTATION)

    @with_signature(sigs, func_name=fn_name, qualname=fn_name, doc=cls.__doc__)
    def inner(*args, **kwargs):
        return cls(**kwargs)(*args)

    return inner


def class_to_method(cls: type):
    """
    Wrap
        class A():
            def __init__(self, a: int, b: str = "x", **kwargs):
                pass
    into a function:
        def a(self, *run_args, a: int, b: str = "x", **kwargs):
            return A(a=a, b=b, **kwargs)(self, *run_args)
    """
    assert isinstance(cls, type), "The consumer argument must be a class"
    fn = types.MethodType(cls.__init__, object)
    fn_name = cls.__name__.lower()
    sigs = list(inspect.signature(fn).parameters.values())
    for i, sig in enumerate(sigs):
        if sig.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError(f"The input class {cls}.__init__ should not have *run_args: VAR_POSITIONAL parameter.")
        elif sig.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            sigs[i] = sig.replace(kind=inspect.Parameter.KEYWORD_ONLY)
    sigs = [SELF_SIG, ARGS_SIG] + sigs
    sigs = inspect.Signature(sigs, return_annotation=OUTPUT_ANNOTATION)

    @with_signature(sigs, func_name=fn_name, qualname=fn_name, doc=cls.__doc__)
    def inner(self, *args, **kwargs):
        return cls(**kwargs)(self, *args)

    return inner


def extend_method(cls):
    def set_method(obj: Union[type, Callable]):
        import inspect

        funcs = []
        if inspect.isclass(obj):
            for name, func in inspect.getmembers(obj):
                if not name.startswith('__'):
                    funcs.append(func)
        elif inspect.isfunction(obj):
            funcs.append(obj)
        else:
            raise ValueError("Should be a class or function")

        for func in funcs:
            setattr(cls, func.__name__, func)

    return set_method


def class_deco(base_cls: type, method_name: str):
    def deco(fn: Callable = None, **kwargs):
        """
        For base_cls is Task, method_name is run

        wrap  no-self argument function
            @deco
            def test(a, b, c) -> d:
                "doc"
                pass

        into a Class:
            class Test(Task):
                def run(self, a, b, c) -> d:
                    "doc"
                    return test(a, b, c)
        and return:
            Test()

        wrap with-self argument function
            @deco
            def test(self, a, b, c) -> d:
                "doc"
                pass

        into a Class:
            class Test(Task):
                def run(self, a, b, c) -> d:
                    "doc"
                    return test(self, a, b, c)
        and return:
            Test()
        """
        if fn is None:
            # TODO builtin partial does not maintain signature while makefun.partial has bug
            return partial(deco, **kwargs)
        assert inspect.isfunction(fn), "Only functions are supported"
        cls_name: str = fn.__name__
        cls_name = cls_name[0].upper() + cls_name[1:]

        sig = inspect.signature(fn)
        sigs = list(sig.parameters.values())
        params = {
            'doc': fn.__doc__,
            'func_name': method_name,
            'qualname': method_name
        }
        if not (sigs[0].name == 'self' and sigs[0].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD):
            sigs.insert(0, SELF_SIG)

            @with_signature(inspect.Signature(sigs, return_annotation=sig.return_annotation), **params)
            def wrapper(self, *args, **kwargs):
                return fn(*args, **kwargs)
        else:
            @with_signature(inspect.Signature(sigs, return_annotation=sig.return_annotation), **params)
            def wrapper(self, *args, **kwargs):
                return fn(self, *args, **kwargs)
        # used for source the real func
        wrapper.__source_func__ = fn
        return type(cls_name, (base_cls,), {method_name: wrapper})(**kwargs)

    deco.__name__ = deco.__qualname__ = base_cls.__name__.lower()
    return deco


def copy_sig_meta(*func_name_pairs: Tuple[str, str]) -> type:
    assert all(len(item) == 2 for item in func_name_pairs)

    class CopySigMeta(NumpyDocInheritor):
        def __new__(mcs, class_name, bases, class_dict):
            for src_fn, tgt_fn in func_name_pairs:
                if src_fn == tgt_fn:
                    raise ValueError(f"src {src_fn} and tgt {tgt_fn} can not be the same.")
                src = class_dict.get(src_fn) or next(getattr(c, src_fn) for c in bases)
                tgt = class_dict.get(tgt_fn) or next(getattr(c, tgt_fn) for c in bases)
                sigs = inspect.signature(src)

                @with_signature(sigs, func_name=tgt.__name__, qualname=tgt.__qualname__, doc=src.__doc__)
                def new_tgt_fn(*args, **kwargs):
                    return tgt(*args, **kwargs)

                # used for source the real func
                new_tgt_fn.__source_func__ = src
                class_dict[tgt_fn] = new_tgt_fn

            return super().__new__(mcs, class_name, bases, class_dict)

    return CopySigMeta


@contextmanager
def change_cwd(path: Union[str, Path]) -> Path:
    """
    A context manager which changes the working directory to the given
    path, and then changes it back to its previous _output on exit.
    Usage:
    > # Do something in original directory
    > with working_directory('/my/new/path'):
    >     # Do something in new directory
    > # Back to old directory
    """
    path = Path(path).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield Path(path)
    finally:
        os.chdir(prev_cwd)


