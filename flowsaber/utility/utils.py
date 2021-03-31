# TODO try to make things more simple for ease of pickle problems

import builtins
import inspect
import os
import types
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Sequence
from typing import Union, Callable

from makefun import with_signature

from flowsaber.core.channel import Channel, End

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
        For base_cls is Task, method_name isrun

        wrap  no-self argument function
            @deco
            def test(a, b, c) -> d:
                "doc"
                pass

        into a Class:
            class Test(Task):
                defrun(self, a, b, c) -> d:
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
                defrun(self, a, b, c) -> d:
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
    try:
        prev_cwd = os.getcwd()
        os.chdir(path)
    except Exception as e:
        raise e

    try:
        yield Path(path)
    finally:
        os.chdir(prev_cwd)


@contextmanager
def capture_local(event='return'):
    import sys
    local_vars = {}

    def capture_tracer(frame, _event, arg=None):
        if _event == event:
            local_vars.update(frame.f_locals)
        else:
            return capture_tracer

    sys.settrace(capture_tracer)

    yield local_vars

    sys.settrace(None)
