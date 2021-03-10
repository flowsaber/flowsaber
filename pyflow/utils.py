import types
import inspect
import builtins
from typing import Union

from makefun import with_signature

from .channel import Channel, ChannelList, End

INPUT = ChannelList
OUTPUT = Union[ChannelList, Channel]
DATA = Union[object, End]


def extend_method(cls):
    def set_method(obj):
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


def get_sig_param(sig, param_type) -> tuple:
    return tuple(p for p in sig.parameters.values()
                 if p.kind == param_type)


def _a(*args: Union[object, Channel]):
    pass


def _c(self):
    pass


def _b() -> OUTPUT:
    pass


ARGS_SIG = list(inspect.signature(_a).parameters.values())[0]
SELF_SIG = list(inspect.signature(_c).parameters.values())[0]
OUTPUT_ANNOTATION = inspect.signature(_b).return_annotation


def generate_operator(cls: type):
    """
    wrap:

        class A():
            def __init__(self, a: int, b: str = "x", **kwargs):
                pass
    into a function:
        def a(*args, a: int, b: str = "x", **kwargs):
            return A(a=a, b=b, **kwargs)(*args)
    """
    assert isinstance(cls, type), "The input_ch argument must be a class"
    # Get signature of cls.__init__ except for self
    fn = types.MethodType(cls.__init__, object)
    fn_name = cls.__name__.lower()
    # check name collide with builtins
    if fn_name in dir(builtins):
        fn_name += "_by"
    # replace POSITIONAL_OR_KEYWORD to KEYWORD_ONLY
    # append *args VAR_POSITIONAL at the front
    sigs = list(inspect.signature(fn).parameters.values())
    for i, sig in enumerate(sigs):
        if sig.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError("The input_ch cls.__init__ should not have *args: VAR_POSITIONAL parameter.")
        elif sig.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            sigs[i] = sig.replace(kind=inspect.Parameter.KEYWORD_ONLY)
    sigs.insert(0, ARGS_SIG)
    sigs = inspect.Signature(sigs, return_annotation=OUTPUT_ANNOTATION)

    @with_signature(sigs, func_name=fn_name, qualname=fn_name, doc=cls.__doc__)
    def inner(*args, **kwargs):
        return cls(**kwargs)(*args)

    return inner


def generate_method(cls: type):
    """
    Wrap
        class A():
            def __init__(self, a: int, b: str = "x", **kwargs):
                pass
    into a function:
        def a(self, *args, a: int, b: str = "x", **kwargs):
            return A(a=a, b=b, **kwargs)(self, *args)
    """
    assert isinstance(cls, type), "The input_ch argument must be a class"
    fn = types.MethodType(cls.__init__, object)
    fn_name = cls.__name__.lower()
    sigs = list(inspect.signature(fn).parameters.values())
    for i, sig in enumerate(sigs):
        if sig.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError(f"The input class {cls}.__init__ should not have *args: VAR_POSITIONAL parameter.")
        elif sig.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            sigs[i] = sig.replace(kind=inspect.Parameter.KEYWORD_ONLY)
    sigs = [SELF_SIG, ARGS_SIG] + sigs
    sigs = inspect.Signature(sigs, return_annotation=OUTPUT_ANNOTATION)

    @with_signature(sigs, func_name=fn_name, qualname=fn_name, doc=cls.__doc__)
    def inner(self, *args, **kwargs):
        return cls(**kwargs)(self, *args)

    return inner
