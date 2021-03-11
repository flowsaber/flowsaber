from .flow import Flow
from .task import Task
from .utils import SELF_SIG

import inspect
from typing import Callable

from makefun import with_signature
from functools import partial


def make_method_deco(base_cls: type, method_name: str):
    def deco(fn: Callable = None, **kwargs):
        """
        For base_cls is Task, method_name is run

        wrap  no-self argument function
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

        return type(cls_name, (base_cls,), {method_name: wrapper})(**kwargs)

    deco.__name__ = deco.__qualname__ = base_cls.__name__.lower()
    return deco


task = make_method_deco(Task, 'run')

flow = make_method_deco(Flow, 'run')
