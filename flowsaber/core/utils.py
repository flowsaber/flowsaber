import builtins
import inspect
import types
from functools import partial
from typing import Union, Callable, List, Tuple, Any

from makefun import with_signature

from flowsaber.core.channel import ARGS_SIG, OUTPUT_ANNOTATION


def _self_func(self):
    pass


SELF_SIG = list(inspect.signature(_self_func).parameters.values())[0]


def get_sig_param(sig, param_type) -> tuple:
    return tuple(p for p in sig.parameters.values()
                 if p.kind == param_type)


def class_to_func(cls: type):
    """
    wrap:

        class A():
            def __init__(self, a: int, b: str = "x", **kwargs):
                pass
    into a function:
        def a(*data, a: int, b: str = "x", **kwargs):
            return A(a=a, b=b, **kwargs)(*data)
    """
    assert isinstance(cls, type), "The consumer argument must be a class"
    # Get signature of cls.__init__ except for self
    fn = types.MethodType(cls.__init__, object)
    fn_name = cls.__name__.lower()
    # check name collide with builtins
    if fn_name in dir(builtins):
        fn_name += "_by"
    # replace POSITIONAL_OR_KEYWORD to KEYWORD_ONLY
    # append *data VAR_POSITIONAL at the front
    sigs = list(inspect.signature(fn).parameters.values())
    for i, sig in enumerate(sigs):
        if sig.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError("The consumer cls.__init__ should not have *data: VAR_POSITIONAL parameter.")
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
        def a(self, *data, a: int, b: str = "x", **kwargs):
            return A(a=a, b=b, **kwargs)(self, *data)
    """
    assert isinstance(cls, type), "The consumer argument must be a class"
    fn = types.MethodType(cls.__init__, object)
    fn_name = cls.__name__.lower()
    sigs = list(inspect.signature(fn).parameters.values())
    for i, sig in enumerate(sigs):
        if sig.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError(f"The input class {cls}.__init__ should not have *data: VAR_POSITIONAL parameter.")
        elif sig.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            sigs[i] = sig.replace(kind=inspect.Parameter.KEYWORD_ONLY)
    sigs = [SELF_SIG, ARGS_SIG] + sigs
    sigs = inspect.Signature(sigs, return_annotation=OUTPUT_ANNOTATION)

    @with_signature(sigs, func_name=fn_name, qualname=fn_name, doc=cls.__doc__)
    def inner(self, *args, **kwargs):
        return cls(**kwargs)(self, *args)

    return inner


def extend_method(cls):
    """Decorator to extend attributes of a class. Can be used in two ways:

    1:
    @extend_method(some_class)
    def new_method(self):
        pass

    2:
    @extend_method(some_class)
    class A:
        def new_method1(self):
            pass

        def new_method2(self):
            pass

    Parameters
    ----------
    cls

    Returns
    -------

    """

    def set_method(obj: Union[type, Callable]):
        import inspect

        funcs = []
        if inspect.isclass(obj):
            for name, func in inspect.getmembers(obj):
                if not name.startswith('_'):
                    funcs.append((name, func))
        elif inspect.isfunction(obj):
            funcs.append((None, obj))
        else:
            raise ValueError("Should be a class or function")

        for func_name, func in funcs:
            setattr(cls, func_name or func.__name__, func)

    return set_method


def class_deco(base_cls: type, method_name: str) -> Callable:
    def deco(fn: Callable = None, **kwargs) -> base_cls:
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
        cls_name: str = fn.__name__
        cls_name = cls_name[0].upper() + cls_name[1:]

        sig = inspect.signature(fn)
        sigs = list(sig.parameters.values())
        params = {
            'doc': fn.__doc__,
            'func_name': method_name,
            'qualname': method_name
        }
        no_args = not sigs
        first_is_self = sigs and sigs[0].name == 'self' and sigs[0].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
        if no_args or not first_is_self:
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
        cls = type(cls_name, (base_cls,), {method_name: wrapper})(**kwargs)
        return cls

    deco.__name__ = deco.__qualname__ = base_cls.__name__.lower()
    return deco


def check_cycle(edges: List[Tuple[Any, Any]]) -> bool:
    """Given a list of edges, check if the corresponding graph contains cycle by finding a topological sorting

    Parameters
    ----------
    edges

    Returns
    -------

    """
    from collections import defaultdict, deque
    # convert node to index
    nodes = {node for edge in edges for node in edge}
    nodes2id = dict(zip(nodes, range(len(nodes))))
    edges = [(nodes2id[n1], nodes2id[n2]) for n1, n2 in edges]

    # build adjacency graph and in-degree vector
    G, indegree = defaultdict(list), [0] * len(nodes)
    for src, tgt in edges:
        indegree[tgt] += 1
        G[src].append(tgt)

    zero_dq = deque()
    for node, ind in enumerate(indegree):
        if ind == 0:
            zero_dq.append(node)
    # iteratively find zero in-degree node
    path = []
    while len(zero_dq):
        cur = zero_dq.popleft()
        path.append(cur)
        for out in G[cur]:
            indegree[out] -= 1
            if indegree[out] == 0:
                zero_dq.append(out)

    return len(path) != len(nodes)
