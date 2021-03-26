import inspect
from dataclasses import dataclass
from typing import Optional, Union, List, Sequence, Tuple

from makefun import with_signature

from pyflow.context import pyflow
from pyflow.core.channel import Consumer, Channel
from pyflow.utility.doctool import NumpyDocInheritor
from pyflow.utility.logtool import get_logger
from pyflow.utility.utils import TaskOutput

logger = get_logger(__name__)


class CopySigMeta(NumpyDocInheritor):
    def __new__(mcs, class_name, bases, class_dict, func_pairs: Sequence[Tuple[str, str]] = None):
        if func_pairs:
            assert all(len(item) == 2 for item in func_pairs)
            for src_fn, tgt_fn in func_pairs:
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


class FlowComponent(object, metaclass=CopySigMeta):
    def __init__(self, name: str = "", **kwargs):
        self._input_args = None
        self._input_kwargs = None
        self._input: Optional[Consumer] = None
        self._input_len: int = None
        self._output: TaskOutput = None

        self.name = name
        self.identity_name = ""
        self.name_with_id = True

    def __copy__(self):
        from copy import deepcopy
        cls = self.__class__
        new = cls.__new__(cls)
        for k, v in self.__dict__.items():
            if not k.startswith('_'):
                try:
                    setattr(new, k, deepcopy(v))
                except:
                    raise ValueError

        return new

    def copy_new(self, *args, **kwargs):
        from copy import copy
        new = copy(self)
        new.initialize_name()
        new.initialize_input(*args, **kwargs)
        return new

    def copy_clean(self):
        from copy import copy
        return copy(self)

    def __repr__(self):
        name = f"{self.name}|{type(self).__name__}({type(self).__bases__[0].__name__})"
        if self.name_with_id:
            name += f"[{id(self)}]"

        return name.lstrip('|')

    def __call__(self, *args, **kwargs) -> Union[List[Channel], Channel, None]:
        raise NotImplementedError

    def initialize_name(self):
        up_flow = pyflow.up_flow
        up_flow_name = str(up_flow.name) if up_flow else ""
        self.identity_name = f"{up_flow_name}-{self}".lstrip('-')

    def initialize_input(self, *args, **kwargs):
        self._input_args = args
        self._input_kwargs = kwargs
        self._input_len = len(args) + len(kwargs)

    def clean(self):
        return


@dataclass
class TaskConfig:
    # Task
    slug: str = None
    tags: tuple = None
    workdir: str = "work"
    stage: str = "link"
    # Per task resource cost or global resource constraint
    cpu: int = 0
    memory: int = 0
    time: int = 0
    io: int = 0
    # Per task threshold or global threshold
    fork: int = 999999
    retry: int = 0
    skip_error: bool = False
    # Shell Task
    pubdir: str = None
    storedir: str = None
    modules: str = None
    conda_env: str = None
    image: str = None
    runtime: str = "singularity"
    # cache
    cache: bool = True
    cache_type: str = 'local'
    # executor
    executor: str = 'ray'

    def __getattr__(self, item):
        if item.startswith("max_"):
            return 999999999
        else:
            raise AttributeError

    def update(self, dic: dict):
        for k, v in dic.items():
            if hasattr(self, k) or k.startswith('max_'):
                setattr(self, k, v)
