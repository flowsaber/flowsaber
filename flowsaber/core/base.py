import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, List, Sequence

from makefun import with_signature

from flowsaber.context import context
from flowsaber.core.channel import Consumer, Channel
from flowsaber.utility.logtool import get_logger
from flowsaber.utility.utils import TaskOutput

logger = get_logger(__name__)


class CopySigMeta(type):
    PAIR_ARG_NAME = 'FUNC_PAIRS'

    def __new__(mcs, class_name, bases, class_dict):
        func_pairs = class_dict.get(mcs.PAIR_ARG_NAME, None)
        if func_pairs is None:
            for base_cls in bases:
                func_pairs = getattr(base_cls, mcs.PAIR_ARG_NAME, None)
                if func_pairs:
                    break
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
        self._initialized: bool = False
        self._input_args: Optional[tuple] = None
        self._input_kwargs: Optional[dict] = None
        self._input_len: Optional[int] = None
        self._input: Optional[Consumer] = None
        self._output: Optional[TaskOutput] = None

        self.name = name
        self.identity_name = ""
        self.name_with_id = True

    def __repr__(self):
        name = f"{self.name}|{type(self).__name__}({type(self).__bases__[0].__name__})"
        if self.name_with_id:
            name += f"[{id(self)}]"

        return name.lstrip('|')

    def __call__(self, *args, **kwargs) -> Union[List[Channel], Channel, None]:
        raise NotImplementedError

    def copy_clean(self):
        from copy import copy
        return copy(self)

    def __copy__(self):
        from copy import deepcopy
        cls = self.__class__
        new = cls.__new__(cls)
        for k, v in self.__dict__.items():
            if not k.startswith('_'):
                setattr(new, k, deepcopy(v))

        return new

    @property
    def initialized(self):
        return hasattr(self, '_initialized') and self._initialized

    def copy_new(self, *args, **kwargs):
        from copy import copy
        new = copy(self)
        new._initialized = True
        new.initialize_name()
        new.initialize_input(*args, **kwargs)
        return new

    def initialize_name(self):
        up_flow = context.up_flow
        up_flow_name = str(up_flow.name) if up_flow else ""
        self.identity_name = f"{up_flow_name}-{self}".lstrip('-')

    def initialize_input(self, *args, **kwargs):
        self._input_args = args
        self._input_kwargs = kwargs
        self._input_len = len(args) + len(kwargs)

    def clean(self):
        pass

    async def execute(self, **kwargs):
        if not self.initialized:
            raise ValueError("The Task/Flow object is not initialized, "
                             "please use task()/flow() to initialize it.")

    def run(self, *args, **kwargs):
        return NotImplementedError


@dataclass
class TaskConfig:
    # identity info
    slug: str = None
    tags: tuple = None
    # run info
    workdir: str = "work"
    pubdir: Union[str, Sequence[str]] = None
    skip_error: bool = False
    executor: str = 'ray'
    cache: bool = True
    cache_type: str = 'local'
    retry: int = 0
    fork: int = 7
    # resource
    cpu: int = 1
    memory: int = 4
    time: int = 1
    io: int = 3
    # shell task env
    module: str = None
    conda: str = None
    image: str = None
    runtime: str = "singularity"

    def __getitem__(self, item):
        return self.__dict__[item]

    def get(self):
        return self.__dict__.get

    def resources(self):
        resource_keys = ['cpu', 'memory', 'time', 'io']
        return {k: self[k] for k in resource_keys}

    def cost_resources(self, config):
        for k, v in self.resources().items():
            if k in config:
                config[k] -= v

    def release_resources(self, config):
        for k, v in self.resources().items():
            if k in config:
                config[k] += v

    def update(self, dic: dict):
        for k, v in dic.items():
            setattr(self, k, v)

    def get_pubdirs(self):
        if not self.pubdir:
            return []
        pubdirs = self.pubdir
        if not isinstance(self.pubdir, (tuple, list)):
            pubdirs = [self.pubdir]
        pubdirs = [Path(p).expanduser().resolve() for p in pubdirs]
        return pubdirs
