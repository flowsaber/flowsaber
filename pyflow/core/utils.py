from dataclasses import dataclass
from typing import Optional, Union, List

from pyflow.context import DotDict, pyflow
from pyflow.core.channel import Consumer, Channel
from pyflow.utility.doctool import NumpyDocInheritor
from pyflow.utility.logtool import get_logger
from pyflow.utility.utils import TaskOutput

logger = get_logger(__name__)


class FlowComponent(object, metaclass=NumpyDocInheritor):
    def __init__(self, name: str = "", **kwargs):
        self._input_args = None
        self._input_kwargs = None
        self._input: Optional[Consumer] = None
        self._input_len: int = None
        self._output: TaskOutput = None

        self.name = name
        self.full_name = ""
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
        self.full_name = f"{up_flow_name}-{self}".lstrip('-')

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
    cpu: int = 0
    memory: int = 0
    time: int = 0
    io: int = 0
    retry: int = 0
    fork: int = 99999999999
    # Shell Task
    pubdir: str = None
    storedir: str = None
    modules: str = None
    conda_env: str = None
    image: str = None
    runtime: str = "singularity"

    def __getattr__(self, item):
        if item.startswith("max_"):
            return 999999999
        else:
            return getattr(self.__dict__, item)
