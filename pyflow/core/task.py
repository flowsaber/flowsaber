import inspect
import os
import subprocess
from inspect import BoundArguments, Parameter
from pathlib import Path
from typing import Union

from dask.base import tokenize

from pyflow.context import pyflow
from pyflow.core.cache import LocalCache
from pyflow.core.executor import get_executor
from pyflow.core.target import File
from pyflow.utility.doctool import NumpyDocInheritor
from pyflow.utility.logtool import get_logger
from pyflow.utility.utils import INPUT, OUTPUT, DATA, change_cwd, copy_sig_meta, class_deco
from .channel import Channel, QueueChannel, END, ChannelList, End

logger = get_logger(__name__)


class FlowComponent(metaclass=NumpyDocInheritor):
    def __init__(self, name: str = "", retry: int = 1, **kwargs):
        self.input_args = None
        self.input_kwargs = None
        self.input: INPUT = None
        self.output: OUTPUT = None

        self.name = name
        self.name_with_id = True
        self.retry = retry
        self.up_flow: 'Flow' = None
        self.top_flow: 'Flow' = None
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def __repr__(self):
        name = f"{self.name}|{type(self).__name__}({type(self).__bases__[0].__name__})"
        if self.name_with_id:
            name += f"[{hex(hash(self))}]"

        return name.lstrip('|')

    def __call__(self, *args, **kwargs) -> OUTPUT:
        raise NotImplementedError

    def copy_new(self):
        from copy import deepcopy
        # must deepcopy, otherwise all flow will share self.task_futures and other container attrs
        new = deepcopy(self)
        new.up_flow = pyflow.up_flow
        new.top_flow = pyflow.top_flow
        up_flow_name = str(new.up_flow.name) if new.up_flow else ""
        new.name = f"{up_flow_name}-{new}".lstrip('-')
        logger.debug(f"Created new component {new.name}")
        return new

    def initialize_input(self, *args, **kwargs) -> ChannelList:
        self.input_args = args
        self.input_kwargs = kwargs
        self.input = ChannelList(list(args) + list(kwargs.values()), name=str(self), task=self)
        return self.input

    def clean(self):
        return


class BaseTask(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.num_output_ch = 1

    def initialize_output(self) -> OUTPUT:
        if self.num_output_ch == 1:
            self.output = QueueChannel(task=self)
        else:
            channels = tuple(QueueChannel(task=self) for i in range(self.num_output_ch))
            self.output = ChannelList(channels, task=self)
        return self.output

    def register_graph(self, *args, **kwargs):
        g = self.top_flow.graph
        kwargs = dict(
            style="filled",
            colorscheme="svg"
        )
        g.node(self.name, **kwargs)
        for ch in self.input:
            src = ch.task or ch
            shape = 'box' if isinstance(self, Task) else 'ellipse'
            if f"\t{src.name}" not in g.source:
                g.node(src.name, shape=shape, **kwargs)
            g.edge(src.name, self.name)

    def __call__(self, *args, **kwargs) -> OUTPUT:
        task = self.copy_new()
        task.initialize_input(*args, **kwargs)
        task.initialize_output()
        task.register_graph(*args, **kwargs)

        up_flow = pyflow.up_flow
        assert task not in up_flow.tasks
        up_flow.tasks.setdefault(task, {})

        return task.output

    async def execute(self):
        try:
            res = await self.handle_channel_input(self.input)
            await self.output.put(END)
        except Exception as e:
            raise e
        finally:
            self.clean()
        return res

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            await self.handle_data_input(data)
        await self.handle_data_input(END)

    async def handle_data_input(self, input_data: DATA):
        return NotImplemented

    def __ror__(self, lch: Channel) -> Channel:
        return self(lch)


class Task(BaseTask):
    def __init__(self, retry: int = 0, workdir: str = "", pubdir: str = "", cache_cls=LocalCache, **kwargs):
        super().__init__(**kwargs)
        self.retry = retry
        self.cache_cls = cache_cls
        self.cache = None
        self.workdir = workdir
        self.pubdir = pubdir
        self.task_key = None
        self.input_key = None

    def hash_code(self):
        fn = self.run
        # for wrapped func, use __source_func__ as a link
        while hasattr(fn, '__source_func__'):
            fn = fn.__source_func__
        return tokenize(inspect.getsource(fn))

    def copy_new(self):
        new = super().copy_new()
        # task_key must a absolute path
        task_key = f"{new.__class__.__name__}-{self.hash_code()}"
        task_workdir = Path(self.workdir) / Path(task_key)
        task_workdir.mkdir(parents=True, exist_ok=True)

        new.task_key = str(Path(task_workdir).expanduser().resolve())
        new.cache = self.cache_cls(task_key=new.task_key)
        return new

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            # handle input input_args
            bound_args = self.bind_input_args(data)
            self.input_key = input_key = await self.cache.input_key(bound_args)
            # try to use cache
            if await self.cache.contains(input_key):
                res = self.cache.get(input_key)
            else:
                # handle retry
                exception = None
                bound_args = self.pre_handle_data_input(bound_args)
                for i in range(self.retry + 1):
                    try:
                        res = await self.handle_data_input(bound_args)
                    except Exception as e:
                        exception = e
                    else:
                        self.cache.put(input_key, res)
                        break
                else:
                    raise exception
            self.input_key = None
            await self.output.put(res)

        await self.handle_data_input(END)

    def bind_input_args(self, input_data: DATA) -> BoundArguments:
        # 1. build BoundArgument
        if len(self.input) == 1:
            input_data = (input_data,)
        len_args = len(self.input_args)
        args = input_data[:len_args]
        kwargs = {}
        for i, k in enumerate(self.input_kwargs.keys()):
            kwargs[k] = input_data[len_args + i]
        run_args = inspect.signature(self.run).bind(*args, **kwargs)
        run_args.apply_defaults()
        # 2. do some type conversion in case with type annotation
        run_params = dict(inspect.signature(self.run).parameters)
        arguments = run_args.arguments
        for arg, param in run_params.items():
            ano_type = param.annotation
            if ano_type is Parameter.empty or not isinstance(ano_type, type):
                continue
            value = arguments[arg]
            if not isinstance(value, ano_type):
                try:
                    arguments[arg] = ano_type(value)
                except Exception as e:
                    raise TypeError(f"The input argument `{arg}` has annotation `{ano_type}`, but "
                                    f"the input value `{value}` can not be converted.")

        return run_args

    def pre_handle_data_input(self, input_data: BoundArguments) -> BoundArguments:
        return input_data

    async def handle_data_input(self, input_data: BoundArguments):
        if input_data is END:
            return END
        res = await get_executor().run(self.run, *input_data.args, **input_data.kwargs)
        return res

    def clean(self):
        if self.cache:
            self.cache.persist()

    def run(self, *args, **kwargs):
        """
        This function should be stateless
        """
        raise NotImplementedError


class ShellTask(Task, metaclass=copy_sig_meta(('command', 'run'))):
    COMMAND = "COMMAND"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.command_str = None
        self.command_output = None

    def command(self, *args, **kwargs) -> str:
        raise NotImplementedError

    def pre_handle_data_input(self, input_args: BoundArguments) -> BoundArguments:
        # run user defined function and get the true bash commands
        with pyflow():
            self.command_output = output = self.command(*input_args.args, **input_args.kwargs)
            if isinstance(output, tuple) and not all(isinstance(p, str) for p in output):
                raise ValueError(f"The output {output} must be (tuple of) str.")
            if not isinstance(output, (str, tuple)) and output is not None:
                raise ValueError(f"The output {output} must be a str or None.")
            self.command_str = pyflow.get(self.COMMAND)
            if self.command_str is None:
                raise ValueError("ShellTask must be registered with a shell command_str "
                                 "by calling `_(CMD) or Bash(CMD)` inside the function.")
        return input_args

    @property
    def running_path(self) -> Path:
        assert self.task_key is not None and self.input_key is not None
        return Path(self.task_key) / Path(self.input_key)

    def run(self, *args, **kwargs) -> Union[End, str, File, tuple]:
        """
        This function should be stateless
        """

        with change_cwd(self.running_path) as path:
            # run bash
            env = os.environ.copy()
            with subprocess.Popen(self.command_str,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, env=env, shell=True) as p:
                results = p.stdout.read()
                p.wait()
                if p.returncode:
                    raise ValueError(f"The stdout/err is {results}")
            results = results.decode()
            output = self.command_output
            # 1. return stdout
            if output is None:
                return results
            # 2. return
            outputs = output if isinstance(output, tuple) else [output]
            # expect outputs are all str/Path object
            files = [[File(f.resolve()) for f in path.glob(p)] for p in outputs]

            for i, fs in enumerate(files):
                if len(fs) == 0:
                    files[i] = END
                else:
                    for f in fs:
                        f.hash = f.calculate_hash()
                    files[i] = tuple(fs) if len(fs) > 1 else fs[0]

            files = tuple(files) if len(files) > 1 else files[0]
            return files

    @classmethod
    def register_command(cls, cmd: str):
        assert isinstance(cmd, str)
        pyflow[cls.COMMAND] = cmd


Shell = _ = ShellTask.register_command
task = class_deco(Task, 'run')

shell = class_deco(ShellTask, 'command')
