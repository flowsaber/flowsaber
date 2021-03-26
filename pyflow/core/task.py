import inspect
import os
import subprocess
from collections import abc
from inspect import BoundArguments, Parameter
from pathlib import Path
from typing import Optional, Callable

from dask.base import tokenize

from pyflow.context import pyflow, config
from pyflow.core.cache import LocalCache
from pyflow.core.env import Env
from pyflow.core.executor import get_executor
from pyflow.core.target import File, Stdout, Stdin
from pyflow.utility.logtool import get_logger
from pyflow.utility.utils import change_cwd, copy_sig_meta, class_deco, TaskOutput, Data
from .channel import Channel, END, Consumer
from .utils import FlowComponent, TaskConfig

logger = get_logger(__name__)


class BaseTask(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.num_output_ch = 1

    def initialize_input(self, *args, **kwargs) -> Consumer:
        super().initialize_input(*args, **kwargs)
        channels = list(args) + list(kwargs.values())
        self._input = Consumer.from_channels(channels, consumer=self, task=self)
        return self._input

    def initialize_output(self) -> TaskOutput:
        self._output = tuple(Channel(task=self) for i in range(self.num_output_ch))
        if len(self._output) == 1:
            self._output = self._output[0]
        return self._output

    def register_graph(self):
        g = pyflow.top_flow.graph
        kwargs = dict(
            style="filled",
            colorscheme="svg"
        )
        g.node(self.name, **kwargs)
        for q in self._input.queues:
            src = q.ch.task or q.ch
            shape = 'box' if isinstance(self, Task) else 'ellipse'
            if f"\t{src.name}" not in g.source:
                g.node(src.name, shape=shape, **kwargs)
            g.edge(src.name, self.name)

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        new.initialize_output()
        new.register_graph()
        return new

    def __call__(self, *args, **kwargs) -> TaskOutput:
        task = self.copy_new(*args, **kwargs)
        up_flow = pyflow.up_flow
        assert task not in up_flow.tasks
        up_flow.tasks.setdefault(task, {})

        return task._output

    async def execute(self, **kwargs):
        try:
            # extract data using consumer
            res = await self.handle_consumer(self._input, **kwargs)
            # always sends a END to _output channel
            if isinstance(self._output, abc.Sequence):
                for ch in self._output:
                    await ch.put(END)
            else:
                await self._output.put(END)
        except Exception as e:
            raise e
        finally:
            self.clean()
        return res

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        async for data in consumer:
            await self.handle_input(data)
        await self.handle_input(END)

    async def handle_input(self, data, **kwargs):
        return NotImplemented

    def __ror__(self, chs) -> TaskOutput:
        """
        ch | task               -> task(ch)
        [ch1, ch2, ch3] | task  -> task(ch1, ch2, ch3)
        """
        if not isinstance(chs, abc.Sequence):
            chs = [chs]
        assert all(isinstance(ch, Channel) for ch in chs)
        return self(*chs)

    def __rrshift__(self, chs):
        """
        ch >> task              -> task(ch)
        [ch1, ch2, ch3] >> task -> [task(ch1), task(ch2), task(ch3)]
        """
        if isinstance(chs, abc.Sequence):
            assert all(isinstance(ch, Channel) for ch in chs)
            output_chs = [self(ch) for ch in chs]
            if isinstance(chs, tuple):
                output_chs = tuple(output_chs)
            return output_chs
        else:
            assert isinstance(chs, Channel)
            return self(chs)

    def __lshift__(self, chs):
        """
        task << ch
        task << [ch1, ch2, ch3]
        """
        return chs >> self


class Task(BaseTask):
    DEFAULT_CONFIG = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 1. load global default config
        self.config = TaskConfig()
        # 2. load task class's default config
        self.config.update(self.DEFAULT_CONFIG)
        # 3. load config specified by kwargs
        self.config.update(kwargs)
        # task specific
        self.cache = None
        self.skip_fn: Optional[Callable] = None
        self.workdir = None
        self.task_key = None
        # run specific
        self._input_key = None
        self._errors = []

    def __getattr__(self, item):
        return getattr(self.config, item)

    def skip(self, skip_fn: Callable):
        assert callable(skip_fn)
        self.skip_fn = skip_fn

    def clean(self):
        if self.cache:
            self.cache.persist()

    @property
    def hash(self):
        # get the hash of this task defined by the real source code of self.run
        fn = self.run
        # for wrapped func, use __source_func__ as a link
        while hasattr(fn, '__source_func__'):
            fn = fn.__source_func__
        code = fn.__code__.co_code
        annotations = getattr(fn, '__annotations__', None)

        return tokenize(code, annotations)

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        # 4. update user defined config
        cls_name = new.__class__.__name__
        if hasattr(config, cls_name) and isinstance(config[cls_name], dict):
            new.config.update(config[cls_name])
        task_key = f"{new.__class__.__name__}-{new.hash}"
        task_workdir = (Path(new.config.workdir) / Path(task_key)).expanduser().resolve()
        task_workdir.mkdir(parents=True, exist_ok=True)

        new.workdir = task_workdir
        new.task_key = str(task_workdir)
        new.cache = LocalCache(task_key=new.task_key)
        return new

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        scheduler = kwargs.get('scheduler')
        scheduled = False

        async for data in consumer:
            # handle _input _input_args
            # Must convert to tuple since it's untupled by consumer
            _data = (data,) if self._input_len == 1 else data
            run_args = self.create_run_args(_data)
            # should must cost much time
            if scheduler:
                # create a clean task for ease of serialization
                new_task = self.copy_clean()
                scheduler.schedule(self, self.enqueue_output, new_task, run_args, raw_data=data)
                scheduled = True
            else:
                res = await self.handle_input(run_args, raw_data=data)
                await self.enqueue_output(res)
        if scheduler and scheduled:
            wait_q = scheduler.get_wait_q(self)
            await wait_q.join()

    async def enqueue_output(self, res):
        await self._output.put(res)

    def create_run_args(self, data: Data) -> BoundArguments:
        # 1. build BoundArgument
        len_args = len(self._input_args)
        args = data[:len_args]
        kwargs = {}
        for i, k in enumerate(self._input_kwargs.keys()):
            kwargs[k] = data[len_args + i]
        run_args = inspect.signature(self.run).bind(*args, **kwargs)
        run_args.apply_defaults()
        return run_args

    async def run_job(self, run_args: BoundArguments, **kwargs):
        # used for scheduler call
        return await self.handle_input(run_args, **kwargs)

    async def handle_input(self, run_args: BoundArguments, **kwargs):
        run_args = await self.check_run_args(run_args)
        self._input_key = input_key = await self.cache.hash(run_args)
        # try to use cache
        if await self.cache.contains(input_key):
            res = self.cache.get(input_key)
        else:
            # handle retry
            exception = None
            run_args = await self.pre_handle_input(run_args)
            for i in range(self.config.retry + 1):
                try:
                    # skip if possible
                    if await self.need_skip(run_args):
                        res = kwargs['raw_data']
                    else:
                        res = await get_executor().run(self.run, *run_args.args, *run_args.kwargs)
                except Exception as e:
                    exception = e
                    if not hasattr(self, '_errors'):
                        self._errors = []
                    self._errors.append(e)
                else:
                    self.cache.put(input_key, res)
                    break
            else:
                raise exception
        self._input_key = None
        return res

    async def check_run_args(self, run_args: BoundArguments) -> BoundArguments:
        # 2. do some type conversion in case with type annotation
        run_params = dict(inspect.signature(self.run).parameters)
        arguments = run_args.arguments
        for arg, param in run_params.items():
            ano_type = param.annotation
            if ano_type is not Parameter.empty and isinstance(ano_type, type):
                value = arguments[arg]
                if not isinstance(value, ano_type):
                    try:
                        arguments[arg] = ano_type(value)
                    except Exception as e:
                        raise TypeError(f"The input argument `{arg}` has annotation `{ano_type}`, but "
                                        f"the input value `{value}` can not be converted.")
            # make sure each File's  checksum being computed
            # only check the first level
            value = arguments[arg]
            values = [value] if not isinstance(value, (list, tuple, set)) else value
            for f in [v for v in values if isinstance(v, File)]:
                if not f.initialized:
                    new_hash = await get_executor().run(f.calculate_hash)
                    f.hash = new_hash

        return run_args

    async def pre_handle_input(self, data: BoundArguments) -> BoundArguments:
        return data

    async def need_skip(self, bound_args: BoundArguments) -> bool:
        if self.skip_fn:
            if inspect.iscoroutine(self.skip_fn):
                return await self.skip_fn(*bound_args.args, **bound_args.kwargs)
            else:
                return self.skip_fn(*bound_args.args, **bound_args.kwargs)
        else:
            return False

    def run(self, *args, **kwargs):
        """
        This function should be stateless
        """
        raise NotImplementedError


class ShellTask(Task, metaclass=copy_sig_meta(('command', 'run'))):
    SCRIPT_CMD = "SCRIPT_CMD"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # task specific
        self.env = None
        # run specific
        self._cmd = None
        self._cmd_output = None
        self._stdin = ''

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        new.env = Env(workdir=new.workdir,
                      modules=new.config.modules,
                      conda_env=new.config.conda_env,
                      image=new.config.image,
                      runtime=new.config.runtime)
        return new

    def command(self, *args, **kwargs) -> str:
        raise NotImplementedError

    @property
    def running_path(self) -> Path:
        assert self.task_key is not None and self._input_key is not None
        return Path(self.task_key) / Path(self._input_key)

    async def pre_handle_input(self, input_args: BoundArguments) -> BoundArguments:
        # run user defined function and get the true bash commands
        with pyflow():
            self._cmd_output = self.command(*input_args.args, **input_args.kwargs)
            self._cmd = pyflow.get(self.SCRIPT_CMD)
            assert isinstance(self._cmd, Script)
            if self._cmd is None:
                raise ValueError("ShellTask must be registered with a shell script_cmd "
                                 "by calling `_(CMD) or Bash(CMD)` inside the function.")
        self._stdin = ''
        # check if there are _stdin
        for arg in list(input_args.args) + list(input_args.kwargs.values()):
            if isinstance(arg, Stdin):
                if self._stdin:
                    raise RuntimeError(f"Found two stdin inputs.")
                self._stdin = arg

        return input_args

    def run(self, *args, **kwargs):
        """
        This function should be stateless
        """

        with change_cwd(self.running_path) as path:
            # run bash
            env = os.environ.copy()
            # do some cleaning
            self.env.init()

            # prepare environment, stdout and stderr are merged
            try:
                prepare_env_cmd, script = self.env.prepare_cmd("")
                stdout_file = (self.running_path / Path(f"{script}.stdout")).open('w+')
                with subprocess.Popen(prepare_env_cmd,
                                      stdout=stdout_file,
                                      stderr=subprocess.STDOUT, env=env, shell=True) as p:
                    p.wait()

                stdout_file.seek(0)
                stderr = stdout_file.read(1000)
                stdout_file.close()
            except Exception as e:
                print(stdout_file)
                raise e
            if p.returncode:
                print(stdout_file)
                raise ValueError(f"Prepare environment for task {self} with error: {stderr} check {stdout_file}")

            # run in environment, stdout and stderr are separated, stdout of other commands are redirected
            run_env_cmd, script = self.env.run_cmd(str(self._cmd))
            stdout_file = (self.running_path / Path(f"{script}.stdout")).open('w')
            stderr_file = (self.running_path / Path(f"{script}.stderr")).open('w+')
            with subprocess.Popen(f"{self._stdin} {run_env_cmd}",
                                  stdout=stdout_file,
                                  stderr=stderr_file, env=env, shell=True) as p:
                p.wait()

            stderr_file.seek(0)
            stderr = stderr_file.read(1000)
            stdout_file.close()
            stderr_file.close()
            if p.returncode:
                raise ValueError(f"Run in environment for task {self} with error: {stderr}")

            # return stdout or globed new files
            output = self._cmd_output

            # 1. return stdout simulated by file
            if output is None:
                stdout_path = (self.running_path / Path(f"{script}.stdout")).resolve()
                stdout = Stdout(stdout_path)
                stdout.initialize_hash()
                return stdout

            # 2. return globed files
            # 1. convert to list
            single = False
            if not isinstance(output, (list, tuple)):
                output = [output]
                single = True
            # 2. convert tuple to list
            if isinstance(output, tuple):
                output = list(output)

            # 3. loop through element in the first level

            def check_item(item):
                if type(item) is str:
                    files = [File(p.resolve()) for p in Path().glob(item) if not p.name.startswith('.')]
                    for f in files:
                        f.initialize_hash()
                    return files[0] if len(files) == 1 else tuple(files)
                # initialize File
                elif isinstance(item, File):
                    item.initialize_hash()

                elif isinstance(item, dict):
                    for k, v in item.items():
                        item[k] = check_item(v)
                return item

            for i, item in enumerate(output):
                # convert str to File, initialize File
                output[i] = check_item(item)

            return output if not single else output[0]


class Script(object):
    def __init__(self, cmd: str):
        assert isinstance(cmd, str)
        self.cmd = cmd
        pyflow[ShellTask.SCRIPT_CMD] = self

    def __str__(self):
        return f"{self.cmd}"


class ShellScript(Script):
    pass


class PythonScript(Script):
    def __init__(self, cmd: str):
        super().__init__(cmd)
        raise ValueError("You should write python syntax right in here.")


class Rscript(Script):
    SCRIPT_FILE = ".__Rscript.R"

    def __str__(self):
        Path(self.SCRIPT_FILE).write_text(self.cmd)
        return f"Rscript {self.SCRIPT_FILE};"


# decorator to make Task
task = class_deco(Task, 'run')
shell = class_deco(ShellTask, 'command')
# function to register command
Shell = ShellScript
Python = PythonScript
Rscript = Rscript
