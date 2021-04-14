import asyncio
import inspect
import os
import subprocess
from collections import abc, defaultdict
from functools import partial
from inspect import Parameter
from pathlib import Path
from typing import Callable, Sequence, Optional

from dask.base import tokenize

from flowsaber.core.executor import Executor
from flowsaber.core.utils.cache import get_cache_cls, Cache
from flowsaber.core.utils.context import context, config as flow_config
from flowsaber.core.utils.env import Env, EnvCreator
from flowsaber.core.utils.target import File, Stdout, Stdin, END
from flowsaber.utility.logtool import get_logger
from flowsaber.utility.utils import change_cwd, class_deco, TaskOutput, Data, capture_local
from .base import FlowComponent, TaskConfig
from .channel import Channel, Consumer, ConstantChannel, Queue
from .runner.task_runner import get_task_runner_cls
from .scheduler import TaskScheduler
from .utils.state import *

logger = get_logger(__name__)


class BaseTask(FlowComponent):
    def __init__(self, num_out: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.num_out = num_out
        self._dependencies: Optional[dict] = None

    def __call__(self, *args, **kwargs) -> TaskOutput:
        task = self.copy_new(*args, **kwargs)
        up_flow = context.up_flow
        assert task not in up_flow._tasks
        up_flow._tasks.setdefault(task, {})

        return task._output

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        new.initialize_output()
        new.register_graph(new._input.queues)
        return new

    def initialize_input(self, *args, **kwargs) -> Consumer:
        super().initialize_input(*args, **kwargs)
        channels = list(args) + list(kwargs.values())
        self._input = Consumer.from_channels(channels, consumer=self, task=self)
        self._dependencies = {}
        return self._input

    def initialize_output(self) -> TaskOutput:
        self._output = tuple(Channel(task=self) for i in range(self.num_out))
        if len(self._output) == 1:
            self._output = self._output[0]
        return self._output

    def register_graph(self, qv: Sequence[Queue]):
        g = context.top_flow.graph
        kwargs = dict(
            style="filled",
            colorscheme="svg"
        )
        g.node(self.identity_name, **kwargs)
        for q in qv:
            src = q.ch.task or q.ch
            src_name = getattr(src, 'identity_name', None) or src.name
            shape = 'box' if isinstance(self, Task) else 'ellipse'
            if f"\t{src_name}" not in g.source:
                g.node(src_name, shape=shape, **kwargs)
            g.edge(src_name, self.identity_name)

    def add_dependency(self, name: str, channel: Channel):
        assert name not in self._dependencies
        self._dependencies[name] = channel
        self._input.add_channel(channel, consumer=self)
        self.register_graph([self._input.queues[-1]])

    async def execute(self, **kwargs):
        await super().execute(**kwargs)
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

    async def handle_input(self, data, *args, **kwargs):
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

    def __new__(cls, *args, **kwargs):
        # handle for pickle not support __getattr__
        obj = super().__new__(cls)
        # 1. load global default config
        obj.config = TaskConfig()
        # 2. load task class's default config
        obj.config.update(cls.DEFAULT_CONFIG)
        # 3. load config specified by kwargs
        obj.config.update(kwargs)
        return obj

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # task specific
        self.skip_fn: Optional[Callable] = None
        self.workdir = None
        # run specific
        self.run_info: Optional[dict] = None

    def __getattr__(self, item):
        return getattr(self.config, item)

    def copy_new(self, *args, **kwargs):
        new: Task = super().copy_new(*args, **kwargs)
        # 4. update user defined config
        cls_name = new.__class__.__name__
        # update if has self's class name
        if hasattr(flow_config, cls_name) and isinstance(flow_config[cls_name], dict):
            new.config.update(flow_config[cls_name])
        # update if has self's base classes
        for base in self.__class__.__mro__:
            if base in flow_config:
                new.config.update(flow_config[base])

        # set task_key and working directory
        # task key is the unique identifier of the task and task' working directory, cache, run_key_lock
        new.workdir = Path(new.config.workdir, new.task_key).expanduser().resolve()
        logger.debug(f"the working directory of {self} is {new.task_key}")
        # set cache
        return new

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        # make working directory
        self.workdir.mkdir(parents=True, exist_ok=True)
        scheduler: TaskScheduler = kwargs.get('scheduler')
        futs = []

        async for data in consumer:
            # handle _input _input_args
            # tuple if needed
            run_data = (data,) if self._input.single else data
            logger.debug(f"{self} get data {data} from consumer.")
            len_args = self._input_len
            # split run data and dependent data
            run_data, depend_data = run_data[:len_args], run_data[len_args:]
            run_args = self.create_run_args(run_data)
            depend_args = dict(zip(self._dependencies, depend_data))
            # initialize run_info
            state = Pending(context=depend_args)
            # submit to scheduler
            if scheduler:
                # create a clean task for ease of serialization
                job = scheduler.submit(self.handle_input(run_args, state=state))
                job.add_async_done_callback(self.handle_target_state)
                if self.config.fork <= 1:
                    await job
                futs.append(job)
            # or in current loop
            else:
                state = await self.handle_input(run_args, state=state)
                await self.handle_target_state(state)
        if scheduler and len(futs):
            done, pending = asyncio.wait(*futs, return_when=asyncio.ALL_COMPLETED)

    async def handle_target_state(self, state):
        assert isinstance(state, Done)
        if isinstance(state, Success):
            await self._output.put(state.result)

    def create_run_args(self, data: Data) -> BoundArguments:
        # 1. build BoundArgument
        len_args = len(self._input_args)
        args = data[:len_args]
        kwargs = {
            k: data[len_args + i] for i, k in enumerate(self._input_kwargs.keys())
        }

        run_args = inspect.signature(self.run).bind(*args, **kwargs)
        run_args.apply_defaults()
        return run_args

    def need_skip(self, bound_args: BoundArguments) -> bool:
        if self.skip_fn:
            return self.skip_fn(**bound_args.arguments)
        else:
            return False

    async def handle_input(self, run_args: BoundArguments, **kwargs) -> State:
        logger.debug(f"{self} start handle_input()")

        run_args = await self.check_run_args(run_args)
        input_hash = self.cache.hash(run_args, **self.input_hash_source)
        run_key = self.run_key(input_hash)
        logger.debug(f"{self} {run_args} {self.input_hash_source} {run_key}")

        # 1. set _running info and lock key
        # must lock input key to avoid collision in cache and files in _running path
        async with self.run_key_lock(run_key):
            clean_task = self.copy_clean()
            task_runner = get_task_runner_cls()(task=clean_task)
            state = kwargs['state']
            state.inputs = run_args
            state.context.update({
                'run_key': run_key
            })
            state = await self.executor.run(task_runner.run, state)
        logger.debug(f"{self} release lock:{run_key}")
        # 3. unset _running info
        return state

    async def check_run_args(self, run_args: BoundArguments) -> BoundArguments:
        run_params = dict(inspect.signature(self.run).parameters)
        arguments = run_args.arguments
        for arg, param in run_params.items():
            ano_type = param.annotation
            if ano_type is not Parameter.empty and isinstance(ano_type, type):
                value = arguments[arg]
                # 1. do some type conversion in case with type annotation
                if not isinstance(value, ano_type):
                    try:
                        arguments[arg] = ano_type(value)
                    except Exception as e:
                        raise TypeError(f"The input argument `{arg}` has annotation `{ano_type}`, but "
                                        f"the input value `{value}` can not be converted.")
                # 2. if has File annotation, make sure it exists
                if ano_type is File and not arguments[arg].is_file():
                    raise ValueError(f"The argument {arg} has a File annotation but "
                                     f"the file {arguments[arg]} does not exists.")
            # 3. make sure each File's  checksum being computed, only check the first level
            value = arguments[arg]
            values = [value] if not isinstance(value, (list, tuple, set)) else value
            for f in [v for v in values if isinstance(v, File)]:
                if not f.initialized:
                    new_hash = await self.executor.run(f.calculate_hash)
                    f.hash = new_hash
        return run_args

    def get_run_info(self, data: BoundArguments) -> dict:
        return {}

    def run(self, *args, **kwargs):
        """
        This function should be stateless
        """
        raise NotImplementedError

    @property
    def task_key(self):
        return f"{self.__class__.__name__}-{self.task_hash}"

    @property
    def task_hash(self):
        # get the task_hash of this task defined by the real source code of self.run
        fn = self.run
        # for wrapped func, use __source_func__ as a link
        while hasattr(fn, '__source_func__'):
            fn = fn.__source_func__
        code = fn.__code__.co_code
        annotations = getattr(fn, '__annotations__', None)

        return tokenize(code, annotations)

    @property
    def cache(self) -> Cache:
        create_cache = partial(get_cache_cls, self.config.cache_type, task=self)
        caches = context.setdefault('__caches__', defaultdict(create_cache))
        return caches[self.task_key]

    @property
    def executor(self) -> Executor:
        """Note: The executor can not use threads, otherwise functions relies on `os.getcwd()` may change at any time"""
        assert '__executor__' in context.__dict__, "Executor not in flowsaber.context"
        return context['__executor__']

    @property
    def input_hash_source(self) -> dict:
        return {}

    def run_key(self, input_hash):
        assert self.task_key is not None and Path(self.workdir).is_dir()
        return str(Path(self.workdir, input_hash))

    def run_key_lock(self, run_key: str):
        locks = context.setdefault('__run_key_locks__', defaultdict(asyncio.Lock))
        return locks[run_key]

    def skip(self, skip_fn: Callable):
        assert callable(skip_fn)
        self.skip_fn = skip_fn

    def clean(self):
        if hasattr(self, 'cache'):
            self.cache.persist()


class ShellTask(Task):
    FUNC_PAIRS = [('command', 'run')]
    SCRIPT_CMD = "SCRIPT_CMD"

    class Script(object):
        def __init__(self, cmd: str):
            assert isinstance(cmd, str)
            self.cmd = cmd
            context[ShellTask.SCRIPT_CMD] = self

        def __str__(self):
            return f"{self.cmd}"

    class ShellScript(Script):
        pass

    class PythonScript(Script):
        def __init__(self, cmd: str):
            super().__init__(cmd)
            raise NotImplementedError("Why not write python code in Task?")

    class Rscript(Script):
        SCRIPT_FILE = ".__Rscript.R"

        def __str__(self):
            Path(self.SCRIPT_FILE).write_text(self.cmd)
            return f"Rscript {self.SCRIPT_FILE};"

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        # handling env dependency
        config = new.config
        if config.module or config.conda or config.image:
            env_creator = EnvCreator(
                module=config.module,
                conda=config.conda,
                image=config.image
            )
            if env_creator not in context.env_tasks:
                context.env_tasks[env_creator] = EnvTask(env_creator=env_creator)()
            env_task_out_ch: Channel = context.env_tasks[env_creator]
            new.add_dependency(name='env', channel=env_task_out_ch)

        return new

    def get_run_info(self, data: BoundArguments) -> dict:
        # run user defined function and get the true bash commands
        run_info = super().get_run_info(data)
        # find the real shell commands
        with context(), capture_local() as local_vars:
            cmd_output = self.command(**data.arguments)
            cmd: str = context.get(self.SCRIPT_CMD, None)
        # two options: 1. use Shell('cmd') 2. use __doc__ = 'cmd'
        if cmd is None:
            cmd = self.command.__doc__
            if cmd is None:
                raise ValueError("ShellTask must be registered with a shell script_cmd "
                                 "by calling `_(CMD) or Bash(CMD)` inside the function or add the "
                                 "script_cmd as the command() method's documentation by setting __doc__ ")
            local_vars.update({'self': self})
            cmd = cmd.format(**local_vars)
        # check if there are _stdin
        stdin = ''
        for arg in list(data.args) + list(data.kwargs.values()):
            if isinstance(arg, Stdin):
                if stdin:
                    raise RuntimeError(f"Found two stdin inputs.")
                stdin = arg

        run_info.update({
            'cmd_output': cmd_output,
            'cmd': cmd,
            'stdin': stdin
        })
        return run_info

    def command(self, *args, **kwargs) -> str:
        raise NotImplementedError

    def run(self, *args, **kwargs):
        """
        This function should be stateless
        """
        run_info = self.run_info
        stdin = run_info['stdin']
        cmd = run_info['cmd']
        cmd_output = run_info['cmd_output']
        run_key = run_info['run_key']
        env: Env = run_info.get('env') or Env()
        running_path = Path(run_key)
        pubdirs = self.config.get_pubdirs()
        with change_cwd(running_path) as path:
            # 2. run in environment, stdout and stderr are separated, stdout of other commands are redirected
            run_env_cmd = env.gen_cmd(str(cmd))
            stdout_file = (running_path / Path(f".__run__.stdout")).open('w')
            stderr_file = (running_path / Path(f".__run__.stderr")).open('w+')
            with subprocess.Popen(f"{stdin} {run_env_cmd}",
                                  stdout=stdout_file,
                                  stderr=stderr_file,
                                  env=os.environ.copy(), shell=True) as p:
                p.wait()

            stderr_file.seek(0)
            stderr = stderr_file.read(1000)
            stdout_file.close()
            stderr_file.close()
            if p.returncode:
                raise ValueError(
                    f"Run in environment for task {self} with error: {stderr} in {running_path} task: {self}")
            # return stdout or globed new files
            output = cmd_output

            # 1. return stdout simulated by file
            if output is None:
                stdout_path = Path(stdout_file.name).resolve()
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
                    files = [File(p.resolve())
                             for p in Path().glob(item)
                             if not p.name.startswith('.') and p.is_file()]
                    for f in files:
                        check_item(f)

                    return files[0] if len(files) == 1 else tuple(files)
                # initialize File
                elif isinstance(item, File):
                    # initialize hash
                    item.initialize_hash()
                    # publish to pubdir
                    for pubdir in pubdirs:
                        pubdir.mkdir(parents=True, exist_ok=True)
                        pub_file = pubdir / Path(item.name)
                        if not pub_file.exists():
                            item.link_to(pub_file)

                elif isinstance(item, dict):
                    for k, v in item.items():
                        item[k] = check_item(v)
                return item

            for i, item in enumerate(output):
                # convert str to File, initialize File
                output[i] = check_item(item)

            return output if not single else output[0]


class EnvTask(ShellTask):
    def __init__(self, env_creator: EnvCreator = None, **kwargs):
        super().__init__(**kwargs)
        self.env_creator: EnvCreator = env_creator
        self.env: Optional[Env] = None

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        if new.env_creator is None:
            new.env_creator = EnvCreator(
                module=new.config.module,
                conda=new.config.conda,
                image=new.config.image
            )
        return new

    def initialize_output(self) -> TaskOutput:
        self._output = ConstantChannel(task=self)
        return self._output

    def command(self) -> Env:
        running_path = Path(self.run_info['run_key'])
        with change_cwd(running_path) as path:
            cmd, env = self.env_creator.gen_cmd_env()
        Shell(cmd)
        self.env = env
        return env

    @property
    def input_hash_source(self) -> dict:
        return {
            'env_hash': self.env_creator.hash
        }


# decorator to make Task
task = class_deco(Task, 'run')
shell = class_deco(ShellTask, 'command')
# function to register command
Shell = ShellTask.ShellScript
Python = ShellTask.PythonScript
Rscript = ShellTask.Rscript
