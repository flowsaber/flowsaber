import asyncio
import inspect
import os
import subprocess
from abc import ABC
from collections import abc
from inspect import Parameter, BoundArguments
from pathlib import Path
from typing import Callable, Sequence

from dask.base import tokenize

import flowsaber
from flowsaber.core.base import Component
from flowsaber.core.channel import Channel, Consumer, ConstantChannel, Output
from flowsaber.core.utility.env import Env, EnvCreator
from flowsaber.core.utility.state import *
from flowsaber.core.utility.target import File, Stdout, Stdin, END, Data
from flowsaber.engine.task_runner import TaskRunner
from flowsaber.server.database import *
from flowsaber.utility.utils import change_cwd, capture_local


class BaseTask(Component):
    default_config = {
        'workdir': 'work'
    }

    def __init__(self, num_out: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.num_out = num_out

    @property
    def config_name(self) -> str:
        return "task_config"

    def __call__(self, *args, **kwargs) -> Output:
        # update context and store to task.context
        task = self.call_initialize(*args, **kwargs)
        # enter task context
        with flowsaber.context(task.context):
            task.initialize_input(*args, **kwargs)
            task.initialize_output()
        # register to top flow for serializing
        top_flow = flowsaber.context.top_flow
        top_flow.tasks.append(task)
        # register to up flow for execution
        up_flow = flowsaber.context.up_flow
        up_flow.compoments.append(task)

        return task.output

    def initialize_context(self):
        super().initialize_context()
        # only expose the info of the outermost flow
        self.context.update({
            'task_id': self.config_dict['id'],
            'task_name': self.config_dict['name'],
            'task_full_name': self.config_dict['full_name'],
            'task_labels': self.config_dict['labels']
        })

    def initialize_input(self, *args, **kwargs):
        super().initialize_input(*args, **kwargs)
        channels = list(args) + list(kwargs.values())
        self.input = Consumer.from_channels(channels, task=self)
        # register edges into the up-most flow
        for input_q in self.input.queues:
            edge = Edge(channel=input_q.ch, task=self)
            flowsaber.context.top_flow.edges.apend(edge)

    def initialize_output(self):
        self.output = tuple(Channel(task=self) for i in range(self.num_out))
        if self.num_out == 1:
            self.output = self.output[0]

    async def start_execute(self, **kwargs):
        await super(self).start_execute(**kwargs)
        res = await self.handle_consumer(self.input, **kwargs)
        # always sends a END to _output channel
        end_signal = END if self.num_out == 1 else [END] * self.num_out
        await self.enqueue_res(end_signal)

        return res

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        async for data in consumer:
            await self.handle_input(data)
        await self.handle_input(END)

    async def handle_input(self, data, *args, **kwargs):
        if data is not END:
            await self.enqueue_res(data)

    async def enqueue_res(self, data, index=None):
        # enqueue data into the output channel
        if self.num_out != 1 and isinstance(self.output, Sequence):
            try:
                if index is None:
                    for ch, _res in zip(self.output, data):
                        await ch.put(_res)
                else:
                    self.output[index].put(data)
            except TypeError as e:
                raise RuntimeError(f"The output: {data} can't be split into {self.num_out} channels."
                                   f"The error is {e}")
        else:
            await self.output.put(data)

    def __ror__(self, chs) -> Output:
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

    @classmethod
    def source_code(cls) -> str:
        import inspect
        return inspect.getsource(cls)

    @classmethod
    def input_signature(cls) -> dict:
        call_signature = {
            param: str(param_type)
            for param, param_type
            in inspect.signature(cls.__call__).parameters.items()
        }

        return call_signature

    def serialize(self) -> TaskInput:
        config = self.config
        outputs = self.output
        if self.num_out == 1:
            outputs = [self.output]
        return TaskInput(
            id=config.id,
            flow_id=self.context['flow_id'],
            name=config.name,
            labels=config.labels,
            input_signature=self.input_signature(),
            outputs=[ch.serialize() for ch in outputs],
            source_code=inspect.getsource(type(self))
        )


class RunTask(BaseTask):
    """RunTask represents tasks with run method.
    1. Runs of multiple inputs will be executed in parallel.
    2. Runs will be executed in the main loop.
    """
    FUNC_PAIRS = [('run', '__call__', True)]
    default_config = {
        'publish_dirs': [],
        'drop_error': False,
        'cache': 'local',
        'retry': 0,
        'fork': 7,
        'cpu': 1,
        'gpu': 0,
        'memory': 0.2,
        'time': 1,
        'io': 1,
        'executor_type': 'dask',
    }

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        scheduler: 'flowsaber.core.engine.TaskScheduler' = kwargs.get("scheduler")
        # pop it, do not pass into task runner
        kwargs.pop('scheduler')
        futures = []

        async for data in consumer:
            run_data = (data,) if self.input.single else data
            # split run data and dependent data
            run_data = self.create_run_data(run_data)
            job_coro = self.handle_run_data(run_data, **kwargs)
            # create_task to scheduler
            if scheduler:
                fut = scheduler.create_task(job_coro)
            else:
                fut = asyncio.create_task(job_coro)
            futures.append(fut)
        fut = asyncio.gather(*futures)
        return fut

    def create_run_data(self, data: Data) -> BoundArguments:
        # 1. build BoundArgument
        len_args = len(self.input_args)
        args = data[:len_args]
        kwargs = {
            k: data[len_args + i] for i, k in enumerate(self.input_kwargs.keys())
        }

        run_data = inspect.signature(self.run).bind(*args, **kwargs)
        run_data.apply_defaults()
        return run_data

    async def handle_run_data(self, data: BoundArguments, **kwargs):
        with flowsaber.context(self.context):
            data: BoundArguments = await self.check_run_data(data, **kwargs)
            res = await self.call_run(data, **kwargs)
            await self.handle_res(res)

    async def check_run_data(self, data: BoundArguments, **kwargs):
        run_params = dict(inspect.signature(self.run).parameters)
        arguments = data.arguments
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
                    new_hash = await flowsaber.context.executor.run(f.calculate_hash)
                    f.hash = new_hash
        return data

    async def call_run(self, data: BoundArguments, **kwargs):
        """
        RunTask.run should be run within the context of task.context
        """
        from copy import copy
        clean_task = copy(self)
        res = clean_task.run(**data.arguments)
        return res

    async def handle_res(self, res):
        await self.enqueue_res(res)

    def run(self, *args, **kwargs):
        raise NotImplementedError("Please implement this method.")


class Task(RunTask, ABC):
    """Task is subclass of RunTask:
    1. Each Task will have a unique task_key/task_workdir
    2. Each input's run will have a unique run_key/task_workdir.
    3. Task's run will be executed in executor and handled by a task runner.
    4. Within the task runner, task will pass through a state machine, callbacks can be registered to each state changes.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.skip_fn: Optional[Callable] = None

    @property
    def task_hash(self) -> str:
        # get the task_hash of this task defined by the real source code of self.run
        fn = self.run
        # for wrapped func, use __source_func__ as a link
        while hasattr(fn, '__source_func__'):
            fn = fn.__source_func__
        code = fn.__code__.co_code
        annotations = getattr(fn, '__annotations__', None)

        return tokenize(code, annotations)

    @property
    def task_key(self) -> str:
        return f"{str(self)}-{self.task_hash}"

    def initialize_context(self):
        super().initialize_context()
        task_key = self.task_key
        self.config_dict.update({
            'task_key': task_key,
        })
        self.context.update({
            'task_key': task_key,
        })

    @property
    def task_workdir(self) -> Path:
        workdir = Path(self.context['task_config']['workdir'], self.context['task_key'])
        if workdir.is_absolute():
            return workdir
        workdir = Path(self.context['flow_config']['workdir'], workdir)
        if workdir.is_absolute():
            return workdir
        workdir = Path(self.context['flow_workdir'], workdir)
        assert workdir.is_absolute()
        return workdir

    @property
    def run_workdir(self) -> Path:
        return Path(self.task_workdir, self.context['run_key'])

    async def call_run(self, data: BoundArguments, **kwargs) -> State:
        from copy import copy
        run_key = flowsaber.context.cache.hash(data, **self.input_hash_source)
        run_workdir = self.run_workdir
        task = copy(self)
        context_update = {
            'run_key': run_key,
            'run_workdir': run_workdir
        }
        task.context.update(context_update)
        flowsaber.context.update(context_update)
        state = Scheduled()
        # 1. set _running info and lock key
        # must lock input key to avoid collision in cache and files in _running path
        async with flowsaber.context.run_lock:
            task_runner = TaskRunner(task=task, inputs=data)
            state = await flowsaber.context.executor.run(task_runner.run, state, **kwargs)
        # 3. unset _running info
        return state

    async def handle_res(self, res):
        assert isinstance(res, Done)
        if isinstance(res, Success):
            await self.enqueue_res(res.result)

    def need_skip(self, bound_args: BoundArguments) -> bool:
        if self.skip_fn:
            return self.skip_fn(**bound_args.arguments)
        else:
            return False

    @property
    def input_hash_source(self) -> dict:
        return {}

    def skip(self, skip_fn: Callable):
        assert callable(skip_fn)
        self.skip_fn = skip_fn

    def clean(self):
        if hasattr(self, 'cache'):
            self.cache.persist()


class ShellTask(Task):

    def run(self, cmd: str, output=None, env: Env = None):
        env = env or Env()
        flow_workdir = self.context['flow_workdir']
        run_workdir = self.context['run_workdir']

        # resolve publish dirs
        publish_dirs = []
        for pub_dir in self.config_dict['publish_dirs']:
            pub_dir = Path(pub_dir).expanduser().resolve()
            if pub_dir.is_absolute():
                publish_dirs.append(pub_dir)
            else:
                publish_dirs.append(Path(flow_workdir, pub_dir).resolve())
        # start run
        with change_cwd(run_workdir) as path:
            # 2. run in environment, stdout and stderr are separated, stdout of other commands are redirected
            run_env_cmd = env.gen_cmd(str(cmd))
            stdout_f = run_workdir / Path(f".__run__.stdout")
            stderr_f = run_workdir / Path(f".__run__.stderr")
            with stdout_f.open('w') as stdout_file, stderr_f.open('w+') as stderr_file:
                with subprocess.Popen(f"{run_env_cmd}",
                                      stdout=stdout_file,
                                      stderr=stderr_file,
                                      env=os.environ.copy(), shell=True) as p:
                    p.wait()

                stderr_file.seek(0)
                stderr = stderr_file.read(1000)
                if p.returncode:
                    raise ValueError(
                        f"Run in environment for task {self} with error: {stderr} in {run_workdir} task: {self}")

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
                    # publish to publish dir
                    for pub_dir in publish_dirs:
                        pub_dir.mkdir(parents=True, exist_ok=True)
                        pub_file = pub_dir / Path(item.name)
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


class CommandTask(RunTask):
    FUNC_PAIRS = [('command', 'run')]

    def __init__(self, **kwargs):
        super().__init__(num_out=3, **kwargs)

    def command(self, *args, **kwargs) -> str:
        raise NotImplementedError("Please implement this function and return a bash script.")

    def run(self, *args, **kwargs):
        # TWO options: 1: use local var: CMD or use docstring
        with capture_local() as local_vars:
            cmd_output = self.command(*args, **kwargs)
        cmd = local_vars.get("CMD")
        if cmd is None:
            cmd = self.command.__doc__
            if cmd is None:
                raise ValueError("CommandTask must be registered with a shell script_cmd "
                                 "by calling `_(CMD) or Bash(CMD)` inside the function or add the "
                                 "script_cmd as the command() method's documentation by setting __doc__ ")
            local_vars.update({'self': self})
            cmd = cmd.format(**local_vars)
        # add cat stdin cmd
        stdins = [arg for arg in list(args) + list(kwargs.values()) if isinstance(arg, Stdin)]
        if len(stdins) > 1:
            raise RuntimeError(f"Found more than two stdin inputs: {stdins}")
        stdin = f"{stdins[0]} " if len(stdins) else ""
        cmd = f"{stdin}{cmd}"
        return cmd, cmd_output


class EnvTask(ShellTask):
    __ENV_TASKS__ = {}

    def __call__(self, *args, **kwargs):
        env_hash = self.env_creator.hash
        if env_hash not in self.__ENV_TASKS__:
            self.__ENV_TASKS__[env_hash] = super().__call__(*args, **kwargs)
        return self.__ENV_TASKS__[env_hash]

    def __init__(self, module: str = None, conda: str = None, image: str = None, **kwargs):
        super().__init__(**kwargs)
        self.env_creator: EnvCreator = EnvCreator(module, conda, image)
        self.env: Optional[Env] = None

    def initialize_output(self) -> Output:
        self.output = ConstantChannel(task=self)
        return self.output

    def run(self, cmd: str, output=None, env: Env = None):
        run_workdir = self.context['run_workdir']
        with change_cwd(run_workdir) as path:
            cmd, env = self.env_creator.gen_cmd_env()
        super().run(cmd=cmd)
        self.env = env
        return env

    @property
    def input_hash_source(self) -> dict:
        return {
            'env_hash': self.env_creator.hash
        }


class Edge(object):
    def __init__(self, channel: Channel, task: BaseTask):
        self.channel: Channel = channel
        self.task: BaseTask = task

    def serialize(self) -> EdgeInput:
        return EdgeInput(
            channel_id=self.channel.id,
            task_id=self.task.context['task_id']
        )
