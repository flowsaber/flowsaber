import asyncio
import inspect
from collections import abc
from inspect import Parameter, BoundArguments
from pathlib import Path
from typing import Callable, Sequence, Optional, TYPE_CHECKING, List

from dask.base import tokenize

import flowsaber
from flowsaber.core.base import Component, aenter_context
from flowsaber.core.channel import Channel, Consumer, Output, ConstantChannel
from flowsaber.core.engine.task_runner import TaskRunner
from flowsaber.core.utility.state import State, Done, Success, Failure
from flowsaber.core.utility.target import File, END, Data
from flowsaber.core.utils import class_deco
from flowsaber.server.database.models import EdgeInput, TaskInput
from flowsaber.utility.utils import change_cwd

if TYPE_CHECKING:
    from flowsaber.core.engine.scheduler import TaskScheduler


class RunDataTypeError(TypeError):
    pass


class RunDataFileNotFoundError(RuntimeError):
    pass


class BaseTask(Component):
    """Base class of all Tasks, internally, BaseTask iteratively fetch items emitted by Channel inputs asynchronously.
    And then push the processed result of each item into the _output channels. All items are handled in sequence.
    """
    default_config = {
        'workdir': 'work',
    }

    def __init__(self, num_out: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.num_out = num_out

    @property
    def config_name(self) -> str:
        return "task_config"

    def initialize_context(self):
        """Initialize some attributes of self.config into self.context
        """
        super().initialize_context()
        # only expose the info of the outermost flow
        self.context.update({
            'task_id': self.config_dict['id'],
            'task_name': self.config_dict['name'],
            'task_full_name': self.config_dict['full_name'],
            'task_labels': self.config_dict['labels'],
        })

    def call_build(self, *args, **kwargs) -> Output:
        with flowsaber.context(self.context):
            self.initialize_input(*args, **kwargs)
            self.initialize_output()
        # enter task context
        # register to top flow for serializing
        top_flow = flowsaber.context.top_flow
        top_flow.tasks.append(self)
        # register to up flow for execution
        up_flow = flowsaber.context.up_flow
        up_flow.components.append(self)

        return self.output

    def initialize_input(self, *args, **kwargs):
        """Wrap all _input channels into a consumer object for simultaneous data ferching,

        Parameters
        ----------
        args
        kwargs
        """
        super().initialize_input(*args, **kwargs)
        channels = list(args) + list(kwargs.values())
        self.input = Consumer.from_channels(*channels, task=self)
        if all(isinstance(q.ch, ConstantChannel) for q in self.input.queues):
            flowsaber.context.logger.warn(f"All inputs of {self} are ConstantChannel")
        # register edges into the up-most flow
        for input_q in self.input.queues:
            edge = Edge(channel=input_q.ch, task=self)
            try:
                flowsaber.context.top_flow.edges.append(edge)
            except Exception as e:
                raise e

    def initialize_output(self):
        """Create _output channels according to self.num_output
        """
        self.output = tuple(Channel(task=self) for i in range(self.num_out))
        if self.num_out == 1:
            self.output = self.output[0]

    async def start_execute(self, **kwargs):
        await super().start_execute(**kwargs)
        await self.handle_consumer(self.input, **kwargs)
        # always sends a END to _output channel
        end_signal = END if self.num_out == 1 else [END] * self.num_out
        await self.enqueue_res(end_signal)

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        """Iteratively fetch data from consumer and then call processing function

        Parameters
        ----------
        consumer
        kwargs
        """
        async for data in consumer:
            res = await self.handle_input(data)
            if res is END:
                break
        await self.handle_input(END)

    async def handle_input(self, data, *args, **kwargs):
        """Do nothing, send the _input data directly to _output channel
        Parameters
        ----------
        data
        args
        kwargs
        """
        if data is not END:
            await self.enqueue_res(data)

    async def enqueue_res(self, data, index=None):
        """Enqueue processed data into _output channels.
        Parameters
        ----------
        data
        index
        """
        # enqueue data into the _output channel
        if self.num_out != 1 and isinstance(self.output, Sequence):
            try:
                if index is None:
                    for ch, _res in zip(self.output, data):
                        await ch.put(_res)
                else:
                    await self.output[index].put(data)
            except TypeError as e:
                raise RuntimeError(f"The _output: {data} can't be split into {self.num_out} channels."
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

    def serialize(self) -> TaskInput:
        # TODO can not fetch source code of type(self), if it's due to makefun ?
        config = self.config
        output = self.output
        if self.num_out == 1:
            output = [self.output]
        return TaskInput(
            id=config.id,
            flow_id=self.context['flow_id'],
            name=config.name,
            full_name=config.full_name,
            labels=config.labels,
            output=[ch.serialize() for ch in output],
            docstring=type(self).__doc__ or "",
            context=self.context
        )


class RunTask(BaseTask):
    """RunTask is subclass of BaseTask, representing tasks with run method exposed to users to implement specific
    item processing logics.
    Compared to BaseTask:
    1. Runs of multiple inputs will be executed in parallel.
    2. Runs will be executed in the main loop.
    """
    FUNC_PAIRS = [('run', '__call__', True)]
    default_config = {
        'cache_type': 'local',
        'executor_type': 'dask',
        'timeout': 0,
        'retry': 0,
        'retry_delay': 5,
        'drop_error': False,
        # resources
        'fork': 1,
        'cpu': 1,
        'gpu': 0,
        'memory': 0.2,
        'time': 1,
        'io': 1,
        'resources_limit': {
            'fork': 7,
        },
    }

    def initialize_context(self):
        """Expose cache_type and executor_type into self.context
        """
        super().initialize_context()
        self.context.update({
            'cache_type': self.config_dict['cache_type'],
            'executor_type': self.config_dict['executor_type']
        })

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        """Run processing functions in parallel by submitting jobs into schedulers that
        return awaitable Future-like objects.
        Parameters
        ----------
        consumer
        kwargs

        Returns
        -------

        """
        # get the custom scheduler and pop it, do not pass into task runner
        scheduler: 'TaskScheduler' = kwargs.get("scheduler", None)
        kwargs.pop('scheduler', None)

        futures = []
        async for data in consumer:
            run_data = (data,) if self.input.single else data
            # split run data and dependent data
            run_data = self.create_run_data(run_data)
            job_coro = self.handle_run_data(run_data, **kwargs)
            # add_task to scheduler
            if scheduler:
                fut = scheduler.create_task(job_coro)
            else:
                fut = asyncio.create_task(job_coro)
            futures.append(fut)
        # asyncio.wait can not accept empty list
        if not futures:
            return []

        # wait for the first exception and cancel all functions
        done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)
        for fut in pending:
            # TODO wait for truely cancelled
            fut.cancel()

        res_futures = list(done) + list(pending)
        self.check_future_exceptions(res_futures)

        return res_futures

    def create_run_data(self, data: Data) -> BoundArguments:
        """Wrap consumer fetched data tuple into a BoundArgument paired with self.run's signature.
        Parameters
        ----------
        data

        Returns
        -------

        """
        # 1. build BoundArgument
        len_args = len(self._input_args)
        args = data[:len_args]
        kwargs = {
            k: data[len_args + i] for i, k in enumerate(self._input_kwargs.keys())
        }

        run_sig = inspect.signature(self.run)
        try:
            run_data = run_sig.bind(*args, **kwargs)
        except TypeError as exc:
            raise ValueError(f"The input data: {data} can not be passed "
                             f"to {self.run} with signature of {run_sig}") from exc
        run_data.apply_defaults()
        return run_data

    @aenter_context
    async def handle_run_data(self, data: BoundArguments, **kwargs):
        """This coroutine will be executed in parallel, thus need to re-enter self.context.
        Parameters
        ----------
        data
        kwargs
        """
        data: BoundArguments = await self.check_run_data(data, **kwargs)
        res = await self.call_run(data, **kwargs)
        await self.handle_res(res)

    async def check_run_data(self, data: BoundArguments, **kwargs):
        """Match types of _input datas into self.run's annotations by type conversion. Check file integrity.
        Parameters
        ----------
        data
        kwargs

        Returns
        -------

        """
        run_params = dict(inspect.signature(self.run).parameters)
        arguments = data.arguments
        for arg, param in run_params.items():
            ano_type = param.annotation
            if ano_type is not Parameter.empty and isinstance(ano_type, type):
                value = arguments[arg]
                # 1. do some type conversion in case with type annotation
                is_default = param.default is not inspect.Signature.empty and value == param.default
                if not is_default and not isinstance(value, ano_type):
                    try:
                        # for File, Folder type, their path should be resolve in flow_workdir
                        with change_cwd(self.context.get('flow_workdir', '')) as p:
                            arguments[arg] = ano_type(value)
                    except Exception as e:
                        raise RunDataTypeError(f"The input argument `{arg}` has annotation `{ano_type}`, "
                                               f"but the input value `{value}` can not be converted.") from e
                # 2. if has File annotation, make sure it exists
                if ano_type is File and not arguments[arg].is_file():
                    raise RunDataFileNotFoundError(f"The argument {arg} has a File annotation, "
                                                   f"but the file {value} does not exists.")
            # 3. make sure each File's  checksum being computed, only check the first level
            from copy import deepcopy
            # if this function yield in await, the object may already being modifed. we make a deep copy
            value = deepcopy(arguments[arg])
            arguments[arg] = value
            values = [value] if not isinstance(value, (list, tuple, set)) else value
            for f in [v for v in values if isinstance(v, File)]:
                if not f.initialized:
                    new_hash = await flowsaber.context.executor.run(f.calculate_hash)
                    f.hash = new_hash
        return data

    async def call_run(self, data: BoundArguments, **kwargs):
        """Create a fresh task object and call it's run method for real data processing.
        """
        from copy import copy
        clean_task = copy(self)
        res = clean_task.run(*data.args, **data.kwargs)
        return res

    async def handle_res(self, res):
        await self.enqueue_res(res)

    def run(self, *args, **kwargs):
        """The method users need to implement for processing the data emited by _input channels.
        Parameters
        ----------
        args
        kwargs
        """
        raise NotImplementedError("Please implement this method.")


class Task(RunTask):
    """Task is subclass of RunTask:
    1. Each Task will have a unique task_key/task_workdir
    2. Each _input's run will have a unique run_key/task_workdir.
    3. Task's run will be executed in executor and handled by a task runner.
    4. Within the task runner, task will pass through a state machine, callbacks can be registered to each state changes.
    """

    default_config = {
        'run_workdir': "",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.skip_fn: Optional[Callable] = None

    @property
    def task_hash(self) -> str:
        """get the task_hash of this task defined by the real source code of self.run

        Returns
        -------

        """
        fn = self.run
        # for wrapped func, use __source_func__ as a link
        while hasattr(fn, '__source_func__'):
            fn = fn.__source_func__
        code = fn.__code__.co_code
        annotations = getattr(fn, '__annotations__', None)

        return tokenize(code, annotations)

    @property
    def task_key(self) -> str:
        # TODO use which info as task key?
        # Task are supposed to be statefull, same input with same output
        return f"{type(self).__name__}-{self.task_hash}"

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
        """Task's workdir is resolved in a bottom up way.
        In this hierarchical way, users can change flowrun.context['flow_workdir'] by setting up the flowrun's
        initial context, thus make the flow's workdir configurable.

        1: if the task_workdir is already absolute, then use it
        2: if parent_flow_workdir/task_workdir is already absolute, then use it
        3: otherwise use top_flow_workdir/parent_flow_workdir/task_workdir as the workdir

        Returns
        -------

        """
        workdir = Path(self.context['task_config']['workdir'], self.context['task_key'])
        if workdir.is_absolute():
            return workdir
        workdir = Path(self.context['up_flow_config']['workdir'], workdir)
        if workdir.is_absolute():
            return workdir
        workdir = Path(self.context['flow_workdir'], workdir)
        assert workdir.is_absolute()
        return workdir

    @property
    def run_workdir(self) -> Path:
        return Path(self.task_workdir, self.context['run_key'])

    def run_hash_source(self, run_data: BoundArguments, **kwargs) -> dict:
        return {
            'data': tuple(run_data.arguments.values())
        }

    @property
    def run_lock_source(self) -> List[str]:
        return [self.context.get('run_workdir')]

    async def call_run(self, data: BoundArguments, **kwargs) -> State:
        """Call self.run within the control of a asyncio.Lock identified by run_workdir
        Parameters
        ----------
        data
        kwargs

        Returns
        -------

        """
        from copy import copy
        # get input hash, we do not count for parameter names, we only care about orders
        # must use tuple, data.arguments.values() return an object instead of a container
        cache_type = self.context.get('cache_type', None)
        if cache_type:
            run_key: str = flowsaber.context.cache.hash(**self.run_hash_source(data, **kwargs))
        else:
            run_key: str = flowsaber.context.random_id
        assert run_key

        # create a fresh new task
        task = copy(self)
        task.context['run_key'] = run_key  # task.run_workdir need this
        context_update = {
            'run_key': run_key,
            'run_workdir': str(task.run_workdir)
        }
        # safe to update, flowsaber.context belongs to this run since we call handle_run_data
        task.context.update(context_update)
        flowsaber.context.update(context_update)
        # must lock _input key to avoid collision in cache and files in _running path
        async with flowsaber.context.lock(task.run_lock_source):
            task_runner = TaskRunner(
                task=task,
                inputs=data,
                server_address=self.context.get('server_address', None)
            )
            state = await flowsaber.context.executor.run(task_runner.run, **kwargs)
        return state

    async def handle_res(self, res):
        """Only push Success state result into _output channels. Some state may be skipped in case of
        Exceptions occurred within the task runner and thus return a Drop(Failure) state as a signal.
        Parameters
        ----------
        res
        """
        assert isinstance(res, Done), f"The result is {res}, should be a state of instance of Done"
        if isinstance(res, Success):
            await self.enqueue_res(res.result)
        elif isinstance(res, Failure):
            raise res.result
        # for Drop, just ignore it

    def need_skip(self, data: BoundArguments) -> bool:
        """ Check if the _input can be directly passed into _output channels by predicate of user specified self.skip_fn
        Parameters
        ----------
        data

        Returns
        -------

        """
        if self.skip_fn:
            return self.skip_fn(*data.args, **data.kwargs)
        else:
            return False

    def skip(self, skip_fn: Callable):
        """A decorator/function exposed for users to specify skip function.

        Parameters
        ----------
        skip_fn
        """
        assert callable(skip_fn)
        self.skip_fn = skip_fn

    def clean(self):
        """Functions called after the execution of task. For example, Cache need to persist cached data.
        """
        pass


class Edge(object):
    """A edge represents a dependency between a channel and a task. the Task consumes data emited by the channel.
    """

    def __init__(self, channel: Channel, task: BaseTask):
        self.channel: Channel = channel
        self.task: BaseTask = task

    def serialize(self) -> EdgeInput:
        return EdgeInput(
            channel_id=self.channel.id,
            task_id=self.task.config_dict['id']
        )


class GetContext(RunTask):
    """Emit context endlessly, this is aimed for building dependency between flow and input context/config.
    it's optional, users can also change parameters for different flowruns by rebuilding the flow.
    However, to make it also work in remote execution, the flow is supposed to read parameters from output of this task.
    Also note that, since the flow upload to server are built flow, the structure of flow is fixed, python expressions
    like if, will not work again as to change the structure. With the help of this task, it can simulate some sort of
    dynamics.
    """

    def __init__(self, check_fn: Callable = None, **kwargs):
        super().__init__(**kwargs)
        self.check_fn = check_fn

    def run(self):
        import flowsaber
        context = flowsaber.context.to_dict()
        if self.check_fn:
            self.check_fn(context)
        return context


run = class_deco(RunTask, 'run')
task = class_deco(Task, 'run')
get_context = GetContext()
