import asyncio
from typing import Callable
from collections import OrderedDict

from .store import get_flow_stack, get_up_flow, get_top_flow
from .channel import Channel, ChannelList, END
from .task import BaseTask
from .utils import INPUT, OUTPUT


class Flow(object):
    def __init__(self, name="", **kwargs):
        self.name = name
        self.input_args = None
        self.input_kwargs = None
        self.input: INPUT = None
        self.output: OUTPUT = None
        self.top_flow = None
        self.tasks = OrderedDict()
        self.task_futures = []
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def __repr__(self):
        return self.__class__.__name__ + "-" + str(hash(self))

    def __enter__(self):
        get_flow_stack().append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        get_flow_stack().pop(-1)

    def initialize_input(self, *args, **kwargs) -> ChannelList:
        self.input_args = args
        self.input_kwargs = kwargs
        self.input = ChannelList(list(args) + list(kwargs.values()), name=str(self), task=self)
        return self.input

    def __call__(self, *args, **kwargs) -> Channel:
        from copy import deepcopy
        # must deepcopy, otherwise all flow will share self.task_futures and other container attrs
        flow = deepcopy(self)
        # set up flows environments
        with flow:
            # set input_ch within Task.__call
            flow.initialize_input(*args, **kwargs)
            flow.output = flow.run(*args, **kwargs)
            if flow.output is not None and not isinstance(flow.output, Channel):
                raise ValueError("Flow's run must return a Channel or Nothing/None(Which means a END Channel)")
            # in case the output is None/Nothing, create a END Channel
            if flow.output is None:
                flow.output = Channel.end()
            flow.top_flow = get_top_flow()

        up_flow = get_up_flow()
        if up_flow:
            assert flow not in up_flow.tasks
            up_flow.tasks.setdefault(flow, {})

        flow.output.flows.append(flow)
        return flow.output

    async def execute(self):
        for task, task_info in self.tasks.items():
            if isinstance(task, BaseTask):
                task.future = asyncio.ensure_future(task.execute())
                self.top_flow.task_futures.append(task.future)
                loop = asyncio.get_running_loop()
                # used fo debugging
                if not hasattr(loop, 'task_futures'):
                    loop.task_futures = []
                loop.task_futures.append((task, task.future))
            elif isinstance(task, Flow):
                await task.execute()

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError


class FlowRunner(object):
    def __init__(self, flow):
        self.flow = flow

    async def _run(self, *args):
        output = self.flow(*args)
        if not isinstance(output, Channel):
            raise ValueError("The return value of the outermost flows must be"
                             " a single Channel, instead of a list of Channel.")
        flow: Flow = output.flows[-1]
        await flow.execute()
        done, pending = await asyncio.wait(flow.task_futures, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            if task.exception() is not None:
                raise task.exception()
        return flow

    def run(self, *args):
        flow = asyncio.run(self._run(*args))

        results = []
        while not flow.output.empty():
            item = flow.output.get_nowait()
            if item is END:
                break
            results.append(item)
        return results