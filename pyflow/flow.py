import asyncio
from typing import Callable
from collections import OrderedDict

from .store import get_flow_stack, get_up_flow, get_top_flow
from .channel import Channel, ChannelDict, END
from .task import BaseTask, default_execute, OUTPUT, initialize_inputs


class Flow(object):
    def __init__(self, name=""):
        self.name = name
        self.inputs: ChannelDict = None
        self.output: OUTPUT = None
        self.tasks = OrderedDict()
        self.task_futures = []
        self.execute: Callable = default_execute

    def __repr__(self):
        return self.__class__.__name__ + "-" + str(hash(self))

    def __enter__(self):
        get_flow_stack().append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        get_flow_stack().pop(-1)

    def __call__(self, *args, **kwargs) -> Channel:
        flow = self.copy()
        # set up flows environments
        with flow:
            # set inputs within Task.__call
            self.inputs = ChannelDict(initialize_inputs(self, *args, **kwargs))
            flow.output = flow.run(*args, **kwargs)
            if not isinstance(flow.output, Channel):
                raise ValueError("Flow's run must return a Channel")
            top_flow = get_top_flow()

        async def execute():
            for task, task_info in flow.tasks.items():
                if isinstance(task, BaseTask):
                    task.future = asyncio.ensure_future(task.execute())
                    top_flow.task_futures.append(task.future)
                    loop = asyncio.get_running_loop()
                    if not hasattr(loop, 'task_futures'):
                        loop.task_futures = []
                    loop.task_futures.append((task, task.future))
                elif isinstance(task, Flow):
                    await task.execute()

        flow.execute = execute
        up_flow = get_up_flow()
        if up_flow:
            assert flow not in up_flow.tasks
            up_flow.tasks.setdefault(flow, {})
        if not isinstance(flow.output, Channel):
            raise ValueError("The output of flows must be a single Channel instead of list/tuple of Channel")

        flow.output.flows.append(flow)
        return flow.output

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError

    def copy(self):
        from copy import deepcopy
        return deepcopy(self)

    def __str__(self):
        return type(self).__name__


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


