import asyncio
from collections import OrderedDict

from graphviz import Digraph

from .store import get_flow_stack
from .channel import Channel, END
from .task import FlowComponent
from .utils import INPUT, OUTPUT


class Flow(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tasks = OrderedDict()
        self.all_tasks = OrderedDict()
        self.task_futures = []
        self._graph = None

    def __enter__(self):
        get_flow_stack().append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        get_flow_stack().pop(-1)

    def __call__(self, *args, **kwargs) -> OUTPUT:
        flow = self.copy_new()
        flow.initialize_input(*args, **kwargs)
        # set up flows environments
        with flow:
            flow.output = flow.run(*args, **kwargs)
            if flow.output is not None and not isinstance(flow.output, Channel):
                raise ValueError("Flow's run must return a Channel or Nothing/None(Which means a END Channel)")
            # in case the output is None/Nothing, create a END Channel
            if flow.output is None:
                flow.output = Channel.end()

        if flow.up_flow:
            assert flow not in flow.up_flow.tasks
            flow.up_flow.tasks.setdefault(flow, {})

        flow.output.flows.append(flow)
        return flow.output

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError

    async def execute(self):
        for task, task_info in self.tasks.items():
            future = task_info['future'] = asyncio.ensure_future(task.execute())
            self.task_futures.append(future)

            # used fo debugging
            loop = asyncio.get_running_loop()
            if not hasattr(loop, 'task_futures'):
                loop.task_futures = []
            loop.task_futures.append((task, future))

        res_list = await asyncio.gather(*self.task_futures)
        for res, task_info in zip(res_list, self.tasks.values()):
            task_info['result'] = res_list

        return res_list

    @property
    def graph(self):
        if self._graph is None:
            self._graph = Digraph()
        return self._graph


class FlowRunner(object):
    def __init__(self, flow):
        self.flow = flow

    def run(self, *args, **kwargs):
        output = self.flow(*args, **kwargs)
        flow = output.flows[-1]

        asyncio.run(flow.execute())

        return flow
