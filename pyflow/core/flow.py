import asyncio
from collections import defaultdict
from typing import Union

from graphviz import Digraph

from pyflow.context import pyflow
from pyflow.utility.utils import class_deco, TaskOutput
from .base import FlowComponent
from .channel import Channel
from .scheduler import Scheduler


class Flow(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tasks = defaultdict(dict)
        self._graph = None

    def __enter__(self):
        pyflow.flow_stack.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pyflow.flow_stack.pop(-1)

    def __call__(self, *args, **kwargs) -> Union[TaskOutput, 'Flow']:
        flow = self.copy_new()
        # set up flows environments
        with flow:
            flow._output = flow.run(*args, **kwargs)
            # in case the _output is None/Nothing, create a END Channel
            if flow._output is None:
                flow._output = Channel.end()

        if pyflow.up_flow:
            pyflow.up_flow.tasks.setdefault(flow, {})
            return flow._output
        # in the top level flow, just return the flow itself
        else:
            return flow

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError

    async def execute(self, **kwargs):
        task_futures = []
        for task, task_info in self.tasks.items():
            future = task_info['future'] = asyncio.ensure_future(task.execute(**kwargs))
            task_futures.append(future)

            # used fo debugging
            loop = asyncio.get_running_loop()
            if not hasattr(loop, '_task_futures'):
                loop._task_futures = []
            loop._task_futures.append((task, future))

        res_list = await asyncio.gather(*task_futures)

        return res_list

    @property
    def graph(self):
        if not getattr(self, '_graph', None):
            self._graph = Digraph()
        return self._graph


class FlowRunner(object):
    def __init__(self, flow):
        self.src_flow = flow
        self.flow = None
        self.scheduler = Scheduler()

    def run(self, *args, **kwargs):
        self.flow = self.src_flow(*args, **kwargs)

        return self, self.flow

    def execute(self):

        async def _execute():
            loop = asyncio.get_running_loop()
            loop._scheduler = self.scheduler
            asyncio.ensure_future(self.scheduler.execute())
            exec_res = await self.flow.execute(scheduler=self.scheduler)
            self.scheduler.check_error()
            return exec_res

        assert self.flow is not None, "Please call run() method before running the flow."
        try:
            res = asyncio.run(_execute())
        except Exception as e:
            raise e
        return res


flow = class_deco(Flow, 'run')
