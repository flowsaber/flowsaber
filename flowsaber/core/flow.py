import asyncio
from collections import defaultdict
from typing import Union

from graphviz import Digraph

from flowsaber.context import flowsaber, config
from flowsaber.utility.utils import class_deco, TaskOutput
from .base import FlowComponent
from .channel import Channel
from .scheduler import Scheduler, process


class Flow(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tasks = defaultdict(dict)
        self._graph = None

    def __enter__(self):
        flowsaber.flow_stack.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        flowsaber.flow_stack.pop(-1)

    def __call__(self, *args, **kwargs) -> Union[TaskOutput, 'Flow']:
        flow = self.copy_new()
        # set up flows environments
        with flow:
            flow._output = flow.run(*args, **kwargs)
            # in case the _output is None/Nothing, create a END Channel
            if flow._output is None:
                flow._output = Channel.end()

        if flowsaber.up_flow:
            flowsaber.up_flow.tasks.setdefault(flow, {})
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

    async def _execute(self):
        assert self.flow is not None, "Please call run() method before running the flow."
        # print config
        from rich import print
        print("Your config is: ", config.__dict__)

        # TODO carefully handle ctrl + C signal caused exception
        loop = asyncio.get_running_loop()
        loop._scheduler = self.scheduler
        asyncio.ensure_future(self.scheduler.execute())
        flow_exec_res = await self.flow.execute(scheduler=self.scheduler)
        await asyncio.sleep(0.5)
        process.stop()
        # stop executors
        for executor in flowsaber.get('__executors__', {}).values():
            executor.shutdown()

        return flow_exec_res

    def execute(self):
        try:
            res = asyncio.run(self._execute())
        except Exception as e:
            raise e
        return res

    async def aexecute(self):
        try:
            res = await self._execute()
        except Exception as e:
            raise e
        return res


flow = class_deco(Flow, 'run')
