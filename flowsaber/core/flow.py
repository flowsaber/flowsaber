import asyncio
from collections import defaultdict
from typing import Union, Dict, Optional

from graphviz import Digraph

from flowsaber.context import context
from flowsaber.utility.logtool import get_logger
from flowsaber.utility.utils import class_deco, TaskOutput
from .base import FlowComponent
from .channel import Channel

logger = get_logger(__name__)


class Flow(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tasks: Optional[Dict[FlowComponent, {}]] = None
        self._graph = None

    def tasks(self):
        if getattr(self, '_tasks') is None:
            raise ValueError("You are using a copied or fresh new flow, please initialize")
        return self._tasks

    @property
    def graph(self):
        if not getattr(self, '_graph', None):
            self._graph = Digraph()
        return self._graph

    def __enter__(self):
        context.flow_stack.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        context.flow_stack.pop(-1)

    def __call__(self, *args, **kwargs) -> Union[TaskOutput, 'Flow']:
        flow = self.copy_new()
        # set up flows environments
        with flow:
            flow._output = flow.run(*args, **kwargs)
            # in case the _output is None/Nothing, create a END Channel
            if flow._output is None:
                flow._output = Channel.end()

        if context.up_flow:
            context.up_flow._tasks.setdefault(flow, {})
            return flow._output
        # in the top level flow, just return the flow itself
        else:
            return flow

    def copy_new(self, *args, **kwargs):
        new = super().copy_new(*args, **kwargs)
        new._tasks = defaultdict(dict)
        return new

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError("Not implemented. Users are supposed to compose flow/task in this method.")

    async def execute(self, **kwargs) -> asyncio.Future:
        await super().execute(**kwargs)
        task_futures = []
        for task, task_info in self._tasks.items():
            future = task_info['future'] = asyncio.ensure_future(task.execute(**kwargs))
            task_futures.append(future)

            # used fo debugging
            loop = asyncio.get_running_loop()
            if not hasattr(loop, '_task_futures'):
                loop._task_futures = []
            loop._task_futures.append((task, future))

        fut = await asyncio.gather(*task_futures)

        return fut


flow = class_deco(Flow, 'run')
