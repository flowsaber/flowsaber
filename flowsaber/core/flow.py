import asyncio
import uuid
from pathlib import Path
from typing import Union, List, Optional, Tuple

import flowsaber
from flowsaber.core.base import Component
from flowsaber.core.channel import Channel, Output
from flowsaber.core.task import BaseTask, Edge
from flowsaber.core.utils import check_cycle, class_deco
from flowsaber.server.database import FlowInput
from flowsaber.utility.utils import change_cwd


class Flow(Component):
    """Represents the organizer of tasks, flows can also be used as components. Except for the top-most flow
    which represents the whole running unit, all flows within are simply virtual flows and don't have running state
    like flowrun or taskrun. However, flows and tasks all can have personalized configs.
    """
    FUNC_PAIRS = [('run', '__call__', True)]

    default_config = {
        'resources_limit': {
            'fork': 20
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.components: Optional[List[Component]] = None
        self.tasks: Optional[List[BaseTask]] = None
        self.edges: Optional[List[Edge]] = None

    @property
    def config_name(self) -> str:
        return "flow_config"

    def __enter__(self):
        flowsaber.context.flow_stack.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        flowsaber.context.flow_stack.pop(-1)

    def call_initialize(self, *args, **kwargs):
        super().call_initialize(*args, **kwargs)
        # all flow record child executable components
        self.components = []
        # only the up-most flow record infos
        if not flowsaber.context.up_flow:
            self.tasks = []
            self.edges = []

    def initialize_context(self):
        """Expose some attributes of self.config into self.context.
        """
        # a task's workdir == context.flow_workdir / context.flow_config.workdir / context.task_config.workdir
        # If any of the above dirs is absolute dir, then use it instead
        super().initialize_context()
        # only inject the up-most flow's info
        if not flowsaber.context.up_flow:
            flow_workdir = self.config_dict['workdir']
            if not Path(flow_workdir).is_absolute():
                flow_workdir = str(Path().resolve().absolute())
            self.context.update({
                'flow_id': self.config_dict['id'],
                'flow_name': self.config_dict['name'],
                'flow_full_name': self.config_dict['full_name'],
                'flow_labels': self.config_dict['labels'],
                'flow_workdir': flow_workdir,
                'top_flow_config': self.config_dict
            })

    def call_build(self, *args, **kwargs) -> Union[Output, 'Flow']:
        # enter flow context
        from copy import deepcopy
        # used for child component
        self_context = deepcopy(self.context)
        self_context["up_" + self.config_name] = self_context[self.config_name]
        self_context.pop(self.config_name)
        with self, flowsaber.context(self_context):
            # TODO in case of None return, make a channel yield a end when all components run done
            self.output = self.run(*args, **kwargs) or Channel.end()

        # in order to check up_flow, can not run within with statement
        # now the dependency has been built
        if flowsaber.context.up_flow:
            flowsaber.context.up_flow.components.append(self)
            return self.output
        else:
            if check_cycle(self.task_id_edges):
                raise ValueError("The dependency graph has cycle.")
            # in the top level flow, just return the flow itself
            return self

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError("Not implemented. Users are supposed to compose flow/task in this method.")

    async def start_execute(self, **kwargs):
        """The up most flow needd to initialize executors.
        Parameters
        ----------
        kwargs

        Returns
        -------

        """
        await super().start_execute(**kwargs)

        async def execute_child_components():
            futures = []
            for component in self.components:
                future = asyncio.create_task(component.start(**kwargs))
                futures.append(future)

            # used fo debugging
            loop = asyncio.get_running_loop()
            if not hasattr(loop, 'flowsaber_futures'):
                loop.flowsaber_futures = []
            loop.flowsaber_futures += list(zip(self.components, futures))

            done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)
            # TODO wait for truely cancelled
            for fut in pending:
                fut.cancel()

            res_futures = list(done) + list(pending)
            self.check_future_exceptions(res_futures)

            return res_futures

        # for the top most flow, clear context info, initialize executors, go to flow_worker
        if self.config_dict['id'] == self.context['flow_id']:
            flowsaber.context._info.clear()
            with change_cwd(self.context.get('flow_workdir', '')) as p:
                async with flowsaber.context:
                    await execute_child_components()
                    # for the top most flow, return None, since Flowrunner's returned final
                    # state will inlcude this
        else:
            await execute_child_components()

    @property
    def task_id_edges(self) -> List[Tuple[str, str]]:
        edges = []
        for edge in self.edges:
            src_task = getattr(edge.channel, 'task', None)
            src_task_id = src_task.config_dict['id'] if src_task else str(uuid.uuid4())
            edges.append((src_task_id, edge.task.config_dict['id']))
        return edges

    def serialize(self) -> FlowInput:
        import base64
        import zlib
        from distributed.protocol.pickle import dumps
        # TODO can not fetch source code of type(self), if it's due to makefun ?
        assert self.initialized
        config = self.config
        compressed_flow = zlib.compress(dumps(self))
        serialized_flow = base64.encodebytes(compressed_flow).decode()
        return FlowInput(
            id=config.id,
            name=config.name,
            full_name=config.full_name,
            labels=config.labels,
            tasks=[task.serialize() for task in self.tasks],
            edges=[edge.serialize() for edge in self.edges],
            docstring=type(self).__doc__ or "",
            context=self.context,
            serialized_flow=serialized_flow
        )

    @classmethod
    def deserialize(cls, serialized_flow: str) -> "Flow":
        import base64
        import zlib
        from distributed.protocol.pickle import loads
        compressed_flow = base64.decodebytes(serialized_flow.encode())
        initialized_flow: 'Flow' = loads(zlib.decompress(compressed_flow))
        assert initialized_flow.initialized
        return initialized_flow


flow = class_deco(Flow, 'run')
