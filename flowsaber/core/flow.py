from typing import Union

import cloudpickle

from flowsaber.core.task import *


class Flow(Component):
    FUNC_PAIRS = [('run', '__call__', True)]

    default_config = {
        'fork': 20,
        'workdir': ""
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.components = Optional[List[Component]] = None
        self.tasks = Optional[List[BaseTask]] = None
        self.edges = Optional[List[Edge]] = None

    @property
    def config_name(self) -> str:
        return "flow_config"

    def __enter__(self):
        flowsaber.context.flow_stack.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        flowsaber.context.flow_stack.pop(-1)

    def __call__(self, *args, **kwargs) -> Union[Output, 'Flow']:
        # update context and store to flow.context
        flow = self.call_initialize()
        # enter flow context
        with flow, flowsaber.context(flow.context):
            flow.output = flow.run(*args, **kwargs) or Channel.end()

        if flowsaber.context.up_flow:
            flowsaber.context.up_flow.components.append(flow)
            return flow.output
        else:
            # in the top level flow, just return the flow itself
            return flow

    def call_initialize(self, *args, **kwargs):
        new: "Flow" = super().call_initialize(*args, **kwargs)
        # only the up-most flow record infos
        if not flowsaber.context.up_flow:
            new.components = []
            new.tasks = []
            new.edges = []
        return new

    def initialize_context(self):
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

    def run(self, *args, **kwargs) -> Channel:
        raise NotImplementedError("Not implemented. Users are supposed to compose flow/task in this method.")

    async def start_execute(self, **kwargs):
        await super().start_execute(**kwargs)

        async def execute_child_components():
            futures = []
            for component in self.components:
                future = asyncio.ensure_future(component.start(**kwargs))
                futures.append(future)

            # used fo debugging
            loop = asyncio.get_running_loop()
            if not hasattr(loop, 'flowsaber_futures'):
                loop.flowsaber_futures = []
            loop.flowsaber_futures += list(zip(self.components, futures))

            fut = await asyncio.gather(*futures)
            return fut

        # for the top most flow, initialize executors
        if self.config_dict['id'] == self.context['flow_id']:
            async with flowsaber.context:
                return await execute_child_components()
        else:
            return await execute_child_components()

    def serialize(self) -> FlowInput:
        config = self.config
        return FlowInput(
            id=config.id,
            name=config.name,
            labels=config.labels,
            tasks=[task.serialize() for task in self.tasks],
            edges=[edge.serialize() for edge in self.edges],
            source_code=inspect.getsource(type(self)),
            serialized_flow=str(cloudpickle.dumps(self))
        )

    @classmethod
    def deserialize(cls, flow: str) -> "Flow":
        return cloudpickle.loads(bytes(flow))
