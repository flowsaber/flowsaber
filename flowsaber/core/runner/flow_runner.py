import asyncio

from .runner import *
from ..scheduler import TaskScheduler
from ..utils.state import *


class FlowRunner(Runner):
    def __init__(self, flow, **kwargs):
        super().__init__(**kwargs)
        assert flow.initialized
        self.flow = flow
        self.name: str = None
        self.labels: List[str] = None

    def serialize(self, old_state: State, new_state: State, state_only=False) -> RunInput:
        if state_only:
            return FlowRunInput(
                id=self.key,
                state=(new_state - old_state).serialize()
            )
        else:
            return TaskRunInput(
                id=self.key,
                flow_id=self.task.flow_key,
                agent_id=self.agent_id,
                name=self.name,
                labels=self.labels,
                state=new_state.serialize()
            )

    @call_state_change_handlers
    @catch_to_failure
    def run(self, state: State) -> State:
        state = self.initialize_run(state)
        state = self.set_state(state, Running)
        state = self.run_flow(state)

        return state

    @call_state_change_handlers
    @catch_to_failure
    def run_flow(self, state):
        res = asyncio.run(self.async_run_flow())
        state = Success.copy(state)
        state.result = res
        return state

    async def async_run_flow(self):
        async with TaskScheduler().start() as scheduler:
            res = await self.flow.execute(scheduler=scheduler)
        return res
