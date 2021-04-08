import asyncio

from .runner import *
from ..utils.state import *


class FlowRunner(Runner):
    def __init__(self, flow, **kwargs):
        super().__init__(**kwargs)
        assert flow.initialized
        self.flow = flow

    @call_state_change_handlers
    def set_running(self, state):
        return Running.copy(state)

    @call_state_change_handlers
    def run_flow(self, state):
        res = asyncio.run(self.flow.execute())
        state = Success.copy(state)
        state.result = res
        return state

    def run(self, state) -> State:
        try:
            state = self.initialize_run(state)
            state = self.set_running(state)
            state = self.run_flow(state)
        except Exception as exc:
            e_str = f"Unexpected error: {exc} when calling flow_runner.run"
            if self.logger:
                self.logger.exception(e_str)
            state = Failure(result=exc, message=e_str)

        return state
