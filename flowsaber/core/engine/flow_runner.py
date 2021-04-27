import asyncio

import flowsaber
from flowsaber.core.base import enter_context
from flowsaber.core.engine.runner import (
    Runner, catch_to_failure, call_state_change_handlers, redirect_std_to_logger
)
from flowsaber.core.engine.scheduler import TaskScheduler
from flowsaber.core.flow import Flow
from flowsaber.core.utility.state import State, Pending, Running, Success
from flowsaber.server.database.models import FlowRunInput


class FlowRunner(Runner):
    """Aimed for executing flow and maintaining/recording/responding state changes of the flow.
    """

    def __init__(self, flow: Flow, **kwargs):
        super().__init__(**kwargs)
        assert isinstance(flow, Flow) and flow.initialized
        self.flow: Flow = flow
        self.component = self.flow
        if self.client:
            self.add_task(self.check_cancellation)
            self.add_task(self.maintain_heartbeat)

    def initialize_context(self, *args, **kwargs):
        update_context = {
            'flowrun_id': self.id,
            'server_address': self.server_address
        }
        self.context.update(**update_context)
        if 'context' in kwargs:
            kwargs['context'].update(**update_context)
        flowsaber.context.update(**update_context)

    @enter_context
    @redirect_std_to_logger
    @call_state_change_handlers
    @catch_to_failure
    def start_run(self, state: State = None, **kwargs) -> State:
        state = self.initialize_run(state, **kwargs)
        state = self.set_state(state, Pending)
        state = self.set_state(state, Running)
        state = self.run_flow(state, **kwargs)

        return state

    def leave_run(self, *args, **kwargs):
        super().leave_run()

    @call_state_change_handlers
    @catch_to_failure
    def run_flow(self, state, **kwargs):
        res = asyncio.run(self.async_run_flow(**kwargs))
        state = Success.copy(state)
        state.result = res
        return state

    async def async_run_flow(self, **kwargs):
        async with TaskScheduler() as scheduler:
            res = await self.flow.start(scheduler=scheduler, **kwargs)
        return res

    def serialize(self, state: State, state_only=True) -> FlowRunInput:
        info = {'id': self.id, 'state': state.to_dict()}
        if not state_only:
            info.update({
                'agent_id': self.context.get('agent_id'),
                'flow_id': self.context['flow_id'],
                'name': self.name,
                'labels': self.labels,
                'context': {},
            })

        flowrun_input = FlowRunInput(**info)

        return flowrun_input
