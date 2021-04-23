import asyncio

import flowsaber
from flowsaber.core.base import enter_context
from flowsaber.core.engine.runner import (
    Runner, catch_to_failure, call_state_change_handlers, redirect_std_to_logger
)
from flowsaber.core.engine.scheduler import TaskScheduler
from flowsaber.core.flow import Flow
from flowsaber.core.utility.state import State, Scheduled, Pending, Running, Success
from flowsaber.server.database.models import FlowRunInput, RunInput


class FlowRunner(Runner):
    """Aimed for executing flow and maintaining/recording/responding state changes of the flow.
    """

    def __init__(self, flow: Flow, **kwargs):
        super().__init__(**kwargs)
        assert isinstance(flow, Flow) and flow.initialized
        self.flow: Flow = flow
        self.component = self.flow

    def enter_run(self, *args, **kwargs):
        super().enter_run(*args, **kwargs)
        # this is redundant, keep it for uniformity
        self.context.update(flowrun_id=self.id)
        if 'context' in kwargs:
            kwargs['context'].update(flowrun_id=self.id)
        flowsaber.context.update(flowrun_id=self.id)

    @enter_context
    @call_state_change_handlers
    @catch_to_failure
    def start_run(self, state: State = None, **kwargs) -> State:
        print("come here0")
        state = self.initialize_run(state, **kwargs)
        print("come here1")
        state = self.set_state(state, Pending)
        print("come here2")
        state = self.set_state(state, Running)
        print("come here3")
        state = self.run_flow(state, **kwargs)
        print("leave herer")

        return state

    @call_state_change_handlers
    @catch_to_failure
    @redirect_std_to_logger
    def run_flow(self, state, **kwargs):
        res = asyncio.run(self.async_run_flow(**kwargs))
        state = Success.copy(state)
        state.result = res
        return state

    async def async_run_flow(self, **kwargs):
        async with TaskScheduler() as scheduler:
            res = await self.flow.start(scheduler=scheduler, **kwargs)
        return res

    def serialize(self, old_state: State, new_state: State) -> RunInput:
        flowrun_input = FlowRunInput(id=self.id, state=new_state.to_dict())
        if isinstance(old_state, Scheduled) and isinstance(new_state, Pending):
            flowrun_input.__dict__.update({
                'task_id': flowsaber.context.get('task_id'),
                'flow_id': flowsaber.context.get('flow_id'),
                'agent_id': flowsaber.context.get('agent_id'),
                'name': flowsaber.context.get('flow_name'),
                'labels': flowsaber.context.get('flow_labels'),
                'inputs': {},
                'context': flowsaber.context.to_dict()
            })

        return flowrun_input
