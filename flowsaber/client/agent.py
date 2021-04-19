import asyncio
from functools import partial
from uuid import uuid4

from .client import Client
from ..core.runner.flow_runner import FlowRunner
from ..core.scheduler import FlowScheduler
from ..core.utils.state import *
from ..server.models import *


class Agent(object):
    def __init__(self, client: Client):
        self.client = client
        self.flowruns = {}
        self.agent_id = None

    async def start(self):
        self.agent_id = await self.client.register_agent(AgentInput(id=uuid4(), name=uuid4()))
        with FlowScheduler().start() as scheduler:
            # retrieve scheduled flowruns endlessly
            while True:
                # fetch flowrun ids
                get_flowruns_input = GetFlowRunsInput(state_type=[Scheduled().state_type])
                flowrun_ids = await self.client.get_flowruns(get_flowruns_input)
                for flowrun_id in flowrun_ids:
                    # fetch flowrun and flow
                    flow_run = await self.client.get_flowrun(flowrun_id)
                    flow = await self.client.get_flow(flow_run.flow_id)
                    state = State.from_model(flow_run.state)
                    state = Pending.copy(state)
                    await self.client.update_flowrun(FlowRunInput(state=state.to_model()))
                    # use flowrunner to run the flow
                    flow_runner = FlowRunner(flow)
                    fut = scheduler.create_task(partial(flow_runner.run, state))
                    self.flowruns[flowrun_id] = fut

                    def remove_flowrun(fut):
                        del self.flowruns[flowrun_id]

                    fut.add_done_callback(remove_flowrun)
                # sleep for some time
                await asyncio.sleep(3)
