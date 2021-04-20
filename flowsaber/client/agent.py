import asyncio

import flowsaber
from flowsaber.client.client import Client
from flowsaber.core.runner.flow_runner import FlowRunner
from flowsaber.core.scheduler import FlowScheduler
from flowsaber.core.utility.state import *
from flowsaber.server.database.models import *


class Agent(object):
    def __init__(self, client: Client, agent_id: str = None, name: str = None, labels: list = None):
        self.client = client
        self.flowruns = {}
        self.id = agent_id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []

    async def start(self):
        agent_input = AgentInput(
            id=self.id,
            name=self.name,
            labels=self.labels
        )
        agent_id = await self.client.register_agent(agent_input)
        assert agent_id == self.id
        with FlowScheduler().start() as scheduler:
            # retrieve scheduled flowruns endlessly
            while True:
                # fetch flowrun ids
                get_flowruns_input = GetFlowRunsInput(state_type=[Scheduled().state_type])
                flowrun_ids = await self.client.get_flowruns(get_flowruns_input)

                for flowrun_id in flowrun_ids:
                    # fetch flowrun and flow
                    flow_run = await self.client.get_flowrun(flowrun_id)
                    flow = await self.client.get_flow(flow_run['flow_id'])
                    state = State.from_dict(flow_run['state'])

                    # use flow_runner to run the flow
                    flow_runner = FlowRunner(flow)
                    flow_run_kwargs = {
                        'context': flow_run['context']
                    }
                    fut = scheduler.create_task(flow_runner.run, state, **flow_run_kwargs)
                    self.flowruns[flowrun_id] = fut

                    def remove_flowrun(fut):
                        del self.flowruns[flowrun_id]

                    fut.add_done_callback(remove_flowrun)
                # sleep for some time
                await asyncio.sleep(3)
