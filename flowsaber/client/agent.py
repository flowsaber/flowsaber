import asyncio
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING

import flowsaber
from flowsaber.client.client import Client
from flowsaber.core.engine.scheduler import FlowScheduler
from flowsaber.core.utility.state import Scheduled
from flowsaber.server.database.models import AgentInput, GetFlowRunsInput

if TYPE_CHECKING:
    from concurrent.futures import Future


# default ProcessPoolExecutor can not pickle build flow
def run_flowrun(flowrun_data: dict, flow_data: dict, server_address: str, agent_id: str):
    from flowsaber.api import Flow, FlowRunner, State

    flow_run_kwargs = {
        'context': {
            **flowrun_data['context'],
            'agent_id': agent_id
        }
    }

    try:
        state = State.from_dict(flowrun_data['state'])
        flow = Flow.deserialize(flow_data['serialized_flow'])
        flow_runner = FlowRunner(
            flow,
            server_address=server_address,
            id=flowrun_data['id'],
            name=flowrun_data['name'],
            labels=flowrun_data['labels']
        )
        flow_runner.run(state=state, **flow_run_kwargs)

    except BaseException as e:
        raise RuntimeError(f"Run failed, with error: {e}.")


class Agent(object):
    """Agent fetches available flowruns in Scheduled state, and use FlowScheduler and Flowrunner
    for executing flow in processes.
    """

    def __init__(self, server: str, id: str = None, name: str = None, labels: list = None):
        self.id = id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []
        self.client = Client(server)
        self.server = server

        self.flowruns = defaultdict(dict)

    async def start(self):
        flowsaber.context.logger.info(f"Started aagent: {self.__dict__}")
        agent_input = AgentInput(
            id=self.id,
            name=self.name,
            labels=self.labels
        )
        await self.client.mutation('delete_agent', self.id, "success")
        agent_data = await self.client.mutation('create_agent', agent_input, "id")
        assert agent_data['id'] == self.id
        with FlowScheduler() as scheduler:
            # retrieve scheduled flowruns endlessly
            while True:
                # fetch flowrun ids
                get_flowruns_input = GetFlowRunsInput(
                    state_type=[Scheduled().state_type],
                    agent_id=[self.id]
                )
                flowruns = await self.client.query(
                    'get_flowruns',
                    get_flowruns_input,
                    "id name labels flow_id state {state_type result message} context"
                )
                for flowrun in flowruns:
                    flowrun_id = flowrun['id']
                    if flowrun_id in self.flowruns:
                        flowsaber.context.logger.debug(f"Find already executed flowrun: {flowrun_id}, Pass.")
                        continue
                    else:
                        flowsaber.context.logger.info(f"Fetched scheduled flowrun: {flowrun_id} "
                                                      f"for flow: {flowrun['flow_id']}, try for running...")
                    try:
                        # fetch flow
                        flow = await self.client.query('get_flow', flowrun['flow_id'],
                                                       "serialized_flow")
                        # use flow_runner to run the flow

                        fut = scheduler.create_task(
                            run_flowrun,
                            flowrun,
                            flow,
                            self.server,
                            self.id
                        )
                        flowrun_info = self.flowruns[flowrun_id]
                        flowrun_info.update({
                            'flow': flow,
                            'future': fut,
                        })

                        fut.add_done_callback(partial(self.report, flowrun_id))

                    except Exception as e:
                        flowsaber.context.logger.info(f"Meet error when fetching/executing "
                                                      f"flowrun:{flowrun_id} 's flow with error: {e}")
                # sleep for some time
                await asyncio.sleep(3)

    def report(self, flowrun_id: int, fut: 'Future'):
        logger = flowsaber.context.logger
        if fut.cancelled():
            logger.info(f"The flowrun: {flowrun_id} has been cancelled, remove it from flowruns.")
            self.flowruns.pop(flowrun_id)
        elif fut.exception():
            logger.info(f"The flowrun: {flowrun_id} run failed with exception: {fut.exception()}")
        else:
            logger.info(f"The flowrun: {flowrun_id} run succeed with return: {fut.result()}")


if __name__ == "__main__":
    agent = Agent("http://127.0.0.1:8123", id="test")

    asyncio.run(agent.start())
