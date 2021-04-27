import asyncio
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING

import flowsaber
from flowsaber.client.client import Client
from flowsaber.core.engine.flow_runner import FlowRunner
from flowsaber.core.engine.scheduler import FlowScheduler
from flowsaber.core.flow import Flow
from flowsaber.core.utility.state import Scheduled, State
from flowsaber.server.database.models import AgentInput, GetFlowRunsInput

if TYPE_CHECKING:
    from concurrent.futures import Future


class Agent(object):
    """Agent fetches available flowruns in Scheduled state, and use FlowScheduler and Flowrunner
    for executing flow in processes.
    """

    def __init__(self, server: str, id: str = None, name: str = None, labels: list = None):
        self.id = id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []
        self.client = Client(server)

        self.flowruns = defaultdict(dict)

    async def start(self):
        agent_input = AgentInput(
            id=self.id,
            name=self.name,
            labels=self.labels
        )
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
                data = await self.client.query('get_flowruns', get_flowruns_input, "id")
                for flowrun_id in data['id']:
                    if flowrun_id in self.flowruns:
                        flowsaber.context.logger.info(f"Find already executed flowrun: {flowrun_id}, Pass.")
                        continue
                    else:
                        flowsaber.context.logger.info(f"Fetched scheduled flowrun: {flowrun_id}, try for running...")
                    # fetch flowrun
                    flowrun_data = await self.client.query(
                        'get_flowrun',
                        flowrun_id,
                        "id flow_id state {state_type, result, message}"
                    )
                    flowrun_id = flowrun_data['id']
                    state = State.from_dict(flowrun_data['state'])
                    assert isinstance(state, Scheduled)

                    try:
                        # fetch flow
                        flow_data = await self.client.query('get_flow', flowrun_data['flow_id'],
                                                            "serialized_flow context")
                        flow = Flow.deserialize(flow_data['serialized_flow'])
                        context = flow_data['context']

                        # use flow_runner to run the flow
                        flow_run_kwargs = {
                            'context': {
                                **context,
                                'agent_id': self.id
                            }
                        }
                        flow_runner = FlowRunner(flow)
                        fut = scheduler.create_task(flow_runner.run, state=state, **flow_run_kwargs)
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
                flowsaber.context.logger.info("Waiting for 3 seconds ... ...")
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
