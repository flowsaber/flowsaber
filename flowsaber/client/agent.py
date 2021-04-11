import asyncio
from functools import partial

from .client import Client
from ..core.runner.flow_runner import FlowRunner
from ..core.scheduler import FlowScheduler
from ..core.utils.state import *


class Agent(object):
    def __init__(self, client: Client):
        self.client = client
        self.flowruns = {}

    async def start(self):
        await self.client.register_agent(self)
        with FlowScheduler().start() as scheduler:
            while True:
                flowrun_ids = await self.client.get_flowruns()
                for flowrun_id in flowrun_ids:
                    flow_run = await self.client.get_flowrun(flowrun_id)
                    flow = await self.client.get_flow(flow_run.flow_id)
                    flow_runner = FlowRunner(flow)
                    flow_state = Pending(context=flow_run.context)
                    flowrun_fut = scheduler.submit(partial(flow_runner.run, flow_state))
                    self.flowruns[flowrun_id] = flowrun_fut

                    def remove_flowrun(fut):
                        del self.flowruns[flowrun_id]

                    flowrun_fut.add_done_callback(remove_flowrun)

                await asyncio.sleep(3)
