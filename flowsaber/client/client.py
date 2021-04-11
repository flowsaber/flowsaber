from typing import Sequence

from ..core import Flow


class Client(object):
    def __init__(self, server_url: str = None):
        self.server_url = server_url

    # WRITE
    async def register_agent(self, agent_id):
        pass

    async def delete_agent(self, agent_id):
        pass

    async def create_flow(self):
        pass

    async def delete_flow(self):
        pass

    async def update_flowrun(self):
        pass

    async def update_taskrun(self):
        pass

    async def write_taskrun_logs(self):
        pass

    # READ
    async def get_flow(self) -> Flow:
        pass

    async def get_flows(self) -> Sequence[int]:
        pass

    async def get_flowrun(self):
        pass

    async def get_flowruns(self) -> Sequence[int]:
        pass
