import inspect
from typing import Sequence

import aiohttp
from pydantic import validate_arguments

from flowsaber.core.flow import Flow
from flowsaber.server.models import *


class GraphqlError(Exception):
    pass


class ServerError(Exception):
    pass


class ValidateMeta(type):
    def __new__(cls, clsname, bases, clsdict: dict):
        for k, v in clsdict.items():
            if inspect.isfunction(v):
                clsdict[k] = validate_arguments(v)
        return super().__new__(cls, clsname, bases, clsdict)


class Client(object, metaclass=ValidateMeta):
    def __init__(self, server_url: str = None, retry: int = 3):
        self.server_url = server_url
        self.retry = retry

    async def post(self, url, json=None, **kwargs) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=json, **kwargs) as rsp:
                return await rsp.json()

    async def graphql(self, query: str, variables: dict, **kwargs) -> dict:
        json = {
            'query': query,
            'variables': variables
        }
        # with multiple retry
        success = False
        retry = self.retry
        error = None
        rsp = {}
        while not success and retry > 0:
            try:
                rsp = await self.post(self.server_url, json=json, **kwargs)
                success = True
            except Exception as e:
                retry -= 1
                error = e
        # raise request error
        if not success and error:
            raise error
        # raise graphql error
        if 'errors' in rsp:
            raise GraphqlError(str(rsp['errors']))
        # raise server error
        if 'success' in rsp['data'] and not rsp['data']['success']:
            raise ServerError(str(rsp['data']))

        return rsp['data']

    async def register_agent(self, agent_input: AgentInput) -> int:
        query = """
            mutation($input: agent_input!) {
                register_agent(input: $input) {
                    id
                }
            }
        """
        result = await self.graphql(query, dict(input=agent_input.dict()))

        return result['register_agent']['id']

    async def delete_agent(self, agent_id: int) -> dict:
        query = """
            mutation($input: UUID!) {
                delete_agent(agent_id: $input) {
                    success
                    message
                }
            }
        """
        result = await self.graphql(query, dict(input=agent_id))

        return result['delete_agent']

    async def create_flow(self, flow_input: FlowInput) -> int:
        query = """
            mutation($input: FlowInput!) {
                create_flow(input: $input) {
                    id
                }
            }
        """
        result = await self.graphql(query, dict(input=flow_input))

        return result['create_flow']['id']

    async def delete_flow(self, flow_id: int) -> dict:
        query = """
            mutation($input: UUID!) {
                delete_flow(flow_id: $input) {
                    success
                    message
                }
            }
        """
        result = await self.graphql(query, dict(input=flow_id))

        return result['delete_flow']

    async def update_flowrun(self, flowrun_input: FlowRunInput) -> int:
        query = """
            mutation($input: FlowRunInput!) {
                update_flowrun(input: $input) {
                    id
                }    
            }
        """

        result = await self.graphql(query, dict(input=flowrun_input))

        return result['update_flowrun']['id']

    async def update_taskrun(self, taskrun_input: TaskRunInput) -> int:
        query = """
            mutation($input: TaskRunInput!) {
                update_taskrun(input: $input) {
                    id
                }    
            }
        """

        result = await self.graphql(query, dict(input=taskrun_input.dict()))

        return result['update_taskrun']['id']

    async def write_run_logs(self, runlogs: Sequence[RunLogInput]) -> dict:
        query = """
            mutation($input: [RunLogInput!]!) {
                write_runlogs(logs: $input) {
                    success
                    message
                }
            }
        """

        result = await self.graphql(query, dict(input=[runlog.dict() for runlog in runlogs]))

        return result['write_runlogs']

    async def get_flow(self, flow_id: int) -> Flow:
        query = """
            query($input: UUID!) {
                get_flow(flow_id: $input) {
                    serialized_flow
                }
            }
        """

        result = await self.graphql(query, dict(input=flow_id))

        flow = Flow.deserialize(result['get_flow']['serialized_flow'])
        assert flow.initialized

        return flow

    async def get_flows(self, get_flows_input: GetFlowsInput) -> Sequence[int]:
        query = """
            query($input: GetFlowsInput!) {
                get_flows(input: $input) {
                    id
                }
            }
        """

        result = await self.graphql(query, dict(input=get_flows_input.dict()))

        return result['get_flows']

    async def get_flowrun(self, flowrun_id: int) -> dict:
        query = """
            query($input: UUID!) {
                get_flowrun(flowrun_id: $input) {
                    id
                    flow_id
                    state
                    context
                }
            }
        """

        result = await self.graphql(query, dict(input=flowrun_id))

        return result['get_flowrun']

    async def get_flowruns(self, get_flowruns_input: GetFlowRunsInput) -> Sequence[int]:
        query = """
            query($input: GetFlowRunsInput!) {
                get_flowruns(input: $input) {
                    id
                }
            }
        """

        result = await self.graphql(query, dict(input=get_flowruns_input.dict()))

        return result['get_flowruns']
