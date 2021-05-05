from collections import defaultdict
from typing import Union

from ariadne import QueryType, MutationType, ScalarType, ObjectType

from flowsaber.server.database.db import DataBase
from flowsaber.server.database.models import *


def ch_id(data: dict) -> dict:
    if "_id" in data:
        data['id'] = data.pop('_id')
    elif "id" in data:
        data['_id'] = data.pop("id")
    return data


def get_time_exp(input) -> dict:
    exp = {}
    before = getattr(input, 'before')
    after = getattr(input, 'after')
    if after:
        exp['$gt'] = after
    if before:
        exp['$lt'] = before

    return exp


def update_notnone_exp(data: dict):
    # Note: does not check for list
    exp = {}

    def resolve(value, prevk=""):
        for k, v in value.items():
            if isinstance(v, dict):
                resolve(v, f"{k}.")
            elif v is not None:
                exp[f'{prevk}{k}'] = v

    resolve(data)
    exp.pop("id", None)
    exp.pop("_id", None)
    return {"$set": exp}


def get_field(obj: Union[BaseModel, dict], filed: str):
    if isinstance(obj, BaseModel):
        return getattr(obj, filed)
    else:
        return obj[filed]


def get_resolvers(db: DataBase):
    assert isinstance(db, DataBase)

    query = QueryType()
    mutation = MutationType()
    agent = ObjectType("Agent")
    flow = ObjectType("Flow")
    task = ObjectType("Task")
    flowrun = ObjectType("FlowRun")

    timestamp_scalar = ScalarType("TimeStamp")
    uuid_scalar = ScalarType("UUID")
    json_scalar = ScalarType("JSON")

    @timestamp_scalar.serializer
    def serialize_timestamp(value: float) -> float:
        assert isinstance(value, float)
        return value

    @uuid_scalar.serializer
    def serialize_uuid(value: str) -> str:
        assert isinstance(value, str)
        return value

    @json_scalar.serializer
    def serialize_json(value: dict) -> dict:
        assert isinstance(value, dict)
        return value

    # query

    @query.field('hello')
    async def hello(obj, info) -> str:
        return "Hello!"

    @query.field('get_agent')
    async def get_agent(obj, info, input: str) -> dict:
        agent_id = input
        agent_dict = await db.agent.find_one({"_id": agent_id})
        return ch_id(agent_dict)

    @query.field('get_agents')
    async def get_agents(obj, info) -> List[dict]:
        agents = []
        async for agent_dict in db.agent.find({}):
            agents.append(ch_id(agent_dict))

        return agents

    @query.field("get_flow")
    async def get_flow(obj, info, input: str) -> Flow:
        flow_id = input
        flow_dict = await db.flow.find_one({"_id": flow_id})
        flow_dict = ch_id(flow_dict)
        flow = Flow(**flow_dict)
        return flow

    @query.field("get_flows")
    async def get_flows(obj, info, input: dict) -> List[dict]:
        input = GetFlowsInput(**input)
        exp = {}
        if input.id or input.name or input.labels:
            exp.update({
                "$or":
                    [
                        {'_id': {'$in': input.id}},
                        {"name": {"$in": input.name}},
                        {"labels": {"$all": input.labels}}
                    ]

            })
        flows = []
        async for flow_dict in db.flow.find(exp):
            flows.append(ch_id(flow_dict))

        return flows

    @query.field("get_taskrun")
    async def get_taskrun(obj, info, input: str) -> TaskRun:
        taskrun_id = input
        taskrun_dict = await db.taskrun.find_one({"_id": taskrun_id})
        taskrun_dict = ch_id(taskrun_dict)
        taskrun = TaskRun(**taskrun_dict)
        return taskrun

    @query.field("get_taskruns")
    async def get_taskruns(obj, info, input: dict) -> List[dict]:
        input = GetTaskRunsInput(**input)
        exp = {}
        has_or_exp = input.id or input.task_id or input.flow_id
        if has_or_exp:
            exp.update({
                "$or":
                    [
                        {"_id": {"$in": input.id}},
                        {'task_id': {"$in": input.task_id}},
                        {"flow_id": {"$in": input.flow_id}},
                    ]
            })
        if input.flowrun_id:
            exp.update({
                "flowrun_id": {"$in": input.flowrun_id},
            })
        if input.agent_id:
            exp.update({
                "agent_id": {"$in": input.agent_id},
            })
        if input.state_type:
            exp.update({
                "state.state_type": {"$in": input.state_type}
            })
        if input.before or input.after:
            time_exp = get_time_exp(input)
            exp.update({"start_time": time_exp})
        taskruns = []
        async for taskrun_dict in db.taskrun.find(exp):
            taskruns.append(ch_id(taskrun_dict))

        return taskruns

    @query.field("get_flowrun")
    async def get_flowrun(obj, info, input: str) -> FlowRun:
        flowrun_id = input
        flowrun_dict = await db.flowrun.find_one({"_id": flowrun_id})
        if flowrun_dict:
            flowrun_dict = ch_id(flowrun_dict)
            flowrun = FlowRun(**flowrun_dict)
        else:
            # for check_cancelling task, return a fake one
            flowrun = {'state': {'state_type': "Scheduled"}}
        return flowrun

    @query.field("get_flowruns")
    async def get_flowruns(obj, info, input: dict) -> List[dict]:
        input = GetFlowRunsInput(**input)
        exp = {}
        has_or_exp = input.id or input.flow_id or input.name or input.labels
        if has_or_exp:
            exp.update({
                "$or":
                    [
                        {"_id": {"$in": input.id}},
                        {"flow_id": {"$in": input.flow_id}},
                        {"name": {"$in": input.name}},
                        {"labels": {"$all": input.labels}},
                    ]
            })
        if input.agent_id:
            exp.update({
                "agent_id": {"$in": input.agent_id},
            })
        if input.state_type:
            exp.update({
                "state.state_type": {"$in": input.state_type}
            })
        if input.before or input.after:
            time_exp = get_time_exp(input)
            exp.update({"start_time": time_exp})

        flowruns = []
        async for flowrun_dict in db.flowrun.find(exp):
            flowruns.append(ch_id(flowrun_dict))

        return flowruns

    @query.field("get_runlogs")
    async def get_runlogs(obj, info, input: dict) -> List[dict]:
        input = GetRunLogsInput(**input)
        exp = {}
        has_or_exp = input.id or input.taskrun_id or input.flowrun_id or input.agent_id
        if has_or_exp:
            exp.update({
                "$or": [
                    {"_id": {"$in": input.id}},
                    {"taskrun_id": {"$in": input.taskrun_id}},
                    {"flowrun_id": {"$in": input.flowrun_id}},
                    {"agent_id": {"$in": input.agent_id}},
                ],
            })
        if input.level:
            exp.update({
                "level": {"$in": input.level}
            })
        if input.before or input.after:
            time_exp = get_time_exp(input)
            exp.update({"time": time_exp})

        runlogs = []
        async for runlog_dict in db.runlog.find(exp):
            runlogs.append(ch_id(runlog_dict))

        return runlogs

    # mutation

    @mutation.field("hello")
    async def resolve_write_hello(obj, info):
        return SuccessPayload()

    @mutation.field("create_agent")
    async def create_agent(obj, info, input: dict):
        agent_input = AgentInput(**input)
        request = info.context['request']
        address = request.client.host
        agent = Agent(**agent_input.dict(), address=address)
        await db.agent.delete_one({"_id": agent.id})
        await db.agent.insert_one(ch_id(agent.dict()))
        return agent

    @mutation.field("delete_agent")
    async def delete_agent(obj, info, input: str):
        agent_id = input
        res = await db.agent.delete_one({"_id": agent_id})
        return SuccessPayload(success=res.deleted_count == 1)

    @mutation.field("create_flow")
    async def create_flow(obj, info, input: dict):
        flow_input = FlowInput(**input)
        docs_dict = defaultdict(list)
        # store channels, tasks, flows
        for i, task_input in enumerate(flow_input.tasks):
            for j, ch_input in enumerate(task_input.output):
                task_input.output[j] = ch_input.id
                docs_dict['channel'].append(Channel(**ch_input.dict()))
            flow_input.tasks[i] = task_input.id
            docs_dict['task'].append(Task(**task_input.dict()))
        docs_dict['flow'].append(Flow(**flow_input.dict()))

        for collection, docs in docs_dict.items():
            docs = [ch_id(doc.dict()) for doc in docs]
            await getattr(db, collection).insert_many(docs)

        return docs_dict['flow'][0]

    @mutation.field("delete_flow")
    async def delete_flow(obj, info, input: str):
        flow_id = input
        res = await db.flow.delete_one({"_id": flow_id})
        return SuccessPayload(success=res.deleted_count == 1)

    @mutation.field("update_flowrun")
    async def update_flowrun(obj, info, input: dict):
        flowrun_input = FlowRunInput(**input)
        flowrun_id = flowrun_input.id
        flowrun = await db.flowrun.find_one({"_id": flowrun_id})
        if flowrun is None:
            # insert a new flowrun
            flowrun = FlowRun(**flowrun_input.dict())
            if not flowrun.start_time:
                flowrun.start_time = flowrun.last_heartbeat
            await db.flowrun.insert_one(ch_id(flowrun.dict()))
            # append to agent, flow 's flowruns
            await db.agent.update_one({"_id": flowrun.agent_id}, {"$push": {"flowruns": flowrun.id}})
            await db.flow.update_one({"_id": flowrun.flow_id}, {"$push": {"flowruns": flowrun.id}})
            return flowrun
        else:
            state_type = flowrun['state']['state_type']
            # in cancelling state, only allow cancelled new state
            if state_type == "Cancelling":
                if flowrun_input.state and flowrun_input.state.state_type != 'Cancelled':
                    return ch_id(flowrun)
            # in cancelled state, do not allow new state
            elif state_type == "Cancelled":
                return ch_id(flowrun)
            update_exp = update_notnone_exp(flowrun_input.dict())
            await db.flowrun.update_one({'_id': flowrun_id}, update_exp)
            updated_flowrun = await db.flowrun.find_one({"_id": flowrun_id})
            return ch_id(updated_flowrun)

    @mutation.field("update_taskrun")
    async def update_taskrun(obj, info, input: dict):
        taskrun_input = TaskRunInput(**input)
        taskrun_id = taskrun_input.id
        taskrun = await db.taskrun.find_one({"_id": taskrun_id})
        if taskrun is None:
            # insert a new task run
            taskrun = TaskRun(**taskrun_input.dict())
            if not taskrun.start_time:
                taskrun.start_time = taskrun.last_heartbeat
            await db.taskrun.insert_one(ch_id(taskrun.dict()))
            # append taskrun into the flowrun
            await db.flowrun.update_one({"_id": taskrun.flowrun_id}, {"$push": {"taskruns": taskrun.id}})
            return taskrun
        else:
            update_exp = update_notnone_exp(taskrun_input.dict())
            await db.taskrun.update_one({'_id': taskrun_id}, update_exp)
            updated_taskrun = await db.taskrun.find_one({"_id": taskrun_id})
            return ch_id(updated_taskrun)

    @mutation.field("write_runlogs")
    async def write_runlogs(obj, info, input: dict):
        runlogs_input = RunLogsInput(**input)
        run_logs = [ch_id(run_log.dict()) for run_log in runlogs_input.logs]
        await db.runlog.insert_many(run_logs)
        return SuccessPayload()

    #
    @flow.field("tasks")
    async def resolve_tasks(obj, info) -> List[dict]:
        task_ids = get_field(obj, 'tasks')
        tasks = await db.task.find({"_id": {"$in": task_ids}}).to_list(len(task_ids))
        tasks = [ch_id(task) for task in tasks]
        return tasks

    @task.field('output')
    async def resolve_channels(obj, info) -> List[dict]:
        channel_ids = get_field(obj, 'output')
        channels = await db.channel.find({"_id": {"$in": channel_ids}}).to_list(len(channel_ids))
        channels = [ch_id(channel) for channel in channels]
        return channels

    @flow.field("flowruns")
    @agent.field("flowruns")
    async def resolve_flowruns(obj, info) -> List[dict]:
        flowrun_ids = get_field(obj, 'flowruns')
        flowruns = await db.flowrun.find({"_id": {"$in": flowrun_ids}}).to_list(len(flowrun_ids))
        flowruns = [ch_id(flowrun) for flowrun in flowruns]
        return flowruns

    @flowrun.field("taskruns")
    async def resolve_taskruns(obj, info) -> List[dict]:
        taskrun_ids = get_field(obj, 'taskruns')
        taskruns = await db.taskrun.find({"_id": {"$in": taskrun_ids}}).to_list(len(taskrun_ids))
        taskruns = [ch_id(taskrun) for taskrun in taskruns]
        return taskruns

    return locals()
