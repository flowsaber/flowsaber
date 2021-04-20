from collections import defaultdict
from typing import Sequence

from ariadne import QueryType, MutationType, EnumType, ScalarType, ObjectType
from graphql import GraphQLResolveInfo
from starlette.requests import Request

from flowsaber.server.database.api import get_db
from flowsaber.server.database.models import *

USERNAME = "admin"
PASSWORD = "admin"

db = get_db(f"mongodb+srv://{USERNAME}:{PASSWORD}@flowsaber.bkirk.mongodb.net"
            f"/myFirstDatabase?retryWrites=true&w=majority")

query = QueryType()
mutation = MutationType()
agent = ObjectType("Agent")
flow = ObjectType("Flow")
task = ObjectType("Task")
flowrun = ObjectType("FlowRun")

log_level = EnumType("LogLevel", LogLevel)

datetime_scalar = ScalarType("DateTime")


@datetime_scalar.serializer
def serialize_datetime(value: datetime) -> str:
    assert isinstance(value, datetime)
    return value.isoformat()


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
    if before or after:
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
                exp[f'{prevk}k'] = v

    resolve(data)

    return {"$set": exp}


@query.field('hello')
async def hello(obj, info) -> str:
    return "Hello!"


@query.field('get_agent')
async def get_agent(obj, info, agent_id: str) -> Agent:
    agent = await db.agent.find_one({"_id": agent_id})
    agent = Agent(**ch_id(agent))
    return agent


@query.field('get_agents')
async def get_agents(obj, info) -> IdsPaload:
    ids = []
    async for agent in db.agent.find({}, {'_id': 1}):
        ids.append(agent['_id'])

    return IdsPaload(id=ids)


@query.field("get_flow")
async def get_flow(obj, info, flow_id: str) -> Flow:
    flow = await db.flow.find_one({"_id": flow_id})
    flow = Flow(**ch_id(flow))
    return flow


@query.field("get_taskrun")
async def get_flows(obj, info, input: dict) -> IdsPaload:
    input = GetFlowsInput(**input)
    exp = {
        "$or":
            [
                {'_id': {'$in': input.id}},
                {"name": {"$in": input.name}},
                {"labels": {"$all": input.labels}}
            ]
    }

    ids = []
    async for flow in db.flow.find(exp, {"_id": 1}):
        ids.append(flow['_id'])

    return IdsPaload(id=ids)


@query.field("get_taskrun")
async def get_taskrun(obj, info, taskrun_id: str) -> TaskRun:
    taskrun = await db.taskrun.find({"_id": taskrun_id})
    taskrun = TaskRun(**ch_id(taskrun))
    return taskrun


@query.field("get_taskruns")
async def get_taskruns(obj, info, input: dict) -> IdsPaload:
    input = GetTaskRunsInput(**input)
    exp_taskrun = {
        "$or":
            [
                {"_id": {"$in": input.taskrun_id}},
                {'task_id': {"$in": input.task_id}},
                {"flow_id": {"$in": input.flow_id}},
                {"agent_id": {"$in": input.agent_id}},
                {"flowrun_id": {"$in": input.flowrun_id}},
                {"state.state_type": {"$in": input.state_type}}
            ]
    }
    time_exp = get_time_exp(input)
    if time_exp:
        exp_taskrun.update({"start_time": time_exp})
    ids = []
    async for taskrun in db.taskrun.find(exp_taskrun, {"_id": 1}):
        ids.append(taskrun['_id'])

    return IdsPaload(id=ids)


@query.field("get_flowrun")
async def get_flowrun(obj, info, flowrun_id: str) -> FlowRun:
    flowrun = await db.flowrun.find_one({"_id": flowrun_id})
    flowrun = FlowRun(**ch_id(flowrun))
    return flowrun


@query.field("get_flowruns")
async def get_flowruns(obj, info, input: dict) -> IdsPaload:
    input = GetFlowRunsInput(**input)
    exp_flowrun = {
        "$or":
            [
                {"_id": {"$in": input.flowrun_id}},
                {"flow_id": {"$in": input.flow_id}},
                {"agent_id": {"$in": input.agent_id}},
                {"name": {"$in": input.name}},
                {"labels": {"$all": input.labels}},
            ],
        "state.state_type": {"$in": input.state_type}
    }
    time_exp = get_time_exp(input)
    if time_exp:
        exp_flowrun.update({"start_time": time_exp})
    ids = []
    async for flowrun in db.flowrun.find(exp_flowrun, {"_id": 1}):
        ids.append(flowrun['_id'])

    return IdsPaload(id=ids)


@query.field("get_runlogs")
async def get_runlogs(obj, info, input: dict) -> List[RunLog]:
    input = GetRunLogsInput(**input)
    exp_runlog = {
        "$or": [
            {"taskrun_id": {"$in": input.taskrun_id}},
            {"flowrun_id": {"$in": input.flowrun_id}},
            {"agent_id": {"$in": input.agent_id}},
        ],
        "level": {"$in": input.level}
    }
    time_exp = get_time_exp(input)
    if time_exp:
        exp_runlog.update({"time": time_exp})

    runlogs = []
    async for runlog in db.runlog.find(exp_runlog, {"_id": 0}):
        runlogs.append(RunLog(**ch_id(runlog)))

    return runlogs


@mutation.field("hello")
async def resolve_write_hello(obj, info):
    return SuccessPayload()


@mutation.field("create_agent")
async def create_agent(obj, info: GraphQLResolveInfo, input: dict):
    request: Request = info.context['request']
    address = request.client.host
    agent = Agent(**input, address=address).dict()
    res = await db.agent.insert_one(ch_id(agent))
    return agent


@mutation.field("delete_agent")
async def delete_agent(obj, info, agent_id: str):
    await db.agent.delete_one({"_id": agent_id})
    return SuccessPayload()


@mutation.field("create_flow")
async def create_flow(obj, info, input: dict):
    flow_input = FlowInput(**input)
    docs_dict = defaultdict(list)
    # store channels, tasks, flows
    for i, task_input in enumerate(flow_input.tasks):
        for j, ch_input in enumerate(task_input.outputs):
            task_input.outputs[j] = ch_input.id
            docs_dict['channel'].append(Channel(**ch_input.dict()))
        flow_input.tasks[i] = task_input.id
        docs_dict['task'].append(Task(**task_input.dict()))
    docs_dict['flow'].append(Flow(**flow_input.dict()))

    for collection, docs in docs_dict.items():
        docs = [ch_id(doc.dict()) for doc in docs]
        await getattr(db, collection).insert_many(docs)

    return docs_dict['flow'][0]


@mutation.field("delete_flow")
async def delete_flow(obj, info, flow_id: str):
    await db.flow.delete_one({"_id": flow_id})
    return SuccessPayload()


@mutation.field("update_flowrun")
async def update_flowrun(obj, info, input: dict):
    flowrun_input = FlowRunInput(**input)
    flowrun = await db.find_one({"_id": flowrun_input.id})
    if flowrun is None:
        flowrun = FlowRun(**flowrun_input.dict())
        await db.insert_one(ch_id(flowrun.dict()))
        return flowrun
    else:
        update_exp = update_notnone_exp(flowrun_input.dict())
        await db.flowrun.update_one(update_exp)
        updated_flowrun = await db.flowrun.find_one({"_id": flowrun_input.id})
        return updated_flowrun


@mutation.field("update_taskrun")
async def update_taskrun(obj, info, input: dict):
    taskrun_input = TaskRunInput(**input)
    taskrun = await db.find_one({"_id": taskrun_input.id})
    if taskrun is None:
        taskrun = TaskRun(**taskrun_input.dict())
        await db.insert_one(ch_id(taskrun.dict()))
        return taskrun
    else:
        update_exp = update_notnone_exp(taskrun_input.dict())
        await db.taskrun.update_one(update_exp)
        updated_taskrun = await db.taskrun.find_one({"_id": taskrun_input.id})
        return updated_taskrun


@mutation.field("write_runlogs")
async def write_runlogs(obj, info, run_logs: Sequence[dict]):
    run_logs = [ch_id(run_log) for run_log in run_logs]
    await db.runlog.insert_many(run_logs)
    return SuccessPayload()


#


@agent.field("flowruns")
@flow.field("flowruns")
async def resolve_flowruns(obj, info):
    cursor = db.flowrun.find({"_id": {"$in": obj.flowruns}})
    flowruns = await cursor.to_list(len(obj.flowruns))
    flowruns = [FlowRun(**ch_id(flowrun)) for flowrun in flowruns]
    return flowruns


@flowrun.field("taskruns")
@task.field("taskruns")
async def resolve_taskruns(obj, info):
    cursor = db.taskrun.find({"_id": {"$in": obj.taskruns}})
    taskruns = await cursor.to_list(len(obj.taskruns))
    taskruns = [TaskRun(**ch_id(taskrun)) for taskrun in taskruns]
    return taskruns


@flow.field("tasks")
async def resolve_tasks(obj, info):
    cursor = db.task.find({"_id": {"$in": obj.tasks}})
    tasks = await cursor.to_list(len(obj.tasks))
    tasks = [Task(**ch_id(task)) for task in tasks]
    return tasks
