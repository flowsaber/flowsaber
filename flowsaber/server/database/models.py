import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Model(BaseModel):
    pass


class SuccessPayload(Model):
    success: bool = True
    error: str = None


class IdsPayload(Model):
    id: List[str] = Field(default_factory=list)


class RunLog(Model):
    id: str
    level: str
    time: datetime
    message: str
    task_id: str = None
    flow_id: str = None
    taskrun_id: str = None
    flowrun_id: str = None
    agent_id: str = None


class RunLogInput(Model):
    id: str = Field(default_factory=uuid.uuid4)
    level: str
    time: datetime = Field(default_factory=datetime.utcnow)
    message: str = ""
    task_id: str = None
    flow_id: str = None
    taskrun_id: str = None
    flowrun_id: str = None
    agent_id: str = None


class RunLogsInput(Model):
    logs: List[RunLogInput] = Field(default_factory=list)


class GetRunLogsInput(Model):
    id: List[str] = Field(default_factory=list)
    taskrun_id: List[str] = Field(default_factory=list)
    flowrun_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    level: List[str] = Field(default_factory=list)
    before: datetime = None
    after: datetime = None


class Agent(Model):
    id: str
    name: str
    labels: List[str]
    address: str
    flowruns: List[str] = Field(default_factory=list)


class AgentInput(Model):
    id: str
    name: str
    labels: List[str]


class Channel(Model):
    id: str
    task_id: str = None
    flow_id: str = None


class ChannelInput(Model):
    id: str
    task_id: Optional[str]
    flow_id: Optional[str]


class Edge(Model):
    channel_id: str
    task_id: str


class EdgeInput(Model):
    channel_id: str
    task_id: str


class Task(Model):
    id: str
    name: str
    full_name: str
    labels: List[str]
    flow_id: str
    output: List[str]  # in database, it stores id
    docstring: str
    context: dict


class TaskInput(Model):
    id: str
    name: str
    full_name: str
    labels: List[str]
    flow_id: str
    output: List[ChannelInput]
    docstring: str
    context: dict


class Flow(Model):
    id: str
    name: str
    full_name: str
    labels: List[str]
    tasks: List[str]  # in db, store id
    edges: List[Edge]
    docstring: str
    serialized_flow: str
    context: dict
    flowruns: List[str]


class FlowInput(Model):
    id: str
    name: str
    full_name: str
    labels: List[str]
    tasks: List[TaskInput]
    edges: List[EdgeInput]
    docstring: str
    context: dict
    serialized_flow: str


class GetFlowsInput(Model):
    id: List[str] = Field(default_factory=list)
    name: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)


class State(Model):
    state_type: str
    result: str = None
    message: str = None


class StateInput(Model):
    state_type: str
    result: str = None
    message: str = None


class RunInput(Model):
    pass


class TaskRun(Model):
    id: str
    flowrun_id: str
    agent_id: str
    task_id: str
    flow_id: str
    context: dict
    state: State
    start_time: datetime = None
    end_time: datetime = None
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)


class TaskRunInput(RunInput):
    id: str
    flowrun_id: str = None
    agent_id: str = None
    task_id: str = None
    flow_id: str = None
    context: dict = None
    state: StateInput = None


class GetTaskRunsInput(Model):
    id: List[str] = Field(default_factory=list)
    flowrun_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    task_id: List[str] = Field(default_factory=list)
    flow_id: List[str] = Field(default_factory=list)
    state_type: List[str] = Field(default_factory=list)
    after: datetime = None
    before: datetime = None


class FlowRun(Model):
    id: str
    agent_id: str = None
    flow_id: str
    name: str
    labels: List[str]
    context: dict
    state: State
    start_time: datetime = None
    end_time: datetime = None
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    taskruns: List[str] = Field(default_factory=list)  # db store id


class FlowRunInput(RunInput):
    id: str
    agent_id: str = None
    flow_id: str = None
    name: str = None
    labels: List[str] = None
    context: dict = None
    state: StateInput = None


class GetFlowRunsInput(Model):
    id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    flow_id: List[str] = Field(default_factory=list)
    name: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)
    state_type: List[str] = Field(default_factory=list)
    after: datetime = None
    before: datetime = None
