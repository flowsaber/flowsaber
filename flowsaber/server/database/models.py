import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Model(BaseModel):
    pass


class SuccessPayload(Model):
    success: bool = True
    error: str = None


class IdsPaload(Model):
    id: List[str] = Field(default_factory=list)


class RunLog(Model):
    id: str
    level: str
    time: datetime
    task_id: str
    flow_id: str
    taskrun_id: str
    flowrun_id: str
    agent_id: str
    message: str


class RunLogInput(Model):
    id: str = Field(default_factory=uuid.uuid4)
    level: str
    time: datetime = Field(default_factory=datetime.utcnow)
    task_id: str = None
    flow_id: str = None
    taskrun_id: str = None
    flowrun_id: str = None
    agent_id: str = None
    message: str = ""


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
    labels: List[str] = Field(default_factory=list)


class Channel(Model):
    id: str
    task_id: str
    flow_id: str


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


class State(Model):
    state_type: str
    result: str = None
    message: str = None


class StateInput(Model):
    state_type: str
    result: str = None
    message: str = None


class Task(Model):
    id: str
    flow_id: str
    name: str
    config: dict
    input_signature: dict
    output_signature: str
    outputs: List[str]
    source_code: str
    command: str = None


class Flow(Model):
    id: str
    name: str
    full_name: str
    labels: List[str]
    tasks: List[str]
    edges: List[Edge]
    docstring: str
    serialized_flow: str
    context: dict
    flowruns: List[str] = Field(default_factory=list)


class TaskInput(Model):
    id: str
    flow_id: str
    name: str
    full_name: str
    labels: List[str]
    output: List[ChannelInput]
    docstring: str
    context: dict


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


class RunInput(Model):
    pass


class TaskRun(Model):
    id: str
    task_id: str
    flow_id: str
    flowrun_id: str
    agent_id: str
    inputs: dict
    context: dict
    state: State
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: datetime = None
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)


class TaskRunInput(RunInput):
    id: str
    task_id: str = None
    flow_id: str = None
    agent_id: str = None
    flowrun_id: str = None
    inputs: dict = None
    context: dict = None
    state: StateInput = None


class GetTaskRunsInput(Model):
    taskrun_id: List[str] = Field(default_factory=list)
    task_id: List[str] = Field(default_factory=list)
    flow_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    flowrun_id: List[str] = Field(default_factory=list)
    state_type: List[str] = Field(default_factory=list)
    after: datetime = None
    before: datetime = None


class FlowRun(Model):
    id: str
    flow_id: str
    agent_id: str
    name: str
    labels: List[str]
    inputs: dict
    context: dict
    state: State
    start_time: datetime
    end_time: datetime = None
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    taskruns: List[str] = Field(default_factory=list)


class FlowRunInput(RunInput):
    id: str
    flow_id: str = None
    agent_id: str = None
    name: str = None
    labels: List[str] = None
    inputs: dict = None
    context: dict = None
    state: StateInput = None


class GetFlowRunsInput(Model):
    flowrun_id: List[str] = Field(default_factory=list)
    flow_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    name: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)
    state_type: List[str] = Field(default_factory=list)
    after: datetime = None
    before: datetime = None
