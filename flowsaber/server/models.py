import enum
from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class LogLevel(enum.IntEnum):
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


class SuccessPayload(BaseModel):
    success: bool = True
    error: str = None


class IdsPaload(BaseModel):
    id: List[str] = Field(default_factory=list)


class RunLogInput(BaseModel):
    id: str
    level: LogLevel
    time: datetime = Field(default_factory=datetime.utcnow)
    taskrun_id: str = None
    flowrun_id: str = None
    agent_id: str = None
    message: str = ""
    info: dict = Field(default_factory=dict)


class GetRunLogsInput(BaseModel):
    id: List[str] = Field(default_factory=list)
    taskrun_id: List[str] = Field(default_factory=list)
    flowrun_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    level: List[LogLevel] = Field(default_factory=list)
    before: datetime = None
    after: datetime = None


class AgentInput(BaseModel):
    id: str
    name: str
    labels: List[str] = Field(default_factory=list)


class EdgeInput(BaseModel):
    channel_id: str
    task_id: str


class ChannelInput(BaseModel):
    id: str
    task_id: str
    flow_id: str


class TaskInput(BaseModel):
    id: str
    flow_id: str
    name: str
    config: dict
    input_signature: dict
    output_signature: str
    outputs: List[ChannelInput]
    source_code: str
    command: str = ""


class FlowInput(BaseModel):
    id: str
    name: str
    labels: List[str]
    config: dict
    source_code: str
    serialized_flow: str
    tasks: List[TaskInput]
    edges: List[EdgeInput]


class GetFlowsInput(BaseModel):
    id: List[str] = Field(default_factory=list)
    name: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)


class StateInput(BaseModel):
    state_type: str
    inputs: dict = None
    result: str = None
    context: dict = None
    message: str = None


class TaskRunInput(BaseModel):
    id: str
    task_id: str = None
    flow_id: str = None
    agent_id: str = None
    flowrun_id: str = None
    state: StateInput = None


class FlowRunInput(BaseModel):
    id: str
    flow_id: str = None
    agent_id: str = None
    name: str = None
    labels: List[str] = None
    state: StateInput = None


class GetFlowRunsInput(BaseModel):
    flowrun_id: List[str] = Field(default_factory=list)
    flow_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    name: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)
    state_type: List[str] = Field(default_factory=list)
    after: datetime = None
    before: datetime = None


class GetTaskRunsInput(BaseModel):
    taskrun_id: List[str] = Field(default_factory=list)
    task_id: List[str] = Field(default_factory=list)
    flow_id: List[str] = Field(default_factory=list)
    agent_id: List[str] = Field(default_factory=list)
    flowrun_id: List[str] = Field(default_factory=list)
    state_type: List[str] = Field(default_factory=list)
    after: datetime = None
    before: datetime = None
