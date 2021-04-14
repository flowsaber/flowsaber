from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from ..models import LogLevel


class Model(BaseModel):
    pass


class Agent(Model):
    id: str
    name: str
    labels: List[str]
    address: str
    flowruns: List[str] = Field(default_factory=list)


class Channel(Model):
    id: str
    task_id: str
    flow_id: str


class Edge(Model):
    channel_id: str
    task_id: str


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
    taskruns: List[str] = Field(default_factory=list)


class Flow(Model):
    id: str
    name: str
    labels: List[str]
    config: dict
    source_code: str
    serialized_flow: str
    tasks: List[str]
    edges: List[Edge]
    flowruns: List[str] = Field(default_factory=list)


class State(Model):
    state_tpye: str
    inputs: dict = None
    result: str = None
    context: dict = None
    message: str = None


class TaskRun(Model):
    id: str
    task_id: str
    flow_id: str
    agent_id: str
    flowrun_id: str
    state: State
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: datetime = None
    last_heatbeat: datetime = Field(default_factory=datetime.utcnow)


class FlowRun(Model):
    id: str
    flow_id: str
    agent_id: str
    name: str
    labels: List[str]
    state: State
    start_time: datetime
    end_time: datetime = None
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    taskruns: List[str] = Field(default_factory=list)


class RunLog(Model):
    id: str
    level: LogLevel
    time: datetime
    taskrun_id: str
    flowrun_id: str
    agent_id: str
    message: str
    info: dict
