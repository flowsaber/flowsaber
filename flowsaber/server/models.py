from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RunLog(BaseModel):
    taskrun_id: str
    flowrun_id: str
    level: Optional[int]
    time: datetime = Field(default_factory=datetime.utcnow)
    message: Optional[str]
    info: Optional[str]
