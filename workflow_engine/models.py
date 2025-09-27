from pydantic import BaseModel, Field
from typing import List
from enum import Enum

class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

class TaskModel(BaseModel):
    id: str
    dependencies: List[str] = Field(default_factory=list)
    runtime: int = 1
    dynamic: bool = False
    state: TaskState = TaskState.PENDING

class WorkflowModel(BaseModel):
    version: str
    name: str
    tasks: List[TaskModel]