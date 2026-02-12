from typing import List

from pydantic import BaseModel


class ArrayTaskInstance(BaseModel):
    TASK_ID: int
    TASK_NAME: str
    ARRAY_NAME: str
    TASK_INSTANCE_ID: int
    OUTPUT_PATH: str
    ERROR_PATH: str


class ArrayTasksResponse(BaseModel):
    array_tasks: List[ArrayTaskInstance]
