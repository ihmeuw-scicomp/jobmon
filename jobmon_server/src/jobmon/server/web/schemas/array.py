from typing import List

from pydantic import BaseModel


class ArrayTaskInstance(BaseModel):
    task_id: int
    task_name: str
    array_name: str
    task_instance_id: int
    OUTPUT_PATH: str
    ERROR_PATH: str


class ArrayTasksResponse(BaseModel):
    array_tasks: List[ArrayTaskInstance]
