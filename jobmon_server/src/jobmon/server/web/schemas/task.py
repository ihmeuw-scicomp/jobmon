from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class TaskStatusRequest(BaseModel):
    """Request model for task status queries."""

    task_ids: Optional[Union[int, List[int]]] = None
    status: Optional[Union[str, List[str]]] = None


class TaskStatusItem(BaseModel):
    """Individual task status item."""

    TASK_ID: int
    task_status: str
    TASK_INSTANCE_ID: int
    DISTRIBUTOR_ID: Optional[str]
    STATUS: str
    RESOURCE_USAGE: Optional[str]
    STDOUT: Optional[str]
    STDERR: Optional[str]
    ERROR_TRACE: Optional[str]


class TaskStatusResponse(BaseModel):
    """Response model for task status."""

    task_instance_status: str  # JSON string from pandas DataFrame


class TaskSubdagRequest(BaseModel):
    """Request model for task subdag."""

    task_ids: List[int]
    task_status: Optional[List[str]] = []


class TaskSubdagResponse(BaseModel):
    """Response model for task subdag."""

    workflow_id: Optional[int]
    sub_task: Optional[Dict[int, Any]]  # Changed from str to int - keys are task IDs


class TaskDependencyItem(BaseModel):
    """Individual task dependency item."""

    id: int
    status: str
    name: str


class TaskDependenciesResponse(BaseModel):
    """Response model for task dependencies."""

    up: List[List[TaskDependencyItem]]
    down: List[List[TaskDependencyItem]]


class TasksRecursiveRequest(BaseModel):
    """Request model for recursive tasks."""

    task_ids: List[int]


class TasksRecursiveResponse(BaseModel):
    """Response model for recursive tasks."""

    task_ids: List[int]


class TaskResourceUsageResponse(BaseModel):
    """Response model for task resource usage."""

    resource_usage: List[Any]  # tuple from SerializeTaskResourceUsage.to_wire()


class DownstreamTasksRequest(BaseModel):
    """Request model for downstream tasks."""

    task_ids: List[int]
    dag_id: int


class DownstreamTasksResponse(BaseModel):
    """Response model for downstream tasks."""

    downstream_tasks: Dict[int, List[Any]]


class TaskInstanceDetailItem(BaseModel):
    """Individual task instance detail item."""

    ti_id: int
    ti_status: str
    ti_stdout: Optional[str]
    ti_stderr: Optional[str]
    ti_stdout_log: Optional[str]
    ti_stderr_log: Optional[str]
    ti_distributor_id: Optional[str]
    ti_nodename: Optional[str]
    ti_error_log_description: Optional[str]
    ti_wallclock: Optional[float]
    ti_maxrss: Optional[float]
    ti_resources: Optional[str]
    ti_submit_date: Optional[str]
    ti_status_date: Optional[str]
    ti_queue_name: Optional[str]


class TaskInstanceDetailsResponse(BaseModel):
    """Response model for task instance details."""

    taskinstances: List[TaskInstanceDetailItem]


class TaskDetailItem(BaseModel):
    """Individual task detail item."""

    task_status: str
    workflow_id: int
    task_name: str
    task_command: str
    task_status_date: str
    task_template_id: int


class TaskDetailsResponse(BaseModel):
    """Response model for task details."""

    task_details: List[TaskDetailItem]
