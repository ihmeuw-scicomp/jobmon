from typing import List, Optional

from pydantic import BaseModel


class WorkflowValidationRequest(BaseModel):
    """Request model for workflow validation."""

    task_ids: List[int]


class WorkflowValidationResponse(BaseModel):
    """Response model for workflow validation."""

    validation: bool
    workflow_status: Optional[str] = None


class WorkflowTasksResponse(BaseModel):
    """Response model for workflow tasks."""

    workflow_tasks: str  # JSON string from pandas DataFrame


class WorkflowUserValidationResponse(BaseModel):
    """Response model for workflow user validation."""

    validation: bool


class WorkflowResetRequest(BaseModel):
    """Request model for workflow reset."""

    partial_reset: bool = False


class WorkflowRunForResetResponse(BaseModel):
    """Response model for workflow run reset validation."""

    workflow_run_id: Optional[int]


class WorkflowStatusResponse(BaseModel):
    """Response model for workflow status."""

    workflows: str  # JSON string from pandas DataFrame


class WorkflowStatusVizResponse(BaseModel):
    """Response model for workflow status visualization."""

    # Dictionary mapping workflow_id to status data
    pass  # This will be a Dict[int, Any] but FastAPI handles this automatically


class WorkflowOverviewRequest(BaseModel):
    """Request model for workflow overview filters."""

    user: Optional[str] = None
    tool: Optional[str] = None
    wf_name: Optional[str] = None
    wf_args: Optional[str] = None
    wf_attribute_value: Optional[str] = None
    wf_attribute_key: Optional[str] = None
    wf_id: Optional[str] = None
    date_submitted: Optional[str] = None
    date_submitted_end: Optional[str] = None
    status: Optional[str] = None


class WorkflowOverviewItem(BaseModel):
    """Individual workflow item in overview response."""

    wf_id: int
    wf_name: str
    wf_submitted_date: str
    wf_status_date: str
    wf_args: Optional[str]
    wfr_count: int
    wf_status: str
    wf_tool: str
    # Task status counts
    PENDING: int = 0
    SCHEDULED: int = 0
    RUNNING: int = 0
    DONE: int = 0
    FATAL: int = 0


class WorkflowOverviewResponse(BaseModel):
    """Response model for workflow overview."""

    workflows: List[WorkflowOverviewItem]


class TaskTableItem(BaseModel):
    """Individual task item in task table response."""

    task_id: int
    task_name: str
    task_status: str
    task_command: str
    task_num_attempts: int
    task_status_date: str
    task_max_attempts: int


class TaskTableResponse(BaseModel):
    """Response model for task table visualization."""

    tasks: List[TaskTableItem]


class WorkflowDetailsItem(BaseModel):
    """Workflow details item."""

    wf_name: str
    wf_args: Optional[str]
    wf_created_date: str
    wf_status_date: str
    tool_name: str
    wf_status: (
        str  # Changed from int to str - database returns string status codes like 'D'
    )
    wf_status_desc: str
    wfr_jobmon_version: Optional[str]
    wfr_heartbeat_date: Optional[str]
    wfr_user: str


class WorkflowDetailsResponse(BaseModel):
    """Response model for workflow details."""

    pass  # This will be a List[WorkflowDetailsItem] but FastAPI handles this automatically
