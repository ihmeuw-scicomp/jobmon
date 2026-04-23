from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, computed_field


class TaskTemplateResourceUsageRequest(BaseModel):
    task_template_version_id: int
    workflows: Optional[List[int]] = None
    node_args: Optional[Dict[str, List[str]]] = None
    ci: Optional[str] = None
    viz: bool = False
    page: Optional[int] = Field(default=None, ge=0)
    page_size: Optional[int] = Field(default=None, ge=1, le=5000)


class TaskTemplateResourceAggregatesRequest(BaseModel):
    """Request for the lightweight aggregates endpoint."""

    task_template_version_id: int
    workflows: Optional[List[int]] = None
    node_args: Optional[Dict[str, List[str]]] = None
    # When True (default, matches the UI's default "latest only" toggle),
    # aggregate only over the latest TaskInstance per Task. When False,
    # aggregate across all attempts — useful for diagnosing retry waste.
    latest_only: bool = True


class ResourceClusterItem(BaseModel):
    """One requested-resource cluster summary.

    We ship only the numeric ``(runtime, memory)`` pair plus the count;
    the frontend derives the string cluster key itself using the same
    rounding/formatting it applies to scatter rows, so there's no
    cross-language string contract that can drift between Python and JS.

    Cluster → instance membership is also recomputed frontend-side from
    streaming scatter rows — shipping per-instance IDs here would
    dominate the aggregates payload on large templates.
    """

    runtime: float
    memory: float
    task_count: int


class ResourceEfficiencyMetrics(BaseModel):
    """Aggregate resource efficiency metrics for a task template."""

    memory_utilization: float = 0.0
    runtime_utilization: float = 0.0
    over_allocated_memory: int = 0
    under_allocated_memory: int = 0
    over_allocated_runtime: int = 0
    under_allocated_runtime: int = 0
    p95_memory: Optional[float] = None
    p95_runtime: Optional[float] = None
    outlier_count: int = 0


class TaskTemplateResourceAggregatesResponse(BaseModel):
    """Compact server-computed aggregates for a task template.

    Memory and runtime stats are ``Optional[float]`` (not ``int``) so
    the client's per-field merge doesn't permanently overwrite more
    precise client-computed floats with a truncated integer.
    ``efficiency`` defaults to ``None`` when the template has no
    efficiency-relevant data (zero-TI workflow, or TIs without
    requested resources) so the frontend can distinguish "no server
    data" from "server says all zeros".
    """

    num_tasks: Optional[int] = None
    min_mem: Optional[float] = None
    max_mem: Optional[float] = None
    mean_mem: Optional[float] = None
    median_mem: Optional[float] = None
    p95_mem: Optional[float] = None
    min_runtime: Optional[float] = None
    max_runtime: Optional[float] = None
    mean_runtime: Optional[float] = None
    median_runtime: Optional[float] = None
    p95_runtime: Optional[float] = None
    median_requested_runtime: Optional[float] = None
    median_requested_memory: Optional[float] = None
    resource_clusters: List[ResourceClusterItem] = []
    efficiency: Optional[ResourceEfficiencyMetrics] = None


class RequestedResourcesModel(BaseModel):  # Optional: For parsing the JSON string
    memory: Optional[float] = None
    runtime: Optional[float] = None
    cores: Optional[float] = None
    queue: Optional[str] = None
    # Add other fields that might be in the requested_resources JSON


class TaskResourceDetailItem(BaseModel):
    r: Optional[float] = Field(default=None, alias="wallclock")
    m: Optional[int] = Field(default=None, alias="maxrss")
    node_id: int
    task_id: int
    task_instance_id: Optional[int] = None
    task_name: Optional[str] = None
    requested_resources: Optional[str] = None  # Raw JSON string from DB
    task_resources_id: Optional[int] = None
    attempt_number_of_instance: Optional[int] = None  # Added field
    status: Optional[str] = None  # Added field: Will hold 'D', 'F', etc.
    task_status_date: Optional[datetime] = None
    task_command: Optional[str] = None
    task_num_attempts: Optional[int] = None
    task_max_attempts: Optional[int] = None
    workflow_run_id: Optional[int] = None
    model_config = ConfigDict(populate_by_name=True)


class TaskResourceVizItem(BaseModel):
    r: Optional[float] = None
    m: Optional[int] = None
    node_id: int
    task_id: int
    task_instance_id: Optional[int] = None
    task_name: Optional[str] = None
    requested_resources: Optional[str] = None
    task_resources_id: Optional[int] = None
    attempt_number_of_instance: Optional[int] = None
    status: Optional[str] = None
    task_status_date: Optional[datetime] = None
    task_command: Optional[str] = None
    task_num_attempts: Optional[int] = None
    task_max_attempts: Optional[int] = None
    workflow_run_id: Optional[int] = None


class FormattedStats(BaseModel):
    """Formatted statistics for legacy client compatibility."""

    num_tasks: Optional[int] = None
    min_mem: Optional[str] = None
    max_mem: Optional[str] = None
    mean_mem: Optional[str] = None
    min_runtime: Optional[int] = None
    max_runtime: Optional[int] = None
    mean_runtime: Optional[float] = None
    median_mem: Optional[str] = None
    median_runtime: Optional[float] = None
    ci_mem: Optional[List[Union[float, None]]] = None
    ci_runtime: Optional[List[Union[float, None]]] = None


class TaskTemplateResourceUsageResponse(BaseModel):
    """Unified response model for task template resource usage."""

    model_config = ConfigDict(populate_by_name=True)

    # Core statistics (what both GUI and client need)
    num_tasks: Optional[int] = None
    min_mem: Optional[int] = None  # bytes
    max_mem: Optional[int] = None  # bytes
    mean_mem: Optional[float] = None  # bytes
    min_runtime: Optional[int] = None  # seconds
    max_runtime: Optional[int] = None  # seconds
    mean_runtime: Optional[float] = None  # seconds
    median_mem: Optional[float] = None  # bytes
    median_runtime: Optional[float] = None  # seconds

    # Confidence intervals (can be null for small datasets)
    ci_mem: Optional[List[Union[float, None]]] = None
    ci_runtime: Optional[List[Union[float, None]]] = None

    # Visualization data (optional, only when viz=True)
    result_viz: Optional[List[TaskResourceVizItem]] = None

    # Pagination metadata (only populated when request supplied page/page_size)
    total_count: Optional[int] = None
    page: Optional[int] = None
    page_size: Optional[int] = None

    # Computed properties for client convenience
    @computed_field
    def formatted_stats(self) -> FormattedStats:
        """Provide formatted statistics similar to legacy client format."""
        return FormattedStats(
            num_tasks=self.num_tasks,
            min_mem=f"{self.min_mem}B" if self.min_mem is not None else None,
            max_mem=f"{self.max_mem}B" if self.max_mem is not None else None,
            mean_mem=f"{self.mean_mem}B" if self.mean_mem is not None else None,
            min_runtime=self.min_runtime,
            max_runtime=self.max_runtime,
            mean_runtime=self.mean_runtime,
            median_mem=(f"{self.median_mem}B" if self.median_mem is not None else None),
            median_runtime=self.median_runtime,
            ci_mem=self.ci_mem,
            ci_runtime=self.ci_runtime,
        )


class TaskTemplateDetailsResponse(BaseModel):
    task_template_id: int
    task_template_name: str
    task_template_version_id: int


class TaskTemplateVersionItem(BaseModel):
    """Individual task template version item."""

    id: int
    name: str


class TaskTemplateVersionResponse(BaseModel):
    """Response model for task template version queries."""

    task_template_version_ids: List[TaskTemplateVersionItem]


class CoreInfoItem(BaseModel):
    """Individual core info item."""

    id: int
    min: int
    max: int
    avg: int


class RequestedCoresResponse(BaseModel):
    """Response model for requested cores queries."""

    core_info: List[CoreInfoItem]


class QueueInfoItem(BaseModel):
    """Individual queue info item."""

    id: int
    queue: str
    queue_id: int


class MostPopularQueueResponse(BaseModel):
    """Response model for most popular queue queries."""

    queue_info: List[QueueInfoItem]


class WorkflowTaskTemplateStatusItem(BaseModel):
    """Individual workflow task template status item."""

    id: int
    name: str
    tasks: int
    PENDING: int
    SCHEDULED: int
    RUNNING: int
    DONE: int
    FATAL: int
    MAXC: Union[int, str]  # Can be int or "NA"
    num_attempts_min: Optional[float]
    num_attempts_max: Optional[float]
    num_attempts_avg: Optional[float]
    task_template_version_id: int


class FatalErrorBreakdownResponse(BaseModel):
    """Breakdown of fatal errors by type for a single template."""

    resource: int = 0
    app: int = 0
    infra: int = 0
    resource_error_total: int = 0
    resource_error_retried: int = 0
    resource_error_ti_ids: List[int] = []


class WorkflowResourceErrorCheckResponse(BaseModel):
    """Response for checking if a workflow has any resource errors."""

    has_resource_errors: bool


class ErrorLogItem(BaseModel):
    """Error log item - can represent individual errors or clustered errors."""

    # Individual error fields (required for non-clustered, optional for clustered)
    task_id: Optional[int] = None
    task_instance_id: Optional[int] = None
    task_instance_err_id: Optional[int] = None
    error_time: Optional[datetime] = None
    error: Optional[str] = None
    task_instance_stderr_log: Optional[str] = None

    # Common fields (always present)
    workflow_run_id: int
    workflow_id: int

    # Clustering fields (only present when clustering is enabled)
    error_score: Optional[float] = None
    group_instance_count: Optional[int] = None
    task_instance_ids: Optional[List[int]] = None
    task_ids: Optional[List[int]] = None
    sample_error: Optional[str] = None
    first_error_time: Optional[datetime] = None

    model_config = ConfigDict(
        # This will serialize datetime objects to ISO strings in JSON responses
        json_encoders={datetime: lambda v: v.isoformat()}
    )


class ErrorLogResponse(BaseModel):
    """Response model for error log queries."""

    error_logs: List[ErrorLogItem]
    total_count: int
    page: int
    page_size: int
