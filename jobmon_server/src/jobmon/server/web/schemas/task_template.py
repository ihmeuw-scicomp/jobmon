from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, computed_field


class TaskTemplateResourceUsageRequest(BaseModel):
    task_template_version_id: int
    workflows: Optional[List[int]] = None
    node_args: Optional[Dict[str, List[str]]] = None
    ci: Optional[str] = None
    viz: bool = False


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
    task_name: Optional[str] = None
    requested_resources: Optional[str] = None  # Raw JSON string from DB
    attempt_number_of_instance: Optional[int] = None  # Added field
    status: Optional[str] = None  # Added field: Will hold 'D', 'F', etc.
    model_config = ConfigDict(populate_by_name=True)


class TaskResourceVizItem(BaseModel):
    r: Optional[float] = None
    m: Optional[int] = None
    node_id: int
    task_id: int
    task_name: Optional[str] = None
    requested_resources: Optional[str] = None
    attempt_number_of_instance: Optional[int] = None
    status: Optional[str] = None


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

    # Computed properties for client convenience
    @computed_field
    def formatted_stats(self) -> Dict[str, Any]:
        """Provide formatted statistics similar to legacy client format."""
        return {
            "num_tasks": self.num_tasks,
            "min_mem": f"{self.min_mem}B" if self.min_mem is not None else None,
            "max_mem": f"{self.max_mem}B" if self.max_mem is not None else None,
            "mean_mem": f"{self.mean_mem}B" if self.mean_mem is not None else None,
            "min_runtime": self.min_runtime,
            "max_runtime": self.max_runtime,
            "mean_runtime": self.mean_runtime,
            "median_mem": (
                f"{self.median_mem}B" if self.median_mem is not None else None
            ),
            "median_runtime": self.median_runtime,
            "ci_mem": self.ci_mem,
            "ci_runtime": self.ci_runtime,
        }


class TaskTemplateDetailsResponse(BaseModel):
    task_template_id: int
    task_template_name: str
    task_template_version_id: int