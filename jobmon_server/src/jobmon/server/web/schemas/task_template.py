from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


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
    requested_resources: Optional[str] = None  # Raw JSON string from DB
    attempt_number_of_instance: Optional[int] = None  # Added field
    status: Optional[str] = None  # Added field: Will hold 'D', 'F', etc.

    # Example for parsing if you choose to send requested_resources_obj
    # @validator('requested_resources_obj', pre=True, always=True)
    # def parse_requested_resources(cls, v, values):
    #     if values.get('requested_resources'):
    #         try:
    #             return RequestedResourcesModel(**json.loads(values['requested_resources']))
    #         except json.JSONDecodeError:
    #             return None
    #     return None

    model_config = ConfigDict(populate_by_name=True)


class TaskResourceVizItem(BaseModel):
    r: Optional[float] = None
    m: Optional[int] = None
    node_id: int
    task_id: int
    requested_resources: Optional[str] = None
    attempt_number_of_instance: Optional[int] = None  # Added field
    status: Optional[str] = None  # Added field: Will hold 'D', 'F', etc.


class TaskTemplateResourceUsageResponse(BaseModel):
    # This field will only be present if viz=True in the request
    result_viz: Optional[List[TaskResourceVizItem]] = None
