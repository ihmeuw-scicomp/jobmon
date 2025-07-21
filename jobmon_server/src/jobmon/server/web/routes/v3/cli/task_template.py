"""Routes for TaskTemplate."""

from http import HTTPStatus as StatusCodes
from typing import Any, Optional

import structlog
from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.server.web.db import get_dialect_name, get_sessionmaker
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.repositories.task_template_repository import (
    TaskTemplateRepository,
)
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.schemas.task_template import (
    TaskResourceVizItem,
    TaskTemplateResourceUsageRequest,
    TaskTemplateResourceUsageResponse,
)

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)
SessionMaker = get_sessionmaker()
DIALECT = get_dialect_name()


@api_v3_router.get("/get_task_template_details")
def get_task_template_details_for_workflow(
    workflow_id: int = Query(..., ge=1),
    task_template_id: int = Query(..., ge=1),
    db: Session = Depends(get_db),
) -> Any:
    """Fetch Task Template details (ID, Name, and Version) for a given Workflow."""
    tt_repo = TaskTemplateRepository(db)
    tt_details_data = tt_repo.get_task_template_details(workflow_id, task_template_id)

    if tt_details_data is None:
        raise HTTPException(
            status_code=404,
            detail="Task Template not found for the given workflow.",
        )

    return tt_details_data


@api_v3_router.get("/get_task_template_version")
def get_task_template_version_for_tasks(
    task_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    db: Session = Depends(get_db),
) -> Any:
    """Get the task_template_version_ids using repository pattern."""
    tt_repo = TaskTemplateRepository(db)
    result = tt_repo.get_task_template_versions(
        task_id=task_id, workflow_id=workflow_id
    )

    if result is None:
        return JSONResponse(
            content={"task_template_version_ids": []}, status_code=StatusCodes.OK
        )

    return result  # FastAPI will automatically serialize the Pydantic model


@api_v3_router.get("/get_requested_cores")
def get_requested_cores(
    task_template_version_ids: Optional[str] = None, db: Session = Depends(get_db)
) -> Any:
    """Get the min, max, and avg of requested cores."""
    if task_template_version_ids is None:
        raise ValueError(
            "No task_template_version_ids returned in /get_requested_cores"
        )

    # Parse the task template version IDs from string format "(1,2,3)"
    ttvis = [int(i) for i in task_template_version_ids[1:-1].split(",")]

    tt_repo = TaskTemplateRepository(db)
    result = tt_repo.get_requested_cores(task_template_version_ids=ttvis)

    return result


@api_v3_router.get("/get_most_popular_queue")
def get_most_popular_queue(
    task_template_version_ids: Optional[str] = Query(...), db: Session = Depends(get_db)
) -> Any:
    """Get the most popular queue of the task template."""
    if task_template_version_ids is None:
        raise ValueError(
            "No task_template_version_ids returned in /get_most_popular_queue."
        )

    # Parse the task template version IDs from string format "(1,2,3)"
    ttvis = [int(i) for i in task_template_version_ids[1:-1].split(",")]

    tt_repo = TaskTemplateRepository(db)
    result = tt_repo.get_most_popular_queue(task_template_version_ids=ttvis)

    return result


@api_v3_router.post(
    "/task_template_resource_usage", response_model=TaskTemplateResourceUsageResponse
)
async def get_task_template_resource_usage(
    request_data: TaskTemplateResourceUsageRequest, db: Session = Depends(get_db)
) -> TaskTemplateResourceUsageResponse:
    """Unified endpoint for task template resource usage.

    Returns modern Pydantic models suitable for both GUI frontend
    and Python client consumption with full type safety.
    """
    repo = TaskTemplateRepository(db)

    try:
        # Get task details using the repository
        task_details = repo.get_task_resource_details(
            task_template_version_id=request_data.task_template_version_id,
            workflows=request_data.workflows,
            node_args=request_data.node_args,
        )

        # Calculate statistics using repository method
        stats = repo.calculate_resource_statistics(
            task_details=task_details,
            confidence_interval=request_data.ci,
            task_template_version_id=request_data.task_template_version_id,
        )

        # Prepare viz data if requested
        viz_data = None
        if request_data.viz and task_details:
            viz_data = []
            for detail_item in task_details:
                viz_data.append(
                    TaskResourceVizItem(
                        r=detail_item.r,
                        m=detail_item.m,
                        node_id=detail_item.node_id,
                        task_id=detail_item.task_id,
                        requested_resources=detail_item.requested_resources,
                        attempt_number_of_instance=detail_item.attempt_number_of_instance,
                        status=detail_item.status,
                    )
                )

        # Return unified Pydantic response
        return TaskTemplateResourceUsageResponse(
            num_tasks=stats.num_tasks,
            min_mem=stats.min_mem,
            max_mem=stats.max_mem,
            mean_mem=stats.mean_mem,
            min_runtime=stats.min_runtime,
            max_runtime=stats.max_runtime,
            mean_runtime=stats.mean_runtime,
            median_mem=stats.median_mem,
            median_runtime=stats.median_runtime,
            ci_mem=stats.ci_mem,
            ci_runtime=stats.ci_runtime,
            result_viz=viz_data,
        )

    except Exception as e:
        logger.error(f"Error fetching resource usage: {e}")
        raise HTTPException(
            status_code=StatusCodes.INTERNAL_SERVER_ERROR,
            detail="Error processing resource usage data.",
        ) from e


@api_v3_router.get("/workflow_tt_status_viz/{workflow_id}")
def get_workflow_tt_status_viz(workflow_id: int, db: Session = Depends(get_db)) -> Any:
    """Get the status of the workflows for GUI."""
    tt_repo = TaskTemplateRepository(db)
    result_dict = tt_repo.get_workflow_tt_status_viz(
        workflow_id=workflow_id, dialect=DIALECT
    )

    # Convert Pydantic models back to serializable dict format for JSONResponse
    result_dict_serializable = {
        key: val.model_dump() for key, val in result_dict.items()
    }

    return JSONResponse(content=result_dict_serializable, status_code=StatusCodes.OK)


@api_v3_router.get("/tt_error_log_viz/{wf_id}/{tt_id}")
@api_v3_router.get("/tt_error_log_viz/{wf_id}/{tt_id}/{ti_id}")
def get_tt_error_log_viz(
    wf_id: int,
    tt_id: Optional[int] = None,
    ti_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 10,
    just_recent_errors: str = "false",
    cluster_errors: str = "false",
    db: Session = Depends(get_db),
) -> Any:
    """Get the error logs for a task template id for GUI."""
    recent_errors = just_recent_errors.lower() == "true"
    output_clustered_errors = cluster_errors.lower() == "true"

    if tt_id is None:
        raise ValueError("Task template ID is required")

    tt_repo = TaskTemplateRepository(db)
    result = tt_repo.get_tt_error_log_viz(
        workflow_id=wf_id,
        task_template_id=tt_id,
        task_instance_id=ti_id,
        page=page,
        page_size=page_size,
        recent_errors_only=recent_errors,
        cluster_errors=output_clustered_errors,
    )

    return result
