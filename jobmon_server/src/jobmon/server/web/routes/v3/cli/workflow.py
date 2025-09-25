"""Routes for Workflow."""

from http import HTTPStatus as StatusCodes
from typing import Any, List, Optional, Union

import structlog
from fastapi import Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.task import Task
from jobmon.server.web.repositories.workflow_repository import WorkflowRepository
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.schemas.workflow import (
    TaskTableResponse,
    WorkflowDetailsItem,
    WorkflowOverviewResponse,
    WorkflowRunForResetResponse,
    WorkflowStatusResponse,
    WorkflowTasksResponse,
    WorkflowUserValidationResponse,
    WorkflowValidationResponse,
)

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


@api_v3_router.post("/workflow_validation")
async def get_workflow_validation_status(
    request: Request, db: Session = Depends(get_db)
) -> WorkflowValidationResponse:
    """Check if workflow is valid."""
    data = await request.json()
    task_ids = data["task_ids"]

    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_validation_status(task_ids)


def _check_downstream_tasks_status(
    session: Session, task_ids: List[int], workflow_id: int, dag_id: int
) -> bool:
    """Check if all downstream tasks are in valid states (G, I, Q).

    Args:
        session: Database session
        task_ids: List of task IDs to check downstream tasks for
        workflow_id: Workflow ID
        dag_id: DAG ID

    Returns:
        True if all downstream tasks are in valid states, False otherwise
    """
    # Valid downstream task states
    valid_states = {"G", "I", "Q"}

    # Get downstream node_ids for each task using the same pattern as get_downstream_tasks
    tasks_and_edges = session.execute(
        select(Task.id, Task.node_id, Edge.downstream_node_ids).where(
            Task.id.in_(task_ids),
            Task.node_id == Edge.node_id,
            Edge.dag_id == dag_id,
        )
    ).all()

    # Collect all downstream node_ids
    downstream_node_ids = set()
    for row in tasks_and_edges:
        if row.downstream_node_ids is not None:
            downstreams = row.downstream_node_ids
            if downstreams:
                downstream_node_ids.update(downstreams)

    if not downstream_node_ids:
        return True  # No downstream tasks, consider valid

    # Get task statuses for downstream nodes
    downstream_status_rows = session.execute(
        select(Task.status).where(
            Task.workflow_id == workflow_id, Task.node_id.in_(list(downstream_node_ids))
        )
    ).all()

    for status_row in downstream_status_rows:
        if status_row[0] not in valid_states:
            return False  # Found a downstream task not in valid state

    return True


@api_v3_router.get("/workflow/{workflow_id}/workflow_tasks")
def get_workflow_tasks(
    workflow_id: int,
    limit: int,
    status: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
) -> WorkflowTasksResponse:
    """Get the tasks for a given workflow."""
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_tasks(workflow_id, limit, status)


@api_v3_router.get("/workflow/{workflow_id}/validate_username/{username}")
def get_workflow_user_validation(
    workflow_id: int, username: str, db: Session = Depends(get_db)
) -> WorkflowUserValidationResponse:
    """Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_user_validation(workflow_id, username)


@api_v3_router.get("/workflow/{workflow_id}/validate_for_workflow_reset/{username}")
def get_workflow_run_for_workflow_reset(
    workflow_id: int, username: str, db: Session = Depends(get_db)
) -> WorkflowRunForResetResponse:
    """Last workflow_run_id associated with a given workflow_id started by the username.

    Used to validate for workflow_reset:
        1. The last workflow_run of the current workflow must be in error state.
        2. This last workflow_run must have been started by the input username.
        3. This last workflow_run is in status 'E'
    """
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_run_for_reset(workflow_id, username)


@api_v3_router.put("/workflow/{workflow_id}/reset")
async def reset_workflow(
    workflow_id: int, request: Request, db: Session = Depends(get_db)
) -> JSONResponse:
    """Update the workflow's status, all its tasks' statuses to 'G'."""
    data = await request.json()
    partial_reset = data.get("partial_reset", False)

    workflow_repo = WorkflowRepository(db)
    workflow_repo.reset_workflow(workflow_id, partial_reset)

    return JSONResponse(content={}, status_code=StatusCodes.OK)


@api_v3_router.get("/workflow_status")
def get_workflow_status(
    workflow_id: Optional[Union[int, str, List[Union[int, str]]]] = Query(None),
    limit: Optional[int] = Query(None),
    user: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
) -> WorkflowStatusResponse:
    """Get the status of the workflow."""
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_status(workflow_id, limit, user)


@api_v3_router.get("/workflow_status_viz")
def get_workflow_status_viz(
    workflow_ids: List[int] = Query(None), db: Session = Depends(get_db)
) -> Any:
    """Get the status of the workflows for GUI."""
    workflow_repo = WorkflowRepository(db)
    result = workflow_repo.get_workflow_status_viz(workflow_ids)
    return JSONResponse(content=result, status_code=StatusCodes.OK)


@api_v3_router.get("/workflow_overview_viz")
def workflows_by_user_form(
    user: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    wf_name: Optional[str] = Query(None),
    wf_args: Optional[str] = Query(None),
    wf_attribute_value: Optional[str] = Query(None),
    wf_attribute_key: Optional[str] = Query(None),
    wf_id: Optional[str] = Query(None),
    date_submitted: Optional[str] = Query(None),
    date_submitted_end: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> WorkflowOverviewResponse:
    """Fetch associated workflows and workflow runs by username."""
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_overview(
        user=user,
        tool=tool,
        wf_name=wf_name,
        wf_args=wf_args,
        wf_attribute_value=wf_attribute_value,
        wf_attribute_key=wf_attribute_key,
        wf_id=wf_id,
        date_submitted=date_submitted,
        date_submitted_end=date_submitted_end,
        status=status,
    )


@api_v3_router.get("/task_table_viz/{workflow_id}")
def task_details_by_wf_id(
    workflow_id: int, tt_name: str, db: Session = Depends(get_db)
) -> TaskTableResponse:
    """Fetch Task details associated with Workflow ID and TaskTemplate name."""
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_task_details_by_workflow_id(workflow_id, tt_name)


@api_v3_router.get("/workflow_details_viz/{workflow_id}")
def wf_details_by_wf_id(
    workflow_id: int, db: Session = Depends(get_db)
) -> List[WorkflowDetailsItem]:
    """Fetch name, args, dates, tool for a Workflow provided WF ID."""
    workflow_repo = WorkflowRepository(db)
    return workflow_repo.get_workflow_details_by_id(workflow_id)
