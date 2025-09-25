"""Routes for Tasks."""

from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Optional, Union, cast

import structlog
from fastapi import Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core import constants
from jobmon.server.web.db import get_sessionmaker
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_status import WorkflowStatus
from jobmon.server.web.repositories.task_repository import TaskRepository
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.routes.v3.cli.workflow import _check_downstream_tasks_status
from jobmon.server.web.schemas.task import (
    DownstreamTasksResponse,
    TaskDependenciesResponse,
    TaskDetailsResponse,
    TaskInstanceDetailsResponse,
    TaskResourceUsageResponse,
    TasksRecursiveResponse,
    TaskStatusResponse,
    TaskSubdagResponse,
)
from jobmon.server.web.server_side_exception import InvalidUsage

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


@api_v3_router.get("/task_status")
def get_task_status(
    task_ids: Optional[Union[int, list[int]]] = Query(...),
    status: Optional[Union[str, list[str]]] = Query(None),
    db: Session = Depends(get_db),
) -> TaskStatusResponse:
    """Get the status of a task."""
    logger.info(f"task_ids: {task_ids}, status_request: {status}")

    task_repo = TaskRepository(db)
    result = task_repo.get_task_status(task_ids, status)

    return result


@api_v3_router.post("/task/subdag")
async def get_task_subdag(
    request: Request, db: Session = Depends(get_db)
) -> TaskSubdagResponse:
    """Used to get the sub dag of a given task.

    It returns a list of sub tasks as well as a list of sub nodes.
    """
    data = cast(Dict, await request.json())
    task_ids = data.get("task_ids", [])
    task_status = data.get("task_status", [])

    task_repo = TaskRepository(db)
    result = task_repo.get_task_subdag(task_ids, task_status)

    return result


def parse_request_data(
    data: Dict,
) -> tuple[str, bool, Union[List[int], str], str]:
    """Parse and validate request data."""
    try:
        workflow_id = data["workflow_id"]
        recursive = data.get("recursive", False)

        task_ids = data["task_ids"]
        if isinstance(task_ids, int):
            task_ids = [task_ids]
        if task_ids == "all":
            recursive = False

        new_status = data["new_status"]

        return workflow_id, recursive, task_ids, new_status
    except KeyError as e:
        raise InvalidUsage(f"problem with {str(e)} in request", status_code=400) from e


def validate_workflow_for_update(task_ids: List[int], session: Session) -> str:
    """Validate workflow status for task updates.

    Validates that:
    - All tasks belong to the same workflow
    - The workflow status allows updates (FAILED, DONE, ABORTED, HALTED) OR
    - All downstream tasks are in valid states (G, I, Q) for non-terminal workflows

    Args:
        task_ids: List of task IDs to validate
        session: Database session

    Returns:
        The workflow status if validation passes

    Raises:
        InvalidUsage: If validation fails with detailed error message
    """
    logger.info(f"Validating workflow for task_ids: {task_ids}")

    # Skip validation for empty task list
    if not task_ids:
        return ""

    # Query workflow status for all tasks
    query = (
        select(Task.workflow_id, Workflow.status, Workflow.dag_id)
        .where(Task.workflow_id == Workflow.id, Task.id.in_(task_ids))
        .distinct()
    )
    rows = session.execute(query).all()
    workflow_statuses = [row.status for row in rows]

    # Validate results
    if len(workflow_statuses) != 1:
        error_msg = _get_validation_error_message(workflow_statuses)
        logger.warning(f"Validation failed: {error_msg}")
        raise InvalidUsage(error_msg, status_code=400)

    current_status = workflow_statuses[0]
    allowed_statuses = {
        WorkflowStatus.FAILED,
        WorkflowStatus.DONE,
        WorkflowStatus.ABORTED,
        WorkflowStatus.HALTED,
    }

    if current_status in allowed_statuses:
        # Workflow is in terminal state, allow update
        logger.info(f"Validation passed, workflow status: {current_status}")
        return current_status
    else:
        # Workflow is not in terminal state, check downstream tasks
        workflow_id = rows[0].workflow_id
        dag_id = rows[0].dag_id

        if _check_downstream_tasks_status(session, task_ids, workflow_id, dag_id):
            return current_status
        else:
            error_msg = (
                "Task status updates are only allowed when the workflow is in "
                "FAILED, DONE, ABORTED, or HALTED status, or when all downstream "
                "tasks are in registered, instantiating, or queued states."
            )
            logger.warning(f"Validation failed: {error_msg}")
            raise InvalidUsage(error_msg, status_code=400)


def _get_validation_error_message(workflow_statuses: List[str]) -> str:
    """Get appropriate error message based on workflow status validation results."""
    if len(workflow_statuses) > 1:
        return (
            "Task status cannot be updated because the tasks belong to "
            "multiple workflows. All tasks must belong to the same workflow."
        )
    elif len(workflow_statuses) == 0:
        return (
            "Task status cannot be updated because no valid workflow "
            "was found for the given tasks."
        )
    else:
        # This shouldn't happen given the calling context, but handle it gracefully
        return "Task status cannot be updated due to workflow validation issues."


def create_response(new_status: str) -> JSONResponse:
    """Create a JSON HTTP response indicating a successful status update.

    Args:
        new_status (str): The new status that was applied.

    Returns:
        JSONResponse: A FastAPI JSONResponse object with a success message.
    """
    message = f"updated to status {new_status}"
    resp = JSONResponse(content={"message": message}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.put("/task/update_statuses")
async def update_task_statuses(request: Request, db: Session = Depends(get_db)) -> Any:
    """Update the status of the tasks.

    Description:
        - When workflow_id='all', it updates all tasks in the workflow with
        recursive=False. This improves performance.
        - When recursive=True, it updates the tasks and its dependencies all
        the way up or down the DAG.
        - When recursive=False, it updates only the tasks in the task_ids list.
        - Validates workflow status before proceeding with updates.
        - After updating the tasks, it checks the workflow status and updates it.
    """

    def add_cors_headers(response: JSONResponse) -> JSONResponse:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "PUT, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

    try:
        data = cast(Dict, await request.json())
        workflow_id, recursive, task_ids, new_status = parse_request_data(data)

        with get_sessionmaker()() as session, session.begin():
            if isinstance(task_ids, str):
                if task_ids != "all":
                    raise InvalidUsage(
                        f"Invalid task_ids value: {task_ids}", status_code=400
                    )
                task_ids_for_validation = [
                    task_id
                    for task_id, in session.query(Task.id)
                    .filter(Task.workflow_id == workflow_id)
                    .all()
                ]
            else:
                task_ids_for_validation = task_ids

            workflow_status = validate_workflow_for_update(
                task_ids_for_validation, session
            )

            TaskRepository(session).update_task_statuses(
                workflow_id, recursive, workflow_status, task_ids, new_status
            )

        return add_cors_headers(create_response(new_status))

    except Exception as error:
        status_code = getattr(error, "status_code", 500)
        content = {"detail": str(error)}
        return add_cors_headers(JSONResponse(content=content, status_code=status_code))


@api_v3_router.get("/task_dependencies/{task_id}")
def get_task_dependencies(
    task_id: int, db: Session = Depends(get_db)
) -> TaskDependenciesResponse:
    """Get task's downstream and upstream tasks and their status."""
    task_repo = TaskRepository(db)
    result = task_repo.get_task_dependencies(task_id)

    return result


@api_v3_router.put("/tasks_recursive/{direction}")
async def get_tasks_recursive(
    direction: str, request: Request, db: Session = Depends(get_db)
) -> TasksRecursiveResponse:
    """Get all input task_ids'.

    Either downstream or upsteam tasks based on direction;
    return all recursive(including input set) task_ids in the defined direction.
    """
    direct = constants.Direction.UP if direction == "up" else constants.Direction.DOWN
    data = await request.json()
    # define task_ids as set in order to eliminate dups
    task_ids = set(data.get("task_ids", []))

    try:
        task_repository = TaskRepository(session=db)
        tasks_recursive = task_repository._get_tasks_recursive(task_ids, direct)

        return TasksRecursiveResponse(task_ids=list(tasks_recursive))
    except InvalidUsage as e:
        raise e


@api_v3_router.get("/task_resource_usage")
def get_task_resource_usage(
    task_id: int, db: Session = Depends(get_db)
) -> TaskResourceUsageResponse:
    """Return the resource usage for a given Task ID."""
    task_repo = TaskRepository(db)
    result = task_repo.get_task_resource_usage(task_id)

    return result


@api_v3_router.post("/task/get_downstream_tasks")
async def get_downstream_tasks(
    request: Request, db: Session = Depends(get_db)
) -> DownstreamTasksResponse:
    """Get only the direct downstreams of a task."""
    # Get client version from query parameters
    client_version = request.query_params.get("client_jobmon_version")
    data = cast(Dict, await request.json())

    task_ids = data["task_ids"]
    dag_id = data["dag_id"]

    task_repo = TaskRepository(db)
    result = task_repo.get_downstream_tasks(task_ids, dag_id, client_version)

    return result


@api_v3_router.get("/task/get_ti_details_viz/{task_id}")
def get_task_details(
    task_id: int, db: Session = Depends(get_db)
) -> TaskInstanceDetailsResponse:
    """Get information about TaskInstances associated with specific Task ID."""
    task_repo = TaskRepository(db)
    result = task_repo.get_task_instance_details(task_id)

    return result


@api_v3_router.get("/task/get_task_details_viz/{task_id}")
def get_task_details_viz(
    task_id: int, db: Session = Depends(get_db)
) -> TaskDetailsResponse:
    """Get status of Task from Task ID."""
    task_repo = TaskRepository(db)
    result = task_repo.get_task_details_viz(task_id)

    return result
