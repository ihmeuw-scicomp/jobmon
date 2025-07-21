"""Routes for Tasks."""

from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Optional, Union, cast

import structlog
from fastapi import Depends, Query, Request
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core import constants
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.repositories.task_repository import TaskRepository
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
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
) -> tuple[str, bool, Optional[str], Union[List[int], str], str]:
    """Parse and validate request data."""
    try:
        workflow_id = data["workflow_id"]
        recursive = data.get("recursive", False)
        workflow_status = data.get("workflow_status", None)

        task_ids = data["task_ids"]
        if isinstance(task_ids, int):
            task_ids = [task_ids]
        if task_ids == "all":
            recursive = False

        new_status = data["new_status"]

        return workflow_id, recursive, workflow_status, task_ids, new_status
    except KeyError as e:
        raise InvalidUsage(f"problem with {str(e)} in request", status_code=400) from e


def create_response(new_status: str) -> JSONResponse:
    """Create the JSON response with CORS headers."""
    message = f"updated to status {new_status}"
    resp = JSONResponse(content={"message": message}, status_code=StatusCodes.OK)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "PUT, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


@api_v3_router.put("/task/update_statuses")
async def update_task_statuses(request: Request, db: Session = Depends(get_db)) -> Any:
    """Update the status of the tasks.

    Description:
        - When workflow_id='all', it updates all tasks in the workflow with
        recursive=False. This improves performance.
        - When recursive=True, it updates the tasks and it's dependencies all
        the way up or down the DAG.
        - When recursive=False, it updates only the tasks in the task_ids list.
        - When workflow_status is None, it gets the workflow status from the db.
        - After updating the tasks, it checks the workflow status and updates it.

    Notes:
        It integrated the logic in update_task_status from status_commands.py.
    """
    data = cast(Dict, await request.json())

    # Parse and validate request data
    workflow_id, recursive, workflow_status, task_ids, new_status = parse_request_data(
        data
    )

    task_repository = TaskRepository(session=db)
    task_repository.update_task_statuses(
        workflow_id, recursive, workflow_status, task_ids, new_status
    )

    return create_response(new_status)


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
    data = cast(Dict, await request.json())

    task_ids = data["task_ids"]
    dag_id = data["dag_id"]

    task_repo = TaskRepository(db)
    result = task_repo.get_downstream_tasks(task_ids, dag_id)

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
