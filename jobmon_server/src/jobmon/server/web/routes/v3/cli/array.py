from http import HTTPStatus
from typing import Any, Optional

from fastapi import Query
from sqlalchemy import select
from starlette.responses import JSONResponse

from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router

SessionLocal = get_session_local()


@api_v3_router.get("/array/{workflow_id}/get_array_tasks")
def get_array_task_instances(
    workflow_id: int,
    array_name: str,
    job_name: Optional[str] = Query(None),
    limit: Optional[int] = Query(None),
) -> Any:
    """Return error/output filepaths for task instances filtered by array name.

    The user can also optionally filter by job name as well.

    To avoid overly-large returned results, the user must also pass in a workflow ID.
    """
    query_filters = [
        Task.workflow_id == workflow_id,
        TaskInstance.task_id == Task.id,
        Task.array_id == Array.id,
    ]

    if array_name:
        query_filters.append(Array.name == array_name)

    if job_name:
        query_filters.append(Task.name == job_name)

    # set default limit to 5
    if limit is None:
        limit = 5

    with SessionLocal() as session:
        with session.begin():
            select_stmt = (
                select(
                    Task.id,
                    Task.name,
                    Array.name,
                    TaskInstance.id,
                    TaskInstance.stdout,
                    TaskInstance.stderr,
                )
                .where(*query_filters)
                .limit(limit)
            )
            result = session.execute(select_stmt).all()

        column_names = (
            "TASK_ID",
            "TASK_NAME",
            "ARRAY_NAME",
            "TASK_INSTANCE_ID",
            "OUTPUT_PATH",
            "ERROR_PATH",
        )
        array_tasks = [dict(zip(column_names, ti)) for ti in result]
        resp = JSONResponse(
            content={"array_tasks": array_tasks}, status_code=HTTPStatus.OK
        )
    return resp
