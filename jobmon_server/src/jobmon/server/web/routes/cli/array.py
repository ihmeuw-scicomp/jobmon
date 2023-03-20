from http import HTTPStatus
from typing import Any

from flask import jsonify, request
from sqlalchemy import select

from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.cli import blueprint


@blueprint.route("/array/<workflow_id>/get_array_tasks")
def get_array_task_instances(workflow_id: int) -> Any:
    """Return error/output filepaths for task instances filtered by array name.

    The user can also optionally filter by job name as well.

    To avoid overly-large returned results, the user must also pass in a workflow ID.
    """
    data = request.args
    array_name = data.get("array_name")
    job_name = data.get("job_name")
    limit = data.get("limit", 5)

    query_filters = [
        Task.workflow_id == workflow_id,
        TaskInstance.task_id == Task.id,
        Task.array_id == Array.id,
    ]

    if array_name:
        query_filters.append(Array.name == array_name)

    if job_name:
        query_filters.append(Task.name == job_name)

    session = SessionLocal()
    with SessionLocal.begin():
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
    resp = jsonify(array_tasks=[dict(zip(column_names, ti)) for ti in result])
    resp.status_code = HTTPStatus.OK
    return resp
