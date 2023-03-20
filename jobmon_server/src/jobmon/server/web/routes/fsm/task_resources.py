"""Routes for Task Resources."""
import ast
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from sqlalchemy import select
import structlog

from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint


logger = structlog.get_logger(__name__)


@blueprint.route("/task_resources/<task_resources_id>", methods=["POST"])
def get_task_resources(task_resources_id: int) -> Any:
    """Return an task_resources."""
    structlog.contextvars.bind_contextvars(task_resources_id=task_resources_id)

    session = SessionLocal()
    with session.begin():
        select_stmt = (
            select(TaskResources.requested_resources, Queue.name)
            .join_from(TaskResources, Queue, TaskResources.queue_id == Queue.id)
            .where(TaskResources.id == task_resources_id)
        )
        requested_resources, queue_name = session.execute(select_stmt).fetchone()
        requested_resources = ast.literal_eval(requested_resources)

    resp = jsonify(requested_resources=requested_resources, queue_name=queue_name)
    resp.status_code = StatusCodes.OK
    return resp
