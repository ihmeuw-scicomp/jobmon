"""Routes for TaskInstances."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import select
import structlog

from jobmon.core import constants
from jobmon.core.exceptions import InvalidStateTransition
from jobmon.server.web._compat import add_time
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.v1 import api_v1_blueprint

logger = structlog.get_logger(__name__)


@api_v1_blueprint.route(
    "/task_instance/<task_instance_id>/log_running", methods=["POST"]
)
def log_running(task_instance_id: int) -> Any:
    """Log a task_instance as running.

    Args:
        task_instance_id: id of the task_instance to log as running
    """
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())

    session = SessionLocal()
    with session.begin():
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()

        if data.get("distributor_id", None) is not None:
            task_instance.distributor_id = data["distributor_id"]
        if data.get("nodename", None) is not None:
            task_instance.nodename = data["nodename"]
        task_instance.process_group_id = data["process_group_id"]
        try:
            task_instance.transition(constants.TaskInstanceStatus.RUNNING)
            task_instance.report_by_date = add_time(data["next_report_increment"])
        except InvalidStateTransition as e:
            if task_instance.status == constants.TaskInstanceStatus.RUNNING:
                logger.warning(e)
            elif task_instance.status == constants.TaskInstanceStatus.KILL_SELF:
                task_instance.transition(constants.TaskInstanceStatus.ERROR_FATAL)
            else:
                # Tried to move to an illegal state
                logger.error(e)

        wire_format = task_instance.to_wire_as_worker_node_task_instance()

    resp = jsonify(task_instance=wire_format)
    resp.status_code = StatusCodes.OK
    return resp
