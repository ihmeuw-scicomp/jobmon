"""Routes for WorkflowRuns."""

from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from sqlalchemy import case, func, update
import structlog

from jobmon.core import constants
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.v1 import api_v1_blueprint
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)


@api_v1_blueprint.route(
    "/workflow_run/<workflow_run_id>/set_status_for_triaging", methods=["POST"]
)
def set_status_for_triaging(workflow_run_id: int) -> Any:
    """Two triaging related status sets.

    Query all task instances that are submitted to distributor or running which haven't
    reported as alive in the allocated time, and set them for Triaging(from Running)
    and Kill_self(from Launched).
    """
    structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    logger.info(f"Set to triaging those overdue tis for wfr {workflow_run_id}")

    session = SessionLocal()
    with session.begin():
        update_stmt = (
            update(TaskInstance)
            .where(
                TaskInstance.workflow_run_id == workflow_run_id,
                TaskInstance.status.in_(
                    [
                        constants.TaskInstanceStatus.LAUNCHED,
                        constants.TaskInstanceStatus.RUNNING,
                    ]
                ),
                TaskInstance.report_by_date <= func.now(),
            )
            .values(
                status=case(
                    (
                        TaskInstance.status == constants.TaskInstanceStatus.RUNNING,
                        constants.TaskInstanceStatus.TRIAGING,
                    ),
                    else_=constants.TaskInstanceStatus.KILL_SELF,
                )
            )
            .execution_options(synchronize_session=False)
        )
        session.execute(update_stmt)
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp
