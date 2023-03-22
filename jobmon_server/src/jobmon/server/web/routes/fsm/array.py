"""Routes for Arrays."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import select, update
import structlog

from jobmon.server.web.models.api import Array
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint


logger = structlog.get_logger(__name__)


@blueprint.route("/array", methods=["POST"])
def add_array() -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = cast(Dict, request.get_json())
    workflow_id = int(data["workflow_id"])
    task_template_version_id = int(data["task_template_version_id"])

    structlog.contextvars.bind_contextvars(
        task_template_version_id=task_template_version_id,
        workflow_id=workflow_id,
    )

    # Check if the array is already bound, if so return it
    session = SessionLocal()
    with session.begin():
        select_stmt = select(Array).where(
            Array.workflow_id == workflow_id,
            Array.task_template_version_id == task_template_version_id,
        )
        array = session.execute(select_stmt).scalars().one_or_none()

        if array is None:  # not found, so need to add it
            array = Array(
                task_template_version_id=data["task_template_version_id"],
                workflow_id=data["workflow_id"],
                max_concurrently_running=data["max_concurrently_running"],
                name=data["name"],
            )
            session.add(array)
        else:
            update_stmt = (
                update(Array)
                .where(Array.id == array.id)
                .values(max_concurrently_running=data["max_concurrently_running"])
            )
            session.execute(update_stmt)
        session.commit()

    # return result
    resp = jsonify(array_id=array.id)
    resp.status_code = StatusCodes.OK
    return resp

