"""Routes for Tool Versions."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
import sqlalchemy
from sqlalchemy import select
import structlog

from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import InvalidUsage


logger = structlog.get_logger(__name__)


@blueprint.route("/tool_version", methods=["POST"])
def add_tool_version() -> Any:
    """Add a new version for a Tool."""
    # check input variable
    data = cast(Dict, request.get_json())

    try:
        tool_id = int(data["tool_id"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    session = SessionLocal()
    try:
        with session.begin():
            tool_version = ToolVersion(tool_id=tool_id)
            session.add(tool_version)
    except sqlalchemy.exc.IntegrityError:
        with session.begin():
            select_stmt = select(ToolVersion).where(ToolVersion.tool_id == tool_id)
            tool_version = session.execute(select_stmt).scalars().one()

    wire_format = tool_version.to_wire_as_client_tool_version()

    resp = jsonify(tool_version=wire_format)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/tool_version/<tool_version_id>/task_templates", methods=["GET"])
def get_task_templates(tool_version_id: int) -> Any:
    """Get the Tool Version."""
    # check input variable
    structlog.contextvars.bind_contextvars(tool_version_id=tool_version_id)
    logger.info("Getting available task_templates")

    session = SessionLocal()
    with session.begin():
        select_stmt = select(TaskTemplate).where(
            TaskTemplate.tool_version_id == tool_version_id
        )
        task_templates = session.execute(select_stmt).scalars().all()
        wire_format = [t.to_wire_as_client_task_template() for t in task_templates]

    resp = jsonify(task_templates=wire_format)
    resp.status_code = StatusCodes.OK
    return resp
