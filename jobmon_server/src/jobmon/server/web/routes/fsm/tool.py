"""Routes for Tools."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
import sqlalchemy
from sqlalchemy import select
import structlog

from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import InvalidUsage


logger = structlog.get_logger(__name__)


@blueprint.route("/tool", methods=["POST"])
def add_tool() -> Any:
    """Add a tool to the database."""
    data = cast(Dict, request.get_json())
    try:
        tool_name = data["name"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    # add tool to db
    session = SessionLocal()
    print(session.bind)
    try:
        with session.begin():
            logger.info(f"Adding tool {tool_name}")
            tool = Tool(name=tool_name)
            session.add(tool)
    except sqlalchemy.exc.IntegrityError:
        with session.begin():
            select_stmt = select(Tool).where(Tool.name == tool_name)
            tool = session.execute(select_stmt).scalars().one()
    wire_format = tool.to_wire_as_client_tool()

    resp = jsonify(tool=wire_format)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/tool/<tool_id>/tool_versions", methods=["GET"])
def get_tool_versions(tool_id: int) -> Any:
    """Get the Tool Version."""
    # check input variable
    structlog.contextvars.bind_contextvars(tool_id=tool_id)
    logger.info(f"Getting available tool versions for tool_id {tool_id}")
    try:
        tool_id = int(tool_id)
    except Exception as e:
        raise InvalidUsage(
            f"Variable tool_id must be an int in {request.path}", status_code=400
        ) from e

    # get data from db
    session = SessionLocal()
    with session.begin():
        select_stmt = select(ToolVersion).where(ToolVersion.tool_id == tool_id)
        tool_versions = session.execute(select_stmt).scalars().all()
    wire_format = [t.to_wire_as_client_tool_version() for t in tool_versions]

    logger.info(f"Tool version for {tool_id} is {wire_format}")
    resp = jsonify(tool_versions=wire_format)
    resp.status_code = StatusCodes.OK
    return resp
