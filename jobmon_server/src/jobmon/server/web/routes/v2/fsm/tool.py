"""Routes for Tools."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
import sqlalchemy
from sqlalchemy import select
import structlog

from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.routes.v1 import api_v1_blueprint
from jobmon.server.web.routes.v2 import api_v2_blueprint
from jobmon.server.web.routes.v2 import SessionLocal
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)


@api_v1_blueprint.route("/tool", methods=["POST"])
@api_v2_blueprint.route("/tool", methods=["POST"])
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


@api_v1_blueprint.route("/tool/<tool_id>/tool_versions", methods=["GET"])
@api_v2_blueprint.route("/tool/<tool_id>/tool_versions", methods=["GET"])
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


@api_v1_blueprint.route("/tool/<tool_id>/tool_resource_usage", methods=["GET"])
@api_v2_blueprint.route("/tool/<tool_id>/tool_resource_usage", methods=["GET"])
def get_tool_resource_usage(tool_id: int) -> Any:
    """
    Returns requested and utilized resource usage and node args for every task instance associated with a given tool.

    We limit this by date to not overwhelm the database.

    Parameters:
        tool_id: ID of the tool

    Returns:
        A list ?
    """
    # arguments = request.args
    # start_date = arguments.get("start_date")
    #
    # return f"start date: {start_date}"

    session = SessionLocal()
    with session.begin():
        sql = (
            select(
                TaskInstance.id,
                TaskInstance.wallclock,
                TaskInstance.maxrss,
                NodeArg.val,
                TaskResources.requested_resources,
            ).join_from(
                TaskInstance,
                Task,
                TaskInstance.task_id == Task.id
            )
            .join_from(
                Task,
                Workflow,
                Task.workflow_id == Workflow.id
            )
            .join_from(
                Workflow,
                ToolVersion,
                Workflow.tool_version_id == ToolVersion.id
            )
            .join_from(
                ToolVersion,
                Tool,
                ToolVersion.tool_id == Tool.id
            )
            .join_from(
                Task,
                NodeArg,
                Task.node_id == NodeArg.node_id
            )
            .join_from(
                Task,
                TaskResources,
                Task.task_resources_id == TaskResources.id
            )
            .where(
                Tool.name == 'large_wf_tool',
                Workflow.created_date >= '2024-06-15',
                Task.status == 'D',
                TaskInstance.status == 'D'
            )
        )
        results = session.execute(sql).all()

    column_names = (
        "ti_id",
        "ti_wallclock",
        "ti_maxrss",
        "node_arg_val",
        "ti_requested_resources"
    )
    results = [dict(zip(column_names, result)) for result in results]

    resp = jsonify(results)
    resp.status_code = StatusCodes.OK
    return resp
