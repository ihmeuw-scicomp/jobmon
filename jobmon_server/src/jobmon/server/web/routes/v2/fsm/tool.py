"""Routes for Tools."""

from datetime import datetime, timedelta
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict, Optional

from fastapi import Request
import sqlalchemy
from sqlalchemy import select
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.routes.v1.fsm import fsm_router as api_v1_router
from jobmon.server.web.routes.v2.fsm import fsm_router as api_v2_router
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()


@api_v1_router.post("/tool")
@api_v2_router.post("/tool")
async def add_tool(request: Request) -> Any:
    """Add a tool to the database."""
    data = cast(Dict, await request.json())
    try:
        tool_name = data["name"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    # add tool to db
    try:
        with SessionLocal() as session:
            with session.begin():
                logger.info(f"Adding tool {tool_name}")
                tool = Tool(name=tool_name)
                session.add(tool)
                session.flush()
                session.refresh(tool)
                wire_format = tool.to_wire_as_client_tool()
    except sqlalchemy.exc.IntegrityError:
        with SessionLocal() as session:
            with session.begin():
                select_stmt = select(Tool).where(Tool.name == tool_name)
                tool = session.execute(select_stmt).scalars().one()
                wire_format = tool.to_wire_as_client_tool()

    resp = JSONResponse(content={"tool": wire_format}, status_code=StatusCodes.OK)
    return resp


@api_v1_router.get("/tool/{tool_id}/tool_versions")
@api_v2_router.get("/tool/{tool_id}/tool_versions")
def get_tool_versions(tool_id: int, request: Request) -> Any:
    """Get the Tool Version."""
    # check input variable
    structlog.contextvars.bind_contextvars(tool_id=tool_id)
    logger.info(f"Getting available tool versions for tool_id {tool_id}")
    try:
        tool_id = int(tool_id)
    except Exception as e:
        raise InvalidUsage(
            f"Variable tool_id must be an int in {request.url.path}", status_code=400
        ) from e

    # get data from db
    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(ToolVersion).where(ToolVersion.tool_id == tool_id)
            tool_versions = session.execute(select_stmt).scalars().all()
        wire_format = [t.to_wire_as_client_tool_version() for t in tool_versions]

        logger.info(f"Tool version for {tool_id} is {wire_format}")

        resp = JSONResponse(
            content={"tool_versions": wire_format}, status_code=StatusCodes.OK
        )
    return resp


@api_v1_router.get("/tool/{tool_name}/tool_resource_usage")
@api_v2_router.get("/tool/{tool_name}/tool_resource_usage")
def get_tool_resource_usage(
    tool_name: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Any:
    """Gets resource usage and node args for all TaskInstances associated with a given tool.

    We limit this to one week time spans to not overwhelm the database.

    Args:
        tool_name (str): Name of the tool.
        start_date (str): The start date in 'YYYY-MM-DD' format (query parameter).
        end_date (str): The end date in 'YYYY-MM-DD' format (query parameter).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing TaskInstance ID,
        node argument value, TaskInstance maxrss, TaskInstance wallclock, and
        requested resources.

    Example Call:
        /tool/large_wf_tool/tool_resource_usage?start_date=2024-07-11&end_date=2024-07-18

    Example Response:
        [
            {
                "node_arg_val": "--provenance True",
                "ti_id": 12345677,
                "ti_maxrss": 50844672,
                "ti_requested_resources": {"runtime": 21600, "memory": 10},
                "ti_wallclock": 20
            },
            {
                "node_arg_val": "--intrinsic False",
                "ti_id": 12345678,
                "ti_maxrss": 43960320,
                "ti_requested_resources": {"runtime": 21600, "memory": 10},
                "ti_wallclock": 22
            }
        ]
    """
    # Validate that user passed in both dates
    if not start_date or not end_date:
        return JSONResponse(
            content={"error": "Both start_date and end_date are required."},
            status_code=StatusCodes.BAD_REQUEST,
        )

    try:
        start_date_object = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_object = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return JSONResponse(
            content={"error": "Dates must be in 'YYYY-MM-DD' format."},
            status_code=StatusCodes.BAD_REQUEST,
        )

    date_difference = end_date_object - start_date_object

    # Check if the difference exceeds one week
    if date_difference > timedelta(weeks=1):
        return JSONResponse(
            content={"error": "Date range must be within one week."},
            status_code=StatusCodes.BAD_REQUEST,
        )

    with SessionLocal() as session:
        with session.begin():
            sql = (
                select(
                    TaskInstance.id,
                    TaskInstance.wallclock,
                    TaskInstance.maxrss,
                    NodeArg.val,
                    TaskResources.requested_resources,
                )
                .join_from(TaskInstance, Task, TaskInstance.task_id == Task.id)
                .join_from(Task, Workflow, Task.workflow_id == Workflow.id)
                .join_from(
                    Workflow, ToolVersion, Workflow.tool_version_id == ToolVersion.id
                )
                .join_from(ToolVersion, Tool, ToolVersion.tool_id == Tool.id)
                .join_from(Task, NodeArg, Task.node_id == NodeArg.node_id)
                .join_from(
                    Task, TaskResources, Task.task_resources_id == TaskResources.id
                )
                .where(
                    Tool.name == tool_name,
                    Workflow.created_date.between(start_date, end_date),
                    Task.status == "D",
                    TaskInstance.status == "D",
                )
            )
            results = session.execute(sql).all()

        column_names = (
            "ti_id",
            "ti_wallclock",
            "ti_maxrss",
            "node_arg_val",
            "ti_requested_resources",
        )
        results_formatted: list[Dict[str, Any]] = [
            dict(zip(column_names, result)) for result in results
        ]

    resp = JSONResponse(content=results_formatted, status_code=StatusCodes.OK)
    return resp
