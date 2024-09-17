"""Routes for Tool Versions."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from fastapi import Request
import sqlalchemy
from sqlalchemy import select
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage


logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()


@api_v3_router.post("/tool_version")
async def add_tool_version(request: Request) -> Any:
    """Add a new version for a Tool."""
    # check input variable
    data = cast(Dict, await request.json())

    try:
        tool_id = int(data["tool_id"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    with SessionLocal() as session:
        try:
            with session.begin():
                tool_version = ToolVersion(tool_id=tool_id)
                session.add(tool_version)

        except sqlalchemy.exc.IntegrityError:
            with session.begin():
                select_stmt = select(ToolVersion).where(ToolVersion.tool_id == tool_id)
                tool_version = session.execute(select_stmt).scalars().one()

        session.refresh(tool_version)
        wire_format = tool_version.to_wire_as_client_tool_version()

    resp = JSONResponse(
        content={"tool_version": wire_format}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.get("/tool_version/{tool_version_id}/task_templates")
def get_task_templates(tool_version_id: int) -> Any:
    """Get the Tool Version."""
    # check input variable
    structlog.contextvars.bind_contextvars(tool_version_id=tool_version_id)
    logger.info("Getting available task_templates")

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(TaskTemplate).where(
                TaskTemplate.tool_version_id == tool_version_id
            )
            task_templates = session.execute(select_stmt).scalars().all()
            wire_format = [t.to_wire_as_client_task_template() for t in task_templates]

        resp = JSONResponse(
            content={"task_templates": wire_format}, status_code=StatusCodes.OK
        )
    return resp
