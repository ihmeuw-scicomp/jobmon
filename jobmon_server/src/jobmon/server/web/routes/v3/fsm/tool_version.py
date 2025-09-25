"""Routes for Tool Versions."""

from http import HTTPStatus as StatusCodes
from typing import Any, Dict, cast

import sqlalchemy
import structlog
from fastapi import Depends, Request
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)


@api_v3_router.post("/tool_version")
async def add_tool_version(request: Request, db: Session = Depends(get_db)) -> Any:
    """Add a new version for a Tool."""
    # check input variable
    data = cast(Dict, await request.json())

    try:
        tool_id = int(data["tool_id"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    try:
        tool_version = ToolVersion(tool_id=tool_id)
        db.add(tool_version)
        db.flush()
    except sqlalchemy.exc.IntegrityError:
        db.rollback()
        select_stmt = select(ToolVersion).where(ToolVersion.tool_id == tool_id)
        tool_version = db.execute(select_stmt).scalars().one()

    db.refresh(tool_version)
    wire_format = tool_version.to_wire_as_client_tool_version()

    resp = JSONResponse(
        content={"tool_version": wire_format}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.get("/tool_version/{tool_version_id}/task_templates")
def get_task_templates(tool_version_id: int, db: Session = Depends(get_db)) -> Any:
    """Get the Tool Version."""
    # check input variable
    structlog.contextvars.bind_contextvars(tool_version_id=tool_version_id)
    logger.info("Getting available task_templates")

    select_stmt = select(TaskTemplate).where(
        TaskTemplate.tool_version_id == tool_version_id
    )
    task_templates = db.execute(select_stmt).scalars().all()
    wire_format = [t.to_wire_as_client_task_template() for t in task_templates]

    resp = JSONResponse(
        content={"task_templates": wire_format}, status_code=StatusCodes.OK
    )
    return resp
