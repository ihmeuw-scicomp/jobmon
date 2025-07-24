"""Routes for TaskTemplates."""

from http import HTTPStatus as StatusCodes
from typing import Any, Dict, cast

import structlog
from fastapi import Depends, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core import constants
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.template_arg_map import TemplateArgMap
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)


@api_v3_router.post("/task_template")
async def get_task_template(request: Request, db: Session = Depends(get_db)) -> Any:
    """Add a task template for a given tool to the database."""
    # check input variable
    data = cast(Dict, await request.json())
    try:
        tool_version_id = int(data["tool_version_id"])
        name = data["task_template_name"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    structlog.contextvars.bind_contextvars(tool_version_id=tool_version_id)
    logger.info(f"Add task tamplate for tool_version_id {tool_version_id} ")

    # add to DB
    try:
        task_template = TaskTemplate(tool_version_id=tool_version_id, name=name)
        db.add(task_template)
        db.flush()
        db.refresh(task_template)
        ttid = task_template.id
    except IntegrityError:
        # Race condition: another process created it
        select_stmt = select(TaskTemplate).where(
            TaskTemplate.tool_version_id == tool_version_id,
            TaskTemplate.name == name,
        )
        task_template = db.execute(select_stmt).scalars().one()
        ttid = task_template.id
    resp = JSONResponse(content={"task_template_id": ttid}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/task_template/{task_template_id}/versions")
def get_task_template_versions(
    task_template_id: int, db: Session = Depends(get_db)
) -> Any:
    """Get the task_template_version."""
    # get task template version object
    structlog.contextvars.bind_contextvars(task_template_id=task_template_id)
    logger.info(f"Getting task template version for task template: {task_template_id}")

    select_stmt = select(TaskTemplateVersion).where(
        TaskTemplateVersion.task_template_id == task_template_id
    )
    ttvs = db.execute(select_stmt).scalars().all()
    wire_obj = [ttv.to_wire_as_client_task_template_version() for ttv in ttvs]

    resp = JSONResponse(
        content={"task_template_versions": wire_obj}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.post("/task_template/{task_template_id}/add_version")
async def add_task_template_version(
    task_template_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> Any:
    """Add a task_template_version safely using injected DB session."""
    structlog.contextvars.bind_contextvars(task_template_id=task_template_id)

    def _add_or_get_arg(name: str, session: Session) -> Arg:
        try:
            # First try to get existing
            select_stmt = select(Arg).where(Arg.name == name)
            arg = session.execute(select_stmt).scalars().one_or_none()
            if arg:
                return arg

            # If not found, create new
            arg = Arg(name=name)
            session.add(arg)
            session.flush()
            return arg

        except IntegrityError:
            # Race condition: another process created it
            select_stmt = select(Arg).where(Arg.name == name)
            arg = session.execute(select_stmt).scalars().one()
            return arg

    # Parse and validate request
    try:
        data = cast(Dict, await request.json())
        node_args = data["node_args"]
        task_args = data["task_args"]
        op_args = data["op_args"]
        command_template = data["command_template"].strip()
        arg_mapping_hash = str(data["arg_mapping_hash"]).strip()
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    try:
        # Resolve args in same session
        arg_mapping_dct = {
            constants.ArgType.NODE_ARG: [
                arg for arg in [_add_or_get_arg(arg, db) for arg in node_args] if arg
            ],
            constants.ArgType.TASK_ARG: [
                arg for arg in [_add_or_get_arg(arg, db) for arg in task_args] if arg
            ],
            constants.ArgType.OP_ARG: [
                arg for arg in [_add_or_get_arg(arg, db) for arg in op_args] if arg
            ],
        }

        # Main transaction block
        ttv = TaskTemplateVersion(
            task_template_id=task_template_id,
            command_template=command_template,
            arg_mapping_hash=arg_mapping_hash,
        )
        db.add(ttv)
        db.flush()

        # Lock to ensure exclusive write
        db.refresh(ttv, with_for_update=True)

        for arg_type_id, args in arg_mapping_dct.items():
            for arg in args:
                ctatm = TemplateArgMap(
                    task_template_version_id=ttv.id,
                    arg_id=arg.id,
                    arg_type_id=arg_type_id,
                )
                db.add(ctatm)
        db.flush()
        task_template_version = ttv.to_wire_as_client_task_template_version()
        return JSONResponse(
            content={"task_template_version": task_template_version},
            status_code=StatusCodes.OK,
        )

    except IntegrityError as e:
        logger.error(f"IntegrityError: {e}")
        # Race condition: another process may have inserted this TTV
        # Note: With Depends(get_db), session will auto-rollback on exception
        select_stmt = select(TaskTemplateVersion).where(
            TaskTemplateVersion.task_template_id == task_template_id,
            TaskTemplateVersion.command_template == command_template,
            TaskTemplateVersion.arg_mapping_hash == arg_mapping_hash,
        )
        existing_ttv = db.execute(select_stmt).scalars().one_or_none()
        if existing_ttv is None:
            # Still not found - let the IntegrityError bubble up
            raise e
        else:
            task_template_version = (
                existing_ttv.to_wire_as_client_task_template_version()
            )
            return JSONResponse(
                content={"task_template_version": task_template_version},
                status_code=StatusCodes.OK,
            )


@api_v3_router.get("/task_template/id/{task_template_version_id}")
def get_task_template_id_for_task_template_version(
    task_template_version_id: int,
    db: Session = Depends(get_db),
) -> int:
    """Get the task_template_id for a given task_template_version_id."""
    structlog.contextvars.bind_contextvars(
        task_template_version_id=task_template_version_id
    )
    logger.info(
        f"Getting task template id for task template version: {task_template_version_id}"
    )

    select_stmt = select(TaskTemplateVersion).where(
        TaskTemplateVersion.id == task_template_version_id
    )
    ttv = db.execute(select_stmt).scalars().one()
    ttid = ttv.task_template_id

    return int(ttid)  # type: ignore
