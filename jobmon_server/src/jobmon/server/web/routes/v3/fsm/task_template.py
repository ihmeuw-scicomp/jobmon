"""Routes for TaskTemplates."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from fastapi import Request
import sqlalchemy
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse
import structlog

from jobmon.core import constants
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.template_arg_map import TemplateArgMap
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()


@api_v3_router.post("/task_template")
async def get_task_template(request: Request) -> Any:
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
        with SessionLocal() as session:
            with session.begin():
                task_template = TaskTemplate(tool_version_id=tool_version_id, name=name)
                session.add(task_template)
                session.flush()
                session.refresh(task_template)
                ttid = task_template.id
    except sqlalchemy.exc.IntegrityError:
        with SessionLocal() as session:
            with session.begin():
                select_stmt = select(TaskTemplate).where(
                    TaskTemplate.tool_version_id == tool_version_id,
                    TaskTemplate.name == name,
                )
                task_template = session.execute(select_stmt).scalars().one()
                ttid = task_template.id
        ()
    resp = JSONResponse(content={"task_template_id": ttid}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/task_template/{task_template_id}/versions")
def get_task_template_versions(task_template_id: int) -> Any:
    """Get the task_template_version."""
    # get task template version object
    structlog.contextvars.bind_contextvars(task_template_id=task_template_id)
    logger.info(f"Getting task template version for task template: {task_template_id}")

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(TaskTemplateVersion).where(
                TaskTemplateVersion.task_template_id == task_template_id
            )
            ttvs = session.execute(select_stmt).scalars().all()
            wire_obj = [ttv.to_wire_as_client_task_template_version() for ttv in ttvs]

    resp = JSONResponse(
        content={"task_template_versions": wire_obj}, status_code=StatusCodes.OK
    )
    return resp


def _add_or_get_arg(name: str, session: Session) -> Arg:
    retries = 0
    while retries <= 5:
        try:
            with session.begin():
                arg = Arg(name=name)
                session.add(arg)
                session.flush()
                break  # Successfully added, break the loop
        except IntegrityError:
            with session.begin():
                select_stmt = select(Arg).where(Arg.name == name)
                arg = session.execute(select_stmt).scalars().one()
                break  # Successfully retrieved, break the loop
        except OperationalError as e:
            if "Deadlock" in str(e):
                retries += 1
                continue  # Deadlock detected, retrying
            else:
                raise  # For other OperationalErrors, propagate the exception
    return arg


@api_v3_router.post("/task_template/{task_template_id}/add_version")
async def add_task_template_version(task_template_id: int, request: Request) -> Any:
    """Add a tool to the database."""
    # check input variables
    structlog.contextvars.bind_contextvars(task_template_id=task_template_id)
    data = cast(Dict, await request.json())
    try:
        task_template_id = int(task_template_id)
        node_args = data["node_args"]
        task_args = data["task_args"]
        op_args = data["op_args"]
        command_template = data["command_template"].strip()
        arg_mapping_hash = str(data["arg_mapping_hash"]).strip()
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    # populate the argument table
    arg_mapping_dct: dict = {
        constants.ArgType.NODE_ARG: [],
        constants.ArgType.TASK_ARG: [],
        constants.ArgType.OP_ARG: [],
    }
    with SessionLocal() as session:
        for arg_name in node_args:
            arg_mapping_dct[constants.ArgType.NODE_ARG].append(
                _add_or_get_arg(arg_name, session)
            )
        for arg_name in task_args:
            arg_mapping_dct[constants.ArgType.TASK_ARG].append(
                _add_or_get_arg(arg_name, session)
            )
        for arg_name in op_args:
            arg_mapping_dct[constants.ArgType.OP_ARG].append(
                _add_or_get_arg(arg_name, session)
            )

        try:
            with session.begin():
                ttv = TaskTemplateVersion(
                    task_template_id=task_template_id,
                    command_template=command_template,
                    arg_mapping_hash=arg_mapping_hash,
                )
                session.add(ttv)
                session.flush()

                # get a lock
                session.refresh(ttv, with_for_update=True)
                for arg_type_id in arg_mapping_dct.keys():
                    for arg in arg_mapping_dct[arg_type_id]:
                        ctatm = TemplateArgMap(
                            task_template_version_id=ttv.id,
                            arg_id=arg.id,
                            arg_type_id=arg_type_id,
                        )
                        session.add(ctatm)
                session.flush()

                task_template_version = ttv.to_wire_as_client_task_template_version()

        except sqlalchemy.exc.IntegrityError:
            with session.begin():
                # if another process is adding this task_template_version then this query
                # should block until the template_arg_map has been populated and committed
                select_stmt = select(TaskTemplateVersion).where(
                    TaskTemplateVersion.task_template_id == task_template_id,
                    TaskTemplateVersion.command_template == command_template,
                    TaskTemplateVersion.arg_mapping_hash == arg_mapping_hash,
                )
                ttv = session.execute(select_stmt).scalars().one()

                task_template_version = ttv.to_wire_as_client_task_template_version()
        ()
    resp = JSONResponse(
        content={"task_template_version": task_template_version},
        status_code=StatusCodes.OK,
    )
    return resp
