"""Routes for Task Resources."""

import ast
from http import HTTPStatus as StatusCodes
from typing import Any

from sqlalchemy import select
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router


logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()


@api_v3_router.post("/task_resources/{task_resources_id}")
def get_task_resources(task_resources_id: int) -> Any:
    """Return an task_resources."""
    structlog.contextvars.bind_contextvars(task_resources_id=task_resources_id)

    with SessionLocal() as session:
        with session.begin():
            select_stmt = (
                select(TaskResources.requested_resources, Queue.name)
                .join_from(TaskResources, Queue, TaskResources.queue_id == Queue.id)
                .where(TaskResources.id == task_resources_id)
            )
            row = session.execute(select_stmt).fetchone()
            requested_resources_raw, queue_name = row if row else (None, None)
            requested_resources = (
                ast.literal_eval(requested_resources_raw)
                if requested_resources_raw
                else None
            )

    resp = JSONResponse(
        content={"requested_resources": requested_resources, "queue_name": queue_name},
        status_code=StatusCodes.OK,
    )
    return resp
