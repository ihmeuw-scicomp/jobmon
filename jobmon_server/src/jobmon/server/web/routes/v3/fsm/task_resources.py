"""Routes for Task Resources."""

import ast
import json
from http import HTTPStatus as StatusCodes
from typing import Any

import structlog
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core.logging import set_jobmon_context
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router

logger = structlog.get_logger(__name__)


def _deserialize_requested_resources(raw: str) -> Any:
    """Deserialize a requested_resources column value.

    New rows are written via ``json.dumps`` by ``/task/bind_resources``, which
    emits JSON (lowercase ``true``/``false``/``null``). Older rows may be
    Python ``repr``-style (``True``/``False``/``None``) from pre-FastAPI
    versions. Try JSON first, fall back to ``ast.literal_eval``.
    """
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return ast.literal_eval(raw)


@api_v3_router.post("/task_resources/{task_resources_id}")
def get_task_resources(task_resources_id: int, db: Session = Depends(get_db)) -> Any:
    """Return an task_resources."""
    set_jobmon_context(task_resources_id=task_resources_id)

    select_stmt = (
        select(TaskResources.requested_resources, Queue.name)
        .join_from(TaskResources, Queue, TaskResources.queue_id == Queue.id)
        .where(TaskResources.id == task_resources_id)
    )
    row = db.execute(select_stmt).fetchone()
    requested_resources_raw, queue_name = row if row else (None, None)
    requested_resources = (
        _deserialize_requested_resources(requested_resources_raw)
        if requested_resources_raw
        else None
    )

    resp = JSONResponse(
        content={"requested_resources": requested_resources, "queue_name": queue_name},
        status_code=StatusCodes.OK,
    )
    return resp
