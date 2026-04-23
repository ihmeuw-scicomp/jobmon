"""Routes for Task Resources."""

from http import HTTPStatus as StatusCodes
from typing import Any, List, Optional

import structlog
from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core.logging import set_jobmon_context
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.utils.json_compat import deserialize_requested_resources

logger = structlog.get_logger(__name__)

# Reject oversized batch requests — prevents a buggy/malicious client
# from triggering a 100k-IN query.
MAX_BATCH_IDS = 1000


class TaskResourcesBatchRequest(BaseModel):
    """Request body for ``/task_resources/batch``."""

    task_resources_ids: List[int]


class TaskResourcesBatchItem(BaseModel):
    """One entry in the batch response."""

    task_resources_id: int
    requested_resources: Any = None
    queue_name: Optional[str] = None


class TaskResourcesBatchResponse(BaseModel):
    """Response for ``/task_resources/batch``."""

    resources: List[TaskResourcesBatchItem]


@api_v3_router.post(
    "/task_resources/batch",
    response_model=TaskResourcesBatchResponse,
)
def get_task_resources_batch(
    request_data: TaskResourcesBatchRequest,
    db: Session = Depends(get_db),
) -> TaskResourcesBatchResponse:
    """Return ``requested_resources`` + ``queue_name`` for many ids in one query."""
    ids = list(set(request_data.task_resources_ids))
    if not ids:
        return TaskResourcesBatchResponse(resources=[])
    if len(ids) > MAX_BATCH_IDS:
        raise HTTPException(
            status_code=StatusCodes.BAD_REQUEST,
            detail=f"task_resources_ids exceeds {MAX_BATCH_IDS}",
        )

    rows = db.execute(
        select(
            TaskResources.id,
            TaskResources.requested_resources,
            Queue.name,
        )
        .outerjoin(Queue, TaskResources.queue_id == Queue.id)
        .where(TaskResources.id.in_(ids))
    ).all()

    items: List[TaskResourcesBatchItem] = []
    for tr_id, raw, queue_name in rows:
        try:
            parsed: Any = deserialize_requested_resources(raw)
        except (ValueError, SyntaxError):
            parsed = None
        items.append(
            TaskResourcesBatchItem(
                task_resources_id=int(tr_id),
                requested_resources=parsed,
                queue_name=queue_name,
            )
        )
    return TaskResourcesBatchResponse(resources=items)


# NOTE: keep this route below ``/task_resources/batch`` so FastAPI
# matches the literal ``batch`` path before falling through to the
# generic path-param route.
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
        deserialize_requested_resources(requested_resources_raw)
        if requested_resources_raw
        else None
    )

    resp = JSONResponse(
        content={"requested_resources": requested_resources, "queue_name": queue_name},
        status_code=StatusCodes.OK,
    )
    return resp
