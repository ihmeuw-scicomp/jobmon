"""Routes for Clusters."""

from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from sqlalchemy import select

from jobmon.server.web.models.queue import Queue
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.api import SessionLocal


@api_v3_router.get("/cluster/<cluster_id>/queue/<queue_name>")
def get_queue_by_cluster_queue_names(cluster_id: int, queue_name: str) -> Any:
    """Get the id, name, cluster_name and parameters of a Queue.

    Based on cluster_name and queue_name.
    """
    session = SessionLocal()
    with session.begin():
        select_stmt = select(Queue).where(
            Queue.cluster_id == cluster_id, Queue.name == queue_name
        )
        queue = session.execute(select_stmt).scalars().one_or_none()

    # send back json
    if queue is None:
        resp = jsonify(queue=None)
    else:
        resp = jsonify(queue=queue.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
