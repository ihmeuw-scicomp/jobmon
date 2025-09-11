"""Routes for DAGs."""

from http import HTTPStatus as StatusCodes
from typing import Any, Dict, cast

import sqlalchemy
import structlog
from fastapi import Depends, Request
from sqlalchemy import insert, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from starlette.responses import JSONResponse

from jobmon.server.web.db import get_dialect_name
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)
DIALECT = get_dialect_name()


@api_v3_router.post("/dag")
async def add_dag(request: Request, db: Session = Depends(get_db)) -> Any:
    """Add a new dag to the database.

    Args:
        request: The request object.
        db: The database session.
    """
    data = cast(Dict, await request.json())

    # add dag
    dag_hash = data.pop("dag_hash")
    structlog.contextvars.bind_contextvars(dag_hash=str(dag_hash))
    logger.info(f"Add dag:{dag_hash}")

    try:
        dag = Dag(hash=dag_hash)
        db.add(dag)
        db.flush()
    except sqlalchemy.exc.IntegrityError:
        db.rollback()
        select_stmt = select(Dag).filter(Dag.hash == dag_hash)
        dag = db.execute(select_stmt).scalar_one()

    # return result
    if dag.created_date:
        resp = JSONResponse(
            content={"dag_id": dag.id, "created_date": str(dag.created_date)},
            status_code=StatusCodes.OK,
        )
    else:
        resp = JSONResponse(
            content={"dag_id": dag.id, "created_date": None},
            status_code=StatusCodes.OK,
        )
    return resp


@api_v3_router.post("/dag/{dag_id}/edges")
async def add_edges(
    dag_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Add edges to the edge table."""
    structlog.contextvars.bind_contextvars(dag_id=dag_id)
    logger.info(f"Add edges for dag {dag_id}")
    try:
        data = cast(Dict, await request.json())
        edges_to_add = data.pop("edges_to_add")
        mark_created = bool(data.pop("mark_created"))
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    # add dag and cast types
    for edge in edges_to_add:
        edge["dag_id"] = dag_id
        edge["upstream_node_ids"] = edge["upstream_node_ids"] or None
        edge["downstream_node_ids"] = edge["downstream_node_ids"] or None

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    insert_stmt = insert(Edge).values(edges_to_add)
    if DIALECT == "mysql":
        insert_stmt = insert_stmt.prefix_with("IGNORE")
    elif DIALECT == "sqlite":
        insert_stmt = insert_stmt.prefix_with("OR IGNORE")
    else:
        raise ServerError(f"Unsupported SQL dialect '{DIALECT}'")
    db.execute(insert_stmt)

    if mark_created:
        update_stmt = (
            update(Dag).where(Dag.id == dag_id).values(created_date=func.now())
        )
        db.execute(update_stmt)

    # return result
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp
