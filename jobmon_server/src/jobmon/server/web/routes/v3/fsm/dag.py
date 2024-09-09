"""Routes for DAGs."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from fastapi import Request
import sqlalchemy
from sqlalchemy import insert, select, update
from sqlalchemy.sql import func
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.db_admin import SessionLocal
from jobmon.server.web.server_side_exception import InvalidUsage


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)

@api_v3_router.post("/dag")
async  def add_dag(request: Request) -> Any:
    """Add a new dag to the database.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    data = await cast(Dict, request.json())

    # add dag
    dag_hash = data.pop("dag_hash")
    structlog.contextvars.bind_contextvars(dag_hash=str(dag_hash))
    logger.info(f"Add dag:{dag_hash}")

    session = SessionLocal()

    try:
        with session.begin():
            dag = Dag(hash=dag_hash)
            session.add(dag)

    except sqlalchemy.exc.IntegrityError:
        with session.begin():
            select_stmt = select(Dag).filter(Dag.hash == dag_hash)
            dag = session.execute(select_stmt).scalar_one()

    # return result
    resp = JSONResponse(content={"dag_id": dag.id, "created_date": dag.created_date},
                        status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/dag/{dag_id}/edges")
async def add_edges(dag_id: int, request: Request) -> Any:
    """Add edges to the edge table."""
    structlog.contextvars.bind_contextvars(dag_id=dag_id)
    logger.info(f"Add edges for dag {dag_id}")
    try:
        data = cast(Dict, await request.json())
        edges_to_add = data.pop("edges_to_add")
        mark_created = bool(data.pop("mark_created"))
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    # add dag and cast types
    for edges in edges_to_add:
        edges["dag_id"] = dag_id
        if len(edges["upstream_node_ids"]) == 0:
            edges["upstream_node_ids"] = None
        else:
            edges["upstream_node_ids"] = str(edges["upstream_node_ids"])

        if len(edges["downstream_node_ids"]) == 0:
            edges["downstream_node_ids"] = None
        else:
            edges["downstream_node_ids"] = str(edges["downstream_node_ids"])

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    session = SessionLocal()
    with session.begin():
        insert_stmt = insert(Edge).values(edges_to_add)
        if (
            SessionLocal
            and SessionLocal.bind
            and SessionLocal.bind.dialect.name == "mysql"
        ):
            insert_stmt = insert_stmt.prefix_with("IGNORE")
        if (
            SessionLocal
            and SessionLocal.bind
            and SessionLocal.bind.dialect.name == "sqlite"
        ):
            insert_stmt = insert_stmt.prefix_with("OR IGNORE")
        session.execute(insert_stmt)
        session.flush()

        if mark_created:
            update_stmt = (
                update(Dag).where(Dag.id == dag_id).values(created_date=func.now())
            )
            session.execute(update_stmt)

    # return result
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp
