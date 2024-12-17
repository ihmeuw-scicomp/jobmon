"""Routes for DAGs."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from fastapi import Request
import pandas as pd
import sqlalchemy
from sqlalchemy import insert, select, update
from sqlalchemy.sql import func
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.node import Node
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()
_CONFIG = get_jobmon_config()


@api_v3_router.post("/dag")
async def add_dag(request: Request) -> Any:
    """Add a new dag to the database.

    Args:
        request: The request object.
    """
    data = cast(Dict, await request.json())

    # add dag
    dag_hash = data.pop("dag_hash")
    structlog.contextvars.bind_contextvars(dag_hash=str(dag_hash))
    logger.info(f"Add dag:{dag_hash}")

    with SessionLocal() as session:
        try:
            with session.begin():
                dag = Dag(hash=dag_hash)
                session.add(dag)
                ()
        except sqlalchemy.exc.IntegrityError:
            with session.begin():
                select_stmt = select(Dag).filter(Dag.hash == dag_hash)
                dag = session.execute(select_stmt).scalar_one()

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
            f"{str(e)} in request to {request.url.path}", status_code=400
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
    with SessionLocal() as session:
        with session.begin():
            insert_stmt = insert(Edge).values(edges_to_add)
            if SessionLocal and "mysql" in _CONFIG.get("db", "sqlalchemy_database_uri"):
                insert_stmt = insert_stmt.prefix_with("IGNORE")
            if SessionLocal and "sqlite" in _CONFIG.get(
                    "db", "sqlalchemy_database_uri"
            ):
                insert_stmt = insert_stmt.prefix_with("OR IGNORE")
            session.execute(insert_stmt)
            ()

        if mark_created:
            update_stmt = (
                update(Dag).where(Dag.id == dag_id).values(created_date=func.now())
            )
            session.execute(update_stmt)

    # return result
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/dag/{dag_id}/task_template_dag")
async def task_template_dag(dag_id: int) -> Any:
    with SessionLocal() as session:
        with session.begin():
            query = session.query(
                Edge.node_id,
                Edge.upstream_node_ids,
                Edge.downstream_node_ids,
                TaskTemplate.name
            ).join(
                Node, Edge.node_id == Node.id
            ).join(
                TaskTemplateVersion, Node.task_template_version_id == TaskTemplateVersion.id
            ).join(
                TaskTemplate, TaskTemplateVersion.task_template_id == TaskTemplate.id
            ).filter(
                Edge.dag_id == dag_id
            )

            res = session.execute(query)

            results_list = [
                {
                    "node_id": row.node_id,
                    "upstream_node_ids": row.upstream_node_ids,
                    "downstream_node_ids": row.downstream_node_ids,
                    "name": row.name
                }
                for row in res
            ]

    df = pd.DataFrame(results_list)
    task_template_lookup = df[["node_id", "name"]]
    df["downstream_node_ids"] = (
        df["downstream_node_ids"]
        .fillna("[]")
        .str.rstrip('"]')
        .str.lstrip('["')
        .str.split(",")
    )

    df = df.explode("downstream_node_ids")[["node_id", "downstream_node_ids"]]
    df = df.loc[df["downstream_node_ids"] != ""].astype(int)
    df = df.merge(task_template_lookup, on="node_id", how="left")
    df = df.merge(
        task_template_lookup.rename(
            columns={
                "name": "downstream_task_template_id",
                "node_id": "downstream_node_ids",
            }
        ),
        how="left",
        on="downstream_node_ids",
    )

    df = df[["name", "downstream_task_template_id"]].drop_duplicates()

    resp = JSONResponse(
        content={"tt_dag": df.to_dict(orient="records")},
        status_code=StatusCodes.OK,
    )

    return resp
