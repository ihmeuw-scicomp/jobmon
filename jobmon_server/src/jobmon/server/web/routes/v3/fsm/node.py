"""Routes used by the main jobmon client."""

from http import HTTPStatus as StatusCodes
from time import sleep
from typing import Any, Dict, cast

import structlog
from fastapi import Depends, Request
from sqlalchemy import insert, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.server.web.db import get_dialect_name
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import ServerError

logger = structlog.get_logger(__name__)
DIALECT = get_dialect_name()


@api_v3_router.post("/nodes")
async def add_nodes(request: Request, db: Session = Depends(get_db)) -> Any:
    """Add a chunk of nodes to the database.

    Args:
        request: The request object.
        db: The database session.
    """
    data = cast(Dict, await request.json())
    # Extract node and node_args

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    node_keys = [
        (n["task_template_version_id"], n["node_args_hash"]) for n in data["nodes"]
    ]
    node_insert_stmt = insert(Node).values(
        [
            {"task_template_version_id": ttv, "node_args_hash": arghash}
            for ttv, arghash in node_keys
        ]
    )
    if DIALECT == "mysql":
        node_insert_stmt = node_insert_stmt.prefix_with("IGNORE")
    elif DIALECT == "sqlite":
        node_insert_stmt = node_insert_stmt.prefix_with("OR IGNORE")
    else:
        raise ServerError(f"Unsupported SQL dialect '{DIALECT}'")

    # Retry logic for deadlock handling
    max_retries = 5

    for attempt in range(max_retries):
        try:
            db.execute(node_insert_stmt)
            db.flush()
            break  # Success, exit retry loop
        except OperationalError as e:
            if (
                "database is locked" in str(e)
                or "Deadlock found" in str(e)
                or "Lock wait timeout" in str(e)
                or "could not obtain lock" in str(e)
            ):
                logger.warning(
                    f"Database error detected for node insert, retrying attempt "
                    f"{attempt + 1}/{max_retries}. {e}"
                )
                db.rollback()  # Clear the corrupted session state
                sleep(
                    0.001 * (2 ** (attempt + 1))
                )  # Exponential backoff: 2ms, 4ms, 8ms, 16ms, 32ms
            else:
                logger.error(f"Unexpected database error inserting nodes: {e}")
                db.rollback()
                raise e
        except Exception as e:
            logger.error(f"Failed to insert nodes: {e}")
            db.rollback()
            raise e

    # Retrieve the node IDs
    ttvids, node_arg_hashes = zip(*node_keys)
    select_stmt = select(Node).where(
        Node.task_template_version_id.in_(ttvids),
        Node.node_args_hash.in_(node_arg_hashes),
    )
    nodes = db.execute(select_stmt).scalars().all()

    node_id_dict = {(n.task_template_version_id, n.node_args_hash): n.id for n in nodes}

    # Add node args. Cast hash to string to match DB schema
    node_args = {
        (n["task_template_version_id"], n["node_args_hash"]): n["node_args"]
        for n in data["nodes"]
    }

    node_args_list = []
    for node_id_tuple, arg in node_args.items():
        node_id = node_id_dict[node_id_tuple]
        local_logger = logger.bind(node_id=node_id)

        for arg_id, val in arg.items():
            local_logger.debug(
                "Adding node_arg", node_id=node_id, arg_id=arg_id, val=val
            )
            node_args_list.append({"node_id": node_id, "arg_id": arg_id, "val": val})

    # Bulk insert again with raw SQL. Pass the same session.
    _insert_node_args(node_args_list, db)

    # return result
    return_nodes = {
        ":".join(str(i) for i in key): val for key, val in node_id_dict.items()
    }
    resp = JSONResponse(content={"nodes": return_nodes}, status_code=StatusCodes.OK)
    return resp


def _insert_node_args(node_args_list: list, db: Session) -> None:
    if node_args_list:
        node_arg_insert_stmt = insert(NodeArg).values(node_args_list)
        if DIALECT == "mysql":
            node_arg_insert_stmt = node_arg_insert_stmt.prefix_with("IGNORE")
        elif DIALECT == "sqlite":
            node_arg_insert_stmt = node_arg_insert_stmt.prefix_with("OR IGNORE")
        else:
            raise ServerError(f"Unsupported SQL dialect '{DIALECT}'")

        db.execute(node_arg_insert_stmt)
