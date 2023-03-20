"""Routes used by the main jobmon client."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import insert, select
import structlog


from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint


logger = structlog.get_logger(__name__)


@blueprint.route("/nodes", methods=["POST"])
def add_nodes() -> Any:
    """Add a chunk of nodes to the database.

    Args:
        nodes: a list of
            node_args_hash: unique identifier of all NodeArgs associated with a node.
        task_template_version_id: version id of the task_template a node belongs to.
        node_args: key-value pairs of arg_id and a value.
    """
    data = cast(Dict, request.get_json())
    # Extract node and node_args

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    session = SessionLocal()
    with session.begin():
        node_keys = [
            (n["task_template_version_id"], n["node_args_hash"]) for n in data["nodes"]
        ]
        node_insert_stmt = insert(Node).values(
            [
                {"task_template_version_id": ttv, "node_args_hash": arghash}
                for ttv, arghash in node_keys
            ]
        )
        if SessionLocal.bind.dialect.name == "mysql":
            node_insert_stmt = node_insert_stmt.prefix_with("IGNORE")
        if SessionLocal.bind.dialect.name == "sqlite":
            node_insert_stmt = node_insert_stmt.prefix_with("OR IGNORE")

        session.execute(node_insert_stmt)
        session.flush()

        # Retrieve the node IDs
        ttvids, node_arg_hashes = zip(*node_keys)
        select_stmt = select(Node).where(
            Node.task_template_version_id.in_(ttvids),
            Node.node_args_hash.in_(node_arg_hashes),
        )
        nodes = session.execute(select_stmt).scalars().all()

        node_id_dict = {
            (n.task_template_version_id, n.node_args_hash): n.id for n in nodes
        }

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
                node_args_list.append(
                    {"node_id": node_id, "arg_id": arg_id, "val": val}
                )

        # Bulk insert again with raw SQL
        if node_args_list:
            node_arg_insert_stmt = insert(NodeArg).values(node_args_list)
            if SessionLocal.bind.dialect.name == "mysql":
                node_arg_insert_stmt = node_arg_insert_stmt.prefix_with("IGNORE")
            if SessionLocal.bind.dialect.name == "sqlite":
                node_arg_insert_stmt = node_arg_insert_stmt.prefix_with("OR IGNORE")

            session.execute(node_arg_insert_stmt)
            session.flush()

    # return result
    return_nodes = {
        ":".join(str(i) for i in key): val for key, val in node_id_dict.items()
    }
    resp = jsonify(nodes=return_nodes)
    resp.status_code = StatusCodes.OK
    return resp
