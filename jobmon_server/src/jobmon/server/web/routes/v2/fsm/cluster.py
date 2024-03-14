"""Routes for Clusters."""

from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from sqlalchemy import select

from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.routes.v1 import api_v1_blueprint
from jobmon.server.web.routes.v2 import api_v2_blueprint
from jobmon.server.web.routes.v2 import SessionLocal


@api_v1_blueprint.route("/cluster/<cluster_name>", methods=["GET"])
@api_v2_blueprint.route("/cluster/<cluster_name>", methods=["GET"])
def get_cluster_by_name(cluster_name: str) -> Any:
    """Get the id, cluster_type_name and connection_parameters of a Cluster."""
    session = SessionLocal()
    with session.begin():
        select_stmt = (
            select(Cluster)
            .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)
            .filter(Cluster.name == cluster_name)
        )
        cluster = session.execute(select_stmt).scalars().one_or_none()

    # send back json
    if cluster is None:
        resp = jsonify(cluster=None)
    else:
        resp = jsonify(cluster=cluster.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
