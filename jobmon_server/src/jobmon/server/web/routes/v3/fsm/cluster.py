"""Routes for Clusters."""

from http import HTTPStatus as StatusCodes
from typing import Any

from sqlalchemy import select
from starlette.responses import JSONResponse

from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router

SessionLocal = get_session_local()


@api_v3_router.get("/cluster/{cluster_name}")
def get_cluster_by_name(cluster_name: str) -> Any:
    """Get the id, cluster_type_name and connection_parameters of a Cluster."""
    with SessionLocal() as session:
        with session.begin():
            select_stmt = (
                select(Cluster, ClusterType.name)
                .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)
                .filter(Cluster.name == cluster_name)
            )
            result = session.execute(select_stmt).one_or_none()

        # send back json
        if result is None:
            resp = JSONResponse(content={"cluster": None}, status_code=StatusCodes.OK)
        else:
            cluster, cluster_type_name = result
            cluster_list = [
                cluster.id,
                cluster.name,
                cluster_type_name,  # Access the cluster type name here
                cluster.connection_parameters,
            ]
            resp = JSONResponse(
                content={"cluster": cluster_list}, status_code=StatusCodes.OK
            )
        return resp
