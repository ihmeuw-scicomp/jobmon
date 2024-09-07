"""Routes for Clusters."""

from http import HTTPStatus as StatusCodes
from typing import Any

from sqlalchemy import select
from starlette.responses import JSONResponse

from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.api import SessionLocal

@api_v3_router.get("/cluster/{cluster_name}")
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
        resp = JSONResponse(content={"cluster": None}, status_code=StatusCodes.OK)
    else:
        resp = JSONResponse(content={"cluster": cluster}, status_code=StatusCodes.OK)
    resp.status_code = StatusCodes.OK
    return resp
