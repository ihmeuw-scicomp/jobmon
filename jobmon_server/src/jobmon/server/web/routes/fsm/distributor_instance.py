"""Routes for Arrays."""
from http import HTTPStatus as StatusCodes
import random
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import func, select
import structlog

from jobmon.server.web.models.api import (
    Batch,
    DistributorInstance,
    DistributorInstanceCluster,
    TaskInstance,
    WorkflowRun
)
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


logger = structlog.get_logger(__name__)


@blueprint.route("/distributor_instance/register", methods=["POST"])
def register_distributor_instance() -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = cast(Dict, request.get_json())
    cluster_ids = [int(cluster_id) for cluster_id in data["cluster_ids"]]
    next_report = data["next_report_increment"]

    session = SessionLocal()
    with session.begin():

        distributor_instance = DistributorInstance()
        session.add(distributor_instance)
        session.flush()
        distributor_instance_id = distributor_instance.id

        for cluster_id in cluster_ids:
            session.add(
                DistributorInstanceCluster(
                    distributor_instance_id=distributor_instance_id,
                    cluster_id=cluster_id,
                )
            )

        distributor_instance.heartbeat(next_report)

    resp = jsonify(distributor_instance_id=distributor_instance_id)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/distributor_instance/<distributor_instance_id>/heartbeat", methods=["POST"]
)
def log_heartbeat_distributor_instance(distributor_instance_id: int) -> Any:
    """Record a heartbeat for a distributor instance."""
    distributor_instance_id = int(distributor_instance_id)
    data = cast(Dict, request.get_json())
    next_report = data["next_report_increment"]

    session = SessionLocal()
    with session.begin():

        distributor_instance = session.get(DistributorInstance, distributor_instance_id)
        expunged = distributor_instance.expunged
        if not expunged:
            distributor_instance.heartbeat(next_report)

    resp = jsonify(expunged=expunged)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/distributor_instance/expunge", methods=["PUT"])
def expunge_distributor_instances() -> Any:

    data = cast(Dict, request.get_json())
    cluster_id = data["cluster_id"]

    session = SessionLocal()
    with session.begin():
        # Join from the mapping of distributorinstanceclusters

        select_stmt = (
            select(
                DistributorInstance
            ).join(
                DistributorInstanceCluster, isouter=True
            ).where(
                DistributorInstanceCluster.cluster_id == cluster_id,
                DistributorInstance.report_by_date <= func.now(),
                DistributorInstance.expunged.is_(False),
            )
        )
        to_expunge = session.execute(select_stmt).scalars().all()
        # Consider chunking, or bypassing the ORM
        for distributor_instance in to_expunge:
            distributor_instance.expunge()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/distributor_instance/<distributor_instance_id>/sync_status", methods=["POST"]
)
def task_instances_status_check(distributor_instance_id: int) -> Any:
    """Sync status of given task intance IDs."""
    structlog.contextvars.bind_contextvars(
        distributor_instance_id=distributor_instance_id
    )
    try:
        distributor_instance_id = int(distributor_instance_id)
        data = cast(Dict, request.get_json())
        task_instance_ids = data["task_instance_ids"]
        status = data["status"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    session = SessionLocal()
    with session.begin():

        # get time from db
        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")

        where_clause = [
            TaskInstance.batch_id == Batch.id,
            Batch.distributor_instance_id == distributor_instance_id,
            # Filter out task instances belonging to inactive workflowruns.
            # Distributor service will decide what to do with newly inactive task instances
            TaskInstance.workflow_run_id == WorkflowRun.id,
            WorkflowRun.status.in_(WorkflowRun.active_states),
        ]
        if len(task_instance_ids) > 0:
            # Filters for
            # 1) instances that have changed out of the declared status
            # 2) instances that have changed into the declared status
            where_clause.append(
                (
                    TaskInstance.id.in_(task_instance_ids)
                    & (TaskInstance.status != status)
                )
                | (
                    TaskInstance.id.notin_(task_instance_ids)
                    & (TaskInstance.status == status)
                )
            )
        else:
            where_clause.append(TaskInstance.status == status)

        select_stmt = select(
            TaskInstance.id,
            TaskInstance.status,
        ).join(
            WorkflowRun
        ).join(
            Batch
        ).where(
            *where_clause
        )

        task_instances = session.execute(select_stmt).all()
        return_val = list(map(tuple, task_instances))

    resp = jsonify(status_updates=return_val, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/distributor_instance/<cluster_id>/get_active_distributor_instance_id",
    methods=["GET"]
)
def get_active_distributor_instance_id(cluster_id: int) -> Any:

    active_id = _get_active_distributor_instance_id(cluster_id)
    resp = jsonify(distributor_instance_id=active_id)
    resp.status_code = StatusCodes.OK
    return resp


def _get_active_distributor_instance_id(cluster_id: int):

    select_stmt = (
        select(
            DistributorInstance.id,
        )
        .join(
            DistributorInstanceCluster, isouter=True,
        )
        .where(
            DistributorInstance.expunged.is_(False),
            DistributorInstance.report_by_date >= func.now(),
            DistributorInstanceCluster.cluster_id == cluster_id,
        )
    )

    session = SessionLocal()
    with session.begin():
        distributor_instance_ids = session.execute(select_stmt).scalars().all()

    if not any(distributor_instance_ids):
        # No candidates are available
        # TODO: How to handle? Retry?
        raise ServerError(f"No distributors are alive for {cluster_id=}")

    return random.choice(distributor_instance_ids)
