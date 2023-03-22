"""Routes for Arrays."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import bindparam, case, func, insert, literal_column, select, update
import structlog

from jobmon.core.constants import TaskInstanceStatus
from jobmon.server.web._compat import add_time
from jobmon.server.web.models.api import (
    Array,
    Batch,
    Task,
    TaskInstance,
    TaskStatus,
    TaskResources,
)
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.routes.fsm.distributor_instance import (
    _get_active_distributor_instance_id,
)


logger = structlog.get_logger(__name__)


@blueprint.route("/batch/queue_task_batch", methods=["POST"])
def queue_task_batch() -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = cast(Dict, request.get_json())
    task_ids = [int(task_id) for task_id in data["task_ids"]]
    task_resources_id = int(data["task_resources_id"])
    workflow_run_id = int(data["workflow_run_id"])
    array_id = int(data["array_id"])
    cluster_id = int(data["cluster_id"])
    distributor_instance_id = data["distributor_instance_id"]

    if distributor_instance_id is None:
        # Fetch a random active distributor instance id if not known
        distributor_instance_id = _get_active_distributor_instance_id(cluster_id)

    session = SessionLocal()
    with session.begin():

        task_resources = session.get(TaskResources, task_resources_id)
        batch = Batch(
            cluster_id=task_resources.queue.cluster.id,
            task_resources_id=task_resources_id,
            distributor_instance_id=distributor_instance_id,
            workflow_run_id=workflow_run_id,
            array_id=array_id,
        )
        session.add(batch)
        session.flush()

        # update task status to acquire lock
        update_stmt = (
            update(Task)
            .where(
                Task.id.in_(task_ids),
                Task.status.in_(
                    [TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]
                ),
            )
            .values(
                status=TaskStatus.QUEUED,
                status_date=func.now(),
                num_attempts=(Task.num_attempts + 1),
            )
        )
        session.execute(update_stmt)

        # now insert them into task instance
        insert_stmt = insert(TaskInstance).from_select(
            # columns map 1:1 to selected rows
            [
                "task_id",
                "workflow_run_id",
                "array_id",
                "batch_id",
                "array_step_id",  # TODO: rename in 4.0 to batch_index
                "status",
                "status_date",
            ],
            # select statement
            select(
                # unique id
                Task.id.label("task_id"),
                # static associations
                literal_column(str(workflow_run_id)).label("workflow_run_id"),
                literal_column(str(array_id)).label("array_id"),
                # batch info
                literal_column(str(batch.id)).label("batch_id"),
                (func.row_number().over(order_by=Task.id) - 1).label("array_step_id"),
                # status columns
                literal_column(f"'{TaskInstanceStatus.QUEUED}'").label("status"),
                func.now().label("status_date"),
            ).where(Task.id.in_(task_ids), Task.status == TaskStatus.QUEUED),
            # no python side defaults. Server defaults only
            include_defaults=False,
        )
        session.execute(insert_stmt)

    with session.begin():
        tasks_by_status_query = (
            select(Task.status, func.group_concat(Task.id))
            .where(Task.id.in_(task_ids))
            .group_by(Task.status)
        )
        result_dict = {}
        for row in session.execute(tasks_by_status_query):
            result_dict[row[0]] = [int(i) for i in row[1].split(",")]

        batch_id = batch.id

    resp = jsonify(tasks_by_status=result_dict, batch_id=batch_id)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/batch/<batch_id>/transition_to_launched", methods=["POST"])
def transition_batch_to_launched(batch_id: int) -> Any:
    """Transition TIs associated with an array_id and batch_num to launched."""
    batch_id = int(batch_id)

    structlog.contextvars.bind_contextvars(batch_id=batch_id)
    data = cast(Dict, request.get_json())
    next_report = data["next_report_increment"]

    session = SessionLocal()
    with session.begin():

        batch = session.get(Batch, batch_id)
        # Acquire a lock and update tasks to launched
        task_ids_query = (
            select(TaskInstance.task_id)
            .where(
                TaskInstance.workflow_run_id == batch.workflow_run_id,
                TaskInstance.array_batch_num == batch_id,
            )
            .execution_options(synchronize_session=False)
        )

        task_ids = session.execute(task_ids_query).scalars()

        update_task_stmt = (
            update(Task)
            .where(
                Task.workflow_id == batch.workflow_run.workflow_id,
                Task.array_id == batch.array_id,
                Task.id.in_(task_ids),
                Task.status == TaskStatus.QUEUED,
            )
            .values(status=TaskStatus.LAUNCHED, status_date=func.now())
        ).execution_options(synchronize_session=False)
        session.execute(update_task_stmt)

        # Transition all the task instances in the batch
        # Bypassing the ORM for performance reasons.
        update_stmt = (
            update(TaskInstance)
            .where(
                TaskInstance.workflow_run_id == batch.workflow_run.id,
                TaskInstance.array_id == batch.array_id,
                TaskInstance.status == TaskInstanceStatus.QUEUED,
                TaskInstance.array_batch_num == batch_id,
            )
            .values(
                status=TaskInstanceStatus.LAUNCHED,
                submitted_date=func.now(),
                status_date=func.now(),
                report_by_date=add_time(next_report),
            )
        ).execution_options(synchronize_session=False)

        session.execute(update_stmt)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/batch/<batch_id>/log_distributor_id", methods=["POST"])
def log_array_distributor_id(batch_id: int) -> Any:
    """Add distributor_id to the DB for all TIs in an array."""
    batch_id = int(batch_id)
    data = cast(Dict, request.get_json())

    # Create a list of dicts out of the distributor id map.
    params = [
        {"task_instance__id": key, "distributor_id": val} for key, val in data.items()
    ]

    session = SessionLocal()
    with session.begin():
        batch = session.get(Batch, batch_id)

        # Using bindparam only issues one query; unfortunately, the MariaDB optimizer actually
        # performs this operation iteratively. The update is fairly slow despite the fact that
        # we are issuing a single bulk query.
        update_stmt = (
            update(TaskInstance)
            .where(
                TaskInstance.workflow_run_id == batch.workflow_run.id,
                TaskInstance.array_id == batch.array_id,
                TaskInstance.array_batch_num == batch_id,
                TaskInstance.id == bindparam("task_instance_id"),
            )
            .values(
                distributor_id=bindparam("distributor_id"),
                stdout=case(
                    [TaskInstance.stdout.is_(None), bindparam("stdout")],
                    [TaskInstance.stdout.is_not(None), TaskInstance.stdout],
                ),
                stderr=case(
                    [TaskInstance.stderr.is_(None), bindparam("stderr")],
                    [TaskInstance.stderr.is_not(None), TaskInstance.stderr],
                ),
            )
            .execution_options(synchronize_session=False)
        )
        session.connection().execute(update_stmt, params)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/batch/get_batches", methods=["GET"])
def get_batches():
    data = cast(Dict, request.get_json())
    batch_ids = data["batch_ids"]

    select_stmt = select(
        Batch.id, Batch.task_resources_id, Batch.array_id, Array.name
    ).where(Batch.array_id == Array.id, Batch.id.in_(batch_ids))

    session = SessionLocal()
    with session.begin():
        batches = session.execute(select_stmt).scalars()

    resp = jsonify(batches=batches)
    resp.status_code = StatusCodes.OK
    return resp
