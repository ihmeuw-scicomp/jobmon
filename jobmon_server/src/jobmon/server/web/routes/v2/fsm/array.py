"""Routes for Arrays."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import and_, case, func, insert, literal_column, select, update
import structlog

from jobmon.core.constants import TaskInstanceStatus
from jobmon.server.web._compat import add_time
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.routes.v1 import api_v1_blueprint
from jobmon.server.web.routes.v2 import api_v2_blueprint
from jobmon.server.web.routes.v2 import SessionLocal

logger = structlog.get_logger(__name__)


@api_v1_blueprint.route("/array", methods=["POST"])
@api_v2_blueprint.route("/array", methods=["POST"])
def add_array() -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = cast(Dict, request.get_json())
    workflow_id = int(data["workflow_id"])
    task_template_version_id = int(data["task_template_version_id"])

    structlog.contextvars.bind_contextvars(
        task_template_version_id=task_template_version_id,
        workflow_id=workflow_id,
    )

    # Check if the array is already bound, if so return it
    session = SessionLocal()
    with session.begin():
        select_stmt = select(Array).where(
            Array.workflow_id == workflow_id,
            Array.task_template_version_id == task_template_version_id,
        )
        array = session.execute(select_stmt).scalars().one_or_none()

        if array is None:  # not found, so need to add it
            array = Array(
                task_template_version_id=data["task_template_version_id"],
                workflow_id=data["workflow_id"],
                max_concurrently_running=data["max_concurrently_running"],
                name=data["name"],
            )
            session.add(array)
        else:
            update_stmt = (
                update(Array)
                .where(Array.id == array.id)
                .values(max_concurrently_running=data["max_concurrently_running"])
            )
            session.execute(update_stmt)
        session.commit()

    # return result
    resp = jsonify(array_id=array.id)
    resp.status_code = StatusCodes.OK
    return resp


@api_v1_blueprint.route("/array/<array_id>/queue_task_batch", methods=["POST"])
@api_v2_blueprint.route("/array/<array_id>/queue_task_batch", methods=["POST"])
def record_array_batch_num(array_id: int) -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = cast(Dict, request.get_json())
    array_id = int(array_id)
    task_ids = [int(task_id) for task_id in data["task_ids"]]
    task_resources_id = int(data["task_resources_id"])
    workflow_run_id = int(data["workflow_run_id"])
    task_condition = and_(
        Task.id.in_(task_ids),
        Task.status.in_([TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]),
    )

    session = SessionLocal()
    with session.begin():
        # Acquire locks on tasks to be updated
        task_locks = (
            select(Task.id)
            .where(task_condition)
            .with_for_update()
            .execution_options(synchronize_session=False)
        )
        session.execute(task_locks)

        # update task status to acquire lock
        update_stmt = (
            update(Task)
            .where(task_condition)
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
                "task_resources_id",
                "array_batch_num",
                "array_step_id",
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
                literal_column(str(task_resources_id)).label("task_resources_id"),
                # batch info
                select(func.coalesce(func.max(TaskInstance.array_batch_num) + 1, 1))
                .where((TaskInstance.array_id == array_id))
                .label("array_batch_num"),
                (func.row_number().over(order_by=Task.id) - 1).label("array_step_id"),
                # status columns
                literal_column(f"'{TaskInstanceStatus.QUEUED}'").label("status"),
                func.now().label("status_date"),
            )
            .where(Task.id.in_(task_ids), Task.status == TaskStatus.QUEUED)
            .with_for_update(),
            # no python side defaults. Server defaults only
            include_defaults=False,
        )
        session.execute(insert_stmt)

    with session.begin():
        tasks_by_status_query = (
            select(Task.status, Task.id)
            .where(Task.id.in_(task_ids))
            .with_for_update()
            .order_by(
                Task.status
            )  # This line is optional but helps in organizing the result
        )

        result_dict = defaultdict(list)
        for row in session.execute(tasks_by_status_query):
            result_dict[row[0]].append(row[1])

    resp = jsonify(tasks_by_status=result_dict)
    resp.status_code = StatusCodes.OK
    return resp


@api_v1_blueprint.route("/array/<array_id>/transition_to_launched", methods=["POST"])
@api_v2_blueprint.route("/array/<array_id>/transition_to_launched", methods=["POST"])
def transition_array_to_launched(array_id: int) -> Any:
    """Transition TIs associated with an array_id and batch_num to launched."""
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, request.get_json())
    batch_num = data["batch_number"]
    next_report = data["next_report_increment"]

    session = SessionLocal()
    with session.begin():
        # Acquire a lock and update tasks to launched
        task_ids_query = (
            select(TaskInstance.task_id)
            .where(
                TaskInstance.array_id == array_id,
                TaskInstance.array_batch_num == batch_num,
            )
            .execution_options(synchronize_session=False)
        )

        task_ids = session.execute(task_ids_query).scalars()

        task_condition = and_(
            Task.array_id == array_id,
            Task.id.in_(task_ids),
            Task.status == TaskStatus.INSTANTIATING,
        )

        task_locks = (
            select(Task.id)
            .where(task_condition)
            .with_for_update()
            .execution_options(synchronize_session=False)
        )
        session.execute(task_locks)

        update_task_stmt = (
            update(Task)
            .where(task_condition)
            .values(status=TaskStatus.LAUNCHED, status_date=func.now())
        ).execution_options(synchronize_session=False)
        session.execute(update_task_stmt)

    # Update the task instances in a separate session
    _update_task_instance(array_id, batch_num, next_report)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


def _update_task_instance(array_id: int, batch_num: int, next_report: int) -> None:

    task_instance_condition = and_(
        TaskInstance.array_id == array_id,
        TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
        TaskInstance.array_batch_num == batch_num,
    )

    session = SessionLocal()
    with session.begin():
        # Acquire a lock and update tasks to launched
        task_instance_ids_query = (
            select(TaskInstance.id)
            .where(task_instance_condition)
            .with_for_update()
            .execution_options(synchronize_session=False)
        )

        session.execute(task_instance_ids_query)

        # Transition all the task instances in the batch
        # Bypassing the ORM for performance reasons.
        update_stmt = (
            update(TaskInstance)
            .where(task_instance_condition)
            .values(
                status=TaskInstanceStatus.LAUNCHED,
                submitted_date=func.now(),
                status_date=func.now(),
                report_by_date=add_time(next_report),
            )
        ).execution_options(synchronize_session=False)
        session.execute(update_stmt)


@api_v1_blueprint.route("/array/<array_id>/log_distributor_id", methods=["POST"])
@api_v2_blueprint.route("/array/<array_id>/log_distributor_id", methods=["POST"])
def log_array_distributor_id(array_id: int) -> Any:
    """Add distributor_id, stderr/stdout paths to the DB for all TIs in an array."""
    data = request.get_json()

    id_lst = list(data.keys())

    where_condition = and_(
        TaskInstance.id.in_(id_lst),
        TaskInstance.array_id == array_id,
    )

    # Prepare to acquire locks on the task instances
    task_instance_ids_query = (
        select(TaskInstance.id)
        .where(where_condition)
        .with_for_update()
        .execution_options(synchronize_session=False)
    )

    # Prepare the case statement for dynamic updating based on conditions
    case_stmt = case(
        *[
            (TaskInstance.id == int(task_instance_id), distributor_id)
            for task_instance_id, distributor_id in data.items()
        ],
        else_=TaskInstance.distributor_id,
    )

    # Acquire locks and update TaskInstances
    session = SessionLocal()
    with session.begin():
        # locks for the updates
        session.execute(task_instance_ids_query)

        # Using the session to construct an update statement for ORM objects
        update_stmt = (
            update(TaskInstance)
            .where(where_condition)
            .values(distributor_id=case_stmt)
            .execution_options(synchronize_session="fetch")
        )
        # updates
        session.execute(update_stmt)

    resp = jsonify(success=True)
    resp.status_code = StatusCodes.OK
    return resp
