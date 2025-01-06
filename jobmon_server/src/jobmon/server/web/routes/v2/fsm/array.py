"""Routes for Arrays."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from fastapi import Request
from sqlalchemy import and_, case, func, insert, literal_column, select, update
from starlette.responses import JSONResponse
import structlog

from jobmon.core.constants import TaskInstanceStatus
from jobmon.server.web._compat import add_time
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.routes.v1.fsm import fsm_router as api_v1_router
from jobmon.server.web.routes.v2.fsm import fsm_router as api_v2_router

logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()


@api_v1_router.post("/array")
@api_v2_router.post("/array")
async def add_array(request: Request) -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = cast(Dict, await request.json())
    workflow_id = int(data["workflow_id"])
    task_template_version_id = int(data["task_template_version_id"])

    structlog.contextvars.bind_contextvars(
        task_template_version_id=task_template_version_id,
        workflow_id=workflow_id,
    )

    # Check if the array is already bound, if so return it
    with SessionLocal() as session:
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
                array_condition = and_(Array.id == array.id)

                # take a lock on the array that will be updated
                array_locks = (
                    select(Array.id)
                    .where(array_condition)
                    .with_for_update()
                    .execution_options(synchronize_session=False)
                )
                session.execute(array_locks)

                # update the array with the new max_concurrently_running
                update_stmt = (
                    update(Array)
                    .where(array_condition)
                    .values(max_concurrently_running=data["max_concurrently_running"])
                )
                session.execute(update_stmt)

        # return result
        resp = JSONResponse(content={"array_id": array.id}, status_code=StatusCodes.OK)
    return resp


@api_v1_router.post("/array/{array_id}/queue_task_batch")
@api_v2_router.post("/array/{array_id}/queue_task_batch")
async def record_array_batch_num(array_id: int, request: Request) -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = cast(Dict, await request.json())
    array_id = int(array_id)
    task_ids = [int(task_id) for task_id in data["task_ids"]]
    task_resources_id = int(data["task_resources_id"])
    workflow_run_id = int(data["workflow_run_id"])
    task_condition = and_(
        Task.id.in_(task_ids),
        Task.status.in_([TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]),
    )

    with SessionLocal() as session:
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
                    (func.row_number().over(order_by=Task.id) - 1).label(
                        "array_step_id"
                    ),
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
            ()
    resp = JSONResponse(
        content={"tasks_by_status": result_dict}, status_code=StatusCodes.OK
    )
    return resp


@api_v1_router.post("/array/{array_id}/transition_to_launched")
@api_v2_router.post("/array/{array_id}/transition_to_launched")
async def transition_array_to_launched(array_id: int, request: Request) -> Any:
    """Transition TIs associated with an array_id and batch_num to launched."""
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]
    next_report = data["next_report_increment"]

    with SessionLocal() as session:
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
            ()

    # Update the task instances in a separate session
    _update_task_instance(array_id, batch_num, next_report)

    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


def _update_task_instance(array_id: int, batch_num: int, next_report: int) -> None:
    task_instance_condition = and_(
        TaskInstance.array_id == array_id,
        TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
        TaskInstance.array_batch_num == batch_num,
    )

    with SessionLocal() as session:
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


@api_v1_router.post("/array/{array_id}/log_distributor_id")
@api_v2_router.post("/array/{array_id}/log_distributor_id")
async def log_array_distributor_id(array_id: int, request: Request) -> Any:
    """Add distributor_id, stderr/stdout paths to the DB for all TIs in an array."""
    data = await request.json()

    filtered_data = {
        task_instance_id: distributor_id
        for task_instance_id, distributor_id in data.items()
        if isinstance(task_instance_id, str) and task_instance_id.isdigit()
        # Check if key is a string that can be cast to an integer
    }

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
            for task_instance_id, distributor_id in filtered_data.items()
        ],
        else_=TaskInstance.distributor_id,
    )

    # Acquire locks and update TaskInstances
    with SessionLocal() as session:
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
            ()
    resp = JSONResponse(content={"success": True}, status_code=StatusCodes.OK)
    return resp


@api_v2_router.get("/array/{array_id}/get_array_max_concurrently_running")
@api_v2_router.get(
    "/workflow/{workflow_id}/get_array_max_concurrently_running/{task_template_version_id}"
)
async def get_array_max_concurrently_running(
    array_id: int | None = None,
    workflow_id: int | None = None,
    task_template_version_id: int | None = None,
) -> Any:
    """Return the maximum concurrency of this array."""
    structlog.contextvars.bind_contextvars(array_id=array_id)

    if array_id is not None:
        select_stmt = select(Array).where(Array.id == array_id)
    else:
        select_stmt = select(Array).where(
            Array.workflow_id == workflow_id,
            Array.task_template_version_id == task_template_version_id,
        )

    with SessionLocal() as session:
        with session.begin():
            array = session.execute(select_stmt).scalars().one()

        resp = JSONResponse(
            content={"max_concurrently_running": array.max_concurrently_running},
            status_code=StatusCodes.OK,
        )
    return resp


@api_v1_router.post("/array/{array_id}/transition_to_killed")
@api_v2_router.post("/array/{array_id}/transition_to_killed")
async def transition_to_killed(array_id: int, request: Request) -> Any:
    """Transition TIs from KILL_SELF  to ERROR_FATAL.

    Also mark parent Tasks with status=ERROR_FATAL if they're in a killable state.
    """
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]

    # 1) Acquire locks on the parent Tasks, and set them to ERROR_FATAL
    #    if they're in a killable state.
    #    This is analogous to how transition_to_launched locks tasks
    #    that are INSTANTIATING and sets them to LAUNCHED.

    with SessionLocal() as session:
        with session.begin():
            # Find Task IDs belonging to TIs in this array & batch
            task_ids_query = (
                select(TaskInstance.task_id)
                .where(
                    TaskInstance.array_id == array_id,
                    TaskInstance.array_batch_num == batch_num,
                    TaskInstance.status == TaskInstanceStatus.KILL_SELF,
                )
                .execution_options(synchronize_session=False)
            )
            task_ids = session.execute(task_ids_query).scalars().all()

            # We'll define "killable" Task states. Adjust as appropriate.
            killable_task_states = (
                TaskStatus.LAUNCHED,
                TaskStatus.RUNNING,
            )
            task_condition = and_(
                Task.array_id == array_id,
                Task.id.in_(task_ids),
                Task.status.in_(killable_task_states),
            )

            # Lock them with_for_update
            task_locks = (
                select(Task.id)
                .where(task_condition)
                .with_for_update()
                .execution_options(synchronize_session=False)
            )
            session.execute(task_locks)

            # Transition them to ERROR_FATAL
            update_task_stmt = (
                update(Task)
                .where(task_condition)
                .values(status=TaskStatus.ERROR_FATAL, status_date=func.now())
            ).execution_options(synchronize_session=False)
            session.execute(update_task_stmt)
            # The trailing () is not required but you can keep it for consistency
            # if that’s how your code style is set.

    # 2) Now transition the TIs themselves to ERROR_FATAL.
    #    This is in a separate session, just like _update_task_instance
    #    is invoked in transition_to_launched.
    _update_task_instance_killed(array_id, batch_num)

    # 3) Return success
    return JSONResponse(content={}, status_code=StatusCodes.OK)


def _update_task_instance_killed(array_id: int, batch_num: int) -> None:
    """Bulk update TaskInstances in (array_id, batch_num) from KILL_SELF."""
    # In this example, we assume you specifically want to move TIs in KILL_SELF -> ERROR_FATAL.
    # Adapt as needed if you also want to kill TIs in LAUNCHED, RUNNING, etc.
    ti_condition = and_(
        TaskInstance.array_id == array_id,
        TaskInstance.array_batch_num == batch_num,
        TaskInstance.status == TaskInstanceStatus.KILL_SELF,
    )

    with SessionLocal() as session:
        with session.begin():
            # Acquire a lock on these TIs
            task_instance_ids_query = (
                select(TaskInstance.id)
                .where(ti_condition)
                .with_for_update()
                .execution_options(synchronize_session=False)
            )
            session.execute(task_instance_ids_query)

            # Transition them all to ERROR_FATAL
            update_stmt = (
                update(TaskInstance)
                .where(ti_condition)
                .values(
                    status=TaskInstanceStatus.ERROR_FATAL,
                    status_date=func.now(),
                )
            ).execution_options(synchronize_session=False)
            session.execute(update_stmt)
