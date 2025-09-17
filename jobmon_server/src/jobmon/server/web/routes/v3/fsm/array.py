"""Routes for Arrays."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
from time import sleep
from typing import Any, Dict, cast

import structlog
from fastapi import Depends, HTTPException, Request
from sqlalchemy import and_, case, func, insert, literal_column, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.constants import TaskStatus as TaskStatusConstants
from jobmon.server.web._compat import add_time
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router

logger = structlog.get_logger(__name__)


@api_v3_router.post("/array")
async def add_array(request: Request, db: Session = Depends(get_db)) -> Any:
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
    select_stmt = select(Array).where(
        Array.workflow_id == workflow_id,
        Array.task_template_version_id == task_template_version_id,
    )
    array = db.execute(select_stmt).scalars().one_or_none()

    if array is None:  # not found, so need to add it
        array = Array(
            task_template_version_id=data["task_template_version_id"],
            workflow_id=data["workflow_id"],
            max_concurrently_running=data["max_concurrently_running"],
            name=data["name"],
        )
        db.add(array)
    else:
        array_condition = and_(Array.id == array.id)

        # take a lock on the array that will be updated
        array_locks = (
            select(Array.id)
            .where(array_condition)
            .with_for_update()
            .execution_options(synchronize_session=False)
        )
        db.execute(array_locks)

        # update the array with the new max_concurrently_running
        update_stmt = (
            update(Array)
            .where(array_condition)
            .values(max_concurrently_running=data["max_concurrently_running"])
        )
        db.execute(update_stmt)

    db.flush()
    # return result
    resp = JSONResponse(content={"array_id": array.id}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/array/{array_id}/queue_task_batch")
async def record_array_batch_num(
    array_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
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

    # Step 1: Get list of task IDs to modify without locking
    task_ids_to_update = [
        row[0] for row in db.execute(select(Task.id).where(task_condition)).fetchall()
    ]

    if not task_ids_to_update:
        # No tasks to update, return empty result
        return JSONResponse(content={"tasks_by_status": {}}, status_code=StatusCodes.OK)

    # Step 2: Split into batches of 1000
    batch_size = 1000
    task_batches = [
        task_ids_to_update[i : i + batch_size]
        for i in range(0, len(task_ids_to_update), batch_size)
    ]

    # Step 3: Process each batch with retries
    max_retries = 5

    for batch in task_batches:
        # Update Task status with retries
        for attempt in range(max_retries):
            try:
                # Update task status to QUEUED
                update_stmt = (
                    update(Task)
                    .where(
                        and_(
                            Task.id.in_(batch),
                            Task.status.in_(
                                [TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]
                            ),
                        )
                    )
                    .values(
                        status=TaskStatus.QUEUED,
                        status_date=func.now(),
                        num_attempts=(Task.num_attempts + 1),
                    )
                    .execution_options(synchronize_session=False)
                )
                db.execute(update_stmt)
                db.commit()  # Immediate commit
                break  # Success - exit retry loop
            except OperationalError as e:
                if (
                    "database is locked" in str(e)
                    or "Lock wait timeout" in str(e)
                    or "could not obtain lock" in str(e)
                ):
                    logger.warning(
                        f"Lock timeout updating tasks batch, retrying attempt "
                        f"{attempt + 1}/{max_retries}"
                    )
                    db.rollback()
                    sleep(0.001 * (2 ** (attempt + 1)))  # Exponential backoff
                else:
                    logger.error(f"Unexpected database error updating tasks: {e}")
                    db.rollback()
                    raise e
        else:
            # All retries failed
            logger.error(f"Failed to update task batch after {max_retries} attempts")
            db.rollback()
            raise HTTPException(
                status_code=503, detail="Database temporarily unavailable, please retry"
            )

        # Insert into TaskInstance with retries
        for attempt in range(max_retries):
            try:
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
                        literal_column(str(task_resources_id)).label(
                            "task_resources_id"
                        ),
                        # batch info
                        select(
                            func.coalesce(func.max(TaskInstance.array_batch_num) + 1, 1)
                        )
                        .where((TaskInstance.array_id == array_id))
                        .label("array_batch_num"),
                        (func.row_number().over(order_by=Task.id) - 1).label(
                            "array_step_id"
                        ),
                        # status columns
                        literal_column(f"'{TaskInstanceStatus.QUEUED}'").label(
                            "status"
                        ),
                        func.now().label("status_date"),
                    ).where(Task.id.in_(batch), Task.status == TaskStatus.QUEUED),
                    # no python side defaults. Server defaults only
                    include_defaults=False,
                )
                db.execute(insert_stmt)
                db.commit()  # Immediate commit
                break  # Success - exit retry loop
            except OperationalError as e:
                if (
                    "database is locked" in str(e)
                    or "Lock wait timeout" in str(e)
                    or "could not obtain lock" in str(e)
                ):
                    logger.warning(
                        f"Lock timeout inserting task instances batch, retrying attempt "
                        f"{attempt + 1}/{max_retries}"
                    )
                    db.rollback()
                    sleep(0.001 * (2 ** (attempt + 1)))  # Exponential backoff
                else:
                    logger.error(
                        f"Unexpected database error inserting task instances: {e}"
                    )
                    db.rollback()
                    raise e
        else:
            # All retries failed
            logger.error(
                f"Failed to insert task instance batch after {max_retries} attempts"
            )
            db.rollback()
            raise HTTPException(
                status_code=503, detail="Database temporarily unavailable, please retry"
            )

    # Step 4: Run the tasks_by_status_query and return
    tasks_by_status_query = (
        select(Task.status, Task.id).where(Task.id.in_(task_ids)).order_by(Task.status)
    )

    result_dict = defaultdict(list)
    for row in db.execute(tasks_by_status_query):
        result_dict[row[0]].append(row[1])

    resp = JSONResponse(
        content={"tasks_by_status": result_dict}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.post("/array/{array_id}/transition_to_launched")
async def transition_array_to_launched(
    array_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Transition TIs associated with an array_id and batch_num to launched."""
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]
    next_report = data["next_report_increment"]

    # Acquire a lock and update tasks to launched
    task_ids_query = (
        select(TaskInstance.task_id)
        .where(
            TaskInstance.array_id == array_id,
            TaskInstance.array_batch_num == batch_num,
        )
        .execution_options(synchronize_session=False)
    )

    task_ids = db.execute(task_ids_query).scalars()

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
    db.execute(task_locks)

    update_task_stmt = (
        update(Task)
        .where(task_condition)
        .values(status=TaskStatus.LAUNCHED, status_date=func.now())
    ).execution_options(synchronize_session=False)
    db.execute(update_task_stmt)

    # Update the task instances
    _update_task_instance(array_id, batch_num, next_report, db)

    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/array/{array_id}/transition_to_killed")
async def transition_to_killed(
    array_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Transition TIs from KILL_SELF to ERROR_FATAL.

    Also mark parent Tasks with status=ERROR_FATAL if they're in a killable state.
    """
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]

    # 1) Acquire locks on the parent Tasks, and set them to ERROR_FATAL
    #    if they're in a killable state.
    #    This is analogous to how transition_to_launched locks tasks
    #    that are INSTANTIATING and sets them to LAUNCHED.

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
    task_ids = db.execute(task_ids_query).scalars().all()

    # We'll define "killable" Task states. Adjust as appropriate.
    killable_task_states = (
        TaskStatusConstants.LAUNCHED,
        TaskStatusConstants.RUNNING,
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
    db.execute(task_locks)

    # Transition them to ERROR_FATAL
    update_task_stmt = (
        update(Task)
        .where(task_condition)
        .values(status=TaskStatusConstants.ERROR_FATAL, status_date=func.now())
    ).execution_options(synchronize_session=False)
    db.execute(update_task_stmt)

    # 2) Now transition the TIs themselves to ERROR_FATAL.
    _update_task_instance_killed(array_id, batch_num, db)

    # 3) Return success
    return JSONResponse(content={}, status_code=StatusCodes.OK)


def _update_task_instance_killed(array_id: int, batch_num: int, db: Session) -> None:
    """Bulk update TaskInstances in (array_id, batch_num) from KILL_SELF."""
    # In this example, we assume you specifically want to move TIs in KILL_SELF -> ERROR_FATAL.
    # Adapt as needed if you also want to kill TIs in LAUNCHED, RUNNING, etc.
    ti_condition = and_(
        TaskInstance.array_id == array_id,
        TaskInstance.array_batch_num == batch_num,
        TaskInstance.status == TaskInstanceStatus.KILL_SELF,
    )

    # Acquire a lock on these TIs
    task_instance_ids_query = (
        select(TaskInstance.id)
        .where(ti_condition)
        .with_for_update()
        .execution_options(synchronize_session=False)
    )
    db.execute(task_instance_ids_query)

    # Transition them all to ERROR_FATAL
    update_stmt = (
        update(TaskInstance)
        .where(ti_condition)
        .values(
            status=TaskInstanceStatus.ERROR_FATAL,
            status_date=func.now(),
        )
    ).execution_options(synchronize_session=False)
    db.execute(update_stmt)


def _update_task_instance(
    array_id: int, batch_num: int, next_report: int, db: Session
) -> None:
    """Transition task instances with additional safety checks."""
    # First, get the count of task instances that would be affected
    count_query = select(func.count(TaskInstance.id)).where(
        and_(
            TaskInstance.array_id == array_id,
            TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
            TaskInstance.array_batch_num == batch_num,
        )
    )

    count = db.execute(count_query).scalar()
    logger.info(f"Found {count} task instances in INSTANTIATED status to transition")

    if count == 0:
        logger.warning("No task instances in INSTANTIATED status found")
        return

    # Single atomic update
    update_stmt = (
        update(TaskInstance)
        .where(
            and_(
                TaskInstance.array_id == array_id,
                TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
                TaskInstance.array_batch_num == batch_num,
            )
        )
        .values(
            status=TaskInstanceStatus.LAUNCHED,
            submitted_date=func.now(),
            status_date=func.now(),
            report_by_date=add_time(next_report),
        )
        .execution_options(synchronize_session=False)
    )

    result = db.execute(update_stmt)
    logger.info(f"Successfully updated {result.rowcount} task instances to LAUNCHED")


@api_v3_router.post("/array/{array_id}/log_distributor_id")
async def log_array_distributor_id(
    array_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
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
    # locks for the updates
    db.execute(task_instance_ids_query)

    # Using the session to construct an update statement for ORM objects
    update_stmt = (
        update(TaskInstance)
        .where(where_condition)
        .values(distributor_id=case_stmt)
        .execution_options(synchronize_session="fetch")
    )
    # updates
    db.execute(update_stmt)

    resp = JSONResponse(content={"success": True}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/array/{array_id}/get_array_max_concurrently_running")
@api_v3_router.get(
    "/workflow/{workflow_id}/get_array_max_concurrently_running/{task_template_version_id}"
)
async def get_array_max_concurrently_running(
    array_id: int | None = None,
    workflow_id: int | None = None,
    task_template_version_id: int | None = None,
    db: Session = Depends(get_db),
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

    array = db.execute(select_stmt).scalars().one()

    resp = JSONResponse(
        content={"max_concurrently_running": array.max_concurrently_running},
        status_code=StatusCodes.OK,
    )
    return resp
