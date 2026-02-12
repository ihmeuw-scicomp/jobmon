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
from jobmon.core.logging import set_jobmon_context
from jobmon.server.web._compat import add_time
from jobmon.server.web.db.deps import get_db, get_dialect
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.services.transition_service import TransitionService

logger = structlog.get_logger(__name__)


@api_v3_router.post("/array")
async def add_array(request: Request, db: Session = Depends(get_db)) -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = cast(Dict, await request.json())
    workflow_id = int(data["workflow_id"])
    task_template_version_id = int(data["task_template_version_id"])

    set_jobmon_context(
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

    logger.info(
        "Server received batch creation request",
        array_id=array_id,
        task_count=len(task_ids),
        task_resources_id=task_resources_id,
        workflow_run_id=workflow_run_id,
    )
    task_condition = and_(
        Task.id.in_(task_ids),
        Task.status.in_([TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]),
    )

    # Get list of task IDs to modify without locking
    task_ids_to_update = [
        row[0] for row in db.execute(select(Task.id).where(task_condition)).fetchall()
    ]

    if not task_ids_to_update:
        # No tasks to update, but still return their current status (like v2)
        logger.warning(
            f"queue_task_batch: No tasks to update from {len(task_ids)} requested. "
            f"Tasks may already be in QUEUED or other status."
        )
    else:
        # Split into batches of 1000
        batch_size = 1000
        task_batches = [
            task_ids_to_update[i : i + batch_size]
            for i in range(0, len(task_ids_to_update), batch_size)
        ]

        # Process each batch with TransitionService
        for batch in task_batches:
            # Use TransitionService for Task transition with audit logging
            # Service has internal retries and handles SKIP LOCKED
            result = TransitionService.gate_tasks_for_queueing(
                session=db,
                task_ids=batch,
            )

            gated_task_ids = result["gated"]

            if result["locked"]:
                logger.warning(
                    f"Some tasks were locked during queueing, will retry: "
                    f"{result['locked']}"
                )

            if not gated_task_ids:
                # No tasks were gated in this batch
                continue

            # Calculate array_batch_num and create TaskInstances
            # This still uses retry logic for the TI creation part
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    # Calculate array_batch_num separately to avoid deadlock
                    # Use SELECT FOR UPDATE with NOWAIT to prevent waiting on locks
                    batch_num_result = db.execute(
                        select(
                            func.coalesce(func.max(TaskInstance.array_batch_num) + 1, 1)
                        )
                        .where(TaskInstance.array_id == array_id)
                        .with_for_update(nowait=True)
                    ).scalar()

                    # Insert into TaskInstance with the calculated batch number
                    # Only for tasks that passed the gate
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
                            literal_column(str(workflow_run_id)).label(
                                "workflow_run_id"
                            ),
                            literal_column(str(array_id)).label("array_id"),
                            literal_column(str(task_resources_id)).label(
                                "task_resources_id"
                            ),
                            # batch info - use the pre-calculated value
                            literal_column(str(batch_num_result)).label(
                                "array_batch_num"
                            ),
                            (func.row_number().over(order_by=Task.id) - 1).label(
                                "array_step_id"
                            ),
                            # status columns
                            literal_column(f"'{TaskInstanceStatus.QUEUED}'").label(
                                "status"
                            ),
                            func.now().label("status_date"),
                        ).where(
                            Task.id.in_(gated_task_ids),
                            Task.status == TaskStatus.QUEUED,
                        ),
                        # no python side defaults. Server defaults only
                        include_defaults=False,
                    )
                    db.execute(insert_stmt)

                    # Commit TaskInstance insert
                    db.commit()
                    break  # Success - exit retry loop

                except OperationalError as e:
                    logger.warning(
                        f"DB error during TI creation {e}, retrying attempt "
                        f"{attempt + 1}/{max_retries}"
                    )
                    db.rollback()
                    sleep(0.001 * (2 ** (attempt + 1)))  # Exponential backoff
            else:
                # All retries failed
                logger.error(
                    f"Failed to create TaskInstances after {max_retries} attempts"
                )
                db.rollback()
                raise HTTPException(
                    status_code=503,
                    detail="Database temporarily unavailable, please retry",
                )

    # Run the tasks_by_status_query and return
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
    set_jobmon_context(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]
    next_report = data["next_report_increment"]

    logger.info(
        "Server received batch launch transition request",
        array_id=array_id,
        array_batch_num=batch_num,
    )

    # Atomic update of both Task and TaskInstance with retry logic
    max_retries = 5

    for attempt in range(max_retries):
        try:
            # Get both task IDs and task instance IDs for this array and batch
            ids_query = (
                select(TaskInstance.task_id, TaskInstance.id)
                .where(
                    TaskInstance.array_id == array_id,
                    TaskInstance.array_batch_num == batch_num,
                )
                .execution_options(synchronize_session=False)
            )
            results = db.execute(ids_query).all()
            task_ids = [row[0] for row in results]
            task_instance_ids = [row[1] for row in results]

            # 1) Use TransitionService for Task transition with audit logging
            if task_ids:
                result = TransitionService.transition_tasks(
                    session=db,
                    task_ids=task_ids,
                    to_status=TaskStatusConstants.LAUNCHED,
                    use_skip_locked=True,
                )
                transitioned_task_ids = result["transitioned"]
            else:
                transitioned_task_ids = []

            # 2) Transition TaskInstances to LAUNCHED in the same transaction
            # Only for tasks that were actually transitioned
            if transitioned_task_ids:
                # Get TI IDs for the transitioned tasks
                ti_ids_to_update = [
                    row[1] for row in results if row[0] in transitioned_task_ids
                ]

                if ti_ids_to_update:
                    dialect = get_dialect(request)
                    update_ti_stmt = (
                        update(TaskInstance)
                        .where(
                            and_(
                                TaskInstance.id.in_(ti_ids_to_update),
                                TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
                            )
                        )
                        .values(
                            status=TaskInstanceStatus.LAUNCHED,
                            submitted_date=func.now(),
                            status_date=func.now(),
                            report_by_date=add_time(next_report, dialect),
                        )
                        .execution_options(synchronize_session=False)
                    )
                    db.execute(update_ti_stmt)

            # 3) Atomic commit - both updates succeed or both fail
            db.commit()

            # Log each task instance (info level - state transition)
            for task_instance_id in task_instance_ids:
                logger.info(
                    "Task instance transitioned to LAUNCHED in database",
                    task_instance_id=task_instance_id,
                    array_id=array_id,
                    array_batch_num=batch_num,
                )

            logger.info(
                "Batch successfully transitioned to LAUNCHED",
                array_id=array_id,
                array_batch_num=batch_num,
                num_tasks=len(task_instance_ids),
            )
            return JSONResponse(content={}, status_code=StatusCodes.OK)

        except OperationalError as e:
            if (
                "database is locked" in str(e)
                or "Lock wait timeout" in str(e)
                or "could not obtain lock" in str(e)
                or "Deadlock found" in str(e)
                or "lock(s) could not be acquired immediately and NOWAIT is set"
                in str(e)
            ):
                logger.warning(
                    f"Database error detected for atomic Task/TaskInstance launch update, "
                    f"retrying attempt {attempt + 1}/{max_retries}. {e}"
                )
                db.rollback()  # Clear the corrupted session state
                sleep(0.001 * (2 ** (attempt + 1)))  # Exponential backoff: 2ms, 4ms...
            else:
                logger.error(f"Unexpected database error in atomic launch update: {e}")
                db.rollback()
                raise e
        except Exception as e:
            logger.error(f"Failed to update Tasks and TaskInstances to LAUNCHED: {e}")
            db.rollback()
            raise e

    # All retries failed
    logger.error(
        f"Failed to update Tasks and TaskInstances to LAUNCHED after {max_retries} attempts"
    )
    db.rollback()
    raise HTTPException(
        status_code=503, detail="Database temporarily unavailable, please retry"
    )


@api_v3_router.post("/array/{array_id}/transition_to_killed")
async def transition_to_killed(
    array_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Transition TIs from KILL_SELF to ERROR_FATAL.

    Also marks parent Tasks as ERROR_FATAL if they're in a killable state.
    This is safe because the workflow won't be resumable until all KILL_SELF
    TIs are cleaned up (no race condition with new workflow runs).
    """
    set_jobmon_context(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]

    logger.info(
        "Server received kill batch request",
        array_id=array_id,
        array_batch_num=batch_num,
    )

    # Atomic update of both Task and TaskInstance with retry logic
    max_retries = 5

    for attempt in range(max_retries):
        try:
            # Find both Task IDs and TaskInstance IDs in this array & batch
            ids_query = (
                select(TaskInstance.task_id, TaskInstance.id)
                .where(
                    TaskInstance.array_id == array_id,
                    TaskInstance.array_batch_num == batch_num,
                    TaskInstance.status == TaskInstanceStatus.KILL_SELF,
                )
                .execution_options(synchronize_session=False)
            )
            results = db.execute(ids_query).all()
            task_ids = [row[0] for row in results]
            task_instance_ids = [row[1] for row in results]

            if not task_instance_ids:
                logger.info(
                    "No KILL_SELF task instances found in batch",
                    array_id=array_id,
                    array_batch_num=batch_num,
                )
                return JSONResponse(content={}, status_code=StatusCodes.OK)

            # 1) Use TransitionService for Task transition with audit logging
            if task_ids:
                result = TransitionService.transition_tasks(
                    session=db,
                    task_ids=task_ids,
                    to_status=TaskStatusConstants.ERROR_FATAL,
                    use_skip_locked=True,
                )
                transitioned_task_ids = result["transitioned"]
            else:
                transitioned_task_ids = []

            # 2) Transition TaskInstances to ERROR_FATAL in the same transaction
            # Only for tasks that were actually transitioned
            if transitioned_task_ids:
                # Get TI IDs for the transitioned tasks
                ti_ids_to_update = [
                    row[1] for row in results if row[0] in transitioned_task_ids
                ]

                if ti_ids_to_update:
                    update_ti_stmt = (
                        update(TaskInstance)
                        .where(
                            and_(
                                TaskInstance.id.in_(ti_ids_to_update),
                                TaskInstance.status == TaskInstanceStatus.KILL_SELF,
                            )
                        )
                        .values(
                            status=TaskInstanceStatus.ERROR_FATAL,
                            status_date=func.now(),
                        )
                        .execution_options(synchronize_session=False)
                    )
                    db.execute(update_ti_stmt)

            # 3) Atomic commit
            db.commit()

            # Log each killed task instance (info level - state transition)
            for task_instance_id in task_instance_ids:
                logger.info(
                    "Task instance killed (KILL_SELF → ERROR_FATAL)",
                    task_instance_id=task_instance_id,
                    array_id=array_id,
                    array_batch_num=batch_num,
                )

            logger.info(
                "Batch successfully transitioned from KILL_SELF to ERROR_FATAL",
                array_id=array_id,
                array_batch_num=batch_num,
                num_task_instances=len(task_instance_ids),
            )
            return JSONResponse(content={}, status_code=StatusCodes.OK)

        except OperationalError as e:
            if (
                "database is locked" in str(e)
                or "Lock wait timeout" in str(e)
                or "could not obtain lock" in str(e)
                or "Deadlock found" in str(e)
                or "lock(s) could not be acquired immediately and NOWAIT is set"
                in str(e)
            ):
                logger.warning(
                    f"Database error detected for atomic Task/TaskInstance update, "
                    f"retrying attempt {attempt + 1}/{max_retries}. {e}"
                )
                db.rollback()  # Clear the corrupted session state
                sleep(0.001 * (2 ** (attempt + 1)))  # Exponential backoff: 2ms, 4ms...
            else:
                logger.error(f"Unexpected database error in atomic update: {e}")
                db.rollback()
                raise e
        except Exception as e:
            logger.error(f"Failed to update Tasks and TaskInstances: {e}")
            db.rollback()
            raise e

    # All retries failed
    logger.error(
        f"Failed to update Tasks and TaskInstances after {max_retries} attempts"
    )
    db.rollback()
    raise HTTPException(
        status_code=503, detail="Database temporarily unavailable, please retry"
    )


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
    set_jobmon_context(array_id=array_id)

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
