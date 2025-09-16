"""Routes for TaskInstances."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
from time import sleep
from typing import Any, DefaultDict, Dict, Optional, cast

import structlog
from fastapi import Depends, HTTPException, Request
from sqlalchemy import and_, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from starlette.responses import JSONResponse

from jobmon.core import constants
from jobmon.core.serializers import SerializeTaskInstanceBatch
from jobmon.server.web._compat import add_time
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import ServerError

logger = structlog.get_logger(__name__)


def get_new_task_status(task_instance: TaskInstance, new_state: str) -> tuple[str, str]:
    """Get the valid status to transit to. (TaskInstanceStatus, TaskStatus)."""
    current_task_status = task_instance.task.status
    if new_state == TaskInstanceStatus.QUEUED:
        return TaskStatus.QUEUED, TaskStatus.QUEUED
    if new_state == TaskInstanceStatus.INSTANTIATED:
        return TaskStatus.INSTANTIATING, TaskStatus.INSTANTIATING
    if new_state == TaskInstanceStatus.LAUNCHED:
        return TaskStatus.LAUNCHED, TaskStatus.LAUNCHED
    if new_state == TaskInstanceStatus.RUNNING:
        return TaskStatus.RUNNING, TaskStatus.RUNNING
    elif new_state == TaskInstanceStatus.DONE:
        return TaskStatus.DONE, TaskStatus.DONE
    elif new_state in task_instance.error_states:
        if task_instance.task.num_attempts >= task_instance.task.max_attempts:
            logger.info("Giving up task after max attempts.")
            return TaskStatus.ERROR_RECOVERABLE, TaskStatus.ERROR_FATAL
        else:
            if new_state == TaskInstanceStatus.RESOURCE_ERROR:
                logger.debug("Adjust resource for task.")
                return TaskStatus.ERROR_RECOVERABLE, TaskStatus.ADJUSTING_RESOURCES
            else:
                logger.debug("Retrying Task.")
                return TaskStatus.ERROR_RECOVERABLE, TaskStatus.REGISTERING
    elif new_state == TaskInstanceStatus.ERROR_FATAL:
        return TaskStatus.ERROR_RECOVERABLE, TaskStatus.ERROR_FATAL
    else:
        return current_task_status, current_task_status


def get_transit_status(task_instance: TaskInstance, new_ti_status: str) -> dict | None:
    """Get the valid status to transit to. (TaskInstanceStatus, TaskStatus)."""
    if not task_instance._is_timely_transition(new_ti_status):
        return None

    if (task_instance.status, new_ti_status) not in task_instance.valid_transitions:
        return None

    new_task_status, final_task_status = get_new_task_status(
        task_instance, new_ti_status
    )
    if new_task_status is None:
        return None

    if (
        task_instance.task.status,
        new_task_status,
    ) not in task_instance.task.valid_transitions:
        return None

    return {
        "new_ti_status": new_ti_status,
        "new_t_status": new_task_status,
        "final_t_status": final_task_status,
    }


def transit_ti_and_t(
    task_instance: TaskInstance,
    status_dict: dict,
    db: Session,
    report_by_date: Optional[float] = None,
) -> None:
    """Transit the task_instance and task to the new status.

    Update task_instance and task in a single transation or to avoid inconsistent state.
    If unable to obtain logs for both, update none.
    """
    new_ti_status = status_dict["new_ti_status"]
    new_t_status = status_dict["new_t_status"]
    final_t_status = status_dict["final_t_status"]
    task = task_instance.task
    # retry 3 times for lock because we need to lock two tables in a single transaction
    for i in range(3):
        try:
            # Lock TaskInstance first (by ID - deterministic order)
            # please do not lock the two tables at the same time to avoid deadlock
            ti_lock_stmt = (
                select(TaskInstance.id, TaskInstance.task_id)
                .where(TaskInstance.id == task_instance.id)
                .with_for_update()
            )
            ti_result = db.execute(ti_lock_stmt).one()
            task_id = ti_result.task_id

            # Then lock Task (by ID - deterministic order)
            task_lock_stmt = select(Task.id).where(Task.id == task_id).with_for_update()
            db.execute(task_lock_stmt).one()

            # Update TaskInstance with status and optional report_by_date
            ti_values = {
                "status": new_ti_status,
                "status_date": func.now(),
            }
            if report_by_date is not None:
                ti_values["report_by_date"] = add_time(report_by_date)

            update_stmt = (
                update(TaskInstance)
                .where(TaskInstance.id == task_instance.id)
                .values(ti_values)
            )
            db.execute(update_stmt)

            if task.status != new_t_status or new_t_status != final_t_status:
                # Update Task status
                update_stmt = (
                    update(Task)
                    .where(Task.id == task_id)
                    .values(status=new_t_status, status_date=func.now())
                )
                db.execute(update_stmt)
                db.flush()
                # update to the final task status
                if new_t_status != final_t_status:
                    update_stmt = (
                        update(Task)
                        .where(Task.id == task_id)
                        .values(status=final_t_status, status_date=func.now())
                    )
                    db.execute(update_stmt)
            # release locks immediately; please do not use flush() here
            db.commit()
            return
        except OperationalError as e:
            logger.warning(f"Database error  detected {e}, retrying attempt {i+1}/3")
            db.rollback()  # Clear the corrupted session state
            sleep(0.001 * (i + 1))  # Much faster: 1ms, 2ms, 3ms
        except Exception as e:
            logger.error(f"Failed to transit task_instance: {e}")
            db.rollback()  # Clear the corrupted session state
            raise e
    raise HTTPException(
        status_code=503, detail="Database temporarily unavailable, please retry"
    )


@api_v3_router.post("/task_instance/{task_instance_id}/log_running")
async def log_running(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance as running."""
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    try:
        # load task_instance; do not lock
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()

        # Update attributes
        if data.get("distributor_id") is not None:
            task_instance.distributor_id = data["distributor_id"]
        if data.get("nodename") is not None:
            task_instance.nodename = data["nodename"]
        task_instance.process_group_id = data["process_group_id"]

        # Handle state transition
        status = get_transit_status(task_instance, constants.TaskInstanceStatus.RUNNING)
        if status is not None:
            transit_ti_and_t(
                task_instance,
                status,
                db,
                data["next_report_increment"],
            )
        else:
            if task_instance.status == constants.TaskInstanceStatus.RUNNING:
                logger.warning(
                    f"Unable to transition to running from {task_instance.status}"
                )
            elif task_instance.status == constants.TaskInstanceStatus.KILL_SELF:
                status = get_transit_status(
                    task_instance, constants.TaskInstanceStatus.ERROR_FATAL
                )
                if status is not None:
                    transit_ti_and_t(
                        task_instance,
                        status,
                        db,
                        data["next_report_increment"],
                    )
            elif task_instance.status == constants.TaskInstanceStatus.NO_HEARTBEAT:
                status = get_transit_status(
                    task_instance, constants.TaskInstanceStatus.ERROR
                )
                if status is not None:
                    transit_ti_and_t(
                        task_instance,
                        status,
                        db,
                        data["next_report_increment"],
                    )
            else:
                logger.error(
                    f"Unable to transition to running from {task_instance.status}"
                )

        wire_format = task_instance.to_wire_as_worker_node_task_instance()
        return JSONResponse(
            content={"task_instance": wire_format}, status_code=StatusCodes.OK
        )

    except Exception as e:
        raise e


@api_v3_router.post("/task_instance/{task_instance_id}/log_report_by")
async def log_ti_report_by(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance as being responsive with a new report_by_date.

    This is done at the worker node heartbeat_interval rate, so it may not happen at the same
    rate that the reconciler updates batch submitted report_by_dates (also because it causes
    a lot of traffic if all workers are logging report by_dates often compared to if the
    reconciler runs often).

    Args:
        task_instance_id: id of the task_instance to log
        request: fastapi request object
        db: The database session.
    """
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())
    logger.debug(f"Log report_by for TI {task_instance_id}")

    # Retry logic for row lock contention
    max_retries = 3
    for attempt in range(max_retries):
        try:
            vals = {"report_by_date": add_time(data["next_report_increment"])}
            for optional_val in ["distributor_id", "stderr", "stdout"]:
                val = data.get(optional_val, None)
                if data is not None:
                    vals[optional_val] = val

            # do not lock TaskInstance
            select_stmt = select(TaskInstance).where(
                TaskInstance.id == task_instance_id
            )
            task_instance = db.execute(select_stmt).scalars().one()
            # Apply value updates directly to ORM object
            for key, value in vals.items():
                setattr(task_instance, key, value)

            # Handle possible state transition
            if task_instance.status == constants.TaskInstanceStatus.TRIAGING:
                status = get_transit_status(
                    task_instance, constants.TaskInstanceStatus.RUNNING
                )
                if status is not None:
                    transit_ti_and_t(
                        task_instance,
                        status,
                        db,
                        data["next_report_increment"],
                    )
                else:
                    logger.error(
                        f"Unable to transition to running from {task_instance.status}"
                    )

            resp = JSONResponse(
                content={"status": task_instance.status}, status_code=StatusCodes.OK
            )
            return resp

        except OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logger.warning(
                    f"Database lock detected for TI {task_instance_id}, "
                    f"retrying attempt {attempt + 1}/{max_retries}"
                )
                db.rollback()
                sleep(0.001 * (attempt + 1))  # 1ms, 2ms delays
                continue
            else:
                logger.error(
                    f"Failed to log report_by for TI {task_instance_id} "
                    f"after {max_retries} attempts: {e}"
                )
                db.rollback()
                raise e
        except Exception as e:
            logger.error(
                f"Unexpected error logging report_by for TI {task_instance_id}: {e}"
            )
            db.rollback()
            raise e

    # Should not reach here, but just in case
    raise HTTPException(
        status_code=503, detail="Database temporarily unavailable, please retry"
    )


@api_v3_router.post("/task_instance/log_report_by/batch")
async def log_ti_report_by_batch(
    request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log task_instances as being responsive with a new report_by_date.

    This is done at the worker node heartbeat_interval rate, so it may not happen at the same
    rate that the reconciler updates batch submitted report_by_dates (also because it causes
    a lot of traffic if all workers are logging report by_dates often compared to if the
    reconciler runs often).

    Args:
        task_instance_id: id of the task_instance to log
        request: fastapi request object
        db: The database session.
    """
    data = cast(Dict, await request.json())
    tis = data.get("task_instance_ids", None)

    next_report_increment = float(data["next_report_increment"])

    logger.debug(f"Log report_by for TI {tis}.")
    if tis:
        # Retry logic for row lock contention in batch updates
        max_retries = 3
        for attempt in range(max_retries):
            try:
                update_stmt = (
                    update(TaskInstance)
                    .where(
                        TaskInstance.id.in_(tis),
                        TaskInstance.status == constants.TaskInstanceStatus.LAUNCHED,
                    )
                    .values(report_by_date=add_time(next_report_increment))
                )

                db.execute(update_stmt)
                # immediately release the lock
                db.commit()
                resp = JSONResponse(content={}, status_code=StatusCodes.OK)
                return resp

            except OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(
                        f"Database lock detected for batch TI {tis}, "
                        f"retrying attempt {attempt + 1}/{max_retries}"
                    )
                    db.rollback()
                    sleep(0.001 * (attempt + 1))  # 1ms, 2ms delays
                    continue
                else:
                    logger.error(
                        f"Failed to batch log report_by for TI {tis} "
                        f"after {max_retries} attempts: {e}"
                    )
                    db.rollback()
                    raise e
            except Exception as e:
                # The bulk update may fail if any row is locked by another transaction.
                # Log this error for investigation.
                # If this happens a lot, consider update rows one by one.
                # The locked rows may being updated by worker node; thus, just discard.
                logger.warning(f"Failed to batch log report_by for TI {tis}: {e}")
                db.rollback()
                raise e

        # Should not reach here, but just in case
        raise HTTPException(
            status_code=503, detail="Database temporarily unavailable, please retry"
        )


@api_v3_router.post("/task_instance/{task_instance_id}/log_done")
async def log_done(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance as done."""
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    try:
        # Do not lock TaskInstance
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()

        # Update fields if present
        optional_vals = [
            "distributor_id",
            "stdout_log",
            "stderr_log",
            "nodename",
            "stdout",
            "stderr",
        ]
        for field in optional_vals:
            val = data.get(field)
            if val is not None:
                setattr(task_instance, field, val)

        # Attempt transition
        status = get_transit_status(task_instance, constants.TaskInstanceStatus.DONE)
        if status is not None:
            # log_done doesn't provide next_report_increment
            transit_ti_and_t(task_instance, status, db)
        else:
            if task_instance.status == constants.TaskInstanceStatus.DONE:
                logger.warning(
                    f"Unable to transition to done from {task_instance.status}"
                )
            else:
                logger.error(
                    f"Unable to transition to done from {task_instance.status}"
                )

        return JSONResponse(
            content={"status": task_instance.status},
            status_code=StatusCodes.OK,
        )

    except Exception as e:
        logger.error(f"Failed to mark task_instance {task_instance_id} as done: {e}")
        raise e


@api_v3_router.post("/task_instance/{task_instance_id}/log_error_worker_node")
async def log_error_worker_node(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log an error for a task instance."""
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    try:
        # do not lock TaskInstance
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()

        # ti that tries to log error should not in T unles there was a race condition
        # we need to transition to R first
        if task_instance.status == constants.TaskInstanceStatus.TRIAGING:
            all_status = get_transit_status(
                task_instance, constants.TaskInstanceStatus.RUNNING
            )
            if all_status is not None:
                transit_ti_and_t(task_instance, all_status, db)
            else:
                logger.error(
                    f"Unable to transition to running from {task_instance.status}"
                )

        # Optional updates
        optional_vals = [
            "distributor_id",
            "stdout_log",
            "stderr_log",
            "nodename",
            "stdout",
            "stderr",
        ]
        for field in optional_vals:
            val = data.get(field)
            if val is not None:
                setattr(task_instance, field, val)

        # Error handling
        error_state = data["error_state"]
        error_description = data["error_description"]

        status = get_transit_status(task_instance, error_state)
        if status is not None:
            # Error transitions don't need next_report_increment
            transit_ti_and_t(task_instance, status, db)
        else:
            if task_instance.status == error_state:
                logger.warning(
                    f"Unable to transition to {error_state} from {task_instance.status}"
                )
            else:
                logger.error(
                    f"Unable to transition to {error_state} from {task_instance.status}"
                )
        # Create error log entry
        error = TaskInstanceErrorLog(
            task_instance_id=task_instance.id,
            description=error_description,
        )
        db.add(error)
        db.flush()

        return JSONResponse(
            content={"status": task_instance.status},
            status_code=StatusCodes.OK,
        )

    except Exception as e:
        logger.error(f"Failed to log error for TI {task_instance_id}: {e}")
        raise e


@api_v3_router.get("/task_instance/{task_instance_id}/task_instance_error_log")
async def get_task_instance_error_log(
    task_instance_id: int, db: Session = Depends(get_db)
) -> Any:
    """Route to return all task_instance_error_log entries of the task_instance_id.

    Args:
        task_instance_id (int): ID of the task instance
        db: The database session.

    Return:
        jsonified task_instance_error_log result set
    """
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    logger.info(f"Getting task instance error log for ti {task_instance_id}")

    select_stmt = (
        select(TaskInstanceErrorLog)
        .where(TaskInstanceErrorLog.task_instance_id == task_instance_id)
        .order_by(TaskInstanceErrorLog.task_instance_id)
    )
    res = db.execute(select_stmt).scalars().all()
    r = [tiel.to_wire() for tiel in res]
    resp = JSONResponse(
        content={"task_instance_error_log": r}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.get("/get_array_task_instance_id/{array_id}/{batch_num}/{step_id}")
def get_array_task_instance_id(
    array_id: int, batch_num: int, step_id: int, db: Session = Depends(get_db)
) -> Any:
    """Given an array ID and an index, select a single task instance ID.

    Task instance IDs that are associated with the array are ordered, and selected by index.
    This route will be called once per array task instance worker node, so must be scalable.
    """
    structlog.contextvars.bind_contextvars(array_id=array_id)

    select_stmt = select(TaskInstance.id).where(
        TaskInstance.array_id == array_id,
        TaskInstance.array_batch_num == batch_num,
        TaskInstance.array_step_id == step_id,
    )
    task_instance_id = db.execute(select_stmt).scalars().one()

    resp = JSONResponse(
        content={"task_instance_id": task_instance_id}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.post("/task_instance/{task_instance_id}/log_no_distributor_id")
async def log_no_distributor_id(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance_id that did not get an distributor_id upon submission."""
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    logger.info(
        f"Logging ti {task_instance_id} did not get distributor id upon submission"
    )
    data = cast(Dict, await request.json())
    logger.debug(f"Log NO DISTRIBUTOR ID. Data {data['no_id_err_msg']}")
    err_msg = data["no_id_err_msg"]

    select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
    task_instance = db.execute(select_stmt).scalars().one()
    msg = _update_task_instance_state(
        task_instance, constants.TaskInstanceStatus.NO_DISTRIBUTOR_ID, request, db
    )
    error = TaskInstanceErrorLog(task_instance_id=task_instance.id, description=err_msg)
    db.add(error)
    db.flush()
    resp = JSONResponse(content={"message": msg}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/task_instance/{task_instance_id}/log_distributor_id")
async def log_distributor_id(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance's distributor id."""
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
    task_instance = db.execute(select_stmt).scalars().one()

    # Check if task instance is already in a final state
    if task_instance.status in [
        constants.TaskInstanceStatus.DONE,
        constants.TaskInstanceStatus.ERROR,
        constants.TaskInstanceStatus.ERROR_FATAL,
    ]:
        # Just update the distributor_id and report_by_date without transition
        task_instance.distributor_id = data["distributor_id"]
        task_instance.report_by_date = add_time(data["next_report_increment"])
        db.flush()
        resp = JSONResponse(
            content={"message": "Task instance in final state, no transition needed"},
            status_code=StatusCodes.OK,
        )
        return resp

    # Only try to transition if not in final state
    status = get_transit_status(task_instance, constants.TaskInstanceStatus.LAUNCHED)
    if status is not None:
        # need to log report_by_date to avoid race condition
        transit_ti_and_t(task_instance, status, db, data["next_report_increment"])
    else:
        logger.error(f"Unable to transition to launched from {task_instance.status}")

    # Rest of the function...


@api_v3_router.post("/task_instance/{task_instance_id}/log_known_error")
async def log_known_error(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (int): id for task instance.
        request (Request): fastapi request object.
        db: The database session.
    """
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    try:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()

        resp = _log_error(
            db,
            task_instance,
            error_state,
            error_message,
            distributor_id,
            nodename,
            request,
        )
    except OperationalError as e:
        if "database is locked" in str(e):
            logger.warning(
                f"Database lock detected for task_instance {task_instance_id}, retrying..."
            )
            raise HTTPException(
                status_code=503, detail="Database temporarily unavailable, please retry"
            )
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(
            db,
            task_instance,
            error_state,
            new_msg,
            distributor_id,
            nodename,
            request,
        )
    return resp


@api_v3_router.post("/task_instance/{task_instance_id}/log_unknown_error")
async def log_unknown_error(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (int): id for task instance
        request (Request): fastapi request object
        db: The database session.
    """
    structlog.contextvars.bind_contextvars(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    # make sure the task hasn't logged a new heartbeat since we began
    # reconciliation
    try:
        select_stmt = select(TaskInstance).where(
            TaskInstance.id == task_instance_id,
            TaskInstance.report_by_date <= func.now(),
        )
        task_instance = db.execute(select_stmt).scalars().one_or_none()

        if task_instance is not None:
            try:
                resp = _log_error(
                    db,
                    task_instance,
                    error_state,
                    error_message,
                    distributor_id,
                    nodename,
                    request,
                )
            except OperationalError as e:
                if "database is locked" in str(e):
                    logger.warning(
                        f"Database lock detected for ti {task_instance_id}, retrying..."
                    )
                    raise HTTPException(
                        status_code=503,
                        detail="Database temporarily unavailable, please retry",
                    )
                # modify the error message and retry
                new_msg = error_message.encode("latin1", "replace").decode("utf-8")
                resp = _log_error(
                    db,
                    task_instance,
                    error_state,
                    new_msg,
                    distributor_id,
                    nodename,
                    request,
                )
        return resp
    except Exception as e:
        raise e


@api_v3_router.post("/task_instance/instantiate_task_instances")
async def instantiate_task_instances(
    request: Request, db: Session = Depends(get_db)
) -> Any:
    """Sync status of given task intance IDs."""
    data = cast(Dict, await request.json())
    task_instance_ids_list = tuple([int(tid) for tid in data["task_instance_ids"]])

    # update the task table where FSM allows it
    sub_query = (
        select(Task.id)
        .join(TaskInstance, TaskInstance.task_id == Task.id)
        .where(
            and_(
                TaskInstance.id.in_(task_instance_ids_list),
                Task.status == constants.TaskStatus.QUEUED,
            )
        )
    ).alias("derived_table")
    task_update = (
        update(Task)
        .where(Task.id.in_(select(sub_query.c.id)))
        .values(status=constants.TaskStatus.INSTANTIATING, status_date=func.now())
        .execution_options(synchronize_session=False)
    )
    db.execute(task_update)

    # then propagate back into task instance where a change was made
    sub_query = (
        select(TaskInstance.id)
        .join(Task, TaskInstance.task_id == Task.id)
        .where(
            and_(
                # a successful transition
                (Task.status == constants.TaskStatus.INSTANTIATING),
                # and part of the current set
                TaskInstance.id.in_(task_instance_ids_list),
            )
        )
    ).alias("derived_table")
    task_instance_update = (
        update(TaskInstance)
        .where(TaskInstance.id.in_(select(sub_query.c.id)))
        .values(
            status=constants.TaskInstanceStatus.INSTANTIATED,
            status_date=func.now(),
        )
        .execution_options(synchronize_session=False)
    )
    db.execute(task_instance_update)

    db.flush()
    # fetch rows individually without group_concat
    # Key is a tuple of array_id, array_name, array_batch_num, task_resources_id
    # Values are task instances in this batch
    grouped_data: DefaultDict = defaultdict(list)
    instantiated_batches_query = (
        select(
            TaskInstance.array_id,
            Array.name,
            TaskInstance.array_batch_num,
            TaskInstance.task_resources_id,
            TaskInstance.id,
        ).where(
            TaskInstance.id.in_(task_instance_ids_list)
            & (TaskInstance.status == constants.TaskInstanceStatus.INSTANTIATED)
            & (TaskInstance.array_id == Array.id)
        )
        # Optionally, add an order_by clause here to make the rows easier to work with
    )

    # Collect the rows into the defaultdict
    for (
        array_id,
        array_name,
        array_batch_num,
        task_resources_id,
        task_instance_id,
    ) in db.execute(instantiated_batches_query):
        key = (array_id, array_batch_num, array_name, task_resources_id)
        grouped_data[key].append(int(task_instance_id))

    # Serialize the grouped data
    serialized_batches = []
    for key, task_instance_ids in grouped_data.items():
        array_id, array_batch_num, array_name, task_resources_id = key
        serialized_batches.append(
            SerializeTaskInstanceBatch.to_wire(
                array_id=array_id,
                array_name=array_name,
                array_batch_num=array_batch_num,
                task_resources_id=task_resources_id,
                task_instance_ids=task_instance_ids,
            )
        )

    resp = JSONResponse(
        content={"task_instance_batches": serialized_batches},
        status_code=StatusCodes.OK,
    )
    return resp


# ############################ HELPER FUNCTIONS ###############################
def _update_task_instance_state(
    task_instance: TaskInstance, status_id: str, request: Request, db: Session
) -> Any:
    """Advance the states of task_instance and it's associated Task.

    Return any messages that should be published based on the transition.

    Args:
        task_instance (TaskInstance): object of time models.TaskInstance
        status_id (int): id of the status to which to transition
        request (Request): fastapi request object
        db (Session): database session
    """
    response = ""

    try:
        # if the ti is already in the new state, just log it
        if task_instance.status == status_id:
            msg = (
                f"Attempting to transition to existing state."
                f"Not transitioning task, tid= "
                f"{task_instance.id} from {task_instance.status} to "
                f"{status_id}"
            )
            response += msg
        else:
            status = get_transit_status(task_instance, status_id)
            if status is not None:
                transit_ti_and_t(task_instance, status, db)
            else:
                msg = (
                    f"Illegal state transition. Not transitioning task, "
                    f"tid={task_instance.id}, from {task_instance.status} to "
                    f"{status_id}"
                )
                logger.error(msg)
                response += msg
    except Exception as e:
        raise ServerError(
            f"General exception in _update_task_instance_state, jid "
            f"{task_instance}, transitioning to {task_instance}. Not "
            f"transitioning task. Server Error in {request.url.path}",
            status_code=500,
        ) from e
    return response


def _log_error(
    session: Session,
    ti: TaskInstance,
    error_state: str,
    error_msg: str,
    distributor_id: Optional[int] = None,
    nodename: Optional[str] = None,
    request: Optional[Request] = None,
) -> Any:
    if nodename is not None:
        ti.nodename = nodename  # type: ignore
    if distributor_id is not None:
        ti.distributor_id = str(distributor_id)

    try:
        error = TaskInstanceErrorLog(task_instance_id=ti.id, description=error_msg)
        session.add(error)
        msg = _update_task_instance_state(ti, error_state, request, session)  # type: ignore
        session.flush()
        resp = JSONResponse(content={"message": msg}, status_code=StatusCodes.OK)
    except Exception as e:
        # Always complete the request successfully to avoid infinite retries
        logger.error(f"Failed to log error for task_instance {ti.id}: {e}")
        resp = JSONResponse(
            content={"message": "Error logged with warnings"},
            status_code=StatusCodes.OK,
        )

    return resp
