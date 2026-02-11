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
from jobmon.core.logging import set_jobmon_context
from jobmon.core.serializers import SerializeTaskInstanceBatch
from jobmon.server.web._compat import add_time
from jobmon.server.web.db.deps import get_db, get_dialect
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import ServerError
from jobmon.server.web.services.transition_service import TransitionService

logger = structlog.get_logger(__name__)


@api_v3_router.post("/task_instance/{task_instance_id}/log_running")
async def log_running(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance as running."""
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    logger.info(
        "Server received log_running request",
        task_instance_id=task_instance_id,
        nodename=data.get("nodename"),
        distributor_id=data.get("distributor_id"),
    )

    try:
        # load task_instance; do not lock
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()

        # Handle state transition
        dialect = get_dialect(request)
        result = TransitionService.transition_task_instance(
            session=db,
            task_instance_id=task_instance.id,
            task_id=task_instance.task_id,
            current_ti_status=task_instance.status,
            new_ti_status=constants.TaskInstanceStatus.RUNNING,
            task_num_attempts=task_instance.task.num_attempts,
            task_max_attempts=task_instance.task.max_attempts,
            report_by_date=add_time(data["next_report_increment"], dialect),
        )
        if result["ti_updated"]:
            logger.info(
                f"Task instance {task_instance_id} transitioned to RUNNING in database"
            )
            db.commit()
        else:
            if task_instance.status == constants.TaskInstanceStatus.RUNNING:
                logger.warning(
                    f"Unable to transition to running from {task_instance.status}"
                )
            elif task_instance.status == constants.TaskInstanceStatus.KILL_SELF:
                # KILL_SELF means workflow resume requested termination.
                # Transition to ERROR_FATAL. This is safe because the workflow
                # won't be resumable until all KILL_SELF TIs are cleaned up.
                result = TransitionService.transition_task_instance(
                    session=db,
                    task_instance_id=task_instance.id,
                    task_id=task_instance.task_id,
                    current_ti_status=task_instance.status,
                    new_ti_status=constants.TaskInstanceStatus.ERROR_FATAL,
                    task_num_attempts=task_instance.task.num_attempts,
                    task_max_attempts=task_instance.task.max_attempts,
                    report_by_date=add_time(data["next_report_increment"], dialect),
                )
                if result["ti_updated"]:
                    db.commit()
            elif task_instance.status == constants.TaskInstanceStatus.NO_HEARTBEAT:
                result = TransitionService.transition_task_instance(
                    session=db,
                    task_instance_id=task_instance.id,
                    task_id=task_instance.task_id,
                    current_ti_status=task_instance.status,
                    new_ti_status=constants.TaskInstanceStatus.ERROR,
                    task_num_attempts=task_instance.task.num_attempts,
                    task_max_attempts=task_instance.task.max_attempts,
                    report_by_date=add_time(data["next_report_increment"], dialect),
                )
                if result["ti_updated"]:
                    db.commit()
            else:
                if result["error"] == "untimely_transition":
                    logger.warning(
                        f"Untimely transition to running from {task_instance.status}"
                    )
                else:
                    logger.error(
                        f"Unable to transition to running from {task_instance.status}"
                    )

        # Refresh and update attributes after any transition retries/rollbacks
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()
        if data.get("distributor_id") is not None:
            task_instance.distributor_id = data["distributor_id"]
        if data.get("nodename") is not None:
            task_instance.nodename = data["nodename"]
        task_instance.process_group_id = data["process_group_id"]

        # Commit any remaining attribute changes
        db.commit()

        wire_format = task_instance.to_wire_as_worker_node_task_instance()
        return JSONResponse(
            content={"task_instance": wire_format}, status_code=StatusCodes.OK
        )

    except Exception as e:
        logger.error(f"Failed to log running for task_instance {task_instance_id}: {e}")
        db.rollback()
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
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    logger.debug(
        "Server received heartbeat",
        distributor_id=data.get("distributor_id"),
    )
    # Retry logic for row lock contention
    max_retries = 5
    dialect = get_dialect(request)
    for attempt in range(max_retries):
        try:
            vals = {"report_by_date": add_time(data["next_report_increment"], dialect)}
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
                result = TransitionService.transition_task_instance(
                    session=db,
                    task_instance_id=task_instance.id,
                    task_id=task_instance.task_id,
                    current_ti_status=task_instance.status,
                    new_ti_status=constants.TaskInstanceStatus.RUNNING,
                    task_num_attempts=task_instance.task.num_attempts,
                    task_max_attempts=task_instance.task.max_attempts,
                    report_by_date=add_time(data["next_report_increment"], dialect),
                )
                if result["ti_updated"]:
                    logger.info(
                        "Heartbeat triggered transition from TRIAGING to RUNNING",
                        task_instance_id=task_instance_id,
                    )
                    db.commit()
                else:
                    logger.error(
                        f"Unable to transition to running from {task_instance.status}"
                    )

            logger.debug("Heartbeat processed successfully")

            resp = JSONResponse(
                content={"status": task_instance.status}, status_code=StatusCodes.OK
            )
            return resp

        except OperationalError as e:
            logger.warning(
                f"Database lock detected for TI {task_instance_id}, "
                f"retrying attempt {attempt + 1}/{max_retries}. {e}"
            )
            db.rollback()
            sleep(0.001 * (2 ** (attempt + 1)))  # 2ms, 4ms, 8ms delays
            continue
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

    logger.debug(
        "Server received batch heartbeat",
        num_task_instances=len(tis) if tis else 0,
    )
    if tis:
        # Retry logic for row lock contention in batch updates
        max_retries = 5
        for attempt in range(max_retries):
            try:
                dialect = get_dialect(request)
                update_stmt = (
                    update(TaskInstance)
                    .where(
                        TaskInstance.id.in_(tis),
                        TaskInstance.status == constants.TaskInstanceStatus.LAUNCHED,
                    )
                    .values(report_by_date=add_time(next_report_increment, dialect))
                )

                db.execute(update_stmt)
                # immediately release the lock
                db.commit()

                logger.debug(
                    "Batch heartbeat processed successfully",
                    num_task_instances=len(tis) if tis else 0,
                )

                resp = JSONResponse(content={}, status_code=StatusCodes.OK)
                return resp

            except OperationalError as e:
                logger.warning(
                    f"Database error {e} detected for batch TI {tis}, "
                    f"retrying attempt {attempt + 1}/{max_retries}"
                )
                db.rollback()
                sleep(0.001 * (2 ** (attempt + 1)))  # 2ms, 4ms, 8ms delays
                continue
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
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    logger.info(
        "Server received log_done request",
        nodename=data.get("nodename"),
        distributor_id=data.get("distributor_id"),
    )

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
            "wallclock",
            "maxrss",
            "cpu",
            "usage_str",
        ]
        for field in optional_vals:
            val = data.get(field)
            if val is not None:
                setattr(task_instance, field, val)

        # Attempt transition using TransitionService
        result = TransitionService.transition_task_instance(
            session=db,
            task_instance_id=task_instance.id,
            task_id=task_instance.task_id,
            current_ti_status=task_instance.status,
            new_ti_status=constants.TaskInstanceStatus.DONE,
            task_num_attempts=task_instance.task.num_attempts,
            task_max_attempts=task_instance.task.max_attempts,
        )
        if result["ti_updated"]:
            logger.info(
                f"Task instance {task_instance_id} transitioned to DONE in database"
            )
            db.commit()
        else:
            if result["error"] == "untimely_transition":
                logger.warning(
                    f"Untimely transition to DONE from {task_instance.status}"
                )
            elif task_instance.status == constants.TaskInstanceStatus.DONE:
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
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    try:
        # do not lock TaskInstance
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = db.execute(select_stmt).scalars().one()

        # ti that tries to log error should not in T unless there was a race condition
        # we need to transition to R first
        if task_instance.status == constants.TaskInstanceStatus.TRIAGING:
            result = TransitionService.transition_task_instance(
                session=db,
                task_instance_id=task_instance.id,
                task_id=task_instance.task_id,
                current_ti_status=task_instance.status,
                new_ti_status=constants.TaskInstanceStatus.RUNNING,
                task_num_attempts=task_instance.task.num_attempts,
                task_max_attempts=task_instance.task.max_attempts,
            )
            if result["ti_updated"]:
                db.commit()
                # Refresh task_instance to get updated status
                db.refresh(task_instance)
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
            "wallclock",
            "maxrss",
            "cpu",
            "usage_str",
        ]
        for field in optional_vals:
            val = data.get(field)
            if val is not None:
                setattr(task_instance, field, val)

        # Error handling
        error_state = data["error_state"]
        error_description = data["error_description"]

        result = TransitionService.transition_task_instance(
            session=db,
            task_instance_id=task_instance.id,
            task_id=task_instance.task_id,
            current_ti_status=task_instance.status,
            new_ti_status=error_state,
            task_num_attempts=task_instance.task.num_attempts,
            task_max_attempts=task_instance.task.max_attempts,
        )
        if result["ti_updated"]:
            # Create error log entry
            error = TaskInstanceErrorLog(
                task_instance_id=task_instance.id,
                description=error_description,
            )
            db.add(error)
            # release locks immediately
            db.commit()
        else:
            if result["error"] == "untimely_transition":
                logger.warning(
                    f"Untimely transition to {error_state} from {task_instance.status}"
                )
            elif task_instance.status == error_state:
                logger.warning(
                    f"Unable to transition to {error_state} from {task_instance.status}"
                )
            else:
                logger.error(
                    f"Unable to transition to {error_state} from {task_instance.status}"
                )

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
    set_jobmon_context(task_instance_id=task_instance_id)
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
    set_jobmon_context(array_id=array_id)

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
    set_jobmon_context(task_instance_id=task_instance_id)
    logger.info(
        f"Logging ti {task_instance_id} did not get distributor id upon submission"
    )
    data = cast(Dict, await request.json())
    err_msg = data["no_id_err_msg"]

    # Retry logic for database operations
    max_retries = 5

    for attempt in range(max_retries):
        try:
            select_stmt = select(TaskInstance).where(
                TaskInstance.id == task_instance_id
            )
            task_instance = db.execute(select_stmt).scalars().one()
            msg = _update_task_instance_state(
                task_instance,
                constants.TaskInstanceStatus.NO_DISTRIBUTOR_ID,
                request,
                db,
            )
            error = TaskInstanceErrorLog(
                task_instance_id=task_instance.id, description=err_msg
            )
            db.add(error)
            # release locks immediately
            db.commit()
            resp = JSONResponse(content={"message": msg}, status_code=StatusCodes.OK)
            return resp

        except OperationalError as e:
            logger.warning(
                f"Database lock detected for TI {task_instance_id}, "
                f"retrying attempt {attempt + 1}/{max_retries}. {e}"
            )
            db.rollback()
            sleep(0.001 * (2 ** (attempt + 1)))  # 2ms, 4ms, 8ms delays
            continue
        except Exception as e:
            logger.error(
                f"Failed to log no distributor id for task_instance {task_instance_id}: {e}"
            )
            db.rollback()
            raise HTTPException(status_code=503, detail=f"{e}")
    # All retries failed
    logger.error(
        f"Failed to log no distributor id for task_instance {task_instance_id} "
        f"after {max_retries} attempts"
    )
    db.rollback()
    raise HTTPException(
        status_code=503, detail="Database temporarily unavailable, please retry"
    )


@api_v3_router.post("/task_instance/{task_instance_id}/log_distributor_id")
async def log_distributor_id(
    task_instance_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a task_instance's distributor id."""
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())

    select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
    task_instance = db.execute(select_stmt).scalars().one()

    # Update distributor_id and report_by_date with retry logic
    # This must happen regardless of whether we need to transit status or not
    max_retries = 5
    update_successful = False

    dialect = get_dialect(request)
    for attempt in range(max_retries):
        try:
            task_instance.distributor_id = data["distributor_id"]
            task_instance.report_by_date = add_time(
                data["next_report_increment"], dialect
            )
            # release locks immediately
            db.commit()
            update_successful = True
            break  # Success - exit retry loop
        except OperationalError as e:
            logger.warning(
                f"Database lock detected for TI {task_instance_id}, "
                f"retrying attempt {attempt + 1}/{max_retries}. {e}"
            )
            db.rollback()
            sleep(0.001 * (2 ** (attempt + 1)))  # 2ms, 4ms, 8ms delays
            continue
        except Exception as e:
            logger.error(
                f"Failed to log distributor id for task_instance {task_instance_id}: {e}"
            )
            db.rollback()
            raise HTTPException(status_code=503, detail=f"{e}")

    # If all retries failed for the distributor update, we need to exit
    if not update_successful:
        logger.error(f"Failed to update distributor_id after {max_retries} attempts")
        db.rollback()
        raise HTTPException(
            status_code=503, detail="Database temporarily unavailable, please retry"
        )

    # Check if task instance is in a final state
    if task_instance.status in [
        constants.TaskInstanceStatus.DONE,
        constants.TaskInstanceStatus.ERROR,
        constants.TaskInstanceStatus.ERROR_FATAL,
    ]:
        logger.debug(
            "Task instance in final state, no transition needed",
            current_status=task_instance.status,
        )

        return JSONResponse(
            content={"message": "Task instance in final state, no transition needed"},
            status_code=StatusCodes.OK,
        )
    else:
        # Only try to transition if not in final state (distributor_id already updated above)
        result = TransitionService.transition_task_instance(
            session=db,
            task_instance_id=task_instance.id,
            task_id=task_instance.task_id,
            current_ti_status=task_instance.status,
            new_ti_status=constants.TaskInstanceStatus.LAUNCHED,
            task_num_attempts=task_instance.task.num_attempts,
            task_max_attempts=task_instance.task.max_attempts,
            report_by_date=add_time(data["next_report_increment"], dialect),
        )
        if result["ti_updated"]:
            db.commit()
            return JSONResponse(
                content={"message": "Task instance transitioned to LAUNCHED"},
                status_code=StatusCodes.OK,
            )
        else:
            if result["error"] == "untimely_transition":
                logger.warning(
                    f"Untimely transition to LAUNCHED from {task_instance.status}"
                )
            else:
                logger.error(
                    f"Unable to transition to launched from {task_instance.status}"
                )
            # distributor_id was already committed above, so we're done
            return JSONResponse(
                content={
                    "message": f"Unable to transition to launched from {task_instance.status}"
                },
                status_code=StatusCodes.OK,
            )


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
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)

    logger.info(
        "Server received known error from triage",
        error_state=error_state,
        distributor_id=distributor_id,
        nodename=nodename,
    )

    # Query task_instance outside try-except to ensure it's always available
    select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
    task_instance = db.execute(select_stmt).scalars().one()

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
    set_jobmon_context(task_instance_id=task_instance_id)
    data = cast(Dict, await request.json())
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)

    logger.info(
        "Server received unknown error from triage",
        error_state=error_state,
        distributor_id=distributor_id,
        nodename=nodename,
    )

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

    logger.info(
        "Server received batch instantiation request",
        num_tasks=len(task_instance_ids_list),
        task_instance_ids=task_instance_ids_list[:10],  # Log first 10 for debugging
    )

    # Atomic update of both Task and TaskInstance with retry logic
    max_retries = 5

    for attempt in range(max_retries):
        try:
            # 1) Get task IDs from the task instance IDs
            task_ids_query = (
                select(Task.id)
                .join(TaskInstance, TaskInstance.task_id == Task.id)
                .where(TaskInstance.id.in_(task_instance_ids_list))
                .distinct()
            )
            task_ids = [row[0] for row in db.execute(task_ids_query).all()]

            # 2) Use TransitionService for QUEUED -> INSTANTIATING with audit
            if task_ids:
                result = TransitionService.transition_tasks(
                    session=db,
                    task_ids=task_ids,
                    to_status=constants.TaskStatus.INSTANTIATING,
                    use_skip_locked=True,
                )
                transitioned_task_ids = result["transitioned"]
            else:
                transitioned_task_ids = []

            # 3) Then propagate back into task instance where a change was made
            if transitioned_task_ids:
                sub_query = (
                    select(TaskInstance.id)
                    .join(Task, TaskInstance.task_id == Task.id)
                    .where(
                        and_(
                            # Task was transitioned by the service
                            Task.id.in_(transitioned_task_ids),
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

            # 4) Atomic commit - both updates succeed or both fail
            db.commit()

            # Log each successfully instantiated task instance (info level - state transition)
            for task_instance_id in task_instance_ids_list:
                logger.info(
                    "Task instance transitioned to INSTANTIATED",
                    task_instance_id=task_instance_id,
                )

            logger.info(
                "Batch instantiation transition completed",
                num_tasks=len(task_instance_ids_list),
            )

            # Success - continue with the rest of the function
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
                # Log batch summary
                logger.info(
                    f"Batch {array_batch_num} for array {array_name} (ID: {array_id}) "
                    f"instantiated with {len(task_instance_ids)} task instances",
                    array_id=array_id,
                    array_batch_num=array_batch_num,
                    array_name=array_name,
                    task_instance_ids=task_instance_ids,
                )

            resp = JSONResponse(
                content={"task_instance_batches": serialized_batches},
                status_code=StatusCodes.OK,
            )
            return resp

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
                    f"Database error detected for atomic Task/TaskInstance instantiation, "
                    f"retrying attempt {attempt + 1}/{max_retries}. {e}"
                )
                db.rollback()  # Clear the corrupted session state
                sleep(0.001 * (2 ** (attempt + 1)))  # Exponential backoff: 2ms, 4ms...
            else:
                logger.error(f"Unexpected database error in atomic instantiation: {e}")
                db.rollback()
                raise e
        except Exception as e:
            logger.error(f"Failed to instantiate task instances: {e}")
            db.rollback()
            raise e

    # If we get here, all retries failed
    logger.error(f"Failed to instantiate task instances after {max_retries} attempts")
    db.rollback()
    raise HTTPException(
        status_code=503, detail="Database temporarily unavailable, please retry"
    )


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
            result = TransitionService.transition_task_instance(
                session=db,
                task_instance_id=task_instance.id,
                task_id=task_instance.task_id,
                current_ti_status=task_instance.status,
                new_ti_status=status_id,
                task_num_attempts=task_instance.task.num_attempts,
                task_max_attempts=task_instance.task.max_attempts,
            )
            if result["ti_updated"]:
                db.commit()
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

    logger.info(
        "Processing error transition for task instance",
        task_instance_id=ti.id,
        error_state=error_state,
        current_status=ti.status,
    )

    # Check if already in target state (idempotent - no error log needed)
    if ti.status == error_state:
        logger.info(
            "Task instance already in error state, skipping duplicate log",
            task_instance_id=ti.id,
            error_state=error_state,
        )
        resp = JSONResponse(
            content={"message": "Already in error state"},
            status_code=StatusCodes.OK,
        )
        return resp

    try:
        # Perform the transition using TransitionService (has internal retry logic)
        result = TransitionService.transition_task_instance(
            session=session,
            task_instance_id=ti.id,
            task_id=ti.task_id,
            current_ti_status=ti.status,
            new_ti_status=error_state,
            task_num_attempts=ti.task.num_attempts,
            task_max_attempts=ti.task.max_attempts,
        )

        if result["ti_updated"]:
            # Create error log only after a successful transition. TransitionService
            # can rollback on lock contention, which would otherwise clear the log.
            error = TaskInstanceErrorLog(task_instance_id=ti.id, description=error_msg)
            session.add(error)
            logger.info(
                "Task instance transitioned to error state",
                task_instance_id=ti.id,
                error_state=error_state,
                orphaned=result.get("orphaned", False),
            )
            session.commit()
            resp = JSONResponse(content={"message": ""}, status_code=StatusCodes.OK)
            return resp
        else:
            # Transition was not valid - rollback error log
            session.rollback()
            if result["error"] == "untimely_transition":
                logger.warning(
                    "Untimely error transition, not creating error log",
                    task_instance_id=ti.id,
                    current_status=ti.status,
                    requested_status=error_state,
                )
            else:
                logger.warning(
                    "Invalid error transition, not creating error log",
                    task_instance_id=ti.id,
                    current_status=ti.status,
                    requested_status=error_state,
                    error=result["error"],
                )
            resp = JSONResponse(
                content={
                    "message": f"Invalid transition from {ti.status} to {error_state}"
                },
                status_code=StatusCodes.OK,
            )
            return resp

    except Exception as e:
        # Always complete the request successfully to avoid infinite retries
        logger.error(f"Failed to log error for task_instance {ti.id}: {e}")
        session.rollback()
        resp = JSONResponse(
            content={"message": "Error logged with warnings"},
            status_code=StatusCodes.OK,
        )
        return resp
