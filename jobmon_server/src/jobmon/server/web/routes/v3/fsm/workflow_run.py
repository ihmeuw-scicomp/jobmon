"""Routes for WorkflowRuns."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, cast

import structlog
from fastapi import Depends, Request
from sqlalchemy import and_, func, insert, select, update
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core import constants
from jobmon.core.exceptions import InvalidStateTransition
from jobmon.core.logging import set_jobmon_context
from jobmon.server.web._compat import subtract_time
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db.deps import get_db, get_dialect
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)


@api_v3_router.post("/workflow_run")
async def add_workflow_run(request: Request, db: Session = Depends(get_db)) -> Any:
    """Add a workflow run to the db."""
    try:
        data = cast(Dict, await request.json())
        workflow_id = int(data["workflow_id"])
        user = data["user"]
        jobmon_version = data["jobmon_version"]
        next_heartbeat = float(data["next_report_increment"])

        set_jobmon_context(workflow_id=workflow_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    logger.info(f"Add wfr for workflow_id:{workflow_id}.")

    workflow = db.execute(select(Workflow).where(Workflow.id == workflow_id)).scalar()
    err_msg = ""
    if not workflow:
        # Binding to a non-existent workflow, exit early
        err_msg = f"No workflow exists for ID {workflow_id}"
        resp = JSONResponse(
            content={"workflow_run_id": None, "err_msg": err_msg},
            status_code=StatusCodes.OK,
        )
        return resp

    workflow_run = WorkflowRun(
        workflow_id=workflow_id,
        user=user,
        jobmon_version=jobmon_version,
        status=constants.WorkflowRunStatus.REGISTERED,
    )
    db.add(workflow_run)
    db.flush()

    db.refresh(workflow, with_for_update=True)
    # Transition to linking state, with_for_update claims a lock on the workflow
    # Any other actively linking workflows will return the incorrect workflow run id

    dialect = get_dialect(request)
    active_workflow_run = workflow.link_workflow_run(
        workflow_run, next_heartbeat, dialect
    )
    db.flush()

    try:
        if active_workflow_run[0] != workflow_run.id:
            err_msg = (
                f"WorkflowRun {active_workflow_run[0]} is currently"
                f"linking, WorkflowRun {workflow_run.id} will be aborted."
            )
    except IndexError:
        # Raised if the workflow is not resume-able, without any active workflowruns
        # Unlikely to be raised
        err_msg = f"Workflow {workflow_id} is not in a resume-able state"

    if err_msg:
        resp = JSONResponse(
            content={"workflow_run_id": None, "err_msg": err_msg},
            status_code=StatusCodes.OK,
        )
    else:
        logger.info(f"Add workflow_run:{workflow_run.id} for workflow.")
        resp = JSONResponse(
            content={
                "workflow_run_id": workflow_run.id,
                "status": workflow_run.status,
            },
            status_code=StatusCodes.OK,
        )
    return resp


@api_v3_router.put("/workflow_run/{workflow_run_id}/terminate_task_instances")
async def terminate_workflow_run(
    workflow_run_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Terminate task instances for a workflow run being resumed.

    - QUEUED/INSTANTIATED: Set directly to ERROR_FATAL (no worker exists to clean up)
    - LAUNCHED/RUNNING: Set to KILL_SELF (worker will detect and clean up)
    - HOT_RESUME: Preserve RUNNING tasks (let them finish)
    - COLD_RESUME: Kill everything including RUNNING tasks
    """
    set_jobmon_context(workflow_run_id=workflow_run_id)
    logger.info("Terminating task instances for workflow_run")
    try:
        workflow_run_id = int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    select_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
    workflow_run = db.execute(select_stmt).scalars().one()

    # TIs without a worker (no job submitted) - go directly to ERROR_FATAL
    ti_states_to_error_fatal = [
        constants.TaskInstanceStatus.QUEUED,
        constants.TaskInstanceStatus.INSTANTIATED,
    ]

    # TIs with a worker (job in cluster) - set to KILL_SELF for worker cleanup
    # HOT_RESUME: Don't kill running tasks (let them finish)
    # COLD_RESUME: Kill everything including running tasks
    if workflow_run.status == constants.WorkflowRunStatus.HOT_RESUME:
        ti_states_to_kill_self = [
            constants.TaskInstanceStatus.LAUNCHED,
        ]
    else:
        ti_states_to_kill_self = [
            constants.TaskInstanceStatus.LAUNCHED,
            constants.TaskInstanceStatus.RUNNING,
        ]

    # 1) Log and transition QUEUED/INSTANTIATED directly to ERROR_FATAL
    insert_error_log_stmt = insert(TaskInstanceErrorLog).from_select(
        ["task_instance_id", "description", "error_time"],
        select(
            TaskInstance.id,
            (
                "Workflow resume requested. Setting to ERROR_FATAL from status: "
                + TaskInstance.status
                + " (no worker to clean up)"
            ),
            func.now(),
        ).where(
            TaskInstance.workflow_run_id == workflow_run_id,
            TaskInstance.status.in_(ti_states_to_error_fatal),
        ),
    )
    db.execute(insert_error_log_stmt)

    update_to_error_fatal = (
        update(TaskInstance)
        .where(
            TaskInstance.workflow_run_id == workflow_run_id,
            TaskInstance.status.in_(ti_states_to_error_fatal),
        )
        .values(
            status=constants.TaskInstanceStatus.ERROR_FATAL,
            status_date=func.now(),
        )
        .execution_options(synchronize_session=False)
    )
    result_error_fatal = db.execute(update_to_error_fatal)

    # 2) Log and transition LAUNCHED/RUNNING to KILL_SELF (worker will clean up)
    insert_kill_self_log_stmt = insert(TaskInstanceErrorLog).from_select(
        ["task_instance_id", "description", "error_time"],
        select(
            TaskInstance.id,
            (
                "Workflow resume requested. Setting to KILL_SELF from status: "
                + TaskInstance.status
            ),
            func.now(),
        ).where(
            TaskInstance.workflow_run_id == workflow_run_id,
            TaskInstance.status.in_(ti_states_to_kill_self),
        ),
    )
    db.execute(insert_kill_self_log_stmt)

    update_to_kill_self = (
        update(TaskInstance)
        .where(
            TaskInstance.workflow_run_id == workflow_run_id,
            TaskInstance.status.in_(ti_states_to_kill_self),
        )
        .values(
            status=constants.TaskInstanceStatus.KILL_SELF,
            status_date=func.now(),
        )
        .execution_options(synchronize_session=False)
    )
    result_kill_self = db.execute(update_to_kill_self)

    logger.info(
        "Task instances terminated for workflow resume",
        workflow_run_id=workflow_run_id,
        num_error_fatal=result_error_fatal.rowcount,
        num_kill_self=result_kill_self.rowcount,
        resume_type=workflow_run.status,
    )

    resp = JSONResponse(
        content={
            "workflow_run_id": workflow_run_id,
            "num_terminated": result_error_fatal.rowcount + result_kill_self.rowcount,
        },
        status_code=StatusCodes.OK,
    )
    return resp


@api_v3_router.post("/workflow_run/{workflow_run_id}/log_heartbeat")
async def log_workflow_run_heartbeat(
    workflow_run_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Log a heartbeat for the workflow run to show that the client side is still alive."""
    set_jobmon_context(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
        data = cast(Dict, await request.json())
        next_report_increment = data["next_report_increment"]
        status = data["status"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    logger.debug(f"WFR {workflow_run_id} heartbeat data")

    select_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
    workflow_run = db.execute(select_stmt).scalars().one()

    try:
        dialect = get_dialect(request)
        workflow_run.heartbeat(next_report_increment, dialect, status)
        logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
    except InvalidStateTransition as e:
        logger.debug(f"wfr {workflow_run_id} heartbeat rolled back, reason: {e}")

    resp = JSONResponse(
        content={"status": str(workflow_run.status)}, status_code=StatusCodes.OK
    )
    return resp


@api_v3_router.put("/workflow_run/{workflow_run_id}/update_status")
async def log_workflow_run_status_update(
    workflow_run_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Update the status of the workflow run."""
    set_jobmon_context(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
        data = cast(Dict, await request.json())
        status = data["status"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    logger.info(f"Log status update for workflow_run_id:{workflow_run_id}.")

    select_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
    workflow_run = db.execute(select_stmt).scalars().one()

    try:
        workflow_run.transition(status)
    except InvalidStateTransition as e:
        logger.warning(e)

    # Return the status
    status = workflow_run.status

    resp = JSONResponse(content={"status": status}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/workflow_run/{workflow_run_id}/sync_status")
async def task_instances_status_check(
    workflow_run_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Sync status of given task intance IDs."""
    set_jobmon_context(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
        data = cast(Dict, await request.json())
        task_instance_ids = data["task_instance_ids"]
        status = data["status"]
        logger.info(
            f"Sync status check for status={status}, "
            f"task_instance_ids={task_instance_ids}"
        )
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    # get time from db
    db_time = db.execute(select(func.now())).scalar()
    if db_time:
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        str_time = None

    where_clause = [TaskInstance.workflow_run_id == workflow_run_id]
    if len(task_instance_ids) > 0:
        # Filters for
        # 1) instances that have changed out of the declared status
        # 2) instances that have changed into the declared status
        where_clause.append(
            (TaskInstance.id.in_(task_instance_ids) & (TaskInstance.status != status))
            | (
                TaskInstance.id.notin_(task_instance_ids)
                & (TaskInstance.status == status)
            )
        )
    else:
        where_clause.append(TaskInstance.status == status)

    return_dict: Dict[str, List[int]] = defaultdict(list)
    select_stmt = (
        select(TaskInstance.status, TaskInstance.id)
        .where(*where_clause)
        .order_by(TaskInstance.status)  # Optional, but helps organize the result
    )

    for row in db.execute(select_stmt):
        return_dict[row[0]].append(int(row[1]))

    resp = JSONResponse(
        content={"status_updates": dict(return_dict), "time": str_time},
        status_code=StatusCodes.OK,
    )
    return resp


@api_v3_router.post("/workflow_run/{workflow_run_id}/set_status_for_triaging")
async def set_status_for_triaging(
    workflow_run_id: int, request: Request, db: Session = Depends(get_db)
) -> Any:
    """Two triaging related status sets with improved deadlock prevention.

    Query all task instances that are submitted to distributor or running which haven't
    reported as alive in the allocated time, and set them for Triaging(from Running)
    and NO_HEARTBEAT(from Launched).
    """
    # unlike postgres, MySql does not support with_for_update(skip_locked=True)
    # which makes more sence for this use case
    # thus, there isn't a perfect solution to avoild race conditions/deadlocks
    # thus, we are using a buffer to account for query execution time and
    # reduce false positives; split the update for launched and running tasks
    # this is a trade off between performance and correctness
    set_jobmon_context(workflow_run_id=workflow_run_id)
    # get jobmon heartbeat interval

    try:
        workflow_run_id = int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    logger.info(
        "Server checking for overdue task instances to set to TRIAGING",
        workflow_run_id=workflow_run_id,
    )

    # Get heartbeat interval from config for retry exclusion logic
    config = get_jobmon_config()
    heartbeat_interval = float(config.get("heartbeat", "task_instance_interval"))
    hb_buffer_from_config = float(config.get("heartbeat", "report_by_buffer"))
    hb_buffer = hb_buffer_from_config * heartbeat_interval

    total_updated = 0

    # Process RUNNING tasks first
    try:
        # Check for overdue tasks (client already provides 3.1x buffer)
        initial_time = func.now()
        running_select_stmt = select(TaskInstance.id).where(
            and_(
                TaskInstance.workflow_run_id == workflow_run_id,
                TaskInstance.status == constants.TaskInstanceStatus.RUNNING,
                TaskInstance.report_by_date <= initial_time,
            )
        )

        running_ti_ids = [row[0] for row in db.execute(running_select_stmt).fetchall()]

        if running_ti_ids:
            # Get fresh timestamp for update
            update_time = func.now()

            # Update only the specific task instances with fresh time check
            running_update_stmt = (
                update(TaskInstance)
                .where(
                    and_(
                        TaskInstance.id.in_(running_ti_ids),
                        TaskInstance.status == constants.TaskInstanceStatus.RUNNING,
                        TaskInstance.report_by_date <= update_time,
                    )
                )
                .values(
                    status=constants.TaskInstanceStatus.TRIAGING, status_date=func.now()
                )
                .execution_options(synchronize_session=False)
            )

            running_result = db.execute(running_update_stmt)
            total_updated += running_result.rowcount

            # Log each task instance set to triaging (info level - state transition)
            for ti_id in running_ti_ids[: running_result.rowcount]:
                logger.info(
                    "Task instance set to TRIAGING (overdue from RUNNING)",
                    task_instance_id=ti_id,
                )

            logger.info(
                "Set RUNNING task instances to TRIAGING",
                num_task_instances=running_result.rowcount,
            )

            # Step 4: Commit RUNNING updates
            db.commit()

    except Exception as e:
        logger.error(f"Error updating RUNNING task instances: {e}")
        db.rollback()
        raise e

    # Process LAUNCHED tasks separately
    try:
        # Check for overdue tasks (client already provides 3.1x buffer)
        initial_time = func.now()
        launched_select_stmt = select(TaskInstance.id).where(
            and_(
                TaskInstance.workflow_run_id == workflow_run_id,
                TaskInstance.status == constants.TaskInstanceStatus.LAUNCHED,
                TaskInstance.report_by_date <= initial_time,
            )
        )

        launched_ti_ids = [
            row[0] for row in db.execute(launched_select_stmt).fetchall()
        ]

        if launched_ti_ids:
            # Get fresh timestamp for update
            update_time = func.now()

            # Update only the specific task instances with fresh time check
            launched_update_stmt = (
                update(TaskInstance)
                .where(
                    and_(
                        TaskInstance.id.in_(launched_ti_ids),
                        TaskInstance.status == constants.TaskInstanceStatus.LAUNCHED,
                        TaskInstance.report_by_date <= update_time,
                        # Exclude recently changed status tasks (likely retries)
                        # use 2 jobmon heartbeat interval as a buffer
                        TaskInstance.status_date
                        <= subtract_time(hb_buffer * 2, get_dialect(request)),
                    )
                )
                .values(
                    status=constants.TaskInstanceStatus.NO_HEARTBEAT,
                    status_date=func.now(),
                )
                .execution_options(synchronize_session=False)
            )

            launched_result = db.execute(launched_update_stmt)
            total_updated += launched_result.rowcount

            # Log each task instance set to no heartbeat (info level - state transition)
            for ti_id in launched_ti_ids[: launched_result.rowcount]:
                logger.info(
                    "Task instance set to NO_HEARTBEAT (overdue from LAUNCHED)",
                    task_instance_id=ti_id,
                )

            logger.info(
                "Set LAUNCHED task instances to NO_HEARTBEAT",
                num_task_instances=launched_result.rowcount,
            )

            # Step 4: Commit LAUNCHED updates
            db.commit()

    except Exception as e:
        logger.error(f"Error updating LAUNCHED task instances: {e}")
        db.rollback()
        raise e

    logger.info(
        f"Triage check completed for workflow_run {workflow_run_id}: "
        f"{total_updated} task instances updated"
    )
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp
