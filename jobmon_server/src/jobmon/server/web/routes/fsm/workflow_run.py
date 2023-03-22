"""Routes for WorkflowRuns."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict, List

from flask import jsonify, request
from sqlalchemy import case, func, insert, select, update
import structlog

from jobmon.core import constants
from jobmon.core.exceptions import InvalidStateTransition
from jobmon.server.web.models.api import (
    Batch,
    DistributorInstance,
    Task,
    TaskInstance,
    TaskInstanceErrorLog,
    Workflow,
    WorkflowRun,
)
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.routes.fsm.distributor_instance import (
    _get_active_distributor_instance_id,
)
from jobmon.server.web.server_side_exception import InvalidUsage


logger = structlog.get_logger(__name__)


@blueprint.route("/workflow_run", methods=["POST"])
def add_workflow_run() -> Any:
    """Add a workflow run to the db."""
    try:
        data = cast(Dict, request.get_json())
        workflow_id = int(data["workflow_id"])
        user = data["user"]
        jobmon_version = data["jobmon_version"]
        next_heartbeat = float(data["next_report_increment"])

        structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    logger.info(f"Add wfr for workflow_id:{workflow_id}.")

    session = SessionLocal()

    with session.begin():
        workflow = session.execute(
            select(Workflow).where(Workflow.id == workflow_id)
        ).scalar()
        err_msg = ""
        if not workflow:
            # Binding to a non-existent workflow, exit early
            err_msg = f"No workflow exists for ID {workflow_id}"
            resp = jsonify(workflow_run_id=None, err_msg=err_msg)
            resp.status_code = StatusCodes.OK
            return resp

        workflow_run = WorkflowRun(
            workflow_id=workflow_id,
            user=user,
            jobmon_version=jobmon_version,
            status=constants.WorkflowRunStatus.REGISTERED,
        )
        session.add(workflow_run)
        session.flush()

        session.refresh(workflow, with_for_update=True)
        # Transition to linking state, with_for_update claims a lock on the workflow
        # Any other actively linking workflows will return the incorrect workflow run id

        active_workflow_run = workflow.link_workflow_run(workflow_run, next_heartbeat)
        session.flush()

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

    session.commit()
    if err_msg:
        logger.warning(f"Possible race condition in adding workflowrun: {err_msg}")
        resp = jsonify(workflow_run_id=None, err_msg=err_msg)
    else:
        logger.info(f"Add workflow_run:{workflow_run.id} for workflow.")
        resp = jsonify(workflow_run_id=workflow_run.id, status=workflow_run.status)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow_run/<workflow_run_id>/terminate_task_instances", methods=["PUT"]
)
def terminate_workflow_run(workflow_run_id: int) -> Any:
    """Terminate a workflow run and get its tasks in order."""
    structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
    logger.info("Terminate workflow_run")
    try:
        workflow_run_id = int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    session = SessionLocal()
    with session.begin():
        select_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
        workflow_run = session.execute(select_stmt).scalars().one()

        if workflow_run.status == constants.WorkflowRunStatus.HOT_RESUME:
            task_states = [constants.TaskStatus.LAUNCHED]
        else:
            task_states = [constants.TaskStatus.LAUNCHED, constants.TaskStatus.RUNNING]

        insert_error_log_stmt = insert(TaskInstanceErrorLog).from_select(
            ["task_instance_id", "description", "error_time"],
            select(
                TaskInstance.id,
                (
                    "Workflow resume requested. Setting to K from status of: "
                    + TaskInstance.status
                ),
                func.now(),
            ).where(
                TaskInstance.workflow_run_id == workflow_run_id,
                TaskInstance.status == constants.TaskInstanceStatus.KILL_SELF,
            ),
        )
        session.execute(insert_error_log_stmt)

        workflow_id = workflow_run.workflow_id
        update_task_instance_stmt = (
            update(TaskInstance)
            .where(
                TaskInstance.id.in_(
                    select(TaskInstance.id).where(
                        TaskInstance.workflow_run_id == WorkflowRun.id,
                        TaskInstance.task_id.in_(
                            select(Task.id).where(
                                Task.workflow_id == workflow_id,
                                Task.status.in_(task_states),
                            )
                        ),
                    )
                )
            )
            .values(
                status=constants.TaskInstanceStatus.KILL_SELF, status_date=func.now()
            )
            .execution_options(synchronize_session=False)
        )

        session.execute(update_task_instance_stmt)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_run/<workflow_run_id>/log_heartbeat", methods=["POST"])
def log_workflow_run_heartbeat(workflow_run_id: int) -> Any:
    """Log a heartbeat for the workflow run to show that the client side is still alive."""
    structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
        data = cast(Dict, request.get_json())
        next_report_increment = data["next_report_increment"]
        status = data["status"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    logger.debug(f"WFR {workflow_run_id} heartbeat data")

    session = SessionLocal()
    with session.begin():
        select_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
        workflow_run = session.execute(select_stmt).scalars().one()

        try:
            workflow_run.heartbeat(next_report_increment, status)
            logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
        except InvalidStateTransition as e:
            logger.debug(f"wfr {workflow_run_id} heartbeat rolled back, reason: {e}")

    resp = jsonify(status=str(workflow_run.status))
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_run/<workflow_run_id>/update_status", methods=["PUT"])
def log_workflow_run_status_update(workflow_run_id: int) -> Any:
    """Update the status of the workflow run."""
    structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
        data = cast(Dict, request.get_json())
        status = data["status"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    logger.info(f"Log status update for workflow_run_id:{workflow_run_id}.")

    session = SessionLocal()
    with session.begin():
        select_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
        workflow_run = session.execute(select_stmt).scalars().one()

        try:
            workflow_run.transition(status)
        except InvalidStateTransition as e:
            logger.warning(e)

        # Return the status
        status = workflow_run.status

    resp = jsonify(status=status)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow_run/<workflow_run_id>/set_status_for_triaging", methods=["POST"]
)
def set_status_for_triaging(workflow_run_id: int) -> Any:
    """Two triaging related status sets.

    Query all task instances that are submitted to distributor or running which haven't
    reported as alive in the allocated time, and set them for Triaging(from Running)
    and Kill_self(from Launched).

    Also look for batches associated with the active workflow run ID that do not have
    lively distributor instances associated.
    Just reassign the batch ids or move task instances to triaging?
    """
    structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
    try:
        workflow_run_id = int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    logger.info(f"Set to triaging those overdue tis for wfr {workflow_run_id}")

    session = SessionLocal()
    with session.begin():
        update_stmt = (
            update(TaskInstance)
            .where(
                TaskInstance.workflow_run_id == workflow_run_id,
                TaskInstance.status.in_(
                    [
                        constants.TaskInstanceStatus.LAUNCHED,
                        constants.TaskInstanceStatus.RUNNING,
                    ]
                ),
                TaskInstance.report_by_date <= func.now(),
            )
            .values(
                status=case(
                    (
                        TaskInstance.status == constants.TaskInstanceStatus.RUNNING,
                        constants.TaskInstanceStatus.TRIAGING,
                    ),
                    else_=constants.TaskInstanceStatus.KILL_SELF,
                )
            )
            .execution_options(synchronize_session=False)
        )
        session.execute(update_stmt)
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_run/reassign_active_batches", methods=["POST"])
def reassign_active_batches():

    data = cast(Dict, request.get_json())
    batch_ids = data.get("batch_ids")

    batch_select_query = select(Batch.id, Batch.cluster_id).where(
        Batch.id.in_(batch_ids),
        Batch.distributor_instance_id == DistributorInstance.id,
        DistributorInstance.expunged == True,
    )
    session = SessionLocal()

    with session.begin():
        inactive_batches = session.execute(batch_select_query).all()

    if any(inactive_batches):

        # Get a new distributor instance to assign
        # Assumption: all batches are run on a single cluster, therefore can reassign all
        # batches to a single new distributor instance.

        # Since this route is called at the workflow run level (for now), should be a safe
        # assumption
        new_distributor_instance_id = _get_active_distributor_instance_id(
            inactive_batches[0].cluster_id
        )

        update_stmt = (
            update(Batch)
            .where(Batch.id.in_([batch.id for batch in inactive_batches]))
            .values(distributor_instance_id=new_distributor_instance_id)
        )

        with session.begin():
            session.execute(update_stmt)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp
