"""Routes used to move through the finite state."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from sqlalchemy import func, select, update
import structlog

from jobmon.core.exceptions import InvalidStateTransition
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.models.workflow_status import WorkflowStatus
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.reaper import blueprint

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


@blueprint.route("/workflow/<workflow_id>/fix_status_inconsistency", methods=["PUT"])
def fix_wf_inconsistency(workflow_id: int) -> Any:
    """Find wf in F with all tasks in D and fix them.

    For flexibility, pass in the step size. It is easier to redeploy the reaper than the
    service.
    """
    data = request.get_json()
    increase_step = data["increase_step"]
    logger.debug(
        f"Fix inconsistencies starting at workflow {workflow_id} by {increase_step}"
    )
    session = SessionLocal()
    with session.begin():
        sql = select(Workflow.id)
        rows = session.execute(sql).all()
        # the id to return to reaper as next start point
        total_wf = len(rows)

    # move the starting row forward by increase_step
    # It takes about 1 second per thousand; increase_step is passed in from the reaper.
    # Lf the starting row > max row, restart from workflow-id 0.
    # This way, we can get to the unfinished the wf later
    # without querying the whole db every time.

    current_max_wf_id = int(workflow_id) + int(increase_step)
    if current_max_wf_id > total_wf:
        logger.info("Fix inconsistencies starting from workflow_id zero again")
        current_max_wf_id = 0

    # Update wf in F with all task in D to D
    # count(s) will have the total number of tasks, sum(s) is those in D.
    # If the two are equal, then the workflow Tasks are all D and therefore the workflow
    # should be D.
    session = SessionLocal()
    with session.begin():
        query_filter = [
            Workflow.id > workflow_id,
            Workflow.id <= int(workflow_id) + increase_step,
            Workflow.status == "F",
            Workflow.id == Task.workflow_id,
        ]
        sql = select(Workflow.id, Task.status).where(*query_filter)
        rows = session.execute(sql).all()
        result_set = set([r[0] for r in rows])
        for r in rows:
            if r[1] != "D" and r[0] in result_set:
                result_set -= {r[0]}
        result_list = list(result_set)

    if result_list is None or len(result_list) == 0:
        logger.debug("No inconsistent F-D workflows to fix.")
    else:
        logger.info("Fixing inconsistent F-D workflow: {ids}")
        session = SessionLocal()
        with session.begin():
            update_stmt = (
                update(Workflow)
                .where(Workflow.id.in_(result_list))
                .values(status="D", status_date=func.now())
            )
            session.execute(update_stmt)
            session.commit()

        logger.debug("Done fixing F-D inconsistent workflows.")

    resp = jsonify({"wfid": current_max_wf_id})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/workflow_name_and_args", methods=["GET"])
def get_wf_name_and_args(workflow_id: int) -> Any:
    """Return workflow name and args associated with specified workflow ID."""
    session = SessionLocal()
    with session.begin():
        query_filter = [Workflow.id == workflow_id]
        sql = select(Workflow.name, Workflow.workflow_args).where(*query_filter)
        result = session.execute(sql).all()

    if result is None or len(result) == 0:
        # return empty values in case of DB inconsistency
        resp = jsonify(workflow_name=None, workflow_args=None)
        resp.status_code = StatusCodes.OK
        return resp

    resp = jsonify(workflow_name=result[0][0], workflow_args=result[0][1])
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/lost_workflow_run", methods=["GET"])
def get_lost_workflow_runs() -> Any:
    """Return all workflow runs that are currently in the specified state."""
    statuses = request.args.getlist("status")
    version = request.args.get("version")
    session = SessionLocal()
    with session.begin():
        query_filter = [
            WorkflowRun.status.in_(statuses),
            WorkflowRun.heartbeat_date <= func.now(),
            WorkflowRun.jobmon_server_version == version,
        ]
        sql = select(WorkflowRun.id, WorkflowRun.workflow_id).where(*query_filter)
        rows = session.execute(sql).all()
    workflow_runs = [(r[0], r[1]) for r in rows]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_run/<workflow_run_id>/reap", methods=["PUT"])
def reap_workflow_run(workflow_run_id: int) -> Any:
    """If the last task was more than 2 minutes ago, transition wfr to A state.

    Also check WorkflowRun status_date to avoid possible race condition where reaper
    checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
    while waiting for a resume (when workflow is in suspended state).
    """
    structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
    logger.info(f"Reap wfr: {workflow_run_id}")

    session = SessionLocal()
    with session.begin():
        # get the wfr
        query_filter = [
            WorkflowRun.id == workflow_run_id,
            WorkflowRun.heartbeat_date <= func.now(),
        ]
        sql = select(WorkflowRun.id, WorkflowRun.workflow_id, WorkflowRun.status).where(
            *query_filter
        )
        rows = session.execute(sql).all()
    if len(rows) == 0:
        resp = jsonify(status="")
        resp.status_code = StatusCodes.OK
        return resp

    # reap wfr
    wfr_id, wf_id, wfr_status = rows[0][0], rows[0][1], rows[0][2]
    if wfr_status == WorkflowRunStatus.LINKING:
        logger.debug(f"Transitioning wfr {wfr_id} to ABORTED")
        target_wfr_status = WorkflowRunStatus.ABORTED
        target_wf_status = WorkflowStatus.ABORTED
    if wfr_status in [WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.HOT_RESUME]:
        logger.debug(f"Transitioning wfr {wfr_id} to TERMINATED")
        target_wfr_status = WorkflowRunStatus.TERMINATED
        target_wf_status = WorkflowStatus.HALTED
    if wfr_status == WorkflowRunStatus.RUNNING:
        logger.debug(f"Transitioning wfr {wfr_id} to ERROR")
        target_wfr_status = WorkflowRunStatus.ERROR
        target_wf_status = WorkflowStatus.FAILED

    # validate transition
    if (wfr_status, target_wfr_status) not in WorkflowRun().valid_transitions:
        try:
            raise InvalidStateTransition(
                model="WorkflowRun",
                id=wfr_id,
                old_state=wfr_status,
                new_state=target_wfr_status,
            )
        except (InvalidStateTransition, AttributeError) as e:
            # this branch handles race condition or case where no wfr was returned
            logger.debug(f"Unable to reap workflow_run {wfr_id}: {e}")

    # update status
    session = SessionLocal()
    with session.begin():
        query1 = f"""UPDATE workflow_run
                    SET status="{target_wfr_status}"
                    WHERE id={wfr_id}
                """
        session.execute(query1)
        query2 = f"""UPDATE workflow
                     SET status="{target_wf_status}"
                     WHERE id={wf_id}
                """
        session.execute(query2)
        session.commit()

    resp = jsonify(status=target_wfr_status)
    resp.status_code = StatusCodes.OK
    return resp
