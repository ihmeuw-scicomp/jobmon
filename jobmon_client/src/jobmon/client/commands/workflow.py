"""Workflow-related commands.

Commands for querying and managing workflow state, including:
- Status queries
- Task listings
- Reset and resume operations
- Concurrency limits
- Log file paths
"""

import getpass
from io import StringIO
from typing import Dict, List, Optional, Union

import pandas as pd
import structlog

from jobmon.client.logging import configure_client_logging
from jobmon.client.swarm import resume_workflow_run
from jobmon.client.workflow import DistributorContext
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core.exceptions import WorkflowRunStateError
from jobmon.core.requester import Requester

logger = structlog.get_logger(__name__)


def workflow_status(
    workflow_id: Optional[List[int]] = None,
    user: Optional[List[str]] = None,
    json: bool = False,
    limit: Optional[int] = 5,
    requester: Optional[Requester] = None,
) -> pd.DataFrame:
    """Get metadata about workflow progress.

    Args:
        workflow_id: workflow_id/s to retrieve info for. If not specified will pull all
            workflows by user
        user: user/s to retrieve info for. If not specified will return for current user.
        limit: return # of records order by wf id desc. Return 5 if not provided;
            return all if [], [<0].
        json: Flag to return data as JSON
        requester: object to communicate with the flask services

    Returns:
        dataframe of all workflows and their status
    """
    if workflow_id is None:
        workflow_id = []
    if user is None:
        user = []
    logger.debug("workflow_status workflow_id:{}".format(str(workflow_id)))
    msg: dict = {}
    if workflow_id:
        msg["workflow_id"] = workflow_id
    if user:
        msg["user"] = user
    else:
        msg["user"] = getpass.getuser()
    msg["limit"] = limit

    if requester is None:
        requester = Requester.from_defaults()

    rc, res = requester.send_request(
        app_route="/workflow_status", message=msg, request_type="get"
    )
    if json:
        return res["workflows"]
    else:
        df = pd.read_json(StringIO(res["workflows"]))
        # Cast CREATED_DATE back to a date-like object, serialized as an int
        df["CREATED_DATE"] = pd.to_datetime(df["CREATED_DATE"], unit="ms")
        return df


def workflow_tasks(
    workflow_id: int,
    status: Optional[List[str]] = None,
    json: bool = False,
    limit: int = 5,
    requester: Optional[Requester] = None,
) -> pd.DataFrame:
    """Get metadata about task state for a given workflow.

    Args:
        workflow_id: workflow_id/s to retrieve info for
        status: limit task state to one of [PENDING, RUNNING, DONE, FATAL] tasks
        json: Flag to return data as JSON
        limit: return # of records order by wf id desc. Return 5 if not provided
        requester: object to communicate with the flask services

    Returns:
        Dataframe of tasks for a given workflow
    """
    logger.debug("workflow id: {}".format(workflow_id))
    msg: Dict[str, Union[List[str], int]] = {}
    if status:
        msg["status"] = [i.upper() for i in status]
    msg["limit"] = limit

    if requester is None:
        requester = Requester.from_defaults()

    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/workflow_tasks",
        message=msg,
        request_type="get",
    )
    if json:
        return res["workflow_tasks"]
    else:
        return pd.read_json(StringIO(res["workflow_tasks"]))


def workflow_reset(
    workflow_id: int, partial_reset: bool = False, requester: Optional[Requester] = None
) -> str:
    """Reset a workflow to allow re-running.

    Args:
        workflow_id: the workflow id to be reset.
        partial_reset: if False, tasks in D state will be reset as well
        requester: http server interface.

    Returns:
        A string indicating the workflow_reset result.
    """
    if requester is None:
        requester = Requester.from_defaults()

    username = getpass.getuser()

    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/validate_for_workflow_reset/{username}",
        message={},
        request_type="get",
    )
    if res["workflow_run_id"]:
        # Terminate a workflow's active workflowruns. Should go to a terminated state
        rc, _ = requester.send_request(
            app_route=f"/workflow/{workflow_id}/reset",
            message={"partial_reset": partial_reset},
            request_type="put",
        )
        wr_return = f"Workflow {workflow_id} has been reset."
    else:
        wr_return = (
            f"User {username} is not the latest initiator of "
            f"workflow {workflow_id} that has resulted in error('E' status). "
            f"The workflow {workflow_id} has not been reset."
        )

    return wr_return


def concurrency_limit(
    workflow_id: int,
    max_tasks: int,
    requester: Optional[Requester] = None,
) -> str:
    """Update a workflow's max_concurrently_running field in the database.

    Used to dynamically adjust the allowed number of jobs concurrently running.

    Args:
        workflow_id: ID of the running workflow whose max_running value needs to be reset
        max_tasks: new allowed value of parallel tasks
        requester: object to communicate with the flask services

    Returns:
        String displaying success or failure of the update.
    """
    msg = {"max_tasks": max_tasks}

    if requester is None:
        requester = Requester.from_defaults()

    _, resp = requester.send_request(
        app_route=f"/workflow/{workflow_id}/update_max_concurrently_running",
        message=msg,
        request_type="put",
    )

    return resp["message"]


def get_filepaths(
    workflow_id: int,
    array_name: str = "",
    job_name: str = "",
    limit: int = 5,
    requester: Optional[Requester] = None,
) -> dict:
    """Get the stdout/stderr paths of tasks in a workflow.

    Args:
        workflow_id: ID of the workflow to query
        array_name: Optional filter by array name
        job_name: Optional filter by job name
        limit: Maximum number of results to return
        requester: object to communicate with the flask services

    Returns:
        Dictionary of file paths for task instances
    """
    if requester is None:
        requester = Requester.from_defaults()

    app_route = f"/array/{workflow_id}/get_array_tasks"
    _, resp = requester.send_request(
        app_route=app_route,
        message={"array_name": array_name, "job_name": job_name, "limit": limit},
        request_type="get",
    )

    return resp["array_tasks"]


def resume_workflow_from_id(
    workflow_id: int,
    cluster_name: str,
    reset_if_running: bool = True,
    log: bool = True,
    timeout: int = 180,
    seconds_until_timeout: int = 36000,
    increase_resource: bool = True,
    force_cleanup: bool = False,
) -> None:
    """Resume a workflow from its ID.

    Given a workflow ID, resume the workflow. Raises an error if the workflow
    is not completed successfully on resume.

    Args:
        workflow_id: The workflow ID to resume
        cluster_name: Name of the cluster to run on
        reset_if_running: Whether to reset currently running jobs (cold resume)
        log: Whether to configure client logging
        timeout: Timeout for distributor heartbeat and waiting for resume
        seconds_until_timeout: Overall execution timeout
        increase_resource: Whether to increase resources for failed tasks
        force_cleanup: Force cleanup of stuck KILL_SELF task instances
    """
    if log:
        configure_client_logging()

    factory = WorkflowRunFactory(workflow_id=workflow_id)

    # Optionally increase resources for failed tasks with latest TI in RESOURCE_ERROR
    if increase_resource:
        app_route = f"/workflow/{workflow_id}/increase_resources"
        factory.requester.send_request(
            app_route=app_route, message={}, request_type="post"
        )

    # Signal for a resume - move existing workflow runs to C or H resume depending on the input
    factory.set_workflow_resume(
        reset_running_jobs=reset_if_running,
        resume_timeout=timeout,
        force_cleanup=force_cleanup,
    )
    factory.reset_task_statuses(
        reset_if_running=reset_if_running,
        force_cleanup=force_cleanup,
    )
    # Create the client workflow run
    new_wfr = factory.create_workflow_run()
    # Transition to BOUND state before running (all metadata is already bound)
    new_wfr._update_status(WorkflowRunStatus.BOUND)

    # Run workflow using factory function
    with DistributorContext(
        workflow_run_id=new_wfr.workflow_run_id,
        cluster_name=cluster_name,
        timeout=timeout,
    ) as distributor:
        # new_wfr.status is always set after create_workflow_run() and _update_status()
        assert (
            new_wfr.status is not None
        ), "WorkflowRun status should be set after binding"
        result = resume_workflow_run(
            workflow_id=workflow_id,
            workflow_run_id=new_wfr.workflow_run_id,
            distributor_alive=distributor.alive,
            status=new_wfr.status,
            timeout=seconds_until_timeout,
        )

    # Check on the result status - raise an error if != "D"
    if result.final_status == WorkflowRunStatus.DONE:
        print(f"Workflow {workflow_id} has successfully resumed to completion.")
    else:
        raise WorkflowRunStateError(
            f"Workflow run {new_wfr.workflow_run_id}, associated with workflow {workflow_id}",
            f"failed with status {result.final_status}",
        )
