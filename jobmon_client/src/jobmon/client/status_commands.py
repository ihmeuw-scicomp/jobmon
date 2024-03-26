"""Commands to check for workflow and task status (from CLI)."""

import getpass
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from jobmon.client.logging import JobmonLoggerConfig
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.workflow import DistributorContext
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import (
    ExecludeTTVs,
    TaskStatus,
    WorkflowRunStatus,
    WorkflowStatus,
)
from jobmon.core.exceptions import WorkflowRunStateError
from jobmon.core.requester import Requester
from jobmon.core.serializers import SerializeTaskTemplateResourceUsage


logger = logging.getLogger(__name__)


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
        df = pd.read_json(res["workflows"])
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
        return pd.read_json(res["workflow_tasks"])


def task_template_resources(
    task_template_version: int,
    workflows: Optional[list] = None,
    node_args: Optional[Dict] = None,
    ci: Optional[float] = None,
    requester: Optional[Requester] = None,
) -> Optional[Dict]:
    """Get aggregate resource usage data for a given TaskTemplateVersion.

    Args:
        task_template_version: The task template version ID the user wants to find the
            resource usage of.
        workflows: list of workflows a user wants query by.
        node_args: dictionary of node arguments a user wants to query by.
        ci: confidence interval. Not calculate if None.
        requester: object to communicate with the flask services

    Returns:
        Dataframe of TaskTemplate resource usage
    """
    execlue_list = ExecludeTTVs.EXECLUDE_TTVS
    if task_template_version in execlue_list:
        msg = (
            f"Resource usage query for task_template_version {task_template_version}"
            f"  is restricted due to excessive size."
        )
        logger.warning(msg)
        print(msg)
        return None
    message: Dict[Any, Any] = dict()
    message["task_template_version_id"] = task_template_version
    if workflows:
        message["workflows"] = workflows
    if node_args:
        message["node_args"] = node_args
    if ci:
        message["ci"] = ci

    if requester is None:
        requester = Requester.from_defaults()

    app_route = "/task_template_resource_usage"
    return_code, response = requester.send_request(
        app_route=app_route, message=message, request_type="post"
    )

    def format_bytes(value: Any) -> Optional[str]:
        if value is not None:
            return str(value) + "B"
        else:
            return value

    kwargs = SerializeTaskTemplateResourceUsage.kwargs_from_wire(response)
    resources = {
        "num_tasks": kwargs["num_tasks"],
        "min_mem": format_bytes(kwargs["min_mem"]),
        "max_mem": format_bytes(kwargs["max_mem"]),
        "mean_mem": format_bytes(kwargs["mean_mem"]),
        "min_runtime": kwargs["min_runtime"],
        "max_runtime": kwargs["max_runtime"],
        "mean_runtime": kwargs["mean_runtime"],
        "median_mem": format_bytes(kwargs["median_mem"]),
        "median_runtime": kwargs["median_runtime"],
        "ci_mem": kwargs["ci_mem"],
        "ci_runtime": kwargs["ci_runtime"],
    }

    return resources


def task_status(
    task_ids: List[int],
    status: Optional[List[str]] = None,
    json: bool = False,
    requester: Optional[Requester] = None,
) -> Union[dict, pd.DataFrame]:
    """Get metadata about a task and its task instances.

    Args:
        task_ids: a list of task_ids to retrieve task_instance metadata for.
        status: a list of statuses to check for.
        json: Flag to return data as JSON.
        requester: object to communicate with the flask services

    Returns:
        Task status and task_instance metadata
    """
    logger.debug("task_status task_ids:{}".format(str(task_ids)))
    msg: Dict[str, Union[List[str], List[int]]] = {"task_ids": task_ids}
    if status:
        msg["status"] = [i.upper() for i in status]

    if requester is None:
        requester = Requester.from_defaults()

    rc, res = requester.send_request(
        app_route="/task_status", message=msg, request_type="get"
    )
    if json:
        return res["task_instance_status"]
    else:
        return pd.read_json(res["task_instance_status"], dtype=False)


def concurrency_limit(
    workflow_id: int,
    max_tasks: int,
    requester: Optional[Requester] = None,
) -> str:
    """Update a workflow's max_concurrently_running field in the database.

    Used to dynamically adjust the allowed number of jobs concurrently running.

    Args:
        workflow_id (int): ID of the running workflow whose max_running value needs to be reset
        max_tasks (int) : new allowed value of parallel tasks
        requester: object to communicate with the flask services

    Returns: string displaying success or failure of the update.
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


def _chunk_ids(ids: List[int], chunk_size: int = 100) -> List[List[int]]:
    """Chunk the ids into a list of 100 ids list.

    Args:
        ids: list of ids
        chunk_size: the size of each chunk; default to 100

    Returns: a list of list

    """
    return_list = []
    return_list.append(ids[0 : min(chunk_size, len(ids))])
    i = 1
    while i * chunk_size < len(ids):
        return_list.append(ids[i * chunk_size : min((i + 1) * chunk_size, len(ids))])
        i += 1
    return return_list


def update_task_status(
    task_ids: List[int],
    workflow_id: int,
    new_status: str,
    force: bool = False,
    recursive: bool = False,
    requester: Optional[Requester] = None,
) -> Any:
    """Set the specified task IDs to the new status, pending validation.

    Args:
        task_ids: List of task IDs to reset in the database
        workflow_id: The workflow to which each task belongs. Users can only self-service
            1 workflow at a time for the moment.
        new_status: the status to set tasks to
        force: if true, allow all source statuses and all workflow statuses.
        recursive: if true and force, apply recursive update_status downstream
            or upstream depending on new_status
            (upstream if new_status == 'D'; downstream if new_status == 'G').
        requester: object to communicate with the flask services
    """
    if requester is None:
        requester = Requester.from_defaults()

    # Validate the username is appropriate
    user = getpass.getuser()

    validate_username(workflow_id, user, requester)
    workflow_status = validate_workflow(task_ids, requester)

    # Validate the allowed statuses. For now, only "D" and "G" allowed.
    allowed_statuses = [TaskStatus.REGISTERING, TaskStatus.DONE]
    assert (
        new_status in allowed_statuses
    ), f"Only {allowed_statuses} allowed to be set via CLI"

    # Conditional logic: If the new status is "D", only need to set task to "D"
    # Else: All downstreams must also be set to "G", and task instances set to "K"
    if force and recursive:
        rc, res = requester.send_request(
            app_route="/tasks_recursive/" + ("up" if new_status == "D" else "down"),
            message={"task_ids": task_ids},
            request_type="put",
        )
        if rc != 200:
            raise AssertionError(f"Server return HTTP error code: {rc}")
        task_ids = res["task_ids"]
    else:
        if new_status == TaskStatus.REGISTERING:
            subdag_tasks = get_sub_task_tree(task_ids, requester=requester).keys()
            task_ids = task_ids + [*subdag_tasks]

    # We want to prevent excessive requests, with a hard-limit of 10,000 set up
    # to avoid churning on the server.
    if len(task_ids) > 10_000:
        raise AssertionError(
            f"There are too many tasks ({len(task_ids)}) requested "
            f"for the update. Request denied."
        )

    _, resp = requester.send_request(
        app_route="/task/update_statuses",
        message={
            "task_ids": task_ids,
            "new_status": new_status,
            "workflow_status": workflow_status,
            "workflow_id": workflow_id,
        },
        request_type="put",
    )

    return resp


def validate_username(workflow_id: int, username: str, requester: Requester) -> None:
    """Validate that the user is approved to make these changes."""
    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/validate_username/{username}",
        message={},
        request_type="get",
    )
    if not res["validation"]:
        raise AssertionError(f"User {username} is not allowed to reset this workflow.")
    return


def validate_workflow(task_ids: List[int], requester: Requester) -> WorkflowStatus:
    """Validate workflow.

    The task_ids provided belong to the expected workflow,
    and the workflow status is in expected status unless we want to force
    it through.
    """
    rc, res = requester.send_request(
        app_route="/workflow_validation",
        message={"task_ids": task_ids},
        request_type="post",
    )

    if not bool(res["validation"]):
        raise AssertionError(
            "The workflow status of the given task ids are out of "
            "scope of the following required statuses "
            "(FAILED, DONE, ABORTED, HALTED) or multiple workflow statuses "
            "were found."
        )
    return res["workflow_status"]


def get_sub_task_tree(
    task_ids: list,
    task_status: Optional[list] = None,
    requester: Optional[Requester] = None,
) -> dict:
    """Get the sub_tree from tasks to ensure that they end up in the right states."""
    # This is to make the test case happy. Otherwise, requester should not be None.
    if requester is None:
        requester = Requester.from_defaults()

    # Valid input
    rc, res = requester.send_request(
        app_route="/task/subdag",
        message={"task_ids": task_ids, "task_status": task_status},
        request_type="post",
    )
    if rc != 200:
        raise AssertionError(f"Server return HTTP error code: {rc}")
    task_tree_dict = res["sub_task"]
    return task_tree_dict


def get_task_dependencies(task_id: int, requester: Optional[Requester] = None) -> dict:
    """Get the upstream and down stream of a task."""
    # This is to make the test case happy. Otherwise, requester should not be None.
    if requester is None:
        requester = Requester.from_defaults()
    # Valid input
    rc, res = requester.send_request(
        app_route=f"/task_dependencies/{task_id}", message={}, request_type="get"
    )
    if rc != 200:
        if rc == 404:
            raise AssertionError(
                f"Server return HTTP error code: {rc}. "
                f"The jobmon server version may not support this command."
            )
        else:
            raise AssertionError(f"Server return HTTP error code: {rc}")
    return res


def workflow_reset(
    workflow_id: int, partial_reset: bool = False, requester: Optional[Requester] = None
) -> str:
    """Workflow reset.

    Return:
        A string to indicate the workflow_reset result.

    Args:
        workflow_id: the workflow id to be reset.
        partial_reset: if False, tasks in D state will be reset as well
        requester: http server interface.
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


def _get_yaml_data(
    wfid: int, tid: int, v_mem: str, v_core: str, v_runtime: str, requester: Requester
) -> Dict:
    # make it a method for easy mock
    tt_exclude_list = ExecludeTTVs.EXECLUDE_TTVS

    key_map_m = {"avg": "mean_mem", "min": "min_mem", "max": "max_mem"}
    key_map_r = {"avg": "mean_runtime", "min": "min_runtime", "max": "max_runtime"}

    # Get task template version ids
    rc, res = requester.send_request(
        app_route="/get_task_template_version",
        message={"task_id": tid} if wfid is None else {"workflow_id": wfid},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for get_task_template_version."
        )

    # data structure: {ttv_id: [name, core, mem, runtime, queue]}
    ttvis_dic = dict()

    # create a holder for tts that are in the exclude list
    tt_in_exclude = []
    for t in res["task_template_version_ids"]:
        if t["id"] in tt_exclude_list:
            tt_in_exclude.append(t)
            msg = f"Task template {t} contains too many tasks and will be excluded."
            logger.warning(msg)
            print(msg)
        else:
            ttvis_dic[t["id"]] = [t["name"]]

    for t in tt_in_exclude:
        # fill in template using default values
        print(f"Fill template for t using {t}")
        ttvis_dic[t["id"]] = [t["name"], 1, 1, 3600, "all.q"]

    # set of ttvs not in exclude list
    actual_ttvs = set(ttvis_dic.keys()) - set([i["id"] for i in tt_in_exclude])

    if len(actual_ttvs) == 0:
        return ttvis_dic

    # get core
    ttvis = str([i for i in actual_ttvs]).replace("[", "(").replace("]", ")")
    rc, res = requester.send_request(
        app_route="/get_requested_cores",
        message={"task_template_version_ids": f"{ttvis}"},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for /get_requested_cores."
        )
    core_info = res["core_info"]
    for record in core_info:
        ttvis_dic[int(record["id"])].append(record[v_core])

    # Get actually mem and runtime for each ttvi
    for ttv in actual_ttvs:
        rc, res = requester.send_request(
            app_route="/task_template_resource_usage",
            message={"task_template_version_id": ttv},
            request_type="post",
        )
        if rc != 200:
            raise AssertionError(
                f"Server returns HTTP error code: {rc} "
                f"for /task_template_resource_usage."
            )
        usage = SerializeTaskTemplateResourceUsage.kwargs_from_wire(res)
        ttvis_dic[int(ttv)].append(int(usage[key_map_m[v_mem]]))
        ttvis_dic[int(ttv)].append(int(usage[key_map_r[v_runtime]]))

    # get queue
    rc, res = requester.send_request(
        app_route="/get_most_popular_queue",
        message={"task_template_version_ids": f"{ttvis}"},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for /get_most_popular_queue."
        )
    for record in res["queue_info"]:
        ttvis_dic[int(record["id"])].append(record["queue"])
    return ttvis_dic


def _create_yaml(data: Optional[Dict] = None, clusters: Optional[List] = None) -> str:
    yaml = "task_template_resources:\n"
    if clusters is None:
        clusters = []
    if data is None or len(clusters) == 0:
        return yaml
    for ttv in data.keys():
        yaml += f"  {data[ttv][0]}:\n"  # name
        for cluster in clusters:
            yaml += f"    {cluster}:\n"  # cluster
            yaml += f"      cores: {data[ttv][1]}\n"  # core
            yaml += f'      memory: "{data[ttv][2]}B"\n'  # mem
            yaml += f"      runtime: {data[ttv][3]}\n"  # runtime
            yaml += f'      queue: "{data[ttv][4]}"\n'  # queue
    return yaml


def create_resource_yaml(
    wfid: int,
    tid: int,
    v_mem: str,
    v_core: str,
    v_runtime: str,
    clusters: List,
    requester: Optional[Requester] = None,
) -> str:
    """The method to create resource yaml."""
    if requester is None:
        requester = Requester.from_defaults()

    ttvis_dic = _get_yaml_data(wfid, tid, v_mem, v_core, v_runtime, requester)
    yaml = _create_yaml(ttvis_dic, clusters)
    return yaml


def get_filepaths(
    workflow_id: int,
    array_name: str = "",
    job_name: str = "",
    limit: int = 5,
    requester: Optional[Requester] = None,
) -> dict:
    """Allows users to get the stdout/stderr paths of their tasks."""
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
) -> None:
    """Given a workflow ID, resume the workflow.

    Raise an error if the workflow is not completed successfully on resume.
    """
    if log:
        JobmonLoggerConfig.attach_default_handler(
            logger_name="jobmon", log_level=logging.INFO
        )

    factory = WorkflowRunFactory(workflow_id=workflow_id)

    # Signal for a resume - move existing workflow runs to C or H resume depending on the input
    factory.set_workflow_resume(reset_running_jobs=reset_if_running)
    factory.reset_task_statuses(reset_if_running=reset_if_running)
    # Create the client workflow run
    new_wfr = factory.create_workflow_run()

    # Create swarm
    swarm = SwarmWorkflowRun(
        workflow_run_id=new_wfr.workflow_run_id, status=new_wfr.status
    )
    swarm.from_workflow_id(workflow_id)

    with DistributorContext(
        workflow_run_id=new_wfr.workflow_run_id,
        cluster_name=cluster_name,
        timeout=timeout,
    ) as distributor:
        swarm.run(distributor_alive_callable=distributor.alive)

    # Check on the swarm status - raise an error if != "D"
    if swarm.status == WorkflowRunStatus.DONE:
        print(f"Workflow {workflow_id} has successfully resumed to completion.")
    else:
        raise WorkflowRunStateError(
            f"Workflow run {swarm.workflow_run_id}, associated with workflow {workflow_id}",
            f"failed with status {swarm.status}",
        )
