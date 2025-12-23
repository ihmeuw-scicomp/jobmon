"""Task-related commands.

Commands for querying and managing task state, including:
- Task status queries
- Task status updates
- Task dependencies
"""

import getpass
from io import StringIO
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import structlog

from jobmon.core.constants import TaskStatus
from jobmon.core.requester import Requester

logger = structlog.get_logger(__name__)


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
        return pd.read_json(StringIO(res["task_instance_status"]), dtype=False)


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

    Returns:
        Server response with update results
    """
    from jobmon.client.commands.validation import validate_username

    if requester is None:
        requester = Requester.from_defaults()

    # Validate the username is appropriate
    user = getpass.getuser()

    validate_username(workflow_id, user, requester)

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
            "workflow_id": workflow_id,
        },
        request_type="put",
    )

    return resp


def get_sub_task_tree(
    task_ids: list,
    task_status: Optional[list] = None,
    requester: Optional[Requester] = None,
) -> dict:
    """Get the sub_tree from tasks to ensure that they end up in the right states.

    Args:
        task_ids: List of task IDs to get the subtree for
        task_status: Optional filter by task status
        requester: object to communicate with the flask services

    Returns:
        Dictionary mapping task IDs to their subtree information
    """
    if requester is None:
        requester = Requester.from_defaults()

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
    """Get the upstream and downstream dependencies of a task.

    Args:
        task_id: The task ID to get dependencies for
        requester: object to communicate with the flask services

    Returns:
        Dictionary with 'up' and 'down' keys containing dependency information
    """
    if requester is None:
        requester = Requester.from_defaults()

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
