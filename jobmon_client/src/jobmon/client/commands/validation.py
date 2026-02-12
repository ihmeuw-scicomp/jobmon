"""Validation helper functions.

Helper functions for validating user permissions and workflow state:
- Username validation
- Workflow validation
- ID chunking utilities
"""

from typing import List

from jobmon.core.constants import WorkflowStatus
from jobmon.core.requester import Requester


def validate_username(workflow_id: int, username: str, requester: Requester) -> None:
    """Validate that the user is approved to make changes to a workflow.

    Args:
        workflow_id: The workflow ID to check permissions for
        username: The username to validate
        requester: Server requester object

    Raises:
        AssertionError: If the user is not allowed to modify the workflow
    """
    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/validate_username/{username}",
        message={},
        request_type="get",
    )
    if not res["validation"]:
        raise AssertionError(f"User {username} is not allowed to reset this workflow.")
    return


def validate_workflow(task_ids: List[int], requester: Requester) -> WorkflowStatus:
    """Validate that task IDs belong to a valid workflow.

    Validates that the task_ids provided belong to the expected workflow,
    and the workflow status is in expected status unless we want to force
    it through.

    Args:
        task_ids: List of task IDs to validate
        requester: Server requester object

    Returns:
        The workflow status

    Raises:
        AssertionError: If validation fails (wrong workflow status or mixed workflows)
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


def chunk_ids(ids: List[int], chunk_size: int = 100) -> List[List[int]]:
    """Chunk a list of IDs into smaller lists.

    Useful for avoiding HTTP request size limits.

    Args:
        ids: list of IDs to chunk
        chunk_size: the size of each chunk; default to 100

    Returns:
        A list of lists, each containing at most chunk_size IDs
    """
    return_list = []
    return_list.append(ids[0 : min(chunk_size, len(ids))])
    i = 1
    while i * chunk_size < len(ids):
        return_list.append(ids[i * chunk_size : min((i + 1) * chunk_size, len(ids))])
        i += 1
    return return_list
