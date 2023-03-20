"""Reaper Behavior for a given Workflow Run."""
from __future__ import annotations

import logging
from typing import Any

from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import http_request_ok, Requester
from jobmon.core.serializers import SerializeWorkflowRun


logger = logging.getLogger(__file__)


class ReaperWorkflowRun(object):
    """Reaper Behavior for a given Workflow Run."""

    def __init__(
        self, workflow_run_id: int, workflow_id: int, requester: Requester
    ) -> None:
        """Implementing workflow reaper behavior of workflow run.

        Args:
            workflow_run_id (int): id of workflow run object from DB.
            workflow_id (int): id of associated workflow.
            requester (Requester): requester to communicate with Flask.
        """
        self.workflow_run_id = workflow_run_id
        self.workflow_id = workflow_id
        self._requester = requester

    @classmethod
    def from_wire(
        cls: Any, wire_tuple: tuple, requester: Requester
    ) -> ReaperWorkflowRun:
        """Create Reaper Workflow Run object."""
        kwargs = SerializeWorkflowRun.kwargs_from_wire(wire_tuple)
        return cls(
            workflow_run_id=kwargs["id"],
            workflow_id=kwargs["workflow_id"],
            requester=requester,
        )

    def reap(self) -> str:
        """Transition workflow run to error."""
        app_route = f"/workflow_run/{self.workflow_run_id}/reap"
        return_code, response = self._requester.send_request(
            app_route=app_route, message={}, request_type="put"
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from PUT "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        return response["status"]

    def __repr__(self) -> str:
        """Return formatted reaper workflow run data."""
        return (
            f"ReaperWorkflowRun(workflow_run_id={self.workflow_run_id}, "
            f"workflow_id={self.workflow_id}"
        )
