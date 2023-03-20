"""Task Instance object from the distributor's perspective."""
from __future__ import annotations

import logging
from typing import List, Set, Tuple, TYPE_CHECKING

from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import http_request_ok, Requester

if TYPE_CHECKING:
    from jobmon.distributor.task_instance_batch import TaskInstanceBatch


logger = logging.getLogger(__name__)


class DistributorTaskInstance:
    """Object used for communicating with JSM from the distributor node."""

    def __init__(
        self,
        task_instance_id: int,
        workflow_run_id: int,
        status: str,
        requester: Requester,
    ) -> None:
        """Initialization of distributor task instance.

        Args:
            task_instance_id (int): a task_instance_id
            workflow_run_id (int): a workflow_run_id
            status(str): status of the distributor task instance
            requester (Requester, optional): a requester to communicate with
                the JSM. default is shared requester
        """
        self.task_instance_id = task_instance_id
        self.workflow_run_id = workflow_run_id
        self.status = status

        self.error_state = ""
        self.error_msg = ""

        self.requester = requester

    @property
    def submission_name(self) -> str:
        try:
            return self.batch.submission_name
        except AttributeError:
            return str(self.task_instance_id)

    @property
    def batch(self) -> TaskInstanceBatch:
        """Returns the batch the DistributorTaskInstance is in."""
        return self._batch

    @batch.setter
    def batch(self, val: TaskInstanceBatch) -> None:
        """Sets the batch of the DistributorTaskInstance."""
        self._batch = val

    @property
    def array_step_id(self) -> int:
        """Returns the array step of the TI."""
        return self._array_step_id

    @array_step_id.setter
    def array_step_id(self, val: int) -> None:
        self._array_step_id = val

    def transition_to_launched(
        self, distributor_id: str, next_report_increment: float
    ) -> None:
        """Register the submission of a new task instance to a cluster.

        This method is never called by the happy path - only if array submission is not
        implemented on a particular cluster type.
        """
        self.distributor_id = distributor_id
        app_route = f"/task_instance/{self.task_instance_id}/log_distributor_id"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "distributor_id": str(distributor_id),
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        self.status = TaskInstanceStatus.LAUNCHED

    def transition_to_no_distributor_id(
        self,
        no_id_err_msg: str,
    ) -> None:
        """Register that submission failed with the central service.

        Args:
            no_id_err_msg: The error msg from the executor when failed to obtain distributor
                id.
        """
        app_route = f"/task_instance/{self.task_instance_id}/log_no_distributor_id"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"no_id_err_msg": no_id_err_msg},
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

    def _transition_to_error(self, error_message: str, error_state: str) -> None:
        """Transitions the TaskInstance to the specified error state."""
        if self.distributor_id is None:
            raise ValueError("distributor_id cannot be None during log_error")
        distributor_id = self.distributor_id
        logger.debug(f"log_error for distributor_id {distributor_id}")
        if not error_state:
            raise ValueError("cannot log error if error_state isn't set")

        if error_state == TaskInstanceStatus.UNKNOWN_ERROR:
            app_route = f"/task_instance/{self.task_instance_id}/log_unknown_error"
        else:
            app_route = f"/task_instance/{self.task_instance_id}/log_known_error"

        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "error_state": error_state,
                "error_message": error_message,
                "distributor_id": distributor_id,
            },
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        self.error_state = error_state

    def transition_to_unknown_error(
        self, error_message: str, error_state: str
    ) -> Tuple[Set[DistributorTaskInstance], List]:
        """Register that an unknown error was discovered during reconciliation."""
        self._transition_to_error(error_message, error_state)
        return {self}, []

    def transition_to_resource_error(
        self, error_message: str, error_state: str
    ) -> Tuple[Set[DistributorTaskInstance], List]:
        """Register that a resource error was discovered during reconciliation."""
        self._transition_to_error(error_message, error_state)
        return {self}, []

    def transition_to_error(
        self, error_message: str, error_state: str
    ) -> Tuple[Set[DistributorTaskInstance], List]:
        """Register that a known error occurred during reconciliation."""
        self._transition_to_error(error_message, error_state)
        return {self}, []

    def __hash__(self) -> int:
        """Returns the id of the TaskInstance."""
        return self.task_instance_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two tasks are equivalent."""
        if not isinstance(other, DistributorTaskInstance):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: DistributorTaskInstance) -> bool:
        """Check if one hash is less than the has of another Task."""
        return hash(self) < hash(other)

    def __repr__(self) -> str:
        """Return a short representation string."""
        return (
            f"DistributorTaskInstance(task_instance_id={self.task_instance_id},"
            f"status={self.status})"
        )
