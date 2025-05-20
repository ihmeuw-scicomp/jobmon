from __future__ import annotations

import ast
import hashlib
import logging
from typing import Any, Dict, Set, TYPE_CHECKING

from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.requester import Requester

if TYPE_CHECKING:
    from jobmon.distributor.distributor_task_instance import (
        DistributorTaskInstance,
    )


logger = logging.getLogger(__name__)


class TaskInstanceBatch:
    def __init__(
        self,
        array_id: int,
        array_name: str,
        array_batch_num: int,
        task_resources_id: int,
        requester: Requester,
    ) -> None:
        """Initialization of the TaskInstanceBatch object."""
        self.array_id = array_id
        self.array_name = array_name
        self.batch_number = array_batch_num
        self.task_resources_id = task_resources_id
        self.task_instances: Set[DistributorTaskInstance] = set()
        self.requester = requester

    @property
    def submission_name(self) -> str:
        return f"{self.array_name}-{self.batch_number}"

    @property
    def requested_resources(self) -> Dict:
        if not hasattr(self, "_requested_resources"):
            raise AttributeError(
                "Requested Resources cannot be accessed before the array batch is prepared for"
                " launch."
            )
        return self._requested_resources

    def add_task_instance(self, task_instsance: DistributorTaskInstance) -> None:
        self.task_instances.add(task_instsance)
        task_instsance.batch = self

    def load_requested_resources(self) -> None:
        app_route = f"/task_resources/{self.task_resources_id}"
        _, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="post"
        )
        self._requested_resources: Dict[str, Any] = ast.literal_eval(
            str(response["requested_resources"])
        )
        self._requested_resources["queue"] = response["queue_name"]

    def prepare_task_instance_batch_for_launch(self) -> None:
        """Add the current batch number to the current set of registered task instance ids."""
        array_step_id = 0
        for task_instance in sorted(self.task_instances):
            task_instance.array_step_id = array_step_id
            array_step_id += 1

        self.load_requested_resources()

    def set_distributor_ids(self, distributor_id_map: Dict) -> None:
        """Set the distributor_ids on the task instances in the array.

        Args:
            distributor_id_map: map of array_step_id to distributor_id
        """
        for ti in self.task_instances:
            ti.distributor_id = distributor_id_map[ti.array_step_id]

    def transition_to_launched(self, next_report_by: float) -> None:
        """Transition all associated task instances to LAUNCHED state."""
        # Assertion that all bound task instances are indeed instantiated
        for ti in self.task_instances:
            if ti.status != TaskInstanceStatus.INSTANTIATED:
                raise ValueError(
                    f"{ti} is not in INSTANTIATED state, prior to launching."
                )

        app_route = f"/array/{self.array_id}/transition_to_launched"
        data = {
            "batch_number": self.batch_number,
            "next_report_increment": next_report_by,
        }

        self.requester.send_request(
            app_route=app_route, message=data, request_type="post"
        )

        for ti in self.task_instances:
            ti.status = TaskInstanceStatus.LAUNCHED

    def log_distributor_ids(self) -> None:
        """Log the distributor ID in the database for all task instances in the batch."""
        app_route = f"/array/{self.array_id}/log_distributor_id"
        data = {ti.task_instance_id: ti.distributor_id for ti in self.task_instances}
        self.requester.send_request(
            app_route=app_route, message=data, request_type="post"
        )

    def transition_to_killed(self) -> None:
        """Mark all TIs in this batch as killed in the DB (ERROR_FATAL)."""
        # 1) Make an HTTP call to the server if you have a relevant endpoint
        #    that transitions them to a 'killed' or ERROR_FATAL state in bulk.
        app_route = f"/array/{self.array_id}/transition_to_killed"
        data = {"batch_number": self.batch_number}
        self.requester.send_request(
            app_route=app_route, message=data, request_type="post"
        )

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.array_id)).encode("utf-8"))
        hash_value.update(str(self.batch_number).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two tasks are equivalent."""
        if not isinstance(other, TaskInstanceBatch):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: TaskInstanceBatch) -> bool:
        """Check if one hash is less than the has of another Task."""
        return hash(self) < hash(other)
