"""Start up distributing process."""
from typing import Optional

from jobmon.core.cluster import Cluster
from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import http_request_ok, Requester
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


class WorkerNodeFactory:
    def __init__(
        self, cluster_name: str, requester: Optional[Requester] = None
    ) -> None:
        """Initialization of the WorkerNode Factory."""
        self._cluster_name = cluster_name

        cluster = Cluster.get_cluster(cluster_name)
        self._worker_node_interface = cluster.get_worker_node()

    def get_job_task_instance(
        self,
        task_instance_id: int,
    ) -> WorkerNodeTaskInstance:
        """Set up and return WorkerNodeTaskInstance object."""
        worker_node_task_instance = WorkerNodeTaskInstance(
            cluster_interface=self._worker_node_interface,
            task_instance_id=task_instance_id,
        )
        return worker_node_task_instance

    def get_array_task_instance(
        self, array_id: int, batch_number: int
    ) -> WorkerNodeTaskInstance:
        """Set up and return WorkerNodeTaskInstance object."""
        requester = Requester.from_defaults()

        # Always assumed to be a value in the range [1, len(array)]
        array_step_id = self._worker_node_interface.array_step_id

        # Fetch from the database
        app_route = (
            f"/get_array_task_instance_id/{array_id}/{batch_number}/{array_step_id}"
        )
        rc, resp = requester.send_request(
            app_route=app_route, message={}, request_type="get"
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {rc}"
            )
        task_instance_id = resp["task_instance_id"]

        worker_node_task_instance = WorkerNodeTaskInstance(
            cluster_interface=self._worker_node_interface,
            task_instance_id=task_instance_id,
        )
        return worker_node_task_instance
