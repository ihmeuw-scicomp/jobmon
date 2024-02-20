"""Swarm side task object."""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set

from jobmon.client.task_resources import TaskResources
from jobmon.core.cluster import Cluster
from jobmon.core.cluster_protocol import ClusterQueue


logger = logging.getLogger(__name__)


class SwarmTask(object):
    """Swarm side task object."""

    def __init__(
        self,
        task_id: int,
        array_id: int,
        status: str,
        max_attempts: int,
        task_resources: TaskResources,
        cluster: Cluster,
        resource_scales: Optional[Dict] = None,
        fallback_queues: Optional[List[ClusterQueue]] = None,
        compute_resources_callable: Optional[Callable] = None,
    ) -> None:
        """Implementing swarm behavior of tasks.

        Args:
            task_id: id of task object from db auto increment.
            array_id: id of associated array object.
            status: status of task object.
            max_attempts: maximum number of task_instances before failure.
            task_resources: callable to be executed when Task is ready to be run and
                resources can be assigned.
            cluster: The name of the cluster that the user wants to run their tasks on.
            resource_scales: Specifies how much a user wants to scale their requested
                resources after failure.
            fallback_queues: A list of queues that users want to try if their original queue
                isn't able to handle their adjusted resources.
            compute_resources_callable: callable compute resources.
        """
        self.task_id = task_id
        self.array_id = array_id
        self.status = status

        self.downstream_swarm_tasks: Set[SwarmTask] = set()

        self.current_task_resources = task_resources
        self.compute_resources_callable = compute_resources_callable
        self.fallback_queues = fallback_queues
        self.resource_scales = resource_scales if resource_scales is not None else {}
        self.cluster = cluster

        self.max_attempts = max_attempts
        self.num_upstreams: int = 0
        self.num_upstreams_done: int = 0

    @property
    def all_upstreams_done(self) -> bool:
        """Return a bool of if upstreams are done or not."""
        if self.num_upstreams_done == self.num_upstreams:
            return True
        elif self.num_upstreams_done > self.num_upstreams:
            raise RuntimeError(
                "Error in dependency management. More upstream tasks done than exist in DAG."
            )
        else:
            return False

    @property
    def downstream_tasks(self) -> List[SwarmTask]:
        """Return list of downstream tasks."""
        return list(self.downstream_swarm_tasks)

    def __hash__(self) -> int:
        """Returns the ID of the task."""
        return self.task_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two tasks are equivalent."""
        if not isinstance(other, SwarmTask):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: SwarmTask) -> bool:
        """Check if one hash is less than the has of another Task."""
        return hash(self) < hash(other)
