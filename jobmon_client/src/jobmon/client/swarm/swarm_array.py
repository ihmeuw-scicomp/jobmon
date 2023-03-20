"""Array object used by swarm to create task instance batches."""
from __future__ import annotations

import logging
from typing import Set

from jobmon.client.swarm.swarm_task import SwarmTask


logger = logging.getLogger(__name__)


class SwarmArray:
    def __init__(self, array_id: int, max_concurrently_running: int) -> None:
        """Initialization of the SwarmArray."""
        self.array_id = array_id
        self.tasks: Set[SwarmTask] = set()
        self.max_concurrently_running = max_concurrently_running

    def add_task(self, task: SwarmTask) -> None:
        if task.array_id != self.array_id:
            raise ValueError(
                f"array_id mismatch. SwarmTask={task.array_id}. Array={self.array_id}."
            )
        self.tasks.add(task)

    def __hash__(self) -> int:
        """Returns the array ID."""
        return self.array_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two arrays are equivalent."""
        if not isinstance(other, SwarmArray):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: SwarmArray) -> bool:
        """Check if one hash is less than the has of another DistributorArray."""
        return hash(self) < hash(other)
