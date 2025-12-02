"""Scheduler: Task batching and queueing for workflow runs.

This service manages the scheduling of tasks, including batching tasks
with compatible resources and queueing them to the server.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator

import structlog

from jobmon.client.swarm.state import StateUpdate, SwarmState

if TYPE_CHECKING:
    from jobmon.client.swarm.task import SwarmTask
    from jobmon.client.swarm.gateway import ServerGateway

logger = structlog.get_logger(__name__)


@dataclass
class BatchResult:
    """Result of queueing a batch of tasks."""

    task_statuses: dict[int, str] = field(default_factory=dict)
    batch_size: int = 0
    array_id: int = 0


class Scheduler:
    """Generates and executes task queue commands.

    The Scheduler is responsible for:
    - Batching tasks with compatible resources
    - Respecting workflow and array concurrency limits
    - Queueing batches to the server
    - Returning state updates from queue responses

    Usage:
        scheduler = Scheduler(gateway=gateway, state=state)

        # Single scheduling tick
        update = await scheduler.tick(timeout=30.0)
        state.apply_update(update)
    """

    # Maximum tasks per batch to avoid overwhelming the server
    MAX_BATCH_SIZE: int = 500

    def __init__(
        self,
        gateway: "ServerGateway",
        state: SwarmState,
    ):
        """Initialize the scheduler.

        Args:
            gateway: ServerGateway for server communication.
            state: SwarmState containing all task/array state (single source of truth).
        """
        self._gateway = gateway
        self._state = state

    @property
    def max_concurrently_running(self) -> int:
        """Workflow-level concurrency limit (delegated to state)."""
        return self._state.max_concurrently_running

    @max_concurrently_running.setter
    def max_concurrently_running(self, value: int) -> None:
        """Update workflow-level concurrency limit (delegated to state)."""
        self._state.max_concurrently_running = value

    async def tick(self, timeout: float = -1) -> StateUpdate:
        """One scheduling iteration: batch tasks, queue them, return state changes.

        Args:
            timeout: Maximum time to spend scheduling (-1 for unlimited).

        Returns:
            StateUpdate containing task status changes from queued batches.
        """
        combined = StateUpdate.empty()
        start_time = time.perf_counter()
        batches_queued = 0

        for batch in self._generate_batches():
            result = await self._queue_batch(batch)
            combined = combined.merge(
                StateUpdate(task_statuses=result.task_statuses)
            )
            batches_queued += 1

            # Check timeout
            if timeout > 0 and (time.perf_counter() - start_time) >= timeout:
                logger.debug(
                    "Scheduler tick timeout reached",
                    elapsed=time.perf_counter() - start_time,
                    timeout=timeout,
                    batches_queued=batches_queued,
                )
                break

        logger.debug(
            "Scheduler tick complete",
            batches_queued=batches_queued,
            elapsed=time.perf_counter() - start_time,
            ready_to_run_remaining=self._state.get_ready_to_run_count(),
        )

        return combined

    def _generate_batches(self) -> Generator[list["SwarmTask"], None, None]:
        """Yield batches of tasks respecting capacity limits.

        Batches are groups of tasks that:
        - Belong to the same array
        - Have the same task resources (for efficient queueing)
        - Respect workflow and array concurrency limits
        """
        # Use SwarmState to compute capacities
        workflow_capacity = self._state.get_available_capacity()
        array_capacities: dict[int, int] = {
            aid: self._state.get_array_capacity(aid)
            for aid in self._state.arrays
        }

        unscheduled: list["SwarmTask"] = []

        try:
            while self._state.ready_to_run and workflow_capacity > 0:
                next_task = self._state.dequeue_task()
                if next_task is None:
                    break

                array_id = next_task.array_id
                task_resources = next_task.current_task_resources

                if array_capacities.get(array_id, 0) <= 0:
                    # No room in this array
                    unscheduled.append(next_task)
                    continue

                # Start a batch with this task
                current_batch: list["SwarmTask"] = [next_task]
                workflow_capacity -= 1
                array_capacities[array_id] -= 1

                # Try to add compatible tasks to batch
                for _ in range(self._state.get_ready_to_run_count()):
                    candidate = self._state.dequeue_task()
                    if candidate is None:
                        break

                    # Check if this candidate can be added to the current batch
                    can_add = (
                        workflow_capacity > 0
                        and array_capacities.get(candidate.array_id, 0) > 0
                        and candidate.array_id == array_id
                        and candidate.current_task_resources == task_resources
                        and len(current_batch) < self.MAX_BATCH_SIZE
                    )

                    if can_add:
                        current_batch.append(candidate)
                        workflow_capacity -= 1
                        array_capacities[candidate.array_id] -= 1
                    else:
                        # Put it back in the queue
                        self._state.enqueue_task(candidate)

                array = self._state.arrays.get(array_id)
                array_name = array.array_name if array else None
                logger.debug(
                    f"Created batch of {len(current_batch)} tasks",
                    array_id=array_id,
                    array_name=array_name,
                    batch_size=len(current_batch),
                )

                yield current_batch

        finally:
            # Put unscheduled tasks back at the front of the queue
            for task in reversed(unscheduled):
                self._state.enqueue_task(task, front=True)

    async def _queue_batch(self, tasks: list["SwarmTask"]) -> BatchResult:
        """Queue a batch of tasks to the server.

        Args:
            tasks: List of tasks to queue (must have same array_id and resources).

        Returns:
            BatchResult with status updates for queued tasks.
        """
        first_task = tasks[0]
        task_resources = first_task.current_task_resources

        # Ensure resources are bound (has an ID from the server)
        if not task_resources.is_bound:
            session = await self._gateway._ensure_session()
            await task_resources.bind_async(session)

        logger.debug(
            f"Queueing {len(tasks)} tasks to server",
            array_id=first_task.array_id,
            batch_size=len(tasks),
            task_resources_id=task_resources.id,
        )

        response = await self._gateway.queue_task_batch(
            array_id=first_task.array_id,
            task_ids=[task.task_id for task in tasks],
            task_resources_id=task_resources.id,
            cluster_id=first_task.cluster.id,
        )

        # Build task status mapping
        task_statuses: dict[int, str] = {}
        for status, task_ids in response.tasks_by_status.items():
            for tid in task_ids:
                task_statuses[tid] = status

        return BatchResult(
            task_statuses=task_statuses,
            batch_size=len(tasks),
            array_id=first_task.array_id,
        )

    def has_work(self) -> bool:
        """Check if there are tasks ready to run with available capacity."""
        return self._state.get_ready_to_run_count() > 0 and self._state.get_available_capacity() > 0

