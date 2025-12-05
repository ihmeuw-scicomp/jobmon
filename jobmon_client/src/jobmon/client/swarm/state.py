"""SwarmState: Centralized state management for workflow runs.

This module provides a consolidated state container for all workflow run state,
enabling clearer state mutation boundaries and easier testing.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import structlog

from jobmon.core.constants import TaskStatus, WorkflowRunStatus

if TYPE_CHECKING:
    from jobmon.client.swarm.array import SwarmArray
    from jobmon.client.swarm.task import SwarmTask
    from jobmon.client.task_resources import TaskResources

logger = structlog.get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

# Task statuses representing "in-flight" work (used for capacity calculations).
ACTIVE_TASK_STATUSES: tuple[str, ...] = (
    TaskStatus.QUEUED,
    TaskStatus.INSTANTIATING,
    TaskStatus.LAUNCHED,
    TaskStatus.RUNNING,
)

# Workflow-run statuses indicating the server has already decided the run must stop.
SERVER_STOP_STATUSES: frozenset[str] = frozenset(
    {
        WorkflowRunStatus.ERROR,
        WorkflowRunStatus.TERMINATED,
        WorkflowRunStatus.STOPPED,
    }
)

# Workflow-run statuses indicating a resume signal was received.
TERMINATING_STATUSES: tuple[str, ...] = (
    WorkflowRunStatus.COLD_RESUME,
    WorkflowRunStatus.HOT_RESUME,
)


# ──────────────────────────────────────────────────────────────────────────────
# StateUpdate
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class StateUpdate:
    """Represents changes to apply to SwarmState.

    StateUpdate is an immutable record of state changes that can be:
    - Created from server responses
    - Merged together to combine multiple updates
    - Applied atomically to SwarmState

    This pattern provides clear boundaries for state mutations and
    makes it easy to track what changed.
    """

    # Task status changes: task_id -> new_status
    task_statuses: dict[int, str] = field(default_factory=dict)

    # Workflow-level concurrency limit change
    max_concurrently_running: Optional[int] = None

    # Per-array concurrency limit changes: array_id -> new_limit
    array_limits: dict[int, int] = field(default_factory=dict)

    # Workflow run status change
    workflow_run_status: Optional[str] = None

    # Sync timestamp from server
    sync_time: Optional[datetime] = None

    @classmethod
    def empty(cls: type["StateUpdate"]) -> "StateUpdate":
        """Create an empty update (no changes)."""
        return cls()

    @classmethod
    def from_task_status_response(
        cls: type["StateUpdate"],
        tasks_by_status: dict[str, list[int]],
        sync_time: Optional[datetime] = None,
    ) -> "StateUpdate":
        """Create a StateUpdate from a server task status response.

        Args:
            tasks_by_status: Server response format {status: [task_ids]}
            sync_time: Server sync timestamp

        Returns:
            StateUpdate with task_statuses populated.
        """
        task_statuses: dict[int, str] = {}
        for status, task_ids in tasks_by_status.items():
            for task_id in task_ids:
                task_statuses[int(task_id)] = status
        return cls(task_statuses=task_statuses, sync_time=sync_time)

    def merge(self, other: "StateUpdate") -> "StateUpdate":
        """Combine two updates, with `other` taking precedence for conflicts.

        Args:
            other: The update to merge in (takes precedence).

        Returns:
            New StateUpdate with combined changes.
        """
        return StateUpdate(
            task_statuses={**self.task_statuses, **other.task_statuses},
            max_concurrently_running=(
                other.max_concurrently_running
                if other.max_concurrently_running is not None
                else self.max_concurrently_running
            ),
            array_limits={**self.array_limits, **other.array_limits},
            workflow_run_status=(
                other.workflow_run_status
                if other.workflow_run_status is not None
                else self.workflow_run_status
            ),
            sync_time=(
                other.sync_time if other.sync_time is not None else self.sync_time
            ),
        )

    def is_empty(self) -> bool:
        """Check if this update contains any changes."""
        return (
            not self.task_statuses
            and self.max_concurrently_running is None
            and not self.array_limits
            and self.workflow_run_status is None
            and self.sync_time is None
        )

    def __bool__(self) -> bool:
        """Allow truthiness check: `if update:`."""
        return not self.is_empty()


# ──────────────────────────────────────────────────────────────────────────────
# SwarmState
# ──────────────────────────────────────────────────────────────────────────────


class SwarmState:
    """Centralized state container for workflow runs.

    SwarmState consolidates all workflow run state in one place:
    - Task registry and status tracking
    - Array registry
    - Scheduling queue (ready_to_run)
    - Workflow run metadata

    All state queries and mutations go through this class, providing
    clear boundaries and making the code easier to test and reason about.

    Usage:
        state = SwarmState(workflow_id=1, workflow_run_id=10, dag_id=5)
        state.add_task(swarm_task)
        state.apply_update(StateUpdate(task_statuses={1: "D"}))
    """

    def __init__(
        self,
        workflow_id: int,
        workflow_run_id: int,
        dag_id: int,
        max_concurrently_running: int = 10000,
        status: str = "B",  # WorkflowRunStatus.BOUND
    ) -> None:
        """Initialize the state container.

        Args:
            workflow_id: The workflow ID.
            workflow_run_id: The workflow run ID.
            dag_id: The DAG ID.
            max_concurrently_running: Initial workflow concurrency limit.
            status: Initial workflow run status.
        """
        # Identity
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.dag_id = dag_id

        # Task registry: task_id -> SwarmTask
        self.tasks: dict[int, "SwarmTask"] = {}

        # Array registry: array_id -> SwarmArray
        self.arrays: dict[int, "SwarmArray"] = {}

        # Status tracking: status -> set of SwarmTask
        # Using SwarmTask objects for compatibility with existing code
        self._task_status_map: dict[str, set["SwarmTask"]] = {
            TaskStatus.REGISTERING: set(),
            TaskStatus.QUEUED: set(),
            TaskStatus.INSTANTIATING: set(),
            TaskStatus.LAUNCHED: set(),
            TaskStatus.RUNNING: set(),
            TaskStatus.DONE: set(),
            TaskStatus.ADJUSTING_RESOURCES: set(),
            TaskStatus.ERROR_FATAL: set(),
        }

        # Scheduling queue
        self.ready_to_run: deque["SwarmTask"] = deque()

        # Workflow run state
        self.status = status
        self.max_concurrently_running = max_concurrently_running

        # Sync tracking
        self.last_sync: Optional[datetime] = None

        # Cached TaskResources by hash to avoid duplicate binds
        self.task_resources_cache: dict[int, "TaskResources"] = {}

        # Counters for progress tracking
        self.num_previously_complete: int = 0

    # ──────────────────────────────────────────────────────────────────────────
    # Task Management
    # ──────────────────────────────────────────────────────────────────────────

    def add_task(self, task: "SwarmTask") -> None:
        """Register a task with the state.

        Args:
            task: The SwarmTask to add.
        """
        self.tasks[task.task_id] = task
        self._task_status_map[task.status].add(task)

    def add_array(self, array: "SwarmArray") -> None:
        """Register an array with the state.

        Args:
            array: The SwarmArray to add.
        """
        self.arrays[array.array_id] = array

    def get_task(self, task_id: int) -> Optional["SwarmTask"]:
        """Get a task by ID.

        Args:
            task_id: The task ID to look up.

        Returns:
            The SwarmTask or None if not found.
        """
        return self.tasks.get(task_id)

    # ──────────────────────────────────────────────────────────────────────────
    # Query Methods (Pure, No Side Effects)
    # ──────────────────────────────────────────────────────────────────────────

    @property
    def active_task_statuses(self) -> tuple[str, ...]:
        """Task statuses representing in-flight work."""
        return ACTIVE_TASK_STATUSES

    def get_active_task_count(self) -> int:
        """Count of tasks currently in-flight."""
        return sum(len(self._task_status_map[s]) for s in ACTIVE_TASK_STATUSES)

    def get_ready_to_run_count(self) -> int:
        """Count of tasks ready to be scheduled."""
        return len(self.ready_to_run)

    def get_done_count(self) -> int:
        """Count of completed tasks."""
        return len(self._task_status_map[TaskStatus.DONE])

    def get_failed_count(self) -> int:
        """Count of fatally failed tasks."""
        return len(self._task_status_map[TaskStatus.ERROR_FATAL])

    def get_done_tasks(self) -> list["SwarmTask"]:
        """List of completed tasks."""
        return list(self._task_status_map[TaskStatus.DONE])

    def get_failed_tasks(self) -> list["SwarmTask"]:
        """List of fatally failed tasks."""
        return list(self._task_status_map[TaskStatus.ERROR_FATAL])

    def get_tasks_by_status(self, status: str) -> set["SwarmTask"]:
        """Get all tasks with a specific status.

        Args:
            status: The TaskStatus to filter by.

        Returns:
            Set of SwarmTask objects with that status.
        """
        return self._task_status_map.get(status, set())

    def all_tasks_final(self) -> bool:
        """True if all tasks are in terminal state (DONE or ERROR_FATAL)."""
        return len(self.tasks) == self.get_done_count() + self.get_failed_count()

    def has_pending_work(self) -> bool:
        """True if there's in-flight or ready-to-run work."""
        return self.get_active_task_count() > 0 or len(self.ready_to_run) > 0

    def get_available_capacity(self) -> int:
        """How many more tasks can be queued at workflow level."""
        return max(0, self.max_concurrently_running - self.get_active_task_count())

    def get_array_capacity(self, array_id: int) -> int:
        """How many more tasks can be queued for this array.

        Args:
            array_id: The array to check.

        Returns:
            Available capacity for that array.
        """
        if array_id not in self.arrays:
            return 0

        array = self.arrays[array_id]
        active_in_array = sum(
            1 for task in self._get_active_tasks() if task.array_id == array_id
        )
        return max(0, array.max_concurrently_running - active_in_array)

    def _get_active_tasks(self) -> set["SwarmTask"]:
        """Get all tasks currently in active statuses."""
        result: set["SwarmTask"] = set()
        for status in ACTIVE_TASK_STATUSES:
            result |= self._task_status_map[status]
        return result

    def get_percent_done(self) -> float:
        """Calculate completion percentage."""
        if not self.tasks:
            return 0.0
        return round((self.get_done_count() / len(self.tasks)) * 100, 2)

    # ──────────────────────────────────────────────────────────────────────────
    # Mutation Methods
    # ──────────────────────────────────────────────────────────────────────────

    def apply_update(self, update: StateUpdate) -> set["SwarmTask"]:
        """Apply a StateUpdate to this state.

        This method atomically applies all changes in the update and returns
        the set of tasks whose status changed.

        Args:
            update: The StateUpdate to apply.

        Returns:
            Set of SwarmTask objects whose status changed.
        """
        changed_tasks: set["SwarmTask"] = set()

        # Update task statuses
        for task_id, new_status in update.task_statuses.items():
            task = self.tasks.get(task_id)
            if task is not None and task.status != new_status:
                # Remove from old status bucket
                old_status = task.status
                self._task_status_map[old_status].discard(task)

                # Update task and add to new bucket
                task.status = new_status
                self._task_status_map[new_status].add(task)

                changed_tasks.add(task)

        # Update workflow concurrency limit
        if update.max_concurrently_running is not None:
            self.max_concurrently_running = update.max_concurrently_running

        # Update array concurrency limits
        for array_id, limit in update.array_limits.items():
            if array_id in self.arrays:
                self.arrays[array_id].max_concurrently_running = limit

        # Update workflow run status
        if update.workflow_run_status is not None:
            self.status = update.workflow_run_status

        # Update sync time
        if update.sync_time is not None:
            self.last_sync = update.sync_time

        return changed_tasks

    def update_task_status(self, task_id: int, new_status: str) -> bool:
        """Update a single task's status.

        Args:
            task_id: The task to update.
            new_status: The new status.

        Returns:
            True if the task was found and updated.
        """
        task = self.tasks.get(task_id)
        if task is None:
            return False

        if task.status == new_status:
            return False

        # Remove from old bucket
        self._task_status_map[task.status].discard(task)

        # Update and add to new bucket
        task.status = new_status
        self._task_status_map[new_status].add(task)

        return True

    def propagate_completions(
        self, completed_tasks: set["SwarmTask"]
    ) -> list["SwarmTask"]:
        """Update downstream dependency counts for completed tasks.

        Args:
            completed_tasks: Tasks that just completed.

        Returns:
            List of tasks that became ready to run (only REGISTERING tasks).
        """
        newly_ready: list["SwarmTask"] = []

        for task in completed_tasks:
            for downstream in task.downstream_swarm_tasks:
                downstream.num_upstreams_done += 1
                # Only return tasks that are in REGISTERING status and have all
                # upstreams done. Tasks in other states (e.g., already DONE from
                # a previous run, or in an intermediate state) should not be
                # enqueued.
                if (
                    downstream.status == TaskStatus.REGISTERING
                    and downstream.all_upstreams_done
                ):
                    newly_ready.append(downstream)

        return newly_ready

    def enqueue_task(self, task: "SwarmTask", front: bool = False) -> None:
        """Add a task to the ready_to_run queue.

        Args:
            task: The task to enqueue.
            front: If True, add to front of queue (higher priority).
        """
        if front:
            self.ready_to_run.appendleft(task)
        else:
            self.ready_to_run.append(task)

    def dequeue_task(self) -> Optional["SwarmTask"]:
        """Remove and return the next task from the ready_to_run queue.

        Returns:
            The next task, or None if queue is empty.
        """
        if self.ready_to_run:
            return self.ready_to_run.popleft()
        return None

    # ──────────────────────────────────────────────────────────────────────────
    # Resource Caching
    # ──────────────────────────────────────────────────────────────────────────

    def get_cached_resources(self, resource_hash: int) -> Optional["TaskResources"]:
        """Get cached TaskResources by hash.

        Args:
            resource_hash: The hash of the TaskResources.

        Returns:
            Cached TaskResources or None if not found.
        """
        return self.task_resources_cache.get(resource_hash)

    def cache_resources(
        self, resource_hash: int, task_resources: "TaskResources"
    ) -> "TaskResources":
        """Cache TaskResources and return the cached version.

        Args:
            resource_hash: The hash of the TaskResources.
            task_resources: The TaskResources to cache.

        Returns:
            The cached TaskResources (may be existing if already cached).
        """
        if resource_hash not in self.task_resources_cache:
            self.task_resources_cache[resource_hash] = task_resources
        return self.task_resources_cache[resource_hash]

    # ──────────────────────────────────────────────────────────────────────────
    # Initialization Helpers
    # ──────────────────────────────────────────────────────────────────────────

    def compute_initial_upstream_done_counts(self) -> None:
        """Compute initial num_upstreams_done for all tasks.

        Call this after all tasks and their relationships are registered.
        """
        for task in self._task_status_map[TaskStatus.DONE]:
            for downstream in task.downstream_swarm_tasks:
                downstream.num_upstreams_done += 1
