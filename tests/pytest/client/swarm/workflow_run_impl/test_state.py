"""Unit tests for SwarmState and StateUpdate.

These tests verify the state management logic without requiring
a running server or full workflow setup.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock

import pytest

from jobmon.client.swarm.workflow_run_impl.state import (
    ACTIVE_TASK_STATUSES,
    StateUpdate,
    SwarmState,
)
from jobmon.core.constants import TaskStatus


# ──────────────────────────────────────────────────────────────────────────────
# Mock SwarmTask for testing
# ──────────────────────────────────────────────────────────────────────────────


class MockSwarmTask:
    """Minimal SwarmTask mock for testing state operations."""

    def __init__(
        self,
        task_id: int,
        array_id: int = 1,
        status: str = TaskStatus.REGISTERING,
    ) -> None:
        self.task_id = task_id
        self.array_id = array_id
        self.status = status
        self.downstream_swarm_tasks: set[MockSwarmTask] = set()
        self.num_upstreams: int = 0
        self.num_upstreams_done: int = 0

    @property
    def all_upstreams_done(self) -> bool:
        return self.num_upstreams_done >= self.num_upstreams

    def __hash__(self) -> int:
        return self.task_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MockSwarmTask):
            return False
        return self.task_id == other.task_id


class MockSwarmArray:
    """Minimal SwarmArray mock for testing."""

    def __init__(
        self,
        array_id: int,
        max_concurrently_running: int = 100,
    ) -> None:
        self.array_id = array_id
        self.max_concurrently_running = max_concurrently_running
        self.tasks: set[MockSwarmTask] = set()

    def __hash__(self) -> int:
        return self.array_id


# ──────────────────────────────────────────────────────────────────────────────
# StateUpdate Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestStateUpdate:
    """Tests for StateUpdate dataclass."""

    def test_empty_update(self) -> None:
        """Test creating an empty update."""
        update = StateUpdate.empty()
        assert update.is_empty()
        assert not update  # Falsy when empty

    def test_update_with_task_statuses(self) -> None:
        """Test update with task status changes."""
        update = StateUpdate(task_statuses={1: "D", 2: "R"})
        assert not update.is_empty()
        assert update  # Truthy when non-empty
        assert update.task_statuses[1] == "D"
        assert update.task_statuses[2] == "R"

    def test_update_with_all_fields(self) -> None:
        """Test update with all fields populated."""
        now = datetime.now()
        update = StateUpdate(
            task_statuses={1: "D"},
            max_concurrently_running=500,
            array_limits={10: 50},
            workflow_run_status="R",
            sync_time=now,
        )
        assert update.task_statuses == {1: "D"}
        assert update.max_concurrently_running == 500
        assert update.array_limits == {10: 50}
        assert update.workflow_run_status == "R"
        assert update.sync_time == now

    def test_from_task_status_response(self) -> None:
        """Test creating update from server response format."""
        now = datetime.now()
        response = {"D": [1, 2, 3], "R": [4, 5]}
        update = StateUpdate.from_task_status_response(response, sync_time=now)

        assert update.task_statuses[1] == "D"
        assert update.task_statuses[2] == "D"
        assert update.task_statuses[3] == "D"
        assert update.task_statuses[4] == "R"
        assert update.task_statuses[5] == "R"
        assert update.sync_time == now

    def test_merge_task_statuses(self) -> None:
        """Test merging task status updates."""
        update1 = StateUpdate(task_statuses={1: "R", 2: "Q"})
        update2 = StateUpdate(task_statuses={2: "R", 3: "D"})

        merged = update1.merge(update2)

        assert merged.task_statuses[1] == "R"  # From update1
        assert merged.task_statuses[2] == "R"  # Overwritten by update2
        assert merged.task_statuses[3] == "D"  # From update2

    def test_merge_concurrency_limits(self) -> None:
        """Test merging concurrency limit updates."""
        update1 = StateUpdate(max_concurrently_running=100)
        update2 = StateUpdate(max_concurrently_running=200)

        # update2 takes precedence
        merged = update1.merge(update2)
        assert merged.max_concurrently_running == 200

        # None doesn't override existing value
        update3 = StateUpdate()
        merged = update2.merge(update3)
        assert merged.max_concurrently_running == 200

    def test_merge_array_limits(self) -> None:
        """Test merging array limit updates."""
        update1 = StateUpdate(array_limits={1: 10, 2: 20})
        update2 = StateUpdate(array_limits={2: 25, 3: 30})

        merged = update1.merge(update2)

        assert merged.array_limits[1] == 10  # From update1
        assert merged.array_limits[2] == 25  # Overwritten by update2
        assert merged.array_limits[3] == 30  # From update2

    def test_merge_workflow_run_status(self) -> None:
        """Test merging workflow run status updates."""
        update1 = StateUpdate(workflow_run_status="R")
        update2 = StateUpdate(workflow_run_status="D")

        merged = update1.merge(update2)
        assert merged.workflow_run_status == "D"

        # None doesn't override
        update3 = StateUpdate()
        merged = update2.merge(update3)
        assert merged.workflow_run_status == "D"


# ──────────────────────────────────────────────────────────────────────────────
# SwarmState Tests
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def state() -> SwarmState:
    """Create a basic SwarmState for testing."""
    return SwarmState(
        workflow_id=1,
        workflow_run_id=10,
        dag_id=5,
        max_concurrently_running=100,
        status="B",
    )


@pytest.fixture
def state_with_tasks(state: SwarmState) -> SwarmState:
    """Create a SwarmState with some tasks."""
    # Add arrays
    array1 = MockSwarmArray(array_id=1, max_concurrently_running=50)
    array2 = MockSwarmArray(array_id=2, max_concurrently_running=30)
    state.arrays[1] = array1  # type: ignore
    state.arrays[2] = array2  # type: ignore

    # Add tasks in various statuses
    tasks = [
        MockSwarmTask(1, array_id=1, status=TaskStatus.REGISTERING),
        MockSwarmTask(2, array_id=1, status=TaskStatus.REGISTERING),
        MockSwarmTask(3, array_id=1, status=TaskStatus.QUEUED),
        MockSwarmTask(4, array_id=1, status=TaskStatus.RUNNING),
        MockSwarmTask(5, array_id=2, status=TaskStatus.DONE),
        MockSwarmTask(6, array_id=2, status=TaskStatus.ERROR_FATAL),
    ]

    for task in tasks:
        state.add_task(task)  # type: ignore

    return state


class TestSwarmStateInitialization:
    """Tests for SwarmState initialization."""

    def test_basic_initialization(self, state: SwarmState) -> None:
        """Test basic state initialization."""
        assert state.workflow_id == 1
        assert state.workflow_run_id == 10
        assert state.dag_id == 5
        assert state.max_concurrently_running == 100
        assert state.status == "B"
        assert state.tasks == {}
        assert state.arrays == {}
        assert len(state.ready_to_run) == 0

    def test_status_map_initialized(self, state: SwarmState) -> None:
        """Test that status map has all expected statuses."""
        expected_statuses = {
            TaskStatus.REGISTERING,
            TaskStatus.QUEUED,
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
            TaskStatus.DONE,
            TaskStatus.ADJUSTING_RESOURCES,
            TaskStatus.ERROR_FATAL,
        }
        assert set(state._task_status_map.keys()) == expected_statuses


class TestSwarmStateTaskManagement:
    """Tests for task management operations."""

    def test_add_task(self, state: SwarmState) -> None:
        """Test adding a task."""
        task = MockSwarmTask(1, status=TaskStatus.REGISTERING)
        state.add_task(task)  # type: ignore

        assert state.tasks[1] is task
        assert task in state._task_status_map[TaskStatus.REGISTERING]

    def test_add_multiple_tasks(self, state: SwarmState) -> None:
        """Test adding multiple tasks."""
        task1 = MockSwarmTask(1, status=TaskStatus.REGISTERING)
        task2 = MockSwarmTask(2, status=TaskStatus.DONE)

        state.add_task(task1)  # type: ignore
        state.add_task(task2)  # type: ignore

        assert len(state.tasks) == 2
        assert task1 in state._task_status_map[TaskStatus.REGISTERING]
        assert task2 in state._task_status_map[TaskStatus.DONE]

    def test_get_task(self, state_with_tasks: SwarmState) -> None:
        """Test getting a task by ID."""
        task = state_with_tasks.get_task(1)
        assert task is not None
        assert task.task_id == 1

        # Non-existent task
        assert state_with_tasks.get_task(999) is None


class TestSwarmStateQueries:
    """Tests for state query methods."""

    def test_get_active_task_count(self, state_with_tasks: SwarmState) -> None:
        """Test counting active tasks."""
        # Tasks 3 (QUEUED) and 4 (RUNNING) are active
        assert state_with_tasks.get_active_task_count() == 2

    def test_get_done_count(self, state_with_tasks: SwarmState) -> None:
        """Test counting done tasks."""
        assert state_with_tasks.get_done_count() == 1  # Task 5

    def test_get_failed_count(self, state_with_tasks: SwarmState) -> None:
        """Test counting failed tasks."""
        assert state_with_tasks.get_failed_count() == 1  # Task 6

    def test_get_done_tasks(self, state_with_tasks: SwarmState) -> None:
        """Test getting done tasks."""
        done = state_with_tasks.get_done_tasks()
        assert len(done) == 1
        assert done[0].task_id == 5

    def test_get_failed_tasks(self, state_with_tasks: SwarmState) -> None:
        """Test getting failed tasks."""
        failed = state_with_tasks.get_failed_tasks()
        assert len(failed) == 1
        assert failed[0].task_id == 6

    def test_all_tasks_final_false(self, state_with_tasks: SwarmState) -> None:
        """Test all_tasks_final when tasks are still running."""
        assert not state_with_tasks.all_tasks_final()

    def test_all_tasks_final_true(self, state: SwarmState) -> None:
        """Test all_tasks_final when all tasks are done/failed."""
        state.add_task(MockSwarmTask(1, status=TaskStatus.DONE))  # type: ignore
        state.add_task(MockSwarmTask(2, status=TaskStatus.ERROR_FATAL))  # type: ignore
        assert state.all_tasks_final()

    def test_has_pending_work(self, state_with_tasks: SwarmState) -> None:
        """Test checking for pending work."""
        assert state_with_tasks.has_pending_work()  # Has active tasks

        # Clear active tasks
        state = SwarmState(workflow_id=1, workflow_run_id=1, dag_id=1)
        state.add_task(MockSwarmTask(1, status=TaskStatus.DONE))  # type: ignore
        assert not state.has_pending_work()

        # Add ready_to_run
        state.ready_to_run.append(MockSwarmTask(2))  # type: ignore
        assert state.has_pending_work()

    def test_get_available_capacity(self, state_with_tasks: SwarmState) -> None:
        """Test calculating available capacity."""
        # max=100, active=2
        assert state_with_tasks.get_available_capacity() == 98

    def test_get_array_capacity(self, state_with_tasks: SwarmState) -> None:
        """Test calculating array capacity."""
        # Array 1: max=50, active=2 (tasks 3 and 4)
        assert state_with_tasks.get_array_capacity(1) == 48

        # Array 2: max=30, active=0 (task 5 is DONE, task 6 is ERROR_FATAL)
        assert state_with_tasks.get_array_capacity(2) == 30

        # Non-existent array
        assert state_with_tasks.get_array_capacity(999) == 0

    def test_get_percent_done(self, state_with_tasks: SwarmState) -> None:
        """Test calculating percent done."""
        # 1 done out of 6 tasks = 16.67%
        assert state_with_tasks.get_percent_done() == pytest.approx(16.67, rel=0.01)

    def test_get_percent_done_empty(self, state: SwarmState) -> None:
        """Test percent done with no tasks."""
        assert state.get_percent_done() == 0.0


class TestSwarmStateMutations:
    """Tests for state mutation methods."""

    def test_apply_update_task_statuses(self, state_with_tasks: SwarmState) -> None:
        """Test applying task status updates."""
        # Move task 1 from REGISTERING to DONE
        update = StateUpdate(task_statuses={1: TaskStatus.DONE})
        changed = state_with_tasks.apply_update(update)

        assert len(changed) == 1
        task = state_with_tasks.get_task(1)
        assert task.status == TaskStatus.DONE
        assert task in state_with_tasks._task_status_map[TaskStatus.DONE]
        assert task not in state_with_tasks._task_status_map[TaskStatus.REGISTERING]

    def test_apply_update_no_change_same_status(
        self, state_with_tasks: SwarmState
    ) -> None:
        """Test that same-status updates don't count as changes."""
        # Task 5 is already DONE
        update = StateUpdate(task_statuses={5: TaskStatus.DONE})
        changed = state_with_tasks.apply_update(update)

        assert len(changed) == 0

    def test_apply_update_concurrency(self, state_with_tasks: SwarmState) -> None:
        """Test applying concurrency limit update."""
        update = StateUpdate(max_concurrently_running=200)
        state_with_tasks.apply_update(update)

        assert state_with_tasks.max_concurrently_running == 200

    def test_apply_update_array_limits(self, state_with_tasks: SwarmState) -> None:
        """Test applying array limit updates."""
        update = StateUpdate(array_limits={1: 75, 2: 40})
        state_with_tasks.apply_update(update)

        assert state_with_tasks.arrays[1].max_concurrently_running == 75
        assert state_with_tasks.arrays[2].max_concurrently_running == 40

    def test_apply_update_workflow_status(self, state_with_tasks: SwarmState) -> None:
        """Test applying workflow run status update."""
        update = StateUpdate(workflow_run_status="R")
        state_with_tasks.apply_update(update)

        assert state_with_tasks.status == "R"

    def test_apply_update_sync_time(self, state_with_tasks: SwarmState) -> None:
        """Test applying sync time update."""
        now = datetime.now()
        update = StateUpdate(sync_time=now)
        state_with_tasks.apply_update(update)

        assert state_with_tasks.last_sync == now

    def test_update_task_status(self, state_with_tasks: SwarmState) -> None:
        """Test direct task status update."""
        result = state_with_tasks.update_task_status(1, TaskStatus.QUEUED)
        assert result is True

        task = state_with_tasks.get_task(1)
        assert task.status == TaskStatus.QUEUED

    def test_update_task_status_not_found(self, state_with_tasks: SwarmState) -> None:
        """Test updating non-existent task."""
        result = state_with_tasks.update_task_status(999, TaskStatus.DONE)
        assert result is False

    def test_update_task_status_same_status(
        self, state_with_tasks: SwarmState
    ) -> None:
        """Test updating to same status."""
        result = state_with_tasks.update_task_status(
            1, TaskStatus.REGISTERING
        )  # Already REGISTERING
        assert result is False

    def test_propagate_completions(self, state: SwarmState) -> None:
        """Test propagating completions to downstream tasks."""
        # Create a simple DAG: task1 -> task2
        task1 = MockSwarmTask(1, status=TaskStatus.DONE)
        task2 = MockSwarmTask(2, status=TaskStatus.REGISTERING)
        task2.num_upstreams = 1
        task1.downstream_swarm_tasks.add(task2)

        state.add_task(task1)  # type: ignore
        state.add_task(task2)  # type: ignore

        newly_ready = state.propagate_completions({task1})  # type: ignore

        assert task2.num_upstreams_done == 1
        assert task2 in newly_ready

    def test_propagate_completions_not_ready(self, state: SwarmState) -> None:
        """Test propagating when downstream isn't ready yet."""
        # task1, task2 -> task3 (needs both)
        task1 = MockSwarmTask(1, status=TaskStatus.DONE)
        task3 = MockSwarmTask(3, status=TaskStatus.REGISTERING)
        task3.num_upstreams = 2
        task1.downstream_swarm_tasks.add(task3)

        state.add_task(task1)  # type: ignore
        state.add_task(task3)  # type: ignore

        newly_ready = state.propagate_completions({task1})  # type: ignore

        assert task3.num_upstreams_done == 1
        assert task3 not in newly_ready  # Still waiting for task2


class TestSwarmStateQueue:
    """Tests for ready_to_run queue operations."""

    def test_enqueue_task(self, state: SwarmState) -> None:
        """Test enqueueing a task."""
        task = MockSwarmTask(1)
        state.enqueue_task(task)  # type: ignore

        assert len(state.ready_to_run) == 1
        assert state.ready_to_run[0] is task

    def test_enqueue_task_front(self, state: SwarmState) -> None:
        """Test enqueueing at front of queue."""
        task1 = MockSwarmTask(1)
        task2 = MockSwarmTask(2)

        state.enqueue_task(task1)  # type: ignore
        state.enqueue_task(task2, front=True)  # type: ignore

        assert state.ready_to_run[0] is task2  # task2 at front
        assert state.ready_to_run[1] is task1

    def test_dequeue_task(self, state: SwarmState) -> None:
        """Test dequeueing a task."""
        task1 = MockSwarmTask(1)
        task2 = MockSwarmTask(2)

        state.enqueue_task(task1)  # type: ignore
        state.enqueue_task(task2)  # type: ignore

        dequeued = state.dequeue_task()
        assert dequeued is task1
        assert len(state.ready_to_run) == 1

    def test_dequeue_empty(self, state: SwarmState) -> None:
        """Test dequeueing from empty queue."""
        assert state.dequeue_task() is None


class TestSwarmStateResourceCaching:
    """Tests for resource caching operations."""

    def test_cache_resources(self, state: SwarmState) -> None:
        """Test caching task resources."""
        mock_resources = MagicMock()
        resource_hash = 12345

        cached = state.cache_resources(resource_hash, mock_resources)
        assert cached is mock_resources
        assert state.task_resources_cache[resource_hash] is mock_resources

    def test_cache_resources_deduplication(self, state: SwarmState) -> None:
        """Test that caching returns existing resource."""
        mock_resources1 = MagicMock()
        mock_resources2 = MagicMock()
        resource_hash = 12345

        cached1 = state.cache_resources(resource_hash, mock_resources1)
        cached2 = state.cache_resources(resource_hash, mock_resources2)

        assert cached1 is mock_resources1
        assert cached2 is mock_resources1  # Returns existing, not new

    def test_get_cached_resources(self, state: SwarmState) -> None:
        """Test getting cached resources."""
        mock_resources = MagicMock()
        resource_hash = 12345

        state.cache_resources(resource_hash, mock_resources)
        retrieved = state.get_cached_resources(resource_hash)

        assert retrieved is mock_resources

    def test_get_cached_resources_not_found(self, state: SwarmState) -> None:
        """Test getting non-existent cached resources."""
        assert state.get_cached_resources(99999) is None


class TestSwarmStateInitializationHelpers:
    """Tests for initialization helper methods."""

    def test_compute_initial_upstream_done_counts(self, state: SwarmState) -> None:
        """Test computing initial upstream done counts."""
        # task1 (DONE) -> task2, task3
        task1 = MockSwarmTask(1, status=TaskStatus.DONE)
        task2 = MockSwarmTask(2, status=TaskStatus.REGISTERING)
        task3 = MockSwarmTask(3, status=TaskStatus.REGISTERING)

        task1.downstream_swarm_tasks.add(task2)
        task1.downstream_swarm_tasks.add(task3)
        task2.num_upstreams = 1
        task3.num_upstreams = 1

        state.add_task(task1)  # type: ignore
        state.add_task(task2)  # type: ignore
        state.add_task(task3)  # type: ignore

        state.compute_initial_upstream_done_counts()

        assert task2.num_upstreams_done == 1
        assert task3.num_upstreams_done == 1

