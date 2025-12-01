"""Unit tests for Scheduler."""

from __future__ import annotations

import asyncio
from collections import deque
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from jobmon.client.swarm.workflow_run_impl.gateway import QueueResponse
from jobmon.client.swarm.workflow_run_impl.services.scheduler import (
    BatchResult,
    Scheduler,
)
from jobmon.client.swarm.workflow_run_impl.state import StateUpdate
from jobmon.core.constants import TaskStatus


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_gateway():
    """Create a mock ServerGateway."""
    gateway = MagicMock()
    gateway.queue_task_batch = AsyncMock(
        return_value=QueueResponse(tasks_by_status={TaskStatus.QUEUED: [1]})
    )
    return gateway


@pytest.fixture
def mock_task_resources():
    """Create mock task resources."""
    resources = MagicMock()
    resources.is_bound = True
    resources.id = 1
    return resources


@pytest.fixture
def mock_cluster():
    """Create a mock cluster."""
    cluster = MagicMock()
    cluster.id = 1
    return cluster


def create_mock_task(
    task_id: int,
    array_id: int,
    status: str = TaskStatus.REGISTERING,
    task_resources: MagicMock = None,
    cluster: MagicMock = None,
) -> MagicMock:
    """Create a mock SwarmTask."""
    task = MagicMock()
    task.task_id = task_id
    task.array_id = array_id
    task.status = status
    task.current_task_resources = task_resources or MagicMock(is_bound=True, id=1)
    task.cluster = cluster or MagicMock(id=1)
    return task


def create_mock_array(array_id: int, max_concurrently_running: int = 100) -> MagicMock:
    """Create a mock SwarmArray."""
    array = MagicMock()
    array.array_id = array_id
    array.max_concurrently_running = max_concurrently_running
    array.array_name = f"array_{array_id}"
    array.tasks = set()
    return array


@pytest.fixture
def task_status_map():
    """Create empty task status map."""
    return {status: set() for status in [
        TaskStatus.REGISTERING,
        TaskStatus.QUEUED,
        TaskStatus.INSTANTIATING,
        TaskStatus.LAUNCHED,
        TaskStatus.RUNNING,
        TaskStatus.DONE,
        TaskStatus.ERROR_FATAL,
        TaskStatus.ERROR_RECOVERABLE,
        TaskStatus.ADJUSTING_RESOURCES,
    ]}


@pytest.fixture
def scheduler(mock_gateway, task_status_map):
    """Create a Scheduler with default settings."""
    tasks = {}
    arrays = {10: create_mock_array(10)}
    ready_to_run = deque()

    return Scheduler(
        gateway=mock_gateway,
        tasks=tasks,
        arrays=arrays,
        task_status_map=task_status_map,
        ready_to_run=ready_to_run,
        max_concurrently_running=100,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Test Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerInit:
    """Tests for Scheduler initialization."""

    def test_init_stores_parameters(self, mock_gateway, task_status_map):
        """Test that init stores all parameters correctly."""
        tasks = {1: MagicMock()}
        arrays = {10: create_mock_array(10)}
        ready_to_run = deque([MagicMock()])

        sched = Scheduler(
            gateway=mock_gateway,
            tasks=tasks,
            arrays=arrays,
            task_status_map=task_status_map,
            ready_to_run=ready_to_run,
            max_concurrently_running=50,
        )

        assert sched.max_concurrently_running == 50
        assert sched._tasks is tasks
        assert sched._arrays is arrays

    def test_max_concurrently_running_property(self, scheduler):
        """Test max_concurrently_running property getter and setter."""
        assert scheduler.max_concurrently_running == 100

        scheduler.max_concurrently_running = 200
        assert scheduler.max_concurrently_running == 200


# ──────────────────────────────────────────────────────────────────────────────
# Test Capacity Calculations
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerCapacity:
    """Tests for capacity calculations."""

    def test_get_active_task_count_empty(self, scheduler):
        """Test active task count when no active tasks."""
        assert scheduler.get_active_task_count() == 0

    def test_get_active_task_count_with_tasks(self, scheduler, task_status_map):
        """Test active task count with active tasks."""
        task1 = create_mock_task(1, 10, TaskStatus.QUEUED)
        task2 = create_mock_task(2, 10, TaskStatus.RUNNING)
        task3 = create_mock_task(3, 10, TaskStatus.DONE)

        task_status_map[TaskStatus.QUEUED].add(task1)
        task_status_map[TaskStatus.RUNNING].add(task2)
        task_status_map[TaskStatus.DONE].add(task3)

        assert scheduler.get_active_task_count() == 2  # QUEUED + RUNNING

    def test_get_available_capacity(self, scheduler, task_status_map):
        """Test available capacity calculation."""
        scheduler.max_concurrently_running = 10

        # Add 3 active tasks
        for i in range(3):
            task = create_mock_task(i, 10, TaskStatus.RUNNING)
            task_status_map[TaskStatus.RUNNING].add(task)

        assert scheduler.get_available_capacity() == 7

    def test_get_array_capacity(self, scheduler, task_status_map):
        """Test array capacity calculation."""
        array = scheduler._arrays[10]
        array.max_concurrently_running = 5

        # Add 2 active tasks in the array
        task1 = create_mock_task(1, 10, TaskStatus.RUNNING)
        task2 = create_mock_task(2, 10, TaskStatus.QUEUED)
        array.tasks = {task1, task2}
        task_status_map[TaskStatus.RUNNING].add(task1)
        task_status_map[TaskStatus.QUEUED].add(task2)

        assert scheduler.get_array_capacity(10) == 3


# ──────────────────────────────────────────────────────────────────────────────
# Test has_work
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerHasWork:
    """Tests for has_work method."""

    def test_has_work_empty_queue(self, scheduler):
        """Test has_work returns False when queue is empty."""
        assert scheduler.has_work() is False

    def test_has_work_with_capacity(self, scheduler):
        """Test has_work returns True when queue has tasks and capacity available."""
        task = create_mock_task(1, 10)
        scheduler._ready_to_run.append(task)
        scheduler.max_concurrently_running = 10

        assert scheduler.has_work() is True

    def test_has_work_no_capacity(self, scheduler, task_status_map):
        """Test has_work returns False when no capacity."""
        task = create_mock_task(1, 10)
        scheduler._ready_to_run.append(task)
        scheduler.max_concurrently_running = 2

        # Fill up capacity
        for i in range(2):
            active_task = create_mock_task(100 + i, 10, TaskStatus.RUNNING)
            task_status_map[TaskStatus.RUNNING].add(active_task)

        assert scheduler.has_work() is False


# ──────────────────────────────────────────────────────────────────────────────
# Test Batch Generation
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerBatchGeneration:
    """Tests for batch generation."""

    def test_generate_batches_empty_queue(self, scheduler):
        """Test batch generation with empty queue."""
        batches = list(scheduler._generate_batches())
        assert batches == []

    def test_generate_batches_single_task(self, scheduler):
        """Test batch generation with single task."""
        task = create_mock_task(1, 10)
        scheduler._ready_to_run.append(task)
        scheduler._tasks[1] = task

        batches = list(scheduler._generate_batches())

        assert len(batches) == 1
        assert len(batches[0]) == 1
        assert batches[0][0] is task

    def test_generate_batches_groups_compatible_tasks(self, scheduler):
        """Test that compatible tasks are grouped in same batch."""
        shared_resources = MagicMock(is_bound=True, id=1)

        tasks = [
            create_mock_task(i, 10, task_resources=shared_resources)
            for i in range(5)
        ]
        for task in tasks:
            scheduler._ready_to_run.append(task)
            scheduler._tasks[task.task_id] = task

        batches = list(scheduler._generate_batches())

        # All tasks should be in one batch (same array, same resources)
        assert len(batches) == 1
        assert len(batches[0]) == 5

    def test_generate_batches_separates_different_arrays(self, scheduler):
        """Test that tasks from different arrays go to different batches."""
        scheduler._arrays[20] = create_mock_array(20)

        task1 = create_mock_task(1, 10)
        task2 = create_mock_task(2, 20)

        scheduler._ready_to_run.append(task1)
        scheduler._ready_to_run.append(task2)
        scheduler._tasks[1] = task1
        scheduler._tasks[2] = task2

        batches = list(scheduler._generate_batches())

        # Should have 2 batches (different arrays)
        assert len(batches) == 2

    def test_generate_batches_separates_different_resources(self, scheduler):
        """Test that tasks with different resources go to different batches."""
        resources1 = MagicMock(is_bound=True, id=1)
        resources2 = MagicMock(is_bound=True, id=2)

        task1 = create_mock_task(1, 10, task_resources=resources1)
        task2 = create_mock_task(2, 10, task_resources=resources2)

        scheduler._ready_to_run.append(task1)
        scheduler._ready_to_run.append(task2)
        scheduler._tasks[1] = task1
        scheduler._tasks[2] = task2

        batches = list(scheduler._generate_batches())

        # Should have 2 batches (different resources)
        assert len(batches) == 2

    def test_generate_batches_respects_workflow_capacity(self, scheduler, task_status_map):
        """Test that batch generation respects workflow capacity."""
        scheduler.max_concurrently_running = 3

        # Add one running task
        running_task = create_mock_task(100, 10, TaskStatus.RUNNING)
        task_status_map[TaskStatus.RUNNING].add(running_task)
        scheduler._arrays[10].tasks.add(running_task)

        # Add 5 tasks to queue
        shared_resources = MagicMock(is_bound=True, id=1)
        for i in range(5):
            task = create_mock_task(i, 10, task_resources=shared_resources)
            scheduler._ready_to_run.append(task)
            scheduler._tasks[i] = task

        batches = list(scheduler._generate_batches())

        # Should only schedule 2 tasks (capacity = 3 - 1 running = 2)
        total_scheduled = sum(len(b) for b in batches)
        assert total_scheduled == 2

    def test_generate_batches_respects_array_capacity(self, mock_gateway, task_status_map):
        """Test that batch generation respects array capacity."""
        # Create array with capacity of 5
        array = create_mock_array(10, max_concurrently_running=5)
        
        # Add 3 already-active tasks in the array (consuming capacity)
        active_tasks = []
        for i in range(100, 103):
            active_task = create_mock_task(i, 10, TaskStatus.RUNNING)
            task_status_map[TaskStatus.RUNNING].add(active_task)
            array.tasks.add(active_task)
            active_tasks.append(active_task)

        # Now array has capacity for only 2 more (5 - 3 = 2)

        # Add 5 tasks to queue
        tasks = {}
        ready_to_run = deque()
        shared_resources = MagicMock(is_bound=True, id=1)
        for i in range(5):
            task = create_mock_task(i, 10, task_resources=shared_resources)
            ready_to_run.append(task)
            tasks[i] = task
            array.tasks.add(task)

        scheduler = Scheduler(
            gateway=mock_gateway,
            tasks=tasks,
            arrays={10: array},
            task_status_map=task_status_map,
            ready_to_run=ready_to_run,
            max_concurrently_running=100,
        )

        batches = list(scheduler._generate_batches())

        # Should only schedule 2 tasks (array capacity = 5 - 3 active = 2)
        total_scheduled = sum(len(b) for b in batches)
        assert total_scheduled == 2

    def test_generate_batches_puts_unscheduled_back(self, mock_gateway, task_status_map):
        """Test that unscheduled tasks are put back in queue."""
        # Create array with capacity of 3
        array = create_mock_array(10, max_concurrently_running=3)

        # Add 2 already-active tasks in the array (consuming capacity)
        for i in range(100, 102):
            active_task = create_mock_task(i, 10, TaskStatus.RUNNING)
            task_status_map[TaskStatus.RUNNING].add(active_task)
            array.tasks.add(active_task)

        # Now array has capacity for only 1 more (3 - 2 = 1)

        # Add 3 tasks to queue
        tasks = {}
        ready_to_run = deque()
        shared_resources = MagicMock(is_bound=True, id=1)
        for i in range(3):
            task = create_mock_task(i, 10, task_resources=shared_resources)
            ready_to_run.append(task)
            tasks[i] = task
            array.tasks.add(task)

        scheduler = Scheduler(
            gateway=mock_gateway,
            tasks=tasks,
            arrays={10: array},
            task_status_map=task_status_map,
            ready_to_run=ready_to_run,
            max_concurrently_running=100,
        )

        batches = list(scheduler._generate_batches())

        # Should have scheduled 1 task, 2 back in queue
        assert len(batches) == 1
        assert len(batches[0]) == 1
        assert len(ready_to_run) == 2

    def test_generate_batches_max_batch_size(self, scheduler):
        """Test that batches respect MAX_BATCH_SIZE."""
        # Create more tasks than max batch size
        shared_resources = MagicMock(is_bound=True, id=1)
        for i in range(600):
            task = create_mock_task(i, 10, task_resources=shared_resources)
            scheduler._ready_to_run.append(task)
            scheduler._tasks[i] = task

        batches = list(scheduler._generate_batches())

        # First batch should be capped at MAX_BATCH_SIZE
        assert len(batches[0]) <= Scheduler.MAX_BATCH_SIZE


# ──────────────────────────────────────────────────────────────────────────────
# Test Queue Batch
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerQueueBatch:
    """Tests for _queue_batch method."""

    @pytest.mark.asyncio
    async def test_queue_batch_calls_gateway(self, scheduler, mock_gateway):
        """Test _queue_batch calls gateway with correct parameters."""
        task = create_mock_task(1, 10)
        task.current_task_resources.id = 42
        task.cluster.id = 5

        mock_gateway.queue_task_batch = AsyncMock(
            return_value=QueueResponse(tasks_by_status={TaskStatus.QUEUED: [1]})
        )

        await scheduler._queue_batch([task])

        mock_gateway.queue_task_batch.assert_called_once_with(
            array_id=10,
            task_ids=[1],
            task_resources_id=42,
            cluster_id=5,
        )

    @pytest.mark.asyncio
    async def test_queue_batch_binds_resources_if_needed(self, scheduler, mock_gateway):
        """Test _queue_batch binds resources if not already bound."""
        task = create_mock_task(1, 10)
        task.current_task_resources.is_bound = False

        mock_gateway.queue_task_batch = AsyncMock(
            return_value=QueueResponse(tasks_by_status={TaskStatus.QUEUED: [1]})
        )

        await scheduler._queue_batch([task])

        task.current_task_resources.bind.assert_called_once()

    @pytest.mark.asyncio
    async def test_queue_batch_returns_status_updates(self, scheduler, mock_gateway):
        """Test _queue_batch returns correct BatchResult."""
        task = create_mock_task(1, 10)

        mock_gateway.queue_task_batch = AsyncMock(
            return_value=QueueResponse(
                tasks_by_status={
                    TaskStatus.QUEUED: [1, 2],
                    TaskStatus.INSTANTIATING: [3],
                }
            )
        )

        result = await scheduler._queue_batch([task])

        assert result.task_statuses == {
            1: TaskStatus.QUEUED,
            2: TaskStatus.QUEUED,
            3: TaskStatus.INSTANTIATING,
        }
        assert result.batch_size == 1
        assert result.array_id == 10


# ──────────────────────────────────────────────────────────────────────────────
# Test Tick
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerTick:
    """Tests for the tick method."""

    @pytest.mark.asyncio
    async def test_tick_empty_queue(self, scheduler):
        """Test tick with empty queue returns empty update."""
        update = await scheduler.tick()

        assert update == StateUpdate.empty()

    @pytest.mark.asyncio
    async def test_tick_schedules_tasks(self, scheduler, mock_gateway):
        """Test tick schedules ready tasks."""
        task = create_mock_task(1, 10)
        scheduler._ready_to_run.append(task)
        scheduler._tasks[1] = task

        mock_gateway.queue_task_batch = AsyncMock(
            return_value=QueueResponse(tasks_by_status={TaskStatus.QUEUED: [1]})
        )

        update = await scheduler.tick()

        assert update.task_statuses == {1: TaskStatus.QUEUED}
        mock_gateway.queue_task_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_tick_merges_multiple_batches(self, scheduler, mock_gateway):
        """Test tick merges results from multiple batches."""
        scheduler._arrays[20] = create_mock_array(20)

        task1 = create_mock_task(1, 10)
        task2 = create_mock_task(2, 20)
        scheduler._ready_to_run.append(task1)
        scheduler._ready_to_run.append(task2)
        scheduler._tasks[1] = task1
        scheduler._tasks[2] = task2

        call_count = [0]

        async def mock_queue(array_id, task_ids, **kwargs):
            call_count[0] += 1
            return QueueResponse(tasks_by_status={TaskStatus.QUEUED: task_ids})

        mock_gateway.queue_task_batch = AsyncMock(side_effect=mock_queue)

        update = await scheduler.tick()

        assert update.task_statuses == {1: TaskStatus.QUEUED, 2: TaskStatus.QUEUED}
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_tick_respects_timeout(self, scheduler, mock_gateway):
        """Test tick stops when timeout is reached."""
        # Add many tasks across different arrays so we get multiple batches
        scheduler._arrays[20] = create_mock_array(20)
        scheduler._arrays[30] = create_mock_array(30)

        for i in range(30):
            # Distribute across arrays to force multiple batches
            array_id = 10 + (i % 3) * 10
            task = create_mock_task(i, array_id)
            scheduler._ready_to_run.append(task)
            scheduler._tasks[i] = task

        async def slow_queue(**kwargs):
            await asyncio.sleep(0.05)
            return QueueResponse(tasks_by_status={TaskStatus.QUEUED: kwargs["task_ids"]})

        mock_gateway.queue_task_batch = AsyncMock(side_effect=slow_queue)

        update = await scheduler.tick(timeout=0.1)

        # Should have stopped due to timeout (not all 30 tasks scheduled)
        # With 0.05s per batch and 0.1s timeout, should get ~2 batches
        assert len(update.task_statuses) < 30


# ──────────────────────────────────────────────────────────────────────────────
# Test BatchResult
# ──────────────────────────────────────────────────────────────────────────────


class TestBatchResult:
    """Tests for BatchResult dataclass."""

    def test_batch_result_defaults(self):
        """Test BatchResult default values."""
        result = BatchResult()

        assert result.task_statuses == {}
        assert result.batch_size == 0
        assert result.array_id == 0

    def test_batch_result_with_data(self):
        """Test BatchResult with data."""
        result = BatchResult(
            task_statuses={1: TaskStatus.QUEUED, 2: TaskStatus.QUEUED},
            batch_size=2,
            array_id=10,
        )

        assert result.task_statuses == {1: TaskStatus.QUEUED, 2: TaskStatus.QUEUED}
        assert result.batch_size == 2
        assert result.array_id == 10


# ──────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerIntegration:
    """Integration tests for Scheduler."""

    @pytest.mark.asyncio
    async def test_full_scheduling_cycle(self, mock_gateway, task_status_map):
        """Test a complete scheduling cycle with workflow capacity limit."""
        # Setup
        tasks = {}
        arrays = {10: create_mock_array(10, max_concurrently_running=100)}
        ready_to_run = deque()

        scheduler = Scheduler(
            gateway=mock_gateway,
            tasks=tasks,
            arrays=arrays,
            task_status_map=task_status_map,
            ready_to_run=ready_to_run,
            max_concurrently_running=3,  # Workflow limit of 3
        )

        # Add 5 tasks
        shared_resources = MagicMock(is_bound=True, id=1)
        for i in range(5):
            task = create_mock_task(i, 10, task_resources=shared_resources)
            ready_to_run.append(task)
            tasks[i] = task

        queued_task_ids = []

        async def mock_queue(array_id, task_ids, **kwargs):
            queued_task_ids.extend(task_ids)
            return QueueResponse(tasks_by_status={TaskStatus.QUEUED: task_ids})

        mock_gateway.queue_task_batch = AsyncMock(side_effect=mock_queue)

        # First tick - should schedule up to workflow limit (3)
        update = await scheduler.tick()

        # Verify results - should have scheduled 3 tasks (workflow capacity)
        assert len(queued_task_ids) == 3
        # 2 tasks should remain in queue (5 - 3 scheduled)
        assert len(ready_to_run) == 2

    @pytest.mark.asyncio
    async def test_multiple_ticks_drain_queue(self, mock_gateway, task_status_map):
        """Test that multiple ticks eventually drain the queue when capacity allows."""
        tasks = {}
        arrays = {10: create_mock_array(10, max_concurrently_running=100)}
        ready_to_run = deque()

        scheduler = Scheduler(
            gateway=mock_gateway,
            tasks=tasks,
            arrays=arrays,
            task_status_map=task_status_map,
            ready_to_run=ready_to_run,
            max_concurrently_running=2,  # Workflow limit of 2
        )

        # Add 6 tasks
        shared_resources = MagicMock(is_bound=True, id=1)
        for i in range(6):
            task = create_mock_task(i, 10, task_resources=shared_resources)
            ready_to_run.append(task)
            tasks[i] = task

        queued_count = [0]

        async def mock_queue(array_id, task_ids, **kwargs):
            queued_count[0] += len(task_ids)
            return QueueResponse(tasks_by_status={TaskStatus.QUEUED: task_ids})

        mock_gateway.queue_task_batch = AsyncMock(side_effect=mock_queue)

        # First tick - should schedule 2 (workflow limit)
        await scheduler.tick()
        assert queued_count[0] == 2
        assert len(ready_to_run) == 4

        # Second tick - capacity is still available (no active tasks in status map)
        await scheduler.tick()
        assert queued_count[0] == 4
        assert len(ready_to_run) == 2

        # Third tick
        await scheduler.tick()
        assert queued_count[0] == 6
        assert len(ready_to_run) == 0

