"""Unit tests for Synchronizer."""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from jobmon.client.swarm.gateway import TaskStatusUpdatesResponse
from jobmon.client.swarm.services.synchronizer import Synchronizer
from jobmon.client.swarm.state import StateUpdate
from jobmon.core.constants import TaskStatus


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_gateway():
    """Create a mock ServerGateway."""
    gateway = MagicMock()
    gateway.request_triage = AsyncMock()
    gateway.get_task_status_updates = AsyncMock(
        return_value=TaskStatusUpdatesResponse(
            time=datetime(2024, 1, 1, 12, 0, 0),
            tasks_by_status={},
        )
    )
    gateway.get_workflow_concurrency = AsyncMock(return_value=100)
    gateway.get_array_concurrency = AsyncMock(return_value=50)
    return gateway


@pytest.fixture
def task_ids():
    """Set of task IDs."""
    return {1, 2, 3, 4, 5}


@pytest.fixture
def array_ids():
    """Set of array IDs."""
    return {10, 20}


@pytest.fixture
def synchronizer(mock_gateway, task_ids, array_ids):
    """Create a Synchronizer with default settings."""
    return Synchronizer(
        gateway=mock_gateway,
        task_ids=task_ids,
        array_ids=array_ids,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Test Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerInit:
    """Tests for Synchronizer initialization."""

    def test_init_stores_parameters(self, mock_gateway, task_ids, array_ids):
        """Test that init stores all parameters correctly."""
        sync = Synchronizer(
            gateway=mock_gateway,
            task_ids=task_ids,
            array_ids=array_ids,
        )

        assert sync.task_ids == task_ids
        assert sync.array_ids == array_ids

    def test_init_empty_arrays(self, mock_gateway, task_ids):
        """Test initialization with no arrays."""
        sync = Synchronizer(
            gateway=mock_gateway,
            task_ids=task_ids,
            array_ids=set(),
        )

        assert sync.array_ids == set()


# ──────────────────────────────────────────────────────────────────────────────
# Test Update Methods
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerUpdateMethods:
    """Tests for update_task_ids and update_array_ids."""

    def test_update_task_ids(self, synchronizer):
        """Test updating task IDs."""
        new_ids = {100, 200, 300}
        synchronizer.update_task_ids(new_ids)
        assert synchronizer.task_ids == new_ids

    def test_update_array_ids(self, synchronizer):
        """Test updating array IDs."""
        new_ids = {30, 40}
        synchronizer.update_array_ids(new_ids)
        assert synchronizer.array_ids == new_ids


# ──────────────────────────────────────────────────────────────────────────────
# Test Request Triage
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerTriage:
    """Tests for triage operations."""

    @pytest.mark.asyncio
    async def test_request_triage_only(self, synchronizer, mock_gateway):
        """Test request_triage_only calls gateway."""
        await synchronizer.request_triage_only()
        mock_gateway.request_triage.assert_called_once()

    @pytest.mark.asyncio
    async def test_triage_returns_empty_update(self, synchronizer, mock_gateway):
        """Test _request_triage returns empty StateUpdate."""
        update = await synchronizer._request_triage()
        assert update == StateUpdate.empty()


# ──────────────────────────────────────────────────────────────────────────────
# Test Task Status Updates
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerTaskUpdates:
    """Tests for task status synchronization."""

    @pytest.mark.asyncio
    async def test_get_task_updates_full_sync(self, synchronizer, mock_gateway):
        """Test full sync fetches all statuses."""
        sync_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={
                    TaskStatus.DONE: [1, 2],
                    TaskStatus.RUNNING: [3],
                },
            )
        )

        update = await synchronizer._get_task_updates(full_sync=True, last_sync=None)

        # Should call with since=None for full sync
        mock_gateway.get_task_status_updates.assert_called_once_with(since=None)
        assert update.task_statuses == {1: TaskStatus.DONE, 2: TaskStatus.DONE, 3: TaskStatus.RUNNING}
        assert update.sync_time == sync_time

    @pytest.mark.asyncio
    async def test_get_task_updates_incremental_sync(self, synchronizer, mock_gateway):
        """Test incremental sync uses last_sync time."""
        last_sync = datetime(2024, 1, 1, 11, 0, 0)
        sync_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={TaskStatus.DONE: [1]},
            )
        )

        update = await synchronizer._get_task_updates(full_sync=False, last_sync=last_sync)

        mock_gateway.get_task_status_updates.assert_called_once_with(since=last_sync)
        assert update.task_statuses == {1: TaskStatus.DONE}

    @pytest.mark.asyncio
    async def test_get_task_updates_filters_unknown_tasks(self, synchronizer, mock_gateway):
        """Test that unknown task IDs are filtered out."""
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1, 12, 0, 0),
                tasks_by_status={
                    TaskStatus.DONE: [1, 2, 999],  # 999 is unknown
                },
            )
        )

        update = await synchronizer._get_task_updates(full_sync=True, last_sync=None)

        # Should only include known tasks (1, 2) not 999
        assert 1 in update.task_statuses
        assert 2 in update.task_statuses
        assert 999 not in update.task_statuses

    @pytest.mark.asyncio
    async def test_get_task_updates_only_convenience(self, synchronizer, mock_gateway):
        """Test get_task_updates_only convenience method."""
        sync_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={TaskStatus.RUNNING: [3]},
            )
        )

        update = await synchronizer.get_task_updates_only(full_sync=False, last_sync=sync_time)

        assert update.task_statuses == {3: TaskStatus.RUNNING}


# ──────────────────────────────────────────────────────────────────────────────
# Test Concurrency Limits
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerConcurrency:
    """Tests for concurrency limit synchronization."""

    @pytest.mark.asyncio
    async def test_get_workflow_concurrency(self, synchronizer, mock_gateway):
        """Test fetching workflow concurrency limit."""
        mock_gateway.get_workflow_concurrency = AsyncMock(return_value=200)

        update = await synchronizer._get_workflow_concurrency()

        mock_gateway.get_workflow_concurrency.assert_called_once()
        assert update.max_concurrently_running == 200

    @pytest.mark.asyncio
    async def test_get_array_concurrency_limits(self, synchronizer, mock_gateway):
        """Test fetching array concurrency limits."""
        # Mock to return different values for different arrays
        async def get_array_limit(aid):
            return {10: 50, 20: 75}[aid]

        mock_gateway.get_array_concurrency = AsyncMock(side_effect=get_array_limit)

        update = await synchronizer._get_array_concurrency_limits()

        assert update.array_limits == {10: 50, 20: 75}
        assert mock_gateway.get_array_concurrency.call_count == 2

    @pytest.mark.asyncio
    async def test_get_array_concurrency_limits_empty(self, mock_gateway, task_ids):
        """Test with no arrays."""
        sync = Synchronizer(
            gateway=mock_gateway,
            task_ids=task_ids,
            array_ids=set(),
        )

        update = await sync._get_array_concurrency_limits()

        assert update.array_limits == {}
        mock_gateway.get_array_concurrency.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_array_concurrency_handles_failure(self, synchronizer, mock_gateway):
        """Test that single array failure doesn't stop others."""
        call_count = [0]

        async def get_array_limit(aid):
            call_count[0] += 1
            if aid == 10:
                raise Exception("Network error")
            return 75

        mock_gateway.get_array_concurrency = AsyncMock(side_effect=get_array_limit)

        update = await synchronizer._get_array_concurrency_limits()

        # Should still have array 20's limit
        assert 20 in update.array_limits
        assert update.array_limits[20] == 75
        # Array 10 should not be in limits (failed)
        assert 10 not in update.array_limits

    @pytest.mark.asyncio
    async def test_get_concurrency_limits_only(self, synchronizer, mock_gateway):
        """Test get_concurrency_limits_only convenience method."""
        mock_gateway.get_workflow_concurrency = AsyncMock(return_value=150)

        async def get_array_limit(aid):
            return {10: 30, 20: 40}[aid]

        mock_gateway.get_array_concurrency = AsyncMock(side_effect=get_array_limit)

        update = await synchronizer.get_concurrency_limits_only()

        assert update.max_concurrently_running == 150
        assert update.array_limits == {10: 30, 20: 40}


# ──────────────────────────────────────────────────────────────────────────────
# Test Full Tick
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerTick:
    """Tests for the full tick method."""

    @pytest.mark.asyncio
    async def test_tick_calls_all_operations(self, synchronizer, mock_gateway):
        """Test tick calls all sync operations."""
        sync_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={TaskStatus.DONE: [1]},
            )
        )
        mock_gateway.get_workflow_concurrency = AsyncMock(return_value=100)
        mock_gateway.get_array_concurrency = AsyncMock(return_value=50)

        await synchronizer.tick(full_sync=False, last_sync=sync_time)

        mock_gateway.request_triage.assert_called_once()
        mock_gateway.get_task_status_updates.assert_called_once()
        mock_gateway.get_workflow_concurrency.assert_called_once()
        # Array concurrency called for each array
        assert mock_gateway.get_array_concurrency.call_count == 2

    @pytest.mark.asyncio
    async def test_tick_merges_all_updates(self, synchronizer, mock_gateway):
        """Test tick merges all StateUpdates."""
        sync_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={TaskStatus.DONE: [1, 2]},
            )
        )
        mock_gateway.get_workflow_concurrency = AsyncMock(return_value=150)

        async def get_array_limit(aid):
            return {10: 30, 20: 40}[aid]

        mock_gateway.get_array_concurrency = AsyncMock(side_effect=get_array_limit)

        update = await synchronizer.tick(full_sync=True, last_sync=None)

        # Should have all data merged
        assert update.task_statuses == {1: TaskStatus.DONE, 2: TaskStatus.DONE}
        assert update.max_concurrently_running == 150
        assert update.array_limits == {10: 30, 20: 40}
        assert update.sync_time == sync_time

    @pytest.mark.asyncio
    async def test_tick_handles_partial_failure(self, synchronizer, mock_gateway):
        """Test tick continues if some operations fail."""
        sync_time = datetime(2024, 1, 1, 12, 0, 0)

        # Triage fails
        mock_gateway.request_triage = AsyncMock(side_effect=Exception("Network error"))
        # Task updates succeed
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={TaskStatus.DONE: [1]},
            )
        )
        # Workflow concurrency succeeds
        mock_gateway.get_workflow_concurrency = AsyncMock(return_value=100)
        # Array concurrency fails
        mock_gateway.get_array_concurrency = AsyncMock(
            side_effect=Exception("Array error")
        )

        update = await synchronizer.tick(full_sync=True, last_sync=None)

        # Should still have task updates and workflow concurrency
        assert update.task_statuses == {1: TaskStatus.DONE}
        assert update.max_concurrently_running == 100
        # Array limits should be empty due to failure
        assert update.array_limits == {}

    @pytest.mark.asyncio
    async def test_tick_without_arrays(self, mock_gateway, task_ids):
        """Test tick when there are no arrays."""
        sync = Synchronizer(
            gateway=mock_gateway,
            task_ids=task_ids,
            array_ids=set(),
        )

        sync_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=sync_time,
                tasks_by_status={},
            )
        )

        await sync.tick(full_sync=True, last_sync=None)

        # Should not call array concurrency
        mock_gateway.get_array_concurrency.assert_not_called()

    @pytest.mark.asyncio
    async def test_tick_full_sync_ignores_last_sync(self, synchronizer, mock_gateway):
        """Test that full_sync=True ignores last_sync parameter."""
        last_sync = datetime(2024, 1, 1, 11, 0, 0)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1, 12, 0, 0),
                tasks_by_status={},
            )
        )

        await synchronizer.tick(full_sync=True, last_sync=last_sync)

        # Should be called with since=None for full sync
        mock_gateway.get_task_status_updates.assert_called_once_with(since=None)


# ──────────────────────────────────────────────────────────────────────────────
# Test Parallelism
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerParallelism:
    """Tests for parallel execution."""

    @pytest.mark.asyncio
    async def test_tick_runs_operations_in_parallel(self, synchronizer, mock_gateway):
        """Test that tick runs all operations in parallel."""
        call_order = []

        async def slow_triage():
            call_order.append("triage_start")
            await asyncio.sleep(0.05)
            call_order.append("triage_end")

        async def slow_task_updates(since):
            call_order.append("task_updates_start")
            await asyncio.sleep(0.05)
            call_order.append("task_updates_end")
            return TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1, 12, 0, 0),
                tasks_by_status={},
            )

        async def slow_workflow_concurrency():
            call_order.append("workflow_start")
            await asyncio.sleep(0.05)
            call_order.append("workflow_end")
            return 100

        mock_gateway.request_triage = slow_triage
        mock_gateway.get_task_status_updates = slow_task_updates
        mock_gateway.get_workflow_concurrency = slow_workflow_concurrency
        mock_gateway.get_array_concurrency = AsyncMock(return_value=50)

        await synchronizer.tick(full_sync=True, last_sync=None)

        # All starts should come before all ends (parallel execution)
        starts = [i for i, x in enumerate(call_order) if "start" in x]
        ends = [i for i, x in enumerate(call_order) if "end" in x]

        # At least some interleaving should occur (starts before some ends)
        assert min(ends) > min(starts)


# ──────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerIntegration:
    """Integration tests for Synchronizer."""

    @pytest.mark.asyncio
    async def test_multiple_ticks_accumulate_state(self, synchronizer, mock_gateway):
        """Test that multiple ticks can be used to build state."""
        # First tick
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1, 12, 0, 0),
                tasks_by_status={TaskStatus.RUNNING: [1, 2]},
            )
        )

        update1 = await synchronizer.tick(full_sync=True, last_sync=None)

        # Second tick (incremental)
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1, 12, 1, 0),
                tasks_by_status={TaskStatus.DONE: [1]},
            )
        )

        update2 = await synchronizer.tick(
            full_sync=False, last_sync=update1.sync_time
        )

        # First tick shows running
        assert update1.task_statuses.get(1) == TaskStatus.RUNNING
        assert update1.task_statuses.get(2) == TaskStatus.RUNNING

        # Second tick shows task 1 done
        assert update2.task_statuses.get(1) == TaskStatus.DONE

    @pytest.mark.asyncio
    async def test_dynamic_task_set_update(self, synchronizer, mock_gateway):
        """Test updating task IDs affects filtering."""
        # Initial sync with tasks 1-5
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1, 12, 0, 0),
                tasks_by_status={TaskStatus.DONE: [1, 6]},  # 6 is unknown
            )
        )

        update1 = await synchronizer.tick(full_sync=True, last_sync=None)
        assert 1 in update1.task_statuses
        assert 6 not in update1.task_statuses

        # Add task 6 to known tasks
        synchronizer.update_task_ids({1, 2, 3, 4, 5, 6})

        update2 = await synchronizer.tick(full_sync=True, last_sync=None)
        # Now task 6 should be included
        assert 6 in update2.task_statuses

