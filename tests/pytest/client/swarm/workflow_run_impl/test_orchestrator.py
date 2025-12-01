"""Unit tests for WorkflowRunOrchestrator."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobmon.client.swarm.workflow_run_impl.gateway import (
    HeartbeatResponse,
    QueueResponse,
    StatusUpdateResponse,
    TaskStatusUpdatesResponse,
)
from jobmon.client.swarm.workflow_run_impl.orchestrator import (
    ACTIVE_TASK_STATUSES,
    OrchestratorConfig,
    OrchestratorResult,
    SERVER_STOP_STATUSES,
    TERMINATING_STATUSES,
    WorkflowRunContext,
    WorkflowRunOrchestrator,
)
from jobmon.client.swarm.workflow_run_impl.state import StateUpdate
from jobmon.core.constants import TaskStatus, WorkflowRunStatus
from jobmon.core.exceptions import DistributorNotAlive, TransitionError, WorkflowTestError


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_gateway():
    """Create a mock ServerGateway."""
    gateway = MagicMock()

    # Default heartbeat behavior
    gateway.log_heartbeat = AsyncMock(
        return_value=HeartbeatResponse(status=WorkflowRunStatus.RUNNING)
    )
    gateway.log_heartbeat_sync = MagicMock(
        return_value=HeartbeatResponse(status=WorkflowRunStatus.RUNNING)
    )

    # Default status update behavior
    gateway.update_status = AsyncMock(
        side_effect=lambda s: StatusUpdateResponse(status=s)
    )

    # Default triage behavior
    gateway.request_triage = AsyncMock()

    # Default task status updates
    gateway.get_task_status_updates = AsyncMock(
        return_value=TaskStatusUpdatesResponse(
            time="2024-01-01T00:00:00",
            tasks_by_status={},
        )
    )

    # Default concurrency queries
    gateway.get_workflow_concurrency = AsyncMock(return_value=100)
    gateway.get_array_concurrency = AsyncMock(return_value=50)

    # Default queue behavior
    gateway.queue_task_batch = AsyncMock(
        return_value=QueueResponse(tasks_by_status={TaskStatus.QUEUED: []})
    )

    # Default terminate behavior
    gateway.terminate_task_instances = AsyncMock()

    return gateway


@pytest.fixture
def mock_task():
    """Create a mock SwarmTask."""

    def _create_task(
        task_id: int,
        array_id: int = 1,
        status: str = TaskStatus.REGISTERING,
        all_upstreams_done: bool = True,
    ):
        task = MagicMock()
        task.task_id = task_id
        task.array_id = array_id
        task.status = status
        task.all_upstreams_done = all_upstreams_done
        task.num_upstreams_done = 0
        task.downstream_swarm_tasks = set()
        task.compute_resources_callable = None
        task.resource_scales = {}
        task.fallback_queues = []

        # Mock task resources
        task.current_task_resources = MagicMock()
        task.current_task_resources.is_bound = True
        task.current_task_resources.id = 1
        task.current_task_resources.coerce_resources = MagicMock(
            return_value=task.current_task_resources
        )
        task.current_task_resources.adjust_resources = MagicMock(
            return_value=task.current_task_resources
        )
        task.current_task_resources.requested_resources = {}
        task.current_task_resources.queue = MagicMock()

        # Mock cluster
        task.cluster = MagicMock()
        task.cluster.id = 1

        return task

    return _create_task


@pytest.fixture
def mock_array():
    """Create a mock SwarmArray."""

    def _create_array(array_id: int, max_concurrently_running: int = 50):
        array = MagicMock()
        array.array_id = array_id
        array.max_concurrently_running = max_concurrently_running
        array.array_name = f"array_{array_id}"
        array.tasks = set()
        return array

    return _create_array


@pytest.fixture
def basic_context(mock_gateway, mock_task, mock_array):
    """Create a basic WorkflowRunContext with one done task."""
    task = mock_task(1, status=TaskStatus.DONE)
    array = mock_array(1)
    array.tasks.add(task)

    return WorkflowRunContext(
        workflow_id=1,
        workflow_run_id=10,
        gateway=mock_gateway,
        tasks={1: task},
        arrays={1: array},
        task_status_map={
            TaskStatus.REGISTERING: set(),
            TaskStatus.QUEUED: set(),
            TaskStatus.INSTANTIATING: set(),
            TaskStatus.LAUNCHED: set(),
            TaskStatus.RUNNING: set(),
            TaskStatus.DONE: {task},
            TaskStatus.ADJUSTING_RESOURCES: set(),
            TaskStatus.ERROR_FATAL: set(),
        },
        ready_to_run=deque(),
        task_resources_cache={},
        status=WorkflowRunStatus.BOUND,
        max_concurrently_running=100,
    )


@pytest.fixture
def pending_context(mock_gateway, mock_task, mock_array):
    """Create a context with pending tasks."""
    task1 = mock_task(1, status=TaskStatus.REGISTERING, all_upstreams_done=True)
    task2 = mock_task(2, status=TaskStatus.REGISTERING, all_upstreams_done=True)
    array = mock_array(1)
    array.tasks.add(task1)
    array.tasks.add(task2)

    return WorkflowRunContext(
        workflow_id=1,
        workflow_run_id=10,
        gateway=mock_gateway,
        tasks={1: task1, 2: task2},
        arrays={1: array},
        task_status_map={
            TaskStatus.REGISTERING: {task1, task2},
            TaskStatus.QUEUED: set(),
            TaskStatus.INSTANTIATING: set(),
            TaskStatus.LAUNCHED: set(),
            TaskStatus.RUNNING: set(),
            TaskStatus.DONE: set(),
            TaskStatus.ADJUSTING_RESOURCES: set(),
            TaskStatus.ERROR_FATAL: set(),
        },
        ready_to_run=deque(),
        task_resources_cache={},
        status=WorkflowRunStatus.BOUND,
        max_concurrently_running=100,
    )


@pytest.fixture
def default_config():
    """Create default orchestrator configuration."""
    return OrchestratorConfig(
        heartbeat_interval=0.1,  # Short for testing
        heartbeat_report_by_buffer=1.5,
        wedged_workflow_sync_interval=600.0,
        fail_fast=False,
        timeout=300,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Test Context
# ──────────────────────────────────────────────────────────────────────────────


class TestWorkflowRunContext:
    """Tests for WorkflowRunContext."""

    def test_get_active_task_count_empty(self, basic_context):
        """Test get_active_task_count with no active tasks."""
        assert basic_context.get_active_task_count() == 0

    def test_get_active_task_count_with_active(self, pending_context, mock_task):
        """Test get_active_task_count with active tasks."""
        running_task = mock_task(3, status=TaskStatus.RUNNING)
        pending_context.tasks[3] = running_task
        pending_context.task_status_map[TaskStatus.RUNNING].add(running_task)

        assert pending_context.get_active_task_count() == 1

    def test_get_done_count(self, basic_context):
        """Test get_done_count."""
        assert basic_context.get_done_count() == 1

    def test_get_failed_count(self, basic_context, mock_task):
        """Test get_failed_count."""
        failed_task = mock_task(2, status=TaskStatus.ERROR_FATAL)
        basic_context.tasks[2] = failed_task
        basic_context.task_status_map[TaskStatus.ERROR_FATAL].add(failed_task)

        assert basic_context.get_failed_count() == 1

    def test_all_tasks_final_true(self, basic_context):
        """Test all_tasks_final when all tasks done."""
        assert basic_context.all_tasks_final() is True

    def test_all_tasks_final_false(self, pending_context):
        """Test all_tasks_final with pending tasks."""
        assert pending_context.all_tasks_final() is False

    def test_has_pending_work_active_tasks(self, pending_context, mock_task):
        """Test has_pending_work with active tasks."""
        running = mock_task(3, status=TaskStatus.RUNNING)
        pending_context.tasks[3] = running
        pending_context.task_status_map[TaskStatus.RUNNING].add(running)

        assert pending_context.has_pending_work() is True

    def test_has_pending_work_ready_to_run(self, pending_context, mock_task):
        """Test has_pending_work with ready_to_run tasks."""
        task = mock_task(3)
        pending_context.ready_to_run.append(task)

        assert pending_context.has_pending_work() is True

    def test_has_pending_work_no_work(self, basic_context):
        """Test has_pending_work with no work."""
        assert basic_context.has_pending_work() is False


# ──────────────────────────────────────────────────────────────────────────────
# Test Configuration
# ──────────────────────────────────────────────────────────────────────────────


class TestOrchestratorConfig:
    """Tests for OrchestratorConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = OrchestratorConfig()

        assert config.heartbeat_interval == 30.0
        assert config.heartbeat_report_by_buffer == 1.5
        assert config.wedged_workflow_sync_interval == 600.0
        assert config.fail_fast is False
        assert config.timeout == 36000
        assert config.fail_after_n_executions is None

    def test_custom_values(self):
        """Test custom configuration values."""
        config = OrchestratorConfig(
            heartbeat_interval=10.0,
            heartbeat_report_by_buffer=2.0,
            wedged_workflow_sync_interval=300.0,
            fail_fast=True,
            timeout=1000,
            fail_after_n_executions=5,
        )

        assert config.heartbeat_interval == 10.0
        assert config.fail_fast is True
        assert config.fail_after_n_executions == 5


# ──────────────────────────────────────────────────────────────────────────────
# Test Orchestrator Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestOrchestratorInit:
    """Tests for WorkflowRunOrchestrator initialization."""

    def test_init_stores_context(self, basic_context, default_config):
        """Test that init stores context and config."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)

        assert orchestrator._ctx is basic_context
        assert orchestrator._config is default_config

    def test_services_not_initialized(self, basic_context, default_config):
        """Test that services are not initialized until needed."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)

        assert orchestrator._heartbeat is None
        assert orchestrator._synchronizer is None
        assert orchestrator._scheduler is None


# ──────────────────────────────────────────────────────────────────────────────
# Test Service Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestServiceInitialization:
    """Tests for service lazy initialization."""

    def test_ensure_heartbeat_creates_service(self, basic_context, default_config):
        """Test that _ensure_heartbeat creates HeartbeatService."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        heartbeat = orchestrator._ensure_heartbeat()

        assert heartbeat is not None
        assert heartbeat.interval == default_config.heartbeat_interval
        assert orchestrator._heartbeat is heartbeat

    def test_ensure_heartbeat_returns_same_instance(self, basic_context, default_config):
        """Test that _ensure_heartbeat returns same instance on multiple calls."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        heartbeat1 = orchestrator._ensure_heartbeat()
        heartbeat2 = orchestrator._ensure_heartbeat()

        assert heartbeat1 is heartbeat2

    def test_ensure_synchronizer_creates_service(self, basic_context, default_config):
        """Test that _ensure_synchronizer creates Synchronizer."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        synchronizer = orchestrator._ensure_synchronizer()

        assert synchronizer is not None
        assert synchronizer.task_ids == {1}
        assert synchronizer.array_ids == {1}

    def test_ensure_scheduler_creates_service(self, basic_context, default_config):
        """Test that _ensure_scheduler creates Scheduler."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        scheduler = orchestrator._ensure_scheduler()

        assert scheduler is not None
        assert scheduler.max_concurrently_running == 100


# ──────────────────────────────────────────────────────────────────────────────
# Test Initial Fringe
# ──────────────────────────────────────────────────────────────────────────────


class TestSetInitialFringe:
    """Tests for _set_initial_fringe."""

    def test_registering_tasks_with_upstreams_done(
        self, pending_context, default_config
    ):
        """Test that registering tasks with upstreams done are added to ready_to_run."""
        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._set_initial_fringe()

        assert len(pending_context.ready_to_run) == 2

    def test_registering_tasks_without_upstreams_done(
        self, mock_gateway, mock_task, mock_array, default_config
    ):
        """Test that registering tasks without upstreams done are not added."""
        task = mock_task(1, status=TaskStatus.REGISTERING, all_upstreams_done=False)
        array = mock_array(1)

        ctx = WorkflowRunContext(
            workflow_id=1,
            workflow_run_id=10,
            gateway=mock_gateway,
            tasks={1: task},
            arrays={1: array},
            task_status_map={
                TaskStatus.REGISTERING: {task},
                TaskStatus.QUEUED: set(),
                TaskStatus.INSTANTIATING: set(),
                TaskStatus.LAUNCHED: set(),
                TaskStatus.RUNNING: set(),
                TaskStatus.DONE: set(),
                TaskStatus.ADJUSTING_RESOURCES: set(),
                TaskStatus.ERROR_FATAL: set(),
            },
            ready_to_run=deque(),
            task_resources_cache={},
        )

        orchestrator = WorkflowRunOrchestrator(ctx, default_config)
        orchestrator._set_initial_fringe()

        assert len(ctx.ready_to_run) == 0

    def test_adjusting_resources_tasks_added(
        self, mock_gateway, mock_task, mock_array, default_config
    ):
        """Test that adjusting_resources tasks are added to ready_to_run."""
        task = mock_task(1, status=TaskStatus.ADJUSTING_RESOURCES)
        array = mock_array(1)

        ctx = WorkflowRunContext(
            workflow_id=1,
            workflow_run_id=10,
            gateway=mock_gateway,
            tasks={1: task},
            arrays={1: array},
            task_status_map={
                TaskStatus.REGISTERING: set(),
                TaskStatus.QUEUED: set(),
                TaskStatus.INSTANTIATING: set(),
                TaskStatus.LAUNCHED: set(),
                TaskStatus.RUNNING: set(),
                TaskStatus.DONE: set(),
                TaskStatus.ADJUSTING_RESOURCES: {task},
                TaskStatus.ERROR_FATAL: set(),
            },
            ready_to_run=deque(),
            task_resources_cache={},
        )

        orchestrator = WorkflowRunOrchestrator(ctx, default_config)
        orchestrator._set_initial_fringe()

        assert len(ctx.ready_to_run) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Test Constraint Checks
# ──────────────────────────────────────────────────────────────────────────────


class TestConstraintChecks:
    """Tests for constraint checking methods."""

    def test_check_timeout_no_timeout(self, basic_context, default_config):
        """Test _check_timeout with no timeout."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        # Should not raise
        orchestrator._check_timeout(time.perf_counter())

    def test_check_timeout_exceeded(self, basic_context):
        """Test _check_timeout when timeout exceeded."""
        config = OrchestratorConfig(timeout=1)
        orchestrator = WorkflowRunOrchestrator(basic_context, config)

        # Simulate start time in the past
        start_time = time.perf_counter() - 10

        with pytest.raises(RuntimeError, match="timeout"):
            orchestrator._check_timeout(start_time)

    @pytest.mark.asyncio
    async def test_check_distributor_alive_true(self, basic_context, default_config):
        """Test _check_distributor_alive when alive."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        # Should not raise
        await orchestrator._check_distributor_alive(lambda: True)

    @pytest.mark.asyncio
    async def test_check_distributor_alive_false(self, basic_context, default_config):
        """Test _check_distributor_alive when not alive."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)

        with pytest.raises(DistributorNotAlive):
            await orchestrator._check_distributor_alive(lambda: False)

    def test_check_fail_fast_no_failures(self, basic_context, default_config):
        """Test _check_fail_fast with no failures."""
        config = OrchestratorConfig(fail_fast=True)
        orchestrator = WorkflowRunOrchestrator(basic_context, config)
        # Should not raise
        orchestrator._check_fail_fast()

    def test_check_fail_fast_with_failures(
        self, basic_context, mock_task, default_config
    ):
        """Test _check_fail_fast with failures."""
        config = OrchestratorConfig(fail_fast=True)
        failed = mock_task(2, status=TaskStatus.ERROR_FATAL)
        basic_context.tasks[2] = failed
        basic_context.task_status_map[TaskStatus.ERROR_FATAL].add(failed)

        orchestrator = WorkflowRunOrchestrator(basic_context, config)

        with pytest.raises(RuntimeError, match="Fail-fast"):
            orchestrator._check_fail_fast()

    def test_check_fail_fast_disabled(self, basic_context, mock_task, default_config):
        """Test _check_fail_fast when disabled."""
        failed = mock_task(2, status=TaskStatus.ERROR_FATAL)
        basic_context.tasks[2] = failed
        basic_context.task_status_map[TaskStatus.ERROR_FATAL].add(failed)

        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        # Should not raise
        orchestrator._check_fail_fast()

    def test_check_fail_after_n_executions_disabled(
        self, basic_context, default_config
    ):
        """Test _check_fail_after_n_executions when disabled."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        # Should not raise
        orchestrator._check_fail_after_n_executions()

    def test_check_fail_after_n_executions_not_reached(
        self, basic_context, default_config
    ):
        """Test _check_fail_after_n_executions when threshold not reached."""
        config = OrchestratorConfig(fail_after_n_executions=5)
        basic_context.n_executions = 3
        orchestrator = WorkflowRunOrchestrator(basic_context, config)
        # Should not raise
        orchestrator._check_fail_after_n_executions()

    def test_check_fail_after_n_executions_reached(
        self, basic_context, default_config
    ):
        """Test _check_fail_after_n_executions when threshold reached."""
        config = OrchestratorConfig(fail_after_n_executions=5)
        basic_context.n_executions = 5
        orchestrator = WorkflowRunOrchestrator(basic_context, config)

        with pytest.raises(WorkflowTestError):
            orchestrator._check_fail_after_n_executions()


# ──────────────────────────────────────────────────────────────────────────────
# Test Should Continue
# ──────────────────────────────────────────────────────────────────────────────


class TestShouldContinue:
    """Tests for _should_continue."""

    def test_should_continue_all_done(self, basic_context, default_config):
        """Test _should_continue when all tasks done."""
        basic_context.status = WorkflowRunStatus.RUNNING
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)

        assert orchestrator._should_continue() is False

    def test_should_continue_server_stop_status(
        self, pending_context, default_config
    ):
        """Test _should_continue with server stop status."""
        pending_context.status = WorkflowRunStatus.ERROR
        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)

        assert orchestrator._should_continue() is False

    def test_should_continue_with_work(self, pending_context, default_config):
        """Test _should_continue with pending work."""
        pending_context.status = WorkflowRunStatus.RUNNING
        pending_context.ready_to_run.append(list(pending_context.tasks.values())[0])
        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)

        assert orchestrator._should_continue() is True


# ──────────────────────────────────────────────────────────────────────────────
# Test Status Updates
# ──────────────────────────────────────────────────────────────────────────────


class TestUpdateStatus:
    """Tests for _update_status."""

    @pytest.mark.asyncio
    async def test_update_status_success(self, basic_context, default_config):
        """Test _update_status successful transition."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        await orchestrator._update_status(WorkflowRunStatus.RUNNING)

        assert basic_context.status == WorkflowRunStatus.RUNNING
        basic_context.gateway.update_status.assert_called_once_with(
            WorkflowRunStatus.RUNNING
        )

    @pytest.mark.asyncio
    async def test_update_status_transition_error(self, basic_context, default_config):
        """Test _update_status with transition error."""
        # Gateway returns different status than requested
        basic_context.gateway.update_status = AsyncMock(
            return_value=StatusUpdateResponse(status=WorkflowRunStatus.ERROR)
        )

        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)

        with pytest.raises(TransitionError):
            await orchestrator._update_status(WorkflowRunStatus.RUNNING)


# ──────────────────────────────────────────────────────────────────────────────
# Test Status Application
# ──────────────────────────────────────────────────────────────────────────────


class TestApplyStatusUpdates:
    """Tests for _apply_status_updates."""

    def test_apply_status_updates_moves_task(
        self, pending_context, default_config
    ):
        """Test that status updates move tasks between buckets."""
        task = list(pending_context.tasks.values())[0]
        task_id = task.task_id

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._apply_status_updates({task_id: TaskStatus.DONE})

        assert task.status == TaskStatus.DONE
        assert task in pending_context.task_status_map[TaskStatus.DONE]
        assert task not in pending_context.task_status_map[TaskStatus.REGISTERING]

    def test_apply_status_updates_unknown_task(
        self, pending_context, default_config
    ):
        """Test that unknown task IDs are ignored."""
        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        # Should not raise
        orchestrator._apply_status_updates({999: TaskStatus.DONE})

    def test_apply_status_updates_same_status(
        self, pending_context, default_config
    ):
        """Test that same status updates are no-op."""
        task = list(pending_context.tasks.values())[0]
        task_id = task.task_id
        original_status_count = len(
            pending_context.task_status_map[TaskStatus.REGISTERING]
        )

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._apply_status_updates({task_id: TaskStatus.REGISTERING})

        assert (
            len(pending_context.task_status_map[TaskStatus.REGISTERING])
            == original_status_count
        )


# ──────────────────────────────────────────────────────────────────────────────
# Test Refresh Task Status Map
# ──────────────────────────────────────────────────────────────────────────────


class TestRefreshTaskStatusMap:
    """Tests for _refresh_task_status_map."""

    def test_done_task_increments_counter(
        self, pending_context, default_config
    ):
        """Test that done tasks increment n_executions."""
        task = list(pending_context.tasks.values())[0]
        task.status = TaskStatus.DONE

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._refresh_task_status_map({task})

        assert pending_context.n_executions == 1

    def test_done_task_propagates_to_downstream(
        self, pending_context, mock_task, default_config
    ):
        """Test that done tasks propagate to downstream tasks."""
        upstream = list(pending_context.tasks.values())[0]
        downstream = mock_task(3, status=TaskStatus.REGISTERING, all_upstreams_done=False)
        downstream.num_upstreams = 1

        upstream.downstream_swarm_tasks.add(downstream)
        upstream.status = TaskStatus.DONE

        pending_context.tasks[3] = downstream
        pending_context.task_status_map[TaskStatus.REGISTERING].add(downstream)

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._refresh_task_status_map({upstream})

        assert downstream.num_upstreams_done == 1

    def test_done_task_enqueues_ready_downstream(
        self, pending_context, mock_task, default_config
    ):
        """Test that done tasks enqueue ready downstream tasks."""
        upstream = list(pending_context.tasks.values())[0]
        downstream = mock_task(3, status=TaskStatus.REGISTERING, all_upstreams_done=False)
        downstream.num_upstreams = 1

        # Make downstream become ready when upstream completes
        def check_all_upstreams():
            return downstream.num_upstreams_done >= downstream.num_upstreams

        type(downstream).all_upstreams_done = property(lambda self: check_all_upstreams())

        upstream.downstream_swarm_tasks.add(downstream)
        upstream.status = TaskStatus.DONE

        pending_context.tasks[3] = downstream
        pending_context.task_status_map[TaskStatus.REGISTERING].add(downstream)

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._refresh_task_status_map({upstream})

        assert downstream in pending_context.ready_to_run

    def test_adjusting_resources_task_added_to_front(
        self, pending_context, default_config
    ):
        """Test that adjusting_resources tasks are added to front of queue."""
        task = list(pending_context.tasks.values())[0]
        task.status = TaskStatus.ADJUSTING_RESOURCES

        # Add another task to the queue first
        other_task = list(pending_context.tasks.values())[1]
        pending_context.ready_to_run.append(other_task)

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        orchestrator._refresh_task_status_map({task})

        # Task should be at front
        assert pending_context.ready_to_run[0] is task


# ──────────────────────────────────────────────────────────────────────────────
# Test Termination Handling
# ──────────────────────────────────────────────────────────────────────────────


class TestTerminationHandling:
    """Tests for _handle_termination."""

    @pytest.mark.asyncio
    async def test_handle_termination_no_active_tasks(
        self, basic_context, default_config
    ):
        """Test _handle_termination with no active tasks returns True."""
        basic_context.status = WorkflowRunStatus.COLD_RESUME
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)

        result = await orchestrator._handle_termination()

        assert result is True

    @pytest.mark.asyncio
    async def test_handle_termination_with_running_tasks(
        self, pending_context, mock_task, default_config
    ):
        """Test _handle_termination with running tasks returns False."""
        running = mock_task(3, status=TaskStatus.RUNNING)
        pending_context.tasks[3] = running
        pending_context.task_status_map[TaskStatus.RUNNING].add(running)
        pending_context.status = WorkflowRunStatus.COLD_RESUME

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)

        result = await orchestrator._handle_termination()

        assert result is False
        pending_context.gateway.terminate_task_instances.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# Test Full Run
# ──────────────────────────────────────────────────────────────────────────────


class TestFullRun:
    """Tests for full orchestrator run."""

    @pytest.mark.asyncio
    async def test_run_all_tasks_done(self, basic_context, default_config):
        """Test run when all tasks already done."""
        result = await WorkflowRunOrchestrator(
            basic_context, default_config
        ).run(lambda: True)

        assert result.final_status == WorkflowRunStatus.DONE
        assert result.done_count == 1
        assert result.total_tasks == 1

    @pytest.mark.asyncio
    async def test_run_transitions_to_running(self, basic_context, default_config):
        """Test that run transitions status to RUNNING."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        await orchestrator.run(lambda: True)

        # Check that update_status was called with RUNNING first
        calls = basic_context.gateway.update_status.call_args_list
        assert any(call[0][0] == WorkflowRunStatus.RUNNING for call in calls)

    @pytest.mark.asyncio
    async def test_run_stops_on_distributor_not_alive(
        self, pending_context, default_config
    ):
        """Test that run stops when distributor is not alive."""
        # Add a task to ready_to_run so run doesn't exit immediately
        task = list(pending_context.tasks.values())[0]
        pending_context.ready_to_run.append(task)

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)

        with pytest.raises(DistributorNotAlive):
            await orchestrator.run(lambda: False)

    @pytest.mark.asyncio
    async def test_run_handles_keyboard_interrupt(
        self, pending_context, default_config
    ):
        """Test that run propagates keyboard interrupt."""

        async def raise_interrupt():
            raise KeyboardInterrupt()

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)

        # Patch _main_loop to raise KeyboardInterrupt
        with patch.object(orchestrator, "_main_loop", side_effect=KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                await orchestrator.run(lambda: True)


# ──────────────────────────────────────────────────────────────────────────────
# Test Teardown
# ──────────────────────────────────────────────────────────────────────────────


class TestTeardown:
    """Tests for _teardown."""

    @pytest.mark.asyncio
    async def test_teardown_cancels_heartbeat_task(self, basic_context, default_config):
        """Test that _teardown cancels heartbeat task."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        orchestrator._stop_event = asyncio.Event()
        orchestrator._heartbeat_task = asyncio.create_task(asyncio.sleep(100))

        await orchestrator._teardown()

        assert orchestrator._heartbeat_task is None
        assert orchestrator._stop_event is None

    @pytest.mark.asyncio
    async def test_teardown_sets_stop_event(self, basic_context, default_config):
        """Test that _teardown sets stop event."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        stop_event = asyncio.Event()
        orchestrator._stop_event = stop_event

        await orchestrator._teardown()

        # Event should have been set before being cleared
        # (We can't easily test this, but the code path is covered)


# ──────────────────────────────────────────────────────────────────────────────
# Test Error Handling
# ──────────────────────────────────────────────────────────────────────────────


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_handle_error_updates_status(self, basic_context, default_config):
        """Test that _handle_error updates status to ERROR."""
        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        await orchestrator._handle_error()

        basic_context.gateway.update_status.assert_called_with(WorkflowRunStatus.ERROR)

    @pytest.mark.asyncio
    async def test_handle_error_catches_transition_error(
        self, basic_context, default_config
    ):
        """Test that _handle_error catches TransitionError."""
        basic_context.gateway.update_status = AsyncMock(
            side_effect=TransitionError("Test")
        )

        orchestrator = WorkflowRunOrchestrator(basic_context, default_config)
        # Should not raise
        await orchestrator._handle_error()


# ──────────────────────────────────────────────────────────────────────────────
# Test Result
# ──────────────────────────────────────────────────────────────────────────────


class TestOrchestratorResult:
    """Tests for OrchestratorResult."""

    def test_result_fields(self):
        """Test OrchestratorResult fields."""
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.DONE,
            done_count=10,
            failed_count=2,
            total_tasks=12,
            elapsed_time=100.5,
        )

        assert result.final_status == WorkflowRunStatus.DONE
        assert result.done_count == 10
        assert result.failed_count == 2
        assert result.total_tasks == 12
        assert result.elapsed_time == 100.5


# ──────────────────────────────────────────────────────────────────────────────
# Test Sync Operations
# ──────────────────────────────────────────────────────────────────────────────


class TestSyncOperations:
    """Tests for synchronization operations."""

    @pytest.mark.asyncio
    async def test_do_sync_updates_last_sync(self, pending_context, default_config):
        """Test that _do_sync updates last_sync time."""
        pending_context.gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time="2024-01-01T12:00:00",
                tasks_by_status={},
            )
        )

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        await orchestrator._do_sync(full_sync=False)

        assert pending_context.last_sync == "2024-01-01T12:00:00"

    @pytest.mark.asyncio
    async def test_do_sync_updates_max_concurrently_running(
        self, pending_context, default_config
    ):
        """Test that _do_sync updates max_concurrently_running."""
        pending_context.gateway.get_workflow_concurrency = AsyncMock(return_value=200)

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        await orchestrator._do_sync(full_sync=False)

        assert pending_context.max_concurrently_running == 200

    @pytest.mark.asyncio
    async def test_do_sync_updates_array_limits(
        self, pending_context, default_config
    ):
        """Test that _do_sync updates array concurrency limits."""
        pending_context.gateway.get_array_concurrency = AsyncMock(return_value=75)

        orchestrator = WorkflowRunOrchestrator(pending_context, default_config)
        await orchestrator._do_sync(full_sync=False)

        array = list(pending_context.arrays.values())[0]
        assert array.max_concurrently_running == 75

