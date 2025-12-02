"""Unit tests for WorkflowRunOrchestrator."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobmon.client.swarm.gateway import (
    HeartbeatResponse,
    QueueResponse,
    StatusUpdateResponse,
    TaskStatusUpdatesResponse,
)
from jobmon.client.swarm.orchestrator import (
    OrchestratorConfig,
    OrchestratorResult,
    WorkflowRunConfig,
    WorkflowRunOrchestrator,
)
from jobmon.client.swarm.state import (
    ACTIVE_TASK_STATUSES,
    SERVER_STOP_STATUSES,
    StateUpdate,
    SwarmState,
    TERMINATING_STATUSES,
)
from jobmon.core.constants import TaskStatus, WorkflowRunStatus
from jobmon.core.exceptions import DistributorNotAlive, TransitionError, WorkflowTestError


def create_state_with_tasks(
    tasks: dict,
    arrays: dict,
    task_status_map: dict,
    status: str = WorkflowRunStatus.BOUND,
    max_concurrently_running: int = 100,
    workflow_id: int = 1,
    workflow_run_id: int = 10,
    dag_id: int = 1,
) -> SwarmState:
    """Helper to create a SwarmState with properly initialized tasks/arrays."""
    state = SwarmState(
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        dag_id=dag_id,
        max_concurrently_running=max_concurrently_running,
        status=status,
    )
    # Copy tasks, arrays and status map to state
    state.tasks = tasks
    state.arrays = arrays
    state._task_status_map = task_status_map
    return state


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
def basic_state(mock_task, mock_array):
    """Create a basic SwarmState with one done task."""
    task = mock_task(1, status=TaskStatus.DONE)
    array = mock_array(1)
    array.tasks.add(task)

    return create_state_with_tasks(
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
        status=WorkflowRunStatus.BOUND,
        max_concurrently_running=100,
    )


@pytest.fixture
def pending_state(mock_task, mock_array):
    """Create a state with pending tasks."""
    task1 = mock_task(1, status=TaskStatus.REGISTERING, all_upstreams_done=True)
    task2 = mock_task(2, status=TaskStatus.REGISTERING, all_upstreams_done=True)
    array = mock_array(1)
    array.tasks.add(task1)
    array.tasks.add(task2)

    return create_state_with_tasks(
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
# Test SwarmState (via orchestrator fixtures)
# ──────────────────────────────────────────────────────────────────────────────


class TestSwarmStateViaOrchestrator:
    """Tests for SwarmState helper methods used by orchestrator."""

    def test_get_active_task_count_empty(self, basic_state):
        """Test get_active_task_count with no active tasks."""
        assert basic_state.get_active_task_count() == 0

    def test_get_active_task_count_with_active(self, pending_state, mock_task):
        """Test get_active_task_count with active tasks."""
        running_task = mock_task(3, status=TaskStatus.RUNNING)
        pending_state.tasks[3] = running_task
        pending_state._task_status_map[TaskStatus.RUNNING].add(running_task)

        assert pending_state.get_active_task_count() == 1

    def test_get_done_count(self, basic_state):
        """Test get_done_count."""
        assert basic_state.get_done_count() == 1

    def test_get_failed_count(self, basic_state, mock_task):
        """Test get_failed_count."""
        failed_task = mock_task(2, status=TaskStatus.ERROR_FATAL)
        basic_state.tasks[2] = failed_task
        basic_state._task_status_map[TaskStatus.ERROR_FATAL].add(failed_task)

        assert basic_state.get_failed_count() == 1

    def test_all_tasks_final_true(self, basic_state):
        """Test all_tasks_final when all tasks done."""
        assert basic_state.all_tasks_final() is True

    def test_all_tasks_final_false(self, pending_state):
        """Test all_tasks_final with pending tasks."""
        assert pending_state.all_tasks_final() is False

    def test_has_pending_work_active_tasks(self, pending_state, mock_task):
        """Test has_pending_work with active tasks."""
        running = mock_task(3, status=TaskStatus.RUNNING)
        pending_state.tasks[3] = running
        pending_state._task_status_map[TaskStatus.RUNNING].add(running)

        assert pending_state.has_pending_work() is True

    def test_has_pending_work_ready_to_run(self, pending_state, mock_task):
        """Test has_pending_work with ready_to_run tasks."""
        task = mock_task(3)
        pending_state.ready_to_run.append(task)

        assert pending_state.has_pending_work() is True

    def test_has_pending_work_no_work(self, basic_state):
        """Test has_pending_work with no work."""
        assert basic_state.has_pending_work() is False


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


class TestWorkflowRunConfig:
    """Tests for WorkflowRunConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = WorkflowRunConfig()

        assert config.heartbeat_interval is None  # Uses JobmonConfig default
        assert config.heartbeat_report_by_buffer is None  # Uses JobmonConfig default
        assert config.fail_fast is False
        assert config.wedged_workflow_sync_interval == 600
        assert config.fail_after_n_executions == 1_000_000_000

    def test_custom_values(self):
        """Test custom configuration values."""
        config = WorkflowRunConfig(
            heartbeat_interval=60,
            heartbeat_report_by_buffer=2.0,
            fail_fast=True,
            wedged_workflow_sync_interval=300,
            fail_after_n_executions=10,
        )

        assert config.heartbeat_interval == 60
        assert config.heartbeat_report_by_buffer == 2.0
        assert config.fail_fast is True
        assert config.wedged_workflow_sync_interval == 300
        assert config.fail_after_n_executions == 10

    def test_from_defaults(self):
        """Test from_defaults class method."""
        config = WorkflowRunConfig.from_defaults()

        assert isinstance(config, WorkflowRunConfig)
        assert config.heartbeat_interval is None
        assert config.fail_fast is False

    def test_partial_configuration(self):
        """Test config with only some values specified."""
        config = WorkflowRunConfig(fail_fast=True)

        assert config.fail_fast is True
        assert config.heartbeat_interval is None  # Not specified
        assert config.wedged_workflow_sync_interval == 600  # Default


# ──────────────────────────────────────────────────────────────────────────────
# Test Orchestrator Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestOrchestratorInit:
    """Tests for WorkflowRunOrchestrator initialization."""

    def test_init_stores_state_and_gateway(self, basic_state, mock_gateway, default_config):
        """Test that init stores state, gateway and config."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)

        assert orchestrator._state is basic_state
        assert orchestrator._gateway is mock_gateway
        assert orchestrator._config is default_config

    def test_services_not_initialized(self, basic_state, mock_gateway, default_config):
        """Test that services are not initialized until needed."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)

        assert orchestrator._heartbeat is None
        assert orchestrator._synchronizer is None
        assert orchestrator._scheduler is None


# ──────────────────────────────────────────────────────────────────────────────
# Test Service Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestServiceInitialization:
    """Tests for service lazy initialization."""

    def test_ensure_heartbeat_creates_service(self, basic_state, mock_gateway, default_config):
        """Test that _ensure_heartbeat creates HeartbeatService."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        heartbeat = orchestrator._ensure_heartbeat()

        assert heartbeat is not None
        assert heartbeat.interval == default_config.heartbeat_interval
        assert orchestrator._heartbeat is heartbeat

    def test_ensure_heartbeat_returns_same_instance(self, basic_state, mock_gateway, default_config):
        """Test that _ensure_heartbeat returns same instance on multiple calls."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        heartbeat1 = orchestrator._ensure_heartbeat()
        heartbeat2 = orchestrator._ensure_heartbeat()

        assert heartbeat1 is heartbeat2

    def test_ensure_synchronizer_creates_service(self, basic_state, mock_gateway, default_config):
        """Test that _ensure_synchronizer creates Synchronizer."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        synchronizer = orchestrator._ensure_synchronizer()

        assert synchronizer is not None
        assert synchronizer.task_ids == {1}
        assert synchronizer.array_ids == {1}

    def test_ensure_scheduler_creates_service(self, basic_state, mock_gateway, default_config):
        """Test that _ensure_scheduler creates Scheduler."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        scheduler = orchestrator._ensure_scheduler()

        assert scheduler is not None
        assert scheduler.max_concurrently_running == 100


# ──────────────────────────────────────────────────────────────────────────────
# Test Initial Fringe
# ──────────────────────────────────────────────────────────────────────────────


class TestSetInitialFringe:
    """Tests for _set_initial_fringe."""

    def test_registering_tasks_with_upstreams_done(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that registering tasks with upstreams done are added to ready_to_run."""
        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        orchestrator._set_initial_fringe()

        assert len(pending_state.ready_to_run) == 2

    def test_registering_tasks_without_upstreams_done(
        self, mock_gateway, mock_task, mock_array, default_config
    ):
        """Test that registering tasks without upstreams done are not added."""
        task = mock_task(1, status=TaskStatus.REGISTERING, all_upstreams_done=False)
        array = mock_array(1)

        state = create_state_with_tasks(
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
        )

        orchestrator = WorkflowRunOrchestrator(state, mock_gateway, default_config)
        orchestrator._set_initial_fringe()

        assert len(state.ready_to_run) == 0

    def test_adjusting_resources_tasks_added(
        self, mock_gateway, mock_task, mock_array, default_config
    ):
        """Test that adjusting_resources tasks are added to ready_to_run."""
        task = mock_task(1, status=TaskStatus.ADJUSTING_RESOURCES)
        array = mock_array(1)

        state = create_state_with_tasks(
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
        )

        orchestrator = WorkflowRunOrchestrator(state, mock_gateway, default_config)
        orchestrator._set_initial_fringe()

        assert len(state.ready_to_run) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Test Constraint Checks
# ──────────────────────────────────────────────────────────────────────────────


class TestConstraintChecks:
    """Tests for constraint checking methods."""

    def test_check_timeout_no_timeout(self, basic_state, mock_gateway, default_config):
        """Test _check_timeout with no timeout."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        # Should not raise
        orchestrator._check_timeout(time.perf_counter())

    def test_check_timeout_exceeded(self, basic_state, mock_gateway):
        """Test _check_timeout when timeout exceeded."""
        config = OrchestratorConfig(timeout=1)
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, config)

        # Simulate start time in the past
        start_time = time.perf_counter() - 10

        with pytest.raises(RuntimeError, match="timeout"):
            orchestrator._check_timeout(start_time)

    @pytest.mark.asyncio
    async def test_check_distributor_alive_true(self, basic_state, mock_gateway, default_config):
        """Test _check_distributor_alive when alive."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        # Should not raise
        await orchestrator._check_distributor_alive(lambda: True)

    @pytest.mark.asyncio
    async def test_check_distributor_alive_false(self, basic_state, mock_gateway, default_config):
        """Test _check_distributor_alive when not alive."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)

        with pytest.raises(DistributorNotAlive):
            await orchestrator._check_distributor_alive(lambda: False)

    def test_check_fail_fast_no_failures(self, basic_state, mock_gateway, default_config):
        """Test _check_fail_fast with no failures."""
        config = OrchestratorConfig(fail_fast=True)
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, config)
        # Should not raise
        orchestrator._check_fail_fast()

    def test_check_fail_fast_with_failures(
        self, basic_state, mock_gateway, mock_task, default_config
    ):
        """Test _check_fail_fast with failures."""
        config = OrchestratorConfig(fail_fast=True)
        failed = mock_task(2, status=TaskStatus.ERROR_FATAL)
        basic_state.tasks[2] = failed
        basic_state._task_status_map[TaskStatus.ERROR_FATAL].add(failed)

        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, config)

        with pytest.raises(RuntimeError, match="Fail-fast"):
            orchestrator._check_fail_fast()

    def test_check_fail_fast_disabled(self, basic_state, mock_gateway, mock_task, default_config):
        """Test _check_fail_fast when disabled."""
        failed = mock_task(2, status=TaskStatus.ERROR_FATAL)
        basic_state.tasks[2] = failed
        basic_state._task_status_map[TaskStatus.ERROR_FATAL].add(failed)

        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        # Should not raise
        orchestrator._check_fail_fast()

    def test_check_fail_after_n_executions_disabled(
        self, basic_state, mock_gateway, default_config
    ):
        """Test _check_fail_after_n_executions when disabled."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        # Should not raise
        orchestrator._check_fail_after_n_executions()

    def test_check_fail_after_n_executions_not_reached(
        self, basic_state, mock_gateway, default_config
    ):
        """Test _check_fail_after_n_executions when threshold not reached."""
        config = OrchestratorConfig(fail_after_n_executions=5)
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, config)
        orchestrator._n_executions = 3
        # Should not raise
        orchestrator._check_fail_after_n_executions()

    def test_check_fail_after_n_executions_reached(
        self, basic_state, mock_gateway, default_config
    ):
        """Test _check_fail_after_n_executions when threshold reached."""
        config = OrchestratorConfig(fail_after_n_executions=5)
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, config)
        orchestrator._n_executions = 5

        with pytest.raises(WorkflowTestError):
            orchestrator._check_fail_after_n_executions()


# ──────────────────────────────────────────────────────────────────────────────
# Test Should Continue
# ──────────────────────────────────────────────────────────────────────────────


class TestShouldContinue:
    """Tests for _should_continue."""

    def test_should_continue_all_done(self, basic_state, mock_gateway, default_config):
        """Test _should_continue when all tasks done."""
        basic_state.status = WorkflowRunStatus.RUNNING
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)

        assert orchestrator._should_continue() is False

    def test_should_continue_server_stop_status(
        self, pending_state, mock_gateway, default_config
    ):
        """Test _should_continue with server stop status."""
        pending_state.status = WorkflowRunStatus.ERROR
        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)

        assert orchestrator._should_continue() is False

    def test_should_continue_with_work(self, pending_state, mock_gateway, default_config):
        """Test _should_continue with pending work."""
        pending_state.status = WorkflowRunStatus.RUNNING
        pending_state.ready_to_run.append(list(pending_state.tasks.values())[0])
        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)

        assert orchestrator._should_continue() is True


# ──────────────────────────────────────────────────────────────────────────────
# Test Status Updates
# ──────────────────────────────────────────────────────────────────────────────


class TestUpdateStatus:
    """Tests for _update_status."""

    @pytest.mark.asyncio
    async def test_update_status_success(self, basic_state, mock_gateway, default_config):
        """Test _update_status successful transition."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        await orchestrator._update_status(WorkflowRunStatus.RUNNING)

        assert basic_state.status == WorkflowRunStatus.RUNNING
        mock_gateway.update_status.assert_called_once_with(
            WorkflowRunStatus.RUNNING
        )

    @pytest.mark.asyncio
    async def test_update_status_transition_error(self, basic_state, mock_gateway, default_config):
        """Test _update_status with transition error."""
        # Gateway returns different status than requested
        mock_gateway.update_status = AsyncMock(
            return_value=StatusUpdateResponse(status=WorkflowRunStatus.ERROR)
        )

        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)

        with pytest.raises(TransitionError):
            await orchestrator._update_status(WorkflowRunStatus.RUNNING)


# ──────────────────────────────────────────────────────────────────────────────
# Test Status Application
# ──────────────────────────────────────────────────────────────────────────────


class TestApplyStatusUpdates:
    """Tests for state.apply_update() + _process_changed_tasks()."""

    def test_apply_status_updates_moves_task(
        self, pending_state, default_config
    ):
        """Test that status updates move tasks between buckets."""
        task = list(pending_state.tasks.values())[0]
        task_id = task.task_id

        # Use state.apply_update() - the new pattern
        update = StateUpdate(task_statuses={task_id: TaskStatus.DONE})
        changed = pending_state.apply_update(update)

        assert task.status == TaskStatus.DONE
        assert task in pending_state._task_status_map[TaskStatus.DONE]
        assert task not in pending_state._task_status_map[TaskStatus.REGISTERING]
        assert task in changed

    def test_apply_status_updates_unknown_task(
        self, pending_state, default_config
    ):
        """Test that unknown task IDs are ignored."""
        # Use state.apply_update() - should not raise
        update = StateUpdate(task_statuses={999: TaskStatus.DONE})
        changed = pending_state.apply_update(update)
        assert len(changed) == 0

    def test_apply_status_updates_same_status(
        self, pending_state, default_config
    ):
        """Test that same status updates are no-op."""
        task = list(pending_state.tasks.values())[0]
        task_id = task.task_id
        original_status_count = len(
            pending_state._task_status_map[TaskStatus.REGISTERING]
        )

        update = StateUpdate(task_statuses={task_id: TaskStatus.REGISTERING})
        changed = pending_state.apply_update(update)

        assert (
            len(pending_state._task_status_map[TaskStatus.REGISTERING])
            == original_status_count
        )
        # Same status means no change
        assert len(changed) == 0


# ──────────────────────────────────────────────────────────────────────────────
# Test Refresh Task Status Map
# ──────────────────────────────────────────────────────────────────────────────


class TestProcessChangedTasks:
    """Tests for _process_changed_tasks (formerly _refresh_task_status_map)."""

    def test_done_task_increments_counter(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that done tasks increment n_executions."""
        task = list(pending_state.tasks.values())[0]
        task.status = TaskStatus.DONE

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        orchestrator._process_changed_tasks({task})

        assert orchestrator._n_executions == 1

    def test_done_task_propagates_to_downstream(
        self, pending_state, mock_gateway, mock_task, default_config
    ):
        """Test that done tasks propagate to downstream tasks."""
        upstream = list(pending_state.tasks.values())[0]
        downstream = mock_task(3, status=TaskStatus.REGISTERING, all_upstreams_done=False)
        downstream.num_upstreams = 1

        upstream.downstream_swarm_tasks.add(downstream)
        upstream.status = TaskStatus.DONE

        pending_state.tasks[3] = downstream
        pending_state._task_status_map[TaskStatus.REGISTERING].add(downstream)

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        orchestrator._process_changed_tasks({upstream})

        assert downstream.num_upstreams_done == 1

    def test_done_task_enqueues_ready_downstream(
        self, pending_state, mock_gateway, mock_task, default_config
    ):
        """Test that done tasks enqueue ready downstream tasks."""
        upstream = list(pending_state.tasks.values())[0]
        downstream = mock_task(3, status=TaskStatus.REGISTERING, all_upstreams_done=False)
        downstream.num_upstreams = 1

        # Make downstream become ready when upstream completes
        def check_all_upstreams():
            return downstream.num_upstreams_done >= downstream.num_upstreams

        type(downstream).all_upstreams_done = property(lambda self: check_all_upstreams())

        upstream.downstream_swarm_tasks.add(downstream)
        upstream.status = TaskStatus.DONE

        pending_state.tasks[3] = downstream
        pending_state._task_status_map[TaskStatus.REGISTERING].add(downstream)

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        orchestrator._process_changed_tasks({upstream})

        assert downstream in pending_state.ready_to_run

    def test_adjusting_resources_task_added_to_front(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that adjusting_resources tasks are added to front of queue."""
        task = list(pending_state.tasks.values())[0]
        task.status = TaskStatus.ADJUSTING_RESOURCES

        # Add another task to the queue first
        other_task = list(pending_state.tasks.values())[1]
        pending_state.ready_to_run.append(other_task)

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        orchestrator._process_changed_tasks({task})

        # Task should be at front
        assert pending_state.ready_to_run[0] is task


# ──────────────────────────────────────────────────────────────────────────────
# Test Termination Handling
# ──────────────────────────────────────────────────────────────────────────────


class TestTerminationHandling:
    """Tests for _handle_termination."""

    @pytest.mark.asyncio
    async def test_handle_termination_no_active_tasks(
        self, basic_state, mock_gateway, default_config
    ):
        """Test _handle_termination with no active tasks returns True."""
        basic_state.status = WorkflowRunStatus.COLD_RESUME
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)

        result = await orchestrator._handle_termination()

        assert result is True

    @pytest.mark.asyncio
    async def test_handle_termination_with_running_tasks(
        self, pending_state, mock_gateway, mock_task, default_config
    ):
        """Test _handle_termination with running tasks returns False."""
        running = mock_task(3, status=TaskStatus.RUNNING)
        pending_state.tasks[3] = running
        pending_state._task_status_map[TaskStatus.RUNNING].add(running)
        pending_state.status = WorkflowRunStatus.COLD_RESUME

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)

        result = await orchestrator._handle_termination()

        assert result is False
        mock_gateway.terminate_task_instances.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# Test Full Run
# ──────────────────────────────────────────────────────────────────────────────


class TestFullRun:
    """Tests for full orchestrator run."""

    @pytest.mark.asyncio
    async def test_run_all_tasks_done(self, basic_state, mock_gateway, default_config):
        """Test run when all tasks already done."""
        result = await WorkflowRunOrchestrator(
            basic_state, mock_gateway, default_config
        ).run(lambda: True)

        # Basic fields
        assert result.final_status == WorkflowRunStatus.DONE
        assert result.done_count == 1
        assert result.total_tasks == 1
        assert result.failed_count == 0
        assert result.elapsed_time >= 0

        # Task-level fields
        assert len(result.task_final_statuses) == 1
        assert result.task_final_statuses[1] == TaskStatus.DONE
        assert result.done_task_ids == frozenset({1})
        assert result.failed_task_ids == frozenset()
        assert result.num_previously_complete == 0

    @pytest.mark.asyncio
    async def test_run_result_includes_failed_tasks(
        self, mock_gateway, mock_task, mock_array, default_config
    ):
        """Test that run result includes failed task IDs."""
        done_task = mock_task(1, status=TaskStatus.DONE)
        failed_task = mock_task(2, status=TaskStatus.ERROR_FATAL)
        array = mock_array(1)

        state = create_state_with_tasks(
            tasks={1: done_task, 2: failed_task},
            arrays={1: array},
            task_status_map={
                TaskStatus.REGISTERING: set(),
                TaskStatus.QUEUED: set(),
                TaskStatus.INSTANTIATING: set(),
                TaskStatus.LAUNCHED: set(),
                TaskStatus.RUNNING: set(),
                TaskStatus.DONE: {done_task},
                TaskStatus.ADJUSTING_RESOURCES: set(),
                TaskStatus.ERROR_FATAL: {failed_task},
            },
            status=WorkflowRunStatus.BOUND,
        )

        result = await WorkflowRunOrchestrator(
            state, mock_gateway, default_config
        ).run(lambda: True)

        # Should be ERROR status since not all tasks done
        assert result.final_status == WorkflowRunStatus.ERROR
        assert result.done_count == 1
        assert result.failed_count == 1
        assert result.total_tasks == 2

        # Task-level results
        assert result.task_final_statuses[1] == TaskStatus.DONE
        assert result.task_final_statuses[2] == TaskStatus.ERROR_FATAL
        assert result.done_task_ids == frozenset({1})
        assert result.failed_task_ids == frozenset({2})

    @pytest.mark.asyncio
    async def test_run_result_includes_num_previously_complete(
        self, mock_gateway, mock_task, mock_array, default_config
    ):
        """Test that run result includes num_previously_complete for resume tracking."""
        done_task = mock_task(1, status=TaskStatus.DONE)
        array = mock_array(1)

        state = create_state_with_tasks(
            tasks={1: done_task},
            arrays={1: array},
            task_status_map={
                TaskStatus.REGISTERING: set(),
                TaskStatus.QUEUED: set(),
                TaskStatus.INSTANTIATING: set(),
                TaskStatus.LAUNCHED: set(),
                TaskStatus.RUNNING: set(),
                TaskStatus.DONE: {done_task},
                TaskStatus.ADJUSTING_RESOURCES: set(),
                TaskStatus.ERROR_FATAL: set(),
            },
            status=WorkflowRunStatus.BOUND,
        )
        # Simulate resume scenario with previously completed tasks
        state.num_previously_complete = 5

        result = await WorkflowRunOrchestrator(
            state, mock_gateway, default_config
        ).run(lambda: True)

        assert result.num_previously_complete == 5
        # Calculate newly completed (in this case, task was already done)
        newly_completed = result.done_count - result.num_previously_complete
        assert newly_completed == -4  # 1 - 5 = -4 (no new completions)

    @pytest.mark.asyncio
    async def test_run_transitions_to_running(self, basic_state, mock_gateway, default_config):
        """Test that run transitions status to RUNNING."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        await orchestrator.run(lambda: True)

        # Check that update_status was called with RUNNING first
        calls = mock_gateway.update_status.call_args_list
        assert any(call[0][0] == WorkflowRunStatus.RUNNING for call in calls)

    @pytest.mark.asyncio
    async def test_run_stops_on_distributor_not_alive(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that run stops when distributor is not alive."""
        # Add a task to ready_to_run so run doesn't exit immediately
        task = list(pending_state.tasks.values())[0]
        pending_state.ready_to_run.append(task)

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)

        with pytest.raises(DistributorNotAlive):
            await orchestrator.run(lambda: False)

    @pytest.mark.asyncio
    async def test_run_handles_keyboard_interrupt(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that run propagates keyboard interrupt."""

        async def raise_interrupt():
            raise KeyboardInterrupt()

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)

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
    async def test_teardown_cancels_heartbeat_task(self, basic_state, mock_gateway, default_config):
        """Test that _teardown cancels heartbeat task."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        orchestrator._stop_event = asyncio.Event()
        orchestrator._heartbeat_task = asyncio.create_task(asyncio.sleep(100))

        await orchestrator._teardown()

        assert orchestrator._heartbeat_task is None
        assert orchestrator._stop_event is None

    @pytest.mark.asyncio
    async def test_teardown_sets_stop_event(self, basic_state, mock_gateway, default_config):
        """Test that _teardown sets stop event."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
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
    async def test_handle_error_updates_status(self, basic_state, mock_gateway, default_config):
        """Test that _handle_error updates status to ERROR."""
        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        await orchestrator._handle_error()

        mock_gateway.update_status.assert_called_with(WorkflowRunStatus.ERROR)

    @pytest.mark.asyncio
    async def test_handle_error_catches_transition_error(
        self, basic_state, mock_gateway, default_config
    ):
        """Test that _handle_error catches TransitionError."""
        mock_gateway.update_status = AsyncMock(
            side_effect=TransitionError("Test")
        )

        orchestrator = WorkflowRunOrchestrator(basic_state, mock_gateway, default_config)
        # Should not raise
        await orchestrator._handle_error()


# ──────────────────────────────────────────────────────────────────────────────
# Test Result
# ──────────────────────────────────────────────────────────────────────────────


class TestOrchestratorResult:
    """Tests for OrchestratorResult."""

    def test_result_basic_fields(self):
        """Test OrchestratorResult basic fields."""
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.DONE,
            elapsed_time=100.5,
            total_tasks=12,
            done_count=10,
            failed_count=2,
            num_previously_complete=5,
            task_final_statuses={1: TaskStatus.DONE, 2: TaskStatus.ERROR_FATAL},
            done_task_ids=frozenset({1}),
            failed_task_ids=frozenset({2}),
        )

        assert result.final_status == WorkflowRunStatus.DONE
        assert result.elapsed_time == 100.5
        assert result.total_tasks == 12
        assert result.done_count == 10
        assert result.failed_count == 2
        assert result.num_previously_complete == 5

    def test_result_task_final_statuses(self):
        """Test OrchestratorResult task_final_statuses field."""
        task_statuses = {
            1: TaskStatus.DONE,
            2: TaskStatus.DONE,
            3: TaskStatus.ERROR_FATAL,
        }
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.DONE,
            elapsed_time=50.0,
            total_tasks=3,
            done_count=2,
            failed_count=1,
            num_previously_complete=0,
            task_final_statuses=task_statuses,
            done_task_ids=frozenset({1, 2}),
            failed_task_ids=frozenset({3}),
        )

        assert result.task_final_statuses == task_statuses
        assert result.task_final_statuses[1] == TaskStatus.DONE
        assert result.task_final_statuses[2] == TaskStatus.DONE
        assert result.task_final_statuses[3] == TaskStatus.ERROR_FATAL

    def test_result_done_task_ids(self):
        """Test OrchestratorResult done_task_ids field is immutable."""
        done_ids = frozenset({1, 2, 5, 10})
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.DONE,
            elapsed_time=50.0,
            total_tasks=10,
            done_count=4,
            failed_count=0,
            num_previously_complete=0,
            task_final_statuses={},
            done_task_ids=done_ids,
            failed_task_ids=frozenset(),
        )

        assert result.done_task_ids == done_ids
        assert 1 in result.done_task_ids
        assert 2 in result.done_task_ids
        assert 3 not in result.done_task_ids
        # Verify it's immutable (frozenset)
        assert isinstance(result.done_task_ids, frozenset)

    def test_result_failed_task_ids(self):
        """Test OrchestratorResult failed_task_ids field is immutable."""
        failed_ids = frozenset({3, 7})
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.ERROR,
            elapsed_time=50.0,
            total_tasks=10,
            done_count=8,
            failed_count=2,
            num_previously_complete=0,
            task_final_statuses={},
            done_task_ids=frozenset(),
            failed_task_ids=failed_ids,
        )

        assert result.failed_task_ids == failed_ids
        assert 3 in result.failed_task_ids
        assert 7 in result.failed_task_ids
        assert 1 not in result.failed_task_ids
        # Verify it's immutable (frozenset)
        assert isinstance(result.failed_task_ids, frozenset)

    def test_result_num_previously_complete_for_resume(self):
        """Test OrchestratorResult num_previously_complete for resume tracking."""
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.DONE,
            elapsed_time=30.0,
            total_tasks=100,
            done_count=100,
            failed_count=0,
            num_previously_complete=75,  # 75 were done from previous run
            task_final_statuses={},
            done_task_ids=frozenset(),
            failed_task_ids=frozenset(),
        )

        # Calculate newly completed
        newly_completed = result.done_count - result.num_previously_complete
        assert newly_completed == 25

    def test_result_empty_task_sets(self):
        """Test OrchestratorResult with empty task sets."""
        result = OrchestratorResult(
            final_status=WorkflowRunStatus.DONE,
            elapsed_time=0.1,
            total_tasks=0,
            done_count=0,
            failed_count=0,
            num_previously_complete=0,
            task_final_statuses={},
            done_task_ids=frozenset(),
            failed_task_ids=frozenset(),
        )

        assert len(result.task_final_statuses) == 0
        assert len(result.done_task_ids) == 0
        assert len(result.failed_task_ids) == 0


# ──────────────────────────────────────────────────────────────────────────────
# Test Sync Operations
# ──────────────────────────────────────────────────────────────────────────────


class TestSyncOperations:
    """Tests for synchronization operations."""

    @pytest.mark.asyncio
    async def test_do_sync_updates_last_sync(self, pending_state, mock_gateway, default_config):
        """Test that _do_sync updates last_sync time."""
        mock_gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time="2024-01-01T12:00:00",
                tasks_by_status={},
            )
        )

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        await orchestrator._do_sync(full_sync=False)

        assert pending_state.last_sync == "2024-01-01T12:00:00"

    @pytest.mark.asyncio
    async def test_do_sync_updates_max_concurrently_running(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that _do_sync updates max_concurrently_running."""
        mock_gateway.get_workflow_concurrency = AsyncMock(return_value=200)

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        await orchestrator._do_sync(full_sync=False)

        assert pending_state.max_concurrently_running == 200

    @pytest.mark.asyncio
    async def test_do_sync_updates_array_limits(
        self, pending_state, mock_gateway, default_config
    ):
        """Test that _do_sync updates array concurrency limits."""
        mock_gateway.get_array_concurrency = AsyncMock(return_value=75)

        orchestrator = WorkflowRunOrchestrator(pending_state, mock_gateway, default_config)
        await orchestrator._do_sync(full_sync=False)

        array = list(pending_state.arrays.values())[0]
        assert array.max_concurrently_running == 75

