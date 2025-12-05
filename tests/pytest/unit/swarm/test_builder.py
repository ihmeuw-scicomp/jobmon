"""Unit tests for SwarmBuilder.

These tests verify the SwarmBuilder's initialization logic using mocked
responses, without requiring a running server.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from jobmon.client.swarm.array import SwarmArray
from jobmon.client.swarm.builder import SwarmBuilder
from jobmon.client.swarm.state import SwarmState
from jobmon.client.swarm.task import SwarmTask
from jobmon.core.constants import TaskStatus, WorkflowRunStatus
from jobmon.core.exceptions import EmptyWorkflowError
from jobmon.core.requester import Requester

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_requester() -> MagicMock:
    """Create a mock Requester."""
    requester = MagicMock(spec=Requester)
    requester.send_request = MagicMock()
    return requester


@pytest.fixture
def builder(mock_requester: MagicMock) -> SwarmBuilder:
    """Create a SwarmBuilder with a mock requester."""
    return SwarmBuilder(
        requester=mock_requester,
        workflow_run_id=200,
        heartbeat_interval=30.0,
        heartbeat_report_by_buffer=1.5,
    )


@pytest.fixture
def mock_workflow() -> MagicMock:
    """Create a mock Workflow object with minimal structure."""
    workflow = MagicMock()
    workflow.workflow_id = 100
    workflow.dag_id = 50
    workflow.max_concurrently_running = 500

    # Create mock arrays
    mock_array = MagicMock()
    mock_array.array_id = 1
    mock_array.max_concurrently_running = 100
    mock_array.name = "test_array"
    workflow.arrays = {1: mock_array}

    # Create mock tasks
    mock_task = MagicMock()
    mock_task.task_id = 10
    mock_task.cluster_name = "dummy"
    mock_task.fallback_queues = []
    mock_task.array = mock_array
    mock_task.initial_status = TaskStatus.REGISTERING
    mock_task.max_attempts = 3
    mock_task.original_task_resources = MagicMock()
    mock_task.original_task_resources.requested_resources = {}
    mock_task.original_task_resources.queue = MagicMock()
    mock_task.compute_resources_callable = None
    mock_task.resource_scales = {}
    mock_task.upstream_tasks = []
    mock_task.downstream_tasks = []
    workflow.tasks = {10: mock_task}

    # Mock cluster retrieval
    mock_cluster = MagicMock()
    mock_cluster.get_queue = MagicMock(return_value=MagicMock())
    workflow.get_cluster_by_name = MagicMock(return_value=mock_cluster)

    return workflow


# ──────────────────────────────────────────────────────────────────────────────
# Initialization Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestBuilderInitialization:
    """Test SwarmBuilder initialization."""

    def test_initial_state(self, builder: SwarmBuilder) -> None:
        """Test that builder starts with empty state."""
        assert builder.workflow_run_id == 200
        assert builder.heartbeat_interval == 30.0
        assert len(builder.tasks) == 0
        assert len(builder.arrays) == 0
        assert builder.workflow_id is None
        assert builder.dag_id is None

    def test_initial_status_map_after_build(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that all status buckets are initialized after build."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        expected_statuses = [
            TaskStatus.REGISTERING,
            TaskStatus.QUEUED,
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
            TaskStatus.DONE,
            TaskStatus.ADJUSTING_RESOURCES,
            TaskStatus.ERROR_FATAL,
        ]
        for status in expected_statuses:
            assert status in builder.task_status_map
            assert isinstance(builder.task_status_map[status], set)


# ──────────────────────────────────────────────────────────────────────────────
# Build From Workflow Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildFromWorkflow:
    """Tests for building from an in-memory Workflow object."""

    def test_build_from_workflow_creates_state(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that build_from_workflow creates a SwarmState."""
        # Mock server time
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        assert isinstance(builder.state, SwarmState)
        assert builder.workflow_id == 100
        assert builder.state.workflow_run_id == 200
        assert builder.state.max_concurrently_running == 500

    def test_build_from_workflow_creates_arrays(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that arrays are created correctly."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        assert 1 in builder.state.arrays
        array = builder.state.arrays[1]
        assert isinstance(array, SwarmArray)
        assert array.array_id == 1
        assert array.max_concurrently_running == 100

    def test_build_from_workflow_creates_tasks(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that tasks are created correctly."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        assert 10 in builder.state.tasks
        task = builder.state.tasks[10]
        assert isinstance(task, SwarmTask)
        assert task.task_id == 10
        assert task.array_id == 1
        assert task.status == TaskStatus.REGISTERING

    def test_build_from_workflow_populates_status_map(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that tasks are added to correct status buckets."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        # Task 10 is in REGISTERING status
        registering_tasks = builder.state._task_status_map[TaskStatus.REGISTERING]
        assert len(registering_tasks) == 1
        task = list(registering_tasks)[0]
        assert task.task_id == 10

    def test_build_from_workflow_sets_metadata(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that workflow metadata is set correctly."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        assert builder.workflow_id == 100
        assert builder.dag_id == 50
        assert builder.max_concurrently_running == 500
        assert builder.last_sync == now

    def test_build_from_workflow_with_dependencies(
        self,
        mock_requester: MagicMock,
    ) -> None:
        """Test that task dependencies are set up correctly."""
        builder = SwarmBuilder(mock_requester, 200)

        # Create mock workflow with dependencies
        workflow = MagicMock()
        workflow.workflow_id = 100
        workflow.dag_id = 50
        workflow.max_concurrently_running = 500

        # Create array
        mock_array = MagicMock()
        mock_array.array_id = 1
        mock_array.max_concurrently_running = 100
        mock_array.name = "test_array"
        workflow.arrays = {1: mock_array}

        # Create two tasks with dependency
        task1 = MagicMock()
        task1.task_id = 1
        task1.cluster_name = "dummy"
        task1.fallback_queues = []
        task1.array = mock_array
        task1.initial_status = TaskStatus.REGISTERING
        task1.max_attempts = 3
        task1.original_task_resources = MagicMock()
        task1.compute_resources_callable = None
        task1.resource_scales = {}
        task1.upstream_tasks = []

        task2 = MagicMock()
        task2.task_id = 2
        task2.cluster_name = "dummy"
        task2.fallback_queues = []
        task2.array = mock_array
        task2.initial_status = TaskStatus.REGISTERING
        task2.max_attempts = 3
        task2.original_task_resources = MagicMock()
        task2.compute_resources_callable = None
        task2.resource_scales = {}
        task2.upstream_tasks = [task1]

        task1.downstream_tasks = [task2]
        task2.downstream_tasks = []

        workflow.tasks = {1: task1, 2: task2}

        mock_cluster = MagicMock()
        mock_cluster.get_queue = MagicMock(return_value=MagicMock())
        workflow.get_cluster_by_name = MagicMock(return_value=mock_cluster)

        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(workflow)

        # Check relationships
        swarm_task1 = builder.state.tasks[1]
        swarm_task2 = builder.state.tasks[2]

        assert swarm_task1.num_upstreams == 0
        assert swarm_task2.num_upstreams == 1
        assert swarm_task2 in swarm_task1.downstream_swarm_tasks

    def test_build_from_workflow_with_done_tasks(
        self,
        mock_requester: MagicMock,
    ) -> None:
        """Test that done tasks update downstream counts."""
        builder = SwarmBuilder(mock_requester, 200)

        # Create mock workflow with done upstream
        workflow = MagicMock()
        workflow.workflow_id = 100
        workflow.dag_id = 50
        workflow.max_concurrently_running = 500

        mock_array = MagicMock()
        mock_array.array_id = 1
        mock_array.max_concurrently_running = 100
        mock_array.name = "test_array"
        workflow.arrays = {1: mock_array}

        # Task 1 is DONE
        task1 = MagicMock()
        task1.task_id = 1
        task1.cluster_name = "dummy"
        task1.fallback_queues = []
        task1.array = mock_array
        task1.initial_status = TaskStatus.DONE
        task1.max_attempts = 3
        task1.original_task_resources = MagicMock()
        task1.compute_resources_callable = None
        task1.resource_scales = {}
        task1.upstream_tasks = []

        # Task 2 depends on task 1
        task2 = MagicMock()
        task2.task_id = 2
        task2.cluster_name = "dummy"
        task2.fallback_queues = []
        task2.array = mock_array
        task2.initial_status = TaskStatus.REGISTERING
        task2.max_attempts = 3
        task2.original_task_resources = MagicMock()
        task2.compute_resources_callable = None
        task2.resource_scales = {}
        task2.upstream_tasks = [task1]

        task1.downstream_tasks = [task2]
        task2.downstream_tasks = []

        workflow.tasks = {1: task1, 2: task2}

        mock_cluster = MagicMock()
        mock_cluster.get_queue = MagicMock(return_value=MagicMock())
        workflow.get_cluster_by_name = MagicMock(return_value=mock_cluster)

        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(workflow)

        swarm_task2 = builder.state.tasks[2]
        # Task 2 should know task 1 is done
        assert swarm_task2.num_upstreams_done == 1


# ──────────────────────────────────────────────────────────────────────────────
# Build From Workflow ID Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildFromWorkflowId:
    """Tests for building from database (resume scenarios)."""

    def test_build_from_workflow_id_fetches_metadata(
        self,
        builder: SwarmBuilder,
        mock_requester: MagicMock,
    ) -> None:
        """Test that workflow metadata is fetched."""
        now = datetime.now()
        # Initial heartbeat
        mock_requester.send_request.side_effect = [
            (200, {"status": WorkflowRunStatus.LINKING}),  # heartbeat
            (200, {"workflow": [100, 50, 500]}),  # metadata
            (200, {"time": now}),  # server time
            (200, {"tasks": {}}),  # empty tasks (done)
            (200, {"status": WorkflowRunStatus.BOUND}),  # status update
        ]

        builder.build_from_workflow_id(100)

        assert builder.workflow_id == 100
        assert builder.dag_id == 50
        assert builder.state.max_concurrently_running == 500

    def test_build_from_workflow_id_empty_workflow_error(
        self,
        builder: SwarmBuilder,
        mock_requester: MagicMock,
    ) -> None:
        """Test that EmptyWorkflowError is raised when workflow not found."""
        mock_requester.send_request.side_effect = [
            (200, {"status": WorkflowRunStatus.LINKING}),  # heartbeat
            (200, {"workflow": None}),  # no workflow
        ]

        with pytest.raises(EmptyWorkflowError):
            builder.build_from_workflow_id(999)

    def test_build_from_workflow_id_logs_heartbeats(
        self,
        mock_requester: MagicMock,
    ) -> None:
        """Test that heartbeats are logged during long operations."""
        builder = SwarmBuilder(
            mock_requester,
            200,
            heartbeat_interval=0.0,  # Force heartbeat on every check
        )
        now = datetime.now()

        mock_requester.send_request.side_effect = [
            (200, {"status": WorkflowRunStatus.LINKING}),  # initial heartbeat
            (200, {"workflow": [100, 50, 500]}),  # metadata
            (200, {"time": now}),  # server time
            (200, {"status": WorkflowRunStatus.LINKING}),  # heartbeat during tasks
            (200, {"tasks": {}}),  # empty tasks
            (200, {"status": WorkflowRunStatus.BOUND}),  # status update
        ]

        builder.build_from_workflow_id(100)

        # Should have called send_request multiple times for heartbeats
        assert mock_requester.send_request.call_count >= 4


# ──────────────────────────────────────────────────────────────────────────────
# Edge Cases
# ──────────────────────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_gateway_requires_workflow_id(
        self,
        builder: SwarmBuilder,
    ) -> None:
        """Test that gateway cannot be created without workflow_id."""
        with pytest.raises(RuntimeError, match="workflow_id"):
            builder._ensure_gateway()

    def test_ensure_state_requires_workflow_metadata(
        self,
        builder: SwarmBuilder,
    ) -> None:
        """Test that state cannot be created without workflow metadata."""
        with pytest.raises(RuntimeError, match="workflow metadata"):
            builder._ensure_state()

    def test_properties_reflect_internal_state(
        self,
        builder: SwarmBuilder,
        mock_workflow: MagicMock,
        mock_requester: MagicMock,
    ) -> None:
        """Test that properties return the SwarmState's internal data."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(mock_workflow)

        # Properties should delegate to the SwarmState
        assert builder.tasks is builder.state.tasks
        assert builder.arrays is builder.state.arrays
        assert builder.task_status_map is builder.state._task_status_map

    def test_num_previously_complete(
        self,
        mock_requester: MagicMock,
    ) -> None:
        """Test that num_previously_complete returns done task count."""
        builder = SwarmBuilder(mock_requester, 200)

        # Create workflow with one done task
        workflow = MagicMock()
        workflow.workflow_id = 100
        workflow.dag_id = 50
        workflow.max_concurrently_running = 500

        mock_array = MagicMock()
        mock_array.array_id = 1
        mock_array.max_concurrently_running = 100
        mock_array.name = "test"
        workflow.arrays = {1: mock_array}

        task = MagicMock()
        task.task_id = 1
        task.cluster_name = "dummy"
        task.fallback_queues = []
        task.array = mock_array
        task.initial_status = TaskStatus.DONE
        task.max_attempts = 3
        task.original_task_resources = MagicMock()
        task.compute_resources_callable = None
        task.resource_scales = {}
        task.upstream_tasks = []
        task.downstream_tasks = []
        workflow.tasks = {1: task}

        mock_cluster = MagicMock()
        mock_cluster.get_queue = MagicMock(return_value=MagicMock())
        workflow.get_cluster_by_name = MagicMock(return_value=mock_cluster)

        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        builder.build_from_workflow(workflow)

        assert builder.num_previously_complete == 1
