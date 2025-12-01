"""Tests for WorkflowRun feature flag integration.

These tests verify that the USE_NEW_GATEWAY, USE_NEW_STATE, and USE_NEW_HEARTBEAT
flags correctly switch between the old and new implementations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobmon.client.swarm.workflow_run import WorkflowRun
from jobmon.client.swarm.workflow_run_impl.gateway import (
    HeartbeatResponse,
    QueueResponse,
    StatusUpdateResponse,
    TaskStatusUpdatesResponse,
)
from jobmon.client.swarm.workflow_run_impl.gateway import TaskStatusUpdatesResponse
from jobmon.client.swarm.workflow_run_impl.services.heartbeat import HeartbeatService
from jobmon.client.swarm.workflow_run_impl.services.synchronizer import Synchronizer
from jobmon.client.swarm.workflow_run_impl.state import StateUpdate, SwarmState
from jobmon.core.constants import TaskStatus


@pytest.fixture
def mock_requester() -> MagicMock:
    """Create a mock requester."""
    requester = MagicMock()
    requester.send_request = MagicMock()
    requester.send_request_async = AsyncMock()
    return requester


@pytest.fixture
def workflow_run(mock_requester: MagicMock) -> WorkflowRun:
    """Create a WorkflowRun for testing."""
    wfr = WorkflowRun(
        workflow_run_id=100,
        requester=mock_requester,
        workflow_run_heartbeat_interval=30,
        heartbeat_report_by_buffer=2.0,
    )
    # Manually set workflow_id to allow gateway/state creation
    wfr.workflow_id = 1
    wfr.max_concurrently_running = 100
    wfr.dag_id = 1
    wfr.tasks = {}
    wfr.arrays = {}
    wfr.last_sync = datetime.now()
    wfr.num_previously_complete = 0
    return wfr


class TestFeatureFlagOff:
    """Tests with USE_NEW_GATEWAY = False (default)."""

    def test_flag_default_is_false(self) -> None:
        """Verify the default flag value."""
        assert WorkflowRun.USE_NEW_GATEWAY is False

    def test_sync_heartbeat_uses_old_path(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test sync heartbeat uses requester directly when flag is off."""
        WorkflowRun.USE_NEW_GATEWAY = False
        mock_requester.send_request.return_value = (200, {"status": "R"})

        workflow_run._log_heartbeat()

        mock_requester.send_request.assert_called_once()
        call_kwargs = mock_requester.send_request.call_args.kwargs
        assert "/log_heartbeat" in call_kwargs["app_route"]

    @pytest.mark.asyncio
    async def test_async_heartbeat_uses_old_path(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async heartbeat uses requester directly when flag is off."""
        WorkflowRun.USE_NEW_GATEWAY = False
        mock_requester.send_request_async.return_value = (200, {"status": "R"})

        # Patch _ensure_session to return a mock session
        with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
            await workflow_run._log_heartbeat_async()

        mock_requester.send_request_async.assert_called_once()


class TestFeatureFlagOn:
    """Tests with USE_NEW_GATEWAY = True."""

    def test_sync_heartbeat_uses_gateway(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test sync heartbeat uses gateway when flag is on."""
        WorkflowRun.USE_NEW_GATEWAY = True
        try:
            mock_requester.send_request.return_value = (200, {"status": "R"})

            workflow_run._log_heartbeat()

            # Should call through the gateway's sync method
            mock_requester.send_request.assert_called_once()
            assert workflow_run._status == "R"
        finally:
            WorkflowRun.USE_NEW_GATEWAY = False

    @pytest.mark.asyncio
    async def test_async_heartbeat_uses_gateway(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async heartbeat uses gateway when flag is on."""
        WorkflowRun.USE_NEW_GATEWAY = True
        try:
            mock_requester.send_request_async.return_value = (200, {"status": "R"})

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._log_heartbeat_async()

            mock_requester.send_request_async.assert_called()
            assert workflow_run._status == "R"
        finally:
            WorkflowRun.USE_NEW_GATEWAY = False

    @pytest.mark.asyncio
    async def test_async_status_update_uses_gateway(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async status update uses gateway when flag is on."""
        WorkflowRun.USE_NEW_GATEWAY = True
        try:
            mock_requester.send_request_async.return_value = (200, {"status": "D"})

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._update_status_async("D")

            mock_requester.send_request_async.assert_called()
            assert workflow_run._status == "D"
        finally:
            WorkflowRun.USE_NEW_GATEWAY = False

    @pytest.mark.asyncio
    async def test_async_triage_uses_gateway(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async triage request uses gateway when flag is on."""
        WorkflowRun.USE_NEW_GATEWAY = True
        try:
            mock_requester.send_request_async.return_value = (200, {})

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._set_status_for_triaging_async()

            mock_requester.send_request_async.assert_called()
        finally:
            WorkflowRun.USE_NEW_GATEWAY = False

    @pytest.mark.asyncio
    async def test_async_task_status_updates_uses_gateway(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async task status updates uses gateway when flag is on."""
        WorkflowRun.USE_NEW_GATEWAY = True
        try:
            now = datetime.now()
            mock_requester.send_request_async.return_value = (
                200,
                {"time": now, "tasks_by_status": {}},
            )

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._task_status_updates_async(full_sync=True)

            mock_requester.send_request_async.assert_called()
        finally:
            WorkflowRun.USE_NEW_GATEWAY = False

    @pytest.mark.asyncio
    async def test_async_workflow_concurrency_uses_gateway(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async workflow concurrency sync uses gateway when flag is on."""
        WorkflowRun.USE_NEW_GATEWAY = True
        try:
            mock_requester.send_request_async.return_value = (
                200,
                {"max_concurrently_running": 500},
            )

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._synchronize_max_concurrently_running_async()

            assert workflow_run.max_concurrently_running == 500
        finally:
            WorkflowRun.USE_NEW_GATEWAY = False


class TestGatewayInitialization:
    """Tests for gateway lazy initialization."""

    def test_gateway_not_created_initially(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test that gateway is None initially."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
        )
        assert wfr._gateway is None

    def test_ensure_gateway_creates_gateway(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_gateway creates the gateway."""
        assert workflow_run._gateway is None
        gateway = workflow_run._ensure_gateway()
        assert gateway is not None
        assert gateway.workflow_id == workflow_run.workflow_id
        assert gateway.workflow_run_id == workflow_run.workflow_run_id

    def test_ensure_gateway_returns_same_instance(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_gateway returns the same instance."""
        gateway1 = workflow_run._ensure_gateway()
        gateway2 = workflow_run._ensure_gateway()
        assert gateway1 is gateway2

    def test_ensure_gateway_fails_without_workflow_id(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that _ensure_gateway fails if workflow_id not set."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
        )
        # workflow_id is not set, so this should fail
        with pytest.raises(RuntimeError, match="workflow_id"):
            wfr._ensure_gateway()


# ──────────────────────────────────────────────────────────────────────────────
# State Feature Flag Tests (Phase 2)
# ──────────────────────────────────────────────────────────────────────────────


class TestStateFeatureFlagOff:
    """Tests with USE_NEW_STATE = False (default)."""

    def test_state_flag_default_is_false(self) -> None:
        """Verify the default state flag value."""
        assert WorkflowRun.USE_NEW_STATE is False

    def test_get_active_tasks_count_uses_old_path(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test active task count uses old implementation when flag is off."""
        WorkflowRun.USE_NEW_STATE = False

        # Add a mock task to the old status map
        mock_task = MagicMock()
        mock_task.task_id = 1
        mock_task.status = TaskStatus.RUNNING
        workflow_run._task_status_map[TaskStatus.RUNNING].add(mock_task)

        count = workflow_run._get_active_tasks_count()
        assert count == 1

    def test_all_tasks_final_uses_old_path(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test all_tasks_final uses old implementation when flag is off."""
        WorkflowRun.USE_NEW_STATE = False

        # Add a task to tasks dict and mark as done
        mock_task = MagicMock()
        mock_task.task_id = 1
        mock_task.status = TaskStatus.DONE
        workflow_run.tasks[1] = mock_task
        workflow_run._task_status_map[TaskStatus.DONE].add(mock_task)

        assert workflow_run._all_tasks_final() is True


class TestStateFeatureFlagOn:
    """Tests with USE_NEW_STATE = True."""

    def test_get_active_tasks_count_uses_state(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test active task count uses SwarmState when flag is on."""
        WorkflowRun.USE_NEW_STATE = True
        try:
            # Create and attach state
            state = workflow_run._ensure_state()

            # Add a mock task to state
            mock_task = MagicMock()
            mock_task.task_id = 1
            mock_task.array_id = 1
            mock_task.status = TaskStatus.RUNNING
            state.tasks[1] = mock_task
            state._task_status_map[TaskStatus.RUNNING].add(mock_task)

            count = workflow_run._get_active_tasks_count()
            assert count == 1
        finally:
            WorkflowRun.USE_NEW_STATE = False

    def test_all_tasks_final_uses_state(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test all_tasks_final uses SwarmState when flag is on."""
        WorkflowRun.USE_NEW_STATE = True
        try:
            state = workflow_run._ensure_state()

            # Add a task to state and mark as done
            mock_task = MagicMock()
            mock_task.task_id = 1
            mock_task.array_id = 1
            mock_task.status = TaskStatus.DONE
            state.tasks[1] = mock_task
            state._task_status_map[TaskStatus.DONE].add(mock_task)

            assert workflow_run._all_tasks_final() is True
        finally:
            WorkflowRun.USE_NEW_STATE = False


class TestStateInitialization:
    """Tests for state lazy initialization."""

    def test_state_not_created_initially(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that state is None initially."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
        )
        assert wfr._state is None

    def test_ensure_state_creates_state(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_state creates the state."""
        assert workflow_run._state is None
        state = workflow_run._ensure_state()
        assert state is not None
        assert state.workflow_id == workflow_run.workflow_id
        assert state.workflow_run_id == workflow_run.workflow_run_id
        assert state.dag_id == workflow_run.dag_id

    def test_ensure_state_returns_same_instance(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_state returns the same instance."""
        state1 = workflow_run._ensure_state()
        state2 = workflow_run._ensure_state()
        assert state1 is state2

    def test_ensure_state_fails_without_workflow_id(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that _ensure_state fails if workflow_id not set."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
        )
        with pytest.raises(RuntimeError, match="workflow_id"):
            wfr._ensure_state()

    def test_ensure_state_copies_resources_cache(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_state copies the task resources cache."""
        # Add something to the old cache
        mock_resources = MagicMock()
        workflow_run._task_resources[12345] = mock_resources

        state = workflow_run._ensure_state()

        assert state.task_resources_cache[12345] is mock_resources


# ──────────────────────────────────────────────────────────────────────────────
# Heartbeat Feature Flag Tests (Phase 3a)
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatFeatureFlagOff:
    """Tests with USE_NEW_HEARTBEAT = False (default)."""

    def test_heartbeat_flag_default_is_false(self) -> None:
        """Verify the default heartbeat flag value."""
        assert WorkflowRun.USE_NEW_HEARTBEAT is False

    def test_sync_heartbeat_uses_old_path_when_flag_off(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test sync heartbeat uses old implementation when flag is off."""
        WorkflowRun.USE_NEW_HEARTBEAT = False
        WorkflowRun.USE_NEW_GATEWAY = False
        mock_requester.send_request.return_value = (200, {"status": "R"})

        workflow_run._log_heartbeat()

        # Should use the requester directly
        mock_requester.send_request.assert_called_once()
        call_kwargs = mock_requester.send_request.call_args.kwargs
        assert "/log_heartbeat" in call_kwargs["app_route"]

    @pytest.mark.asyncio
    async def test_async_heartbeat_uses_old_path_when_flag_off(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async heartbeat uses old implementation when flag is off."""
        WorkflowRun.USE_NEW_HEARTBEAT = False
        WorkflowRun.USE_NEW_GATEWAY = False
        mock_requester.send_request_async.return_value = (200, {"status": "R"})

        with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
            await workflow_run._log_heartbeat_async()

        mock_requester.send_request_async.assert_called_once()


class TestHeartbeatFeatureFlagOn:
    """Tests with USE_NEW_HEARTBEAT = True."""

    def test_sync_heartbeat_uses_service_when_flag_on(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test sync heartbeat uses HeartbeatService when flag is on."""
        WorkflowRun.USE_NEW_HEARTBEAT = True
        try:
            mock_requester.send_request.return_value = (200, {"status": "R"})

            workflow_run._log_heartbeat()

            # Should have created and used the heartbeat service
            assert workflow_run._heartbeat_service is not None
            assert isinstance(workflow_run._heartbeat_service, HeartbeatService)
            # The service should have called the gateway
            mock_requester.send_request.assert_called_once()
        finally:
            WorkflowRun.USE_NEW_HEARTBEAT = False

    def test_sync_heartbeat_updates_status_from_server(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test sync heartbeat updates status when server changes it."""
        WorkflowRun.USE_NEW_HEARTBEAT = True
        try:
            workflow_run._status = "R"
            # Server responds with a different status
            mock_requester.send_request.return_value = (200, {"status": "H"})

            workflow_run._log_heartbeat()

            # Status should be updated
            assert workflow_run._status == "H"
        finally:
            WorkflowRun.USE_NEW_HEARTBEAT = False

    @pytest.mark.asyncio
    async def test_async_heartbeat_uses_service_when_flag_on(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async heartbeat uses HeartbeatService when flag is on."""
        WorkflowRun.USE_NEW_HEARTBEAT = True
        try:
            mock_requester.send_request_async.return_value = (200, {"status": "R"})

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._log_heartbeat_async()

            assert workflow_run._heartbeat_service is not None
            assert isinstance(workflow_run._heartbeat_service, HeartbeatService)
            mock_requester.send_request_async.assert_called_once()
        finally:
            WorkflowRun.USE_NEW_HEARTBEAT = False

    @pytest.mark.asyncio
    async def test_async_heartbeat_updates_status_from_server(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test async heartbeat updates status when server changes it."""
        WorkflowRun.USE_NEW_HEARTBEAT = True
        try:
            workflow_run._status = "R"
            # Server responds with a different status
            mock_requester.send_request_async.return_value = (200, {"status": "C"})

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run._log_heartbeat_async()

            assert workflow_run._status == "C"
        finally:
            WorkflowRun.USE_NEW_HEARTBEAT = False


class TestHeartbeatServiceInitialization:
    """Tests for heartbeat service lazy initialization."""

    def test_heartbeat_service_not_created_initially(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that heartbeat service is None initially."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
        )
        assert wfr._heartbeat_service is None

    def test_ensure_heartbeat_service_creates_service(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_heartbeat_service creates the service."""
        assert workflow_run._heartbeat_service is None
        service = workflow_run._ensure_heartbeat_service()
        assert service is not None
        assert isinstance(service, HeartbeatService)
        assert service.interval == workflow_run._workflow_run_heartbeat_interval
        assert service.current_status == workflow_run._status

    def test_ensure_heartbeat_service_returns_same_instance(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_heartbeat_service returns the same instance."""
        service1 = workflow_run._ensure_heartbeat_service()
        service2 = workflow_run._ensure_heartbeat_service()
        assert service1 is service2

    def test_ensure_heartbeat_service_requires_gateway(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_heartbeat_service creates gateway if needed."""
        assert workflow_run._gateway is None
        workflow_run._ensure_heartbeat_service()
        # Gateway should have been created as a dependency
        assert workflow_run._gateway is not None

    def test_heartbeat_service_uses_workflow_run_interval(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that service uses the workflow run's heartbeat interval."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
            workflow_run_heartbeat_interval=45,
            heartbeat_report_by_buffer=3.0,
        )
        wfr.workflow_id = 1
        wfr.max_concurrently_running = 100
        wfr.dag_id = 1

        service = wfr._ensure_heartbeat_service()

        assert service.interval == 45
        assert service.next_report_increment == 135  # 45 * 3.0


# ──────────────────────────────────────────────────────────────────────────────
# Synchronizer Feature Flag Tests (Phase 3b)
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerFeatureFlagOff:
    """Tests with USE_NEW_SYNCHRONIZER = False (default)."""

    def test_synchronizer_flag_default_is_false(self) -> None:
        """Verify the default synchronizer flag value."""
        assert WorkflowRun.USE_NEW_SYNCHRONIZER is False

    @pytest.mark.asyncio
    async def test_sync_uses_old_path_when_flag_off(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test synchronize_state uses old implementation when flag is off."""
        WorkflowRun.USE_NEW_SYNCHRONIZER = False
        WorkflowRun.USE_NEW_GATEWAY = False
        WorkflowRun.USE_NEW_HEARTBEAT = False

        # Mock all the responses
        mock_requester.send_request_async.return_value = (
            200,
            {"time": "2024-01-01T12:00:00", "tasks_by_status": {}},
        )
        mock_requester.send_request.return_value = (200, {"status": "R"})

        with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
            await workflow_run.synchronize_state_async(full_sync=False)

        # Should use the old requester path (multiple calls)
        assert mock_requester.send_request_async.call_count >= 1


class TestSynchronizerFeatureFlagOn:
    """Tests with USE_NEW_SYNCHRONIZER = True."""

    @pytest.mark.asyncio
    async def test_sync_uses_synchronizer_when_flag_on(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test synchronize_state uses Synchronizer when flag is on."""
        WorkflowRun.USE_NEW_SYNCHRONIZER = True
        WorkflowRun.USE_NEW_HEARTBEAT = False
        try:
            # Mock all responses
            from datetime import datetime

            mock_requester.send_request_async.return_value = (
                200,
                {"time": datetime(2024, 1, 1, 12, 0, 0), "tasks_by_status": {}},
            )
            mock_requester.send_request.return_value = (200, {"status": "R"})

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run.synchronize_state_async(full_sync=False)

            # Should have created synchronizer
            assert workflow_run._synchronizer is not None
            assert isinstance(workflow_run._synchronizer, Synchronizer)
        finally:
            WorkflowRun.USE_NEW_SYNCHRONIZER = False

    @pytest.mark.asyncio
    async def test_sync_updates_concurrency_limits(
        self, workflow_run: WorkflowRun, mock_requester: MagicMock
    ) -> None:
        """Test synchronize_state updates concurrency limits."""
        WorkflowRun.USE_NEW_SYNCHRONIZER = True
        WorkflowRun.USE_NEW_HEARTBEAT = False
        try:
            from datetime import datetime

            # Add an array
            from jobmon.client.swarm.swarm_array import SwarmArray

            workflow_run.arrays[10] = SwarmArray(array_id=10, max_concurrently_running=50)

            # Mock responses - workflow concurrency and array concurrency
            call_count = [0]

            def mock_response(app_route, message, request_type, **kwargs):
                call_count[0] += 1
                if "task_status_updates" in app_route:
                    return (
                        200,
                        {"time": datetime(2024, 1, 1, 12, 0, 0), "tasks_by_status": {}},
                    )
                elif "get_max_concurrently_running" in app_route:
                    return (200, {"max_concurrently_running": 200})
                elif "get_array_max_concurrently_running" in app_route:
                    return (200, {"max_concurrently_running": 75})
                elif "log_heartbeat" in app_route:
                    return (200, {"status": "R"})
                elif "triaging" in app_route:
                    return (200, {})
                return (200, {})

            mock_requester.send_request_async = AsyncMock(side_effect=mock_response)
            mock_requester.send_request.return_value = (200, {"status": "R"})

            original_workflow_limit = workflow_run.max_concurrently_running
            original_array_limit = workflow_run.arrays[10].max_concurrently_running

            with patch.object(workflow_run, "_ensure_session", new_callable=AsyncMock):
                await workflow_run.synchronize_state_async(full_sync=True)

            # Limits should be updated
            assert workflow_run.max_concurrently_running == 200
            assert workflow_run.arrays[10].max_concurrently_running == 75
        finally:
            WorkflowRun.USE_NEW_SYNCHRONIZER = False


class TestSynchronizerServiceInitialization:
    """Tests for synchronizer service lazy initialization."""

    def test_synchronizer_not_created_initially(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that synchronizer is None initially."""
        wfr = WorkflowRun(
            workflow_run_id=100,
            requester=mock_requester,
        )
        assert wfr._synchronizer is None

    def test_ensure_synchronizer_creates_service(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_synchronizer creates the service."""
        # Add some tasks and arrays
        workflow_run.tasks = {1: MagicMock(task_id=1), 2: MagicMock(task_id=2)}
        workflow_run.arrays = {10: MagicMock(array_id=10)}

        assert workflow_run._synchronizer is None
        service = workflow_run._ensure_synchronizer()
        assert service is not None
        assert isinstance(service, Synchronizer)
        assert service.task_ids == {1, 2}
        assert service.array_ids == {10}

    def test_ensure_synchronizer_returns_same_instance(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_synchronizer returns the same instance."""
        workflow_run.tasks = {1: MagicMock(task_id=1)}
        workflow_run.arrays = {}

        service1 = workflow_run._ensure_synchronizer()
        service2 = workflow_run._ensure_synchronizer()
        assert service1 is service2

    def test_ensure_synchronizer_updates_task_ids(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_synchronizer updates task IDs."""
        workflow_run.tasks = {1: MagicMock(task_id=1)}
        workflow_run.arrays = {}

        service = workflow_run._ensure_synchronizer()
        assert service.task_ids == {1}

        # Add more tasks
        workflow_run.tasks[2] = MagicMock(task_id=2)
        workflow_run.tasks[3] = MagicMock(task_id=3)

        # Ensure again - should update task IDs
        service2 = workflow_run._ensure_synchronizer()
        assert service2 is service  # Same instance
        assert service.task_ids == {1, 2, 3}  # Updated IDs

    def test_ensure_synchronizer_requires_gateway(
        self, workflow_run: WorkflowRun
    ) -> None:
        """Test that _ensure_synchronizer creates gateway if needed."""
        workflow_run.tasks = {}
        workflow_run.arrays = {}

        assert workflow_run._gateway is None
        workflow_run._ensure_synchronizer()
        # Gateway should have been created as a dependency
        assert workflow_run._gateway is not None

