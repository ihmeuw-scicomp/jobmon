"""Unit tests for ServerGateway.

These tests verify the ServerGateway's HTTP communication logic using mocked
responses, without requiring a running server.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from jobmon.client.swarm.gateway import (
    DownstreamTasksResponse,
    HeartbeatResponse,
    QueueResponse,
    ServerGateway,
    StatusUpdateResponse,
    TaskStatusUpdatesResponse,
    WorkflowMetadata,
)
from jobmon.core.requester import Requester

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_requester() -> MagicMock:
    """Create a mock Requester with both sync and async methods."""
    requester = MagicMock(spec=Requester)
    requester.send_request = MagicMock()
    requester.send_request_async = AsyncMock()
    return requester


@pytest.fixture
def gateway(mock_requester: MagicMock) -> ServerGateway:
    """Create a ServerGateway with a mock requester."""
    return ServerGateway(
        requester=mock_requester,
        workflow_id=100,
        workflow_run_id=200,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Response Dataclass Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestResponseDataclasses:
    """Test that response dataclasses are properly structured."""

    def test_heartbeat_response_immutable(self) -> None:
        response = HeartbeatResponse(status="R")
        assert response.status == "R"
        with pytest.raises(Exception):  # FrozenInstanceError
            response.status = "D"  # type: ignore

    def test_status_update_response(self) -> None:
        response = StatusUpdateResponse(status="D")
        assert response.status == "D"

    def test_queue_response(self) -> None:
        response = QueueResponse(tasks_by_status={"Q": [1, 2, 3], "R": [4]})
        assert response.tasks_by_status["Q"] == [1, 2, 3]
        assert response.tasks_by_status["R"] == [4]

    def test_task_status_updates_response(self) -> None:
        now = datetime.now()
        response = TaskStatusUpdatesResponse(
            time=now,
            tasks_by_status={"D": [1], "R": [2, 3]},
        )
        assert response.time == now
        assert response.tasks_by_status["D"] == [1]

    def test_workflow_metadata(self) -> None:
        metadata = WorkflowMetadata(
            workflow_id=1,
            dag_id=2,
            max_concurrently_running=100,
        )
        assert metadata.workflow_id == 1
        assert metadata.dag_id == 2
        assert metadata.max_concurrently_running == 100

    def test_downstream_tasks_response(self) -> None:
        response = DownstreamTasksResponse(
            downstream_tasks={1: (10, [20, 30]), 2: (11, [])}
        )
        assert response.downstream_tasks[1] == (10, [20, 30])
        assert response.downstream_tasks[2] == (11, [])


# ──────────────────────────────────────────────────────────────────────────────
# Async Method Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeat:
    """Tests for heartbeat operations."""

    @pytest.mark.asyncio
    async def test_log_heartbeat_returns_response(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test that log_heartbeat returns a properly typed response."""
        mock_requester.send_request_async.return_value = (200, {"status": "R"})

        response = await gateway.log_heartbeat(
            status="R",
            next_report_increment=30.0,
        )

        assert isinstance(response, HeartbeatResponse)
        assert response.status == "R"
        mock_requester.send_request_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_log_heartbeat_sends_correct_route(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test that heartbeat uses the correct route and payload."""
        mock_requester.send_request_async.return_value = (200, {"status": "R"})

        await gateway.log_heartbeat(status="R", next_report_increment=60.0)

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["app_route"] == "/workflow_run/200/log_heartbeat"
        assert call_kwargs["request_type"] == "post"
        assert call_kwargs["message"]["status"] == "R"
        assert call_kwargs["message"]["next_report_increment"] == 60.0

    @pytest.mark.asyncio
    async def test_log_heartbeat_status_change(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test that status changes are returned from heartbeat."""
        # Server returns a different status (e.g., resume signal)
        mock_requester.send_request_async.return_value = (200, {"status": "H"})

        response = await gateway.log_heartbeat(
            status="R",
            next_report_increment=30.0,
        )

        assert response.status == "H"


class TestLifecycle:
    """Tests for lifecycle operations."""

    @pytest.mark.asyncio
    async def test_update_status(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test status update returns new status."""
        mock_requester.send_request_async.return_value = (200, {"status": "D"})

        response = await gateway.update_status("D")

        assert isinstance(response, StatusUpdateResponse)
        assert response.status == "D"

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["app_route"] == "/workflow_run/200/update_status"
        assert call_kwargs["request_type"] == "put"
        assert call_kwargs["message"]["status"] == "D"

    @pytest.mark.asyncio
    async def test_terminate_task_instances(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test terminate task instances sends correct request."""
        mock_requester.send_request_async.return_value = (200, {})

        await gateway.terminate_task_instances()

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["app_route"] == "/workflow_run/200/terminate_task_instances"
        assert call_kwargs["request_type"] == "put"


class TestStateSynchronization:
    """Tests for state synchronization operations."""

    @pytest.mark.asyncio
    async def test_request_triage(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test triage request."""
        mock_requester.send_request_async.return_value = (200, {})

        await gateway.request_triage()

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["app_route"] == "/workflow_run/200/set_status_for_triaging"
        assert call_kwargs["request_type"] == "post"

    @pytest.mark.asyncio
    async def test_get_task_status_updates_full_sync(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test full sync (no since parameter)."""
        now = datetime.now()
        mock_requester.send_request_async.return_value = (
            200,
            {"time": now, "tasks_by_status": {"D": [1, 2], "R": [3]}},
        )

        response = await gateway.get_task_status_updates(since=None)

        assert isinstance(response, TaskStatusUpdatesResponse)
        assert response.time == now
        assert response.tasks_by_status["D"] == [1, 2]

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["message"] == {}  # No last_sync for full sync

    @pytest.mark.asyncio
    async def test_get_task_status_updates_incremental(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test incremental sync (with since parameter)."""
        since = datetime(2024, 1, 1, 12, 0, 0)
        now = datetime.now()
        mock_requester.send_request_async.return_value = (
            200,
            {"time": now, "tasks_by_status": {"D": [1]}},
        )

        response = await gateway.get_task_status_updates(since=since)

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["message"]["last_sync"] == str(since)

    @pytest.mark.asyncio
    async def test_get_workflow_concurrency(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test workflow concurrency fetch."""
        mock_requester.send_request_async.return_value = (
            200,
            {"max_concurrently_running": 500},
        )

        result = await gateway.get_workflow_concurrency()

        assert result == 500
        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["app_route"] == "/workflow/100/get_max_concurrently_running"

    @pytest.mark.asyncio
    async def test_get_array_concurrency(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test array concurrency fetch."""
        mock_requester.send_request_async.return_value = (
            200,
            {"max_concurrently_running": 100},
        )

        result = await gateway.get_array_concurrency(array_id=42)

        assert result == 100
        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert (
            call_kwargs["app_route"] == "/array/42/get_array_max_concurrently_running"
        )


class TestTaskQueueing:
    """Tests for task queueing operations."""

    @pytest.mark.asyncio
    async def test_queue_task_batch(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test batch queueing returns response."""
        mock_requester.send_request_async.return_value = (
            200,
            {"tasks_by_status": {"Q": [1, 2, 3]}},
        )

        response = await gateway.queue_task_batch(
            array_id=10,
            task_ids=[1, 2, 3],
            task_resources_id=99,
            cluster_id=1,
        )

        assert isinstance(response, QueueResponse)
        assert response.tasks_by_status["Q"] == [1, 2, 3]

        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["app_route"] == "/array/10/queue_task_batch"
        assert call_kwargs["message"]["task_ids"] == [1, 2, 3]
        assert call_kwargs["message"]["task_resources_id"] == 99
        assert call_kwargs["message"]["workflow_run_id"] == 200
        assert call_kwargs["message"]["cluster_id"] == 1


class TestWorkflowSetup:
    """Tests for workflow setup operations."""

    @pytest.mark.asyncio
    async def test_get_workflow_metadata(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test workflow metadata fetch."""
        mock_requester.send_request_async.return_value = (
            200,
            {
                "workflow": [100, 50, 1000]
            },  # workflow_id, dag_id, max_concurrently_running
        )

        metadata = await gateway.get_workflow_metadata()

        assert isinstance(metadata, WorkflowMetadata)
        assert metadata.workflow_id == 100
        assert metadata.dag_id == 50
        assert metadata.max_concurrently_running == 1000

    @pytest.mark.asyncio
    async def test_get_workflow_metadata_not_found(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test workflow metadata raises when not found."""
        mock_requester.send_request_async.return_value = (200, {"workflow": None})

        with pytest.raises(ValueError, match="No workflow found"):
            await gateway.get_workflow_metadata()

    @pytest.mark.asyncio
    async def test_get_tasks(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test task fetch."""
        mock_requester.send_request_async.return_value = (
            200,
            {"tasks": {"1": ["metadata1"], "2": ["metadata2"]}},
        )

        tasks = await gateway.get_tasks(max_task_id=0, chunk_size=100)

        assert tasks == {"1": ["metadata1"], "2": ["metadata2"]}
        call_kwargs = mock_requester.send_request_async.call_args.kwargs
        assert call_kwargs["message"]["max_task_id"] == 0
        assert call_kwargs["message"]["chunk_size"] == 100

    @pytest.mark.asyncio
    async def test_get_downstream_tasks(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test downstream tasks fetch."""
        mock_requester.send_request_async.return_value = (
            200,
            {
                "downstream_tasks": {
                    "1": [10, [20, 30]],
                    "2": [11, []],
                }
            },
        )

        response = await gateway.get_downstream_tasks(
            task_ids=[1, 2],
            dag_id=5,
        )

        assert isinstance(response, DownstreamTasksResponse)
        assert response.downstream_tasks[1] == (10, [20, 30])
        assert response.downstream_tasks[2] == (11, [])

    @pytest.mark.asyncio
    async def test_get_server_time(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test server time fetch."""
        now = datetime.now()
        mock_requester.send_request_async.return_value = (200, {"time": now})

        result = await gateway.get_server_time()

        assert result == now


# ──────────────────────────────────────────────────────────────────────────────
# Synchronous Method Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestSyncMethods:
    """Tests for synchronous wrapper methods."""

    def test_log_heartbeat_sync(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test synchronous heartbeat logging."""
        mock_requester.send_request.return_value = (200, {"status": "R"})

        response = gateway.log_heartbeat_sync(
            status="R",
            next_report_increment=30.0,
        )

        assert isinstance(response, HeartbeatResponse)
        assert response.status == "R"
        mock_requester.send_request.assert_called_once()

    def test_update_status_sync(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test synchronous status update."""
        mock_requester.send_request.return_value = (200, {"status": "D"})

        response = gateway.update_status_sync("D")

        assert isinstance(response, StatusUpdateResponse)
        assert response.status == "D"

    def test_get_server_time_sync(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test synchronous server time fetch."""
        now = datetime.now()
        mock_requester.send_request.return_value = (200, {"time": now})

        result = gateway.get_server_time_sync()

        assert result == now


# ──────────────────────────────────────────────────────────────────────────────
# Session Management Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestSessionManagement:
    """Tests for session management."""

    @pytest.mark.asyncio
    async def test_context_manager_creates_session(
        self, mock_requester: MagicMock
    ) -> None:
        """Test that context manager creates and closes session."""
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_session.closed = False
            mock_session_class.return_value = mock_session

            gateway = ServerGateway(mock_requester, 1, 1)
            async with gateway:
                assert gateway._owns_session is True

            mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_external_session_not_closed(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test that externally provided sessions are not closed."""
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.closed = False

        gateway.set_session(mock_session)
        assert gateway._owns_session is False

        await gateway.close()
        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_session_creates_if_needed(
        self, gateway: ServerGateway, mock_requester: MagicMock
    ) -> None:
        """Test that _ensure_session creates session when needed."""
        mock_requester.send_request_async.return_value = (200, {"status": "R"})

        assert gateway._session is None

        # Calling a method should trigger session creation
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_session.closed = False
            mock_session_class.return_value = mock_session

            await gateway.log_heartbeat("R", 30.0)

            mock_session_class.assert_called_once()
