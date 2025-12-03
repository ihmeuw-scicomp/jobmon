"""Unit tests for HeartbeatService."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from jobmon.client.swarm.gateway import HeartbeatResponse
from jobmon.client.swarm.services.heartbeat import HeartbeatService
from jobmon.client.swarm.state import StateUpdate

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_gateway():
    """Create a mock ServerGateway."""
    gateway = MagicMock()
    # Default: heartbeat returns same status
    gateway.log_heartbeat = AsyncMock(return_value=HeartbeatResponse(status="R"))
    gateway.log_heartbeat_sync = MagicMock(return_value=HeartbeatResponse(status="R"))
    return gateway


@pytest.fixture
def heartbeat_service(mock_gateway):
    """Create a HeartbeatService with default settings."""
    return HeartbeatService(
        gateway=mock_gateway,
        interval=30.0,
        report_by_buffer=1.5,
        initial_status="R",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Test Initialization
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceInit:
    """Tests for HeartbeatService initialization."""

    def test_init_stores_parameters(self, mock_gateway):
        """Test that init stores all parameters correctly."""
        before = time.time()
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=45.0,
            report_by_buffer=2.0,
            initial_status="B",
        )
        after = time.time()

        assert service.interval == 45.0
        assert service.current_status == "B"
        assert service.next_report_increment == 90.0  # 45 * 2
        # last_heartbeat_time is initialized to current time
        assert before <= service.last_heartbeat_time <= after

    def test_init_last_heartbeat_time_is_current_time(self, heartbeat_service):
        """Test that last_heartbeat_time is initialized to current time."""
        # Should be recent (within last second)
        assert time.time() - heartbeat_service.last_heartbeat_time < 1.0


# ──────────────────────────────────────────────────────────────────────────────
# Test Properties
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceProperties:
    """Tests for HeartbeatService properties."""

    def test_interval_property(self, heartbeat_service):
        """Test interval property."""
        assert heartbeat_service.interval == 30.0

    def test_current_status_property(self, heartbeat_service):
        """Test current_status property."""
        assert heartbeat_service.current_status == "R"

    def test_next_report_increment_calculation(self, mock_gateway):
        """Test next_report_increment is interval * buffer."""
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=20.0,
            report_by_buffer=3.0,
            initial_status="R",
        )
        assert service.next_report_increment == 60.0

    def test_time_since_last_heartbeat_at_init(self, heartbeat_service):
        """Test time_since_last_heartbeat returns ~0 at initialization."""
        # Since last_heartbeat_time is initialized to current time,
        # time_since_last_heartbeat should be very small
        assert heartbeat_service.time_since_last_heartbeat() < 1.0

    def test_time_since_last_heartbeat_after_delay(self, heartbeat_service):
        """Test time_since_last_heartbeat after some time passes."""
        # Manually set last_heartbeat_time to 5 seconds ago
        heartbeat_service._last_heartbeat_time = time.time() - 5.0
        elapsed = heartbeat_service.time_since_last_heartbeat()
        assert 4.9 <= elapsed <= 5.5  # Allow some timing tolerance


# ──────────────────────────────────────────────────────────────────────────────
# Test Status Management
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceStatus:
    """Tests for status management."""

    def test_set_status_updates_current_status(self, heartbeat_service):
        """Test set_status updates the status."""
        heartbeat_service.set_status("C")
        assert heartbeat_service.current_status == "C"

    def test_set_status_multiple_times(self, heartbeat_service):
        """Test set_status can be called multiple times."""
        heartbeat_service.set_status("C")
        assert heartbeat_service.current_status == "C"

        heartbeat_service.set_status("H")
        assert heartbeat_service.current_status == "H"

        heartbeat_service.set_status("R")
        assert heartbeat_service.current_status == "R"


# ──────────────────────────────────────────────────────────────────────────────
# Test is_heartbeat_due
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceIsDue:
    """Tests for is_heartbeat_due."""

    def test_is_heartbeat_due_at_init(self, heartbeat_service):
        """Test is_heartbeat_due returns False at init (time just started)."""
        # Since last_heartbeat_time is initialized to current time,
        # heartbeat is not due yet
        assert heartbeat_service.is_heartbeat_due() is False

    def test_is_heartbeat_due_just_logged(self, heartbeat_service):
        """Test is_heartbeat_due returns False immediately after logging."""
        heartbeat_service._last_heartbeat_time = time.time()
        assert heartbeat_service.is_heartbeat_due() is False

    def test_is_heartbeat_due_after_interval(self, heartbeat_service):
        """Test is_heartbeat_due returns True after interval passes."""
        heartbeat_service._last_heartbeat_time = time.time() - 31.0
        assert heartbeat_service.is_heartbeat_due() is True

    def test_is_heartbeat_due_before_interval(self, heartbeat_service):
        """Test is_heartbeat_due returns False before interval passes."""
        heartbeat_service._last_heartbeat_time = time.time() - 15.0
        assert heartbeat_service.is_heartbeat_due() is False


# ──────────────────────────────────────────────────────────────────────────────
# Test tick (async)
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceTick:
    """Tests for the async tick method."""

    @pytest.mark.asyncio
    async def test_tick_calls_gateway(self, heartbeat_service, mock_gateway):
        """Test tick calls gateway with correct parameters."""
        await heartbeat_service.tick()

        mock_gateway.log_heartbeat.assert_called_once_with(
            status="R",
            next_report_increment=45.0,  # 30 * 1.5
        )

    @pytest.mark.asyncio
    async def test_tick_updates_last_heartbeat_time(self, heartbeat_service):
        """Test tick updates last_heartbeat_time."""
        before = time.time()
        await heartbeat_service.tick()
        after = time.time()

        assert before <= heartbeat_service.last_heartbeat_time <= after

    @pytest.mark.asyncio
    async def test_tick_returns_empty_update_when_status_unchanged(
        self, heartbeat_service, mock_gateway
    ):
        """Test tick returns empty StateUpdate when status doesn't change."""
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="R")
        )

        update = await heartbeat_service.tick()

        assert update == StateUpdate.empty()
        assert update.workflow_run_status is None

    @pytest.mark.asyncio
    async def test_tick_returns_status_update_when_changed(
        self, heartbeat_service, mock_gateway
    ):
        """Test tick returns StateUpdate with new status when server changes it."""
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="C")
        )

        update = await heartbeat_service.tick()

        assert update.workflow_run_status == "C"
        assert heartbeat_service.current_status == "C"

    @pytest.mark.asyncio
    async def test_tick_updates_internal_status_on_change(
        self, heartbeat_service, mock_gateway
    ):
        """Test tick updates internal status when server changes it."""
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="H")
        )

        await heartbeat_service.tick()

        assert heartbeat_service.current_status == "H"

    @pytest.mark.asyncio
    async def test_tick_uses_current_status(self, heartbeat_service, mock_gateway):
        """Test tick uses the current status set by set_status."""
        heartbeat_service.set_status("C")
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="C")
        )

        await heartbeat_service.tick()

        mock_gateway.log_heartbeat.assert_called_once_with(
            status="C",
            next_report_increment=45.0,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Test tick_sync
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceTickSync:
    """Tests for the synchronous tick_sync method."""

    def test_tick_sync_calls_gateway(self, heartbeat_service, mock_gateway):
        """Test tick_sync calls gateway with correct parameters."""
        heartbeat_service.tick_sync()

        mock_gateway.log_heartbeat_sync.assert_called_once_with(
            status="R",
            next_report_increment=45.0,
        )

    def test_tick_sync_updates_last_heartbeat_time(self, heartbeat_service):
        """Test tick_sync updates last_heartbeat_time."""
        before = time.time()
        heartbeat_service.tick_sync()
        after = time.time()

        assert before <= heartbeat_service.last_heartbeat_time <= after

    def test_tick_sync_returns_empty_update_when_status_unchanged(
        self, heartbeat_service, mock_gateway
    ):
        """Test tick_sync returns empty StateUpdate when status doesn't change."""
        mock_gateway.log_heartbeat_sync = MagicMock(
            return_value=HeartbeatResponse(status="R")
        )

        update = heartbeat_service.tick_sync()

        assert update == StateUpdate.empty()

    def test_tick_sync_returns_status_update_when_changed(
        self, heartbeat_service, mock_gateway
    ):
        """Test tick_sync returns StateUpdate when server changes status."""
        mock_gateway.log_heartbeat_sync = MagicMock(
            return_value=HeartbeatResponse(status="C")
        )

        update = heartbeat_service.tick_sync()

        assert update.workflow_run_status == "C"
        assert heartbeat_service.current_status == "C"


# ──────────────────────────────────────────────────────────────────────────────
# Test run_background
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceRunBackground:
    """Tests for the background loop."""

    @pytest.mark.asyncio
    async def test_run_background_stops_on_event(self, mock_gateway):
        """Test run_background stops when stop_event is set."""
        # Use short interval so the loop is responsive
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=0.2,
            report_by_buffer=1.5,
            initial_status="R",
        )

        stop_event = asyncio.Event()

        # Start the background task
        task = asyncio.create_task(service.run_background(stop_event))

        # Give it a moment to start
        await asyncio.sleep(0.05)

        # Stop it
        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)

        # Should complete without error
        assert task.done()

    @pytest.mark.asyncio
    async def test_run_background_logs_heartbeats(self, mock_gateway):
        """Test run_background logs heartbeats when due."""
        # Use a very short interval for testing
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=0.2,  # 200ms, tick_interval = 0.1s
            report_by_buffer=1.5,
            initial_status="R",
        )

        stop_event = asyncio.Event()
        task = asyncio.create_task(service.run_background(stop_event))

        # Wait long enough for at least one heartbeat
        await asyncio.sleep(0.5)

        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)

        # Should have logged at least one heartbeat
        assert mock_gateway.log_heartbeat.call_count >= 1

    @pytest.mark.asyncio
    async def test_run_background_handles_cancelled_error(self, mock_gateway):
        """Test run_background properly handles CancelledError."""
        # Use short interval so the loop is responsive
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=0.2,
            report_by_buffer=1.5,
            initial_status="R",
        )

        stop_event = asyncio.Event()
        task = asyncio.create_task(service.run_background(stop_event))

        await asyncio.sleep(0.05)

        # Cancel the task
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_run_background_continues_on_heartbeat_failure(self, mock_gateway):
        """Test run_background continues running if heartbeat fails."""
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=0.2,  # 200ms, tick_interval = 0.1s
            report_by_buffer=1.5,
            initial_status="R",
        )

        # First call fails, subsequent calls succeed
        mock_gateway.log_heartbeat = AsyncMock(
            side_effect=[
                Exception("Network error"),
                HeartbeatResponse(status="R"),
                HeartbeatResponse(status="R"),
                HeartbeatResponse(status="R"),
            ]
        )

        stop_event = asyncio.Event()
        task = asyncio.create_task(service.run_background(stop_event))

        # Wait for multiple heartbeat attempts
        # With interval=0.2s, tick_interval=0.1s, we need enough time for:
        # - First heartbeat due at ~0.2s (fails)
        # - Second heartbeat due at ~0.3s (since timer wasn't updated on failure)
        # Add extra margin for CI timing variability
        await asyncio.sleep(1.0)

        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)

        # Should have made multiple calls despite first failure
        assert mock_gateway.log_heartbeat.call_count >= 2


# ──────────────────────────────────────────────────────────────────────────────
# Test reset_timer
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceResetTimer:
    """Tests for reset_timer."""

    def test_reset_timer_sets_time(self, heartbeat_service):
        """Test reset_timer sets last_heartbeat_time to now."""
        # Set to a time in the past
        heartbeat_service._last_heartbeat_time = time.time() - 100.0

        before = time.time()
        heartbeat_service.reset_timer()
        after = time.time()

        assert before <= heartbeat_service.last_heartbeat_time <= after

    def test_reset_timer_affects_is_heartbeat_due(self, heartbeat_service):
        """Test reset_timer causes is_heartbeat_due to return False."""
        # Make heartbeat overdue
        heartbeat_service._last_heartbeat_time = time.time() - 100.0
        assert heartbeat_service.is_heartbeat_due() is True

        heartbeat_service.reset_timer()

        assert heartbeat_service.is_heartbeat_due() is False


# ──────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceIntegration:
    """Integration tests for HeartbeatService."""

    @pytest.mark.asyncio
    async def test_status_changes_propagate_correctly(self, mock_gateway):
        """Test that status changes from server are properly tracked."""
        service = HeartbeatService(
            gateway=mock_gateway,
            interval=30.0,
            report_by_buffer=1.5,
            initial_status="R",
        )

        # First heartbeat - no change
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="R")
        )
        update1 = await service.tick()
        assert update1.workflow_run_status is None
        assert service.current_status == "R"

        # Second heartbeat - server pauses
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="H")
        )
        update2 = await service.tick()
        assert update2.workflow_run_status == "H"
        assert service.current_status == "H"

        # Third heartbeat - still paused
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="H")
        )
        update3 = await service.tick()
        assert update3.workflow_run_status is None
        assert service.current_status == "H"

        # Fourth heartbeat - resumed
        mock_gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status="R")
        )
        update4 = await service.tick()
        assert update4.workflow_run_status == "R"
        assert service.current_status == "R"

    @pytest.mark.asyncio
    async def test_multiple_ticks_update_time_correctly(self, heartbeat_service):
        """Test that multiple ticks update last_heartbeat_time correctly."""
        times = []

        for _ in range(3):
            await heartbeat_service.tick()
            times.append(heartbeat_service.last_heartbeat_time)
            await asyncio.sleep(0.01)

        # Each tick should have a later timestamp
        assert times[0] <= times[1] <= times[2]
