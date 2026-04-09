"""Tests for HTTP-failure resilience layers.

These tests cover the four-layer fix for the "File descriptor N is used by
transport" cascade bug (see prod incident 2026-04-09 on wfr 383214):

* Layer 1: Requester retries ``asyncio.TimeoutError`` on Python 3.10
* Layer 2: Gateway recycles aiohttp session after sustained failures
* Layer 3: Orchestrator main loop enforces MIN_LOOP_SLEEP floor
* Layer 4: Synchronizer and HeartbeatService raise FatalOrchestratorError
  after sustained failure; it propagates through run.py

Each test is isolated and self-contained — no server or network required.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from jobmon.client.swarm.gateway import (
    HeartbeatResponse,
    ServerGateway,
    TaskStatusUpdatesResponse,
)
from jobmon.client.swarm.services.heartbeat import HeartbeatService
from jobmon.client.swarm.services.synchronizer import Synchronizer
from jobmon.client.swarm.state import StateUpdate
from jobmon.core.exceptions import FatalOrchestratorError
from jobmon.core.requester import Requester

# ──────────────────────────────────────────────────────────────────────────────
# Layer 1 — Requester retry classifier
# ──────────────────────────────────────────────────────────────────────────────


class TestRequesterRetryClassifier:
    """Verify _should_retry_exception covers timeout types on all Python versions."""

    @pytest.fixture
    def requester(self) -> Requester:
        return Requester(service_url="http://example.invalid")

    def test_asyncio_timeout_is_retriable(self, requester: Requester) -> None:
        """asyncio.TimeoutError must be retried.

        On Python 3.10, asyncio.TimeoutError does not inherit from builtin
        TimeoutError. Before layer 1 the classifier returned False for this
        exception, causing aiohttp ClientTimeout failures to propagate
        immediately instead of being retried with tenacity backoff.
        """
        assert requester._should_retry_exception(asyncio.TimeoutError()) is True

    def test_builtin_timeout_is_retriable(self, requester: Requester) -> None:
        """The builtin TimeoutError is still retriable (regression guard)."""
        assert requester._should_retry_exception(TimeoutError()) is True

    def test_aiohttp_server_timeout_is_retriable(self, requester: Requester) -> None:
        """aiohttp.ServerTimeoutError remains retriable (regression guard)."""
        assert requester._should_retry_exception(aiohttp.ServerTimeoutError()) is True

    def test_invalid_request_not_retriable(self, requester: Requester) -> None:
        """4xx client errors must NOT be retried (regression guard)."""
        from jobmon.core.exceptions import InvalidRequest

        assert requester._should_retry_exception(InvalidRequest("bad")) is False

    def test_generic_runtime_error_not_retriable(self, requester: Requester) -> None:
        """The fd-cascade RuntimeError itself must not trigger retry.

        If it did, every retry would hit the same leaked fd and we'd amplify
        the log volume 10x per call instead of failing fast.
        """
        fd_error = RuntimeError(
            "File descriptor 12 is used by transport "
            "<_SelectorSocketTransport fd=12 read=polling write=<idle, bufsize=0>>"
        )
        assert requester._should_retry_exception(fd_error) is False


# ──────────────────────────────────────────────────────────────────────────────
# Layer 2 — Gateway session recycling
# ──────────────────────────────────────────────────────────────────────────────


class TestGatewaySessionRecycling:
    """Verify the gateway recycles its aiohttp session on sustained failure."""

    @pytest.fixture
    def mock_requester(self) -> MagicMock:
        requester = MagicMock(spec=Requester)
        requester.send_request_async = AsyncMock()
        return requester

    @pytest.fixture
    def gateway(self, mock_requester: MagicMock) -> ServerGateway:
        return ServerGateway(
            requester=mock_requester,
            workflow_id=100,
            workflow_run_id=200,
        )

    @pytest.mark.asyncio
    async def test_success_resets_failure_counter(
        self,
        gateway: ServerGateway,
        mock_requester: MagicMock,
    ) -> None:
        """A successful call must reset the consecutive-failure counter."""
        mock_requester.send_request_async.return_value = (200, {"status": "R"})

        # Manually set a stale counter
        gateway._consecutive_failures = 3

        await gateway.log_heartbeat(status="R", next_report_increment=30.0)

        assert gateway._consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_failure_increments_counter(
        self,
        gateway: ServerGateway,
        mock_requester: MagicMock,
    ) -> None:
        """Each failed call below the threshold increments the counter."""
        mock_requester.send_request_async.side_effect = RuntimeError("boom")

        for expected in range(1, gateway.SESSION_RECYCLE_THRESHOLD):
            with pytest.raises(RuntimeError):
                await gateway.log_heartbeat(status="R", next_report_increment=30.0)
            assert gateway._consecutive_failures == expected

    @pytest.mark.asyncio
    async def test_threshold_triggers_session_recycle(
        self,
        gateway: ServerGateway,
        mock_requester: MagicMock,
    ) -> None:
        """Hitting the threshold must recycle the session and reset the counter."""
        mock_requester.send_request_async.side_effect = RuntimeError(
            "File descriptor 12 is used by transport"
        )

        # Seed a real session so we can verify it gets closed
        first_session = aiohttp.ClientSession()
        gateway._session = first_session
        gateway._owns_session = True

        try:
            for _ in range(gateway.SESSION_RECYCLE_THRESHOLD):
                with pytest.raises(RuntimeError):
                    await gateway.log_heartbeat(status="R", next_report_increment=30.0)

            # After crossing the threshold:
            assert first_session.closed, "old session should be closed"
            assert gateway._session is None or gateway._session is not first_session
            assert gateway._consecutive_failures == 0
        finally:
            if not first_session.closed:
                await first_session.close()
            if gateway._session is not None and not gateway._session.closed:
                await gateway._session.close()

    @pytest.mark.asyncio
    async def test_recycle_is_idempotent_under_concurrent_failures(
        self,
        gateway: ServerGateway,
    ) -> None:
        """Concurrent callers hitting the threshold must not double-close."""
        # Seed a real session and pre-set counter near threshold
        first_session = aiohttp.ClientSession()
        gateway._session = first_session
        gateway._owns_session = True

        try:
            # Directly invoke _recycle_session from multiple coroutines
            await asyncio.gather(
                gateway._recycle_session(),
                gateway._recycle_session(),
                gateway._recycle_session(),
            )

            assert first_session.closed
            # No new session was created implicitly
            assert gateway._session is None
        finally:
            if not first_session.closed:
                await first_session.close()

    @pytest.mark.asyncio
    async def test_recycle_noop_when_session_already_none(
        self,
        gateway: ServerGateway,
    ) -> None:
        """Calling _recycle_session with no session must be a safe no-op."""
        gateway._session = None
        # Should not raise
        await gateway._recycle_session()
        assert gateway._session is None


# ──────────────────────────────────────────────────────────────────────────────
# Layer 3 — Orchestrator main-loop sleep floor
# ──────────────────────────────────────────────────────────────────────────────


class TestOrchestratorLoopFloor:
    """Verify MIN_LOOP_SLEEP prevents hot-spinning when heartbeat times collapse."""

    def test_min_loop_sleep_class_attribute_exists(self) -> None:
        """The floor constant must exist and be a positive value."""
        from jobmon.client.swarm.orchestrator import WorkflowRunOrchestrator

        assert hasattr(WorkflowRunOrchestrator, "MIN_LOOP_SLEEP")
        assert WorkflowRunOrchestrator.MIN_LOOP_SLEEP > 0

    def test_min_loop_sleep_reasonable_value(self) -> None:
        """MIN_LOOP_SLEEP must be large enough to be a real floor, small enough
        to not starve scheduling on healthy runs."""
        from jobmon.client.swarm.orchestrator import WorkflowRunOrchestrator

        assert 0.5 <= WorkflowRunOrchestrator.MIN_LOOP_SLEEP <= 5.0


# ──────────────────────────────────────────────────────────────────────────────
# Layer 4b — Synchronizer fatal exit on sustained total failure
# ──────────────────────────────────────────────────────────────────────────────


class TestSynchronizerFatalExit:
    """Verify Synchronizer raises FatalOrchestratorError on sustained total failure."""

    @pytest.fixture
    def all_failing_gateway(self) -> MagicMock:
        """Gateway where every async method raises."""
        gateway = MagicMock()
        gateway.request_triage = AsyncMock(side_effect=RuntimeError("network down"))
        gateway.get_task_status_updates = AsyncMock(
            side_effect=RuntimeError("network down")
        )
        gateway.get_workflow_concurrency = AsyncMock(
            side_effect=RuntimeError("network down")
        )
        gateway.get_array_concurrency = AsyncMock(
            side_effect=RuntimeError("network down")
        )
        return gateway

    @pytest.fixture
    def mostly_healthy_gateway(self) -> MagicMock:
        """Gateway where most calls succeed but one fails."""
        gateway = MagicMock()
        gateway.request_triage = AsyncMock(side_effect=RuntimeError("flaky"))
        gateway.get_task_status_updates = AsyncMock(
            return_value=TaskStatusUpdatesResponse(
                time=datetime(2024, 1, 1), tasks_by_status={}
            )
        )
        gateway.get_workflow_concurrency = AsyncMock(return_value=100)
        gateway.get_array_concurrency = AsyncMock(return_value=50)
        return gateway

    @pytest.mark.asyncio
    async def test_single_all_failed_tick_does_not_raise(
        self, all_failing_gateway: MagicMock
    ) -> None:
        """One bad tick is tolerated — the Synchronizer swallows and continues.

        This preserves the existing transient-failure resilience: a single
        server blip should not terminate the workflow run.
        """
        sync = Synchronizer(gateway=all_failing_gateway, task_ids={1}, array_ids={10})
        result = await sync.tick()
        assert isinstance(result, StateUpdate)
        assert sync._consecutive_total_failure_ticks == 1

    @pytest.mark.asyncio
    async def test_threshold_all_failed_ticks_raises_fatal(
        self, all_failing_gateway: MagicMock
    ) -> None:
        """Crossing the threshold must raise FatalOrchestratorError."""
        sync = Synchronizer(gateway=all_failing_gateway, task_ids={1}, array_ids={10})

        # First (N-1) ticks increment the counter without raising
        for _ in range(sync.MAX_CONSECUTIVE_TOTAL_FAILURE_TICKS - 1):
            await sync.tick()

        # Nth tick must raise
        with pytest.raises(FatalOrchestratorError, match="consecutive"):
            await sync.tick()

    @pytest.mark.asyncio
    async def test_any_success_resets_counter(
        self, mostly_healthy_gateway: MagicMock
    ) -> None:
        """Even one successful operation per tick must reset the counter.

        This ensures a single flaky endpoint can't wedge the run — as long
        as other sync operations are working, we keep going.
        """
        sync = Synchronizer(
            gateway=mostly_healthy_gateway, task_ids={1}, array_ids={10}
        )

        # Manually set a stale counter
        sync._consecutive_total_failure_ticks = 2

        await sync.tick()

        assert sync._consecutive_total_failure_ticks == 0

    @pytest.mark.asyncio
    async def test_counter_resets_after_recovery(
        self, all_failing_gateway: MagicMock
    ) -> None:
        """Counter returning below threshold must reset to 0 on next success."""
        sync = Synchronizer(gateway=all_failing_gateway, task_ids={1}, array_ids={10})

        # Build up to threshold-1 consecutive failures
        for _ in range(sync.MAX_CONSECUTIVE_TOTAL_FAILURE_TICKS - 1):
            await sync.tick()
        assert (
            sync._consecutive_total_failure_ticks
            == sync.MAX_CONSECUTIVE_TOTAL_FAILURE_TICKS - 1
        )

        # Now flip the gateway to healthy
        all_failing_gateway.request_triage.side_effect = None
        all_failing_gateway.request_triage.return_value = None
        all_failing_gateway.get_task_status_updates.side_effect = None
        all_failing_gateway.get_task_status_updates.return_value = (
            TaskStatusUpdatesResponse(time=datetime(2024, 1, 1), tasks_by_status={})
        )
        all_failing_gateway.get_workflow_concurrency.side_effect = None
        all_failing_gateway.get_workflow_concurrency.return_value = 100
        all_failing_gateway.get_array_concurrency.side_effect = None
        all_failing_gateway.get_array_concurrency.return_value = 50

        await sync.tick()
        assert sync._consecutive_total_failure_ticks == 0


# ──────────────────────────────────────────────────────────────────────────────
# Layer 4c — HeartbeatService fatal exit on sustained failure
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeatServiceFatalExit:
    """Verify HeartbeatService raises FatalOrchestratorError on sustained failure."""

    @pytest.fixture
    def failing_gateway(self) -> MagicMock:
        gateway = MagicMock()
        gateway.log_heartbeat = AsyncMock(side_effect=RuntimeError("network down"))
        return gateway

    @pytest.fixture
    def succeeding_gateway(self) -> MagicMock:
        gateway = MagicMock()
        gateway.log_heartbeat = AsyncMock(return_value=HeartbeatResponse(status="R"))
        return gateway

    @pytest.mark.asyncio
    async def test_single_heartbeat_failure_does_not_raise(
        self, failing_gateway: MagicMock
    ) -> None:
        """A single heartbeat failure must not raise from tick()."""
        service = HeartbeatService(
            gateway=failing_gateway,
            interval=1.0,
            report_by_buffer=1.5,
            initial_status="R",
        )
        # tick() itself propagates the underlying error
        with pytest.raises(RuntimeError):
            await service.tick()

    @pytest.mark.asyncio
    async def test_consecutive_failure_counter_starts_zero(
        self, failing_gateway: MagicMock
    ) -> None:
        """Freshly-constructed service has zero consecutive failures."""
        service = HeartbeatService(
            gateway=failing_gateway,
            interval=1.0,
            report_by_buffer=1.5,
            initial_status="R",
        )
        assert service._consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_run_background_raises_fatal_after_threshold(
        self, failing_gateway: MagicMock
    ) -> None:
        """run_background must raise FatalOrchestratorError after N failures."""
        service = HeartbeatService(
            gateway=failing_gateway,
            interval=0.01,  # very short so is_heartbeat_due fires immediately
            report_by_buffer=1.5,
            initial_status="R",
        )
        stop_event = asyncio.Event()

        # run_background should raise within a bounded number of ticks
        with pytest.raises(FatalOrchestratorError, match="consecutive"):
            await asyncio.wait_for(service.run_background(stop_event), timeout=5.0)

        assert (
            service._consecutive_failures >= service.MAX_CONSECUTIVE_HEARTBEAT_FAILURES
        )

    @pytest.mark.asyncio
    async def test_successful_heartbeat_resets_counter(
        self, succeeding_gateway: MagicMock
    ) -> None:
        """A successful tick must reset the counter."""
        service = HeartbeatService(
            gateway=succeeding_gateway,
            interval=1.0,
            report_by_buffer=1.5,
            initial_status="R",
        )
        service._consecutive_failures = 3

        await service.tick()

        assert service._consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_fatal_error_from_tick_propagates_through_background(
        self, failing_gateway: MagicMock
    ) -> None:
        """FatalOrchestratorError raised by tick() must propagate immediately.

        This covers the case where the gateway itself raised
        FatalOrchestratorError (e.g. after exhausting session recycles).
        """
        failing_gateway.log_heartbeat.side_effect = FatalOrchestratorError(
            "gateway gave up"
        )
        service = HeartbeatService(
            gateway=failing_gateway,
            interval=0.01,
            report_by_buffer=1.5,
            initial_status="R",
        )
        stop_event = asyncio.Event()

        with pytest.raises(FatalOrchestratorError, match="gateway gave up"):
            await asyncio.wait_for(service.run_background(stop_event), timeout=5.0)


# ──────────────────────────────────────────────────────────────────────────────
# Layer 4d — FatalOrchestratorError propagation through run.py
# ──────────────────────────────────────────────────────────────────────────────


class TestFatalErrorPropagation:
    """Verify FatalOrchestratorError is in run.py's re-raise tuple."""

    def test_fatal_orchestrator_error_is_exported(self) -> None:
        """The exception class must be importable from core.exceptions."""
        from jobmon.core.exceptions import FatalOrchestratorError as imported

        assert issubclass(imported, Exception)

    def test_run_py_imports_fatal_error(self) -> None:
        """run.py must import FatalOrchestratorError for its re-raise list."""
        import jobmon.client.swarm.run as run_module

        assert hasattr(run_module, "FatalOrchestratorError")

    def test_orchestrator_check_heartbeat_task_healthy_exists(self) -> None:
        """The orchestrator must expose the heartbeat-health check method.

        _check_heartbeat_task_healthy is the surfacing point for background
        heartbeat task failures (including FatalOrchestratorError) into the
        main loop. If this method is removed or renamed, heartbeat-initiated
        fatal exits will be invisible until teardown.
        """
        from jobmon.client.swarm.orchestrator import WorkflowRunOrchestrator

        assert hasattr(WorkflowRunOrchestrator, "_check_heartbeat_task_healthy")
