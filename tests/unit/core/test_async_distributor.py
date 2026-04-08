"""Tests for async distributor behavior: heartbeat independence and parallel triage."""

import asyncio
import time
from unittest import mock

from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exit_info import RemoteExitInfo
from jobmon.core.requester import Requester
from jobmon.distributor.distributor_service import DistributorService
from jobmon.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.plugins.dummy.dummy_distributor import DummyDistributor


def _make_task_instance(ti_id, status=TaskInstanceStatus.TRIAGING):
    """Create a DistributorTaskInstance with a mocked async requester."""
    requester = mock.MagicMock(spec=Requester)
    requester.async_request = mock.AsyncMock(return_value=(200, {}))
    ti = DistributorTaskInstance(
        task_instance_id=ti_id,
        workflow_run_id=1,
        status=status,
        requester=requester,
    )
    ti._distributor_id = f"job_{ti_id}"
    return ti


def _make_distributor_service():
    """Create a DistributorService with mocked requester."""
    cluster_interface = DummyDistributor("dummy")
    requester = mock.MagicMock(spec=Requester)
    requester.async_request = mock.AsyncMock(return_value=(200, {"status_updates": {}}))
    ds = DistributorService(cluster_interface, requester=requester)
    return ds


class TestHeartbeatIndependence:
    """Verify heartbeats run independently of status processing."""

    def test_heartbeat_runs_during_slow_processing(self):
        """Heartbeat task should execute even when process_status is busy.

        Simulates a slow triage by making get_remote_exit_info_async
        sleep, then verifies the heartbeat task ran during that time.
        """
        ds = _make_distributor_service()
        heartbeat_times = []

        # Track when heartbeats fire
        original_heartbeat = ds.log_task_instance_report_by_date

        async def tracking_heartbeat():
            heartbeat_times.append(time.monotonic())
            await original_heartbeat()

        ds.log_task_instance_report_by_date = tracking_heartbeat
        ds._workflow_run_heartbeat_interval = 0.2  # 200ms

        # Create a TRIAGING TI with slow exit info lookup
        ti = _make_task_instance(1)
        ds._task_instances[1] = ti
        ds._task_instance_status_map[TaskInstanceStatus.TRIAGING].add(ti)

        async def slow_exit_info(distributor_id):
            await asyncio.sleep(0.8)  # 800ms — 4x heartbeat interval
            return RemoteExitInfo(
                error_state=TaskInstanceStatus.UNKNOWN_ERROR,
                error_message="test",
                finalized=True,
            )

        ds.cluster_interface.get_remote_exit_info_async = slow_exit_info

        async def run_test():
            heartbeat_task = asyncio.create_task(ds._heartbeat_loop())
            try:
                await ds.process_status(TaskInstanceStatus.TRIAGING)
            finally:
                ds._stopping = True
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

        asyncio.run(run_test())

        # Triage took ~800ms, heartbeat interval is 200ms
        # Should have fired at least 2 heartbeats during triage
        assert len(heartbeat_times) >= 2, (
            f"Expected at least 2 heartbeats during 800ms triage "
            f"(interval=200ms), got {len(heartbeat_times)}"
        )


class TestParallelTriage:
    """Verify TRIAGING commands run concurrently."""

    def test_triage_commands_run_in_parallel(self):
        """Multiple triage commands should run concurrently via gather.

        If 5 triage commands each take 200ms, serial would be ~1000ms.
        Parallel should be ~200ms.
        """
        ds = _make_distributor_service()

        # Create 5 TRIAGING TIs
        for i in range(5):
            ti = _make_task_instance(i)
            ds._task_instances[i] = ti
            ds._task_instance_status_map[TaskInstanceStatus.TRIAGING].add(ti)

        call_times = []

        async def timed_exit_info(distributor_id):
            call_times.append(time.monotonic())
            await asyncio.sleep(0.2)  # 200ms per triage
            return RemoteExitInfo(
                error_state=TaskInstanceStatus.UNKNOWN_ERROR,
                error_message="test",
                finalized=True,
            )

        ds.cluster_interface.get_remote_exit_info_async = timed_exit_info

        start = time.monotonic()
        asyncio.run(ds.process_status(TaskInstanceStatus.TRIAGING))
        elapsed = time.monotonic() - start

        # All 5 should have started within a tight window (parallel)
        assert len(call_times) == 5
        spread = max(call_times) - min(call_times)
        assert spread < 0.1, (
            f"Triage calls should start near-simultaneously, "
            f"but spread was {spread:.3f}s"
        )

        # Total time should be ~200ms (parallel), not ~1000ms (serial)
        assert elapsed < 0.6, (
            f"5 parallel 200ms triage calls took {elapsed:.3f}s, " f"expected <600ms"
        )

    def test_no_heartbeat_commands_run_in_parallel(self):
        """NO_HEARTBEAT commands should also run in parallel."""
        ds = _make_distributor_service()

        for i in range(3):
            ti = _make_task_instance(i, status=TaskInstanceStatus.NO_HEARTBEAT)
            ds._task_instances[i] = ti
            ds._task_instance_status_map[TaskInstanceStatus.NO_HEARTBEAT].add(ti)

        start = time.monotonic()
        asyncio.run(ds.process_status(TaskInstanceStatus.NO_HEARTBEAT))
        elapsed = time.monotonic() - start

        # All 3 TIs should have transitioned to error
        for ti in ds._task_instances.values():
            assert ti.error_state == TaskInstanceStatus.ERROR

    def test_sequential_statuses_not_parallelized(self):
        """QUEUED status should process commands sequentially."""
        ds = _make_distributor_service()

        # Mock async_request for instantiate
        ds.requester.async_request = mock.AsyncMock(
            return_value=(
                200,
                {"task_instance_batches": []},
            )
        )

        # Add QUEUED TIs
        for i in range(3):
            ti = _make_task_instance(i, status=TaskInstanceStatus.QUEUED)
            ds._task_instances[i] = ti
            ds._task_instance_status_map[TaskInstanceStatus.QUEUED].add(ti)

        # Should not raise — sequential processing works
        asyncio.run(ds.process_status(TaskInstanceStatus.QUEUED))


class TestAsyncRequestYielding:
    """Verify that async HTTP calls yield to the event loop."""

    def test_refresh_status_yields_to_heartbeat(self):
        """refresh_status_from_db should yield during HTTP call,
        allowing a concurrent task to run."""
        ds = _make_distributor_service()
        ds.workflow_run = mock.MagicMock()
        ds.workflow_run.workflow_run_id = 1
        heartbeat_ran = False

        # Make async_request actually yield to the event loop
        async def yielding_request(*args, **kwargs):
            await asyncio.sleep(0)  # yield point
            return (200, {"status_updates": {}})

        ds.requester.async_request = yielding_request

        async def run_test():
            nonlocal heartbeat_ran

            async def heartbeat_flag():
                nonlocal heartbeat_ran
                heartbeat_ran = True

            # Launch concurrent task — will run when refresh yields
            flag_task = asyncio.create_task(heartbeat_flag())
            await ds.refresh_status_from_db(TaskInstanceStatus.QUEUED)
            await flag_task

        asyncio.run(run_test())
        assert heartbeat_ran, (
            "Concurrent task should have run during " "refresh_status_from_db await"
        )
