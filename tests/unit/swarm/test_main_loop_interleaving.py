"""Main-loop interleaving test: drive 5-upstream→1-downstream with
realistic async background timing.

Uses the REAL WorkflowRunOrchestrator with a mock gateway that flips
upstream statuses to DONE in the background while the main loop runs.
If propagation has any interleaving bug, the downstream's
num_upstreams_done won't reach 5.
"""

from __future__ import annotations

import asyncio
import random
from typing import Dict, List, Set
from unittest.mock import AsyncMock, MagicMock

import pytest

from jobmon.client.swarm.gateway import (
    HeartbeatResponse,
    QueueResponse,
    StatusUpdateResponse,
    TaskStatusUpdatesResponse,
)
from jobmon.client.swarm.orchestrator import (
    OrchestratorConfig,
    WorkflowRunOrchestrator,
)
from jobmon.client.swarm.state import SwarmState
from jobmon.core.constants import TaskStatus, WorkflowRunStatus


class FakeSwarmTask:
    def __init__(self, task_id: int, status: str) -> None:
        self.task_id = task_id
        self.array_id = 1
        self.status = status
        self.downstream_swarm_tasks: Set["FakeSwarmTask"] = set()
        self.num_upstreams: int = 0
        self.num_upstreams_done: int = 0
        self.compute_resources_callable = None
        self.resource_scales = {}
        self.fallback_queues = []

        self.current_task_resources = MagicMock()
        self.current_task_resources.is_bound = True
        self.current_task_resources.id = 1
        self.current_task_resources.requested_resources = {}
        self.current_task_resources.queue = MagicMock()
        self.current_task_resources.coerce_resources = MagicMock(
            return_value=self.current_task_resources
        )
        self.current_task_resources.adjust_resources = MagicMock(
            return_value=self.current_task_resources
        )

        self.cluster = MagicMock()
        self.cluster.id = 1

    @property
    def all_upstreams_done(self) -> bool:
        if self.num_upstreams_done == self.num_upstreams:
            return True
        if self.num_upstreams_done > self.num_upstreams:
            raise RuntimeError("More upstream tasks done than exist in DAG.")
        return False

    def __hash__(self) -> int:
        return self.task_id

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeSwarmTask) and self.task_id == other.task_id


class FakeSwarmArray:
    def __init__(self, array_id: int) -> None:
        self.array_id = array_id
        self.array_name = f"array_{array_id}"
        self.max_concurrently_running = 100
        self.tasks: Set[FakeSwarmTask] = set()

    def add_task(self, task: FakeSwarmTask) -> None:
        self.tasks.add(task)


def build_state(
    downstream_id: int, upstream_ids: List[int]
) -> tuple[SwarmState, FakeSwarmTask, List[FakeSwarmTask]]:
    downstream = FakeSwarmTask(downstream_id, TaskStatus.REGISTERING)
    upstreams = [FakeSwarmTask(uid, TaskStatus.RUNNING) for uid in upstream_ids]
    for up in upstreams:
        up.downstream_swarm_tasks = {downstream}
    downstream.num_upstreams = len(upstreams)

    array = FakeSwarmArray(1)
    for t in [downstream, *upstreams]:
        array.add_task(t)

    state = SwarmState(
        workflow_id=1,
        workflow_run_id=10,
        dag_id=1,
        max_concurrently_running=100,
        status=WorkflowRunStatus.RUNNING,
    )
    for s in [
        TaskStatus.REGISTERING,
        TaskStatus.QUEUED,
        TaskStatus.INSTANTIATING,
        TaskStatus.LAUNCHED,
        TaskStatus.RUNNING,
        TaskStatus.DONE,
        TaskStatus.ADJUSTING_RESOURCES,
        TaskStatus.ERROR_FATAL,
    ]:
        state._task_status_map.setdefault(s, set())
    for t in [downstream, *upstreams]:
        state.tasks[t.task_id] = t  # type: ignore
        state._task_status_map[t.status].add(t)  # type: ignore
    state.arrays[1] = array  # type: ignore

    return state, downstream, upstreams


def make_gateway(server_state: Dict[int, str], clock: List[int]) -> MagicMock:
    """Mock gateway whose sync response reflects server_state.

    server_state: task_id -> current status (mutated externally to simulate progress)
    clock: single-element list holding the server's logical timestamp
    """
    gateway = MagicMock()
    last_snapshot: Dict[int, tuple[str, int]] = {}

    def tick():
        clock[0] += 1

    async def get_task_status_updates(since=None):
        tick()
        # Return all tasks each time — simpler than implementing incremental
        by_status: Dict[str, List[int]] = {}
        for tid, st in server_state.items():
            by_status.setdefault(st, []).append(tid)
        return TaskStatusUpdatesResponse(time=str(clock[0]), tasks_by_status=by_status)

    async def request_triage():
        tick()

    async def get_workflow_concurrency():
        tick()
        return 100

    async def get_array_concurrency(array_id):
        tick()
        return 100

    async def queue_task_batch(array_id, task_ids, task_resources_id, cluster_id):
        tick()
        # Transition each REGISTERING/ADJUSTING task to QUEUED
        by_status: Dict[str, List[int]] = {}
        for tid in task_ids:
            cur = server_state.get(tid, TaskStatus.REGISTERING)
            if cur in (TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES):
                server_state[tid] = TaskStatus.QUEUED
            by_status.setdefault(server_state[tid], []).append(tid)
        return QueueResponse(tasks_by_status=by_status)

    async def log_heartbeat(status, next_report_increment):
        tick()
        return HeartbeatResponse(status=WorkflowRunStatus.RUNNING)

    async def update_status(status):
        return StatusUpdateResponse(status=status)

    gateway.get_task_status_updates = AsyncMock(side_effect=get_task_status_updates)
    gateway.request_triage = AsyncMock(side_effect=request_triage)
    gateway.get_workflow_concurrency = AsyncMock(side_effect=get_workflow_concurrency)
    gateway.get_array_concurrency = AsyncMock(side_effect=get_array_concurrency)
    gateway.queue_task_batch = AsyncMock(side_effect=queue_task_batch)
    gateway.log_heartbeat = AsyncMock(side_effect=log_heartbeat)
    gateway.update_status = AsyncMock(side_effect=update_status)
    gateway.terminate_task_instances = AsyncMock()
    gateway._ensure_session = AsyncMock(return_value=MagicMock())

    return gateway


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(30))
async def test_main_loop_drives_5_upstreams_to_downstream(seed: int) -> None:
    """Drive the real _main_loop. Upstreams transition DONE on staggered
    timers; downstream's num_upstreams_done must reach 5.
    """
    random.seed(seed)

    downstream_id = 100
    upstream_ids = [1, 2, 3, 4, 5]
    state, downstream, upstreams = build_state(downstream_id, upstream_ids)

    # Mirror server-side status: all upstreams start in RUNNING (as in client)
    server_state: Dict[int, str] = {
        downstream_id: TaskStatus.REGISTERING,
        **{uid: TaskStatus.RUNNING for uid in upstream_ids},
    }
    clock = [0]
    gateway = make_gateway(server_state, clock)

    config = OrchestratorConfig(
        heartbeat_interval=0.01,
        heartbeat_report_by_buffer=1.5,
        wedged_workflow_sync_interval=0.05,
        fail_fast=False,
        timeout=30,
    )

    orch = WorkflowRunOrchestrator(
        state=state,
        gateway=gateway,
        config=config,
    )

    # Mock TaskResources bind
    for t in [downstream, *upstreams]:
        t.current_task_resources.bind_async = AsyncMock(return_value=None)

    # Random staggered DONE events
    async def drive_done_events():
        uids = list(upstream_ids)
        random.shuffle(uids)
        for uid in uids:
            await asyncio.sleep(random.uniform(0.005, 0.03))
            server_state[uid] = TaskStatus.DONE

    distributor_alive = MagicMock(return_value=True)

    driver = asyncio.create_task(drive_done_events())

    async def run_until_stuck_or_done():
        """Drive sync loops until all upstreams DONE or time runs out."""
        # Minimal lifecycle setup (we're not calling run())
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < 3.0:
            try:
                await orch._do_scheduling(timeout=0.01)
            except Exception:
                pass
            await asyncio.sleep(0.005)
            try:
                await orch._do_sync(full_sync=False)
            except Exception:
                pass

            all_up_done = all(
                server_state[uid] == TaskStatus.DONE for uid in upstream_ids
            )
            if all_up_done and downstream.num_upstreams_done == 5:
                return

    try:
        await run_until_stuck_or_done()
    finally:
        driver.cancel()
        try:
            await driver
        except asyncio.CancelledError:
            pass

    # All upstreams should be DONE in server state
    for uid in upstream_ids:
        assert (
            server_state[uid] == TaskStatus.DONE
        ), f"seed={seed}: upstream {uid} not DONE: {server_state[uid]}"

    assert downstream.num_upstreams_done == 5, (
        f"seed={seed}: downstream stuck at "
        f"{downstream.num_upstreams_done}/5. "
        f"downstream.status={downstream.status}"
    )


@pytest.mark.asyncio
async def test_full_sync_recovers_missed_done() -> None:
    """If an incremental sync somehow missed an upstream DONE,
    the next full sync should catch it and propagate.
    """
    downstream_id = 100
    upstream_ids = [1, 2, 3, 4, 5]
    state, downstream, upstreams = build_state(downstream_id, upstream_ids)

    # Server already has all 5 upstreams DONE
    server_state: Dict[int, str] = {
        downstream_id: TaskStatus.REGISTERING,
        **{uid: TaskStatus.DONE for uid in upstream_ids},
    }
    clock = [0]
    gateway = make_gateway(server_state, clock)

    config = OrchestratorConfig(
        heartbeat_interval=0.01,
        heartbeat_report_by_buffer=1.5,
        wedged_workflow_sync_interval=0.01,
        fail_fast=False,
        timeout=30,
    )

    orch = WorkflowRunOrchestrator(
        state=state,
        gateway=gateway,
        config=config,
    )

    for t in [downstream, *upstreams]:
        t.current_task_resources.bind_async = AsyncMock(return_value=None)

    # One full sync should be enough: client cache has upstreams as R,
    # server says D, apply_update sees the change, propagate fires
    await orch._do_sync(full_sync=True)

    assert downstream.num_upstreams_done == 5, (
        f"Full sync didn't propagate all DONEs: counter="
        f"{downstream.num_upstreams_done}/5"
    )
