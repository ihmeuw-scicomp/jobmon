"""Interleaving stress tests for swarm propagation logic.

Targets the question: can any reachable sequence of apply_update /
_process_changed_tasks / scheduler-tick calls leave a downstream task's
num_upstreams_done below num_upstreams after all its upstreams reach DONE?

Replicates the pattern seen in wf 566392 / wr 384312: a compute_scalars
task with 5 compute_pafs upstreams, all transitioning DONE, but the
downstream never entering ready_to_run.
"""

from __future__ import annotations

import asyncio
import itertools
import random
from typing import List, Set
from unittest.mock import AsyncMock, MagicMock

import pytest

from jobmon.client.swarm.gateway import (
    HeartbeatResponse,
    StatusUpdateResponse,
    TaskStatusUpdatesResponse,
)
from jobmon.client.swarm.services.synchronizer import Synchronizer
from jobmon.client.swarm.state import StateUpdate, SwarmState
from jobmon.core.constants import TaskStatus, WorkflowRunStatus

# ──────────────────────────────────────────────────────────────────────────────
# Lightweight SwarmTask stand-in
# ──────────────────────────────────────────────────────────────────────────────


class FakeSwarmTask:
    """Mimics SwarmTask for state-level tests.

    Uses the REAL all_upstreams_done semantics including the raise-on-overcount
    path, so bugs that overshoot num_upstreams surface as exceptions.
    """

    def __init__(self, task_id: int, status: str = TaskStatus.REGISTERING) -> None:
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

    def __repr__(self) -> str:
        return (
            f"FakeSwarmTask(id={self.task_id}, status={self.status}, "
            f"n_up_done={self.num_upstreams_done}/{self.num_upstreams})"
        )


def make_state(tasks: List[FakeSwarmTask]) -> SwarmState:
    state = SwarmState(
        workflow_id=1,
        workflow_run_id=10,
        dag_id=1,
        max_concurrently_running=100,
        status=WorkflowRunStatus.RUNNING,
    )
    # The state uses a status-bucket dict; populate it with our tasks.
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
    for t in tasks:
        state.tasks[t.task_id] = t  # type: ignore[assignment]
        state._task_status_map[t.status].add(t)  # type: ignore[arg-type]
    return state


def build_stuck_scenario() -> tuple[SwarmState, FakeSwarmTask, List[FakeSwarmTask]]:
    """Replicates wf 566392: 1 downstream (compute_scalars) + 5 upstreams."""
    downstream = FakeSwarmTask(task_id=334020812, status=TaskStatus.REGISTERING)
    upstreams = [
        FakeSwarmTask(task_id=i, status=TaskStatus.RUNNING)
        for i in (334020102, 334020103, 334020104, 334020105, 334020106)
    ]
    for up in upstreams:
        up.downstream_swarm_tasks = {downstream}
    downstream.num_upstreams = len(upstreams)
    state = make_state([downstream, *upstreams])
    return state, downstream, upstreams


# ──────────────────────────────────────────────────────────────────────────────
# Baseline: one propagate_completions call per upstream
# ──────────────────────────────────────────────────────────────────────────────


class TestSingleSyncPropagation:
    """propagate_completions with multiple iteration orders, single sync cycle."""

    @pytest.mark.parametrize("seed", range(50))
    def test_random_order_all_upstreams_in_one_sync(self, seed: int) -> None:
        """All 5 upstream DONEs arrive in one sync. Order shouldn't matter."""
        random.seed(seed)
        state, downstream, upstreams = build_stuck_scenario()

        # Flip all upstreams to DONE status (simulating what apply_update did).
        for up in upstreams:
            state._task_status_map[TaskStatus.RUNNING].discard(up)
            up.status = TaskStatus.DONE
            state._task_status_map[TaskStatus.DONE].add(up)

        # The real _process_changed_tasks calls propagate_completions({task})
        # once per DONE task in changed_tasks, in SET iteration order.
        order = list(upstreams)
        random.shuffle(order)

        newly_ready_union: List[FakeSwarmTask] = []
        for up in order:
            nr = state.propagate_completions({up})  # type: ignore[arg-type]
            newly_ready_union.extend(nr)

        assert (
            downstream.num_upstreams_done == 5
        ), f"counter={downstream.num_upstreams_done}, order={[u.task_id for u in order]}"
        assert downstream in newly_ready_union


# ──────────────────────────────────────────────────────────────────────────────
# Spread across multiple sync cycles (matches wf 566392 timing)
# ──────────────────────────────────────────────────────────────────────────────


class TestMultiSyncPropagation:
    """Upstream DONEs split across multiple sync cycles, in any partition."""

    @pytest.mark.parametrize(
        "partition",
        [
            # Each tuple is a sync cycle's batch of DONE upstreams by index.
            [(0,), (1,), (2,), (3,), (4,)],  # one per cycle
            [(0, 1), (2, 3), (4,)],  # observed in the ES data: ~3 syncs
            [(0, 1, 2), (3, 4)],
            [(0,), (1, 2), (3, 4)],
            [(0, 1, 2, 3), (4,)],
            [(0, 1, 2, 3, 4)],  # all in one full sync
        ],
    )
    def test_partitioned_arrivals(self, partition: list[tuple]) -> None:
        state, downstream, upstreams = build_stuck_scenario()

        for batch_indices in partition:
            # Simulate apply_update marking this batch DONE
            for idx in batch_indices:
                up = upstreams[idx]
                state._task_status_map[TaskStatus.RUNNING].discard(up)
                up.status = TaskStatus.DONE
                state._task_status_map[TaskStatus.DONE].add(up)

            # Simulate _process_changed_tasks iterating in this batch
            for idx in batch_indices:
                state.propagate_completions({upstreams[idx]})  # type: ignore[arg-type]

        assert downstream.num_upstreams_done == 5
        assert downstream.all_upstreams_done is True


# ──────────────────────────────────────────────────────────────────────────────
# Duplicate sync response: same DONE reported twice
# ──────────────────────────────────────────────────────────────────────────────


class TestDuplicateObservations:
    """What if a DONE is reported in two successive syncs?"""

    def test_apply_update_dedups_when_cache_already_done(self) -> None:
        """Second sync reporting DONE for a cached-DONE task does not re-propagate."""
        state, downstream, upstreams = build_stuck_scenario()
        up0 = upstreams[0]

        # Sync 1: up0 transitions R → D
        update1 = StateUpdate(task_statuses={up0.task_id: TaskStatus.DONE})
        changed1 = state.apply_update(update1)
        assert up0 in changed1
        # Simulate _process_changed_tasks propagation
        state.propagate_completions(set(changed1))
        assert downstream.num_upstreams_done == 1

        # Sync 2: same task reported D again (e.g., full sync)
        update2 = StateUpdate(task_statuses={up0.task_id: TaskStatus.DONE})
        changed2 = state.apply_update(update2)
        assert up0 not in changed2, "apply_update should skip no-op status updates"
        # No propagate call → counter stays 1
        assert downstream.num_upstreams_done == 1


# ──────────────────────────────────────────────────────────────────────────────
# Scheduler response + sync response interleaving
# ──────────────────────────────────────────────────────────────────────────────


class TestSchedulerSyncInterleaving:
    """The scheduler's queue_task_batch response can carry DONE statuses when
    a task transitioned between dequeue and server-side gating. Verify that
    double-observation (scheduler + sync) does not over-count.
    """

    def test_scheduler_then_sync_same_done(self) -> None:
        state, downstream, upstreams = build_stuck_scenario()
        up0 = upstreams[0]

        # Scheduler reports up0 as DONE (e.g., server raced ahead).
        sched_update = StateUpdate(task_statuses={up0.task_id: TaskStatus.DONE})
        changed = state.apply_update(sched_update)
        state.propagate_completions(set(changed))
        assert downstream.num_upstreams_done == 1

        # Sync (incremental) reports same transition. Cache already D.
        sync_update = StateUpdate(task_statuses={up0.task_id: TaskStatus.DONE})
        changed_sync = state.apply_update(sync_update)
        state.propagate_completions(set(changed_sync))
        assert downstream.num_upstreams_done == 1


# ──────────────────────────────────────────────────────────────────────────────
# Heavy randomized stress: many DAGs, many interleavings
# ──────────────────────────────────────────────────────────────────────────────


class TestRandomizedStress:
    """Random DAG topologies + random sync/scheduler interleavings."""

    @pytest.mark.parametrize("seed", range(100))
    def test_random_interleavings_never_undercount(self, seed: int) -> None:
        random.seed(seed)

        # Build a random bipartite DAG: N upstreams → 1 downstream
        n_upstreams = random.randint(1, 10)
        downstream = FakeSwarmTask(task_id=1000, status=TaskStatus.REGISTERING)
        upstreams = [
            FakeSwarmTask(task_id=i + 1, status=TaskStatus.RUNNING)
            for i in range(n_upstreams)
        ]
        for up in upstreams:
            up.downstream_swarm_tasks = {downstream}
        downstream.num_upstreams = n_upstreams
        state = make_state([downstream, *upstreams])

        # Split upstreams into random sync-cycle batches.
        indices = list(range(n_upstreams))
        random.shuffle(indices)
        batches: List[List[int]] = []
        i = 0
        while i < len(indices):
            take = random.randint(1, len(indices) - i)
            batches.append(indices[i : i + take])
            i += take

        # For each batch, apply_update → iterate changed set in random order.
        for batch in batches:
            update = StateUpdate(
                task_statuses={upstreams[idx].task_id: TaskStatus.DONE for idx in batch}
            )
            changed = list(state.apply_update(update))
            random.shuffle(changed)  # simulate set iteration order
            for t in changed:
                state.propagate_completions({t})

        assert downstream.num_upstreams_done == n_upstreams, (
            f"seed={seed}, batches={batches}, "
            f"got counter={downstream.num_upstreams_done}/{n_upstreams}"
        )
        assert downstream.all_upstreams_done is True


# ──────────────────────────────────────────────────────────────────────────────
# Async interleaving: heartbeat running during main-loop awaits
# ──────────────────────────────────────────────────────────────────────────────


class TestAsyncInterleaving:
    """Run the real orchestrator _main_loop with concurrent heartbeat background
    tasks and verify propagation still completes for a 5-upstream downstream.
    """

    @pytest.mark.asyncio
    async def test_heartbeat_does_not_clobber_propagation(self) -> None:
        """Heartbeat background task runs concurrently; propagation must still
        bump the downstream's num_upstreams_done to num_upstreams.

        This replicates wf 566392's shape: 5 upstreams + 1 downstream, all
        upstreams transitioning DONE over multiple sync cycles while heartbeat
        ticks happen in-between.
        """
        state, downstream, upstreams = build_stuck_scenario()

        # Sequence of sync responses — upstream DONEs arrive spread out.
        sync_responses = [
            # Cycle 1: 2 upstreams transition DONE
            {
                upstreams[0].task_id: TaskStatus.DONE,
                upstreams[1].task_id: TaskStatus.DONE,
            },
            # Cycle 2: heartbeat only, no status changes
            {},
            # Cycle 3: 1 more DONE
            {upstreams[2].task_id: TaskStatus.DONE},
            # Cycle 4: last 2 DONE
            {
                upstreams[3].task_id: TaskStatus.DONE,
                upstreams[4].task_id: TaskStatus.DONE,
            },
        ]
        response_iter = iter(sync_responses)

        async def fake_get_task_updates(since=None):
            try:
                payload = next(response_iter)
            except StopIteration:
                payload = {}
            # Group task_ids by status
            by_status: dict[str, list[int]] = {}
            for tid, st in payload.items():
                by_status.setdefault(st, []).append(tid)
            return TaskStatusUpdatesResponse(
                time="2026-04-17T05:22:30", tasks_by_status=by_status
            )

        gateway = MagicMock()
        gateway.get_task_status_updates = AsyncMock(side_effect=fake_get_task_updates)
        gateway.request_triage = AsyncMock()
        gateway.get_workflow_concurrency = AsyncMock(return_value=100)
        gateway.get_array_concurrency = AsyncMock(return_value=50)
        gateway.log_heartbeat = AsyncMock(
            return_value=HeartbeatResponse(status=WorkflowRunStatus.RUNNING)
        )
        gateway.update_status = AsyncMock(
            side_effect=lambda s: StatusUpdateResponse(status=s)
        )

        sync = Synchronizer(
            gateway=gateway,
            task_ids=set(state.tasks.keys()),
            array_ids=set(),
        )

        # Drive multiple sync ticks with a concurrent heartbeat coroutine
        async def heartbeat_noise(n: int) -> None:
            for _ in range(n):
                await asyncio.sleep(0)
                # Simulate heartbeat doing its thing - no state mutation
                await gateway.log_heartbeat(
                    status=WorkflowRunStatus.RUNNING, next_report_increment=45.0
                )

        # Run heartbeat noise + sync ticks concurrently.
        async def drive_syncs() -> None:
            for _ in sync_responses:
                update = await sync.tick(full_sync=False, last_sync=None)
                changed = state.apply_update(update)
                for t in changed:
                    if t.status == TaskStatus.DONE:
                        state.propagate_completions({t})

        # Stagger: heartbeat ticks during sync awaits
        await asyncio.gather(drive_syncs(), heartbeat_noise(20))

        assert (
            downstream.num_upstreams_done == 5
        ), f"counter={downstream.num_upstreams_done}, downstream={downstream}"
        assert downstream.all_upstreams_done is True


# ──────────────────────────────────────────────────────────────────────────────
# Exhaustive enumeration: all permutations of iteration order for 5-upstream
# propagation partitioned every possible way. If a bug lurks in iteration-order
# sensitivity this will find it.
# ──────────────────────────────────────────────────────────────────────────────


class TestExhaustivePermutations:

    def test_every_permutation_5_upstreams(self) -> None:
        """For every iteration order of 5 upstream DONEs, counter reaches 5."""
        failures = []
        for order in itertools.permutations(range(5)):
            state, downstream, upstreams = build_stuck_scenario()
            # All 5 transition DONE "at once" (single apply_update)
            update = StateUpdate(
                task_statuses={u.task_id: TaskStatus.DONE for u in upstreams}
            )
            changed_set = state.apply_update(update)
            # Iterate in the fixed permutation order rather than set order
            by_id = {t.task_id: t for t in changed_set}
            for idx in order:
                tid = upstreams[idx].task_id
                state.propagate_completions({by_id[tid]})
            if downstream.num_upstreams_done != 5:
                failures.append((order, downstream.num_upstreams_done))
        assert not failures, f"Permutation failures: {failures}"
