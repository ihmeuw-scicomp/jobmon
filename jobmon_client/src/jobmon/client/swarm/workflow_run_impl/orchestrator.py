"""WorkflowRunOrchestrator: Main event loop coordinator for workflow runs.

This module provides the orchestrator that coordinates all workflow run services
(heartbeat, synchronization, scheduling) and manages the main execution loop.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Optional

import structlog

from jobmon.client.swarm.workflow_run_impl.services.heartbeat import HeartbeatService
from jobmon.client.swarm.workflow_run_impl.services.scheduler import Scheduler
from jobmon.client.swarm.workflow_run_impl.services.synchronizer import Synchronizer
from jobmon.client.swarm.workflow_run_impl.state import StateUpdate
from jobmon.core.constants import TaskStatus, WorkflowRunStatus
from jobmon.core.exceptions import (
    CallableReturnedInvalidObject,
    DistributorNotAlive,
    TransitionError,
    WorkflowTestError,
)

if TYPE_CHECKING:
    from jobmon.client.swarm.swarm_array import SwarmArray
    from jobmon.client.swarm.swarm_task import SwarmTask
    from jobmon.client.swarm.workflow_run_impl.gateway import ServerGateway
    from jobmon.client.task_resources import TaskResources

logger = structlog.get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

# Task statuses representing "in-flight" work (used for capacity calculations).
ACTIVE_TASK_STATUSES: tuple[str, ...] = (
    TaskStatus.QUEUED,
    TaskStatus.INSTANTIATING,
    TaskStatus.LAUNCHED,
    TaskStatus.RUNNING,
)

# Workflow-run statuses indicating the server has already decided the run must stop.
SERVER_STOP_STATUSES: frozenset[str] = frozenset(
    {
        WorkflowRunStatus.ERROR,
        WorkflowRunStatus.TERMINATED,
        WorkflowRunStatus.STOPPED,
    }
)

# Workflow-run statuses indicating a resume signal was received.
TERMINATING_STATUSES: tuple[str, ...] = (
    WorkflowRunStatus.COLD_RESUME,
    WorkflowRunStatus.HOT_RESUME,
)


# ──────────────────────────────────────────────────────────────────────────────
# Result Dataclass
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class OrchestratorResult:
    """Result of orchestrator execution."""

    final_status: str
    done_count: int
    failed_count: int
    total_tasks: int
    elapsed_time: float


# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class OrchestratorConfig:
    """Configuration for the WorkflowRunOrchestrator."""

    # Heartbeat settings
    heartbeat_interval: float = 30.0
    heartbeat_report_by_buffer: float = 1.5

    # Sync settings
    wedged_workflow_sync_interval: float = 600.0

    # Flow control
    fail_fast: bool = False
    timeout: int = 36000

    # Test hooks (set to None to disable)
    fail_after_n_executions: Optional[int] = None


# ──────────────────────────────────────────────────────────────────────────────
# Context
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class WorkflowRunContext:
    """Context holding all state and references needed by the orchestrator.

    This context provides access to the workflow run state, allowing the
    orchestrator to work with either SwarmState or WorkflowRun's legacy state.
    """

    # Identity
    workflow_id: int
    workflow_run_id: int

    # Gateway for server communication
    gateway: "ServerGateway"

    # State references (mutable)
    tasks: dict[int, "SwarmTask"]
    arrays: dict[int, "SwarmArray"]
    task_status_map: dict[str, set["SwarmTask"]]
    ready_to_run: deque["SwarmTask"]

    # Task resources cache
    task_resources_cache: dict[int, "TaskResources"]

    # Mutable state values (these will be updated by orchestrator)
    status: str = WorkflowRunStatus.BOUND
    max_concurrently_running: int = 10000

    # Sync tracking
    last_sync: Optional[str] = None

    # Execution counter for test hooks
    n_executions: int = 0

    # Helper properties
    def get_active_task_count(self) -> int:
        """Count of tasks currently in-flight."""
        return sum(len(self.task_status_map[s]) for s in ACTIVE_TASK_STATUSES)

    def get_done_count(self) -> int:
        """Count of completed tasks."""
        return len(self.task_status_map[TaskStatus.DONE])

    def get_failed_count(self) -> int:
        """Count of failed tasks."""
        return len(self.task_status_map[TaskStatus.ERROR_FATAL])

    def all_tasks_final(self) -> bool:
        """Check if all tasks are in terminal state."""
        return len(self.tasks) == self.get_done_count() + self.get_failed_count()

    def has_pending_work(self) -> bool:
        """Check if there's in-flight or ready-to-run work."""
        return self.get_active_task_count() > 0 or len(self.ready_to_run) > 0


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────


class WorkflowRunOrchestrator:
    """Thin coordinator - manages the main event loop.

    The orchestrator coordinates all workflow run services:
    - HeartbeatService: Periodic heartbeat logging
    - Synchronizer: State sync with server
    - Scheduler: Task batching and queueing

    It manages the main execution loop including:
    - Initialization (set initial fringe, transition to RUNNING)
    - Main loop with timeout, distributor alive checks, status-based flow control
    - State propagation (downstream readiness, resource adjustments)
    - Termination handling (resume signals, task draining)
    - Error handling with proper cleanup

    Usage:
        ctx = WorkflowRunContext(...)
        config = OrchestratorConfig(...)
        orchestrator = WorkflowRunOrchestrator(ctx, config)
        result = await orchestrator.run(distributor_alive_callable)
    """

    def __init__(
        self,
        context: WorkflowRunContext,
        config: OrchestratorConfig,
    ):
        """Initialize the orchestrator.

        Args:
            context: WorkflowRunContext with all state references.
            config: OrchestratorConfig with settings.
        """
        self._ctx = context
        self._config = config

        # Services (lazily initialized)
        self._heartbeat: Optional[HeartbeatService] = None
        self._synchronizer: Optional[Synchronizer] = None
        self._scheduler: Optional[Scheduler] = None

        # Background task tracking
        self._stop_event: Optional[asyncio.Event] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None

    # ──────────────────────────────────────────────────────────────────────────
    # Service Initialization
    # ──────────────────────────────────────────────────────────────────────────

    def _ensure_heartbeat(self) -> HeartbeatService:
        """Lazily initialize the heartbeat service."""
        if self._heartbeat is None:
            self._heartbeat = HeartbeatService(
                gateway=self._ctx.gateway,
                interval=self._config.heartbeat_interval,
                report_by_buffer=self._config.heartbeat_report_by_buffer,
                initial_status=self._ctx.status,
            )
        return self._heartbeat

    def _ensure_synchronizer(self) -> Synchronizer:
        """Lazily initialize the synchronizer service."""
        if self._synchronizer is None:
            self._synchronizer = Synchronizer(
                gateway=self._ctx.gateway,
                task_ids=set(self._ctx.tasks.keys()),
                array_ids=set(self._ctx.arrays.keys()),
            )
        else:
            # Keep task/array IDs in sync
            self._synchronizer.update_task_ids(set(self._ctx.tasks.keys()))
            self._synchronizer.update_array_ids(set(self._ctx.arrays.keys()))
        return self._synchronizer

    def _ensure_scheduler(self) -> Scheduler:
        """Lazily initialize the scheduler service."""
        if self._scheduler is None:
            self._scheduler = Scheduler(
                gateway=self._ctx.gateway,
                tasks=self._ctx.tasks,
                arrays=self._ctx.arrays,
                task_status_map=self._ctx.task_status_map,
                ready_to_run=self._ctx.ready_to_run,
                max_concurrently_running=self._ctx.max_concurrently_running,
            )
        else:
            # Keep max_concurrently_running in sync
            self._scheduler.max_concurrently_running = self._ctx.max_concurrently_running
        return self._scheduler

    # ──────────────────────────────────────────────────────────────────────────
    # Main Entry Point
    # ──────────────────────────────────────────────────────────────────────────

    async def run(
        self,
        distributor_alive_callable: Callable[..., bool],
    ) -> OrchestratorResult:
        """Execute the main workflow run event loop.

        Args:
            distributor_alive_callable: Callable that returns True if distributor is alive.

        Returns:
            OrchestratorResult with execution summary.

        Raises:
            Various exceptions from constraint violations or workflow errors.
        """
        self._stop_event = asyncio.Event()
        heartbeat = self._ensure_heartbeat()
        self._heartbeat_task = asyncio.create_task(
            heartbeat.run_background(self._stop_event)
        )

        start_time = time.perf_counter()
        teardown_needed = True

        try:
            # Initialize
            logger.info(
                f"Starting workflow run orchestrator",
                workflow_run_id=self._ctx.workflow_run_id,
            )
            await self._initialize()

            # Main loop
            await self._main_loop(distributor_alive_callable)

            # Finalize
            return await self._finalize(start_time)

        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt raised")
            # Let the caller handle keyboard interrupt
            raise

        except Exception as e:
            logger.exception("Orchestrator error", error=str(e))
            await self._handle_error()
            raise

        finally:
            if teardown_needed:
                await self._teardown()

    # ──────────────────────────────────────────────────────────────────────────
    # Initialization
    # ──────────────────────────────────────────────────────────────────────────

    async def _initialize(self) -> None:
        """Set up initial state and transition to RUNNING."""
        logger.info(
            f"Executing Workflow Run {self._ctx.workflow_run_id}",
            workflow_run_id=self._ctx.workflow_run_id,
        )
        self._set_initial_fringe()
        await self._update_status(WorkflowRunStatus.RUNNING)

    def _set_initial_fringe(self) -> None:
        """Populate ready_to_run with tasks whose upstreams are satisfied."""
        # Tasks in ADJUSTING_RESOURCES status need resource adjustment
        for task in list(self._ctx.task_status_map[TaskStatus.ADJUSTING_RESOURCES]):
            self._set_adjusted_task_resources(task)
            self._ctx.ready_to_run.append(task)

        # Tasks in REGISTERING status with all upstreams done are ready
        for task in list(self._ctx.task_status_map[TaskStatus.REGISTERING]):
            if task.all_upstreams_done:
                self._set_validated_task_resources(task)
                self._ctx.ready_to_run.append(task)

        logger.debug(
            f"Initial fringe set. ready_to_run_count: {len(self._ctx.ready_to_run)}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Main Loop
    # ──────────────────────────────────────────────────────────────────────────

    async def _main_loop(
        self,
        distributor_alive_callable: Callable[..., bool],
    ) -> None:
        """The core scheduling loop."""
        start_time = time.perf_counter()
        time_since_last_full_sync = 0.0

        while self._should_continue():
            iteration_start = time.perf_counter()

            # Check constraints
            self._check_timeout(start_time)
            await self._check_distributor_alive(distributor_alive_callable)

            # Server already decided this run must stop
            if self._ctx.status in SERVER_STOP_STATUSES:
                logger.warning(
                    "Workflow Run status set to %s by server, stopping scheduler",
                    self._ctx.status,
                )
                break

            # Resume signal received — wait for in-flight tasks to drain
            if self._ctx.status in TERMINATING_STATUSES:
                if await self._handle_termination():
                    break
                continue

            # Fail-fast mode: bail after first fatal task error
            self._check_fail_fast()

            # Remaining time until we must re-sync with the server
            heartbeat = self._ensure_heartbeat()
            time_till_next_sync = max(
                0.0,
                self._config.heartbeat_interval - heartbeat.time_since_last_heartbeat(),
            )

            # Do scheduling work if running
            if self._ctx.status == WorkflowRunStatus.RUNNING:
                await self._do_scheduling(timeout=time_till_next_sync)

            # Sleep if we finished early
            loop_elapsed = time.perf_counter() - iteration_start
            if loop_elapsed < time_till_next_sync:
                await asyncio.sleep(time_till_next_sync - loop_elapsed)
                loop_elapsed = time.perf_counter() - iteration_start

            # Sync with server
            if time_since_last_full_sync > self._config.wedged_workflow_sync_interval:
                time_since_last_full_sync = 0.0
                await self._do_sync(full_sync=True)
            else:
                time_since_last_full_sync += loop_elapsed
                await self._do_sync(full_sync=False)

            # Test hook
            self._check_fail_after_n_executions()

            # Check if we should continue
            if not self._should_continue():
                # No observable work still queued, but tasks remain outstanding
                # Force an immediate full sync before deciding to exit
                if not self._ctx.all_tasks_final():
                    await self._do_sync(full_sync=True)
                    time_since_last_full_sync = 0.0

    def _should_continue(self) -> bool:
        """Determine if the main loop should continue."""
        if self._ctx.status in SERVER_STOP_STATUSES:
            return False
        if self._ctx.all_tasks_final():
            return False
        return self._ctx.has_pending_work()

    # ──────────────────────────────────────────────────────────────────────────
    # Constraint Checks
    # ──────────────────────────────────────────────────────────────────────────

    def _check_timeout(self, start_time: float) -> None:
        """Check if workflow has exceeded timeout."""
        elapsed = time.perf_counter() - start_time
        if elapsed >= self._config.timeout:
            raise RuntimeError(
                f"Not all tasks completed within the given workflow timeout length "
                f"({self._config.timeout} seconds). Submitted tasks will still run, "
                f"but the workflow will need to be restarted."
            )

    async def _check_distributor_alive(
        self,
        callable: Callable[..., bool],
    ) -> None:
        """Verify the distributor process is still running."""
        is_alive = await asyncio.to_thread(callable)
        if not is_alive:
            raise DistributorNotAlive(
                "Distributor process unexpectedly stopped. Workflow will error."
            )

    def _check_fail_fast(self) -> None:
        """Check fail-fast condition."""
        if self._config.fail_fast and self._ctx.get_failed_count() > 0:
            logger.info("Failing after first failure, as requested")
            raise RuntimeError("Fail-fast: stopping after first task failure")

    def _check_fail_after_n_executions(self) -> None:
        """Test hook: fail after N task executions."""
        if self._config.fail_after_n_executions is not None:
            if self._ctx.n_executions >= self._config.fail_after_n_executions:
                raise WorkflowTestError(
                    f"WorkflowRun asked to fail after {self._ctx.n_executions} "
                    "executions. Failing now"
                )

    # ──────────────────────────────────────────────────────────────────────────
    # Scheduling
    # ──────────────────────────────────────────────────────────────────────────

    async def _do_scheduling(self, timeout: float) -> None:
        """Run one scheduling iteration."""
        scheduler = self._ensure_scheduler()
        update = await scheduler.tick(timeout=timeout)

        # Apply status updates and propagate completions
        if update.task_statuses:
            self._apply_status_updates(update.task_statuses)

    # ──────────────────────────────────────────────────────────────────────────
    # Synchronization
    # ──────────────────────────────────────────────────────────────────────────

    async def _do_sync(self, full_sync: bool) -> None:
        """Perform state synchronization with server."""
        synchronizer = self._ensure_synchronizer()
        update = await synchronizer.tick(
            full_sync=full_sync,
            last_sync=self._ctx.last_sync,
        )

        # Apply sync updates
        if update.sync_time is not None:
            self._ctx.last_sync = update.sync_time

        if update.max_concurrently_running is not None:
            self._ctx.max_concurrently_running = update.max_concurrently_running
            # Keep scheduler in sync
            if self._scheduler is not None:
                self._scheduler.max_concurrently_running = update.max_concurrently_running

        for array_id, limit in update.array_limits.items():
            if array_id in self._ctx.arrays:
                self._ctx.arrays[array_id].max_concurrently_running = limit

        # Apply task status updates and propagate
        if update.task_statuses:
            self._apply_status_updates(update.task_statuses)

        logger.debug(
            f"State synchronized. ready_to_run_count: {len(self._ctx.ready_to_run)}, "
            f"active_tasks: {self._ctx.get_active_task_count()}, "
            f"full_sync: {full_sync}"
        )

    def _apply_status_updates(self, task_statuses: dict[int, str]) -> None:
        """Apply task status updates and propagate downstream readiness.

        This handles:
        - Moving tasks between status buckets
        - Propagating completions to downstream tasks
        - Setting validated/adjusted resources for newly-ready tasks
        - Enqueuing newly-ready tasks
        """
        changed_tasks: set["SwarmTask"] = set()

        # Update task statuses
        for task_id, new_status in task_statuses.items():
            task = self._ctx.tasks.get(task_id)
            if task is not None and task.status != new_status:
                # Remove from old bucket
                self._ctx.task_status_map[task.status].discard(task)
                # Update and add to new bucket
                task.status = new_status
                self._ctx.task_status_map[new_status].add(task)
                changed_tasks.add(task)

        # Process changed tasks
        self._refresh_task_status_map(changed_tasks)

    def _refresh_task_status_map(self, updated_tasks: set["SwarmTask"]) -> None:
        """Re-bucket tasks whose statuses changed and propagate downstream readiness."""
        num_newly_completed = 0
        num_newly_failed = 0

        for task in updated_tasks:
            if task.status == TaskStatus.DONE:
                num_newly_completed += 1
                self._ctx.n_executions += 1  # Test hook counter

                # Check if there are downstreams that can run
                for downstream in task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1
                    if downstream.all_upstreams_done:
                        self._set_validated_task_resources(downstream)
                        self._ctx.ready_to_run.append(downstream)

            elif task.status == TaskStatus.ERROR_FATAL:
                num_newly_failed += 1

            elif task.status == TaskStatus.REGISTERING and task.all_upstreams_done:
                self._set_validated_task_resources(task)
                self._ctx.ready_to_run.append(task)

            elif task.status == TaskStatus.ADJUSTING_RESOURCES:
                self._set_adjusted_task_resources(task)
                # Put at front of queue since we already tried it once
                self._ctx.ready_to_run.appendleft(task)

            else:
                logger.debug(
                    f"Got status update {task.status} for task_id: {task.task_id}. "
                    "No actions necessary."
                )

        # Log progress
        if num_newly_completed > 0:
            total_tasks = len(self._ctx.tasks)
            done_count = self._ctx.get_done_count()
            percent_done = round((done_count / total_tasks) * 100, 2) if total_tasks > 0 else 0
            logger.info(
                f"Workflow {percent_done}% complete ({done_count}/{total_tasks} tasks)",
                telemetry_newly_completed=num_newly_completed,
                telemetry_percent_done=percent_done,
            )

        if num_newly_failed > 0:
            logger.warning(f"{num_newly_failed} newly failed tasks.")

    # ──────────────────────────────────────────────────────────────────────────
    # Resource Management
    # ──────────────────────────────────────────────────────────────────────────

    def _set_validated_task_resources(self, task: "SwarmTask") -> None:
        """Validate and set task resources, using cache if available."""
        from jobmon.client.task_resources import TaskResources

        task_resources = task.current_task_resources

        # Update with dynamic compute resources if callable provided
        if task.compute_resources_callable is not None:
            dynamic_compute_resources = task.compute_resources_callable()
            if not isinstance(dynamic_compute_resources, dict):
                raise CallableReturnedInvalidObject(
                    f"compute_resources_callable={task.compute_resources_callable} for "
                    f"task_id={task.task_id} returned an invalid type. Must return dict. got "
                    f"{type(dynamic_compute_resources)}."
                )
            requested_resources = task.current_task_resources.requested_resources.copy()
            requested_resources.update(dynamic_compute_resources)
            task.compute_resources_callable = None
            task_resources = TaskResources(
                requested_resources, task.current_task_resources.queue
            )

        # Coerce resources to valid values
        validated_task_resources = task_resources.coerce_resources()

        # Check cache
        validated_resource_hash = hash(validated_task_resources)
        cached = self._ctx.task_resources_cache.get(validated_resource_hash)
        if cached is not None:
            validated_task_resources = cached
        else:
            self._ctx.task_resources_cache[validated_resource_hash] = validated_task_resources

        task.current_task_resources = validated_task_resources

    def _set_adjusted_task_resources(self, task: "SwarmTask") -> None:
        """Adjust the swarm task's resources after a failure."""
        task_resources = task.current_task_resources.adjust_resources(
            resource_scales=task.resource_scales,
            fallback_queues=task.fallback_queues,
        )

        # Check cache
        resource_hash = hash(task_resources)
        cached = self._ctx.task_resources_cache.get(resource_hash)
        if cached is not None:
            task_resources = cached
        else:
            self._ctx.task_resources_cache[resource_hash] = task_resources

        task.current_task_resources = task_resources

    # ──────────────────────────────────────────────────────────────────────────
    # Termination Handling
    # ──────────────────────────────────────────────────────────────────────────

    async def _handle_termination(self) -> bool:
        """Handle resume signal. Returns True if we should exit the loop."""
        wait_states = (
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
        )
        if any(self._ctx.task_status_map[s] for s in wait_states):
            logger.warning(
                f"Workflow Run set to {self._ctx.status}. Waiting for tasks to stop"
            )
            await self._ctx.gateway.terminate_task_instances()
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────────
    # Status Management
    # ──────────────────────────────────────────────────────────────────────────

    async def _update_status(self, status: str) -> None:
        """Update workflow run status on server and locally."""
        response = await self._ctx.gateway.update_status(status)
        new_status = response.status

        if new_status != status:
            raise TransitionError(
                f"Cannot transition WFR {self._ctx.workflow_run_id} from current status "
                f"{self._ctx.status} to {status}."
            )

        self._ctx.status = new_status

        # Keep heartbeat service in sync
        if self._heartbeat is not None:
            self._heartbeat.set_status(new_status)

    # ──────────────────────────────────────────────────────────────────────────
    # Finalization
    # ──────────────────────────────────────────────────────────────────────────

    async def _finalize(self, start_time: float) -> OrchestratorResult:
        """Determine final status and update server."""
        elapsed = time.perf_counter() - start_time
        done_count = self._ctx.get_done_count()
        failed_count = self._ctx.get_failed_count()
        total_tasks = len(self._ctx.tasks)

        # Determine final status
        if total_tasks == done_count:
            logger.info("All tasks are done")
            await self._update_status(WorkflowRunStatus.DONE)
        elif self._ctx.status in TERMINATING_STATUSES:
            await self._update_status(WorkflowRunStatus.TERMINATED)
        elif self._ctx.status in SERVER_STOP_STATUSES:
            # Server already set a terminal status - don't try to transition
            logger.info(
                f"Workflow run exited with server-set status: {self._ctx.status}"
            )
        else:
            await self._update_status(WorkflowRunStatus.ERROR)

        return OrchestratorResult(
            final_status=self._ctx.status,
            done_count=done_count,
            failed_count=failed_count,
            total_tasks=total_tasks,
            elapsed_time=elapsed,
        )

    async def _handle_error(self) -> None:
        """Transition to ERROR state on exception."""
        try:
            await self._update_status(WorkflowRunStatus.ERROR)
        except TransitionError as e:
            logger.warning(
                "Failed to update workflow run status to ERROR",
                error=str(e),
            )

    async def _teardown(self) -> None:
        """Clean up resources."""
        if self._stop_event is not None and not self._stop_event.is_set():
            self._stop_event.set()

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass  # Expected
            except Exception as e:
                logger.warning(
                    "Heartbeat task failed during teardown",
                    error=str(e),
                    exc_info=e,
                )
            self._heartbeat_task = None

        self._stop_event = None

