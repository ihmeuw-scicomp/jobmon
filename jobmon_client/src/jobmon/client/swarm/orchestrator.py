"""WorkflowRunOrchestrator: Main event loop coordinator for workflow runs.

This module provides the orchestrator that coordinates all workflow run services
(heartbeat, synchronization, scheduling) and manages the main execution loop.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

import structlog

from jobmon.client.swarm.services.heartbeat import HeartbeatService
from jobmon.client.swarm.services.scheduler import Scheduler
from jobmon.client.swarm.services.synchronizer import Synchronizer
from jobmon.client.swarm.state import (
    SERVER_STOP_STATUSES,
    TERMINATING_STATUSES,
    SwarmState,
)
from jobmon.core.constants import TaskStatus, WorkflowRunStatus
from jobmon.core.exceptions import (
    CallableReturnedInvalidObject,
    DistributorNotAlive,
    TransitionError,
    WorkflowTestError,
)

if TYPE_CHECKING:
    from jobmon.client.swarm.gateway import ServerGateway
    from jobmon.client.swarm.task import SwarmTask

logger = structlog.get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Result Dataclass
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class OrchestratorResult:
    """Complete result of orchestrator execution.

    Contains all information needed by callers after execution,
    eliminating the need to access mutable state post-run.

    Attributes:
        final_status: Final workflow run status (e.g., "D" for DONE).
        done_count: Number of tasks that completed successfully.
        failed_count: Number of tasks that failed fatally.
        total_tasks: Total number of tasks in the workflow.
        elapsed_time: Total execution time in seconds.
        num_previously_complete: Number of tasks already complete before this run
            (useful for resume tracking).
        task_final_statuses: Mapping of task_id -> final status string.
            Enables callers to get task results without accessing mutable state.
        done_task_ids: Immutable set of task IDs that completed successfully.
        failed_task_ids: Immutable set of task IDs that failed fatally.
    """

    # Workflow run outcome
    final_status: str
    elapsed_time: float

    # Task counts
    total_tasks: int
    done_count: int
    failed_count: int
    num_previously_complete: int

    # Task-level results
    task_final_statuses: dict[int, str]
    done_task_ids: frozenset[int]
    failed_task_ids: frozenset[int]


# ──────────────────────────────────────────────────────────────────────────────
# WorkflowRun Configuration
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class WorkflowRunConfig:
    """Configuration for WorkflowRun execution.

    Consolidates all configuration parameters into a single object
    for cleaner construction and easier defaults management.

    This is the user-facing configuration class. It maps to OrchestratorConfig
    internally but provides a simpler interface.

    Attributes:
        heartbeat_interval: Interval in seconds between heartbeat logs.
            If None, uses default from JobmonConfig.
        heartbeat_report_by_buffer: Multiplier for heartbeat report_by time.
            If None, uses default from JobmonConfig.
        fail_fast: If True, stop workflow on first task failure.
        wedged_workflow_sync_interval: Seconds between full state syncs
            to detect "wedged" workflows.
        fail_after_n_executions: Test hook - fail after N task executions.
            Default is effectively disabled (1 billion).

    Example:
        # Simple usage with defaults
        config = WorkflowRunConfig()

        # Custom configuration
        config = WorkflowRunConfig(
            fail_fast=True,
            heartbeat_interval=60,
        )

        # Use with run_workflow()
        result = run_workflow(workflow, workflow_run_id, distributor.alive, config=config)
    """

    # Heartbeat settings (None = use JobmonConfig defaults)
    heartbeat_interval: Optional[int] = None
    heartbeat_report_by_buffer: Optional[float] = None

    # Flow control
    fail_fast: bool = False
    wedged_workflow_sync_interval: int = 600

    # Test hooks (internal use only)
    fail_after_n_executions: int = 1_000_000_000

    @classmethod
    def from_defaults(cls: type["WorkflowRunConfig"]) -> "WorkflowRunConfig":
        """Create config with all defaults from JobmonConfig.

        Returns:
            WorkflowRunConfig with default values.
        """
        return cls()


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator Configuration (Internal)
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
        orchestrator = WorkflowRunOrchestrator(state, gateway, config)
        result = await orchestrator.run(distributor_alive_callable)
    """

    def __init__(
        self,
        state: SwarmState,
        gateway: "ServerGateway",
        config: OrchestratorConfig,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            state: SwarmState containing all workflow run state.
            gateway: ServerGateway for server communication.
            config: OrchestratorConfig with settings.
        """
        self._state = state
        self._gateway = gateway
        self._config = config

        # Test hook counter (tracks completed task executions)
        self._n_executions = 0

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
                gateway=self._gateway,
                interval=self._config.heartbeat_interval,
                report_by_buffer=self._config.heartbeat_report_by_buffer,
                initial_status=self._state.status,
            )
        return self._heartbeat

    def _ensure_synchronizer(self) -> Synchronizer:
        """Lazily initialize the synchronizer service."""
        if self._synchronizer is None:
            self._synchronizer = Synchronizer(
                gateway=self._gateway,
                task_ids=set(self._state.tasks.keys()),
                array_ids=set(self._state.arrays.keys()),
            )
        else:
            # Keep task/array IDs in sync
            self._synchronizer.update_task_ids(set(self._state.tasks.keys()))
            self._synchronizer.update_array_ids(set(self._state.arrays.keys()))
        return self._synchronizer

    def _ensure_scheduler(self) -> Scheduler:
        """Lazily initialize the scheduler service."""
        if self._scheduler is None:
            self._scheduler = Scheduler(
                gateway=self._gateway,
                state=self._state,
            )
        # Scheduler reads from state directly, no need to sync values
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
                "Starting workflow run orchestrator",
                workflow_run_id=self._state.workflow_run_id,
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
            f"Executing Workflow Run {self._state.workflow_run_id}",
            workflow_run_id=self._state.workflow_run_id,
        )
        self._set_initial_fringe()
        # Only transition to RUNNING if not already running
        if self._state.status != WorkflowRunStatus.RUNNING:
            await self._update_status(WorkflowRunStatus.RUNNING)

    def _set_initial_fringe(self) -> None:
        """Populate ready_to_run with tasks whose upstreams are satisfied."""
        state = self._state

        # Tasks in ADJUSTING_RESOURCES status need resource adjustment
        for task in list(state.get_tasks_by_status(TaskStatus.ADJUSTING_RESOURCES)):
            self._set_adjusted_task_resources(task)
            state.enqueue_task(task)

        # Tasks in REGISTERING status with all upstreams done are ready
        for task in list(state.get_tasks_by_status(TaskStatus.REGISTERING)):
            if task.all_upstreams_done:
                self._set_validated_task_resources(task)
                state.enqueue_task(task)

        logger.debug(
            f"Initial fringe set. ready_to_run_count: {state.get_ready_to_run_count()}"
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

            # Check if heartbeat service detected a status change
            self._sync_heartbeat_status()

            # Server already decided this run must stop
            if self._state.status in SERVER_STOP_STATUSES:
                logger.warning(
                    "Workflow Run status set to %s by server, stopping scheduler",
                    self._state.status,
                )
                break

            # Resume signal received — wait for in-flight tasks to drain
            if self._state.status in TERMINATING_STATUSES:
                if await self._handle_termination():
                    break
                # Fall through to normal sleep + sync logic below

            # Fail-fast mode: bail after first fatal task error
            self._check_fail_fast()

            # Remaining time until we must re-sync with the server
            heartbeat = self._ensure_heartbeat()
            time_till_next_sync = max(
                0.0,
                self._config.heartbeat_interval - heartbeat.time_since_last_heartbeat(),
            )

            # Do scheduling work if running
            if self._state.status == WorkflowRunStatus.RUNNING:
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
                if not self._state.all_tasks_final():
                    await self._do_sync(full_sync=True)
                    time_since_last_full_sync = 0.0

    def _should_continue(self) -> bool:
        """Determine if the main loop should continue."""
        if self._state.status in SERVER_STOP_STATUSES:
            return False
        if self._state.all_tasks_final():
            return False
        return self._state.has_pending_work()

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
        if self._config.fail_fast and self._state.get_failed_count() > 0:
            logger.info("Failing after first failure, as requested")
            raise RuntimeError("Fail-fast: stopping after first task failure")

    def _check_fail_after_n_executions(self) -> None:
        """Test hook: fail after N task executions."""
        if self._config.fail_after_n_executions is not None:
            if self._n_executions >= self._config.fail_after_n_executions:
                raise WorkflowTestError(
                    f"WorkflowRun asked to fail after {self._n_executions} "
                    "executions. Failing now"
                )

    def _sync_heartbeat_status(self) -> None:
        """Sync context status with heartbeat service if it detected a change.

        The heartbeat service runs in the background and may detect status
        changes from the server (e.g., COLD_RESUME signal). This method
        propagates those changes to the orchestrator context.
        """
        if self._heartbeat is not None:
            heartbeat_status = self._heartbeat.current_status
            if heartbeat_status != self._state.status:
                logger.info(
                    "Heartbeat detected status change",
                    old_status=self._state.status,
                    new_status=heartbeat_status,
                )
                self._state.status = heartbeat_status

    # ──────────────────────────────────────────────────────────────────────────
    # Scheduling
    # ──────────────────────────────────────────────────────────────────────────

    async def _do_scheduling(self, timeout: float) -> None:
        """Run one scheduling iteration."""
        scheduler = self._ensure_scheduler()
        update = await scheduler.tick(timeout=timeout)

        # Apply status updates atomically via SwarmState
        if update.task_statuses:
            changed_tasks = self._state.apply_update(update)
            if changed_tasks:
                self._process_changed_tasks(changed_tasks)

    # ──────────────────────────────────────────────────────────────────────────
    # Synchronization
    # ──────────────────────────────────────────────────────────────────────────

    async def _do_sync(self, full_sync: bool) -> None:
        """Perform state synchronization with server."""
        synchronizer = self._ensure_synchronizer()
        update = await synchronizer.tick(
            full_sync=full_sync,
            last_sync=self._state.last_sync,
        )

        # Apply all updates atomically via SwarmState
        changed_tasks = self._state.apply_update(update)

        # Process changed tasks (propagate completions, set resources)
        if changed_tasks:
            self._process_changed_tasks(changed_tasks)

        logger.debug(
            f"State synchronized. ready_to_run_count: {self._state.get_ready_to_run_count()}, "
            f"active_tasks: {self._state.get_active_task_count()}, "
            f"full_sync: {full_sync}"
        )

    def _process_changed_tasks(self, changed_tasks: set["SwarmTask"]) -> None:
        """Process tasks whose status changed.

        This handles post-update operations that can't be done in SwarmState:
        - Propagating completions to downstream tasks and enqueuing newly-ready
        - Setting validated/adjusted resources for newly-ready tasks
        - Updating test hook counters
        - Logging progress

        Note: Status bucket updates are handled by SwarmState.apply_update()

        Note: We track tasks enqueued via propagation to avoid double-enqueuing.
        If an upstream task A (DONE) and downstream task B (REGISTERING) are both
        in changed_tasks, B could be enqueued twice: once via propagate_completions
        when A is processed, and again when B is processed directly (since it has
        REGISTERING status and all_upstreams_done). We prevent this by tracking
        tasks enqueued via propagation.
        """
        num_newly_completed = 0
        num_newly_failed = 0

        # Track tasks enqueued via propagation to prevent double-enqueuing
        enqueued_via_propagation: set["SwarmTask"] = set()

        for task in changed_tasks:
            if task.status == TaskStatus.DONE:
                num_newly_completed += 1
                self._n_executions += 1  # Test hook counter

                # Propagate completion to downstream tasks
                newly_ready = self._state.propagate_completions({task})
                for downstream in newly_ready:
                    self._set_validated_task_resources(downstream)
                    self._state.enqueue_task(downstream)
                    enqueued_via_propagation.add(downstream)

            elif task.status == TaskStatus.ERROR_FATAL:
                num_newly_failed += 1

            elif task.status == TaskStatus.REGISTERING and task.all_upstreams_done:
                # Skip if already enqueued via propagation from an upstream DONE task
                if task not in enqueued_via_propagation:
                    self._set_validated_task_resources(task)
                    self._state.enqueue_task(task)

            elif task.status == TaskStatus.ADJUSTING_RESOURCES:
                self._set_adjusted_task_resources(task)
                # Put at front of queue since we already tried it once
                self._state.enqueue_task(task, front=True)

            else:
                logger.debug(
                    f"Got status update {task.status} for task_id: {task.task_id}. "
                    "No actions necessary."
                )

        # Log progress
        if num_newly_completed > 0:
            total_tasks = len(self._state.tasks)
            done_count = self._state.get_done_count()
            percent_done = self._state.get_percent_done()
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

        # Use SwarmState cache
        validated_resource_hash = hash(validated_task_resources)
        task.current_task_resources = self._state.cache_resources(
            validated_resource_hash, validated_task_resources
        )

    def _set_adjusted_task_resources(self, task: "SwarmTask") -> None:
        """Adjust the swarm task's resources after a failure."""
        task_resources = task.current_task_resources.adjust_resources(
            resource_scales=task.resource_scales,
            fallback_queues=task.fallback_queues,
        )

        # Use SwarmState cache
        resource_hash = hash(task_resources)
        task.current_task_resources = self._state.cache_resources(
            resource_hash, task_resources
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Termination Handling
    # ──────────────────────────────────────────────────────────────────────────

    async def _handle_termination(self) -> bool:
        """Handle resume signal. Returns True if we should exit the loop.

        When tasks are still in-flight, this requests termination and returns
        False to let the main loop handle the normal sleep + sync cycle.
        """
        wait_states = (
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
        )
        if any(self._state._task_status_map[s] for s in wait_states):
            logger.warning(
                f"Workflow Run set to {self._state.status}. Waiting for tasks to stop"
            )
            await self._gateway.terminate_task_instances()
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────────
    # Status Management
    # ──────────────────────────────────────────────────────────────────────────

    async def _update_status(self, status: str) -> None:
        """Update workflow run status on server and locally."""
        response = await self._gateway.update_status(status)
        new_status = response.status

        if new_status != status:
            raise TransitionError(
                f"Cannot transition WFR {self._state.workflow_run_id} from current status "
                f"{self._state.status} to {status}."
            )

        self._state.status = new_status

        # Keep heartbeat service in sync
        if self._heartbeat is not None:
            self._heartbeat.set_status(new_status)

    # ──────────────────────────────────────────────────────────────────────────
    # Finalization
    # ──────────────────────────────────────────────────────────────────────────

    async def _finalize(self, start_time: float) -> OrchestratorResult:
        """Determine final status and update server.

        Returns:
            OrchestratorResult with complete execution results including
            task-level statuses for post-run queries.
        """
        elapsed = time.perf_counter() - start_time
        done_count = self._state.get_done_count()
        failed_count = self._state.get_failed_count()
        total_tasks = len(self._state.tasks)

        # Determine final status
        if total_tasks == done_count:
            logger.info("All tasks are done")
            await self._update_status(WorkflowRunStatus.DONE)
        elif self._state.status in TERMINATING_STATUSES:
            await self._update_status(WorkflowRunStatus.TERMINATED)
        elif self._state.status in SERVER_STOP_STATUSES:
            # Server already set a terminal status - don't try to transition
            logger.info(
                f"Workflow run exited with server-set status: {self._state.status}"
            )
        else:
            await self._update_status(WorkflowRunStatus.ERROR)

        # Build task-level results
        task_final_statuses = {
            task_id: task.status for task_id, task in self._state.tasks.items()
        }
        done_task_ids = frozenset(task.task_id for task in self._state.get_done_tasks())
        failed_task_ids = frozenset(
            task.task_id for task in self._state.get_failed_tasks()
        )

        return OrchestratorResult(
            final_status=self._state.status,
            elapsed_time=elapsed,
            total_tasks=total_tasks,
            done_count=done_count,
            failed_count=failed_count,
            num_previously_complete=self._state.num_previously_complete,
            task_final_statuses=task_final_statuses,
            done_task_ids=done_task_ids,
            failed_task_ids=failed_task_ids,
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
