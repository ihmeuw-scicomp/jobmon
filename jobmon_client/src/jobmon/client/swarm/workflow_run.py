"""Workflow Run is a distributor instance of a declared workflow.

This module contains the core scheduling loop that drives task execution,
heartbeat management, and state synchronization with the Jobmon server.
"""

from __future__ import annotations

import ast

# ──────────────────────────────────────────────────────────────────────────────
# Standard library
# ──────────────────────────────────────────────────────────────────────────────
import asyncio
import numbers
import time
from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Generator, Optional, Union

# ──────────────────────────────────────────────────────────────────────────────
# Third-party
# ──────────────────────────────────────────────────────────────────────────────
import aiohttp
import structlog

# ──────────────────────────────────────────────────────────────────────────────
# Jobmon internals
# ──────────────────────────────────────────────────────────────────────────────
from jobmon.client.array import Array
from jobmon.client.swarm.swarm_array import SwarmArray
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.task_resources import TaskResources
from jobmon.core.cluster import Cluster
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import TaskStatus, WorkflowRunStatus
from jobmon.core.exceptions import (
    CallableReturnedInvalidObject,
    DistributorNotAlive,
    EmptyWorkflowError,
    TransitionError,
    WorkflowTestError,
)
from jobmon.core.logging import set_jobmon_context
from jobmon.core.requester import Requester
from jobmon.core.structlog_utils import bind_method_context

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

# ──────────────────────────────────────────────────────────────────────────────
# Module-level constants
# ──────────────────────────────────────────────────────────────────────────────
logger = structlog.get_logger(__name__)

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


class SwarmCommand:
    def __init__(
        self,
        func: Callable[..., Awaitable[None]],
        *args: list[SwarmTask],
        **kwargs: Any,
    ) -> None:
        """A command to be run by the distributor service.

        Args:
            func: a callable which does work and optionally modifies task instance state
            *args: positional args to be passed into func
            **kwargs: kwargs to be passed into func
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs

    async def __call__(self) -> None:
        await self._func(*self._args, **self._kwargs)


class WorkflowRun:
    """WorkflowRun enables tracking for multiple runs of a single Workflow.

    A Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(
        self,
        workflow_run_id: int,
        workflow_run_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
        fail_fast: bool = False,
        wedged_workflow_sync_interval: int = 600,
        fail_after_n_executions: int = 1_000_000_000,
        status: Optional[str] = None,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialization of the swarm WorkflowRun."""
        self.workflow_run_id = workflow_run_id

        set_jobmon_context(workflow_run_id=workflow_run_id)

        # State tracking — use module-level constant for active statuses
        self.tasks: dict[int, SwarmTask] = {}
        self.arrays: dict[int, SwarmArray] = {}
        self.ready_to_run: deque[SwarmTask] = deque()
        self._task_status_map: dict[str, set[SwarmTask]] = {
            TaskStatus.REGISTERING: set(),
            TaskStatus.QUEUED: set(),
            TaskStatus.INSTANTIATING: set(),
            TaskStatus.LAUNCHED: set(),
            TaskStatus.RUNNING: set(),
            TaskStatus.DONE: set(),
            TaskStatus.ADJUSTING_RESOURCES: set(),
            TaskStatus.ERROR_FATAL: set(),
        }

        # Cache validated TaskResources by hash to avoid duplicate binds
        self._task_resources: dict[int, TaskResources] = {}

        # workflow run attributes
        if status is None:
            status = WorkflowRunStatus.BOUND
        self._status = status
        self._last_heartbeat_time = time.time()

        # flow control
        self.fail_fast = fail_fast
        self.wedged_workflow_sync_interval = wedged_workflow_sync_interval

        # test parameters to force failure
        self._val_fail_after_n_executions = fail_after_n_executions
        self._n_executions = 0

        # optional config
        # config
        config = JobmonConfig()
        if workflow_run_heartbeat_interval is None:
            self._workflow_run_heartbeat_interval = config.get_int(
                "heartbeat", "workflow_run_interval"
            )
        else:
            self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        if heartbeat_report_by_buffer is None:
            self._heartbeat_report_by_buffer = config.get_float(
                "heartbeat", "report_by_buffer"
            )
        else:
            self._heartbeat_report_by_buffer = heartbeat_report_by_buffer

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

        # This signal is set if the workflow run receives a resume
        self._terminated = False

        self.initialized = False  # Need to call from_workflow or from_workflow_id
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None

    @property
    def status(self) -> Optional[str]:
        """Status of the workflow run."""
        return self._status

    @property
    def done_tasks(self) -> list[SwarmTask]:
        return list(self._task_status_map[TaskStatus.DONE])

    @property
    def failed_tasks(self) -> list[SwarmTask]:
        return list(self._task_status_map[TaskStatus.ERROR_FATAL])

    @property
    def active_tasks(self) -> bool:
        """Return True if the workflow run has in-flight or ready-to-run work.

        Short-circuits to False when the run is already terminal (ERROR/TERMINATED).
        """
        if self.status in SERVER_STOP_STATUSES:
            return False
        return any(self._task_status_map[s] for s in ACTIVE_TASK_STATUSES) or bool(
            self.ready_to_run
        )

    def _get_active_tasks_count(self) -> int:
        """Return the number of tasks currently in-flight."""
        return sum(len(self._task_status_map[s]) for s in ACTIVE_TASK_STATUSES)

    def _get_ready_to_run_count(self) -> int:
        """Return the number of tasks ready to run."""
        return len(self.ready_to_run)

    def _decide_run_loop_continue(
        self, time_since_last_full_sync: float
    ) -> tuple[bool, float]:
        """Decide whether the run loop should continue based on task states."""
        if self._has_active_or_ready_tasks():
            logger.debug("Swarm has active or ready to run")
            return True, time_since_last_full_sync

        if self._all_tasks_final():
            logger.debug("Swarm all tasks are done")
            return False, time_since_last_full_sync

        # Returning False tells the caller to perform a (possibly full) sync before
        # re-evaluating whether work remains.
        logger.debug("Swarm idle with outstanding tasks, requesting full sync")
        return False, time_since_last_full_sync

    def _has_active_or_ready_tasks(self) -> bool:
        return (self._get_active_tasks_count() + self._get_ready_to_run_count()) > 0

    def _all_tasks_final(self) -> bool:
        return len(self.tasks) == len(self._task_status_map[TaskStatus.DONE]) + len(
            self._task_status_map[TaskStatus.ERROR_FATAL]
        )

    def from_workflow(self, workflow: Workflow) -> None:
        if self.initialized:
            logger.warning("Swarm has already been initialized")
            return

        logger.debug("Initializing swarm from workflow", num_tasks=len(workflow.tasks))

        # construct arrays
        array: Array
        for array in workflow.arrays.values():
            swarm_array = SwarmArray(
                array.array_id, array.max_concurrently_running, array_name=array.name
            )
            self.arrays[array.array_id] = swarm_array

        # construct SwarmTasks from Client Tasks and populate registry
        for task in workflow.tasks.values():
            cluster = workflow.get_cluster_by_name(task.cluster_name)
            fallback_queues = []
            for queue in task.fallback_queues:
                cluster_queue = cluster.get_queue(queue)
                fallback_queues.append(cluster_queue)

            # create swarmtasks
            swarm_task = SwarmTask(
                task_id=task.task_id,
                array_id=task.array.array_id,
                status=task.initial_status,
                max_attempts=task.max_attempts,
                cluster=cluster,
                task_resources=task.original_task_resources,
                compute_resources_callable=task.compute_resources_callable,
                resource_scales=task.resource_scales,
                fallback_queues=fallback_queues,
            )
            self.tasks[task.task_id] = swarm_task

        # create relationships on swarm task
        for task in workflow.tasks.values():
            swarm_task = self.tasks[task.task_id]

            # assign upstream and downstreams
            swarm_task.num_upstreams = len(task.upstream_tasks)
            swarm_task.downstream_swarm_tasks = set(
                [self.tasks[t.task_id] for t in task.downstream_tasks]
            )

            # create array association
            self.arrays[swarm_task.array_id].add_task(swarm_task)

            # assign each task to the correct set
            self._task_status_map[swarm_task.status].add(swarm_task)

            # compute initial fringe
            if swarm_task.status == TaskStatus.DONE:
                # if task is done check if there are downstreams that can run
                for downstream in swarm_task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1

        self.last_sync = self._get_current_time()
        self.num_previously_complete = len(self._task_status_map[TaskStatus.DONE])
        self.workflow_id = workflow.workflow_id
        self.max_concurrently_running: int = workflow.max_concurrently_running
        self.dag_id = workflow.dag_id
        self.initialized = True

    def from_workflow_id(self, workflow_id: int, edge_chunk_size: int = 500) -> None:
        if self.initialized:
            logger.warning("Swarm has already been initialized")
            return
        # Log heartbeat prior to starting work
        self._log_heartbeat()
        self.set_workflow_metadata(workflow_id)
        self.set_tasks_from_db(chunk_size=edge_chunk_size)
        self.set_downstreams_from_db(chunk_size=edge_chunk_size)
        # Resumed workflow runs initialized in LINKING state, so update to bound here
        # once tasks are all created and the DAG is in memory
        self._update_status(WorkflowRunStatus.BOUND)
        self.initialized = True

    def set_workflow_metadata(self, workflow_id: int) -> None:
        """Fetch the dag_id and max_concurrently_running parameters of this workflow."""
        _, resp = self.requester.send_request(
            app_route=f"/workflow/{workflow_id}/fetch_workflow_metadata",
            message={},
            request_type="get",
        )

        database_wf = resp["workflow"]
        if not database_wf:
            raise EmptyWorkflowError(f"No workflow found for workflow id {workflow_id}")

        workflow_id, dag_id, max_concurrently_running = database_wf

        self.last_sync = self._get_current_time()
        self.num_previously_complete = len(self._task_status_map[TaskStatus.DONE])
        self.workflow_id = workflow_id
        self.max_concurrently_running = max_concurrently_running
        self.dag_id = dag_id
        logger.info(f"Initialized Swarm(workflow_id={workflow_id}, dag_id={dag_id})")

    def set_tasks_from_db(self, chunk_size: int = 500) -> None:
        """Pull the tasks that need to be run associated with this workflow.

        I.e. all tasks that aren't in DONE state.
        """
        # Fetch metadata for all tasks
        # Keep a cluster registry.
        cluster_registry: dict[str, Cluster] = {}
        all_tasks_returned = False
        max_task_id = 0
        logger.info("Fetching tasks from the database")
        while not all_tasks_returned:
            # TODO: make this an asynchronous context manager, avoid duplicating code
            if (
                time.time() - self._last_heartbeat_time
            ) > self._workflow_run_heartbeat_interval:
                self._log_heartbeat()
                logger.info(
                    f"Still fetching tasks, {len(self.tasks)} collected so far..."
                )

            _, resp = self.requester.send_request(
                app_route=f"/workflow/get_tasks/{self.workflow_id}",
                message={"max_task_id": max_task_id, "chunk_size": chunk_size},
                request_type="get",
            )
            task_dict = resp["tasks"]
            # Case when no tasks are returned - happen to have 1000 tasks, for example, and
            # 2 chunks of 500. No more work to be done
            if not task_dict:
                return
            elif len(task_dict) < chunk_size:
                # Last chunk has been returned, exit after task creation
                all_tasks_returned = True

            max_task_id = max(task_dict)

            # populate tasks dict, and arrays registry
            for task_id, metadata in task_dict.items():
                (
                    array_id,
                    status,
                    max_attempts,
                    resource_scales,
                    fallback_queues,
                    requested_resources,
                    cluster_name,
                    queue_name,
                    array_concurrency,
                ) = metadata

                # Convert datatypes as appropriate
                task_id = int(task_id)
                resource_scales = ast.literal_eval(resource_scales)
                for resource, scaler in resource_scales.items():
                    if not isinstance(scaler, numbers.Number):
                        if isinstance(scaler, list):
                            resource_scales[resource] = iter(scaler)
                        else:
                            raise ValueError(
                                "Cannot run CLI resume on non-numeric custom resource "
                                f"scales: found {resource} scaler {scaler} in resource "
                                "scales retrieved from the Jobmon DB."
                            )
                fallback_queues = ast.literal_eval(fallback_queues)
                requested_resources = ast.literal_eval(requested_resources)
                # Construct a queue, a cluster, and a task resources object
                try:
                    cluster = cluster_registry[cluster_name]
                except KeyError:
                    cluster = Cluster(
                        cluster_name=cluster_name, requester=self.requester
                    )
                    cluster.bind()
                    cluster_registry[cluster_name] = cluster

                queue = cluster.get_queue(queue_name)
                fallback_queues = [cluster.get_queue(q) for q in fallback_queues]

                task_resources = TaskResources(
                    requested_resources=requested_resources,
                    queue=queue,
                    requester=self.requester,
                )

                st = SwarmTask(
                    task_id=task_id,
                    array_id=array_id,
                    status=status,
                    max_attempts=max_attempts,
                    task_resources=task_resources,
                    cluster=cluster,
                    resource_scales=resource_scales,
                    fallback_queues=fallback_queues,
                )
                self.tasks[task_id] = st

                # Also create arrays
                if array_id not in self.arrays:
                    array = SwarmArray(
                        array_id=array_id, max_concurrently_running=array_concurrency
                    )
                    self.arrays[array_id] = array
                self.arrays[array_id].add_task(st)

                # Add to correct status queue
                self._task_status_map[st.status].add(st)
        logger.info("All tasks fetched")

    def set_downstreams_from_db(self, chunk_size: int = 500) -> None:
        """Pull downstream edges from the database associated with the workflow."""
        # Get edges in a different route to prevent overload

        # Create two maps: one to map node_id -> task_id
        task_node_id_map: dict[int, int] = {}
        # one to map task_id -> set of downstream node_ids
        task_edge_map: dict[int, set] = {}

        start_idx, end_idx = 0, chunk_size
        task_ids = list(self.tasks.keys())
        logger.info("Setting dependencies on tasks")
        while start_idx < len(task_ids):
            # Log heartbeats if needed
            if (
                time.time() - self._last_heartbeat_time
            ) > self._workflow_run_heartbeat_interval:
                self._log_heartbeat()
                logger.info("Still fetching edges from the database...")
            task_id_chunk = task_ids[start_idx:end_idx]
            start_idx = end_idx
            end_idx += chunk_size

            _, edge_resp = self.requester.send_request(
                app_route="/task/get_downstream_tasks",
                message={"task_ids": task_id_chunk, "dag_id": self.dag_id},
                request_type="post",
            )
            downstream_tasks = edge_resp["downstream_tasks"]
            # Format is {task_id: (node_id, downstream_node_ids)} where
            # downstream_node_ids is a list
            for task_id, values in downstream_tasks.items():
                node_id, downstream_node_ids = values
                # Convert to Python datatypes
                task_id = int(task_id)

                # Assumption: every single node in the downstream edge is not in "D" state
                # Shouldn't be possible to have a downstream node of a task not in "D" state
                # that is complete. If it is, we'll raise unexpected KeyErrors here
                task_node_id_map[node_id] = task_id
                task_edge_map[task_id] = downstream_node_ids

        # Create the dependency graph
        # NOTE: only tasks that are not in status "DONE" are returned. This means the created
        # dependency graph is not the full DAG, only a subset of the tasks that still need to
        # complete.
        logger.info("All edges fetched from the database, starting to build the graph")

        for task_id, swarm_task in self.tasks.items():
            downstream_edges = task_edge_map[task_id]
            if downstream_edges:
                for downstream_node_id in downstream_edges:
                    # Select the appropriate task id
                    downstream_task_id = task_node_id_map[downstream_node_id]
                    # Select the swarm task and add it as a dependency
                    downstream_swarm_task = self.tasks[downstream_task_id]
                    swarm_task.downstream_swarm_tasks.add(downstream_swarm_task)
                    downstream_swarm_task.num_upstreams += 1

        logger.info("Task DAG fully constructed, swarm is ready to run")

    def run(
        self,
        distributor_alive_callable: Callable[..., bool],
        seconds_until_timeout: int = 36000,
        initialize: bool = True,
    ) -> None:
        """Execute the workflow-run event loop."""
        self._run_coroutine(
            self._run_async(
                distributor_alive_callable=distributor_alive_callable,
                seconds_until_timeout=seconds_until_timeout,
                initialize=initialize,
            ),
            cleanup=False,
        )

    async def _run_async(
        self,
        distributor_alive_callable: Callable[..., bool],
        seconds_until_timeout: int,
        initialize: bool,
    ) -> None:
        """Async workflow runner that manages scheduler and heartbeat loops."""
        self._stop_event = asyncio.Event()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        teardown_needed = True

        try:
            if initialize:
                logger.info(f"Executing Workflow Run {self.workflow_run_id}")
                self.set_initial_fringe()
                await self._update_status_async(WorkflowRunStatus.RUNNING)

            time_since_last_full_sync = 0.0
            swarm_start_time = time.perf_counter()
            loop_continue = True

            while loop_continue:
                iteration_start = time.perf_counter()
                total_elapsed_time = time.perf_counter() - swarm_start_time
                if total_elapsed_time >= seconds_until_timeout:
                    raise RuntimeError(
                        f"Not all tasks completed within the given workflow timeout length "
                        f"({seconds_until_timeout} seconds). Submitted tasks will still run, "
                        f"but the workflow will need to be restarted."
                    )

                if not distributor_alive_callable():
                    raise DistributorNotAlive(
                        "Distributor process unexpectedly stopped. Workflow will error."
                    )

                # Server already decided this run must stop — exit immediately.
                if self.status in SERVER_STOP_STATUSES:
                    logger.warning(
                        "Workflow Run status set to %s by server, stopping scheduler",
                        self.status,
                    )
                    break

                # Resume signal received — wait for in-flight tasks to drain.
                if self.status in TERMINATING_STATUSES:
                    wait_states = (
                        TaskStatus.INSTANTIATING,
                        TaskStatus.LAUNCHED,
                        TaskStatus.RUNNING,
                    )
                    if any(self._task_status_map[s] for s in wait_states):
                        logger.warning(
                            f"Workflow Run set to {self.status}. Waiting for tasks to stop"
                        )
                        await self._terminate_task_instances_async()
                    else:
                        break

                # Optional fail-fast mode: bail after first fatal task error.
                if self.fail_fast and self._task_status_map[TaskStatus.ERROR_FATAL]:
                    logger.info("Failing after first failure, as requested")
                    break

                # Remaining time until we must re-sync with the server (heartbeat,
                # state refresh, wedged DAG polling, etc.).
                time_till_next_sync = max(
                    0.0,
                    self._workflow_run_heartbeat_interval
                    - (time.time() - self._last_heartbeat_time),
                )

                if self.status == WorkflowRunStatus.RUNNING:
                    await self.process_commands_async(timeout=time_till_next_sync)

                loop_elapsed = time.perf_counter() - iteration_start
                if loop_elapsed < time_till_next_sync:
                    await asyncio.sleep(time_till_next_sync - loop_elapsed)
                    loop_elapsed = time.perf_counter() - iteration_start

                if time_since_last_full_sync > self.wedged_workflow_sync_interval:
                    time_since_last_full_sync = 0.0
                    await self.synchronize_state_async(full_sync=True)
                else:
                    time_since_last_full_sync += loop_elapsed
                    await self.synchronize_state_async()

                self._check_fail_after_n_executions()
                loop_continue, time_since_last_full_sync = (
                    self._decide_run_loop_continue(time_since_last_full_sync)
                )
                # No observable work still queued, but tasks remain outstanding.
                # Force an immediate full sync (historic behavior) before deciding to exit.
                if not loop_continue and not self._all_tasks_final():
                    await self.synchronize_state_async(full_sync=True)
                    time_since_last_full_sync = 0.0
                    loop_continue = self._has_active_or_ready_tasks()

        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt raised")
            confirm = await asyncio.to_thread(
                input, "Are you sure you want to exit (y/n): "
            )
            confirm = confirm.lower().strip()

            if confirm == "y":
                await self._update_status_async(WorkflowRunStatus.STOPPED)
                raise
            else:
                logger.info("Continuing jobmon...")
                remaining = max(
                    0,
                    int(
                        seconds_until_timeout - (time.perf_counter() - swarm_start_time)
                    ),
                )
                await self._teardown_async()
                teardown_needed = False
                await self._run_async(
                    distributor_alive_callable=distributor_alive_callable,
                    seconds_until_timeout=remaining,
                    initialize=False,
                )
                return
        except Exception as e:
            logger.exception("Workflow run error", error=str(e))
            try:
                await self._update_status_async(WorkflowRunStatus.ERROR)
            except TransitionError as trans:
                logger.warning(
                    "Failed to update workflow run status to ERROR", error=str(trans)
                )
            raise
        else:
            if len(self.tasks) == len(self._task_status_map[TaskStatus.DONE]):
                logger.info("All tasks are done")
                await self._update_status_async(WorkflowRunStatus.DONE)
            elif self.status in TERMINATING_STATUSES:
                await self._update_status_async(WorkflowRunStatus.TERMINATED)
            elif self.status in SERVER_STOP_STATUSES:
                # Server already set a terminal status (ERROR/TERMINATED/STOPPED).
                # Don't try to transition again - just log and accept it.
                logger.info(
                    f"Workflow run exited with server-set status: {self.status}"
                )
            else:
                await self._update_status_async(WorkflowRunStatus.ERROR)
        finally:
            if teardown_needed:
                await self._teardown_async()

    async def _heartbeat_loop(self) -> None:
        """Background task that ensures workflow-run heartbeats are logged."""
        heartbeat_tick = max(1.0, self._workflow_run_heartbeat_interval / 2)
        try:
            while self._stop_event and not self._stop_event.is_set():
                await asyncio.sleep(heartbeat_tick)
                if (
                    time.time() - self._last_heartbeat_time
                    >= self._workflow_run_heartbeat_interval
                ):
                    await self._log_heartbeat_async()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Heartbeat loop error")
            raise

    def set_initial_fringe(self) -> None:
        """Populate ready_to_run with tasks whose upstreams are satisfied."""
        for task in self._task_status_map[TaskStatus.ADJUSTING_RESOURCES]:
            self._set_adjusted_task_resources(task)
            self.ready_to_run.append(task)

        for task in self._task_status_map[TaskStatus.REGISTERING]:
            if task.all_upstreams_done:
                self._set_validated_task_resources(task)
                self.ready_to_run.append(task)

        logger.debug(
            f"Initial fringe set. ready_to_run_count: {len(self.ready_to_run)}"
        )

    def get_swarm_commands(self) -> Generator[SwarmCommand, None, None]:
        """Yield batched queue commands respecting workflow/array concurrency limits."""
        # Gather all currently active tasks to compute remaining capacity.
        active_tasks: set[SwarmTask] = set()
        for status in ACTIVE_TASK_STATUSES:
            active_tasks |= self._task_status_map[status]

        workflow_capacity = self.max_concurrently_running - len(active_tasks)
        array_capacity_lookup: dict[int, int] = {
            aid: arr.max_concurrently_running - len(active_tasks & arr.tasks)
            for aid, arr in self.arrays.items()
        }

        try:
            unscheduled_tasks: list[SwarmTask] = []
            while self.ready_to_run and workflow_capacity > 0:
                # pop the next task off of the queue
                next_task = self.ready_to_run.popleft()
                array_id = next_task.array_id
                task_resources = next_task.current_task_resources

                # Check capacity; start a batch if room.
                current_batch: list[SwarmTask] = []
                array_capacity = array_capacity_lookup[array_id]
                if array_capacity > 0:
                    current_batch.append(next_task)
                    current_batch_size = 1
                    workflow_capacity -= 1
                    array_capacity -= 1

                    # we started a batch. let's try and add compatible tasks
                    for _ in range(len(self.ready_to_run)):
                        # Remove task from the front of the queue.
                        # If compatible, expire from the ready to run queue
                        task = self.ready_to_run.popleft()
                        # check for batch compatible tasks
                        if (
                            workflow_capacity > 0
                            and array_capacity > 0
                            and task.array_id == array_id
                            and task.current_task_resources == task_resources
                            and current_batch_size < 500
                        ):
                            current_batch.append(task)
                            current_batch_size += 1
                            workflow_capacity -= 1
                            array_capacity -= 1
                        else:
                            # Put it back in the queue
                            self.ready_to_run.append(task)

                    # set final array capacity
                    array_capacity_lookup[array_id] = array_capacity

                    array_name = self.arrays[array_id].array_name
                    logger.debug(
                        f"Created batch of {current_batch_size} tasks for {array_name}",
                        array_id=array_id,
                        batch_size=current_batch_size,
                    )
                    yield SwarmCommand(self.queue_task_batch_async, current_batch)

                # no room. keep track for next time method is called
                else:
                    unscheduled_tasks.append(next_task)

        # make sure to put unscheduled back on queue, even when the generator is closed
        finally:
            self.ready_to_run.extendleft(unscheduled_tasks)

    def _get_time_till_next_heartbeat(
        self, timeout: Union[int, float], loop_start: float
    ) -> tuple[float, Union[int, float]]:
        """A method to calculate the time till the next heartbeat.

        This method is used to test a bug in FHS where the timeout is not updated.

        Args:
            timeout: time until we stop processing. -1 means process till no more work
            loop_start: the time the loop started
        """
        if timeout < 0:
            logger.warning("Swarm Timeout is negative")
        elapsed_time = time.time() - loop_start
        return elapsed_time, timeout

    def process_commands(self, timeout: Union[int, float] = -1) -> None:
        self._run_coroutine(self.process_commands_async(timeout))

    async def process_commands_async(self, timeout: Union[int, float] = -1) -> None:
        """Processes swarm commands until all work is done or timeout is reached.

        Args:
            timeout: time until we stop processing. -1 means process till no more work
        """
        swarm_commands = self.get_swarm_commands()

        loop_start_pc = time.time()
        keep_processing = True
        while keep_processing:
            try:
                swarm_command = next(swarm_commands)
                await swarm_command()

                elapsed_time, timeout = self._get_time_till_next_heartbeat(
                    timeout, loop_start_pc
                )
                if not (elapsed_time < timeout or timeout == -1):
                    logger.debug(
                        (
                            "Stopping task processing after loop timeout: "
                            f"elapsed={elapsed_time} timeout={timeout}"
                        )
                    )
                    swarm_commands.close()

            except StopIteration:
                logger.debug(
                    "Stopping task processing because command generator returned StopIteration"
                )
                keep_processing = False

        logger.debug(
            f"Swarm commands processed. ready_to_run_count: {len(self.ready_to_run)}, "
            f"active_tasks: {self.active_tasks}, "
            f"processing_duration: {time.time() - loop_start_pc}"
        )

    def synchronize_state(self, full_sync: bool = False) -> None:
        self._run_coroutine(self.synchronize_state_async(full_sync=full_sync))

    async def synchronize_state_async(self, full_sync: bool = False) -> None:
        # These operations are independent - run them in parallel for better throughput.
        # Include heartbeat for cases where synchronize_state is called directly
        # (not through the full _run_async loop with its background heartbeat task).
        results = await asyncio.gather(
            self._set_status_for_triaging_async(),
            self._log_heartbeat_async(),
            self._task_status_updates_async(full_sync=full_sync),
            self._synchronize_max_concurrently_running_async(),
            self._synchronize_array_max_concurrently_running_async(),
            return_exceptions=True,
        )

        # Log any failures but don't raise - individual sync failures shouldn't
        # stop the workflow run
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                op_names = [
                    "triage",
                    "heartbeat",
                    "task_status_updates",
                    "max_concurrently_running",
                    "array_max_concurrently_running",
                ]
                logger.warning(
                    f"Sync operation '{op_names[i]}' failed",
                    error=str(result),
                    exc_info=result,
                )

        logger.debug(
            f"State synchronized. ready_to_run_count: {len(self.ready_to_run)}, "
            f"active_tasks: {self.active_tasks}, "
            f"full_sync: {full_sync}"
        )

    def _refresh_task_status_map(self, updated_tasks: set[SwarmTask]) -> None:
        """Re-bucket tasks whose statuses changed and propagate downstream readiness."""
        # Remove these tasks from their old buckets.
        for bucket in self._task_status_map.values():
            bucket -= updated_tasks

        num_newly_completed = 0
        num_newly_failed = 0
        for task in updated_tasks:
            # assign each task to the correct set
            self._task_status_map[task.status].add(task)

            if task.status == TaskStatus.DONE:
                num_newly_completed += 1
                self._n_executions += 1  # a test param

                # if task is done check if there are downstreams that can run
                for downstream in task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1
                    if downstream.all_upstreams_done:
                        self._set_validated_task_resources(downstream)
                        self.ready_to_run.append(downstream)

            elif task.status == TaskStatus.ERROR_FATAL:
                num_newly_failed += 1

            elif task.status == TaskStatus.REGISTERING and task.all_upstreams_done:
                self._set_validated_task_resources(task)
                self.ready_to_run.append(task)

            elif task.status == TaskStatus.ADJUSTING_RESOURCES:
                self._set_adjusted_task_resources(task)

                # put at front of queue since we already tried it once
                self.ready_to_run.appendleft(task)

            else:
                logger.debug(
                    f"Got status update {task.status} for task_id: {task.task_id}. "
                    "No actions necessary."
                )

        # if newly done report percent done and check if all done
        if num_newly_completed > 0:
            percent_done = round(
                (len(self._task_status_map[TaskStatus.DONE]) / len(self.tasks)) * 100, 2
            )
            logger.info(
                f"Workflow {percent_done}% complete "
                f"({len(self.done_tasks)}/{len(self.tasks)} tasks)",
                telemetry_newly_completed=num_newly_completed,
                telemetry_percent_done=percent_done,
            )

        # if newly failed, report failures and check if we should error out
        if num_newly_failed > 0:
            logger.warning(f"{num_newly_failed} newly failed tasks.")

    async def _set_status_for_triaging_async(self) -> None:
        """Request server to triage overdue task instances."""
        logger.debug("Swarm requesting triage check for overdue task instances")
        app_route = f"/workflow_run/{self.workflow_run_id}/set_status_for_triaging"
        await self._request_async(app_route=app_route, message={}, request_type="post")
        logger.debug("Triage check completed")

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _close_session(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _request_async(
        self,
        app_route: str,
        message: dict[str, Any],
        request_type: str,
        tenacious: bool = True,
    ) -> tuple[int, Any]:
        session = await self._ensure_session()
        return await self.requester.send_request_async(
            session=session,
            app_route=app_route,
            message=message,
            request_type=request_type,
            tenacious=tenacious,
        )

    def _run_coroutine(self, coro: Awaitable[Any], cleanup: bool = True) -> Any:
        async def runner() -> Any:
            try:
                return await coro
            finally:
                if cleanup:
                    await self._close_session()

        return asyncio.run(runner())

    async def _teardown_async(self) -> None:
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass  # Expected when we cancel the task
            except Exception as e:
                # Log but don't raise - we don't want heartbeat errors to mask
                # the original exception from the main workflow loop
                logger.warning(
                    "Heartbeat task failed during teardown", error=str(e), exc_info=e
                )
            self._heartbeat_task = None
        await self._close_session()
        self._stop_event = None

    def _log_heartbeat(self) -> None:
        next_report_increment = (
            self._workflow_run_heartbeat_interval * self._heartbeat_report_by_buffer
        )
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        _, response = self.requester.send_request(
            app_route=app_route,
            message={
                "status": self._status,
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        self._apply_heartbeat_response(response)

    async def _log_heartbeat_async(self) -> None:
        next_report_increment = (
            self._workflow_run_heartbeat_interval * self._heartbeat_report_by_buffer
        )
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        _, response = await self._request_async(
            app_route=app_route,
            message={
                "status": self._status,
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        self._apply_heartbeat_response(response)

    def _apply_heartbeat_response(self, response: dict[str, Any]) -> None:
        self._status = response["status"]
        self._last_heartbeat_time = time.time()

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        _, response = self.requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
        )
        self._validate_status_transition(status, response["status"])

    async def _update_status_async(self, status: str) -> None:
        """Async status update helper."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        _, response = await self._request_async(
            app_route=app_route,
            message={"status": status},
            request_type="put",
        )
        self._validate_status_transition(status, response["status"])

    def _validate_status_transition(self, desired_status: str, new_status: str) -> None:
        self._status = new_status
        if self.status != desired_status:
            raise TransitionError(
                f"Cannot transition WFR {self.workflow_run_id} from current status "
                f"{self._status} to {desired_status}."
            )

    async def _terminate_task_instances_async(self) -> None:
        """Signal the server to terminate all task instances for this workflow run."""
        app_route = f"/workflow_run/{self.workflow_run_id}/terminate_task_instances"
        await self._request_async(app_route=app_route, message={}, request_type="put")
        self._terminated = True

    def _check_fail_after_n_executions(self) -> None:
        """Raise the test hook exception when the execution threshold is met."""
        if self._n_executions >= self._val_fail_after_n_executions:
            raise WorkflowTestError(
                f"WorkflowRun asked to fail after {self._n_executions} "
                "executions. Failing now"
            )

    def _set_fail_after_n_executions(self, n: int) -> None:
        """For use during testing.

        Force the TaskDag to 'fall over' after n executions, so that the resume case can be
        tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self._val_fail_after_n_executions = n

    def _get_current_time(self) -> datetime:
        app_route = "/time"
        _, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get"
        )
        return response["time"]

    async def _task_status_updates_async(self, full_sync: bool = False) -> None:
        """Fetch task status changes from the server and update local state."""
        message = {} if full_sync else {"last_sync": str(self.last_sync)}
        app_route = f"/workflow/{self.workflow_id}/task_status_updates"
        _, response = await self._request_async(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        self._apply_task_status_updates(response)

    def _apply_task_status_updates(self, response: dict[str, Any]) -> None:
        self.last_sync = response["time"]

        new_status_tasks: set[SwarmTask] = set()
        for current_status, task_ids in response["tasks_by_status"].items():
            task_ids = set(task_ids).intersection(self.tasks)
            for task_id in task_ids:
                task = self.tasks[task_id]
                if current_status != task.status:
                    task.status = current_status
                    new_status_tasks.add(task)
        self._refresh_task_status_map(new_status_tasks)

    async def _synchronize_max_concurrently_running_async(self) -> None:
        """Refresh workflow-level max_concurrently_running from server."""
        app_route = f"/workflow/{self.workflow_id}/get_max_concurrently_running"
        _, response = await self._request_async(
            app_route=app_route, message={}, request_type="get"
        )
        self.max_concurrently_running = response["max_concurrently_running"]

    async def _synchronize_array_max_concurrently_running_async(self) -> None:
        """Refresh per-array max_concurrently_running limits from server."""
        if not self.arrays:
            return

        async def fetch_array_concurrency(aid: int) -> tuple[int, int]:
            app_route = f"/array/{aid}/get_array_max_concurrently_running"
            _, response = await self._request_async(
                app_route=app_route, message={}, request_type="get"
            )
            return aid, response["max_concurrently_running"]

        # Fetch all array limits in parallel
        results = await asyncio.gather(
            *[fetch_array_concurrency(aid) for aid in self.arrays],
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, BaseException):
                logger.warning(
                    "Failed to sync array concurrency",
                    error=str(result),
                    exc_info=result,
                )
            else:
                aid, max_running = result  # type: tuple[int, int]
                self.arrays[aid].max_concurrently_running = max_running

    @bind_method_context(workflow_run_id="workflow_run_id")
    async def queue_task_batch_async(self, tasks: list[SwarmTask]) -> None:
        first_task = tasks[0]
        task_resources = first_task.current_task_resources
        if not task_resources.is_bound:
            task_resources.bind()

        logger.debug(
            f"Queueing {len(tasks)} tasks to server",
            array_id=first_task.array_id,
            batch_size=len(tasks),
        )

        app_route = f"/array/{first_task.array_id}/queue_task_batch"
        _, response = await self._request_async(
            app_route=app_route,
            message={
                "task_ids": [task.task_id for task in tasks],
                "task_resources_id": task_resources.id,
                "workflow_run_id": self.workflow_run_id,
                "cluster_id": first_task.cluster.id,
            },
            request_type="post",
        )
        updated_tasks = set()
        for status, task_ids in response["tasks_by_status"].items():
            for task_id in task_ids:
                task = self.tasks[task_id]
                task.status = status
                updated_tasks.add(task)
        self._refresh_task_status_map(updated_tasks)

    def _set_validated_task_resources(self, task: SwarmTask) -> None:
        # original resources
        task_resources = task.current_task_resources

        # update with dynamic params
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

        # now check if we need to coerce them to valid values
        validated_task_resources = task_resources.coerce_resources()

        # check for a cached version
        validated_resource_hash = hash(validated_task_resources)
        try:
            validated_task_resources = self._task_resources[validated_resource_hash]
        except KeyError:
            self._task_resources[validated_resource_hash] = validated_task_resources

        # now set as current
        task.current_task_resources = validated_task_resources

    def _set_adjusted_task_resources(self, task: SwarmTask) -> None:
        """Adjust the swarm task's parameters.

        Use the cluster API to generate the new resources, then bind to input swarmtask.
        """
        # current resources
        task_resources = task.current_task_resources.adjust_resources(
            resource_scales=task.resource_scales,
            fallback_queues=task.fallback_queues,
        )

        resource_hash = hash(task_resources)
        try:
            task_resources = self._task_resources[resource_hash]
        except KeyError:
            self._task_resources[resource_hash] = task_resources
        task.current_task_resources = task_resources
