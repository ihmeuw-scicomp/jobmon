"""SwarmBuilder: Service for building workflow run state.

This module provides the SwarmBuilder service that handles all initialization
of workflow run state, including:
- Building from an in-memory Workflow object (new runs)
- Building from database state (resume scenarios)
- Fetching tasks and dependencies in chunks with periodic heartbeats
"""

from __future__ import annotations

import ast
import numbers
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import structlog

from jobmon.client.swarm.array import SwarmArray
from jobmon.client.swarm.gateway import ServerGateway
from jobmon.client.swarm.services.heartbeat import HeartbeatService
from jobmon.client.swarm.state import SwarmState
from jobmon.client.swarm.task import SwarmTask
from jobmon.client.task_resources import TaskResources
from jobmon.core.cluster import Cluster
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core.exceptions import EmptyWorkflowError
from jobmon.core.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

logger = structlog.get_logger(__name__)


class SwarmBuilder:
    """Builds workflow run state from Workflow objects or database.

    This service handles all initialization logic, extracting it from the
    WorkflowRun class. It builds fully initialized SwarmState that can be
    passed directly to the WorkflowRunOrchestrator.

    Usage:
        # For new runs (from Workflow object):
        builder = SwarmBuilder(requester, workflow_run_id)
        builder.build_from_workflow(workflow)

        # For resume runs (from database):
        builder = SwarmBuilder(requester, workflow_run_id)
        builder.build_from_workflow_id(workflow_id)

        # Then pass state and gateway to orchestrator:
        orchestrator = WorkflowRunOrchestrator(builder.state, builder._gateway, config)
        result = await orchestrator.run(distributor_alive_callable)
    """

    def __init__(
        self,
        requester: Requester,
        workflow_run_id: int,
        heartbeat_interval: float = 30.0,
        heartbeat_report_by_buffer: float = 1.5,
        initial_status: str = WorkflowRunStatus.BOUND,
    ) -> None:
        """Initialize the builder.

        Args:
            requester: The Requester instance for HTTP communication.
            workflow_run_id: The workflow run ID.
            heartbeat_interval: Interval between heartbeats in seconds.
            heartbeat_report_by_buffer: Multiplier for next report time.
            initial_status: Initial workflow run status.
        """
        self.requester = requester
        self.workflow_run_id = workflow_run_id
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self.initial_status = initial_status

        # SwarmState will be created once workflow_id is known
        self._state: Optional[SwarmState] = None

        # Workflow metadata (set during build, before state creation)
        self._workflow_id: Optional[int] = None
        self._dag_id: Optional[int] = None
        self._max_concurrently_running: int = 10000
        self._last_sync: Optional[datetime] = None

        # Heartbeat tracking
        self._last_heartbeat_time: float = 0.0
        self._status: str = initial_status

        # Gateway (created lazily once workflow_id is known)
        self._gateway: Optional[ServerGateway] = None
        self._heartbeat_service: Optional[HeartbeatService] = None

    def _ensure_state(self) -> SwarmState:
        """Get or create the SwarmState (requires workflow_id to be set)."""
        if self._state is None:
            if self._workflow_id is None or self._dag_id is None:
                raise RuntimeError(
                    "Cannot create SwarmState before workflow metadata is set. "
                    "Call _set_workflow_metadata() first."
                )
            self._state = SwarmState(
                workflow_id=self._workflow_id,
                workflow_run_id=self.workflow_run_id,
                dag_id=self._dag_id,
                max_concurrently_running=self._max_concurrently_running,
                status=self._status,
            )
            if self._last_sync is not None:
                self._state.last_sync = self._last_sync
        return self._state

    def _ensure_gateway(self) -> ServerGateway:
        """Get or create the gateway (requires workflow_id to be set)."""
        if self._gateway is None:
            if self._workflow_id is None:
                raise RuntimeError(
                    "Cannot create gateway before workflow_id is set. "
                    "Call build_from_workflow() or set workflow_id first."
                )
            self._gateway = ServerGateway(
                requester=self.requester,
                workflow_id=self._workflow_id,
                workflow_run_id=self.workflow_run_id,
            )
        return self._gateway

    def _ensure_heartbeat_service(self) -> HeartbeatService:
        """Get or create the heartbeat service."""
        if self._heartbeat_service is None:
            gateway = self._ensure_gateway()
            self._heartbeat_service = HeartbeatService(
                gateway=gateway,
                interval=self.heartbeat_interval,
                report_by_buffer=self.heartbeat_report_by_buffer,
                initial_status=self._status,
            )
        return self._heartbeat_service

    def _log_heartbeat(self) -> None:
        """Log a synchronous heartbeat if interval has elapsed.

        Uses direct requester calls before workflow_id is set,
        or HeartbeatService after.
        """
        if self._workflow_id is None:
            # Direct request during early initialization
            next_report = self.heartbeat_interval * self.heartbeat_report_by_buffer
            _, response = self.requester.send_request(
                app_route=f"/workflow_run/{self.workflow_run_id}/log_heartbeat",
                message={
                    "status": self._status,
                    "next_report_increment": next_report,
                },
                request_type="post",
            )
            self._status = response["status"]
            self._last_heartbeat_time = time.time()
        else:
            # Use HeartbeatService
            heartbeat = self._ensure_heartbeat_service()
            heartbeat.set_status(self._status)
            update = heartbeat.tick_sync()
            if update.workflow_run_status:
                self._status = update.workflow_run_status
            self._last_heartbeat_time = heartbeat.last_heartbeat_time

    def _maybe_heartbeat(self) -> None:
        """Log heartbeat if enough time has passed since last one."""
        if (time.time() - self._last_heartbeat_time) > self.heartbeat_interval:
            self._log_heartbeat()

    def _get_server_time(self) -> datetime:
        """Get current time from server."""
        if self._gateway is not None:
            return self._gateway.get_server_time_sync()
        _, response = self.requester.send_request(
            app_route="/time",
            message={},
            request_type="get",
        )
        return response["time"]

    def _update_status(self, status: str) -> None:
        """Update workflow run status on server."""
        gateway = self._ensure_gateway()
        response = gateway.update_status_sync(status)
        self._status = response.status

    # ──────────────────────────────────────────────────────────────────────────
    # Build from Workflow (New Runs)
    # ──────────────────────────────────────────────────────────────────────────

    def build_from_workflow(self, workflow: "Workflow") -> None:
        """Build state from an in-memory Workflow object.

        This is used for new workflow runs where the Workflow object is
        already constructed in memory. After calling this, access
        `builder.state` and `builder._gateway` to get the built state.

        Args:
            workflow: The Workflow object containing tasks and arrays.
        """
        logger.debug("Building swarm from workflow", num_tasks=len(workflow.tasks))

        # Set workflow metadata (must be done before creating SwarmState)
        self._workflow_id = workflow.workflow_id
        self._dag_id = workflow.dag_id
        self._max_concurrently_running = workflow.max_concurrently_running
        self._last_sync = self._get_server_time()

        # Create SwarmState
        state = self._ensure_state()

        # Construct arrays and add to state
        for array in workflow.arrays.values():
            swarm_array = SwarmArray(
                array.array_id,
                array.max_concurrently_running,
                array_name=array.name,
            )
            state.add_array(swarm_array)

        # Build a temporary task map for relationship resolution
        temp_tasks: dict[int, SwarmTask] = {}

        # Construct SwarmTasks from Client Tasks
        for task in workflow.tasks.values():
            cluster = workflow.get_cluster_by_name(task.cluster_name)
            fallback_queues = [
                cluster.get_queue(queue) for queue in task.fallback_queues
            ]

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
            temp_tasks[task.task_id] = swarm_task

        # Create relationships on swarm tasks
        for task in workflow.tasks.values():
            swarm_task = temp_tasks[task.task_id]

            # Assign upstream and downstream counts
            swarm_task.num_upstreams = len(task.upstream_tasks)
            swarm_task.downstream_swarm_tasks = {
                temp_tasks[t.task_id] for t in task.downstream_tasks
            }

            # Create array association
            state.arrays[swarm_task.array_id].add_task(swarm_task)

            # Add task to SwarmState (handles status bucket)
            state.add_task(swarm_task)

        # Compute initial upstream done counts for downstream propagation
        state.compute_initial_upstream_done_counts()

    # ──────────────────────────────────────────────────────────────────────────
    # Build from Workflow ID (Resume Scenarios)
    # ──────────────────────────────────────────────────────────────────────────

    def build_from_workflow_id(
        self,
        workflow_id: int,
        edge_chunk_size: int = 500,
    ) -> None:
        """Build state by fetching from database.

        This is used for resume scenarios where we need to reconstruct
        the workflow state from the database. After calling this, access
        `builder.state` and `builder._gateway` to get the built state.

        Args:
            workflow_id: The workflow ID to fetch.
            edge_chunk_size: Number of edges to fetch per chunk.
        """
        # Log initial heartbeat before starting work
        self._log_heartbeat()

        # Fetch workflow metadata
        self._set_workflow_metadata(workflow_id)

        # Fetch tasks in chunks with periodic heartbeats
        self._set_tasks_from_db(chunk_size=edge_chunk_size)

        # Fetch dependencies in chunks with periodic heartbeats
        self._set_downstreams_from_db(chunk_size=edge_chunk_size)

        # Transition to BOUND now that we're initialized
        self._update_status(WorkflowRunStatus.BOUND)

    def _set_workflow_metadata(self, workflow_id: int) -> None:
        """Fetch workflow metadata from server and create SwarmState."""
        _, resp = self.requester.send_request(
            app_route=f"/workflow/{workflow_id}/fetch_workflow_metadata",
            message={},
            request_type="get",
        )

        database_wf = resp["workflow"]
        if not database_wf:
            raise EmptyWorkflowError(f"No workflow found for workflow id {workflow_id}")

        wf_id, dag_id, max_concurrently_running = database_wf

        self._workflow_id = wf_id
        self._dag_id = dag_id
        self._max_concurrently_running = max_concurrently_running
        self._last_sync = self._get_server_time()

        # Now we can create SwarmState with the metadata
        self._ensure_state()

        logger.info(f"Fetched workflow metadata: workflow_id={wf_id}, dag_id={dag_id}")

    def _set_tasks_from_db(self, chunk_size: int = 500) -> None:
        """Fetch tasks that need to run from database in chunks.

        Logs heartbeats periodically during long fetches.
        """
        cluster_registry: dict[str, Cluster] = {}
        all_tasks_returned = False
        max_task_id = 0

        logger.info("Fetching tasks from the database")

        while not all_tasks_returned:
            # Log heartbeat if needed
            self._maybe_heartbeat()
            state = self._ensure_state()
            if len(state.tasks) > 0 and len(state.tasks) % 1000 == 0:
                logger.info(
                    f"Still fetching tasks, {len(state.tasks)} collected so far..."
                )

            # Fetch next chunk
            _, resp = self.requester.send_request(
                app_route=f"/workflow/get_tasks/{self._workflow_id}",
                message={"max_task_id": max_task_id, "chunk_size": chunk_size},
                request_type="get",
            )
            task_dict = resp["tasks"]

            # Check if done
            if not task_dict:
                return
            if len(task_dict) < chunk_size:
                all_tasks_returned = True

            max_task_id = max(task_dict)

            # Process tasks
            for task_id_str, metadata in task_dict.items():
                self._process_task_from_db(int(task_id_str), metadata, cluster_registry)

        logger.info(f"All tasks fetched: {len(self._ensure_state().tasks)} total")

    def _process_task_from_db(
        self,
        task_id: int,
        metadata: tuple,
        cluster_registry: dict[str, Cluster],
    ) -> None:
        """Process a single task from database response."""
        state = self._ensure_state()

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

        # Parse resource_scales
        resource_scales = ast.literal_eval(resource_scales)
        for resource, scaler in resource_scales.items():
            if not isinstance(scaler, numbers.Number):
                if isinstance(scaler, list):
                    resource_scales[resource] = iter(scaler)
                else:
                    raise ValueError(
                        f"Cannot run CLI resume on non-numeric custom resource "
                        f"scales: found {resource} scaler {scaler} in resource "
                        "scales retrieved from the Jobmon DB."
                    )

        # Parse other fields
        fallback_queues_parsed = ast.literal_eval(fallback_queues)
        requested_resources = ast.literal_eval(requested_resources)

        # Get or create cluster
        if cluster_name not in cluster_registry:
            cluster = Cluster(cluster_name=cluster_name, requester=self.requester)
            cluster.bind()
            cluster_registry[cluster_name] = cluster
        cluster = cluster_registry[cluster_name]

        # Create queue and fallback queues
        queue = cluster.get_queue(queue_name)
        fallback_queue_objs = [cluster.get_queue(q) for q in fallback_queues_parsed]

        # Create TaskResources
        task_resources = TaskResources(
            requested_resources=requested_resources,
            queue=queue,
            requester=self.requester,
        )

        # Create SwarmTask
        swarm_task = SwarmTask(
            task_id=task_id,
            array_id=array_id,
            status=status,
            max_attempts=max_attempts,
            task_resources=task_resources,
            cluster=cluster,
            resource_scales=resource_scales,
            fallback_queues=fallback_queue_objs,
        )

        # Create or update array in SwarmState
        if array_id not in state.arrays:
            array = SwarmArray(
                array_id=array_id,
                max_concurrently_running=array_concurrency,
            )
            state.add_array(array)
        state.arrays[array_id].add_task(swarm_task)

        # Add task to SwarmState (handles status bucket)
        state.add_task(swarm_task)

    def _set_downstreams_from_db(self, chunk_size: int = 500) -> None:
        """Fetch downstream edges from database in chunks.

        Logs heartbeats periodically during long fetches.
        """
        state = self._ensure_state()

        # Build node_id -> task_id and task_id -> downstream_node_ids maps
        task_node_id_map: dict[int, int] = {}
        task_edge_map: dict[int, set[int]] = {}

        task_ids = list(state.tasks.keys())
        start_idx = 0

        logger.info("Setting dependencies on tasks")

        while start_idx < len(task_ids):
            # Log heartbeat if needed
            self._maybe_heartbeat()

            # Get chunk
            end_idx = start_idx + chunk_size
            task_id_chunk = task_ids[start_idx:end_idx]
            start_idx = end_idx

            # Fetch edges for this chunk
            _, edge_resp = self.requester.send_request(
                app_route="/task/get_downstream_tasks",
                message={"task_ids": task_id_chunk, "dag_id": self._dag_id},
                request_type="post",
            )

            downstream_tasks = edge_resp["downstream_tasks"]

            # Process response
            for task_id_str, values in downstream_tasks.items():
                node_id, downstream_node_ids = values
                task_id = int(task_id_str)
                task_node_id_map[node_id] = task_id
                task_edge_map[task_id] = (
                    set(downstream_node_ids) if downstream_node_ids else set()
                )

        # Build the dependency graph
        logger.info("All edges fetched from the database, building dependency graph")

        for task_id, swarm_task in state.tasks.items():
            downstream_node_ids = task_edge_map.get(task_id, set())
            for downstream_node_id in downstream_node_ids:
                downstream_task_id = task_node_id_map.get(downstream_node_id)
                if downstream_task_id is not None:
                    downstream_swarm_task = state.tasks.get(downstream_task_id)
                    if downstream_swarm_task is not None:
                        swarm_task.downstream_swarm_tasks.add(downstream_swarm_task)
                        downstream_swarm_task.num_upstreams += 1

        logger.info("Task DAG fully constructed, swarm is ready to run")

    # ──────────────────────────────────────────────────────────────────────────
    # Properties for backward compatibility
    # ──────────────────────────────────────────────────────────────────────────

    @property
    def state(self) -> Optional[SwarmState]:
        """Get the SwarmState (available after build)."""
        return self._state

    @property
    def tasks(self) -> dict[int, SwarmTask]:
        """Get the built tasks dictionary."""
        return self._state.tasks if self._state else {}

    @property
    def arrays(self) -> dict[int, SwarmArray]:
        """Get the built arrays dictionary."""
        return self._state.arrays if self._state else {}

    @property
    def task_status_map(self) -> dict[str, set[SwarmTask]]:
        """Get the task status map."""
        return self._state._task_status_map if self._state else {}

    @property
    def workflow_id(self) -> Optional[int]:
        """Get the workflow ID (set after build)."""
        return self._workflow_id

    @property
    def dag_id(self) -> Optional[int]:
        """Get the DAG ID (set after build)."""
        return self._dag_id

    @property
    def max_concurrently_running(self) -> int:
        """Get max concurrently running limit."""
        return (
            self._state.max_concurrently_running
            if self._state
            else self._max_concurrently_running
        )

    @property
    def last_sync(self) -> Optional[datetime]:
        """Get last sync time."""
        return self._state.last_sync if self._state else self._last_sync

    @property
    def status(self) -> str:
        """Get current status."""
        return self._state.status if self._state else self._status

    @property
    def num_previously_complete(self) -> int:
        """Get count of tasks that were already complete."""
        return self._state.get_done_count() if self._state else 0
