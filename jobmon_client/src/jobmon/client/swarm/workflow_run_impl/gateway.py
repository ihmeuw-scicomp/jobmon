"""ServerGateway: Centralized HTTP communication with the Jobmon server.

This module consolidates all HTTP requests for workflow run operations
into a single class, providing:
- Type-safe response objects
- Consistent session management
- Clear API boundaries for testing
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import aiohttp
import structlog

from jobmon.core.requester import Requester

logger = structlog.get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Response Dataclasses
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HeartbeatResponse:
    """Response from heartbeat logging."""

    status: str


@dataclass(frozen=True)
class StatusUpdateResponse:
    """Response from workflow run status update."""

    status: str


@dataclass(frozen=True)
class QueueResponse:
    """Response from task queueing operations."""

    tasks_by_status: dict[str, list[int]]


@dataclass(frozen=True)
class TaskStatusUpdatesResponse:
    """Response from task status synchronization."""

    time: datetime
    tasks_by_status: dict[str, list[int]]


@dataclass(frozen=True)
class WorkflowMetadata:
    """Workflow metadata from the server."""

    workflow_id: int
    dag_id: int
    max_concurrently_running: int


@dataclass(frozen=True)
class DownstreamTasksResponse:
    """Response from downstream tasks query."""

    # Maps task_id -> (node_id, list of downstream_node_ids)
    downstream_tasks: dict[int, tuple[int, list[int]]]


# ──────────────────────────────────────────────────────────────────────────────
# ServerGateway
# ──────────────────────────────────────────────────────────────────────────────


class ServerGateway:
    """Centralized HTTP communication with the Jobmon server.

    This class consolidates all server communication for workflow run operations,
    providing a clean interface with typed responses.

    Usage:
        async with ServerGateway(requester, workflow_id, workflow_run_id) as gateway:
            response = await gateway.log_heartbeat(status="R", next_report_increment=30.0)

    Or without context manager (session managed externally):
        gateway = ServerGateway(requester, workflow_id, workflow_run_id)
        gateway.set_session(existing_session)
        response = await gateway.log_heartbeat(...)
    """

    def __init__(
        self,
        requester: Requester,
        workflow_id: int,
        workflow_run_id: int,
    ) -> None:
        """Initialize the gateway.

        Args:
            requester: The Requester instance for HTTP communication.
            workflow_id: The workflow ID for workflow-level operations.
            workflow_run_id: The workflow run ID for run-level operations.
        """
        self.requester = requester
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self._session: Optional[aiohttp.ClientSession] = None
        self._owns_session: bool = False

    async def __aenter__(self) -> "ServerGateway":
        """Async context manager entry - creates session."""
        self._session = aiohttp.ClientSession()
        self._owns_session = True
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Async context manager exit - closes session if we own it."""
        await self.close()

    def set_session(self, session: aiohttp.ClientSession) -> None:
        """Set an external session (gateway will not close it)."""
        self._session = session
        self._owns_session = False

    async def close(self) -> None:
        """Close the session if we own it."""
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._owns_session = False

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure we have an active session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._owns_session = True
        return self._session

    async def _request(
        self,
        app_route: str,
        message: dict[str, Any],
        request_type: str,
        tenacious: bool = True,
    ) -> tuple[int, Any]:
        """Make an async HTTP request.

        Args:
            app_route: The API route to request.
            message: The request payload.
            request_type: HTTP method ('get', 'post', 'put').
            tenacious: Whether to use retry logic.

        Returns:
            Tuple of (status_code, response_content).
        """
        session = await self._ensure_session()
        return await self.requester.send_request_async(
            session=session,
            app_route=app_route,
            message=message,
            request_type=request_type,
            tenacious=tenacious,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Heartbeat Operations
    # ──────────────────────────────────────────────────────────────────────────

    async def log_heartbeat(
        self,
        status: str,
        next_report_increment: float,
    ) -> HeartbeatResponse:
        """Log a heartbeat to the server.

        Args:
            status: Current workflow run status.
            next_report_increment: Seconds until next expected heartbeat.

        Returns:
            HeartbeatResponse with potentially updated status.
        """
        _, response = await self._request(
            app_route=f"/workflow_run/{self.workflow_run_id}/log_heartbeat",
            message={
                "status": status,
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        return HeartbeatResponse(status=response["status"])

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle Operations
    # ──────────────────────────────────────────────────────────────────────────

    async def update_status(self, status: str) -> StatusUpdateResponse:
        """Update the workflow run status.

        Args:
            status: The new status to set.

        Returns:
            StatusUpdateResponse with the actual new status.
        """
        _, response = await self._request(
            app_route=f"/workflow_run/{self.workflow_run_id}/update_status",
            message={"status": status},
            request_type="put",
        )
        return StatusUpdateResponse(status=response["status"])

    async def terminate_task_instances(self) -> None:
        """Signal the server to terminate all task instances for this workflow run."""
        await self._request(
            app_route=f"/workflow_run/{self.workflow_run_id}/terminate_task_instances",
            message={},
            request_type="put",
        )

    # ──────────────────────────────────────────────────────────────────────────
    # State Synchronization Operations
    # ──────────────────────────────────────────────────────────────────────────

    async def request_triage(self) -> None:
        """Request server to triage overdue task instances."""
        logger.debug("Requesting triage check for overdue task instances")
        await self._request(
            app_route=f"/workflow_run/{self.workflow_run_id}/set_status_for_triaging",
            message={},
            request_type="post",
        )
        logger.debug("Triage check completed")

    async def get_task_status_updates(
        self,
        since: Optional[datetime] = None,
    ) -> TaskStatusUpdatesResponse:
        """Fetch task status changes from the server.

        Args:
            since: If provided, only get updates since this time.
                   If None, get full status snapshot.

        Returns:
            TaskStatusUpdatesResponse with time and status updates.
        """
        message = {} if since is None else {"last_sync": str(since)}
        _, response = await self._request(
            app_route=f"/workflow/{self.workflow_id}/task_status_updates",
            message=message,
            request_type="post",
        )
        return TaskStatusUpdatesResponse(
            time=response["time"],
            tasks_by_status=response["tasks_by_status"],
        )

    async def get_workflow_concurrency(self) -> int:
        """Get the workflow-level max_concurrently_running limit.

        Returns:
            The maximum number of tasks that can run concurrently.
        """
        _, response = await self._request(
            app_route=f"/workflow/{self.workflow_id}/get_max_concurrently_running",
            message={},
            request_type="get",
        )
        return response["max_concurrently_running"]

    async def get_array_concurrency(self, array_id: int) -> int:
        """Get the array-level max_concurrently_running limit.

        Args:
            array_id: The array to query.

        Returns:
            The maximum number of tasks that can run concurrently in this array.
        """
        _, response = await self._request(
            app_route=f"/array/{array_id}/get_array_max_concurrently_running",
            message={},
            request_type="get",
        )
        return response["max_concurrently_running"]

    # ──────────────────────────────────────────────────────────────────────────
    # Task Queueing Operations
    # ──────────────────────────────────────────────────────────────────────────

    async def queue_task_batch(
        self,
        array_id: int,
        task_ids: list[int],
        task_resources_id: int,
        cluster_id: int,
    ) -> QueueResponse:
        """Queue a batch of tasks for execution.

        Args:
            array_id: The array containing the tasks.
            task_ids: List of task IDs to queue.
            task_resources_id: The bound task resources ID.
            cluster_id: The cluster to run on.

        Returns:
            QueueResponse with task status updates.
        """
        _, response = await self._request(
            app_route=f"/array/{array_id}/queue_task_batch",
            message={
                "task_ids": task_ids,
                "task_resources_id": task_resources_id,
                "workflow_run_id": self.workflow_run_id,
                "cluster_id": cluster_id,
            },
            request_type="post",
        )
        return QueueResponse(tasks_by_status=response["tasks_by_status"])

    # ──────────────────────────────────────────────────────────────────────────
    # Workflow Setup Operations (used during initialization)
    # ──────────────────────────────────────────────────────────────────────────

    async def get_workflow_metadata(self) -> WorkflowMetadata:
        """Fetch workflow metadata from the server.

        Returns:
            WorkflowMetadata with dag_id and max_concurrently_running.

        Raises:
            ValueError: If no workflow found for the given ID.
        """
        _, response = await self._request(
            app_route=f"/workflow/{self.workflow_id}/fetch_workflow_metadata",
            message={},
            request_type="get",
        )
        database_wf = response["workflow"]
        if not database_wf:
            raise ValueError(f"No workflow found for workflow id {self.workflow_id}")

        workflow_id, dag_id, max_concurrently_running = database_wf
        return WorkflowMetadata(
            workflow_id=workflow_id,
            dag_id=dag_id,
            max_concurrently_running=max_concurrently_running,
        )

    async def get_tasks(
        self,
        max_task_id: int = 0,
        chunk_size: int = 500,
    ) -> dict[int, Any]:
        """Fetch tasks that need to be run from the database.

        Args:
            max_task_id: Starting point for pagination (exclusive).
            chunk_size: Number of tasks to fetch per request.

        Returns:
            Dictionary mapping task_id to task metadata.
        """
        _, response = await self._request(
            app_route=f"/workflow/get_tasks/{self.workflow_id}",
            message={"max_task_id": max_task_id, "chunk_size": chunk_size},
            request_type="get",
        )
        return response["tasks"]

    async def get_downstream_tasks(
        self,
        task_ids: list[int],
        dag_id: int,
    ) -> DownstreamTasksResponse:
        """Fetch downstream edges for a batch of tasks.

        Args:
            task_ids: Task IDs to query edges for.
            dag_id: The DAG ID for this workflow.

        Returns:
            DownstreamTasksResponse with edge information.
        """
        _, response = await self._request(
            app_route="/task/get_downstream_tasks",
            message={"task_ids": task_ids, "dag_id": dag_id},
            request_type="post",
        )

        # Convert the response format
        downstream_tasks: dict[int, tuple[int, list[int]]] = {}
        for task_id_str, values in response["downstream_tasks"].items():
            task_id = int(task_id_str)
            node_id, downstream_node_ids = values
            downstream_tasks[task_id] = (node_id, downstream_node_ids)

        return DownstreamTasksResponse(downstream_tasks=downstream_tasks)

    async def get_server_time(self) -> datetime:
        """Get the current time from the server.

        Returns:
            The server's current datetime.
        """
        _, response = await self._request(
            app_route="/time",
            message={},
            request_type="get",
        )
        return response["time"]

    # ──────────────────────────────────────────────────────────────────────────
    # Synchronous Wrappers (for backward compatibility)
    # ──────────────────────────────────────────────────────────────────────────

    def log_heartbeat_sync(
        self,
        status: str,
        next_report_increment: float,
    ) -> HeartbeatResponse:
        """Synchronous heartbeat logging (uses sync Requester).

        Args:
            status: Current workflow run status.
            next_report_increment: Seconds until next expected heartbeat.

        Returns:
            HeartbeatResponse with potentially updated status.
        """
        _, response = self.requester.send_request(
            app_route=f"/workflow_run/{self.workflow_run_id}/log_heartbeat",
            message={
                "status": status,
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        return HeartbeatResponse(status=response["status"])

    def update_status_sync(self, status: str) -> StatusUpdateResponse:
        """Synchronous status update (uses sync Requester).

        Args:
            status: The new status to set.

        Returns:
            StatusUpdateResponse with the actual new status.
        """
        _, response = self.requester.send_request(
            app_route=f"/workflow_run/{self.workflow_run_id}/update_status",
            message={"status": status},
            request_type="put",
        )
        return StatusUpdateResponse(status=response["status"])

    def get_server_time_sync(self) -> datetime:
        """Get the current time from the server (synchronous).

        Returns:
            The server's current datetime.
        """
        _, response = self.requester.send_request(
            app_route="/time",
            message={},
            request_type="get",
        )
        return response["time"]

