"""Synchronizer: State synchronization between local state and server.

This service manages synchronization of workflow run state with the Jobmon server,
including task status updates and concurrency limit refreshes.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import structlog

from jobmon.client.swarm.state import StateUpdate

if TYPE_CHECKING:
    from jobmon.client.swarm.gateway import ServerGateway

logger = structlog.get_logger(__name__)


class Synchronizer:
    """Keeps local workflow run state aligned with the server.

    The Synchronizer is responsible for:
    - Requesting server to triage overdue task instances
    - Fetching task status updates (full or incremental)
    - Synchronizing workflow-level concurrency limits
    - Synchronizing per-array concurrency limits

    All operations are performed in parallel for better throughput.

    Usage:
        sync = Synchronizer(
            gateway=gateway,
            task_ids=set(tasks.keys()),
            array_ids=set(arrays.keys()),
        )

        # Single sync tick
        update = await sync.tick(full_sync=False, last_sync=last_sync_time)
        state.apply_update(update)

        # Full sync (ignores last_sync time)
        update = await sync.tick(full_sync=True, last_sync=None)
        state.apply_update(update)
    """

    def __init__(
        self,
        gateway: "ServerGateway",
        task_ids: set[int],
        array_ids: set[int],
    ) -> None:
        """Initialize the synchronizer.

        Args:
            gateway: ServerGateway for server communication.
            task_ids: Set of known task IDs to filter status updates.
            array_ids: Set of array IDs to sync concurrency limits for.
        """
        self._gateway = gateway
        self._task_ids = task_ids
        self._array_ids = array_ids

    @property
    def task_ids(self) -> set[int]:
        """Set of task IDs this synchronizer knows about."""
        return self._task_ids

    @property
    def array_ids(self) -> set[int]:
        """Set of array IDs to sync concurrency limits for."""
        return self._array_ids

    def update_task_ids(self, task_ids: set[int]) -> None:
        """Update the set of known task IDs.

        Args:
            task_ids: New set of task IDs.
        """
        self._task_ids = task_ids

    def update_array_ids(self, array_ids: set[int]) -> None:
        """Update the set of array IDs.

        Args:
            array_ids: New set of array IDs.
        """
        self._array_ids = array_ids

    async def tick(
        self,
        full_sync: bool = False,
        last_sync: Optional[datetime] = None,
    ) -> StateUpdate:
        """Perform a synchronization tick with the server.

        Runs all sync operations in parallel:
        1. Request triage for overdue task instances
        2. Fetch task status updates
        3. Fetch workflow concurrency limit
        4. Fetch array concurrency limits

        Args:
            full_sync: If True, fetch all task statuses regardless of last_sync.
            last_sync: Timestamp of last sync (for incremental updates).
                      Ignored if full_sync is True.

        Returns:
            StateUpdate containing all state changes to apply.

        Note:
            Individual operation failures are logged but don't stop other operations.
            The returned StateUpdate will contain whatever data was successfully fetched.
        """
        # Build list of tasks to run in parallel
        tasks = [
            self._request_triage(),
            self._get_task_updates(full_sync=full_sync, last_sync=last_sync),
            self._get_workflow_concurrency(),
        ]

        # Only fetch array limits if we have arrays
        if self._array_ids:
            tasks.append(self._get_array_concurrency_limits())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and combine into a single StateUpdate
        combined = StateUpdate.empty()
        op_names = ["triage", "task_status_updates", "workflow_concurrency"]
        if self._array_ids:
            op_names.append("array_concurrency")

        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                logger.warning(
                    f"Sync operation '{op_names[i]}' failed",
                    error=str(result),
                    exc_info=result,
                )
            elif isinstance(result, StateUpdate):
                combined = combined.merge(result)

        logger.debug(
            "Synchronization complete",
            full_sync=full_sync,
            task_status_count=len(combined.task_statuses),
            workflow_limit=combined.max_concurrently_running,
            array_limit_count=len(combined.array_limits),
        )

        return combined

    async def _request_triage(self) -> StateUpdate:
        """Request server to triage overdue task instances.

        Returns:
            Empty StateUpdate (triage doesn't return data).
        """
        logger.debug("Requesting triage check for overdue task instances")
        await self._gateway.request_triage()
        logger.debug("Triage check completed")
        return StateUpdate.empty()

    async def _get_task_updates(
        self,
        full_sync: bool,
        last_sync: Optional[datetime],
    ) -> StateUpdate:
        """Fetch task status updates from the server.

        Args:
            full_sync: If True, fetch all statuses regardless of last_sync.
            last_sync: Timestamp for incremental sync.

        Returns:
            StateUpdate with task status changes and sync time.
        """
        since = None if full_sync else last_sync
        response = await self._gateway.get_task_status_updates(since=since)

        # Build task status mapping, filtering to known tasks
        task_statuses: dict[int, str] = {}
        for status, task_ids in response.tasks_by_status.items():
            for tid in task_ids:
                if tid in self._task_ids:
                    task_statuses[tid] = status

        return StateUpdate(
            task_statuses=task_statuses,
            sync_time=response.time,
        )

    async def _get_workflow_concurrency(self) -> StateUpdate:
        """Fetch workflow-level concurrency limit from server.

        Returns:
            StateUpdate with max_concurrently_running.
        """
        limit = await self._gateway.get_workflow_concurrency()
        return StateUpdate(max_concurrently_running=limit)

    async def _get_array_concurrency_limits(self) -> StateUpdate:
        """Fetch per-array concurrency limits from server.

        Returns:
            StateUpdate with array_limits.
        """
        if not self._array_ids:
            return StateUpdate.empty()

        async def fetch_one(aid: int) -> tuple[int, int]:
            limit = await self._gateway.get_array_concurrency(aid)
            return aid, limit

        results = await asyncio.gather(
            *[fetch_one(aid) for aid in self._array_ids],
            return_exceptions=True,
        )

        array_limits: dict[int, int] = {}
        for result in results:
            if isinstance(result, BaseException):
                logger.warning(
                    "Failed to fetch array concurrency limit",
                    error=str(result),
                    exc_info=result,
                )
            else:
                aid, limit = result
                array_limits[aid] = limit

        return StateUpdate(array_limits=array_limits)

    async def request_triage_only(self) -> None:
        """Convenience method to only request triage without full sync.

        This is useful when you only need to trigger the server's
        triage logic without fetching state updates.
        """
        await self._gateway.request_triage()

    async def get_task_updates_only(
        self,
        full_sync: bool = False,
        last_sync: Optional[datetime] = None,
    ) -> StateUpdate:
        """Convenience method to only fetch task status updates.

        Args:
            full_sync: If True, fetch all statuses.
            last_sync: Timestamp for incremental sync.

        Returns:
            StateUpdate with task status changes.
        """
        return await self._get_task_updates(full_sync=full_sync, last_sync=last_sync)

    async def get_concurrency_limits_only(self) -> StateUpdate:
        """Convenience method to only fetch concurrency limits.

        Returns:
            StateUpdate with workflow and array concurrency limits.
        """
        tasks = [self._get_workflow_concurrency()]
        if self._array_ids:
            tasks.append(self._get_array_concurrency_limits())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        combined = StateUpdate.empty()
        for result in results:
            if isinstance(result, StateUpdate):
                combined = combined.merge(result)

        return combined
