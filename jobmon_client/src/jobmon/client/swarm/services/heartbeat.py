"""HeartbeatService: Background heartbeat management for workflow runs.

This service manages periodic heartbeat logging to the Jobmon server,
ensuring the workflow run is kept alive and receiving any status updates
from the server (e.g., pause/resume signals).
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Optional

import structlog

from jobmon.client.swarm.state import StateUpdate

if TYPE_CHECKING:
    from jobmon.client.swarm.gateway import ServerGateway

logger = structlog.get_logger(__name__)


class HeartbeatService:
    """Background heartbeat management for workflow runs.

    The HeartbeatService is responsible for:
    - Logging periodic heartbeats to the server
    - Tracking the current workflow run status
    - Detecting status changes from heartbeat responses (e.g., pause signals)

    Usage:
        heartbeat = HeartbeatService(
            gateway=gateway,
            interval=30.0,
            report_by_buffer=1.5,
            initial_status="R",
        )

        # Manual tick (for testing or explicit heartbeat)
        update = await heartbeat.tick()
        if update.workflow_run_status:
            state.apply_update(update)

        # Background loop (typical usage)
        stop_event = asyncio.Event()
        task = asyncio.create_task(heartbeat.run_background(stop_event))
        # ... do work ...
        stop_event.set()
        await task
    """

    def __init__(
        self,
        gateway: "ServerGateway",
        interval: float,
        report_by_buffer: float,
        initial_status: str,
    ):
        """Initialize the heartbeat service.

        Args:
            gateway: ServerGateway for server communication.
            interval: Seconds between heartbeats.
            report_by_buffer: Multiplier for next_report_increment sent to server.
                Server expects heartbeat within (interval * report_by_buffer).
            initial_status: Starting workflow run status to report.
        """
        self._gateway = gateway
        self._interval = interval
        self._report_by_buffer = report_by_buffer
        self._current_status = initial_status
        self._last_heartbeat_time: float = 0.0

    @property
    def interval(self) -> float:
        """Heartbeat interval in seconds."""
        return self._interval

    @property
    def current_status(self) -> str:
        """Current status being reported in heartbeats."""
        return self._current_status

    @property
    def last_heartbeat_time(self) -> float:
        """Timestamp of the last successful heartbeat."""
        return self._last_heartbeat_time

    @property
    def next_report_increment(self) -> float:
        """Time window the server expects the next heartbeat within."""
        return self._interval * self._report_by_buffer

    def set_status(self, status: str) -> None:
        """Update the status that will be reported in heartbeats.

        Args:
            status: New workflow run status to report.
        """
        self._current_status = status

    def time_since_last_heartbeat(self) -> float:
        """Seconds since the last heartbeat was logged."""
        if self._last_heartbeat_time == 0.0:
            return float("inf")
        return time.time() - self._last_heartbeat_time

    def is_heartbeat_due(self) -> bool:
        """Check if a heartbeat is due based on the interval."""
        return self.time_since_last_heartbeat() >= self._interval

    def _handle_heartbeat_response(self, response_status: str) -> StateUpdate:
        """Process heartbeat response and return any status change.

        Args:
            response_status: Status returned by the server.

        Returns:
            StateUpdate with workflow_run_status if status changed,
            otherwise an empty StateUpdate.
        """
        self._last_heartbeat_time = time.time()

        # Check if server indicated a status change
        if response_status != self._current_status:
            logger.info(
                "Heartbeat received status change",
                old_status=self._current_status,
                new_status=response_status,
            )
            self._current_status = response_status
            return StateUpdate(workflow_run_status=response_status)

        return StateUpdate.empty()

    async def tick(self) -> StateUpdate:
        """Log a heartbeat and return any status change.

        Returns:
            StateUpdate with workflow_run_status if status changed,
            otherwise an empty StateUpdate.

        Raises:
            Exception: If the heartbeat request fails.
        """
        response = await self._gateway.log_heartbeat(
            status=self._current_status,
            next_report_increment=self.next_report_increment,
        )
        return self._handle_heartbeat_response(response.status)

    def tick_sync(self) -> StateUpdate:
        """Synchronous version of tick for non-async contexts.

        Returns:
            StateUpdate with workflow_run_status if status changed,
            otherwise an empty StateUpdate.

        Raises:
            Exception: If the heartbeat request fails.
        """
        response = self._gateway.log_heartbeat_sync(
            status=self._current_status,
            next_report_increment=self.next_report_increment,
        )
        return self._handle_heartbeat_response(response.status)

    async def run_background(self, stop_event: asyncio.Event) -> None:
        """Background task that logs heartbeats periodically.

        This method runs in a loop, checking if a heartbeat is due and
        logging it if necessary. It continues until the stop_event is set.

        Args:
            stop_event: Event to signal when to stop the loop.

        Note:
            This method catches and logs exceptions but re-raises them
            to allow proper error handling by the caller.
        """
        # Check more frequently than the interval to ensure we don't miss beats
        # Use a minimum of 0.1s to avoid tight loops in testing, but allow
        # very short intervals for production use with short heartbeat intervals
        tick_interval = max(0.1, self._interval / 2)

        logger.debug(
            "Starting heartbeat background loop",
            interval=self._interval,
            tick_interval=tick_interval,
        )

        try:
            while not stop_event.is_set():
                # Use wait with timeout instead of sleep to be responsive to stop_event
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=tick_interval)
                    # If we get here, the event was set - exit the loop
                    break
                except asyncio.TimeoutError:
                    # Timeout expired - check if heartbeat is due
                    pass

                if self.is_heartbeat_due():
                    try:
                        update = await self.tick()
                        if update.workflow_run_status:
                            # Log that we received a status change
                            # The orchestrator should handle applying this
                            logger.debug(
                                "Background heartbeat detected status change",
                                new_status=update.workflow_run_status,
                            )
                    except Exception:
                        logger.exception("Background heartbeat failed")
                        # Don't re-raise - keep trying heartbeats

        except asyncio.CancelledError:
            logger.debug("Heartbeat background loop cancelled")
            raise
        except Exception:
            logger.exception("Heartbeat loop error")
            raise

    def reset_timer(self) -> None:
        """Reset the heartbeat timer to the current time.

        Useful when a heartbeat is logged externally (e.g., during sync).
        """
        self._last_heartbeat_time = time.time()

