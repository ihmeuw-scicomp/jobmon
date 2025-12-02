"""Factory functions for running workflow runs.

This module provides the primary entry points for executing workflows:
- `run_workflow()`: Execute a workflow from an in-memory Workflow object
- `resume_workflow_run()`: Resume a workflow from database state

These are the main public API for the swarm package.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Callable, Optional

import aiohttp
import structlog

from jobmon.client.swarm.gateway import ServerGateway
from jobmon.client.swarm.orchestrator import (
    OrchestratorConfig,
    OrchestratorResult,
    WorkflowRunConfig,
    WorkflowRunOrchestrator,
)
from jobmon.client.swarm.builder import SwarmBuilder
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core.exceptions import TransitionError
from jobmon.core.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

logger = structlog.get_logger(__name__)


def run_workflow(
    workflow: "Workflow",
    workflow_run_id: int,
    distributor_alive: Callable[..., bool],
    status: str = WorkflowRunStatus.BOUND,
    config: Optional[WorkflowRunConfig] = None,
    timeout: int = 36000,
    requester: Optional[Requester] = None,
) -> OrchestratorResult:
    """Execute a workflow run.

    This is the primary entry point for running workflows. It:
    1. Builds the swarm state from the workflow
    2. Runs the orchestrator to completion
    3. Returns complete execution results

    Args:
        workflow: The Workflow object to execute.
        workflow_run_id: The workflow run ID (from WorkflowRunFactory).
        distributor_alive: Callable that returns True while distributor is alive.
        status: Initial workflow run status (default: BOUND).
        config: Optional WorkflowRunConfig. Uses defaults if not provided.
        timeout: Maximum execution time in seconds.
        requester: Optional requester for server communication.

    Returns:
        OrchestratorResult with complete execution results.

    Raises:
        RuntimeError: If timeout exceeded.
        DistributorNotAlive: If distributor dies during execution.
    """
    return asyncio.run(
        _run_workflow_async(
            workflow=workflow,
            workflow_run_id=workflow_run_id,
            distributor_alive=distributor_alive,
            status=status,
            config=config,
            timeout=timeout,
            requester=requester,
        )
    )


def resume_workflow_run(
    workflow_id: int,
    workflow_run_id: int,
    distributor_alive: Callable[..., bool],
    status: str = WorkflowRunStatus.BOUND,
    config: Optional[WorkflowRunConfig] = None,
    timeout: int = 36000,
    requester: Optional[Requester] = None,
) -> OrchestratorResult:
    """Resume a workflow run from database state.

    Used for resume scenarios where we need to reconstruct the workflow
    state from the database and continue execution.

    Args:
        workflow_id: The workflow to resume.
        workflow_run_id: The new workflow run ID.
        distributor_alive: Callable that returns True while distributor is alive.
        status: Initial workflow run status (default: BOUND).
        config: Optional WorkflowRunConfig. Uses defaults if not provided.
        timeout: Maximum execution time in seconds.
        requester: Optional requester for server communication.

    Returns:
        OrchestratorResult with complete execution results.
    """
    return asyncio.run(
        _resume_workflow_run_async(
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            distributor_alive=distributor_alive,
            status=status,
            config=config,
            timeout=timeout,
            requester=requester,
        )
    )


async def _run_workflow_async(
    workflow: "Workflow",
    workflow_run_id: int,
    distributor_alive: Callable[..., bool],
    status: str,
    config: Optional[WorkflowRunConfig],
    timeout: int,
    requester: Optional[Requester],
) -> OrchestratorResult:
    """Async implementation of run_workflow."""
    # Resolve defaults
    config = config or WorkflowRunConfig()
    requester = requester or Requester.from_defaults()
    jobmon_config = JobmonConfig()

    # Resolve heartbeat settings from config or JobmonConfig
    heartbeat_interval = config.heartbeat_interval
    if heartbeat_interval is None:
        heartbeat_interval = jobmon_config.get_int("heartbeat", "workflow_run_interval")

    heartbeat_report_by_buffer = config.heartbeat_report_by_buffer
    if heartbeat_report_by_buffer is None:
        heartbeat_report_by_buffer = jobmon_config.get_float(
            "heartbeat", "report_by_buffer"
        )

    # Build state from workflow
    builder = SwarmBuilder(
        requester=requester,
        workflow_run_id=workflow_run_id,
        heartbeat_interval=heartbeat_interval,
        heartbeat_report_by_buffer=heartbeat_report_by_buffer,
        initial_status=status,
    )
    builder.build_from_workflow(workflow)

    # Run with orchestrator (use _ensure_gateway() to create gateway if needed)
    return await _run_orchestrator(
        state=builder.state,
        gateway=builder._ensure_gateway(),
        distributor_alive=distributor_alive,
        config=config,
        timeout=timeout,
        heartbeat_interval=heartbeat_interval,
        heartbeat_report_by_buffer=heartbeat_report_by_buffer,
    )


async def _resume_workflow_run_async(
    workflow_id: int,
    workflow_run_id: int,
    distributor_alive: Callable[..., bool],
    status: str,
    config: Optional[WorkflowRunConfig],
    timeout: int,
    requester: Optional[Requester],
) -> OrchestratorResult:
    """Async implementation of resume_workflow_run."""
    # Resolve defaults
    config = config or WorkflowRunConfig()
    requester = requester or Requester.from_defaults()
    jobmon_config = JobmonConfig()

    # Resolve heartbeat settings from config or JobmonConfig
    heartbeat_interval = config.heartbeat_interval
    if heartbeat_interval is None:
        heartbeat_interval = jobmon_config.get_int("heartbeat", "workflow_run_interval")

    heartbeat_report_by_buffer = config.heartbeat_report_by_buffer
    if heartbeat_report_by_buffer is None:
        heartbeat_report_by_buffer = jobmon_config.get_float(
            "heartbeat", "report_by_buffer"
        )

    # Build state from database
    builder = SwarmBuilder(
        requester=requester,
        workflow_run_id=workflow_run_id,
        heartbeat_interval=heartbeat_interval,
        heartbeat_report_by_buffer=heartbeat_report_by_buffer,
        initial_status=status,
    )
    builder.build_from_workflow_id(workflow_id)

    # Run with orchestrator (use _ensure_gateway() to get gateway)
    return await _run_orchestrator(
        state=builder.state,
        gateway=builder._ensure_gateway(),
        distributor_alive=distributor_alive,
        config=config,
        timeout=timeout,
        heartbeat_interval=heartbeat_interval,
        heartbeat_report_by_buffer=heartbeat_report_by_buffer,
    )


async def _run_orchestrator(
    state,
    gateway: ServerGateway,
    distributor_alive: Callable[..., bool],
    config: WorkflowRunConfig,
    timeout: int,
    heartbeat_interval: int,
    heartbeat_report_by_buffer: float,
    start_time: float | None = None,
) -> OrchestratorResult:
    """Run the orchestrator and handle cleanup.

    This is the common execution path for both new runs and resumes.
    """
    import time

    from jobmon.core.constants import TaskStatus

    if start_time is None:
        start_time = time.perf_counter()

    # Create HTTP session
    session = aiohttp.ClientSession()
    gateway.set_session(session)

    try:
        # Create orchestrator config
        orch_config = OrchestratorConfig(
            heartbeat_interval=heartbeat_interval,
            heartbeat_report_by_buffer=heartbeat_report_by_buffer,
            wedged_workflow_sync_interval=config.wedged_workflow_sync_interval,
            fail_fast=config.fail_fast,
            timeout=timeout,
            fail_after_n_executions=(
                config.fail_after_n_executions
                if config.fail_after_n_executions < 1_000_000_000
                else None
            ),
        )

        # Create and run orchestrator
        orchestrator = WorkflowRunOrchestrator(state, gateway, orch_config)

        try:
            result = await orchestrator.run(distributor_alive)

            logger.info(
                f"Workflow run completed: {result.done_count}/{result.total_tasks} done, "
                f"{result.failed_count} failed, status={result.final_status}, "
                f"elapsed={result.elapsed_time:.1f}s"
            )

            return result

        except KeyboardInterrupt:
            # Handle keyboard interrupt - prompt user for confirmation
            logger.warning("Keyboard interrupt raised")
            confirm = await asyncio.to_thread(
                input, "Are you sure you want to exit (y/n): "
            )
            confirm = confirm.lower().strip()

            if confirm == "y":
                try:
                    await gateway.update_status(WorkflowRunStatus.STOPPED)
                except TransitionError:
                    logger.debug("Status already stopped or cannot transition")
                raise
            else:
                logger.info("Continuing jobmon...")
                # Re-run orchestrator (recursive call, preserve start_time)
                return await _run_orchestrator(
                    state=state,
                    gateway=gateway,
                    distributor_alive=distributor_alive,
                    config=config,
                    timeout=timeout,
                    heartbeat_interval=heartbeat_interval,
                    heartbeat_report_by_buffer=heartbeat_report_by_buffer,
                    start_time=start_time,
                )

        except RuntimeError as e:
            # Check if this is a timeout error (should re-raise) vs fail-fast (return result)
            if "timeout" in str(e).lower():
                raise

            # For fail-fast and other RuntimeErrors, construct result from state
            logger.warning(f"Workflow run RuntimeError: {e}")
            elapsed = time.perf_counter() - start_time

            # Gather task final statuses from state
            task_final_statuses = {
                task.task_id: task.status for task in state.tasks.values()
            }
            done_task_ids = frozenset(
                t.task_id for t in state.get_tasks_by_status(TaskStatus.DONE)
            )
            failed_task_ids = frozenset(
                t.task_id for t in state.get_tasks_by_status(TaskStatus.ERROR_FATAL)
            )

            return OrchestratorResult(
                final_status=state.status,
                elapsed_time=elapsed,
                total_tasks=len(state.tasks),
                done_count=len(done_task_ids),
                failed_count=len(failed_task_ids),
                num_previously_complete=state.num_previously_complete,
                task_final_statuses=task_final_statuses,
                done_task_ids=done_task_ids,
                failed_task_ids=failed_task_ids,
            )

        except Exception as e:
            # On other errors (fail-fast, etc.), construct result from state
            logger.warning(f"Workflow run error: {e}")
            elapsed = time.perf_counter() - start_time

            # Gather task final statuses from state
            task_final_statuses = {
                task.task_id: task.status for task in state.tasks.values()
            }
            done_task_ids = frozenset(
                t.task_id for t in state.get_tasks_by_status(TaskStatus.DONE)
            )
            failed_task_ids = frozenset(
                t.task_id for t in state.get_tasks_by_status(TaskStatus.ERROR_FATAL)
            )

            return OrchestratorResult(
                final_status=state.status,
                elapsed_time=elapsed,
                total_tasks=len(state.tasks),
                done_count=len(done_task_ids),
                failed_count=len(failed_task_ids),
                num_previously_complete=state.num_previously_complete,
                task_final_statuses=task_final_statuses,
                done_task_ids=done_task_ids,
                failed_task_ids=failed_task_ids,
            )

    finally:
        # Cleanup
        if not session.closed:
            await session.close()

