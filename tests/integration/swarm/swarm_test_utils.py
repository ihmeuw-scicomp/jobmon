"""Test utilities for swarm tests.

This module provides helper functions that encapsulate common swarm operations
using the SwarmBuilder and related components directly.

These utilities allow tests to prepare and queue tasks without going through
the full run_workflow() flow, which is useful for unit and integration tests
that need fine-grained control over the execution steps.
"""

import asyncio
from typing import TYPE_CHECKING, Optional, Tuple

from jobmon.client.swarm.builder import SwarmBuilder
from jobmon.client.swarm.gateway import ServerGateway
from jobmon.client.swarm.orchestrator import OrchestratorConfig, WorkflowRunOrchestrator
from jobmon.client.swarm.state import SwarmState
from jobmon.core.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.swarm.task import SwarmTask
    from jobmon.client.workflow import Workflow


def create_test_context(
    workflow: "Workflow",
    workflow_run_id: int,
    requester: Optional[Requester] = None,
    initial_status: str = "B",
) -> Tuple[SwarmState, ServerGateway, WorkflowRunOrchestrator]:
    """Build state, gateway, and orchestrator for testing.

    This is the primary entry point for tests that need fine-grained control
    over swarm execution.

    Args:
        workflow: The Workflow object to build state from.
        workflow_run_id: The workflow run ID.
        requester: Optional requester for server communication.
        initial_status: Initial workflow run status (default: BOUND).

    Returns:
        Tuple of (SwarmState, ServerGateway, WorkflowRunOrchestrator).
    """
    requester = requester or Requester.from_defaults()

    builder = SwarmBuilder(
        requester=requester,
        workflow_run_id=workflow_run_id,
        initial_status=initial_status,
    )
    builder.build_from_workflow(workflow)

    config = OrchestratorConfig()
    orchestrator = WorkflowRunOrchestrator(
        builder.state, builder._ensure_gateway(), config
    )

    return builder.state, builder._ensure_gateway(), orchestrator


def create_builder(
    workflow: "Workflow",
    workflow_run_id: int,
    requester: Optional[Requester] = None,
    initial_status: str = "B",
) -> SwarmBuilder:
    """Create a SwarmBuilder and build state from a workflow.

    Args:
        workflow: The Workflow object to build state from.
        workflow_run_id: The workflow run ID.
        requester: Optional requester for server communication.
        initial_status: Initial workflow run status (default: BOUND).

    Returns:
        SwarmBuilder with state built from the workflow.
    """
    requester = requester or Requester.from_defaults()

    builder = SwarmBuilder(
        requester=requester,
        workflow_run_id=workflow_run_id,
        initial_status=initial_status,
    )
    builder.build_from_workflow(workflow)
    return builder


def set_initial_fringe(
    state: SwarmState, orchestrator: WorkflowRunOrchestrator
) -> None:
    """Populate ready_to_run with tasks whose upstreams are satisfied.

    Uses the Orchestrator's _set_initial_fringe() method.

    Args:
        state: The SwarmState to operate on.
        orchestrator: The orchestrator to use for setting resources.
    """
    orchestrator._set_initial_fringe()


async def queue_tasks_async(
    state: SwarmState,
    gateway: ServerGateway,
    orchestrator: WorkflowRunOrchestrator,
    timeout: float = -1,
) -> None:
    """Queue ready tasks to the server.

    Uses the Orchestrator's _do_scheduling() method which internally uses
    the Scheduler service and applies status updates.

    Args:
        state: The SwarmState containing ready tasks.
        gateway: The ServerGateway for server communication.
        orchestrator: The orchestrator to use for scheduling.
        timeout: Maximum time to spend processing (-1 for unlimited).
    """
    import aiohttp

    # Set up gateway session
    session = aiohttp.ClientSession()
    gateway.set_session(session)

    try:
        await orchestrator._do_scheduling(timeout=timeout if timeout > 0 else -1)
    finally:
        if not session.closed:
            await session.close()
        # Clear session reference to avoid "Unclosed client session" warnings
        gateway._session = None


def queue_tasks(
    state: SwarmState,
    gateway: ServerGateway,
    orchestrator: WorkflowRunOrchestrator,
    timeout: float = -1,
) -> None:
    """Queue ready tasks to the server (synchronous wrapper).

    Args:
        state: The SwarmState containing ready tasks.
        gateway: The ServerGateway for server communication.
        orchestrator: The orchestrator to use for scheduling.
        timeout: Maximum time to spend processing (-1 for unlimited).
    """
    asyncio.run(queue_tasks_async(state, gateway, orchestrator, timeout))


def prepare_and_queue_tasks(
    state: SwarmState,
    gateway: ServerGateway,
    orchestrator: WorkflowRunOrchestrator,
    timeout: float = -1,
) -> None:
    """Prepare initial fringe and queue tasks in one operation.

    This is a convenience function that combines set_initial_fringe() and
    queue_tasks() for common test patterns.

    Args:
        state: The SwarmState to operate on.
        gateway: The ServerGateway for server communication.
        orchestrator: The orchestrator to use.
        timeout: Maximum time to spend processing (-1 for unlimited).
    """
    set_initial_fringe(state, orchestrator)
    queue_tasks(state, gateway, orchestrator, timeout)


async def synchronize_state_async(
    state: SwarmState,
    gateway: ServerGateway,
    orchestrator: WorkflowRunOrchestrator,
    full_sync: bool = False,
) -> None:
    """Synchronize local state with the server.

    Uses the Orchestrator's _do_sync() method which internally uses
    the Synchronizer service. Also performs a heartbeat tick to get
    the current workflow run status from the server.

    Args:
        state: The SwarmState to synchronize.
        gateway: The ServerGateway for server communication.
        orchestrator: The orchestrator to use.
        full_sync: If True, fetch all task statuses; otherwise incremental.
    """
    import aiohttp

    # Set up gateway session
    session = aiohttp.ClientSession()
    gateway.set_session(session)

    try:
        # Do a heartbeat tick to get the current workflow run status
        heartbeat = orchestrator._ensure_heartbeat()
        heartbeat_update = await heartbeat.tick()
        if heartbeat_update.workflow_run_status:
            state.apply_update(heartbeat_update)

        # Then do the regular sync
        await orchestrator._do_sync(full_sync=full_sync)
    finally:
        if not session.closed:
            await session.close()
        # Clear session reference to avoid "Unclosed client session" warnings
        gateway._session = None


def synchronize_state(
    state: SwarmState,
    gateway: ServerGateway,
    orchestrator: WorkflowRunOrchestrator,
    full_sync: bool = False,
) -> None:
    """Synchronize local state with the server (synchronous wrapper).

    Args:
        state: The SwarmState to synchronize.
        gateway: The ServerGateway for server communication.
        orchestrator: The orchestrator to use.
        full_sync: If True, fetch all task statuses; otherwise incremental.
    """
    asyncio.run(synchronize_state_async(state, gateway, orchestrator, full_sync))


def set_validated_task_resources(
    orchestrator: WorkflowRunOrchestrator,
    task: "SwarmTask",
) -> None:
    """Validate and set task resources, using cache if available.

    Uses the Orchestrator's _set_validated_task_resources() method.

    Args:
        orchestrator: The orchestrator (for resource cache access).
        task: The task to validate resources for.
    """
    orchestrator._set_validated_task_resources(task)


def set_adjusted_task_resources(
    orchestrator: WorkflowRunOrchestrator,
    task: "SwarmTask",
) -> None:
    """Adjust the swarm task's resources after a failure.

    Uses the Orchestrator's _set_adjusted_task_resources() method.

    Args:
        orchestrator: The orchestrator (for resource cache access).
        task: The task to adjust resources for.
    """
    orchestrator._set_adjusted_task_resources(task)


# ──────────────────────────────────────────────────────────────────────────────
# Convenience aliases for simpler test patterns
# ──────────────────────────────────────────────────────────────────────────────


def build_and_prepare(
    workflow: "Workflow",
    workflow_run_id: int,
    requester: Optional[Requester] = None,
) -> Tuple[SwarmState, ServerGateway, WorkflowRunOrchestrator]:
    """Build state and prepare initial fringe in one call.

    Args:
        workflow: The Workflow object.
        workflow_run_id: The workflow run ID.
        requester: Optional requester.

    Returns:
        Tuple of (state, gateway, orchestrator) with initial fringe set.
    """
    state, gateway, orchestrator = create_test_context(
        workflow, workflow_run_id, requester
    )
    set_initial_fringe(state, orchestrator)
    return state, gateway, orchestrator
