"""Swarm package for executing workflow runs.

This package provides the primary API for executing workflows:

    from jobmon.client.swarm import run_workflow, resume_workflow_run, WorkflowRunConfig

    # Execute a workflow
    result = run_workflow(
        workflow=workflow,
        workflow_run_id=wfr_id,
        distributor_alive=distributor.alive,
        config=WorkflowRunConfig(fail_fast=True),
    )

    # Resume a workflow from database
    result = resume_workflow_run(
        workflow_id=123,
        workflow_run_id=456,
        distributor_alive=distributor.alive,
    )

For testing purposes, additional components can be imported directly:

    from jobmon.client.swarm.builder import SwarmBuilder
    from jobmon.client.swarm.state import SwarmState
    from jobmon.client.swarm.task import SwarmTask
    from jobmon.client.swarm.array import SwarmArray
"""

from jobmon.client.swarm.orchestrator import (
    OrchestratorResult,
    WorkflowRunConfig,
)
from jobmon.client.swarm.run import resume_workflow_run, run_workflow

__all__ = [
    "OrchestratorResult",
    "WorkflowRunConfig",
    "resume_workflow_run",
    "run_workflow",
]
