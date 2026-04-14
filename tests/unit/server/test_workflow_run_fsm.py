"""Unit tests for WorkflowRun and Workflow valid_transitions.

Covers the B->R and I->R shortcuts added to unblock the orchestrator
startup race. See workflow_run.py valid_transitions comment for context.
"""

from jobmon.core.constants import WorkflowRunStatus, WorkflowStatus
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun


class TestWorkflowRunTransitions:
    """WorkflowRun state machine shortcuts for orchestrator startup."""

    def test_bound_to_running_is_valid(self) -> None:
        assert (
            WorkflowRunStatus.BOUND,
            WorkflowRunStatus.RUNNING,
        ) in WorkflowRun.valid_transitions

    def test_instantiated_to_running_is_valid(self) -> None:
        assert (
            WorkflowRunStatus.INSTANTIATED,
            WorkflowRunStatus.RUNNING,
        ) in WorkflowRun.valid_transitions

    def test_launched_to_running_still_valid(self) -> None:
        assert (
            WorkflowRunStatus.LAUNCHED,
            WorkflowRunStatus.RUNNING,
        ) in WorkflowRun.valid_transitions

    def test_legacy_chain_still_valid(self) -> None:
        """The old B->I->O->R chain must remain valid for older distributors."""
        for pair in [
            (WorkflowRunStatus.BOUND, WorkflowRunStatus.INSTANTIATED),
            (WorkflowRunStatus.INSTANTIATED, WorkflowRunStatus.LAUNCHED),
            (WorkflowRunStatus.LAUNCHED, WorkflowRunStatus.RUNNING),
        ]:
            assert pair in WorkflowRun.valid_transitions


class TestWorkflowTransitions:
    """Workflow state machine shortcuts that mirror the WFR cascade."""

    def test_queued_to_running_is_valid(self) -> None:
        assert (
            WorkflowStatus.QUEUED,
            WorkflowStatus.RUNNING,
        ) in Workflow.valid_transitions

    def test_instantiating_to_running_is_valid(self) -> None:
        assert (
            WorkflowStatus.INSTANTIATING,
            WorkflowStatus.RUNNING,
        ) in Workflow.valid_transitions

    def test_launched_to_running_still_valid(self) -> None:
        assert (
            WorkflowStatus.LAUNCHED,
            WorkflowStatus.RUNNING,
        ) in Workflow.valid_transitions
