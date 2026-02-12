"""Integration tests for SKIP LOCKED functionality.

Verifies that SKIP LOCKED works correctly with SQLite (for tests)
and documents expected behavior for MySQL 8.0+.
"""

import pytest
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskStatus
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.services.transition_service import TransitionService


@pytest.fixture
def workflow_with_tasks(dbsession: Session):
    """Create a workflow with multiple tasks for testing."""
    from jobmon.server.web.models.dag import Dag
    from jobmon.server.web.models.node import Node
    from jobmon.server.web.models.task_template import TaskTemplate
    from jobmon.server.web.models.task_template_version import TaskTemplateVersion
    from jobmon.server.web.models.tool import Tool
    from jobmon.server.web.models.tool_version import ToolVersion

    # Create tool and tool_version
    tool = Tool(name="test_tool_skip_locked")
    dbsession.add(tool)
    dbsession.flush()

    tool_version = ToolVersion(tool_id=tool.id)
    dbsession.add(tool_version)
    dbsession.flush()

    # Create task template and version
    task_template = TaskTemplate(tool_version_id=tool_version.id, name="test_tt_sl")
    dbsession.add(task_template)
    dbsession.flush()

    task_template_version = TaskTemplateVersion(
        task_template_id=task_template.id,
        command_template="echo {arg}",
        arg_mapping_hash="arg_hash_sl",
    )
    dbsession.add(task_template_version)
    dbsession.flush()

    # Create node
    node = Node(
        task_template_version_id=task_template_version.id, node_args_hash="node_hash_sl"
    )
    dbsession.add(node)
    dbsession.flush()

    # Create dag
    dag = Dag(hash="dag_hash_sl")
    dbsession.add(dag)
    dbsession.flush()

    # Create workflow
    workflow = Workflow(
        tool_version_id=tool_version.id,
        dag_id=dag.id,
        name="test_workflow_skip_locked",
        workflow_args_hash="test_hash_skip_locked",
        task_hash="task_hash_sl",
        max_concurrently_running=10,
        status="G",  # REGISTERING
    )
    dbsession.add(workflow)
    dbsession.flush()

    # Create tasks
    tasks = []
    for i in range(5):
        task = Task(
            workflow_id=workflow.id,
            node_id=node.id,
            task_args_hash=f"hash_sl_{i}",
            name=f"test_task_{i}",
            command=f"echo task {i}",
            status=TaskStatus.REGISTERING,
            max_attempts=3,
        )
        dbsession.add(task)
        tasks.append(task)

    dbsession.flush()

    return {"workflow": workflow, "tasks": tasks}


class TestSkipLockedBehavior:
    """Test SKIP LOCKED behavior with the transition service."""

    def test_transition_tasks_with_skip_locked_all_available(
        self, dbsession: Session, workflow_with_tasks
    ):
        """When no rows are locked, all eligible tasks should transition."""
        tasks = workflow_with_tasks["tasks"]
        task_ids = [t.id for t in tasks]

        # All tasks are in REGISTERING, should transition to QUEUED
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=task_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=True,
        )

        assert len(result["transitioned"]) == 5
        assert len(result["invalid"]) == 0
        assert len(result["locked"]) == 0
        assert len(result["not_found"]) == 0

        # Verify tasks are updated
        for task in tasks:
            dbsession.refresh(task)
            assert task.status == TaskStatus.QUEUED
            assert task.num_attempts == 1

    def test_transition_tasks_with_skip_locked_invalid_state(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Tasks not in valid source state should be in invalid list."""
        tasks = workflow_with_tasks["tasks"]

        # Transition first 2 tasks to QUEUED
        first_two_ids = [tasks[0].id, tasks[1].id]
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=first_two_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=True,
        )

        # Now try to transition all 5 to QUEUED again
        all_ids = [t.id for t in tasks]
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=all_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=True,
        )

        # First 2 should be invalid (already QUEUED), last 3 should transition
        assert len(result["transitioned"]) == 3
        assert len(result["invalid"]) == 2
        assert set(result["invalid"]) == {tasks[0].id, tasks[1].id}

    def test_transition_tasks_with_skip_locked_not_found(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Non-existent task IDs should be in not_found list."""
        tasks = workflow_with_tasks["tasks"]
        task_ids = [t.id for t in tasks] + [99999, 99998]  # Add non-existent IDs

        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=task_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=True,
        )

        assert len(result["transitioned"]) == 5
        assert len(result["not_found"]) == 2
        assert set(result["not_found"]) == {99999, 99998}

    def test_gate_tasks_for_queueing(self, dbsession: Session, workflow_with_tasks):
        """Test the gate_tasks_for_queueing convenience method."""
        tasks = workflow_with_tasks["tasks"]
        task_ids = [t.id for t in tasks]

        result = TransitionService.gate_tasks_for_queueing(
            session=dbsession,
            task_ids=task_ids,
        )

        # Result uses "gated" instead of "transitioned"
        assert len(result["gated"]) == 5
        assert len(result["invalid"]) == 0
        assert len(result["locked"]) == 0


class TestSkipLockedConcurrency:
    """Tests that document expected SKIP LOCKED behavior under concurrency.

    Note: SQLite has limited concurrency support and may behave differently
    than MySQL. These tests document the expected behavior for MySQL 8.0+.
    """

    def test_skip_locked_documentation(self, dbsession: Session):
        """Document expected SKIP LOCKED behavior.

        In MySQL 8.0+:
        - Session 1 locks rows with FOR UPDATE
        - Session 2 with SKIP LOCKED will skip those rows
        - Session 2 can still process unlocked rows

        This is the behavior we rely on for concurrent task transitions.
        SQLite doesn't support true concurrent transactions in the same way,
        so this test documents the expected behavior for MySQL.
        """
        # This is a documentation test - actual concurrency testing
        # requires MySQL and multiple connections

    def test_nowait_vs_skip_locked_choice(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Document when to use NOWAIT vs SKIP LOCKED.

        NOWAIT (use_skip_locked=False):
        - Used for single-task operations (worker transitions)
        - Fails immediately if row is locked
        - Service has internal retry with exponential backoff
        - Better for operations that must succeed or fail atomically

        SKIP LOCKED (use_skip_locked=True):
        - Used for bulk operations (queue_task_batch, etc.)
        - Processes available rows, skips locked ones
        - Locked rows returned in result["locked"] for caller to handle
        - Better for operations that can partially succeed
        """
        tasks = workflow_with_tasks["tasks"]

        # Single task with NOWAIT
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[tasks[0].id],
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=False,  # NOWAIT for single task
        )
        assert len(result["transitioned"]) == 1

        # Bulk with SKIP LOCKED
        remaining_ids = [t.id for t in tasks[1:]]
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=remaining_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=True,  # SKIP LOCKED for bulk
        )
        assert len(result["transitioned"]) == 4
