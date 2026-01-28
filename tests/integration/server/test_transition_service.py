"""Integration tests for TransitionService.

Tests the unified transition service with audit logging,
TI-centric model, and FSM gating.
"""

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskInstanceStatus, TaskStatus
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.services.task_fsm import TaskFSM
from jobmon.server.web.services.transition_service import TransitionService


@pytest.fixture
def workflow_with_tasks(dbsession: Session):
    """Create a workflow with multiple tasks for testing."""
    from jobmon.server.web.models.cluster import Cluster
    from jobmon.server.web.models.cluster_type import ClusterType
    from jobmon.server.web.models.dag import Dag
    from jobmon.server.web.models.node import Node
    from jobmon.server.web.models.queue import Queue
    from jobmon.server.web.models.task_resources import TaskResources
    from jobmon.server.web.models.task_resources_type import TaskResourcesType
    from jobmon.server.web.models.task_template import TaskTemplate
    from jobmon.server.web.models.task_template_version import TaskTemplateVersion
    from jobmon.server.web.models.tool import Tool
    from jobmon.server.web.models.tool_version import ToolVersion

    # Create cluster type
    cluster_type = ClusterType(name="test_cluster_type_ts")
    dbsession.add(cluster_type)
    dbsession.flush()

    # Create cluster and queue
    cluster = Cluster(name="test_cluster_ts", cluster_type_id=cluster_type.id)
    dbsession.add(cluster)
    dbsession.flush()

    queue = Queue(name="test_queue_ts", cluster_id=cluster.id, parameters="{}")
    dbsession.add(queue)
    dbsession.flush()

    # Create task resources type (use existing constant if available)
    task_resources_type = TaskResourcesType(id="O", label="ORIGINAL")  # O = ORIGINAL
    dbsession.merge(task_resources_type)  # merge in case it exists
    dbsession.flush()

    # Create tool and tool_version
    tool = Tool(name="test_tool_ts")
    dbsession.add(tool)
    dbsession.flush()

    tool_version = ToolVersion(tool_id=tool.id)
    dbsession.add(tool_version)
    dbsession.flush()

    # Create task template and version
    task_template = TaskTemplate(tool_version_id=tool_version.id, name="test_tt_ts")
    dbsession.add(task_template)
    dbsession.flush()

    task_template_version = TaskTemplateVersion(
        task_template_id=task_template.id,
        command_template="echo {arg}",
        arg_mapping_hash="arg_hash_ts",
    )
    dbsession.add(task_template_version)
    dbsession.flush()

    # Create node
    node = Node(
        task_template_version_id=task_template_version.id, node_args_hash="node_hash"
    )
    dbsession.add(node)
    dbsession.flush()

    # Create dag
    dag = Dag(hash="dag_hash_ts")
    dbsession.add(dag)
    dbsession.flush()

    # Create workflow
    workflow = Workflow(
        tool_version_id=tool_version.id,
        dag_id=dag.id,
        name="test_workflow_ts",
        workflow_args_hash="test_hash_ts",
        task_hash="task_hash_ts",
        max_concurrently_running=10,
        status="G",  # REGISTERING
    )
    dbsession.add(workflow)
    dbsession.flush()

    # Create array
    array = Array(
        workflow_id=workflow.id,
        task_template_version_id=task_template_version.id,
        name="test_array_ts",
        max_concurrently_running=10,
    )
    dbsession.add(array)
    dbsession.flush()

    # Create workflow run
    workflow_run = WorkflowRun(
        workflow_id=workflow.id,
        status="R",  # RUNNING
        user="test_user",
    )
    dbsession.add(workflow_run)
    dbsession.flush()

    # Create task resources
    task_resources = TaskResources(
        queue_id=queue.id,
        task_resources_type_id="O",
        requested_resources="{}",
    )
    dbsession.add(task_resources)
    dbsession.flush()

    # Create tasks
    tasks = []
    for i in range(5):
        task = Task(
            workflow_id=workflow.id,
            node_id=node.id,
            array_id=array.id,
            task_args_hash=f"hash_ts_{i}",
            name=f"test_task_ts_{i}",
            command=f"echo task {i}",
            status=TaskStatus.REGISTERING,
            max_attempts=3,
            task_resources_id=task_resources.id,
        )
        dbsession.add(task)
        tasks.append(task)

    dbsession.flush()

    return {
        "workflow": workflow,
        "workflow_run": workflow_run,
        "tasks": tasks,
        "array": array,
        "task_resources": task_resources,
    }


class TestTransitionTasksWithAudit:
    """Test bulk task transitions with audit logging."""

    def test_transition_tasks_creates_audit_records(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Transition should create audit records for each transitioned task."""
        tasks = workflow_with_tasks["tasks"]
        workflow = workflow_with_tasks["workflow"]
        task_ids = [t.id for t in tasks]

        # Transition to QUEUED
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=task_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=True,
        )

        assert len(result["transitioned"]) == 5

        # Verify audit records created
        audit_records = (
            dbsession.execute(
                select(TaskStatusAudit).where(
                    TaskStatusAudit.workflow_id == workflow.id
                )
            )
            .scalars()
            .all()
        )

        assert len(audit_records) == 5
        for audit in audit_records:
            assert audit.previous_status == TaskStatus.REGISTERING
            assert audit.new_status == TaskStatus.QUEUED
            assert audit.workflow_id == workflow.id

    def test_transition_tasks_no_audit_for_invalid(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Invalid transitions should not create audit records."""
        tasks = workflow_with_tasks["tasks"]
        workflow = workflow_with_tasks["workflow"]

        # Try to transition REGISTERING directly to RUNNING (invalid per FSM)
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[tasks[0].id],
            to_status=TaskStatus.RUNNING,
            use_skip_locked=False,
        )

        assert len(result["transitioned"]) == 0
        assert len(result["invalid"]) == 1

        # Verify no audit records created
        audit_records = (
            dbsession.execute(
                select(TaskStatusAudit).where(
                    TaskStatusAudit.workflow_id == workflow.id
                )
            )
            .scalars()
            .all()
        )
        assert len(audit_records) == 0

    def test_fsm_gate_rejects_invalid_transitions(
        self, dbsession: Session, workflow_with_tasks
    ):
        """FSM should reject invalid transitions."""
        tasks = workflow_with_tasks["tasks"]

        # REGISTERING -> DONE is not valid
        result = TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[tasks[0].id],
            to_status=TaskStatus.DONE,
            use_skip_locked=False,
        )

        assert len(result["transitioned"]) == 0
        assert len(result["invalid"]) == 1

    def test_increment_attempts_on_queued(
        self, dbsession: Session, workflow_with_tasks
    ):
        """QUEUED transition should increment num_attempts when specified."""
        tasks = workflow_with_tasks["tasks"]
        task = tasks[0]
        original_attempts = task.num_attempts

        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            use_skip_locked=False,
        )

        dbsession.refresh(task)
        assert task.num_attempts == original_attempts + 1


class TestTransitionTaskInstance:
    """Test TI-centric transitions that cascade to Task."""

    def test_ti_transition_cascades_to_task(
        self, dbsession: Session, workflow_with_tasks
    ):
        """TI transition should cascade to Task and create audit."""
        tasks = workflow_with_tasks["tasks"]
        workflow = workflow_with_tasks["workflow"]
        workflow_run = workflow_with_tasks["workflow_run"]
        array = workflow_with_tasks["array"]
        task_resources = workflow_with_tasks["task_resources"]
        task = tasks[0]

        # First transition task to QUEUED
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
        )

        # Then to INSTANTIATING
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.INSTANTIATING,
        )

        # Then to LAUNCHED
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.LAUNCHED,
        )

        # Create a TaskInstance
        ti = TaskInstance(
            task_id=task.id,
            workflow_run_id=workflow_run.id,
            array_id=array.id,
            task_resources_id=task_resources.id,
            array_batch_num=1,
            array_step_id=0,
            status=TaskInstanceStatus.LAUNCHED,
        )
        dbsession.add(ti)
        dbsession.flush()

        # Now use transition_task_instance for LAUNCHED -> RUNNING
        result = TransitionService.transition_task_instance(
            session=dbsession,
            task_instance_id=ti.id,
            task_id=task.id,
            new_ti_status=TaskInstanceStatus.RUNNING,
            task_num_attempts=task.num_attempts,
            task_max_attempts=task.max_attempts,
        )

        assert result["ti_updated"] is True
        assert result["task_transitioned"] is True
        assert result["task_status"] == TaskStatus.RUNNING

        # Verify TI status
        dbsession.refresh(ti)
        assert ti.status == TaskInstanceStatus.RUNNING

        # Verify Task status
        dbsession.refresh(task)
        assert task.status == TaskStatus.RUNNING

        # Verify audit record for Task (LAUNCHED -> RUNNING)
        audit_records = (
            dbsession.execute(
                select(TaskStatusAudit)
                .where(TaskStatusAudit.task_id == task.id)
                .where(TaskStatusAudit.new_status == TaskStatus.RUNNING)
            )
            .scalars()
            .all()
        )
        assert len(audit_records) == 1
        assert audit_records[0].previous_status == TaskStatus.LAUNCHED

    def test_ti_error_skips_error_recoverable(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Error TI status should skip ERROR_RECOVERABLE and go to final state."""
        tasks = workflow_with_tasks["tasks"]
        workflow_run = workflow_with_tasks["workflow_run"]
        array = workflow_with_tasks["array"]
        task_resources = workflow_with_tasks["task_resources"]
        task = tasks[0]

        # Set up task in RUNNING state
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
        )
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.INSTANTIATING,
        )
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.LAUNCHED,
        )
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.RUNNING,
        )

        # Create a TaskInstance in RUNNING
        ti = TaskInstance(
            task_id=task.id,
            workflow_run_id=workflow_run.id,
            array_id=array.id,
            task_resources_id=task_resources.id,
            array_batch_num=1,
            array_step_id=0,
            status=TaskInstanceStatus.RUNNING,
        )
        dbsession.add(ti)
        dbsession.flush()

        # Refresh task to get current num_attempts
        dbsession.refresh(task)

        # Transition to ERROR (should go to REGISTERING since attempts < max)
        result = TransitionService.transition_task_instance(
            session=dbsession,
            task_instance_id=ti.id,
            task_id=task.id,
            new_ti_status=TaskInstanceStatus.ERROR,
            task_num_attempts=task.num_attempts,  # 1
            task_max_attempts=task.max_attempts,  # 3
        )

        # Should go directly to REGISTERING, skipping ERROR_RECOVERABLE
        assert result["ti_updated"] is True
        assert result["task_transitioned"] is True
        assert result["task_status"] == TaskStatus.REGISTERING

        # Verify task status
        dbsession.refresh(task)
        assert task.status == TaskStatus.REGISTERING

    def test_ti_error_fatal_when_max_attempts(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Error with max attempts should go to ERROR_FATAL."""
        tasks = workflow_with_tasks["tasks"]
        workflow_run = workflow_with_tasks["workflow_run"]
        array = workflow_with_tasks["array"]
        task_resources = workflow_with_tasks["task_resources"]
        task = tasks[0]

        # Set up task with max attempts reached
        task.num_attempts = 3  # Equal to max_attempts
        dbsession.flush()

        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.QUEUED,
            increment_attempts=False,  # Don't increment, already at max
        )
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.INSTANTIATING,
        )
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.LAUNCHED,
        )
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[task.id],
            to_status=TaskStatus.RUNNING,
        )

        # Create a TaskInstance in RUNNING
        ti = TaskInstance(
            task_id=task.id,
            workflow_run_id=workflow_run.id,
            array_id=array.id,
            task_resources_id=task_resources.id,
            array_batch_num=1,
            array_step_id=0,
            status=TaskInstanceStatus.RUNNING,
        )
        dbsession.add(ti)
        dbsession.flush()

        # Transition to ERROR (should go to ERROR_FATAL since at max attempts)
        result = TransitionService.transition_task_instance(
            session=dbsession,
            task_instance_id=ti.id,
            task_id=task.id,
            new_ti_status=TaskInstanceStatus.ERROR,
            task_num_attempts=3,  # At max
            task_max_attempts=3,
        )

        assert result["task_status"] == TaskStatus.ERROR_FATAL

        dbsession.refresh(task)
        assert task.status == TaskStatus.ERROR_FATAL


class TestGateTasksForQueueing:
    """Test the gate_tasks_for_queueing method."""

    def test_gate_tasks_returns_gated_ids(
        self, dbsession: Session, workflow_with_tasks
    ):
        """gate_tasks_for_queueing should return gated task IDs."""
        tasks = workflow_with_tasks["tasks"]
        task_ids = [t.id for t in tasks]

        result = TransitionService.gate_tasks_for_queueing(
            session=dbsession,
            task_ids=task_ids,
        )

        assert len(result["gated"]) == 5
        assert len(result["invalid"]) == 0
        assert len(result["locked"]) == 0

        # Verify all tasks are now QUEUED
        for task in tasks:
            dbsession.refresh(task)
            assert task.status == TaskStatus.QUEUED

    def test_gate_tasks_filters_invalid(self, dbsession: Session, workflow_with_tasks):
        """gate_tasks_for_queueing should filter out tasks in invalid states."""
        tasks = workflow_with_tasks["tasks"]

        # Transition some tasks past REGISTERING
        TransitionService.transition_tasks(
            session=dbsession,
            task_ids=[tasks[0].id, tasks[1].id],
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
        )

        # Now gate all tasks
        all_ids = [t.id for t in tasks]
        result = TransitionService.gate_tasks_for_queueing(
            session=dbsession,
            task_ids=all_ids,
        )

        # First 2 should be invalid, last 3 should be gated
        assert len(result["gated"]) == 3
        assert len(result["invalid"]) == 2


class TestTaskFSMMapping:
    """Test TaskFSM TI to Task status mapping."""

    def test_get_task_status_for_ti_running(self):
        """RUNNING TI should map to RUNNING Task."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.RUNNING,
            task_num_attempts=1,
            task_max_attempts=3,
        )
        assert status == TaskStatus.RUNNING

    def test_get_task_status_for_ti_done(self):
        """DONE TI should map to DONE Task."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.DONE,
            task_num_attempts=1,
            task_max_attempts=3,
        )
        assert status == TaskStatus.DONE

    def test_get_task_status_for_ti_error_can_retry(self):
        """ERROR TI with retries left should map to REGISTERING."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.ERROR,
            task_num_attempts=1,
            task_max_attempts=3,
        )
        assert status == TaskStatus.REGISTERING

    def test_get_task_status_for_ti_error_no_retries(self):
        """ERROR TI with no retries should map to ERROR_FATAL."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.ERROR,
            task_num_attempts=3,
            task_max_attempts=3,
        )
        assert status == TaskStatus.ERROR_FATAL

    def test_get_task_status_for_ti_resource_error(self):
        """RESOURCE_ERROR TI should map to ADJUSTING_RESOURCES."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.RESOURCE_ERROR,
            task_num_attempts=1,
            task_max_attempts=3,
        )
        assert status == TaskStatus.ADJUSTING_RESOURCES

    def test_get_task_status_for_ti_triaging_returns_none(self):
        """TRIAGING TI should return None (no Task transition)."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.TRIAGING,
            task_num_attempts=1,
            task_max_attempts=3,
        )
        assert status is None

    def test_get_task_status_for_ti_kill_self_returns_none(self):
        """KILL_SELF TI should return None (no Task transition)."""
        status = TaskFSM.get_task_status_for_ti(
            ti_status=TaskInstanceStatus.KILL_SELF,
            task_num_attempts=1,
            task_max_attempts=3,
        )
        assert status is None


class TestFSMValidTransitions:
    """Test FSM valid transitions."""

    def test_launched_to_error_fatal_for_kill(self):
        """LAUNCHED -> ERROR_FATAL should be valid for kill operations."""
        assert TaskFSM.is_valid_transition(TaskStatus.LAUNCHED, TaskStatus.ERROR_FATAL)

    def test_running_to_error_fatal(self):
        """RUNNING -> ERROR_FATAL should be valid."""
        assert TaskFSM.is_valid_transition(TaskStatus.RUNNING, TaskStatus.ERROR_FATAL)

    def test_running_to_registering_for_retry(self):
        """RUNNING -> REGISTERING should be valid for retry."""
        assert TaskFSM.is_valid_transition(TaskStatus.RUNNING, TaskStatus.REGISTERING)

    def test_running_to_adjusting_resources(self):
        """RUNNING -> ADJUSTING_RESOURCES should be valid."""
        assert TaskFSM.is_valid_transition(
            TaskStatus.RUNNING, TaskStatus.ADJUSTING_RESOURCES
        )

    def test_terminal_states_have_no_transitions(self):
        """DONE and ERROR_FATAL should be terminal."""
        assert TaskFSM.is_terminal(TaskStatus.DONE)
        assert TaskFSM.is_terminal(TaskStatus.ERROR_FATAL)
