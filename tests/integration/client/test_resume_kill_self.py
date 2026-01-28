"""Tests for KILL_SELF cleanup during workflow resume."""

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus, TaskStatus, WorkflowRunStatus
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.workflow_run import WorkflowRun
from tests.integration.swarm.swarm_test_utils import (
    create_test_context,
    prepare_and_queue_tasks,
)

load_model()


def create_workflow_run_and_instances(workflow, requester, db_engine):
    """Create a workflow run and its task instances.

    Returns:
        Tuple of (workflow_run_id, workflow_id)
    """
    workflow.bind()
    workflow._bind_tasks()

    factory = WorkflowRunFactory(workflow_id=workflow.workflow_id, requester=requester)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    wfr_id = wfr.workflow_run_id

    # Create task instances using swarm utilities
    state, gateway, orchestrator = create_test_context(workflow, wfr_id, requester)
    prepare_and_queue_tasks(state, gateway, orchestrator)

    return wfr_id, workflow.workflow_id


class TestTerminateTaskInstancesStateSplit:
    """Test that terminate_task_instances correctly splits TIs by state."""

    def test_queued_goes_to_error_fatal(
        self, db_engine, tool, task_template, requester_no_retry
    ):
        """QUEUED TIs should go directly to ERROR_FATAL (no worker to clean them up)."""
        # Create a workflow with tasks
        workflow = tool.create_workflow(name="test_queued_error_fatal")
        workflow.add_tasks([task_template.create_task(arg="task1")])

        # Create workflow run and task instances
        wfr_id, workflow_id = create_workflow_run_and_instances(
            workflow, requester_no_retry, db_engine
        )

        # Use direct session to update database (bypasses dbsession transaction)
        with Session(db_engine) as session:
            # Set workflow run to COLD_RESUME state
            session.execute(
                update(WorkflowRun)
                .where(WorkflowRun.id == wfr_id)
                .values(status=WorkflowRunStatus.COLD_RESUME)
            )
            # Set task instance to QUEUED (it should already be QUEUED, but make sure)
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.workflow_run_id == wfr_id)
                .values(status=TaskInstanceStatus.QUEUED)
            )
            session.commit()

        # Call terminate_task_instances
        response = tool.requester.send_request(
            app_route=f"/workflow_run/{wfr_id}/terminate_task_instances",
            message={},
            request_type="put",
        )

        # Verify response
        assert response[0] == 200

        # Check that the TI is now ERROR_FATAL
        with Session(db_engine) as session:
            ti = session.execute(
                select(TaskInstance).where(TaskInstance.workflow_run_id == wfr_id)
            ).scalar_one()
            assert ti.status == TaskInstanceStatus.ERROR_FATAL

    def test_launched_goes_to_kill_self(
        self, db_engine, tool, task_template, requester_no_retry
    ):
        """LAUNCHED TIs should go to KILL_SELF (worker will clean them up)."""
        # Create a workflow with tasks
        workflow = tool.create_workflow(name="test_launched_kill_self")
        workflow.add_tasks([task_template.create_task(arg="task1")])

        # Create workflow run and task instances
        wfr_id, workflow_id = create_workflow_run_and_instances(
            workflow, requester_no_retry, db_engine
        )

        # Use direct session to update database
        with Session(db_engine) as session:
            # Set workflow run to COLD_RESUME state
            session.execute(
                update(WorkflowRun)
                .where(WorkflowRun.id == wfr_id)
                .values(status=WorkflowRunStatus.COLD_RESUME)
            )
            # Set task to LAUNCHED and task instance to LAUNCHED
            session.execute(
                update(Task)
                .where(Task.workflow_id == workflow_id)
                .values(status=TaskStatus.LAUNCHED)
            )
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.workflow_run_id == wfr_id)
                .values(status=TaskInstanceStatus.LAUNCHED)
            )
            session.commit()

        # Call terminate_task_instances
        response = tool.requester.send_request(
            app_route=f"/workflow_run/{wfr_id}/terminate_task_instances",
            message={},
            request_type="put",
        )

        assert response[0] == 200

        # Check that the TI is now KILL_SELF
        with Session(db_engine) as session:
            ti = session.execute(
                select(TaskInstance).where(TaskInstance.workflow_run_id == wfr_id)
            ).scalar_one()
            assert ti.status == TaskInstanceStatus.KILL_SELF


class TestReaperKillSelfCleanup:
    """Test that the reaper waits for KILL_SELF TIs before transitioning WFR."""

    def test_reaper_waits_for_kill_self_cleanup(
        self, db_engine, tool, task_template, requester_no_retry
    ):
        """Reaper should not transition WFR to TERMINATED while KILL_SELF TIs exist."""
        # Create a workflow with tasks
        workflow = tool.create_workflow(name="test_reaper_waits")
        workflow.add_tasks([task_template.create_task(arg="task1")])

        # Create workflow run and task instances
        wfr_id, workflow_id = create_workflow_run_and_instances(
            workflow, requester_no_retry, db_engine
        )

        # Use direct session to update database
        with Session(db_engine) as session:
            from datetime import datetime, timedelta

            # Set workflow run to COLD_RESUME state with expired heartbeat
            # (reaper only processes WFRs where heartbeat_date <= now())
            past_time = datetime.utcnow() - timedelta(minutes=5)
            session.execute(
                update(WorkflowRun)
                .where(WorkflowRun.id == wfr_id)
                .values(
                    status=WorkflowRunStatus.COLD_RESUME,
                    heartbeat_date=past_time,
                )
            )
            # Set task instance to KILL_SELF
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.workflow_run_id == wfr_id)
                .values(status=TaskInstanceStatus.KILL_SELF)
            )
            session.commit()

        # Call reaper
        response = tool.requester.send_request(
            app_route=f"/workflow_run/{wfr_id}/reap",
            message={},
            request_type="put",
        )

        assert response[0] == 200

        # Check that WFR is still in COLD_RESUME (not TERMINATED)
        with Session(db_engine) as session:
            wfr_record = session.execute(
                select(WorkflowRun).where(WorkflowRun.id == wfr_id)
            ).scalar_one()
            assert wfr_record.status == WorkflowRunStatus.COLD_RESUME

        # Response should indicate KILL_SELF TIs remaining
        assert response[1].get("kill_self_remaining", 0) > 0


class TestForceCleanupKillSelf:
    """Test the force_cleanup_kill_self endpoint."""

    def test_force_cleanup_endpoint(
        self, db_engine, tool, task_template, requester_no_retry
    ):
        """Force cleanup should transition KILL_SELF TIs to ERROR_FATAL."""
        # Create a workflow with tasks
        workflow = tool.create_workflow(name="test_force_cleanup")
        workflow.add_tasks([task_template.create_task(arg="task1")])

        # Create workflow run and task instances
        wfr_id, workflow_id = create_workflow_run_and_instances(
            workflow, requester_no_retry, db_engine
        )

        # Use direct session to update database
        with Session(db_engine) as session:
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.workflow_run_id == wfr_id)
                .values(status=TaskInstanceStatus.KILL_SELF)
            )
            session.commit()

        # Call force_cleanup_kill_self
        response = tool.requester.send_request(
            app_route=f"/workflow/{workflow_id}/force_cleanup_kill_self",
            message={},
            request_type="post",
        )

        assert response[0] == 200
        assert response[1].get("num_cleaned_up", 0) >= 1

        # Verify TI is now ERROR_FATAL
        with Session(db_engine) as session:
            ti = session.execute(
                select(TaskInstance).where(TaskInstance.workflow_run_id == wfr_id)
            ).scalar_one()
            assert ti.status == TaskInstanceStatus.ERROR_FATAL

    def test_is_resumable_returns_pending_kill_self(
        self, db_engine, tool, task_template, requester_no_retry
    ):
        """is_resumable endpoint should return pending_kill_self count."""
        from jobmon.server.web.models.workflow import Workflow

        # Create a workflow with tasks
        workflow = tool.create_workflow(name="test_is_resumable_pending")
        workflow.add_tasks([task_template.create_task(arg="task1")])

        # Create workflow run and task instances
        wfr_id, workflow_id = create_workflow_run_and_instances(
            workflow, requester_no_retry, db_engine
        )

        # Use direct session to update database
        with Session(db_engine) as session:
            # Set workflow to resumable state (HALTED)
            session.execute(
                update(Workflow).where(Workflow.id == workflow_id).values(status="H")
            )
            # Set task instance to KILL_SELF
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.workflow_run_id == wfr_id)
                .values(status=TaskInstanceStatus.KILL_SELF)
            )
            session.commit()

        # Call is_resumable
        response = tool.requester.send_request(
            app_route=f"/workflow/{workflow_id}/is_resumable",
            message={},
            request_type="get",
        )

        assert response[0] == 200
        # Should report pending KILL_SELF
        assert response[1].get("pending_kill_self", 0) >= 1
        # Workflow is not resumable while KILL_SELF TIs exist
        assert response[1].get("workflow_is_resumable") is False


class TestValidateTransitionTerminalState:
    """Test that validate_transition allows TIs to go to ERROR_FATAL when Task is terminal."""

    def test_no_heartbeat_to_error_when_task_done(
        self, db_engine, tool, task_template, requester_no_retry
    ):
        """NO_HEARTBEAT TI should be able to go to ERROR_FATAL when Task is DONE."""
        # Create a workflow with tasks
        workflow = tool.create_workflow(name="test_terminal_transition")
        workflow.add_tasks([task_template.create_task(arg="task1")])

        # Create workflow run and task instances
        wfr_id, workflow_id = create_workflow_run_and_instances(
            workflow, requester_no_retry, db_engine
        )

        # Get the task instance ID
        with Session(db_engine) as session:
            ti = session.execute(
                select(TaskInstance).where(TaskInstance.workflow_run_id == wfr_id)
            ).scalar_one()
            ti_id = ti.id
            task_id = ti.task_id

        # Use direct session to update database
        with Session(db_engine) as session:
            # Set Task to DONE (terminal state) - simulating another TI succeeded
            session.execute(
                update(Task).where(Task.id == task_id).values(status=TaskStatus.DONE)
            )
            # Set TI to NO_HEARTBEAT
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.id == ti_id)
                .values(status=TaskInstanceStatus.NO_HEARTBEAT, distributor_id="123")
            )
            session.commit()

        # Try to log an error (this is what the distributor does for NO_HEARTBEAT TIs)
        response = tool.requester.send_request(
            app_route=f"/task_instance/{ti_id}/log_known_error",
            message={
                "error_state": TaskInstanceStatus.ERROR,
                "error_message": "Task instance never reported heartbeat",
                "distributor_id": 123,
            },
            request_type="post",
        )

        assert response[0] == 200

        # Verify TI transitioned to ERROR_FATAL (since Task is terminal)
        with Session(db_engine) as session:
            ti = session.execute(
                select(TaskInstance).where(TaskInstance.id == ti_id)
            ).scalar_one()
            assert ti.status == TaskInstanceStatus.ERROR_FATAL

            # Task should still be DONE (unchanged)
            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.status == TaskStatus.DONE
