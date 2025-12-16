import pytest

from jobmon.client.api import Tool
from jobmon.client.swarm import WorkflowRunConfig
from jobmon.client.swarm.builder import SwarmBuilder
from jobmon.client.workflow_run import WorkflowRun, WorkflowRunFactory
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core.exceptions import WorkflowNotResumable


@pytest.fixture
def tool(client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


def test_workflow_run_bind(tool, task_template, requester_no_retry):
    """Test binding logic"""
    wf = tool.create_workflow()
    t1 = task_template.create_task(arg="sleep 1")
    wf.add_tasks([t1])
    wf.bind()
    assert wf.workflow_id is not None

    # Tasks not bound yet, therefore workflow has not logged created_date.
    # Ensure workflowrun bind fails with not resumable
    factory = WorkflowRunFactory(
        workflow_id=wf.workflow_id, requester=requester_no_retry
    )
    with pytest.raises(WorkflowNotResumable) as error:
        factory.create_workflow_run()
        assert "has not completed binding tasks" in str(error.value)

    # bind tasks, try again
    wf._bind_tasks()
    assert t1.task_id is not None
    wfr = factory.create_workflow_run()
    assert wfr.status == WorkflowRunStatus.LINKING

    # Assert no workflow run created if workflow id doesn't exist
    wfr2 = WorkflowRun(workflow_id=-1, requester=requester_no_retry)
    with pytest.raises(WorkflowNotResumable) as error:
        wfr2.bind()
        assert "No workflow exists" in str(error.value)

    # WFR 1 is linking still. A resume should fail
    with pytest.raises(WorkflowNotResumable) as error:
        factory.create_workflow_run()
        assert "not in a resume-able state" in str(error.value)

    # If we signal for a resume first, then wfr3 should be able to bind
    # Set to bound state so it's detected as an active wfr, can be terminated by resume
    wfr._update_status(WorkflowRunStatus.BOUND)
    with pytest.raises(WorkflowNotResumable):
        # Resume signal set, but workflowrun can't be moved out of Cold Resume since
        # there isn't an active swarm. Terminate it ourselves
        factory.set_workflow_resume(resume_timeout=0)
    # WF in Q state
    wfr._update_status(WorkflowRunStatus.TERMINATED)

    wfr3 = factory.create_workflow_run()
    assert wfr3.status == WorkflowRunStatus.LINKING


def test_task_resources_conversion(tool, task_template):
    too_many_cores = {"memory": "20G", "queue": "null.q", "runtime": "01:02:33"}
    t1 = task_template.create_task(
        arg="echo 1", compute_resources=too_many_cores, cluster_name="multiprocess"
    )
    wf1 = tool.create_workflow()
    wf1.add_task(t1)

    # Check the workflow can still bind
    wf1.bind()
    assert wf1.workflow_id is not None
    wf1._bind_tasks()
    assert t1.task_id is not None
    task_resources = list(wf1._task_resources.values())[0]
    assert task_resources.requested_resources["memory"] == 20
    assert task_resources.queue.queue_name == "null.q"
    assert task_resources.requested_resources["runtime"] == 3753

    assert wf1._status == "G"


class TestSwarmWorkflowRunConfig:
    """Tests for SwarmBuilder initialization with config."""

    def test_init_with_config_object(self):
        """Test initialization using WorkflowRunConfig object."""
        from jobmon.core.requester import Requester

        config = WorkflowRunConfig(
            heartbeat_interval=60,
            heartbeat_report_by_buffer=2.0,
            fail_fast=True,
            wedged_workflow_sync_interval=300,
            fail_after_n_executions=10,
        )

        # Create a builder with config values
        builder = SwarmBuilder(
            requester=Requester.from_defaults(),
            workflow_run_id=1,
            heartbeat_interval=config.heartbeat_interval,
            heartbeat_report_by_buffer=config.heartbeat_report_by_buffer,
        )

        assert builder.heartbeat_interval == 60
        assert builder.heartbeat_report_by_buffer == 2.0

    def test_init_with_default_config(self):
        """Test initialization with default config uses sensible defaults."""
        from jobmon.core.requester import Requester

        builder = SwarmBuilder(
            requester=Requester.from_defaults(),
            workflow_run_id=1,
        )

        # Should use defaults
        assert builder.heartbeat_interval == 30.0
        assert builder.heartbeat_report_by_buffer == 1.5

    def test_config_values_propagate(self):
        """Test that config values propagate correctly."""
        config = WorkflowRunConfig(
            fail_fast=True,
            wedged_workflow_sync_interval=300,
        )

        assert config.fail_fast is True
        assert config.wedged_workflow_sync_interval == 300
