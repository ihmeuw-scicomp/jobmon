import pytest

from jobmon.client.api import Tool
from jobmon.client.workflow_run import WorkflowRunFactory, WorkflowRun
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
    wf1._bind_tasks()
    task_resources = list(wf1._task_resources.values())[0]
    assert task_resources.requested_resources["memory"] == 20
    assert task_resources.queue.queue_name == "null.q"
    assert task_resources.requested_resources["runtime"] == 3753

    assert wf1._status == "G"
