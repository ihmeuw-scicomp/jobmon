from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.client.workflow import DistributorContext


def test_distributor_context(tool, task_template, client_env):
    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([t1])
    workflow.bind()
    assert workflow.workflow_id is not None
    workflow._bind_tasks()
    assert t1.task_id is not None
    wfr = WorkflowRunFactory(workflow.workflow_id).create_workflow_run()

    distributor_context = DistributorContext("sequential", wfr.workflow_run_id, 15)
    distributor_context.__enter__()
    assert distributor_context.alive()

    distributor_context._shutdown()
    assert not distributor_context.alive()
