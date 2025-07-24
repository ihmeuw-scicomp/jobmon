from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.dummy.dummy_distributor import DummyDistributor


def test_queued(tool, task_template):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)

    workflow = tool.create_workflow(name="test_n_queued", max_concurrently_running=10)
    workflow.add_tasks(tasks)
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    assert swarm.max_concurrently_running == 10

    # create a batch
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        DummyDistributor("dummy"), requester=workflow.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.QUEUED])
        == 10
    )

    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    assert (
        len(
            distributor_service._task_instance_status_map[
                TaskInstanceStatus.INSTANTIATED
            ]
        )
        == 10
    )
