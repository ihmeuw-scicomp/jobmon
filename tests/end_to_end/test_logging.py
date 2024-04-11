import time

from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.sequential.seq_distributor import SequentialDistributor
from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessDistributor


def test_sequential_logging(tool, task_template, tmp_path):
    """create a 1 task workflow and confirm it works end to end"""

    workflow = tool.create_workflow(
        name="test_sequential_logging", default_cluster_name="sequential"
    )
    t1 = task_template.create_task(
        arg="echo 'hello world'",
        name="stdout_task",
        compute_resources={"stdout": f"{str(tmp_path)}"},
    )
    t2 = task_template.create_task(
        arg="foobar",
        name="stderr_task",
        compute_resources={"stderr": f"{str(tmp_path)}"},
        upstream_tasks=[t1],
    )
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id, requester=workflow.requester)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        SequentialDistributor("sequential"),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    with open(tmp_path / "stdout_task.o1") as f:
        assert "hello world\n" in f.readlines()

    swarm.synchronize_state()
    swarm.process_commands()

    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    with open(tmp_path / "stderr_task.e2") as f:
        assert "not found" in f.readline().rstrip()


def test_multiprocess_logging(tool, task_template, tmp_path):
    """create a 1 task workflow and confirm it works end to end"""

    workflow = tool.create_workflow(
        name="test_multiprocess_logging",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )
    t1 = task_template.create_task(
        arg="echo 'hello world'",
        name="stdout_task",
        compute_resources={"stdout": f"{str(tmp_path)}"},
    )
    t2 = task_template.create_task(
        arg="foobar",
        name="stderr_task",
        compute_resources={"stderr": f"{str(tmp_path)}"},
        upstream_tasks=[t1],
    )
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id, requester=workflow.requester)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=2),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.cluster_interface.start()
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    counter = 0
    while distributor_service.cluster_interface.get_submitted_or_running():
        time.sleep(1)
        counter += 1
        if counter > 10:
            break

    with open(tmp_path / "stdout_task.o1_0") as f:
        assert "hello world\n" in f.readlines()

    swarm.synchronize_state()
    swarm.process_commands()

    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    counter = 0
    while distributor_service.cluster_interface.get_submitted_or_running():
        time.sleep(1)
        counter += 1
        if counter > 10:
            break

    with open(tmp_path / "stderr_task.e2_0") as f:
        assert "not found" in f.readline().rstrip()

    distributor_service.cluster_interface.stop()


def test_dummy_executor_with_bad_log_path(tool, task_template, tmp_path):
    """Check that the Dummy executor ignores bad paths. A real executor will fail this workflow"""

    workflow = tool.create_workflow(
        name="test_dummy_executor_with_bad_log_path",
        default_cluster_name="dummy",
        default_compute_resources_set={"dummy": {"queue": "null.q"}},
    )
    t1 = task_template.create_task(
        arg="echo helloworld",
        name="bad_stderr_task",
        compute_resources={
            "stdout": "/utterly/bogus/file/path",
            "stderr": "/utterly/bogus/file/path",
        },
    )
    workflow.add_tasks([t1])
    workflow_run_status = workflow.run()
    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow._num_newly_completed == 1
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 0
