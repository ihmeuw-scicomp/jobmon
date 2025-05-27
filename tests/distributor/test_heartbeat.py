from sqlalchemy import select
from sqlalchemy.orm import Session

from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessDistributor
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task_instance import TaskInstance

load_model()


def test_heartbeat_on_launched(tool, db_engine, task_template):
    # create the workflow and bind to database
    t1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 10")
    t2 = tool.active_task_templates["simple_template"].create_task(arg="sleep 11")

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs_on_sequential")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    assert workflow.workflow_id is not None
    workflow._bind_tasks()
    assert t1.task_id is not None
    assert t2.task_id is not None
    wfr = WorkflowRunFactory(workflow.workflow_id).create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # launch the task then log a heartbeat
    distributor_service = DistributorService(
        MultiprocessDistributor(cluster_name="multiprocessing", parallelism=2),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    # distributor_service.refresh_status_from_db(TaskInstanceStatus.LAUNCHED)

    # log a heartbeat. sequential will think it's still running
    distributor_service.log_task_instance_report_by_date()

    # check the heartbeat date is greater than the latest status
    with Session(bind=db_engine) as session:
        select_stmt = (
            select(TaskInstance)
            .where(TaskInstance.task_id.in_([t1.task_id, t2.task_id]))
            .order_by(TaskInstance.id)
        )
        task_instances = session.execute(select_stmt).scalars().all()
        session.commit()

        for ti in task_instances:
            assert ti.status_date < ti.report_by_date

    distributor_service.cluster_interface.stop()
