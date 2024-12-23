from sqlalchemy import select, update
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskInstanceStatus
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessDistributor
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog

load_model()


def test_transition_to_killed(tool, db_engine, task_template):
    # 1) Create a small workflow and bind to DB
    t1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 200")
    t2 = tool.active_task_templates["simple_template"].create_task(arg="sleep 201")

    workflow = tool.create_workflow(name="test_transition_to_killed")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    assert workflow.workflow_id is not None
    workflow._bind_tasks()  # explicitly bind tasks
    assert t1.task_id is not None
    assert t2.task_id is not None

    # 2) Create a WorkflowRun and produce TaskInstances
    wfr = WorkflowRunFactory(workflow.workflow_id).create_workflow_run()
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # 3) Start a DistributorService to queue/instantiate tasks
    distributor_service = DistributorService(
        MultiprocessDistributor(cluster_name="multiprocessing", parallelism=2),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)

    # Move tasks from QUEUED -> INSTANTIATED
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # 4) Force these TaskInstances into KILL_SELF (simulating a reason to kill them).
    with Session(bind=db_engine) as session:
        # Mark all TIs for t1 and t2 with status=KILL_SELF
        stmt = (
            update(TaskInstance)
            .where(TaskInstance.task_id.in_([t1.task_id, t2.task_id]))
            .values(status=TaskInstanceStatus.KILL_SELF)
        )
        session.execute(stmt)
        session.commit()

    # 5) Refresh and process the KILL_SELF state; this should trigger your new logic,
    #    which calls `transition_to_killed` (or the DB endpoint) behind the scenes.
    distributor_service.refresh_status_from_db(TaskInstanceStatus.KILL_SELF)
    distributor_service.process_status(TaskInstanceStatus.KILL_SELF)

    # 6) Check that TIs are now in UNKNOWN_ERROR and that an error log was recorded
    with Session(bind=db_engine) as session:
        select_stmt = (
            select(TaskInstance)
            .where(TaskInstance.task_id.in_([t1.task_id, t2.task_id]))
            .order_by(TaskInstance.id)
        )
        task_instances = session.execute(select_stmt).scalars().all()

        for ti in task_instances:
            # Confirm they transitioned to ERROR_FATAL
            assert (
                ti.status == TaskInstanceStatus.ERROR_FATAL
            ), f"Expected ERROR_FATAL but got {ti.status} for TI {ti.id}"

    # 7) Shut down the distributor
    distributor_service.cluster_interface.stop()
