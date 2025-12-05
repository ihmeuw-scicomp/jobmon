import asyncio
from unittest import mock

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from jobmon.client.api import Tool
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessDistributor
from jobmon.server.web._compat import subtract_time
from jobmon.server.web.models import load_model
from tests.pytest.swarm.swarm_test_utils import (
    create_test_context,
    prepare_and_queue_tasks,
)

load_model()


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
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


def test_set_status_for_triaging(tool, db_engine, task_template):
    """tests that a task can be triaged and log as unknown error"""
    from jobmon.server.web.models.task_instance import TaskInstance

    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )

    tis = [task_template.create_task(arg="sleep 10" + str(x)) for x in range(3)]
    workflow = tool.create_workflow(name="test_set_status_for_no_heartbeat")

    workflow.add_tasks(tis)
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()

    # create task instances
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )
    prepare_and_queue_tasks(state, gateway, orchestrator)

    distributor = MultiprocessDistributor("multiprocess", 5)
    distributor.start()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        distributor, requester=workflow.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)

    # turn the 3 task instances in different testing paths
    # 1. stage the report_by_date, along with respective status
    with Session(bind=db_engine) as session:
        launched_stmt = (
            update(TaskInstance)
            .where(TaskInstance.task_id.in_([tis[0].task_id, tis[2].task_id]))
            .values(
                report_by_date=subtract_time(500),
                status=TaskInstanceStatus.LAUNCHED,
                status_date=subtract_time(
                    3600
                ),  # 1 hour ago - much older than heartbeat interval
            )
        )
        session.execute(launched_stmt)
        running_stmt = (
            update(TaskInstance)
            .where(TaskInstance.task_id == tis[1].task_id)
            .values(
                report_by_date=subtract_time(500),
                status=TaskInstanceStatus.RUNNING,
                status_date=subtract_time(
                    3600
                ),  # 1 hour ago - much older than heartbeat interval
            )
        )
        session.execute(running_stmt)
        session.commit()

    # 2. call gateway.request_triage()
    async def request_triage():
        import aiohttp

        session = aiohttp.ClientSession()
        gateway.set_session(session)
        try:
            await gateway.request_triage()
        finally:
            await session.close()

    asyncio.run(request_triage())

    # check the jobs to be Triaging
    with Session(bind=db_engine) as session:
        select_stmt = (
            select(TaskInstance)
            .where(TaskInstance.task_id.in_([ti.task_id for ti in tis]))
            .order_by(TaskInstance.id)
        )
        task_instances = session.execute(select_stmt).scalars().all()
        session.commit()

        assert len(task_instances) == len(tis)
        assert task_instances[0].status == TaskInstanceStatus.NO_HEARTBEAT
        assert task_instances[1].status == TaskInstanceStatus.TRIAGING
        assert task_instances[2].status == TaskInstanceStatus.NO_HEARTBEAT

    distributor.stop()


@pytest.mark.parametrize(
    "error_state, error_message",
    [
        (
            TaskInstanceStatus.RESOURCE_ERROR,
            "Insufficient resources requested. Task was lost",
        ),
        (
            TaskInstanceStatus.UNKNOWN_ERROR,
            "One possible reason might be that either stderr or stdout is not accessible or writable",
        ),
        (TaskInstanceStatus.ERROR_FATAL, "Error Fatal occurred"),
    ],
)
def test_triaging_to_specific_error(
    tool, db_engine, task_template, error_state, error_message
):
    """tests that a task can be triaged and log as unknown error"""
    from jobmon.server.web.models.task_instance import TaskInstance

    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )

    tis = [task_template.create_task(arg="sleep 6" + str(x)) for x in range(2)]
    workflow = tool.create_workflow(name="test_triaging_on_multiprocess")

    workflow.add_tasks(tis)
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()

    # create task instances
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )
    prepare_and_queue_tasks(state, gateway, orchestrator)

    distributor = MultiprocessDistributor("multiprocess", 5)
    distributor.start()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        distributor, requester=workflow.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # stage all the task instances as triaging
    with Session(bind=db_engine) as session:
        update_stmt = (
            update(TaskInstance)
            .where(TaskInstance.task_id.in_([tis[x].task_id for x in range(len(tis))]))
            .values(
                report_by_date=subtract_time(500), status=TaskInstanceStatus.TRIAGING
            )
        )
        session.execute(update_stmt)
        session.commit()

    # synchronize statuses from the db and get new work
    # distributor_service._check_for_work(TaskInstanceStatus.TRIAGING)

    with mock.patch(
        "jobmon.plugins.multiprocess.multiproc_distributor."
        "MultiprocessDistributor.get_remote_exit_info",
        return_value=(error_state, error_message),
    ):
        # code logic to test
        distributor_service.refresh_status_from_db(TaskInstanceStatus.TRIAGING)
        distributor_service.process_status(TaskInstanceStatus.TRIAGING)

    # check the jobs to be UNKNOWN_ERROR as expected
    with Session(bind=db_engine) as session:
        select_stmt = (
            select(TaskInstance)
            .where(TaskInstance.task_id.in_([ti.task_id for ti in tis]))
            .order_by(TaskInstance.id)
        )
        task_instances = session.execute(select_stmt).scalars().all()

        assert len(task_instances) == len(tis)

        for ti in task_instances:
            assert ti.status == error_state
            assert ti.errors[0].description == error_message

    distributor.stop()
