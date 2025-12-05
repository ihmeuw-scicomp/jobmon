import asyncio
import os
import sys
import time
from unittest.mock import patch

import pytest

from jobmon.client.tool import Tool
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.core.exceptions import WorkflowAlreadyExists, WorkflowNotResumable
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessDistributor
from tests.pytest.integration.swarm.swarm_test_utils import (
    create_test_context,
    prepare_and_queue_tasks,
    synchronize_state,
)


@pytest.fixture
def tool(client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )
    return tool


def get_task_template(tool, template_name="my_template"):
    tt = tool.get_task_template(
        template_name=template_name,
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


@pytest.fixture
def task_template(tool):
    return get_task_template(tool)


def get_task_template_fail_one(tool, template_name="task_template_fail_one"):
    # set fail always as op args so it can be modified on resume without
    # changing the workflow hash
    tt = tool.get_task_template(
        template_name="foo",
        command_template=(
            "{python} "
            "{script} "
            "--sleep_secs {sleep_secs} "
            "--output_file_path {output_file_path} "
            "--task_name {task_name} "
            "{fail_always}"
        ),
        node_args=["task_name"],
        task_args=["sleep_secs", "output_file_path"],
        op_args=["python", "script", "fail_always"],
    )
    return tt


@pytest.fixture
def task_template_fail_one(tool):
    # set fail always as op args so it can be modified on resume without
    # changing the workflow hash
    return get_task_template_fail_one(tool)


this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(
    os.path.expanduser(f"{this_file}/../../_scripts/remote_sleep_and_write.py")
)


def test_fail_one_task_resume(tool, task_template_fail_one, tmpdir):
    """test that a workflow with a task that fails. The workflow is resumed and
    the task then finishes successfully and the workflow runs to completion"""

    # create workflow and execute
    workflow1 = tool.create_workflow(name="fail_one_task_resume")
    t1 = task_template_fail_one.create_task(
        task_name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="--fail_always",
    )
    workflow1.add_tasks([t1])
    workflow_run_status = workflow1.run()

    assert workflow_run_status == WorkflowRunStatus.ERROR
    assert len(workflow1.task_errors) == 1
    assert workflow1.last_workflow_run_id is not None

    # set workflow args and name to be identical to previous workflow
    workflow2 = tool.create_workflow(
        name=workflow1.name, workflow_args=workflow1.workflow_args
    )
    t2 = task_template_fail_one.create_task(
        task_name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="",
    )  # fail bool is not set. workflow should succeed
    workflow2.add_tasks([t2])

    with pytest.raises(WorkflowAlreadyExists):
        workflow2.run()

    workflow_run_status = workflow2.run(resume=True)

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow1.workflow_id == workflow2.workflow_id


def get_two_wave_tasks(tool):
    tasks = []

    task_template_1 = get_task_template(tool, "phase_1")
    wave_1 = []
    for i in range(3):
        tm = 5 + i
        t = task_template_1.create_task(arg=f"sleep {tm}")
        tasks.append(t)
        wave_1.append(t)

    task_template_2 = get_task_template(tool, "phase_2")
    for i in range(3):
        tm = 8 + i
        t = task_template_2.create_task(arg=f"sleep {tm}", upstream_tasks=wave_1)
        tasks.append(t)
    return tasks


class MockDistributorProc:
    def is_alive(self):
        return True


def test_cold_resume(tool):
    """"""
    # prepare first workflow
    workflow1 = tool.create_workflow(name="cold_resume")
    workflow1.add_tasks(get_two_wave_tasks(tool))
    workflow1.bind()
    workflow1._bind_tasks()
    factory = WorkflowRunFactory(workflow1.workflow_id)
    wfr1 = factory.create_workflow_run()
    wfr1._update_status(WorkflowRunStatus.BOUND)

    # create task instances
    state, gateway, orchestrator = create_test_context(
        workflow1, wfr1.workflow_run_id, workflow1.requester
    )
    prepare_and_queue_tasks(state, gateway, orchestrator)

    # run first 3 tasks
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=3),
        requester=workflow1.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr1.workflow_run_id)
    distributor_service.workflow_run.transition_to_launched()

    # Update state status
    state.status = WorkflowRunStatus.RUNNING
    gateway.update_status_sync(WorkflowRunStatus.RUNNING)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    distributor_service.cluster_interface.start()

    i = 0
    while state.get_done_count() < 3 and i < 20:
        synchronize_state(state, gateway, orchestrator, full_sync=True)
        i += 1
        time.sleep(1)
    distributor_service.cluster_interface.stop()
    assert state.get_done_count() == 3

    # create new workflow run, causing the old one to reset. resume timeout is
    # 1 second meaning this workflow run will not actually be created
    with pytest.raises(WorkflowNotResumable):
        workflow2 = tool.create_workflow(
            name=workflow1.name, workflow_args=workflow1.workflow_args
        )
        workflow2.add_tasks(get_two_wave_tasks(tool))
        workflow2.bind()
        workflow2._bind_tasks()
        fact2 = WorkflowRunFactory(workflow2.workflow_id)
        fact2.set_workflow_resume(resume_timeout=1)

    # test if resume signal is received
    synchronize_state(state, gateway, orchestrator, full_sync=True)
    assert state.status == WorkflowRunStatus.COLD_RESUME

    # set workflow run to terminated
    async def terminate_task_instances():
        import aiohttp

        session = aiohttp.ClientSession()
        gateway.set_session(session)
        try:
            await gateway.terminate_task_instances()
        finally:
            await session.close()

    asyncio.run(terminate_task_instances())

    distributor_service.refresh_status_from_db(TaskInstanceStatus.KILL_SELF)
    distributor_service.process_status(TaskInstanceStatus.KILL_SELF)
    gateway.update_status_sync(WorkflowRunStatus.TERMINATED)
    state.status = WorkflowRunStatus.TERMINATED

    # now resume it till done
    # prepare first workflow
    workflow3 = tool.create_workflow(
        name=workflow1.name,
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
        workflow_args=workflow1.workflow_args,
    )
    workflow3.add_tasks(get_two_wave_tasks(tool))
    workflow_run_status = workflow3.run(resume=True)

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow3._num_newly_completed >= 3  # number of newly completed tasks


def test_hot_resume(tool, task_template):
    workflow1 = tool.create_workflow(name="hot_resume")
    tasks = []
    for i in range(6):
        t = task_template.create_task(arg=f"sleep {10 + i}")
        tasks.append(t)
    workflow1.add_tasks(tasks)
    workflow1.bind()
    workflow1._bind_tasks()

    factory = WorkflowRunFactory(workflow_id=workflow1.workflow_id)
    wfr1 = factory.create_workflow_run()
    wfr1._update_status(WorkflowRunStatus.BOUND)

    # run first 3 tasks
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=3),
        requester=workflow1.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr1.workflow_run_id)
    distributor_service.workflow_run.transition_to_launched()

    state, gateway, orchestrator = create_test_context(
        workflow1, wfr1.workflow_run_id, workflow1.requester
    )
    state.status = WorkflowRunStatus.RUNNING
    gateway.update_status_sync(WorkflowRunStatus.RUNNING)

    # create task instances
    prepare_and_queue_tasks(state, gateway, orchestrator)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    distributor_service.cluster_interface.start()

    i = 0
    while state.get_done_count() < 3 and i < 20:
        synchronize_state(state, gateway, orchestrator, full_sync=True)
        i += 1
        time.sleep(1)
    distributor_service.cluster_interface.stop()
    assert state.get_done_count() == 3

    # now make another workflow and set a hot resume with a quick timeout
    workflow2 = tool.create_workflow(
        name="hot_resume", workflow_args=workflow1.workflow_args
    )
    tasks = []
    for i in range(6):
        t = task_template.create_task(arg=f"sleep {10 + i}")
        tasks.append(t)
    workflow2.add_tasks(tasks)
    workflow2.bind()
    workflow2._bind_tasks()

    fact2 = WorkflowRunFactory(workflow2.workflow_id)
    with pytest.raises(WorkflowNotResumable):
        fact2.set_workflow_resume(reset_running_jobs=False, resume_timeout=1)

    # test if resume signal is received
    synchronize_state(state, gateway, orchestrator, full_sync=True)
    assert state.status == WorkflowRunStatus.HOT_RESUME

    # set workflow run to terminated
    gateway.update_status_sync(WorkflowRunStatus.TERMINATED)
    state.status = WorkflowRunStatus.TERMINATED

    workflow_run_status = workflow2.run(resume=True)
    assert workflow_run_status == WorkflowRunStatus.DONE


def test_stopped_resume(tool):
    """test that a workflow with two task where the workflow is stopped with a
    keyboard interrupt mid stream. The workflow is resumed and
    the tasks then finishes successfully and the workflow runs to completion"""
    from jobmon.client.swarm.orchestrator import WorkflowRunOrchestrator

    workflow1 = tool.create_workflow(name="stopped_resume")
    upstream_tasks = []
    for phase in [1, 2, 3]:
        task_template = get_task_template(tool, template_name=f"phase_{phase}")
        task = task_template.create_task(arg="echo a", upstream_tasks=upstream_tasks)
        workflow1.add_task(task)
        upstream_tasks = [task]

    # Patch the orchestrator's _check_fail_after_n_executions to raise KeyboardInterrupt
    # This simulates a user pressing Ctrl+C during workflow execution
    with pytest.raises(KeyboardInterrupt):
        with patch.object(
            WorkflowRunOrchestrator,
            "_check_fail_after_n_executions",
            side_effect=KeyboardInterrupt,
        ):
            # will ask if we want to exit. answer is 'y'
            with patch("builtins.input") as input_patch:
                input_patch.return_value = "y"
                workflow1.run()

    # now resume it
    workflow1 = tool.create_workflow(
        name="stopped_resume", workflow_args=workflow1.workflow_args
    )
    upstream_tasks = []
    for phase in [1, 2, 3]:
        task_template = get_task_template(tool, template_name=f"phase_{phase}")
        task = task_template.create_task(arg="echo a", upstream_tasks=upstream_tasks)
        workflow1.add_task(task)
        upstream_tasks = [task]

    wfrs2 = workflow1.run(resume=True)

    assert wfrs2 == WorkflowRunStatus.DONE
