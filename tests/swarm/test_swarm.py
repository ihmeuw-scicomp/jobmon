import ast
import logging
import os
import time

import pytest
from sqlalchemy import text, update
from sqlalchemy.orm import Session

from jobmon.plugins.dummy.dummy_distributor import DummyDistributor
from jobmon.plugins.sequential.seq_distributor import SequentialDistributor
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.workflow import DistributorContext
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import WorkflowRunStatus, TaskInstanceStatus
from jobmon.core.exceptions import CallableReturnedInvalidObject
from jobmon.distributor.distributor_service import DistributorService
from jobmon.server.web._compat import subtract_time
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models import load_model
from jobmon.worker_node.cli import WorkerNodeCLI


load_model()


logger = logging.getLogger(__name__)


this_dir = os.path.dirname(os.path.abspath(__file__))
resource_file = os.path.join(this_dir, "resources.txt")


class MockDistributorProc:
    def is_alive(self):
        return True


def test_blocking_update_timeout(tool, task_template):
    """This test runs a 1 task workflow and confirms that the workflow_run
    will timeout with an appropriate error message if timeout is set
    """

    task = task_template.create_task(arg="sleep 3", name="foobarbaz")
    workflow = tool.create_workflow(name="my_simple_dag")
    workflow.add_tasks([task])
    workflow.bind()
    workflow._bind_tasks()
    workflow._distributor_proc = MockDistributorProc()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()

    # Move workflow and wfr through Bound -> Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.BOUND)
    wfr._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)

    with pytest.raises(RuntimeError) as error:
        swarm.run(lambda: True, seconds_until_timeout=2)

    expected_msg = (
        "Not all tasks completed within the given workflow "
        "timeout length (2 seconds). Submitted tasks will still"
        " run, but the workflow will need to be restarted."
    )
    assert expected_msg == str(error.value)


def test_sync_statuses(client_env, tool, task_template):
    """this test executes a single task workflow where the task fails. It
    is testing to confirm that the status updates are propagated into the
    swarm objects"""

    # client calls
    task = task_template.create_task(arg="fizzbuzz", name="bar", max_attempts=1)
    workflow = tool.create_workflow()
    workflow.add_tasks([task])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # move workflow to launched state
    distributor_service = DistributorService(
        SequentialDistributor("sequential"),
        workflow.requester,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)

    # test from_workflow updates last_sync
    now = swarm.last_sync
    assert now is not None

    # distribute the task
    swarm.set_initial_fringe()
    swarm.process_commands()
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    time.sleep(2)

    swarm.synchronize_state(full_sync=True)
    assert len(swarm.failed_tasks) == 1
    assert len(swarm.done_tasks) == 0


def test_wedged_dag(db_engine, tool, task_template, requester_no_retry):
    """This test runs a 3 task dag where one of the tasks updates it status
    without updating its status date. This would cause the normal pathway of
    status collection in the workflow run to fail. Instead the test uses the
    wedged_workflow_sync_interval set to -1 second to force a full sync of
    the workflow tasks which resolves the wedge"""

    class WedgedDistributor(DummyDistributor):
        wedged_task_id = None

        def submit_to_batch_distributor(
            self, command: str, name: str, requested_resources
        ) -> str:
            logger.info("Now entering WedgedExecutor execute")

            cli = WorkerNodeCLI()
            args = cli.parse_args(command)
            # need to get task id from task instance here to compare to wedged
            # task id that will be set later in the code
            with Session(bind=db_engine) as session:
                task_instance = (
                    session.query(TaskInstance)
                    .filter_by(id=args.task_instance_id)
                    .one()
                )
                task_id = int(task_instance.task.id)

            if task_id == self.wedged_task_id:

                with Session(bind=db_engine) as session:
                    logger.info(
                        f"task instance is {self.wedged_task_id}, entering"
                        " first if statement"
                    )
                    task_inst_stmt = (
                        update(TaskInstance)
                        .where(TaskInstance.id == args.task_instance_id)
                        .values(status="D")
                    )
                    task_stmt = (
                        update(Task)
                        .where(Task.id == task_id)
                        .values(status="D", status_date=subtract_time(600))
                    )

                    session.execute(task_inst_stmt)
                    session.execute(task_stmt)
                    session.commit()

                    exec_id = str(123456789)
            else:
                exec_id = super().submit_to_batch_distributor(
                    command, name, requested_resources
                )

            return exec_id

    workflow = tool.create_workflow()
    workflow.requester = requester_no_retry
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 3")
    t2 = tool.active_task_templates["phase_1"].create_task(arg="sleep 5")
    t3 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 7", upstream_tasks=[t2]
    )
    workflow.add_tasks([t1, t2, t3])

    # bind workflow to db
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # create distributor with WedgedDistributor
    distributor = WedgedDistributor("dummy")
    distributor.wedged_task_id = t2.task_id
    distributor_service = DistributorService(
        distributor, requester=workflow.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # queue first 2 tasks
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # check that we get the instantiating signal
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    swarm.synchronize_state()
    assert swarm.tasks[t1.task_id].status == TaskStatus.INSTANTIATING
    assert swarm.tasks[t2.task_id].status == TaskStatus.INSTANTIATING

    # run the normal workflow sync protocol. only t1 should be done
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    swarm.synchronize_state()
    assert swarm.tasks[t1.task_id].status == TaskStatus.DONE
    assert swarm.tasks[t2.task_id].status == TaskStatus.INSTANTIATING
    assert swarm.tasks[t3.task_id].status == TaskStatus.REGISTERING

    # Force the workflow run back to instantiating state, since the distributor service
    # transitions the workflow_run to launched
    with Session(bind=db_engine) as session:
        sql = """
            UPDATE workflow_run
            SET status = 'O'
            WHERE id = :workflow_run_id
        """
        session.execute(text(sql), {"workflow_run_id": wfr.workflow_run_id})

        sql = """
            UPDATE workflow
            SET status = 'O'
            WHERE id = :workflow_id
        """
        session.execute(text(sql), {"workflow_id": workflow.workflow_id})
        session.commit()
    # now run wedged dag route. make sure task 2 is now in done state
    with pytest.raises(RuntimeError):
        swarm.wedged_workflow_sync_interval = -1
        swarm.run(lambda: True, seconds_until_timeout=1)
    assert swarm.tasks[t1.task_id].status == TaskStatus.DONE
    assert swarm.tasks[t2.task_id].status == TaskStatus.DONE
    assert swarm.ready_to_run[0] == swarm.tasks[t3.task_id]


def test_fail_fast(tool):
    """set up a dag where a middle job fails. The fail_fast parameter should
    ensure that not all tasks finish"""

    # The sleep for t3 must be long so that the swarm has time to notice that t2
    # died and react accordingly.
    workflow = tool.create_workflow(name="test_fail_fast")
    t1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_1"].create_task(
        arg="erroring_out 1", upstream_tasks=[t1], max_attempts=1
    )
    t3 = tool.active_task_templates["phase_1"].create_task(
        arg="sleep 20", upstream_tasks=[t1]
    )
    t4 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 3", upstream_tasks=[t3]
    )
    t5 = tool.active_task_templates["phase_3"].create_task(
        arg="sleep 4", upstream_tasks=[t4]
    )

    workflow.add_tasks([t1, t2, t3, t4, t5])

    workflow.run(fail_fast=True)

    assert len(workflow.task_errors) == 1
    num_done = len(
        [
            task
            for task in workflow.tasks.values()
            if task.final_status == TaskStatus.DONE
        ]
    )
    assert num_done >= 1
    assert num_done <= 3


def test_propagate_result(tool, task_template):
    """set up workflow with 3 tasks on one layer and 3 tasks as dependant"""

    workflow = tool.create_workflow(name="test_propagate_result")

    t1 = tool.active_task_templates["phase_1"].create_task(arg="echo 1")
    t2 = tool.active_task_templates["phase_1"].create_task(arg="echo 2")
    t3 = tool.active_task_templates["phase_1"].create_task(arg="echo 3")
    t4 = tool.active_task_templates["phase_2"].create_task(
        arg="echo 4", upstream_tasks=[t1, t2, t3]
    )
    t5 = tool.active_task_templates["phase_2"].create_task(
        arg="echo 5", upstream_tasks=[t1, t2, t3]
    )
    t6 = tool.active_task_templates["phase_2"].create_task(
        arg="echo 6", upstream_tasks=[t1, t2, t3]
    )
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # run the distributor
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        # swarm calls
        swarm = SwarmWorkflowRun(
            workflow_run_id=wfr.workflow_run_id,
            requester=workflow.requester,
        )
        swarm.from_workflow(workflow)
        swarm.run(distributor.alive)

    assert swarm.status == WorkflowRunStatus.DONE
    assert len(swarm.done_tasks) == 6
    assert swarm.tasks[t4.task_id].num_upstreams_done >= 3
    assert swarm.tasks[t5.task_id].num_upstreams_done >= 3
    assert swarm.tasks[t6.task_id].num_upstreams_done >= 3


def test_callable_returns_valid_object(tool, task_template):
    """Test when the provided callable returns the correct parameters"""

    def resource_file_does_exist(*args, **kwargs):
        # file contains dict with
        # {'m_mem_free': '2G', 'max_runtime_seconds': 30, 'num_cores': 1,
        # 'queue': 'all.q'}
        with open(resource_file, "r") as file:
            resources = file.read()
            resource_dict = ast.literal_eval(resources)
        return resource_dict

    workflow = tool.create_workflow(workflow_args="dynamic_resource_wf_good_file")
    task = task_template.create_task(
        arg="sleep 1",
        name="good_callable_task",
        compute_resources_callable=resource_file_does_exist,
    )
    workflow.add_task(task)
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)

    # swarm calls
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        try:
            swarm.run(distributor.alive, seconds_until_timeout=1)
        except RuntimeError:
            pass
    assert swarm.tasks[task.task_id].current_task_resources.id is not None


def test_callable_returns_wrong_object(tool, task_template):
    """test that the callable cannot return an invalid object"""

    def wrong_return_params(*args, **kwargs):
        wrong_format = ["1G", 60, 1]
        return wrong_format

    task = task_template.create_task(
        arg="sleep 1",
        name="good_callable_task",
        compute_resources_callable=wrong_return_params,
    )
    wf = tool.create_workflow(workflow_args="dynamic_resource_wf_wrong_param_obj")
    wf.add_task(task)
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)
    swarm = SwarmWorkflowRun(workflow_run_id=wfr.workflow_run_id)
    swarm.from_workflow(wf)
    with pytest.raises(CallableReturnedInvalidObject):
        swarm.set_initial_fringe()


def test_callable_fails_bad_filepath(tool, task_template):
    """test that an exception in the callable gets propagated up the call stack"""

    def resource_filepath_does_not_exist(*args, **kwargs):
        fp = os.path.join(this_dir, "file_that_does_not_exist.txt")
        file = open(fp, "r")
        file.read()

    task = task_template.create_task(
        name="bad_callable_wrong_file",
        arg="sleep 1",
        compute_resources_callable=resource_filepath_does_not_exist,
    )
    wf = tool.create_workflow(workflow_args="dynamic_resource_wf_bad_file")
    wf.add_task(task)
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)
    swarm = SwarmWorkflowRun(workflow_run_id=wfr.workflow_run_id)
    swarm.from_workflow(wf)
    with pytest.raises(FileNotFoundError):
        swarm.set_initial_fringe()


def test_swarm_fails(tool):
    """Test the swarm exits on error appropriately."""

    workflow = tool.create_workflow(name="test_propagate_result")

    t1 = tool.active_task_templates["phase_1"].create_task(arg="echo 1")
    t2 = tool.active_task_templates["phase_1"].create_task(arg="exit 1", max_attempts=1)
    t3 = tool.active_task_templates["phase_2"].create_task(
        arg="echo 3", upstream_tasks=[t2]
    )
    workflow.add_tasks([t1, t2, t3])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # run the distributor
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        # swarm calls
        swarm = SwarmWorkflowRun(
            workflow_run_id=wfr.workflow_run_id,
            requester=workflow.requester,
        )
        swarm.from_workflow(workflow)
        swarm.run(distributor.alive)

    assert swarm.status == WorkflowRunStatus.ERROR
    assert len(swarm.done_tasks) == 1
    assert len(swarm.failed_tasks) == 1
    assert len(swarm.ready_to_run) == 0
    assert swarm.tasks[t3.task_id].num_upstreams_done == 0
    assert not swarm.active_tasks


def test_swarm_terminate(tool):
    """Test that when the workflow run terminates properly."""

    class MockSwarm(SwarmWorkflowRun):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.sync_attempts = 0

        def synchronize_state(self, full_sync: bool = False) -> None:
            super().synchronize_state(full_sync)
            self.sync_attempts += 1
            if self.sync_attempts == 2:
                # Signal a cold resume
                self._update_status(WorkflowRunStatus.COLD_RESUME)

    workflow = tool.create_workflow(name="test_terminate")

    t1 = tool.active_task_templates["phase_1"].create_task(
        arg="sleep 1000", max_attempts=1
    )  # Long sleep time
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # run the distributor
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        # swarm calls
        swarm = MockSwarm(
            workflow_run_id=wfr.workflow_run_id,
            requester=workflow.requester,
        )
        swarm.from_workflow(workflow)
        swarm.run(distributor.alive)

    assert swarm.status == WorkflowRunStatus.TERMINATED
    assert len(swarm.done_tasks) == 0
    assert len(swarm.failed_tasks) == 1
    assert len(swarm.ready_to_run) == 0


def test_build_swarm_from_workflow_id(tool, task_template):
    workflow = tool.create_workflow()

    # Create a small example DAG.
    #       t1
    #     /    \
    #    t2     t3
    #      \   /
    #        t4
    t1 = task_template.create_task(arg="sleep 1")
    t2 = task_template.create_task(arg="sleep 2", upstream_tasks=[t1])
    t3 = task_template.create_task(arg="exit 1", upstream_tasks=[t1], max_attempts=1)
    t4 = task_template.create_task(
        arg="sleep 4",
        upstream_tasks=[t2, t3],
        compute_resources={"foo": "bar"},
        fallback_queues=["null.q"],
    )
    workflow.add_tasks([t1, t2, t3, t4])
    # Run the workflow. Task 3 should error, task 4 doesn't run.
    workflow.run()
    # Task states should be [D, D, F, G] at this point

    # Test a resumed workflow
    resume_factory = WorkflowRunFactory(workflow.workflow_id)
    resume_factory.set_workflow_resume()
    resume_factory.reset_task_statuses()
    resume_wfr = resume_factory.create_workflow_run()

    resume_swarm = SwarmWorkflowRun(
        workflow_run_id=resume_wfr.workflow_run_id, status=resume_wfr.status
    )
    assert resume_swarm.status == WorkflowRunStatus.LINKING

    # Pull workflow metadata
    resume_swarm.set_workflow_metadata(workflow.workflow_id)
    assert resume_swarm.dag_id == workflow._dag.dag_id
    assert resume_swarm.max_concurrently_running == workflow.max_concurrently_running

    # Fetch tasks from the database.
    # Expect 2 tasks back, use a chunk size of 1 to test edge cases.
    resume_swarm.set_tasks_from_db(chunk_size=1)
    # 2 tasks, t3 and t4 expected back.
    assert len(resume_swarm.tasks) == 2
    assert set(resume_swarm.tasks.keys()) == {t3.task_id, t4.task_id}
    st3, st4 = resume_swarm.tasks[t3.task_id], resume_swarm.tasks[t4.task_id]

    assert st3.current_task_resources.requested_resources == {}
    assert st4.current_task_resources.requested_resources == {"foo": "bar"}
    assert len(st4.fallback_queues) == 1
    assert st4.fallback_queues[0].queue_id == 2  # null.q, sequential cluster
    assert st3.status == TaskStatus.REGISTERING
    # T3 and T4 share same array, 1 value present
    assert len(resume_swarm.arrays) == 1
    swarmarray = resume_swarm.arrays[st3.array_id]
    assert swarmarray.tasks == {st3, st4}
    assert resume_swarm._task_status_map[TaskStatus.REGISTERING] == {st3, st4}

    # Set downstreams from the database
    resume_swarm.set_downstreams_from_db(30)
    assert st3.downstream_swarm_tasks == {st4}
    assert st4.num_upstreams == 1
    assert st4.downstream_swarm_tasks == set()

    resume_swarm.set_initial_fringe()
    assert len(resume_swarm.ready_to_run) == 1
    assert resume_swarm.ready_to_run[0] == st3

    # Run a full sync, test that no keyerrors are raised
    resume_swarm._task_status_updates(full_sync=True)
