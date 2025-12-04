import ast
import logging
import os
import time

import pytest
from sqlalchemy import text, update
from sqlalchemy.orm import Session

from jobmon.client.swarm import WorkflowRunConfig, run_workflow
from jobmon.client.swarm.builder import SwarmBuilder
from jobmon.client.swarm.orchestrator import OrchestratorConfig, WorkflowRunOrchestrator
from jobmon.client.workflow import DistributorContext
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.core.exceptions import CallableReturnedInvalidObject
from jobmon.core.requester import Requester
from jobmon.distributor.distributor_service import DistributorService
from jobmon.plugins.dummy.dummy_distributor import DummyDistributor
from jobmon.plugins.sequential.seq_distributor import SequentialDistributor
from jobmon.server.web._compat import subtract_time
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.worker_node.cli import WorkerNodeCLI
from tests.pytest.integration.swarm.swarm_test_utils import (
    create_builder,
    create_test_context,
    prepare_and_queue_tasks,
    set_initial_fringe,
    synchronize_state,
)

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

    # Use run_workflow with short timeout
    with pytest.raises(RuntimeError) as error:
        run_workflow(
            workflow=workflow,
            workflow_run_id=wfr.workflow_run_id,
            distributor_alive=lambda: True,
            status=wfr.status,
            timeout=2,
            requester=workflow.requester,
        )

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

    # Build state using builder
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )

    # test from_workflow updates last_sync
    now = state.last_sync
    assert now is not None

    # distribute the task
    prepare_and_queue_tasks(state, gateway, orchestrator)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    time.sleep(2)

    synchronize_state(state, gateway, orchestrator, full_sync=True)
    assert state.get_failed_count() == 1
    assert state.get_done_count() == 0


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

    # Build state and queue first 2 tasks
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )
    prepare_and_queue_tasks(state, gateway, orchestrator)

    # check that we get the instantiating signal
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    synchronize_state(state, gateway, orchestrator)
    assert state.tasks[t1.task_id].status == TaskStatus.INSTANTIATING
    assert state.tasks[t2.task_id].status == TaskStatus.INSTANTIATING

    # run the normal workflow sync protocol. only t1 should be done
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    synchronize_state(state, gateway, orchestrator)
    assert state.tasks[t1.task_id].status == TaskStatus.DONE
    assert state.tasks[t2.task_id].status == TaskStatus.INSTANTIATING
    assert state.tasks[t3.task_id].status == TaskStatus.REGISTERING

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

    # now run with wedged_workflow_sync_interval=-1 to force full sync
    config = WorkflowRunConfig(wedged_workflow_sync_interval=-1)
    with pytest.raises(RuntimeError):
        run_workflow(
            workflow=workflow,
            workflow_run_id=wfr.workflow_run_id,
            distributor_alive=lambda: True,
            status=wfr.status,
            config=config,
            timeout=1,
            requester=workflow.requester,
        )

    # Verify results directly from database (since build_from_workflow_id skips DONE tasks)
    with Session(bind=db_engine) as session:
        # t1 was done before the wedge
        task1_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t1.task_id}
        ).scalar_one()
        assert task1_status == TaskStatus.DONE

        # t2 was "wedged" but became done via full sync
        task2_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t2.task_id}
        ).scalar_one()
        assert task2_status == TaskStatus.DONE

        # t3 should be in some state - could be anything depending on timing
        task3_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t3.task_id}
        ).scalar_one()
        assert task3_status in {
            TaskStatus.REGISTERING,
            TaskStatus.QUEUED,
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
            TaskStatus.DONE,
        }


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


def test_propagate_result(db_engine, tool, task_template):
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
        result = run_workflow(
            workflow=workflow,
            workflow_run_id=wfr.workflow_run_id,
            distributor_alive=distributor.alive,
            status=wfr.status,
            requester=workflow.requester,
        )

    assert result.final_status == WorkflowRunStatus.DONE
    assert result.done_count == 6

    # Verify all tasks completed - particularly that downstream tasks (t4, t5, t6)
    # correctly waited for all 3 upstreams (t1, t2, t3) before running
    with Session(bind=db_engine) as session:
        for task in [t1, t2, t3, t4, t5, t6]:
            status = session.execute(
                text("SELECT status FROM task WHERE id = :tid"),
                {"tid": task.task_id},
            ).scalar_one()
            assert status == TaskStatus.DONE


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

    # Build state
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )

    # run with distributor
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        try:
            run_workflow(
                workflow=workflow,
                workflow_run_id=wfr.workflow_run_id,
                distributor_alive=distributor.alive,
                status=wfr.status,
                timeout=1,
                requester=workflow.requester,
            )
        except RuntimeError:
            pass

    # Rebuild state to check results
    builder = create_builder(workflow, wfr.workflow_run_id, workflow.requester)
    assert builder.tasks[task.task_id].current_task_resources.id is not None


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

    state, gateway, orchestrator = create_test_context(wf, wfr.workflow_run_id)

    with pytest.raises(CallableReturnedInvalidObject):
        set_initial_fringe(state, orchestrator)


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

    state, gateway, orchestrator = create_test_context(wf, wfr.workflow_run_id)

    with pytest.raises(FileNotFoundError):
        set_initial_fringe(state, orchestrator)


def test_swarm_fails(db_engine, tool):
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
        result = run_workflow(
            workflow=workflow,
            workflow_run_id=wfr.workflow_run_id,
            distributor_alive=distributor.alive,
            status=wfr.status,
            requester=workflow.requester,
        )

    assert result.final_status == WorkflowRunStatus.ERROR
    assert result.done_count == 1
    assert result.failed_count == 1

    # Verify specific task states: t1 done, t2 failed, t3 never ran (still REGISTERING)
    with Session(bind=db_engine) as session:
        t1_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t1.task_id}
        ).scalar_one()
        assert t1_status == TaskStatus.DONE

        t2_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t2.task_id}
        ).scalar_one()
        assert t2_status == TaskStatus.ERROR_FATAL

        # t3 should not have run since its upstream (t2) failed
        t3_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t3.task_id}
        ).scalar_one()
        assert t3_status == TaskStatus.REGISTERING


def test_swarm_terminate(db_engine, tool):
    """Test that when the workflow run receives a COLD_RESUME signal, it terminates properly.

    This test simulates the server setting the workflow_run status to COLD_RESUME
    (e.g., from another process requesting a resume). The swarm should detect this
    during heartbeat/sync and terminate gracefully.
    """
    import threading

    workflow = tool.create_workflow(name="test_terminate")

    t1 = tool.active_task_templates["phase_1"].create_task(
        arg="sleep 1000", max_attempts=1
    )  # Long sleep - will be terminated
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # Function to update the workflow_run status to COLD_RESUME after a delay
    def trigger_cold_resume():
        # Wait for workflow to start running
        # Use a longer delay to be resilient to parallel test execution (CI with many workers)
        time.sleep(8)
        with Session(bind=db_engine) as session:
            # Update workflow_run status to COLD_RESUME
            session.execute(
                text("UPDATE workflow_run SET status = :status WHERE id = :wfr_id"),
                {
                    "status": WorkflowRunStatus.COLD_RESUME,
                    "wfr_id": wfr.workflow_run_id,
                },
            )
            session.commit()
            logger.info(f"Set workflow_run {wfr.workflow_run_id} to COLD_RESUME")

    # Start background thread to trigger COLD_RESUME
    trigger_thread = threading.Thread(target=trigger_cold_resume, daemon=True)
    trigger_thread.start()

    # Run the distributor and swarm
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        config = WorkflowRunConfig(
            heartbeat_interval=1,  # Short interval to detect status change faster
        )
        result = run_workflow(
            workflow=workflow,
            workflow_run_id=wfr.workflow_run_id,
            distributor_alive=distributor.alive,
            status=wfr.status,
            config=config,
            requester=workflow.requester,
        )

    trigger_thread.join(timeout=1)

    # Workflow should have terminated due to COLD_RESUME
    assert result.final_status == WorkflowRunStatus.TERMINATED
    assert result.done_count == 0
    assert result.failed_count == 1  # t1 should be failed/terminated

    # Verify t2 never ran (still REGISTERING) since t1 was terminated
    with Session(bind=db_engine) as session:
        t2_status = session.execute(
            text("SELECT status FROM task WHERE id = :tid"), {"tid": t2.task_id}
        ).scalar_one()
        assert t2_status == TaskStatus.REGISTERING


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

    # Test a resumed workflow using SwarmBuilder
    resume_factory = WorkflowRunFactory(workflow.workflow_id)
    resume_factory.set_workflow_resume()
    resume_factory.reset_task_statuses()
    resume_wfr = resume_factory.create_workflow_run()

    # Use SwarmBuilder to build the swarm from workflow_id
    requester = Requester.from_defaults()
    builder = SwarmBuilder(
        requester=requester,
        workflow_run_id=resume_wfr.workflow_run_id,
        initial_status=resume_wfr.status,
    )

    # Build from workflow_id with small chunk size to test edge cases
    builder.build_from_workflow_id(
        workflow_id=workflow.workflow_id,
        edge_chunk_size=1,
    )

    # Verify workflow metadata was fetched
    assert builder.dag_id == workflow._dag.dag_id
    assert builder.state.max_concurrently_running == workflow.max_concurrently_running

    # Verify tasks were fetched correctly
    # Expect 2 tasks back (t3 and t4 - the ones not in DONE state)
    assert len(builder.state.tasks) == 2
    assert set(builder.state.tasks.keys()) == {t3.task_id, t4.task_id}
    st3, st4 = builder.state.tasks[t3.task_id], builder.state.tasks[t4.task_id]

    assert st3.current_task_resources.requested_resources == {}
    assert st4.current_task_resources.requested_resources == {"foo": "bar"}
    assert len(st4.fallback_queues) == 1
    assert st4.fallback_queues[0].queue_id == 2  # null.q, sequential cluster
    assert st3.status == TaskStatus.REGISTERING

    # T3 and T4 share same array, 1 value present
    assert len(builder.state.arrays) == 1
    swarmarray = builder.state.arrays[st3.array_id]
    assert swarmarray.tasks == {st3, st4}
    assert builder.state._task_status_map[TaskStatus.REGISTERING] == {st3, st4}

    # Verify downstream relationships were set up
    assert st3.downstream_swarm_tasks == {st4}
    assert st4.num_upstreams == 1
    assert st4.downstream_swarm_tasks == set()

    # Also test via builder with orchestrator for full integration
    state, gateway, orchestrator = create_test_context(
        workflow,
        resume_wfr.workflow_run_id,
        requester,
        initial_status=resume_wfr.status,
    )

    # Note: create_test_context builds from workflow (all tasks), not from workflow_id
    # So let's build from workflow_id instead
    resume_builder = SwarmBuilder(
        requester=requester,
        workflow_run_id=resume_wfr.workflow_run_id,
        initial_status=resume_wfr.status,
    )
    resume_builder.build_from_workflow_id(workflow.workflow_id, edge_chunk_size=1)

    config = OrchestratorConfig()
    resume_orchestrator = WorkflowRunOrchestrator(
        resume_builder.state, resume_builder._ensure_gateway(), config
    )

    set_initial_fringe(resume_builder.state, resume_orchestrator)
    assert len(resume_builder.state.ready_to_run) == 1
    assert resume_builder.state.ready_to_run[0].task_id == t3.task_id

    # Run a full sync, test that no keyerrors are raised
    synchronize_state(
        resume_builder.state,
        resume_builder._ensure_gateway(),
        resume_orchestrator,
        full_sync=True,
    )
