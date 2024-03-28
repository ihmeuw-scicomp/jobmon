import logging
import random
from typing import List

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.client.task import Task
from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow
from jobmon.client.workflow_run import WorkflowRun, WorkflowRunFactory
from jobmon.core.constants import MaxConcurrentlyRunning, WorkflowRunStatus
from jobmon.core.exceptions import (
    WorkflowAlreadyComplete,
    DuplicateNodeArgsError,
    WorkflowAlreadyExists,
    NodeDependencyNotExistError,
)


def test_wfargs_update(tool):
    """test that 2 workflows with different names, have different ids and tasks"""
    # Create identical dags
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )
    t3 = tool.active_task_templates["phase_3"].create_task(
        arg="sleep 3", upstream_tasks=[t2]
    )

    t4 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t5 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t4]
    )
    t6 = tool.active_task_templates["phase_3"].create_task(
        arg="sleep 3", upstream_tasks=[t5]
    )

    wfa1 = "v1"
    wf1 = tool.create_workflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1.bind()
    wf1._bind_tasks()

    wfa2 = "v2"
    wf2 = tool.create_workflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.bind()
    wf2._bind_tasks()

    # Make sure the second Workflow has a distinct Workflow ID & WorkflowRun ID
    assert wf1.workflow_id != wf2.workflow_id

    # Make sure the second Workflow has a distinct hash
    assert hash(wf1) != hash(wf2)

    # Make sure the second Workflow has a distinct set of Tasks
    wfr1 = WorkflowRun(wf1.workflow_id)
    wfr1.bind()
    wfr2 = WorkflowRun(wf2.workflow_id)
    wfr2.bind()
    assert not (
        set([t.task_id for _, t in wf1.tasks.items()])
        & set([t.task_id for _, t in wf2.tasks.items()])
    )


def test_attempt_resume_on_complete_workflow(tool):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)
    """
    # Create identical dags
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    wf1 = tool.create_workflow(name="attempt_resume_on_completed")
    wf1.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    wf1.bind()
    wf1._bind_tasks()
    wfr1 = WorkflowRun(wf1.workflow_id)
    wfr1.bind()
    wfr1._update_status(WorkflowRunStatus.BOUND)
    wfr1._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)
    wfr1._update_status(WorkflowRunStatus.DONE)

    # second workflow shouldn't be able to start
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    workflow2 = tool.create_workflow(
        wf1.workflow_args, name="attempt_resume_on_completed"
    )
    workflow2.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    with pytest.raises(WorkflowAlreadyComplete):
        workflow2.run()


def test_resume_with_old_and_new_workflow_attributes(tool, db_engine):
    """Should allow a resume, and should not fail on duplicate workflow_attribute keys"""
    from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
    from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType

    # Create identical dags
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    wf1 = tool.create_workflow(
        name="attempt_resume_on_failed",
        workflow_attributes={"location_id": 5, "year": "2019"},
    )
    wf1.add_tasks([t1, t2])

    # bind workflow to db and move to ERROR state
    wf1.bind()
    wf1._bind_tasks()
    factory1 = WorkflowRunFactory(wf1.workflow_id)
    wfr1 = factory1.create_workflow_run()
    wfr1._update_status(WorkflowRunStatus.BOUND)
    wfr1._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)
    wfr1._update_status(WorkflowRunStatus.ERROR)

    # second workflow
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    workflow2 = tool.create_workflow(
        wf1.workflow_args,
        name="attempt_resume_on_failed",
        workflow_attributes={"location_id": 5, "year": "2022", "sex": "F"},
    )
    workflow2.add_tasks([t1, t2])

    # bind workflow to db and run resume
    workflow2.bind()
    workflow2._bind_tasks()
    fact2 = WorkflowRunFactory(workflow2.workflow_id)
    fact2.create_workflow_run()

    # check database entries are populated correctly
    with Session(bind=db_engine) as session:
        wf_attributes = (
            session.query(WorkflowAttributeType.name, WorkflowAttribute.value)
            .join(
                WorkflowAttribute,
                WorkflowAttribute.workflow_attribute_type_id
                == WorkflowAttributeType.id,
            )
            .filter(WorkflowAttribute.workflow_id == wf1.workflow_id)
            .all()
        )
    assert set(wf_attributes) == {("location_id", "5"), ("year", "2022"), ("sex", "F")}


def test_workflow_identical_args(tool, task_template):
    """test that 2 workflows with identical arguments can't exist
    simultaneously"""

    # first workflow bound
    wf1 = tool.create_workflow(workflow_args="same")
    task = task_template.create_task(arg="sleep 1")
    wf1.add_task(task)
    wf1.bind()
    wf1._bind_tasks()
    WorkflowRunFactory(wf1.workflow_id).create_workflow_run()

    # tries to create an identical workflow without the restart flag
    wf2 = tool.create_workflow(workflow_args="same")
    task = task_template.create_task(arg="sleep 1")
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.run()


def test_add_same_node_args_twice(client_env):
    tool = Tool()
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
        op_args=[],
    )
    a = tt.create_task(node_arg="a", task_arg="a")
    b = tt.create_task(node_arg="a", task_arg="b")

    workflow = tool.create_workflow()
    workflow.add_task(a)
    with pytest.raises(DuplicateNodeArgsError):
        workflow.add_task(b)


def test_non_serializable_node_args(tool):
    """Test passing an object (set) that is not JSON serializable to node and task args."""
    workflow = tool.create_workflow(name="numpy_test_wf")
    template = tool.get_task_template(
        template_name="numpy_test_template",
        command_template="echo {node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
    )
    task = template.create_task(node_arg={1, 2}, task_arg={3, 4})
    workflow.add_tasks([task])
    workflow.bind()
    assert workflow.workflow_id


def test_empty_workflow(tool):
    """
    Create a real_dag with no Tasks. Call all the creation methods and check
    that it raises no Exceptions.
    """

    workflow = tool.create_workflow(name="test_empty_real_dag")

    with pytest.raises(RuntimeError):
        workflow.run()


def test_workflow_attribute(db_engine, tool, client_env, task_template):
    """Test the workflow attributes feature"""
    from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
    from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType

    wf1 = tool.create_workflow(
        name="test_wf_attributes",
        workflow_attributes={"location_id": 5, "year": 2019, "sex": 1},
    )

    # Check the workflow has a tool property
    assert wf1.tool == tool

    t1 = task_template.create_task(arg="exit -0")
    wf1.add_task(t1)
    wf1.bind()

    # check database entries are populated correctly
    with Session(bind=db_engine) as session:
        wf_attributes = (
            session.query(WorkflowAttributeType.name, WorkflowAttribute.value)
            .join(
                WorkflowAttribute,
                WorkflowAttribute.workflow_attribute_type_id
                == WorkflowAttributeType.id,
            )
            .filter(WorkflowAttribute.workflow_id == wf1.workflow_id)
            .all()
        )
    assert set(wf_attributes) == {("location_id", "5"), ("year", "2019"), ("sex", "1")}

    # Add and update attributes
    wf1.add_attributes({"age_group_id": 1, "sex": 2})

    with Session(bind=db_engine) as session:
        wf_attributes = (
            session.query(WorkflowAttributeType.name, WorkflowAttribute.value)
            .join(
                WorkflowAttribute,
                WorkflowAttribute.workflow_attribute_type_id
                == WorkflowAttributeType.id,
            )
            .filter(WorkflowAttribute.workflow_id == wf1.workflow_id)
            .all()
        )
    assert set(wf_attributes) == {
        ("location_id", "5"),
        ("year", "2019"),
        ("sex", "2"),
        ("age_group_id", "1"),
    }

    # Test workflow w/o attributes
    wf2 = tool.create_workflow(
        name="test_empty_wf_attributes",
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    wf2.add_task(t1)
    wf2.bind()

    with Session(bind=db_engine) as session:
        wf_attributes = (
            session.query(WorkflowAttribute)
            .filter_by(workflow_id=wf2.workflow_id)
            .all()
        )

    assert wf_attributes == []


def test_chunk_size(tool, client_env, task_template):
    wf_a = tool.create_workflow(name="test_wf_chunks_a", chunk_size=3)

    task_a = task_template.create_task(arg="echo a", upstream_tasks=[])  # To be clear
    wf_a.add_task(task_a)
    wf_a.bind()

    wf_b = tool.create_workflow(name="test_wf_chunks_b", chunk_size=10)
    task_b = task_template.create_task(arg="echo b", upstream_tasks=[])  # To be clear
    wf_b.add_task(task_b)
    wf_b.bind()

    assert wf_a._chunk_size == 3
    assert wf_b._chunk_size == 10


def test_add_tasks_dependencynotexist(tool, client_env, task_template):
    t1 = task_template.create_task(arg="echo 1")
    t2 = task_template.create_task(arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")
    t3.add_upstream(t2)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = tool.create_workflow(name="TestWF1")
        wf.add_tasks([t1, t2])
        wf.bind()
    assert "Downstream" in str(excinfo.value)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = tool.create_workflow(name="TestWF2")
        wf.add_tasks([t1, t3])
        wf.bind()
    assert "Upstream" in str(excinfo.value)
    wf = tool.create_workflow(name="TestWF3")
    wf.add_tasks([t1, t2, t3])
    wf.bind()
    assert len(wf.tasks) == 3
    wf = tool.create_workflow(name="TestWF4")
    wf.add_tasks([t1])
    wf.add_tasks([t2])
    wf.add_tasks([t3])
    wf.bind()
    assert len(wf.tasks) == 3


@pytest.mark.skip()
def test_workflow_validation(tool, task_template, caplog):
    """Test the workflow.validate() function, and ensure idempotency"""
    too_many_cores = {"cores": 1000, "queue": "null.q", "runtime": "01:02:33"}
    good_resources = {"cores": 20, "queue": "null.q", "runtime": "01:02:33"}
    t1 = task_template.create_task(
        arg="echo 1", compute_resources=too_many_cores, cluster_name="multiprocess"
    )
    wf1 = tool.create_workflow()
    wf1.add_task(t1)

    with pytest.raises(ValueError):
        wf1.validate(
            raise_on_error=True
        )  # Max cores on multiprocess null.q is 20. Should fail

    # Without fail set, validate and check coercion
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="jobmon.client"):
        wf1.validate(strict=True)
        assert (
            "Failed validation, reasons: ResourceError: provided cores 1000 exceeds queue"
            in caplog.records[-1].message
        )

    # Try again for idempotency
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="jobmon.client"):
        wf1.validate(strict=True)
        assert (
            "Failed validation, reasons: ResourceError: provided cores 1000 exceeds queue"
            in caplog.records[-1].message
        )

    # Try with valid resources
    t2 = task_template.create_task(
        arg="echo 1", compute_resources=good_resources, cluster_name="multiprocess"
    )
    wf2 = tool.create_workflow()
    wf2.add_task(t2)
    wf2.validate()

    # Test that a validate call fails if a different DAG is bound.
    # Bind wf1, and create a new workflow with the same args but a different DAG.
    wf1.bind()
    wf3 = tool.create_workflow(workflow_args=wf1.workflow_args)
    t3 = task_template.create_task(arg="echo 3")
    wf3.add_task(t3)
    with pytest.raises(WorkflowAlreadyExists):
        wf3._matching_wf_args_diff_hash()
    with pytest.raises(WorkflowAlreadyExists):
        wf3.bind()


def test_workflow_get_errors(tool, task_template, db_engine):
    """test that num attempts gets reset on a resume."""

    from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
    from jobmon.server.web.models.task_status import TaskStatus
    from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus

    # setup workflow 1
    workflow1 = tool.create_workflow(name="test_workflow_get_errors")
    task_a = task_template.create_task(arg="sleep 5", max_attempts=1)
    workflow1.add_task(task_a)
    task_b = task_template.create_task(arg="sleep 6", max_attempts=1)
    workflow1.add_task(task_b)

    # add workflow to database
    workflow1.bind()
    workflow1._bind_tasks()
    wfr_1 = WorkflowRunFactory(workflow1.workflow_id).create_workflow_run()

    # for a just initialized task, get_errors() should be None
    assert task_a.get_errors() is None

    # now set everything to error fail
    with Session(bind=db_engine) as session:
        # fake workflow run
        session.execute(
            text(
                """
                UPDATE workflow_run
                SET status ='{s}'
                WHERE id={wfr_id}""".format(
                    s=WorkflowRunStatus.RUNNING, wfr_id=wfr_1.workflow_run_id
                )
            )
        )
        session.execute(
            text(
                """
                INSERT INTO task_instance (workflow_run_id, task_id, status)
                VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                    wfr_id=wfr_1.workflow_run_id,
                    t_id=task_a.task_id,
                    s=TaskInstanceStatus.LAUNCHED,
                )
            )
        )
        ti = session.execute(
            text("SELECT id from task_instance where task_id={}".format(task_a.task_id))
        ).fetchone()
        ti_id_a = ti[0]
        session.execute(
            text(
                """
                UPDATE task
                SET status ='{s}'
                WHERE id={t_id}""".format(
                    s=TaskStatus.INSTANTIATING, t_id=task_a.task_id
                )
            )
        )
        session.execute(
            text(
                """
                INSERT INTO task_instance (workflow_run_id, task_id, status)
                VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                    wfr_id=wfr_1.workflow_run_id,
                    t_id=task_b.task_id,
                    s=TaskInstanceStatus.LAUNCHED,
                )
            )
        )
        ti = session.execute(
            text("SELECT id FROM task_instance WHERE task_id={}".format(task_b.task_id))
        ).fetchone()
        ti_id_b = ti[0]
        session.execute(
            text(
                """
                UPDATE task
                SET status ='{s}'
                WHERE id={t_id}""".format(
                    s=TaskStatus.INSTANTIATING, t_id=task_b.task_id
                )
            )
        )
        session.commit()

    # log task_instance fatal error for task_a
    app_route = f"/task_instance/{ti_id_a}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={
            "error_state": TaskInstanceStatus.ERROR,
            "error_description": "bla bla bla",
        },
        request_type="post",
    )
    assert return_code == 200

    # log task_instance fatal error - 2nd error for task_a
    app_route = f"/task_instance/{ti_id_a}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_description": "ble ble ble"},
        request_type="post",
    )
    assert return_code == 200

    # log task_instance fatal error for task_b
    app_route = f"/task_instance/{ti_id_b}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_description": "cla cla cla"},
        request_type="post",
    )
    assert return_code == 200

    # make sure we see the 2 tasks in the workflow_errors(task_a and task_b)
    # and task_b one has 1 task_instance_error_log
    workflow_errors = workflow1.get_errors()
    assert type(workflow_errors) == dict
    assert len(workflow_errors) == 2
    task_b_errors = workflow_errors[task_b.task_id]
    assert task_b_errors["task_instance_id"] == ti_id_b
    error_log_b = task_b_errors["error_log"]
    assert type(error_log_b) == list
    assert len(error_log_b) == 1
    err_1st_b = error_log_b[0]
    assert type(err_1st_b) == dict
    assert err_1st_b["description"] == "cla cla cla"


def test_concurrency_limit(client_env, db_engine):
    """The max_concurrently_running should be the biggest of wf and its arrays' size."""

    # no array
    # should be the default value MaxConcurrentlyRunning
    tool = Tool("i_am_a_new_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="dummy", compute_resources={"queue": "null.q"}
    )
    tt = tool.get_task_template(
        template_name="tt",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    workflow1 = tool.create_workflow(name="test_1")
    assert (
        workflow1.max_concurrently_running
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    task = tt.create_task(arg="no", max_attempts=1)
    workflow1.add_task(task)
    workflow1.bind()
    workflow1._bind_tasks()
    assert (
        workflow1.max_concurrently_running
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    # verify server side
    with Session(bind=db_engine) as session:
        sql = f"""
        SELECT max_concurrently_running 
        FROM workflow
        WHERE id={workflow1.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
        sql = f"""
        SELECT max_concurrently_running 
        FROM array 
        WHERE workflow_id={workflow1.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING

    # array
    # the array max_concurrently_running
    workflow2 = tool.create_workflow(name="test_2")
    assert (
        workflow2.max_concurrently_running
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    tt2 = tool.get_task_template(
        template_name="tt",
        command_template="echoo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    temp_args = [f"array1-{i}" for i in range(19)]
    tasks2 = tt2.create_tasks(arg=temp_args)
    workflow2.add_tasks(tasks2)
    workflow2.bind()
    workflow2._bind_tasks()
    assert (
        workflow2.max_concurrently_running
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    # verify server side
    with Session(bind=db_engine) as session:
        sql = f"""
            SELECT max_concurrently_running 
            FROM workflow
            WHERE id={workflow2.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
        sql = f"""
                SELECT max_concurrently_running 
                FROM array 
                WHERE workflow_id={workflow2.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING

    # array level user setting
    workflow3 = tool.create_workflow(name="test_3")
    assert (
        workflow3.max_concurrently_running
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    tt31 = tool.get_task_template(
        template_name="tt",
        command_template="a {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tt32 = tool.get_task_template(
        template_name="tt2",
        command_template="b {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    temp_args = [f"array2-{i}" for i in range(20)]
    tasks3_1 = tt31.create_tasks(arg=temp_args, max_concurrently_running=20)
    workflow3.add_tasks(tasks3_1)
    temp_args = [f"array3-{i}" for i in range(40)]
    tasks3_2 = tt32.create_tasks(arg=temp_args, max_concurrently_running=40)
    workflow3.add_tasks(tasks3_2)
    workflow3.bind()
    workflow3._bind_tasks()
    assert (
        workflow3.max_concurrently_running
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    # verify server side
    with Session(bind=db_engine) as session:
        sql = f"""
                SELECT max_concurrently_running 
                FROM workflow
                WHERE id={workflow3.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
        sql = f"""
                        SELECT max_concurrently_running 
                        FROM array 
                        WHERE workflow_id={workflow3.workflow_id}"""
        rows = session.execute(text(sql)).fetchall()
        for r in rows:
            assert r[0] in [20, 40]

    # workflow level max_concurrently_running
    # the workflow max_concurrently_running should not be overwrite
    workflow4 = tool.create_workflow(name="test_4", max_concurrently_running=23)
    assert workflow4.max_concurrently_running == 23
    tt4 = tool.get_task_template(
        template_name="tt",
        command_template="c {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    temp_args = [f"array4-{i}" for i in range(46)]
    tasks4 = tt4.create_tasks(arg=temp_args)
    workflow4.add_tasks(tasks4)
    workflow4.bind()
    workflow4._bind_tasks()
    assert workflow4.max_concurrently_running == 23
    # verify server side
    with Session(bind=db_engine) as session:
        sql = f"""
                SELECT max_concurrently_running 
                FROM workflow
                WHERE id={workflow4.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == 23
        sql = f"""
                        SELECT max_concurrently_running 
                        FROM array 
                        WHERE workflow_id={workflow4.workflow_id}"""
        r = session.execute(text(sql)).fetchone()
        assert r[0] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING


class TestDAGCycles:
    def create_workflow(self, tool: Tool) -> Workflow:
        return tool.create_workflow(
            name="test_workflow",
            default_cluster_name="dummy",
            workflow_args=f"dummy_wf_{random.randint(0, 100_000_000)}",
        )

    def create_tasks(self, tool: Tool, num_tasks: int) -> List[Task]:
        task_template = tool.get_task_template(
            template_name="dummy_task",
            command_template="sleep {task_number}",
            node_args=["task_number"],
            op_args=[],
            task_args=[],
        )
        return [
            task_template.create_task(
                task_number=i,
                name=f"task_{i}",
                compute_resources={
                    "memory": "1G",
                    "cores": 1,
                    "runtime": "1m",
                    "queue": "null.q",
                    "project": "dummy_proj",
                    "stderr": "/tmp/errors",
                    "stdout": "/tmp/output",
                },
            )
            for i in range(num_tasks)
        ]

    def test_simple(self, tool: Tool) -> None:
        """Ensure an error is raised with a simple cycle like:
        t0 -> t1 -> t2
        ^------------'
        """
        # Create workflow and tasks
        wf = self.create_workflow(tool)
        t0, t1, t2 = self.create_tasks(tool, 3)

        # Set the dependencies between the tasks creating a cycle, and verify the cycle
        # was created correctly:
        # t0 -> t1 -> t2
        #  ^----------'
        t0.add_downstream(t1)
        t1.add_downstream(t2)
        t2.add_downstream(t0)

        # t2 <- t0 -> t1
        assert t0.upstream_tasks == {t2}
        assert t0.downstream_tasks == {t1}

        # t0 <- t1 -> t2
        assert t1.upstream_tasks == {t0}
        assert t1.downstream_tasks == {t2}

        # t1 <- t2 -> t0
        assert t2.upstream_tasks == {t1}
        assert t2.downstream_tasks == {t0}

        # Add the tasks to the workflow
        wf.add_tasks([t0, t1, t2])

        # Exercise & Verify an error is raised
        with pytest.raises(Exception, match="Cycle detected in the task graph"):
            wf.bind()

    def test_midway(self, tool: Tool) -> None:
        """Ensure an error is raised with a cycle like:
        t0 -> t1 -> t2 -> t3 -> t4
               ^-----------'
        """
        # Create workflow and tasks
        wf = self.create_workflow(tool)
        t0, t1, t2, t3, t4 = self.create_tasks(tool, 5)

        # Set the dependencies between the tasks creating a cycle, and verify the cycle
        # was created correctly:
        # t0 -> t1 -> t2 -> t3 -> t4
        #        ^-----------'
        t0.add_downstream(t1)
        t1.add_downstream(t2)
        t2.add_downstream(t3)
        t3.add_downstream(t4)
        t3.add_downstream(t1)

        # t0 -> t1
        assert t0.downstream_tasks == {t1}
        assert t0.upstream_tasks == set()

        # t0 <- t1 -> t2
        #        ^
        #       t3
        assert t1.upstream_tasks == {t0, t3}
        assert t1.downstream_tasks == {t2}

        # t1 <- t2 -> t3
        assert t2.upstream_tasks == {t1}
        assert t2.downstream_tasks == {t3}

        #       t1
        #       ^
        # t2 <- t3 -> t4
        assert t3.upstream_tasks == {t2}
        assert t3.downstream_tasks == {t1, t4}

        # t3 <- t4
        assert t4.upstream_tasks == {t3}

        # Add the tasks to the workflow
        wf.add_tasks([t0, t1, t2, t3, t4])

        # Exercise & Verify an error is raised
        with pytest.raises(Exception, match="Cycle detected in the task graph"):
            wf.bind()

    def test_fork_join(self, tool: Tool) -> None:
        r"""Ensure an error is raised with a cycle like:
               - t0
             /  /  \
            |  t1  t2
            |   \  /
             `---t3
        """
        # Create workflow and tasks
        wf = self.create_workflow(tool)
        t0, t1, t2, t3 = self.create_tasks(tool, 4)

        # Set the dependencies between the tasks creating a cycle, and verify the cycle
        # was created correctly:
        #        - t0
        #      /  /  \
        #     |  t1  t2
        #     |   \  /
        #      `---t3
        t0.add_downstream(t1)
        t0.add_downstream(t2)
        t1.add_downstream(t3)
        t2.add_downstream(t3)
        t3.add_downstream(t0)

        #     ,-> t1
        #    t0
        #     `-> t2
        assert t0.downstream_tasks == {t1, t2}

        # t1 -> t3
        # t2 ---^
        assert t1.downstream_tasks == {t3}
        assert t2.downstream_tasks == {t3}

        # t3 -> t0
        assert t3.downstream_tasks == {t0}

        # Add the tasks to the workflow
        wf.add_tasks([t0, t1, t2, t3])

        # Exercise & Verify an error is raised
        with pytest.raises(Exception, match="Cycle detected in the task graph"):
            wf.bind()

    def test_subtree(self, tool: Tool) -> None:
        r"""Ensure an error is raised with a cycle like:
               --  t0
             /    /  \
            |   t1    t2
            | /  \   /  \
             t3  t4 t5  t6
                       /  \
                      t7  t8
        """
        # Create workflow and tasks
        wf = self.create_workflow(tool)
        t0, t1, t2, t3, t4, t5, t6, t7, t8 = self.create_tasks(tool, 9)

        # Set the dependencies between the tasks creating a cycle, and verify the cycle
        # was created correctly:
        #       --  t0
        #     /    /  \
        #    |   t1    t2
        #    | /  \   /  \
        #     t3  t4 t5  t6
        #               /  \
        #              t7  t8
        t0.add_downstream(t1)
        t0.add_downstream(t2)
        t1.add_downstream(t3)
        t1.add_downstream(t4)
        t2.add_downstream(t5)
        t2.add_downstream(t6)
        t6.add_downstream(t7)
        t6.add_downstream(t8)
        t3.add_downstream(t0)

        #     ,-> t1
        #    t0
        #     `-> t2
        assert t0.downstream_tasks == {t1, t2}

        #     ,-> t3
        #    t1
        #     `-> t4
        assert t1.downstream_tasks == {t3, t4}

        #     ,-> t5
        #    t2
        #     `-> t6
        assert t2.downstream_tasks == {t5, t6}

        #     ,-> t7
        #    t6
        #     `-> t8
        assert t6.downstream_tasks == {t7, t8}

        # t3 -> t0
        assert t3.downstream_tasks == {t0}

        # Add the tasks to the workflow
        wf.add_tasks([t0, t1, t2, t3, t4, t5, t6, t7, t8])

        # Exercise & Verify an error is raised
        with pytest.raises(Exception, match="Cycle detected in the task graph"):
            wf.bind()

    def test_no_cycle(self, tool: Tool) -> None:
        r"""Ensure no error is raised with a DAG that has no cycles.

                   t0
                  /  \
                t1    t2
              /  \   /  \
             t3  t4 t5  t6
                       /  \
                      t7  t8
        """
        # Create workflow and tasks
        wf = self.create_workflow(tool)
        t0, t1, t2, t3, t4, t5, t6, t7, t8 = self.create_tasks(tool, 9)

        # Set the dependencies between the tasks creating a cycle, and verify the cycle
        # was created correctly:
        #           t0
        #          /  \
        #        t1    t2
        #      /  \   /  \
        #     t3  t4 t5  t6
        #               /  \
        #              t7  t8
        t0.add_downstream(t1)
        t0.add_downstream(t2)
        t1.add_downstream(t3)
        t1.add_downstream(t4)
        t2.add_downstream(t5)
        t2.add_downstream(t6)
        t6.add_downstream(t7)
        t6.add_downstream(t8)

        #     ,-> t1
        #    t0
        #     `-> t2
        assert t0.downstream_tasks == {t1, t2}

        #     ,-> t3
        #    t1
        #     `-> t4
        assert t1.downstream_tasks == {t3, t4}

        #     ,-> t5
        #    t2
        #     `-> t6
        assert t2.downstream_tasks == {t5, t6}

        #     ,-> t7
        #    t6
        #     `-> t8
        assert t6.downstream_tasks == {t7, t8}

        # Add the tasks to the workflow
        wf.add_tasks([t0, t1, t2, t3, t4, t5, t6, t7, t8])

        # Exercise & Verify an error **IS NOT** raised
        wf.bind()
