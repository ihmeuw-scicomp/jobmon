import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from jobmon.client.task import Task
from jobmon.client.workflow_run import WorkflowRun
from jobmon.core.constants import WorkflowRunStatus, TaskStatus, TaskInstanceStatus
from jobmon.core.exceptions import InvalidRequest, InvalidResponse
from jobmon.server.web.models import load_model
from jobmon.server.web.models import task
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType

load_model()


def test_good_names():
    """tests that a few legal names return as valid"""

    assert Task.is_valid_job_name("fred")
    assert Task.is_valid_job_name("fred123")
    assert Task.is_valid_job_name("fred_and-friends")


def test_bad_names():
    """tests that invalid names return a ValueError"""

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("")
    assert "None" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("16")
    assert "digit" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("bad/dog")
    assert "special" in str(exc.value)


def test_equality(task_template):
    """tests that 2 identical tasks are equal and that non-identical tasks
    are not equal"""
    a = task_template.create_task(arg="a")
    a_again = task_template.create_task(arg="a")
    assert a == a_again

    b = task_template.create_task(arg="b", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.node.upstream_nodes) == 1


def test_default_task_name(task_template):
    """test that name based on hash"""
    # noral case
    a = task_template.create_task(arg="a")
    assert a.name == "simple_template_arg-a"
    # long name
    a = task_template.create_task(arg="a" * 256)
    assert a.name == ("simple_template_arg-" + "a" * 256)[0:249]
    # special char
    a = task_template.create_task(arg="abc'abc/abc")
    assert a.name == "simple_template_arg-abc_abc_abc"
    # spaces
    a = task_template.create_task(arg="echo 10")
    assert a.name == "simple_template_arg-echo_10"


def test_task_attribute(db_engine, tool):
    """Test that you can add task attributes to Bash and Python tasks"""

    workflow1 = tool.create_workflow(name="test_task_attribute")
    task_template = tool.active_task_templates["simple_template"]
    task1 = task_template.create_task(
        arg="sleep 2",
        task_attributes={"LOCATION_ID": 1, "AGE_GROUP_ID": 5, "SEX": 1},
        cluster_name="sequential",
        compute_resources={"queue": "null.q"},
    )
    task2 = task_template.create_task(
        arg="sleep 3",
        task_attributes=["NUM_CORES", "NUM_YEARS"],
        cluster_name="sequential",
        compute_resources={"queue": "null.q"},
    )

    task3 = task_template.create_task(
        arg="sleep 4",
        task_attributes={"NUM_CORES": 3, "NUM_YEARS": 5},
        cluster_name="sequential",
        compute_resources={"queue": "null.q"},
    )
    workflow1.add_tasks([task1, task2, task3])
    workflow1.bind()
    assert workflow1.workflow_id is not None
    workflow1._bind_tasks()
    for t in [task1, task2, task3]:
        assert t.task_id is not None
    client_wfr = WorkflowRun(workflow1.workflow_id)
    client_wfr.bind()

    with Session(bind=db_engine) as session:
        select_stmt = (
            select(TaskAttribute.value, TaskAttributeType.name, TaskAttributeType.id)
            .join_from(
                TaskAttribute,
                TaskAttributeType,
                TaskAttribute.task_attribute_type_id == TaskAttributeType.id,
            )
            .where(
                TaskAttribute.task_id.in_([task1.task_id, task2.task_id, task3.task_id])
            )
            .order_by(TaskAttributeType.name, TaskAttribute.task_id)
        )
        resp = session.execute(select_stmt).all()
        values = [tup[0] for tup in resp]
        names = [tup[1] for tup in resp]

        expected_vals = ["5", "1", None, "3", None, "5", "1"]
        expected_names = [
            "AGE_GROUP_ID",
            "LOCATION_ID",
            "NUM_CORES",
            "NUM_CORES",
            "NUM_YEARS",
            "NUM_YEARS",
            "SEX",
        ]

        assert values == expected_vals
        assert names == expected_names
        num_cores_set = set()
        num_years_set = set()
        for item in resp:
            if item[1] == "NUM_CORES":
                num_cores_set.add(item[2])
            if item[1] == "NUM_YEARS":
                num_years_set.add(item[2])
        assert len(num_years_set) == 1
        assert len(num_cores_set) == 1


def test_compute_resource_copy(tool, task_template):
    """test that 1 compute resources object passed to multiple tasks are distinct objects."""
    compute_resources = {
        "m_mem_free": "1G",
        "max_runtime_seconds": 60,
        "num_cores": 1,
        "queue": "all.q",
    }

    task1 = task_template.create_task(
        name="foo", arg="echo foo", compute_resources=compute_resources
    )
    task2 = task_template.create_task(
        name="bar", arg="echo bar", compute_resources=compute_resources
    )

    # Ensure memory addresses are different
    assert id(task1.compute_resources) != id(task2.compute_resources)


def test_reset_attempts_on_resume(db_engine, tool):
    """test that num attempts gets reset on a resume"""

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts

    # setup workflow 1
    workflow1 = tool.create_workflow(name="test_reset_attempts_on_resume")
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 5")
    workflow1.add_task(task_a)

    # add workflow to database
    workflow1.bind()
    assert workflow1.workflow_id is not None
    workflow1._bind_tasks()
    assert task_a.task_id is not None
    wfr_1 = WorkflowRun(workflow1.workflow_id)
    wfr_1.bind()
    assert wfr_1.workflow_run_id is not None
    wfr_1._update_status(WorkflowRunStatus.BOUND)
    wfr_1._update_status(WorkflowRunStatus.ERROR)

    # now set everything to error fail
    with Session(bind=db_engine) as session:
        session.execute(
            text(
                """
                UPDATE task
                SET status='{s}', num_attempts=3, max_attempts=3
                WHERE task.id={task_id}""".format(
                    s=TaskStatus.ERROR_FATAL, task_id=task_a.task_id
                )
            )
        )

    # create a second workflow and actually run it
    workflow2 = tool.create_workflow(
        name="test_reset_attempts_on_resume", workflow_args=workflow1.workflow_args
    )
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 5")
    workflow2.add_task(task_a)
    workflow2.bind()
    workflow2._bind_tasks()

    # Validate that the database indicates the Dag and its Jobs are complete
    with Session(bind=db_engine) as session:
        t = session.get(task.Task, task_a.task_id)
        assert t.max_attempts == 3
        assert t.num_attempts == 0
        assert t.status == TaskStatus.REGISTERING


@pytest.mark.skip(reason="check constraints on sqlite not enabled currently")
def test_binding_length(db_engine, client_env, tool):
    """Test that mysql exceptions return the appropriate error code."""

    # Test that args/attributes that are too long return sensible errors
    tt = tool.get_task_template(
        template_name="test_tt",
        command_template="{narg} {targ}",
        node_args=["narg"],
        task_args=["targ"],
    )
    # Task 1: too long task args (3 * 683 = 2049, max length=2048)
    task1 = tt.create_task(name="foo", narg="abc", targ="def" * 683)
    wf = tool.create_workflow()
    wf.add_task(task1)
    wf.bind()
    assert wf.workflow_id is not None
    with pytest.raises(InvalidRequest) as resp:
        wf._bind_tasks()
        assert task1.task_id is not None
    exc_msg = resp.value.args[0]
    assert "Client error with status code 400" in exc_msg

    # task2: super long attributes
    task2 = tt.create_task(
        name="foo", narg="abc", targ="def", task_attributes={"hello": "world" * 60}
    )
    wf2 = tool.create_workflow()
    wf2.add_task(task2)
    wf2.bind()
    assert wf2.workflow_id is not None
    with pytest.raises(InvalidRequest) as resp2:
        wf2._bind_tasks()
        assert task2.task_id is not None
    exc_msg = resp2.value.args[0]
    assert "Task attributes are constrained to 255 characters" in exc_msg
    assert "Client error with status code 400" in exc_msg


def test_binding_tasks(db_engine, client_env, tool):
    tt = tool.get_task_template(
        template_name="test_tt",
        command_template="{arg1} {arg2} {arg3}",
        node_args=["arg1"],
        task_args=["arg2", "arg3"],
    )
    task1 = tt.create_task(
        name="foo", task_attributes={"aa": "a"}, arg1="abc", arg2="def", arg3="ghi"
    )
    wf = tool.create_workflow()
    wf.add_task(task1)
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    assert task1.task_id is not None
    # verify the task is correctly bind, so are the args
    assert task1.task_id is not None
    with Session(bind=db_engine) as session:
        # verify attribute
        mysql = f"SELECT value FROM task_attribute WHERE task_id={task1.task_id}"
        rows = session.execute(text(mysql)).fetchall()
        assert len(rows[0]) == 1
        assert rows[0][0] == "a"
        # verify args
        mysql = f"SELECT val FROM task_arg WHERE task_id={task1.task_id}"
        rows = session.execute(text(mysql)).fetchall()
        print(f"!!!!!!!!!!!!!{rows}")
        result_set = {rows[0][0], rows[1][0]}
        assert result_set == {"def", "ghi"}


def test_default_max_attemps(db_engine, client_env, tool):
    # test task level default
    tt = tool.get_task_template(
        template_name="test_tt1",
        command_template="true|| abc {arg1} {arg2}",
        node_args=["arg1"],
        task_args=["arg2"],
    )

    task1 = tt.create_task(
        name="task1",
        arg1="arg1_1",
        arg2="arg2_1",
        max_attempts=2,
    )
    task2 = tt.create_task(
        name="task2",
        arg1="arg1_2",
        arg2="arg2_2",
    )
    wf = tool.create_workflow()
    wf.add_tasks([task1, task2])

    assert task1.max_attempts == 2
    assert task2.max_attempts == 3

    # test task level default
    task3 = tt.create_task(
        name="task3",
        arg1="arg1_3",
        arg2="arg2_3",
    )
    assert tt.default_max_attempts == None
    wf2 = tool.create_workflow(default_max_attempts=1000)
    assert wf2.default_max_attempts == 1000
    wf2.add_task(task3)
    assert task3.max_attempts == 1000

    # test tt level default
    tt2 = tool.get_task_template(
        template_name="test_tt1",
        command_template="true|| def {arg1} {arg2}",
        node_args=["arg1"],
        task_args=["arg2"],
    )
    tt2.set_default_max_attempts(7)
    task4 = tt2.create_task(
        name="task4",
        arg1="arg1_4",
        arg2="arg2_4",
    )
    wf3 = tool.create_workflow()
    assert wf3.default_max_attempts == None
    wf3.add_task(task4)
    assert task4.max_attempts == 3

    # test tool level default
    tool.set_default_max_attempts(17)
    assert tool.default_max_attempts == 17
    tt3 = tool.get_task_template(
        template_name="test_tt1",
        command_template="true|| ghi {arg1} {arg2}",
        node_args=["arg1"],
        task_args=["arg2"],
    )
    assert tt3.default_max_attempts == 17
    task5 = tt2.create_task(
        name="task5",
        arg1="arg1_5",
        arg2="arg2_5",
    )
    wf4 = tool.create_workflow()
    assert wf4.default_max_attempts == 17
    wf4.add_task(task5)
    assert task5.max_attempts == 17

    # test wf always have a default max attempts so existing code works
    wf5 = tool.create_workflow(default_max_attempts=None)
    assert wf5.default_max_attempts == tool.default_max_attempts is not None


def test_downstream_task(client_env, tool, db_engine):
    """Test case to verify the downstream and the upstream tasks."""
    wf = tool.create_workflow()
    tt = tool.get_task_template(
        template_name="test_tt",
        command_template="{arg1} {arg2}",
        node_args=["arg1"],
        task_args=["arg2"],
    )
    task1 = tt.create_task(name="task1", arg1="abc1", arg2="def")
    task2 = tt.create_task(
        name="task2", arg1="abc2", arg2="def", upstream_tasks=[task1]
    )
    task3 = tt.create_task(
        name="task3", arg1="abc3", arg2="def", upstream_tasks=[task1]
    )
    wf.add_tasks([task1, task2, task3])
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    assert task1.task_id is not None
    assert task2.task_id is not None
    assert task3.task_id is not None
    # use the /task/get_downstream_tasks endpoint to verify the downstream tasks
    with Session(bind=db_engine) as session:
        # verify edge
        import re

        res = session.execute(
            text(
                f"select downstream_node_ids from task, edge where task.id={task1.task_id} and task.node_id=edge.node_id"
            )
        ).fetchall()
        assert len(res) == 1
        two_id_patten = r"^\"\[\s*-?\d+(\.\d+)?,\s*-?\d+(\.\d+)?\s*\]\"$"  # '"[1, 2]"'
        assert re.match(two_id_patten, res[0][0])

        one_id_pattern = r"^\"\[\s*-?\d+(\.\d+)?\s*\]\"$"  # '"[1]"'
        res = session.execute(
            text(
                f"select upstream_node_ids from task, edge where task.id={task2.task_id} and task.node_id=edge.node_id"
            )
        ).fetchall()
        assert len(res) == 1
        assert re.match(one_id_pattern, res[0][0])
        res = session.execute(
            text(
                f"select upstream_node_ids from task, edge where task.id={task3.task_id} and task.node_id=edge.node_id"
            )
        ).fetchall()
        assert len(res) == 1
        assert re.match(one_id_pattern, res[0][0])
