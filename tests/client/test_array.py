import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.client.array import Array
from jobmon.client.tool import Tool


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
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


@pytest.fixture
def task_template_dummy(tool):
    tt = tool.get_task_template(
        template_name="dummy_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


def test_create_array(task_template):
    tasks = task_template.create_tasks(arg="echo 1")
    array = tasks[0].array
    assert (
        array.compute_resources
        == task_template.default_compute_resources_set["sequential"]
    )
    # test assigned name
    assert "simple_template" in array.name
    # test given name
    tasks = task_template.create_tasks(name="test_array", arg="echo 2")
    array = tasks[0].array
    assert "test_array" in array.name


def test_array_bind(db_engine, client_env, task_template_dummy, tool):
    task_template = task_template_dummy

    tasks = task_template.create_tasks(
        arg="echo 10", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    for t in tasks:
        assert t.task_id is not None

    assert hasattr(wf.arrays["dummy_template"], "_array_id")

    with Session(bind=db_engine) as session:
        array_stmt = """
        SELECT workflow_id, task_template_version_id 
        FROM array
        WHERE id = {}
        """.format(
            wf.arrays["dummy_template"].array_id
        )

        array_db = session.execute(text(array_stmt)).fetchone()
        session.commit()

        assert array_db.workflow_id == wf.workflow_id
        assert (
            array_db.task_template_version_id
            == task_template.active_task_template_version.id
        )

    # Assert the bound task has the correct array ID
    with Session(bind=db_engine) as session:
        task_query = """
        SELECT array_id
        FROM task
        WHERE id = {}
        """.format(
            list(wf.arrays["dummy_template"].tasks.values())[0].task_id
        )
        task = session.execute(text(task_query)).fetchone()

        assert task.array_id == wf.arrays["dummy_template"].array_id


def test_node_args_expansion():
    node_args = {"location_id": [1, 2, 3], "sex": ["m", "f"]}

    expected_expansion = [
        {"location_id": 1, "sex": "m"},
        {"location_id": 1, "sex": "f"},
        {"location_id": 2, "sex": "m"},
        {"location_id": 2, "sex": "f"},
        {"location_id": 3, "sex": "m"},
        {"location_id": 3, "sex": "f"},
    ]

    node_arg_generator = Array.expand_dict(**node_args)
    combos = []
    for node_arg in node_arg_generator:
        assert node_arg in expected_expansion
        assert node_arg not in combos
        combos.append(node_arg)

    assert len(combos) == len(expected_expansion)


def test_create_tasks(db_engine, client_env, tool):
    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{command} {task_arg} {narg1} {narg2} {op_arg}",
        node_args=["narg1", "narg2"],
        task_args=["task_arg"],
        op_args=["command", "op_arg"],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )

    tasks = rich_task_template.create_tasks(
        command="foo",
        task_arg="bar",
        narg1=[1, 2, 3],
        narg2=["a", "b", "c"],
        op_arg="baz",
        compute_resources={"queue": "null.q"},
    )

    assert len(tasks) == 9  # Created on init
    wf = tool.create_workflow()
    wf.add_tasks(tasks)

    assert len(wf.tasks) == 9  # Tasks bound to workflow

    # Assert tasks templated correctly
    commands = [t.command for t in tasks]
    assert "foo bar 1 c baz" in commands
    assert "foo bar 3 a baz" in commands

    # Check node and task args are recorded in the proper tables
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    for t in tasks:
        assert t.task_id is not None

    with Session(bind=db_engine) as session:
        # Check narg1 and narg2 are represented in node_arg
        q = """
        SELECT * 
        FROM arg
        JOIN node_arg na ON na.arg_id = arg.id
        WHERE arg.name IN ('narg1', 'narg2')
        """
        res = session.execute(text(q)).fetchall()
        session.commit()

        assert len(res) == 18  # 2 args per node * 9 nodes
        names = [r.name for r in res]
        assert set(names) == {"narg1", "narg2"}

        # Check task_arg in the task arg table
        task_q = """
        SELECT * 
        FROM arg
        JOIN task_arg ta ON ta.arg_id = arg.id
        WHERE arg.name IN ('task_arg')"""
        task_args = session.execute(text(task_q)).fetchall()
        session.commit()

        assert len(task_args) == 9  # 9 unique tasks, 1 task_arg each
        task_arg_names = [r.name for r in task_args]
        assert set(task_arg_names) == {"task_arg"}

    # Define individual node_args, and expect to get a list of one task
    three_c_node_args = {"narg1": 3, "narg2": "c"}
    three_c_tasks = wf.get_tasks_by_node_args("simple_template", **three_c_node_args)
    assert len(three_c_tasks) == 1

    # Define out of scope node_args, and expect to get a empty list
    x_node_args = {"narg1": 3, "narg2": "x"}
    x_tasks = wf.get_tasks_by_node_args("simple_template", **x_node_args)
    assert len(x_tasks) == 0

    # Define narg1 only, expect to see a list of 3 tasks
    three_node_args = {"narg1": 3}
    three_tasks = wf.get_tasks_by_node_args("simple_template", **three_node_args)
    assert len(three_tasks) == 3

    # Define an empty dict, expect to see a list of 9 tasks
    empty_node_args = {}
    all_tasks = wf.get_tasks_by_node_args("simple_template", **empty_node_args)
    assert len(all_tasks) == 9

    # Define a list(3,2 items)-valued case for narg1 and narg2, expect to see 6
    empty_node_args = {"narg1": [1, 2, 3], "narg2": ["a", "b"]}
    all_tasks = wf.get_tasks_by_node_args("simple_template", **empty_node_args)
    assert len(all_tasks) == 6

    # Define a task_template_name in-scope valid node_args, expect to see a list of 3 tasks
    two_node_args = {"narg1": 2}
    two_wf_tasks = wf.get_tasks_by_node_args(
        task_template_name="simple_template", **two_node_args
    )
    assert len(two_wf_tasks) == 3

    # Define a task_template_name out-of-scope node_args, expect to see a list of 3 tasks
    two_node_args = {"narg1": 2}
    with pytest.raises(ValueError):
        two_wf_tasks = wf.get_tasks_by_node_args(
            task_template_name="OUT_OF_SCOPE_simple_template", **two_node_args
        )


def test_empty_array(client_env, tool):
    """Check that an empty array raises the appropriate error."""

    tt = tool.get_task_template(
        template_name="empty",
        command_template="",
        node_args=[],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    tasks = tt.create_tasks()
    assert not tasks


def test_array_max_attempts(client_env, tool):
    """Check the wf default_max_attempts pass down"""
    tt = tool.get_task_template(
        template_name="test_tt1",
        command_template="true|| abc {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    # test default
    tasks1 = tt.create_tasks(arg=[1, 2])
    wf1 = tool.create_workflow()
    wf1.add_tasks(tasks1)
    array1 = tasks1[0].array
    assert array1.max_attempts is None
    assert tasks1[0].max_attempts == tasks1[1].max_attempts == 3
    # test wf pass down
    tasks2 = tt.create_tasks(arg=[10, 20])
    wf2 = tool.create_workflow(default_max_attempts=1000)
    wf2.add_tasks(tasks2)
    array2 = tasks2[0].array
    assert (
        array2.max_attempts == tasks2[0].max_attempts == tasks2[1].max_attempts == 1000
    )
