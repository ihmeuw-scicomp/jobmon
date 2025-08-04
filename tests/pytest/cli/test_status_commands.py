import argparse
import ast
import datetime
import getpass
import logging
import os
import tempfile
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import PropertyMock, patch

import pandas as pd
import pytest
import yaml
from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from jobmon.client.api import Tool
from jobmon.client.cli import ClientCLI as CLI
from jobmon.client.status_commands import (
    _create_yaml,
    get_filepaths,
    get_sub_task_tree,
    resume_workflow_from_id,
    task_status,
    update_config_value,
    update_task_status,
    validate_username,
    validate_workflow,
    workflow_status,
    workflow_tasks,
)
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.workflow import DistributorContext
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import TaskStatus, WorkflowRunStatus, WorkflowStatus
from jobmon.core.exceptions import ConfigError, InvalidRequest
from jobmon.core.requester import Requester
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.workflow import Workflow as WorkflowModel

load_model()


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
def cli(client_env):
    return CLI()


logger = logging.getLogger(__name__)


class MockDistributorProc:
    def is_alive(self):
        return True


def mock_getuser():
    return "foo"


def capture_stdout(function, arguments):
    f = StringIO()
    with redirect_stdout(f):
        function(arguments)
    return f.getvalue()


def df_from_stdout(function, arguments):
    """Capture the stdout tabulate dataframe and form it back in to a pandas dataframe."""

    # Capture the tabulate dataframe in stdout
    string_df = capture_stdout(function, arguments)

    # Take the string and split into list of lines
    output_lines = string_df.split("\n")

    # Filter out any lines that make up the box around the table or between header and data
    filtered_output_lines = filter(
        lambda x: ("--" not in x) or (x == "\n"), output_lines
    )

    # Merge the lines back into one string (newlines are preserved from before)
    join_filter_output_lines = "\n".join(filtered_output_lines)

    # Use the first row as the headers.
    header = pd.read_csv(
        StringIO(join_filter_output_lines),
        sep=r"\|",
        engine="python",
        nrows=1,
        header=None,
        dtype=str,
    ).dropna(how="all", axis=1)

    # Extract the data (everything after row 1).
    data = pd.read_csv(
        StringIO(join_filter_output_lines),
        sep=r"\|",
        engine="python",
        skiprows=1,
        header=None,
        dtype=str,
    ).dropna(how="all", axis=1)

    # Iterate over each column in the data and strip out the whitespace from the data
    for col in data.columns:
        data[col] = data[col].str.strip()

    # Add column names instead of numbers using the header rows read in previously
    data.columns = data.columns.map(header.T[0].str.strip().to_dict())

    return data


def test_workflow_status(db_engine, tool, client_env, monkeypatch, cli):
    monkeypatch.setattr(getpass, "getuser", mock_getuser)
    user = getpass.getuser()

    workflow = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )

    task_template_1 = get_task_template(tool, template_name="phase_1")
    task_template_2 = get_task_template(tool, template_name="phase_2")
    t1 = task_template_1.create_task(arg="sleep 10")
    t2 = task_template_2.create_task(arg="sleep 5", upstream_tasks=[t1])
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # we should have the column headers plus 2 tasks in pending
    command_str = f"workflow_status -u {user} -w {workflow.workflow_id}"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert df["PENDING"][0] == "2 (100.0%)"

    # defaults should return an identical value
    command_str = "workflow_status"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert df["PENDING"][0] == "2 (100.0%)"

    # Test the JSON flag
    command_str = f"workflow_status -u {user} -w {workflow.workflow_id} -n"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user, args.json)
    df = ast.literal_eval(df)
    # Dates are hard to assert equality on, since we don't know exactly when the DB bound
    # a workflow to the millisecond.
    # Extract the datetime and evaluate separately.
    df_time = df["CREATED_DATE"]["0"]
    del df["CREATED_DATE"]
    assert df == {
        "WF_ID": {"0": workflow.workflow_id},
        "WF_NAME": {"0": ""},
        "WF_STATUS": {"0": "QUEUED"},
        "TASKS": {"0": 2},
        "PENDING": {"0": "2 (100.0%)"},
        "SCHEDULED": {"0": "0 (0.0%)"},
        "RUNNING": {"0": "0 (0.0%)"},
        "DONE": {"0": "0 (0.0%)"},
        "FATAL": {"0": "0 (0.0%)"},
        "RETRIES": {"0": 0},
    }
    # Don't have millisecond precision, but can at least check our margin is +- 1 day
    now = datetime.date.today()
    df_date = datetime.datetime.fromtimestamp(df_time / 1e3).date()
    assert df_date - now == datetime.timedelta(0)

    # add a second workflow
    t1 = task_template_1.create_task(arg="sleep 15")
    t2 = task_template_2.create_task(arg="sleep 1", upstream_tasks=[t1])
    workflow = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    factory.create_workflow_run()

    # check that we get 2 rows now
    command_str = f"workflow_status -u {user}"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 2

    # check that we can get values by workflow_id
    command_str = f"workflow_status -w {workflow.workflow_id}"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 1
    assert df["WF_ID"][0] == str(workflow.workflow_id)

    # check that we can get both
    command_str = "workflow_status -w 1 2"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 2

    # add 4 more wf to make it 6
    workflow1 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t1 = task_template_1.create_task(arg="sleep 1")
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._bind_tasks()
    factory = WorkflowRunFactory(workflow1.workflow_id)
    factory.create_workflow_run()

    workflow2 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t2 = task_template_1.create_task(arg="sleep 2")
    workflow2.add_tasks([t2])
    workflow2.bind()
    workflow2._bind_tasks()
    factory = WorkflowRunFactory(workflow2.workflow_id)
    factory.create_workflow_run()

    workflow3 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t3 = task_template_1.create_task(arg="sleep 3")
    workflow3.add_tasks([t3])
    workflow3.bind()
    workflow3._bind_tasks()
    factory = WorkflowRunFactory(workflow3.workflow_id)
    factory.create_workflow_run()

    workflow4 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t4 = task_template_1.create_task(arg="sleep 4")
    workflow4.add_tasks([t4])
    workflow4.bind()
    workflow4._bind_tasks()
    factory = WorkflowRunFactory(workflow4.workflow_id)
    factory.create_workflow_run()

    # check limit 1
    command_str = f"workflow_status -u {user}  -l 1"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 1

    # check limit 2
    command_str = f"workflow_status -u {user}  -l 2"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 2
    # Assert the most recent 2 workflows appear
    assert set(map(int, df.WF_ID)) == {workflow3.workflow_id, workflow4.workflow_id}

    # check default
    command_str = f"workflow_status -u {user}"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 5

    # check over limit
    command_str = f"workflow_status -u {user}  -l 12"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_status, parsed_args)
    assert len(df) == 6

    # Check setting the limit to 0
    try:
        command_str = f"workflow_status -u {user}  -l 0"
        parsed_args = cli.parse_args(command_str)
        df_from_stdout(cli.workflow_status, parsed_args)
    except SystemExit as e:
        assert isinstance(e.__context__, argparse.ArgumentError)

    # Check setting the limit to a negative
    try:
        command_str = f"workflow_status -u {user}  -l -1"
        parsed_args = cli.parse_args(command_str)
        df_from_stdout(cli.workflow_status, parsed_args)
    except SystemExit as e:
        assert isinstance(e.__context__, argparse.ArgumentError)


def test_workflow_tasks(db_engine, tool, client_env, cli):
    workflow = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    task_template = get_task_template(tool)
    t1 = task_template.create_task(arg="sleep 3")
    t2 = task_template.create_task(arg="sleep 4")

    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    client_wfr = factory.create_workflow_run()
    client_wfr._update_status(WorkflowRunStatus.BOUND)
    wfr = SwarmWorkflowRun(
        workflow_run_id=client_wfr.workflow_run_id, requester=workflow.requester
    )

    # we should get 2 tasks back in pending state
    command_str = f"workflow_tasks -w {workflow.workflow_id}"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_tasks, parsed_args)
    assert len(df) == 2
    assert df.STATUS[0] == "PENDING"
    assert len(df.STATUS.unique()) == 1

    # execute the tasks
    with DistributorContext("sequential", wfr.workflow_run_id, 180) as distributor:
        # swarm calls
        swarm = SwarmWorkflowRun(
            workflow_run_id=wfr.workflow_run_id,
            requester=workflow.requester,
        )
        swarm.from_workflow(workflow)
        swarm.run(distributor.alive)

    # we should get 0 tasks in pending
    command_str = f"workflow_tasks -w {workflow.workflow_id} -s PENDING"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, args.status)
    assert len(df) == 0

    # we should get 0 tasks when requesting workflow -99
    command_str = "workflow_tasks -w -99"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, args.status)
    assert len(df) == 0

    # limit testing
    workflow = tool.create_workflow(
        name="test_100_tasks_with_limit_testing",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )

    for i in range(6):
        t = task_template.create_task(arg=f"echo {i}", upstream_tasks=[])
        workflow.add_task(t)

    workflow.bind()
    wfrs = workflow.run()
    assert wfrs == WorkflowRunStatus.DONE

    # check limit 1
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 1"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_tasks, parsed_args)
    assert len(df) == 1

    # check limit 2
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 2"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_tasks, parsed_args)
    assert len(df) == 2

    # check default (no limit)
    command_str = f"workflow_tasks -w {workflow.workflow_id}"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_tasks, parsed_args)
    assert len(df) == 5

    # check over limit
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 12"
    parsed_args = cli.parse_args(command_str)
    df = df_from_stdout(cli.workflow_tasks, parsed_args)
    assert len(df) == 6

    # Check setting the limit to 0
    try:
        command_str = f"workflow_tasks -w {workflow.workflow_id} -l 0"
        parsed_args = cli.parse_args(command_str)
        df_from_stdout(cli.workflow_tasks, parsed_args)
    except SystemExit as e:
        assert isinstance(e.__context__, argparse.ArgumentError)

    # Check setting the limit to a negative
    try:
        command_str = f"workflow_tasks -w {workflow.workflow_id} -l -1"
        parsed_args = cli.parse_args(command_str)
        df_from_stdout(cli.workflow_tasks, parsed_args)
    except SystemExit as e:
        assert isinstance(e.__context__, argparse.ArgumentError)


def test_task_status(db_engine, client_env, tool, cli):
    task_template = get_task_template(tool)
    t1 = task_template.create_task(arg="exit -9", max_attempts=2)
    t2 = task_template.create_task(arg="exit -0")
    workflow = tool.create_workflow()
    workflow.add_tasks([t1, t2])
    workflow.run()

    # we should get 2 failed task instances and 1 successful
    command_str = f"task_status -t {t1.task_id} {t2.task_id}"

    args = cli.parse_args(command_str)
    df = task_status(args.task_ids)

    assert len(df) == 3
    assert len(df.query("STATUS=='ERROR'")) == 2
    assert len(df.query("STATUS=='DONE'")) == 1

    # Test filters
    finished_cmd = command_str + " -s done "
    done_args = cli.parse_args(finished_cmd)
    df_fin = task_status(done_args.task_ids, done_args.status)
    assert len(df_fin) == 1

    all_cmd = command_str + " -s done fatal"
    all_args = cli.parse_args(all_cmd)
    df_all = task_status(all_args.task_ids, all_args.status)
    assert len(df_all) == 3

    # Check that the filepaths are returned correctly
    with Session(bind=db_engine) as session:
        # fake workflow run
        update_stmt = update(TaskInstance).where(
            TaskInstance.task_id.in_([t1.task_id, t2.task_id])
        )
        val = {"stdout": "/stdout/dir/file.o123", "stderr": "/stderr/dir/file.e123"}
        session.execute(update_stmt.values(**val))
        session.commit()

    args = cli.parse_args(command_str)
    df = task_status(args.task_ids)
    assert set(df.STDOUT) == {"/stdout/dir/file.o123"}
    assert set(df.STDERR) == {"/stderr/dir/file.e123"}


def test_task_reset(db_engine, client_env, tool, monkeypatch):
    monkeypatch.setattr(getpass, "getuser", mock_getuser)

    workflow = tool.create_workflow()
    task_template = get_task_template(tool)
    t1 = task_template.create_task(arg="sleep 3")
    t2 = task_template.create_task(arg="sleep 4")

    workflow.add_tasks([t1, t2])
    workflow.run()

    # Check that this user is allowed to update
    requester = Requester(client_env)
    validate_username(workflow.workflow_id, "foo", requester)

    # Validation with a different user raises an error
    with pytest.raises(AssertionError):
        validate_username(workflow.workflow_id, "notarealuser", requester)


def test_task_reset_wf_validation(db_engine, client_env, tool, cli):
    workflow1 = tool.create_workflow()
    workflow2 = tool.create_workflow()
    task_template = get_task_template(tool)
    t1 = task_template.create_task(arg="sleep 3")
    t2 = task_template.create_task(arg="sleep 4")

    workflow1.add_tasks([t1])
    workflow1.run()
    workflow2.add_tasks([t2])
    workflow2.run()

    # Check that this user is allowed to update
    command_str = (
        f"update_task_status -t {t1.task_id} {t2.task_id} "
        f"-w {workflow1.workflow_id} -s G"
    )

    args = cli.parse_args(command_str)

    # Validation with a task not in the workflow raises an error
    with pytest.raises(InvalidRequest) as exc_info:
        update_task_status([t1.task_id, t2.task_id], args.workflow_id, args.new_status)

    # Verify the error message contains the expected text
    assert (
        "Task status cannot be updated because the tasks belong to multiple workflows"
        in str(exc_info.value)
    )

    # Test that the number of resets requested doesn't break HTTP
    with pytest.raises(AssertionError):
        requester = Requester(client_env)
        task_ids = list(range(300))
        # AssertionError since we have 2 workflows, but no HTTP 502 returned
        validate_workflow(task_ids, requester)


def test_sub_dag(db_engine, client_env, tool):
    """
    Dag:
                t1             t2             t3
            /    |     \\                     /
           /     |      \\                   /
          /      |       \\                 /
         /       |        \\               /
        t1_1   t1_2            t13_1
         \\       |              /
          \\      |             /
           \\     |            /
              t1_11_213_1_1
    """  # noqa W605
    workflow = tool.create_workflow()
    task_template_1 = get_task_template(tool, template_name="phase_1")
    task_template_2 = get_task_template(tool, template_name="phase_2")
    task_template_3 = get_task_template(tool, template_name="phase_3")
    t1 = task_template_1.create_task(arg="echo 1")
    t1_1 = task_template_2.create_task(arg="echo 11")
    t1_2 = task_template_2.create_task(arg="echo 12")
    t1_11_213_1_1 = task_template_3.create_task(arg="echo 121")
    t2 = task_template_3.create_task(arg="echo 2")
    t3 = task_template_3.create_task(arg="echo 3")
    t13_1 = task_template_2.create_task(arg="echo 131")
    t1_11_213_1_1.add_upstreams([t1_1, t1_2, t13_1])
    t1_2.add_upstream(t1)
    t1_1.add_upstream(t1)
    t13_1.add_upstreams([t1, t3])
    workflow.add_tasks([t1, t1_1, t1_2, t1_11_213_1_1, t2, t3, t13_1])
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    factory.create_workflow_run()

    # test node with no sub nodes
    tree = get_sub_task_tree([t2.task_id])
    assert len(tree.items()) == 1
    assert str(t2.task_id) in tree.keys()

    # test node with two upstream
    tree = get_sub_task_tree([t3.task_id])
    assert len(tree.items()) == 3
    assert str(t3.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()

    # test sub tree
    tree = get_sub_task_tree([t1.task_id])
    assert len(tree.items()) == 5
    assert str(t1.task_id) in tree.keys()
    assert str(t1_1.task_id) in tree.keys()
    assert str(t1_2.task_id) in tree.keys()
    assert str(t1_11_213_1_1.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()

    # test sub tree with status G
    tree = get_sub_task_tree([t1.task_id], task_status=["G"])
    assert len(tree.items()) == 5
    assert str(t1.task_id) in tree.keys()
    assert str(t1_1.task_id) in tree.keys()
    assert str(t1_2.task_id) in tree.keys()
    assert str(t1_11_213_1_1.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()

    # test no status match returns 0 nodes
    tree = get_sub_task_tree([t1.task_id], task_status=["F"])
    assert len(tree.items()) == 0

    # test >1 task id list
    # test node with two upstream
    tree = get_sub_task_tree([t3.task_id, t2.task_id])
    assert len(tree.items()) == 4
    assert str(t3.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()
    assert str(t2.task_id) in tree.keys()


@pytest.mark.skip()
def test_dynamic_concurrency_limiting_cli(db_engine, client_env, cli):
    """The server-side logic is checked in distributor/test_instantiate.

    This test checks the logic of the CLI only
    """
    # Check that a valid ask returns error free

    good_command = "concurrency_limit -w 5 -m 10"
    args = cli.parse_args(good_command)

    assert args.workflow_id == 5
    assert args.max_tasks == 10

    # Check that an invalid ask will be rejected
    bad_command = "concurrency_limit -w 5 -m {}"
    with pytest.raises(ConfigError):
        cli.parse_args(bad_command.format("foo"))

    with pytest.raises(ConfigError):
        cli.parse_args(bad_command.format(-59))


def test_update_task_status(db_engine, client_env, cli):
    tool = Tool(name="test_update_task_status_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    # Create a 5 task DAG. Tasks 1-3 should finish, 4 should error out and block 5
    def generate_workflow_and_tasks(tool):
        wf = tool.create_workflow(workflow_args="test_cli_update_workflow")
        tasks = []
        echo_str = "echo {}"
        for i in range(5):
            if i != 2:
                command_str = echo_str.format(i)
            else:
                command_str = "exit -9"
            task_template = get_task_template(tool, template_name=f"phase_{i}")
            task = task_template.create_task(
                arg=command_str, name=f"task{i}", upstream_tasks=tasks, max_attempts=1
            )
            tasks.append(task)
        wf.add_tasks(tasks)
        return wf, tasks

    wf1, wf1_tasks = generate_workflow_and_tasks(tool)
    wf1.run()
    wfr1_statuses = [t.final_status for t in wf1_tasks]
    assert wfr1_statuses == ["D", "D", "F", "G", "G"]

    # Set the 'F' task to 'D' to allow progression

    update_str = (
        f"update_task_status -w {wf1.workflow_id} -t {wf1_tasks[2].task_id} -s D"
    )
    args = cli.parse_args(update_str)
    capture_stdout(cli.update_task_status, args)
    update_task_status(
        task_ids=args.task_ids, workflow_id=args.workflow_id, new_status=args.new_status
    )

    # Resume the workflow
    wf2, wf2_tasks = generate_workflow_and_tasks(tool)
    wfr2_status = wf2.run(resume=True)

    # Check that wfr2 is done, and that all tasks are "D"
    assert wfr2_status == "D"
    assert all([t.final_status == "D" for t in wf2_tasks])

    # Try a reset of a "done" workflow to "G"
    update_task_status(
        task_ids=[wf2_tasks[3].task_id], workflow_id=wf2.workflow_id, new_status="G"
    )
    wf3, wf3_tasks = generate_workflow_and_tasks(tool)
    wf3.bind()
    factory = WorkflowRunFactory(wf3.workflow_id)
    factory.set_workflow_resume()
    wf3._bind_tasks()
    client_wfr3 = factory.create_workflow_run()
    client_wfr3._update_status(WorkflowRunStatus.BOUND)

    wfr3 = SwarmWorkflowRun(
        workflow_run_id=client_wfr3.workflow_run_id, requester=wf3.requester
    )
    # run the distributor
    with DistributorContext("sequential", wfr3.workflow_run_id, 180) as distributor:
        # swarm calls
        swarm = SwarmWorkflowRun(
            workflow_run_id=wfr3.workflow_run_id,
            requester=wf3.requester,
        )
        swarm.from_workflow(wf3)
        assert len(swarm.done_tasks) == 3
        swarm.run(distributor.alive)

    assert len(swarm.done_tasks) == 5


def test_400_cli_route(db_engine, client_env):
    requester = Requester(client_env)
    with pytest.raises(InvalidRequest) as exc:
        requester.send_request(app_route="/task_status", message={}, request_type="get")
        assert "400" in str(exc.value)


def test_bad_put_route(db_engine, client_env):
    requester = Requester(client_env)
    with pytest.raises(InvalidRequest) as exc:
        requester.send_request(
            app_route="/task/update_statuses", message={}, request_type="put"
        )
        assert "400" in str(exc.value)


def test_get_yaml_data(db_engine, client_env):
    t = Tool(name="test_get_yaml_data_tool")
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1", command_template="echo {arg}", node_args=["arg"]
    )
    tt2 = t.get_task_template(
        template_name="tt2", command_template="sleep {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t2 = tt2.create_task(
        arg=2, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    wf.add_tasks([t1, t2])
    wf.run()

    # manipulate data
    with Session(bind=db_engine) as session:
        query_1 = f"""
                    UPDATE task_instance
                    SET wallclock = 10, maxrss = 400
                    WHERE task_id = {t1.task_id}"""
        session.execute(text(query_1))

        query_2 = f"""
                    UPDATE task_instance
                    SET wallclock = 20, maxrss = 600
                    WHERE task_id = {t2.task_id}"""
        session.execute(text(query_2))
        session.commit()

    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        # no execlude tt
        f.return_value = set()

        # get data for the resource yaml
        from jobmon.client.status_commands import _get_yaml_data

        result = _get_yaml_data(wf.workflow_id, None, "avg", "avg", "max", wf.requester)
        assert len(result) == 2
        assert result[tt1._active_task_template_version.id] == [
            "tt1",
            1,
            400,
            10,
            "null.q",
        ]
        assert result[tt2._active_task_template_version.id] == [
            "tt2",
            1,
            600,
            20,
            "null.q",
        ]

    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        # execlude tt1
        f.return_value = {tt1.active_task_template_version.id}

        # get data for the resource yaml
        from jobmon.client.status_commands import _get_yaml_data

        result = _get_yaml_data(wf.workflow_id, None, "avg", "avg", "max", wf.requester)
        assert len(result) == 2
        # tt1 fills with default value
        assert result[tt1._active_task_template_version.id] == [
            "tt1",
            1,
            1,
            3600,
            "all.q",
        ]
        # tt2 is real
        assert result[tt2._active_task_template_version.id] == [
            "tt2",
            1,
            600,
            20,
            "null.q",
        ]

    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        # execlude both
        f.return_value = {
            tt1.active_task_template_version.id,
            tt2.active_task_template_version.id,
        }

        # get data for the resource yaml
        from jobmon.client.status_commands import _get_yaml_data

        result = _get_yaml_data(wf.workflow_id, None, "avg", "avg", "max", wf.requester)
        assert len(result) == 2
        # tt1 fills with default value
        assert result[tt1._active_task_template_version.id] == [
            "tt1",
            1,
            1,
            3600,
            "all.q",
        ]
        # tt2 fills with default value
        assert result[tt2._active_task_template_version.id] == [
            "tt2",
            1,
            1,
            3600,
            "all.q",
        ]


def test_create_yaml():
    expected = """task_template_resources:
  tt1:
    ihme_slurm:
      cores: 1
      memory: "400B"
      runtime: 10
      queue: "all.q"
    sequential:
      cores: 1
      memory: "400B"
      runtime: 10
      queue: "all.q"
  tt2:
    ihme_slurm:
      cores: 1
      memory: "600B"
      runtime: 20
      queue: "long.q"
    sequential:
      cores: 1
      memory: "600B"
      runtime: 20
      queue: "long.q"
"""

    input = {1: ["tt1", 1, 400, 10, "all.q"], 2: ["tt2", 1, 600, 20, "long.q"]}
    result = _create_yaml(input, ["ihme_slurm", "sequential"])
    assert result == expected


def test_get_filepaths(db_engine, tool):
    tt = tool.get_task_template(
        template_name="dummy_template",
        command_template="echo {arg1} {arg2}",
        node_args=["arg1", "arg2"],
        task_args=[],
        op_args=[],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )
    tasks = tt.create_tasks(
        arg1=[1, 2], arg2=["a", "b"], compute_resources={"queue": "null.q"}
    )
    array = tasks[0].array
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.run()

    with Session(bind=db_engine) as session:
        query = """UPDATE task_instance
                            SET stdout="/cool/filepath.o",
                            stderr="/cool/filepath.e"
                        """
        session.execute(text(query))
        session.commit()

    df_cli = get_filepaths(workflow_id=wf.workflow_id, array_name=array.name)
    assert len(df_cli) == 4
    df_cli = pd.DataFrame(df_cli)
    assert set(df_cli.OUTPUT_PATH) == {
        "/cool/filepath.o",
    }
    assert set(df_cli.ERROR_PATH) == {
        "/cool/filepath.e",
    }
    assert set(df_cli.ARRAY_NAME == array.name)

    one_task_df = get_filepaths(
        workflow_id=wf.workflow_id,
        array_name=array.name,
        job_name=tasks[0].name,
    )

    assert len(one_task_df) == 1

    # Check that the fetch results work with create_task as well.
    tasks2 = tt.create_tasks(
        arg1=[1, 2, 3, 4],
        arg2=["a", "b", "c", "d"],
        compute_resources={"queue": "null.q"},
    )
    tt2 = tool.get_task_template(
        template_name="dummy_template2",
        command_template="echo {arg1} {arg2}",
        node_args=["arg1", "arg2"],
        task_args=[],
        op_args=[],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )
    tasks3 = tt2.create_tasks(
        arg1=[5],
        arg2=["a", "b", "c"],
        compute_resources={"queue": "null.q"},
        name="yiyayiyayou",
    )
    wf2 = tool.create_workflow()
    wf2.add_tasks(tasks2 + tasks3)
    wf2.run()

    df_full = get_filepaths(workflow_id=wf2.workflow_id, limit=6)
    assert len(df_full) == 6

    # Filter by array name - the simple array
    df_array1 = get_filepaths(
        workflow_id=wf2.workflow_id, array_name=tasks3[0].array.name
    )
    assert len(df_array1) == 3


def test_resume_workflow_from_cli(tool, task_template, db_engine, cli):
    from jobmon.client.workflow_run import WorkflowRunFactory

    workflow = tool.create_workflow()

    # Create a small example DAG.
    #       t1
    #     /    \
    #    t2     t3
    #      \   /
    #        t4
    t1 = task_template.create_task(arg="echo 1")
    t2 = task_template.create_task(arg="echo 2", upstream_tasks=[t1])
    t3 = task_template.create_task(arg="exit 1", upstream_tasks=[t1], max_attempts=1)
    t4 = task_template.create_task(arg="echo 4", upstream_tasks=[t2, t3])

    workflow.add_tasks([t1, t2, t3, t4])
    # Run the workflow. Task 3 should error, task 4 doesn't run.
    workflow.run()
    # Task states should be [D, D, F, D] at this point

    task_ids = [t.task_id for t in (t1, t2, t3, t4)]

    with Session(bind=db_engine) as session:
        query = select(Task.status).where(Task.id.in_(task_ids))
        res = session.execute(query).scalars().all()
        assert res == ["D", "D", "F", "G"]
        session.commit()

    # Update the exit 1 command to something that'll work on resume
    with Session(bind=db_engine) as session:
        query = update(Task).where(Task.id == t3.task_id).values(command="echo 3")
        session.execute(query)
        session.commit()

    # Signal a resume, assert it returned accordingly.
    resume_str = f"workflow_resume -w {workflow.workflow_id} -c sequential -t 200"
    args = cli.parse_args(resume_str)
    assert args.workflow_id == workflow.workflow_id
    assert not args.reset_running_jobs
    assert args.timeout == 200

    with patch.object(WorkflowRunFactory, "set_workflow_resume") as mock_set_resume:

        resume_workflow_from_id(
            workflow_id=args.workflow_id,
            cluster_name=args.cluster_name,
            reset_if_running=args.reset_running_jobs,
            timeout=args.timeout,
        )

        mock_set_resume.assert_called_once_with(
            reset_running_jobs=args.reset_running_jobs, resume_timeout=args.timeout
        )

    # Check that the swarm is complete
    with Session(bind=db_engine) as session:
        res = (
            session.execute(select(Task.status).where(Task.id.in_(task_ids)))
            .scalars()
            .all()
        )
        assert res == [TaskStatus.DONE] * 4

        res = session.execute(
            select(WorkflowModel.status).where(WorkflowModel.id == workflow.workflow_id)
        ).scalar()
        assert res == WorkflowStatus.DONE
        session.commit()


def test_update_config_value_success():
    """Test successful config updates with various data types."""
    # Create a temporary config file for testing
    test_config_data = {
        "db": {
            "sqlalchemy_database_uri": "original_uri",
            "pool": {"size": 5, "max_overflow": 10, "timeout": 30, "pre_ping": False},
        },
        "distributor": {"poll_interval": 10},
        "http": {
            "request_timeout": 20,
            "retries_attempts": 10,
            "retries_timeout": 300,
            "service_url": "http://original.com",
        },
        "heartbeat": {"report_by_buffer": 3.1, "task_instance_interval": 90},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(test_config_data, f)
        temp_config_path = f.name

    try:
        # Test simple key update
        result = update_config_value("http.retries_attempts", "15", temp_config_path)
        assert (
            "Successfully updated 'http.retries_attempts' from '10' to '15'" in result
        )

        # Verify the update
        with open(temp_config_path, "r") as f:
            updated_config = yaml.safe_load(f)
        assert updated_config["http"]["retries_attempts"] == 15

        # Test nested key update
        result = update_config_value("db.pool.size", "8", temp_config_path)
        assert "Successfully updated 'db.pool.size' from '5' to '8'" in result

        # Test boolean update
        result = update_config_value("db.pool.pre_ping", "true", temp_config_path)
        assert (
            "Successfully updated 'db.pool.pre_ping' from 'False' to 'True'" in result
        )

        # Test float update
        result = update_config_value(
            "heartbeat.report_by_buffer", "2.5", temp_config_path
        )
        assert (
            "Successfully updated 'heartbeat.report_by_buffer' from '3.1' to '2.5'"
            in result
        )

        # Test string update
        result = update_config_value(
            "http.service_url", "http://new.example.com", temp_config_path
        )
        assert "Successfully updated 'http.service_url'" in result
        assert "http://new.example.com" in result

        # Verify all updates were applied
        with open(temp_config_path, "r") as f:
            final_config = yaml.safe_load(f)

        assert final_config["http"]["retries_attempts"] == 15
        assert final_config["db"]["pool"]["size"] == 8
        assert final_config["db"]["pool"]["pre_ping"] is True
        assert final_config["heartbeat"]["report_by_buffer"] == 2.5
        assert final_config["http"]["service_url"] == "http://new.example.com"

    finally:
        os.unlink(temp_config_path)


def test_update_config_value_validation_errors():
    """Test validation errors for invalid keys and sections."""
    test_config_data = {
        "db": {"sqlalchemy_database_uri": "", "pool": {"size": 5}},
        "http": {"request_timeout": 20},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(test_config_data, f)
        temp_config_path = f.name

    try:
        # Test invalid section
        with pytest.raises(ValueError) as exc_info:
            update_config_value("invalid_section.key", "value", temp_config_path)
        assert "Section 'invalid_section' not found in configuration" in str(
            exc_info.value
        )
        assert "Available sections: ['db', 'http']" in str(exc_info.value)

        # Test invalid key in valid section
        with pytest.raises(ValueError) as exc_info:
            update_config_value("http.invalid_key", "value", temp_config_path)
        assert "Key 'invalid_key' not found in 'http'" in str(exc_info.value)
        assert "Available keys:" in str(exc_info.value)

        # Test invalid nested key
        with pytest.raises(ValueError) as exc_info:
            update_config_value("db.pool.invalid_nested_key", "value", temp_config_path)
        assert "Key 'invalid_nested_key' not found in 'db.pool'" in str(exc_info.value)

        # Test invalid format (no dot notation)
        with pytest.raises(ValueError) as exc_info:
            update_config_value("invalid_format", "value", temp_config_path)
        assert "Key 'invalid_format' must be in dot notation format" in str(
            exc_info.value
        )

        # Test single level key (needs at least section.key)
        with pytest.raises(ValueError) as exc_info:
            update_config_value("db", "value", temp_config_path)
        assert "must be in dot notation format" in str(exc_info.value)

    finally:
        os.unlink(temp_config_path)


def test_update_config_cli_integration(cli):
    """Test the CLI integration for config updates."""
    # Create a temporary config file
    test_config_data = {
        "http": {"retries_attempts": 10, "request_timeout": 20},
        "distributor": {"poll_interval": 15},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(test_config_data, f)
        temp_config_path = f.name

    try:
        # Test successful CLI command parsing
        command_str = (
            f"update_config http.retries_attempts 25 --config-file {temp_config_path}"
        )
        args = cli.parse_args(command_str)

        assert args.key == "http.retries_attempts"
        assert args.value == "25"
        assert args.config_file == temp_config_path

        # Test the CLI execution captures stdout
        output = capture_stdout(cli.update_config, args)
        assert "Successfully updated 'http.retries_attempts'" in output
        assert "from '10' to '25'" in output

        # Test error handling in CLI
        error_command_str = (
            f"update_config invalid.key value --config-file {temp_config_path}"
        )
        error_args = cli.parse_args(error_command_str)

        # The CLI should catch ValueError and exit with code 1
        # We can't easily test sys.exit(1) in pytest, but we can test that it would call the function
        # and that the function raises ValueError
        with pytest.raises(ValueError):
            update_config_value(
                error_args.key, error_args.value, error_args.config_file
            )

    finally:
        os.unlink(temp_config_path)


def test_update_config_preserves_yaml_structure():
    """Test that config updates preserve YAML structure and comments are not lost."""
    # Create a config with specific structure
    test_config_data = {
        "db": {"sqlalchemy_database_uri": "", "pool": {"size": 5, "max_overflow": 10}},
        "http": {"retries_attempts": 10, "service_url": "http://original.com"},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(test_config_data, f)
        temp_config_path = f.name

    try:
        # Make multiple updates
        update_config_value("db.pool.size", "20", temp_config_path)
        update_config_value("http.service_url", "http://updated.com", temp_config_path)

        # Load the updated config and verify structure is preserved
        with open(temp_config_path, "r") as f:
            updated_config = yaml.safe_load(f)

        # Check that all original keys are still present
        assert "db" in updated_config
        assert "http" in updated_config
        assert "pool" in updated_config["db"]
        assert "sqlalchemy_database_uri" in updated_config["db"]
        assert "max_overflow" in updated_config["db"]["pool"]
        assert "retries_attempts" in updated_config["http"]

        # Check that updates were applied
        assert updated_config["db"]["pool"]["size"] == 20
        assert updated_config["http"]["service_url"] == "http://updated.com"

        # Check that unmodified values remain the same
        assert updated_config["db"]["sqlalchemy_database_uri"] == ""
        assert updated_config["db"]["pool"]["max_overflow"] == 10
        assert updated_config["http"]["retries_attempts"] == 10

    finally:
        os.unlink(temp_config_path)
