from unittest.mock import PropertyMock, patch

from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.client.cli import ClientCLI as CLI
from jobmon.client.status_commands import task_template_resources
from jobmon.client.tool import Tool


def test_resource_usage(db_engine, client_env):
    """Test Task resource usage method."""

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    workflow = tool.create_workflow(name="resource_usage_test_wf")
    template = tool.get_task_template(
        template_name="resource_usage_test_template",
        command_template="echo a",
    )
    task = template.create_task()
    workflow.add_tasks([task])
    workflow.run()

    # Add fake resource usage to the TaskInstance
    with Session(bind=db_engine) as session:
        sql = """
        UPDATE task_instance
        SET nodename = 'SequentialNode', wallclock = 12, maxpss = 1234
        WHERE task_id = :task_id"""
        session.execute(text(sql), {"task_id": task.task_id})
        session.commit()
    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        f.return_value = set()  # no exclude tt
        used_task_resources = task.resource_usage()
        assert used_task_resources == {
            "memory": "1234",
            "nodename": "SequentialNode",
            "num_attempts": 1,
            "runtime": "12",
        }


def test_tt_resource_usage(db_engine, client_env):
    """Test TaskTemplate resource usage method."""

    tool = Tool("i_am_a_new_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflow_1 = tool.create_workflow(name="task_template_resource_usage_test_wf_1")
    workflow_2 = tool.create_workflow(name="task_template_resource_usage_test_wf_2")
    template = tool.get_task_template(
        template_name="I_have_to_be_new",
        command_template="echo {arg} --foo {arg_2} --bar {task_arg_1} --baz {arg_3}",
        node_args=["arg", "arg_2", "arg_3"],
        task_args=["task_arg_1"],
        op_args=[],
    )
    template_2 = tool.get_task_template(
        template_name="I_have_to_be_new_2",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    task_1 = template.create_task(
        arg="Acadia",
        arg_2="DeathValley",
        task_arg_1="NorthCascades",
        arg_3="Yellowstone",
        compute_resources={"max_runtime_seconds": 30},
    )
    task_2 = template.create_task(
        arg="Zion",
        arg_2="JoshuaTree",
        task_arg_1="Olympic",
        arg_3="GrandTeton",
        compute_resources={"max_runtime_seconds": 30},
    )
    task_3 = template.create_task(
        arg="Rainier",
        arg_2="Badlands",
        task_arg_1="CraterLake",
        arg_3="GrandTeton",
        compute_resources={"max_runtime_seconds": 30},
    )

    workflow_1.add_tasks([task_1, task_2])
    workflow_1.run()
    workflow_2.add_tasks([task_3])
    workflow_2.run()

    # Add fake resource usage to the TaskInstances
    with Session(bind=db_engine) as session:
        query_1 = f"""
        UPDATE task_instance
        SET wallclock = 10, maxrss = 300
        WHERE task_id = {task_1.task_id}"""
        session.execute(text(query_1))

        query_2 = f"""
        UPDATE task_instance
        SET wallclock = 20, maxrss = 600
        WHERE task_id = {task_2.task_id}"""
        session.execute(text(query_2))

        query_3 = f"""
        UPDATE task_instance
        SET wallclock = 30, maxrss = 900
        WHERE task_id = {task_3.task_id}"""
        session.execute(text(query_3))
        session.commit()

    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        f.return_value = {
            template.active_task_template_version.id,
            template_2.active_task_template_version.id,
        }
        # ttv in execlude list should return None
        used_task_template_resources = template.resource_usage(ci=0.95)
        assert used_task_template_resources is None

    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        f.return_value = set()  # no execlude tt

        # Check the aggregate resources for all workflows
        used_task_template_resources = template.resource_usage(ci=0.95)

        resources = {
            "num_tasks": 3,
            "min_mem": "300B",
            "max_mem": "900B",
            "mean_mem": "600.0B",
            "min_runtime": 10,
            "max_runtime": 30,
            "mean_runtime": 20.0,
            "median_mem": "600.0B",
            "median_runtime": 20.0,
            "ci_mem": [-145.24, 1345.24],
            "ci_runtime": [-4.84, 44.84],
        }
        assert used_task_template_resources == resources

        command_str = (
            f"task_template_resources -t {template._active_task_template_version.id}"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version
        )
        assert used_task_template_resources["num_tasks"] == resources["num_tasks"]
        assert used_task_template_resources["min_mem"] == resources["min_mem"]
        assert used_task_template_resources["max_mem"] == resources["max_mem"]
        assert used_task_template_resources["mean_mem"] == resources["mean_mem"]
        assert used_task_template_resources["min_runtime"] == resources["min_runtime"]
        assert used_task_template_resources["max_runtime"] == resources["max_runtime"]
        assert used_task_template_resources["mean_runtime"] == resources["mean_runtime"]
        assert used_task_template_resources["median_mem"] == resources["median_mem"]
        assert (
            used_task_template_resources["median_runtime"]
            == resources["median_runtime"]
        )
        assert used_task_template_resources["ci_mem"][0] is None
        assert used_task_template_resources["ci_mem"][1] is None
        assert used_task_template_resources["ci_runtime"][0] is None
        assert used_task_template_resources["ci_runtime"][1] is None

        # Check the aggregate resources for the first workflow
        used_task_template_resources = template.resource_usage(
            workflows=[workflow_1.workflow_id], ci=0.95
        )
        resources = {
            "num_tasks": 2,
            "min_mem": "300B",
            "max_mem": "600B",
            "mean_mem": "450.0B",
            "min_runtime": 10,
            "max_runtime": 20,
            "mean_runtime": 15.0,
            "median_mem": "450.0B",
            "median_runtime": 15.0,
            "ci_mem": [-1455.93, 2355.93],
            "ci_runtime": [-48.53, 78.53],
        }
        assert used_task_template_resources == resources

        command_str = (
            f"task_template_resources -t {template._active_task_template_version.id} -w"
            f" {workflow_1.workflow_id}"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version,
            workflows=args.workflows,
            ci=0.95,
        )
        assert used_task_template_resources == resources

        # Check the aggregate resources for the first and second workflows
        used_task_template_resources = template.resource_usage(
            workflows=[workflow_1.workflow_id, workflow_2.workflow_id], ci=0.95
        )
        resources = {
            "num_tasks": 3,
            "min_mem": "300B",
            "max_mem": "900B",
            "mean_mem": "600.0B",
            "min_runtime": 10,
            "max_runtime": 30,
            "mean_runtime": 20.0,
            "median_mem": "600.0B",
            "median_runtime": 20.0,
            "ci_mem": [-145.24, 1345.24],
            "ci_runtime": [-4.84, 44.84],
        }
        assert used_task_template_resources == resources

        command_str = (
            f"task_template_resources -t {template._active_task_template_version.id} "
            f"-w {workflow_1.workflow_id} {workflow_2.workflow_id}"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version,
            workflows=args.workflows,
            ci=0.95,
        )
        assert used_task_template_resources == resources

        # Check the outcome of resource_usage of a task_template that has no Tasks
        used_task_template_resources = template_2.resource_usage(ci=0.95)
        resources = {
            "num_tasks": None,
            "min_mem": None,
            "max_mem": None,
            "mean_mem": None,
            "min_runtime": None,
            "max_runtime": None,
            "mean_runtime": None,
            "median_mem": None,
            "median_runtime": None,
            "ci_mem": None,
            "ci_runtime": None,
        }
        assert used_task_template_resources == resources

        command_str = (
            f"task_template_resources -t {template_2._active_task_template_version.id}"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version, ci=0.95
        )
        assert used_task_template_resources == resources

        # Check the aggregate resources when two node args of same type are passed in (tasks 1 & 3)
        used_task_template_resources = template.resource_usage(
            node_args={"arg": ["Acadia", "Rainier"]}, ci=0.95
        )
        resources = {
            "num_tasks": 2,
            "min_mem": "300B",
            "max_mem": "900B",
            "mean_mem": "600.0B",
            "min_runtime": 10,
            "max_runtime": 30,
            "mean_runtime": 20.0,
            "median_mem": "600.0B",
            "median_runtime": 20.0,
            "ci_mem": [-3211.86, 4411.86],
            "ci_runtime": [-107.06, 147.06],
        }
        assert used_task_template_resources == resources

        node_args = '{"arg":["Acadia","Rainier"]}'
        command_str = (
            f"task_template_resources -t {template._active_task_template_version.id} -a"
            f" '{node_args}'"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version,
            node_args=args.node_args,
            ci=0.95,
        )
        assert used_task_template_resources == resources

        # Check the aggregate resources when one node arg of two types are passed in (tasks 2 & 3)
        used_task_template_resources = template.resource_usage(
            node_args={"arg": ["Zion"], "arg_2": ["Badlands"]}, ci=0.99
        )
        resources = {
            "num_tasks": 2,
            "min_mem": "600B",
            "max_mem": "900B",
            "mean_mem": "750.0B",
            "min_runtime": 20,
            "max_runtime": 30,
            "mean_runtime": 25.0,
            "median_mem": "750.0B",
            "median_runtime": 25.0,
            "ci_mem": [-8798.51, 10298.51],
            "ci_runtime": [-293.28, 343.28],
        }
        assert used_task_template_resources == resources

        node_args = '{"arg": ["Zion"], "arg_2": ["Badlands"]}'
        command_str = (
            f"task_template_resources -t {template._active_task_template_version.id} -a"
            f" '{node_args}'"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version,
            node_args=args.node_args,
            ci=0.99,
        )
        assert used_task_template_resources == resources

        # Check the aggregate resources when node args and workflow ids are passed in (Task 3)
        used_task_template_resources = template.resource_usage(
            node_args={"arg_3": ["GrandTeton"]}, workflows=[workflow_2.workflow_id]
        )

        resources = {
            "num_tasks": 1,
            "min_mem": "900B",
            "max_mem": "900B",
            "mean_mem": "900.0B",
            "min_runtime": 30,
            "max_runtime": 30,
            "mean_runtime": 30.0,
            "median_mem": "900.0B",
            "median_runtime": 30.0,
        }
        assert used_task_template_resources["num_tasks"] == resources["num_tasks"]
        assert used_task_template_resources["min_mem"] == resources["min_mem"]
        assert used_task_template_resources["max_mem"] == resources["max_mem"]
        assert used_task_template_resources["mean_mem"] == resources["mean_mem"]
        assert used_task_template_resources["min_runtime"] == resources["min_runtime"]
        assert used_task_template_resources["max_runtime"] == resources["max_runtime"]
        assert used_task_template_resources["mean_runtime"] == resources["mean_runtime"]
        assert used_task_template_resources["median_mem"] == resources["median_mem"]
        assert (
            used_task_template_resources["median_runtime"]
            == resources["median_runtime"]
        )
        assert used_task_template_resources["ci_mem"][0] is None
        assert used_task_template_resources["ci_mem"][1] is None
        assert used_task_template_resources["ci_runtime"][0] is None
        assert used_task_template_resources["ci_runtime"][1] is None

        node_args = '{"arg_3":["GrandTeton"]}'
        command_str = (
            f"task_template_resources -t {template._active_task_template_version.id} -a"
            f" '{node_args}' -w {workflow_2.workflow_id}"
        )
        cli = CLI()
        args = cli.parse_args(command_str)
        used_task_template_resources = task_template_resources(
            task_template_version=args.task_template_version,
            node_args=args.node_args,
            workflows=args.workflows,
        )
        assert used_task_template_resources["num_tasks"] == resources["num_tasks"]
        assert used_task_template_resources["min_mem"] == resources["min_mem"]
        assert used_task_template_resources["max_mem"] == resources["max_mem"]
        assert used_task_template_resources["mean_mem"] == resources["mean_mem"]
        assert used_task_template_resources["min_runtime"] == resources["min_runtime"]
        assert used_task_template_resources["max_runtime"] == resources["max_runtime"]
        assert used_task_template_resources["mean_runtime"] == resources["mean_runtime"]
        assert used_task_template_resources["median_mem"] == resources["median_mem"]
        assert (
            used_task_template_resources["median_runtime"]
            == resources["median_runtime"]
        )
        assert used_task_template_resources["ci_mem"][0] is None
        assert used_task_template_resources["ci_mem"][1] is None
        assert used_task_template_resources["ci_runtime"][0] is None
        assert used_task_template_resources["ci_runtime"][1] is None


def test_tt_resource_usage_with_0(db_engine, client_env):
    """Test TaskTemplate resource usage method."""

    tool = Tool("i_am_a_new_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflow_1 = tool.create_workflow(name="task_template_resource_usage_test_wf_1")
    template = tool.get_task_template(
        template_name="I_have_to_be_new_2",
        command_template="echo {arg} --foo {arg_2} --bar {task_arg_1} --baz {arg_3}",
        node_args=["arg", "arg_2", "arg_3"],
        task_args=["task_arg_1"],
        op_args=[],
    )

    task_1 = template.create_task(
        arg="Acadia",
        arg_2="DeathValley",
        task_arg_1="NorthCascades",
        arg_3="Yellowstone",
        compute_resources={"max_runtime_seconds": 30},
    )
    task_2 = template.create_task(
        arg="Zion",
        arg_2="JoshuaTree",
        task_arg_1="Olympic",
        arg_3="GrandTeton",
        compute_resources={"max_runtime_seconds": 30},
    )
    task_3 = template.create_task(
        arg="Rainier",
        arg_2="Badlands",
        task_arg_1="CraterLake",
        arg_3="GrandTeton",
        compute_resources={"max_runtime_seconds": 30},
    )

    workflow_1.add_tasks([task_1, task_2, task_3])
    workflow_1.run()

    # Add fake resource usage to the TaskInstances
    with Session(bind=db_engine) as session:
        query_1 = f"""
        UPDATE task_instance
        SET wallclock = 10, maxrss = 100
        WHERE task_id = {task_1.task_id}"""
        session.execute(text(query_1))

        query_2 = f"""
        UPDATE task_instance
        SET wallclock = 0, maxrss = 0
        WHERE task_id = {task_2.task_id}"""
        session.execute(text(query_2))

        query_3 = f"""
        UPDATE task_instance
        SET wallclock = 20, maxrss = 200
        WHERE task_id = {task_3.task_id}"""
        session.execute(text(query_3))
        session.commit()

    with patch(
        "jobmon.core.constants.ExecludeTTVs.EXECLUDE_TTVS", new_callable=PropertyMock
    ) as f:
        f.return_value = set()  # no execlude tt

        # Check the aggregate resources for all workflows
        used_task_template_resources = template.resource_usage(ci=0.95)

        resources = {
            "num_tasks": 3,
            "min_mem": "100B",
            "max_mem": "200B",
            "mean_mem": "150.0B",
            "min_runtime": 10,
            "max_runtime": 20,
            "mean_runtime": 15.0,
            "median_mem": "150.0B",
            "median_runtime": 15.0,
            "ci_mem": [-485.31, 785.31],
            "ci_runtime": [-48.53, 78.53],
        }

        assert used_task_template_resources == resources


def test_max_mem(db_engine, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflow_1 = tool.create_workflow(name="task_template_mem_test")
    # ttv 1 query is forbidden, so create a place holder
    tool.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    template = tool.get_task_template(
        template_name="task_template_resource_usage",
        command_template="echo {arg} --foolili {arg_2} --bar {task_arg_1} --baz {arg_3}",
        node_args=["arg", "arg_2", "arg_3"],
        task_args=["task_arg_1"],
        op_args=[],
    )
    task_1 = template.create_task(
        arg="Acadia",
        arg_2="DeathValley",
        task_arg_1="NorthCascades",
        arg_3="Yellowstone",
        compute_resources={"max_runtime_seconds": 30},
    )

    workflow_1.add_tasks([task_1])
    workflow_1.run()

    # return 0 when both null
    with Session(bind=db_engine) as session:
        query_1 = f"""
            UPDATE task_instance
            SET maxpss = null, maxrss=null
            WHERE task_id = {task_1.task_id}"""
        session.execute(text(query_1))
        session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "0B"

    # return the other when 1 is null
    with Session(bind=db_engine) as session:
        query_1 = f"""
                UPDATE task_instance
                SET maxrss=null
                WHERE task_id = {task_1.task_id}"""
        session.execute(text(query_1))
        session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "0B"

    with Session(bind=db_engine) as session:
        query_1 = f"""
                UPDATE task_instance
                SET maxrss=1
                WHERE task_id = {task_1.task_id}"""
        session.execute(text(query_1))
        session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "1B"

    with Session(bind=db_engine) as session:
        query_1 = f"""
                UPDATE task_instance
                SET maxrss= -1
                WHERE task_id = {task_1.task_id}"""
        session.execute(text(query_1))
        session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "0B"
