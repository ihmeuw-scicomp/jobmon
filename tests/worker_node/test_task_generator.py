import pytest
from typing import List
from unittest.mock import Mock

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

def test_simple_task(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get a good looking command string.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#642
    """
    # Set up function
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    tool = Tool()
    @task_generator.task_generator(serializers={}, tool=tool)
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="baz"
    )

    # Verify task name
    assert task.name == "simple_function:foo=1:bar=baz"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} "
        f" --expected_jobmon_version {core_version}"
        f" --module_name worker_node.test_task_generator"
        " --func_name simple_function"
        " --args 'foo=1;bar=baz'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources


def test_list_args(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify we can properly pass args that serialize as lists.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#672
    """
    # Set up functino
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    tool = Tool()
    @task_generator.task_generator(serializers={}, tool=tool)
    def list_function(foo: List[str], bar: List[str]) -> None:
        """Example task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = list_function.create_task(
        compute_resources=compute_resources, foo=["a", "b"], bar=["c", "d"]
    )

    # Verify task name
    assert task.name == "list_function:foo=a,b:bar=c,d"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} "
        f" --expected_jobmon_version {core_version}"
        f" --module_name worker_node.test_task_generator"
        " --func_name list_function"
        " --args 'foo=[a, b];bar=[c, d]'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources

@pytest.mark.parametrize(
        ["naming_args", "expected_name"],
        [
            [["foo", "bar"], "simple_function:foo=1:bar=baz"],
            [None, "simple_function:foo=1:bar=baz"],
            [["foo"], "simple_function:foo=1"],
            [[], "simple_function"],
        ],
    )
def test_naming_args(
        client_env, naming_args, expected_name, monkeypatch: pytest.fixture
) -> None:
    """Verify that the name only includes the expected naming args.


    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#711
    """
    # Set up function
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    tool = Tool()
    @task_generator.task_generator(
        serializers={}, tool=tool, naming_args=naming_args
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="baz"
    )

    # Verify task name matches the expected
    assert task.name == expected_name

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} "
        f" --expected_jobmon_version {core_version}"
        f" --module_name worker_node.test_task_generator"
        " --func_name simple_function"
        " --args 'foo=1;bar=baz'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources