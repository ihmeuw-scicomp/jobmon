import pytest
from unittest.mock import Mock

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

def test_simple_task(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get a good looking command string."""
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