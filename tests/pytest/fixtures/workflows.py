"""Workflow fixtures for Jobmon tests.

These fixtures provide pre-configured Tool, TaskTemplate, and related objects
for workflow testing.

Key fixtures:
    tool: A pre-configured Tool with common task templates
    task_template: The 'simple_template' from the tool
    array_template: The 'array_template' from the tool
"""

import pytest

from jobmon.client.api import Tool


def _get_task_template(tool: Tool, template_name: str) -> None:
    """Helper to create a standard task template on a tool.

    Creates a simple task template with a single node argument.

    Args:
        tool: The Tool instance to add the template to
        template_name: Name for the new template
    """
    tool.get_task_template(
        template_name=template_name,
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )


@pytest.fixture
def tool(client_env):
    """Create a pre-configured Tool for testing.

    The tool comes with several pre-registered task templates:
    - simple_template: Basic template with {arg} command
    - array_template: Template for array tasks with 'echo {arg}'
    - phase_1, phase_2, phase_3: Templates for multi-phase workflow tests

    Args:
        client_env: The client environment URL

    Returns:
        Tool: A configured Tool instance ready for workflow creation

    Example:
        def test_workflow(tool):
            wf = tool.create_workflow(name="test")
            tt = tool.active_task_templates["simple_template"]
            task = tt.create_task(arg="hello")
            wf.add_task(task)
    """
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    # Simple template for basic tests
    tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )

    # Array template for array task tests
    tool.get_task_template(
        template_name="array_template",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )

    # Phase templates for multi-phase workflow tests
    _get_task_template(tool, "phase_1")
    _get_task_template(tool, "phase_2")
    _get_task_template(tool, "phase_3")

    return tool


@pytest.fixture
def task_template(tool):
    """Get the simple_template from the tool fixture.

    Provides convenient access to the most commonly used template.

    Args:
        tool: The tool fixture

    Returns:
        TaskTemplate: The 'simple_template' task template

    Example:
        def test_task(task_template):
            task = task_template.create_task(arg="test")
            assert task.command == "test"
    """
    return tool.active_task_templates["simple_template"]


@pytest.fixture
def array_template(tool):
    """Get the array_template from the tool fixture.

    Provides convenient access to the array-capable template.

    Args:
        tool: The tool fixture

    Returns:
        TaskTemplate: The 'array_template' task template

    Example:
        def test_array(array_template):
            tasks = array_template.create_tasks(arg=["a", "b", "c"])
            assert len(tasks) == 3
    """
    return tool.active_task_templates["array_template"]
