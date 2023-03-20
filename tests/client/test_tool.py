import os
import pytest

from jobmon.client.tool import Tool


def test_create_tool(client_env):
    """test that we can create a tool and recreate it with identical params and
    get the same ID"""

    t1 = Tool(name="foo")
    assert t1.name == "foo"
    # check that we have an id
    assert t1.id is not None
    # check that a tool version got created
    assert len(t1.tool_versions) == 1

    # check that we can initialize with just the name
    t2 = Tool("foo")
    assert t2.id == t1.id
    assert t2.active_tool_version.id == t1.active_tool_version.id


def test_create_tool_version(client_env):
    """test that we create a new tool version"""

    t1 = Tool(name="bar")
    orig_tool_version = t1.active_tool_version.id

    # create a new version
    new_tool_version_id = t1.get_new_tool_version()
    assert len(t1.tool_versions) == 2

    # check that the new version is now active
    assert t1.active_tool_version.id == new_tool_version_id

    # reassign the activer version to the old value and confirm it works
    t1.set_active_tool_version_id(orig_tool_version)
    assert t1.active_tool_version.id == orig_tool_version

    # try to assign to an invalid value
    with pytest.raises(ValueError):
        t1.set_active_tool_version_id(0)

    # use latest to reassign and confirm it works
    t1.set_active_tool_version_id("latest")
    assert t1.active_tool_version.id == new_tool_version_id


def test_yaml_compute_resources_and_scales(client_env):
    """Test that we can set Tool ComputeResources via YAML file."""

    thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
    tool = Tool(name="test_resources_scales")
    tt_1 = tool.get_task_template(
        template_name="preprocess",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tt_2 = tool.get_task_template(
        template_name="model",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tool.set_default_compute_resources_from_yaml(
        default_cluster_name="sequential",
        yaml_file=os.path.join(thisdir, "cluster_resources.yaml"),
        set_task_templates=True,
    )
    tool.set_default_resource_scales_from_yaml(
        default_cluster_name="sequential",
        yaml_file=os.path.join(thisdir, "cluster_resources.yaml"),
        set_task_templates=True,
    )
    assert tool.default_compute_resources_set["sequential"] == {
        "num_cores": 2,
        "m_mem_free": "2G",
        "max_runtime_seconds": "(60 * 60 * 24)",
        "queue": "null.q",
    }
    assert tool.default_resource_scales_set["sequential"] == {
        "memory": 0.1,
        "runtime": 0.1,
    }
    assert tt_1.default_compute_resources_set["sequential"] == {
        "num_cores": 1,
        "m_mem_free": "3G",
        "max_runtime_seconds": "(60 * 60 * 4)",
        "queue": "null.q",
    }
    assert tt_2.default_compute_resources_set["sequential"] == {
        "num_cores": 3,
        "m_mem_free": "2G",
        "max_runtime_seconds": "(60 * 60 * 24)",
        "queue": "null.q",
    }
    assert tt_1.default_resource_scales_set["sequential"] == {
        "memory": 0.2,
        "runtime": 0.3,
    }
    assert tt_2.default_resource_scales_set["sequential"] == {
        "memory": 0.4,
        "runtime": 0.6,
    }
