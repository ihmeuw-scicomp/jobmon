from jobmon.client.tool import Tool


def test_load_task_templates(client_env):
    # from jobmon.client.tool_version import ToolVersion
    # from jobmon.client.task_template import TaskTemplate

    tool = Tool()
    tool_version_id = tool.get_new_tool_version()
    tt1 = tool.get_task_template("foo", "{bar} {baz} --hello")

    tool1 = Tool()
    tool1.set_active_tool_version_id(tool_version_id)
    assert tool.active_task_templates["foo"] == tt1
