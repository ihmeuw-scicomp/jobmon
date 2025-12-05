from jobmon.client.tool import Tool


def test_load_task_templates(client_env):
    # from jobmon.client.tool_version import ToolVersion
    # from jobmon.client.task_template import TaskTemplate

    tool = Tool()
    tool_version_id = tool.get_new_tool_version()

    # Create a task template on one Tool object.
    tt1 = tool.get_task_template("foo", "{bar} {baz} --hello", node_args=["bar", "baz"])
    tt1.create_task(bar=1, baz="september")

    # Prove that from a second Tool object, we can access the same task template
    # created above, and still create a task from it.
    tool2 = Tool()
    tt2 = tool2.active_tool_version.get_task_template("foo")
    tt2.create_task(bar=2, baz="october")

    # Further prove that a fresh Tool object can find an equivalent task-template to the one
    # we originally created, if we set its active tool version.
    tool1 = Tool()
    tool1.set_active_tool_version_id(tool_version_id)
    assert tool.active_task_templates["foo"] == tt1
