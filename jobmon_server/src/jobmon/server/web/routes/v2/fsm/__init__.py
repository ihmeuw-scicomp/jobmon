"""Routes used to move through the finite state."""

from importlib import import_module

for module in [
    "array",
    "cluster",
    "dag",
    "node",
    "queue",
    "task",
    "task_instance",
    "task_template",
    "task_resources",
    "tool",
    "tool_version",
    "workflow",
    "workflow_run",
]:
    import_module(f"jobmon.server.web.routes.v2.fsm.{module}")
