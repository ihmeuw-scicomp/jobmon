from importlib import import_module

from fastapi import APIRouter

fsm_router = APIRouter(tags=["fsm"])

# Import routes that didnt change in v2 first
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

# import the unique v1 routes last
for module in [
    "task_instance",
    "workflow_run",
]:
    import_module(f"jobmon.server.web.routes.v1.fsm.{module}")
