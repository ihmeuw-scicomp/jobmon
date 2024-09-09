"""Routes used to move through the finite state."""

from fastapi import APIRouter
from importlib import import_module

from jobmon.server.web.api import get_app

fsm_router = APIRouter(tags=["fsm"])

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
    import_module(f"jobmon.server.web.routes.v3.fsm.{module}")
