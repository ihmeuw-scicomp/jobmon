from importlib import import_module

from fastapi import APIRouter

cli_router = APIRouter(tags=["cli"])

for module in ["array", "task", "task_template", "workflow"]:
    import_module(f"jobmon.server.web.routes.v2.cli.{module}")
