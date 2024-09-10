from fastapi import APIRouter
from importlib import import_module

cli_router = APIRouter(tags=["cli"])

for module in ["array", "task", "task_template", "workflow"]:
    import_module(f"jobmon.server.web.routes.v3.cli.{module}")
