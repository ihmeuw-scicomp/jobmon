from importlib import import_module

from fastapi import APIRouter

cli_router = APIRouter(tags=["cli"])
# import routes from v2 that did not change
for module in ["array", "task", "task_template", "workflow"]:
    import_module(f"jobmon.server.web.routes.v2.cli.{module}")
