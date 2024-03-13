from importlib import import_module


for module in ["array", "task", "task_template", "workflow"]:
    import_module(f"jobmon.server.web.routes.v2.cli.{module}")
