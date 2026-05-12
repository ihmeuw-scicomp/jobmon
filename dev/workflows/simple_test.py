# Run locally against the Dockerized backend:
#
#   1. Bring up the stack (from repo root):
#        docker compose up -d
#        docker compose exec jobmon_backend jobmon_server db init
#
#   2. Run from the host venv:
#        uv run python dev/workflows/simple_test.py
#
# JOBMON__HTTP__SERVICE_URL below points at the host-published backend port
# (8070 -> container 80). To run inside the jobmon_client container instead,
# change the URL to "http://jobmon_backend:80" and run:
#   docker compose exec jobmon_client python /app/test_scripts/simple_test.py

import os
os.environ["JOBMON__TELEMETRY__TRACING__REQUESTER_ENABLED"] = "false"
os.environ["JOBMON__TELEMETRY__LOGGING__ENABLED"] = "false"
os.environ["JOBMON__HTTP__SERVICE_URL"] = "http://localhost:8070"

from jobmon.client.api import Tool

tool = Tool("simple_test")
workflow = tool.create_workflow(
    name="simple_test_wf",
    default_cluster_name="sequential",
    default_compute_resources_set={"sequential": {"queue": "null.q"}},
)

tt = tool.get_task_template(
    template_name="echo_template",
    command_template="echo 1",
)
task = tt.create_task()
workflow.add_tasks([task])

status = workflow.run(configure_logging=True)
if status != "D":
    raise RuntimeError(f"Workflow finished with status {status}")
