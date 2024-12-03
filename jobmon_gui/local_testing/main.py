"""Initialize Web services.

This file spins up a web server on http://localhost:8070 with database jobmon.db.
The base URL for the web server is http://localhost:8070/api/v3.

It also provides the option to create a workflow for testing.
Pass in any argument to create a workflow.
The workflow runs on the sequential cluster with the null.q queue.
It does not require slurm cluster access, and can run on your Mac.
"""
import os
import string
from random import choices
import uvicorn


def set_environment():
    os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"] = "sqlite:////tmp/tests.sqlite"
    os.environ["JOBMON__OTLP__WEB_ENABLED"] = "false"
    os.environ["JOBMON__OTLP__SPAN_EXPORTER"] = ""
    os.environ["JOBMON__OTLP__LOG_EXPORTER"] = ""
    os.environ["JOBMON__SESSION__SECRET_KEY"] = ''.join(choices(string.ascii_letters + string.digits, k=16))

def main():
    from jobmon.core.configuration import JobmonConfig
    from jobmon.server.web.api import get_app
    from jobmon.server.web.db_admin import init_db

    init_db()
    app = get_app()
    uvicorn.run(app, host="0.0.0.0", port=8070)

if __name__ == "__main__":
    set_environment()
    main()
