"""Initialize Web services.

This file spins up a web server on http://localhost:8070 with database jobmon.db.
The base URL for the web server is http://localhost:8070/api/v3.

It also provides the option to create a workflow for testing.
Pass in any argument to create a workflow.
The workflow runs on the sequential cluster with the null.q queue.
It does not require slurm cluster access, and can run on your Mac.
"""
import signal
import sys
from typing import Any, Optional
import uvicorn
import multiprocessing as mp
import os

import sqlalchemy

database_uri = "sqlite:///jobmon.db"

def config_db() -> bool:
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.core.configuration import JobmonConfig

    config = JobmonConfig(
        dict_config={
            "db": {"sqlalchemy_database_uri": database_uri},
            "otlp": {
                "web_enabled": "false",
                "span_exporter": "",
                "log_exporter": "",
            },
        }
    )
    get_jobmon_config(config)
    from jobmon.server.web.api import configure_logging
    from jobmon.server.web.db_admin import init_db
    from jobmon.server.web.models import load_model

    init_db()
    load_model()
    configure_logging(
        loggers_dict={
            "jobmon.server.web": {
                "handlers": ["console_text"],
                "level": "INFO",
            },
            # enable SQL debug
            "sqlalchemy": {
                "handlers": ["console_text"],
                "level": "WARNING",
            },
        }
    )
    # verify db created
    eng = sqlalchemy.create_engine(database_uri)
    from sqlalchemy.orm import Session

    with Session(eng) as session:
        from sqlalchemy import text

        res = session.execute(text("SELECT * from workflow_status")).fetchall()
        return len(res) > 0


def run_server_with_handler() -> None:
    def sigterm_handler(_signo: int, _stack_frame: Any) -> None:
        # catch SIGTERM and shut down with 0 so pycov finalizers are run
        # Raises SystemExit(0):
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)
    from jobmon.server.web.api import get_app

    app = get_app()
    uvicorn.run(app, host="0.0.0.0", port=8070)



if __name__ == "__main__":
    print("Starting server")
    if config_db():
        run_server_with_handler()
