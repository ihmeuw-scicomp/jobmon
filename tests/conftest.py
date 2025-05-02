import logging
import multiprocessing as mp
import os
import requests
import signal
import socket
import sys
import tempfile
from time import sleep
from types import TracebackType
from typing import Any, Optional
import uvicorn
import pathlib

import pytest
import sqlalchemy
from sqlalchemy.engine import Engine

from jobmon.client.api import Tool
from jobmon.core.requester import Requester

logger = logging.getLogger(__name__)

_api_prefix = "/api/v2"


def pytest_sessionstart(session):
    # Create a unique SQLite file in a temporary directory
    tmp_dir = tempfile.mkdtemp()
    # Resolve the path to be absolute *before* creating the URI
    sqlite_file = pathlib.Path(tmp_dir, "tests.sqlite").resolve()

    # Print information for debugging purposes
    print("Running code before test file import")
    print(f"SQLite file created at: {sqlite_file}")

    # Use four slashes for an absolute path SQLite URI
    os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"] = f"sqlite:////{sqlite_file}"
    os.environ["JOBMON__OTLP__WEB_ENABLED"] = "false"
    os.environ["JOBMON__OTLP__SPAN_EXPORTER"] = ""
    os.environ["JOBMON__OTLP__LOG_EXPORTER"] = ""
    os.environ["JOBMON__SESSION__SECRET_KEY"] = "test"


@pytest.fixture(scope="session")
def api_prefix():
    return _api_prefix


@pytest.fixture(scope="session")
def db_engine() -> Engine:
    from jobmon.server.web.config import get_jobmon_config

    config = get_jobmon_config()
    from jobmon.server.web.db import init_db

    # load_model()  # Remove this, as env.py already calls it
    init_db()  # Then initialize DB (runs migrations + metadata load)

    # verify db created
    eng = sqlalchemy.create_engine(config.get("db", "sqlalchemy_database_uri"))
    from sqlalchemy.orm import Session

    with Session(eng) as session:
        from sqlalchemy import text

        res = session.execute(text("SELECT * from workflow_status")).fetchall()
        assert len(res) > 0
    return eng


class WebServerProcess:
    """Context manager creates the Jobmon web server in a process and tears it down on exit."""

    def __init__(self) -> None:
        """Initializes the web server process.

        Args:
            ephemera: a dictionary containing the connection information for the database,
            specifically the database host, port, service account user, service account
            password, and database name
        """
        if sys.platform == "darwin":
            self.web_host = "127.0.0.1"
        else:
            self.web_host = socket.getfqdn()
        self.web_port = str(10_000 + os.getpid() % 30_000)
        self.api_prefix = _api_prefix

    def __enter__(self) -> Any:
        """Starts the web service process."""
        # jobmon_cli string

        def run_server_with_handler() -> None:
            def sigterm_handler(_signo: int, _stack_frame: Any) -> None:
                # catch SIGTERM and shut down with 0 so pycov finalizers are run
                # Raises SystemExit(0):
                sys.exit(0)

            signal.signal(signal.SIGTERM, sigterm_handler)

            from jobmon.server.web.api import get_app
            from jobmon.server.web import log_config

            dict_config = {
                "version": 1,
                "disable_existing_loggers": True,
                "formatters": log_config.default_formatters.copy(),
                "handlers": log_config.default_handlers.copy(),
                "loggers": {
                    "jobmon.server.web": {
                        "handlers": ["console_text"],
                        "level": "INFO",
                    },
                    # enable SQL debug
                    "sqlalchemy": {
                        "handlers": ["console_text"],
                        "level": "WARNING",
                    },
                },
            }
            log_config.configure_logging(dict_config=dict_config)

            app = get_app(versions=["v2"])
            uvicorn.run(app, host="0.0.0.0", port=int(self.web_port))

        # start server
        ctx = mp.get_context("fork")
        self.p1 = ctx.Process(target=run_server_with_handler)
        self.p1.start()

        # Wait for it to be up
        status = 404
        count = 0
        # We try a total of 10 times with 3 seconds between tries. If the web service is not up
        # in 30 seconds something is likely wrong.
        max_tries = 10
        while not status == 200 and count < max_tries:
            try:
                count += 1
                url = f"http://{self.web_host}:{self.web_port}{self.api_prefix}/health"
                print(url)
                r = requests.get(
                    url,
                    headers={"Content-Type": "application/json"},
                )

                status = r.status_code
            except Exception as e:
                # Connection failures land here
                # Safe to catch all because there is a max retry
                if count >= max_tries:
                    raise TimeoutError(
                        f"Out-of-process jobmon services did not answer after "
                        f"{count} attempts, probably failed to start."
                    ) from e
            # sleep outside of try block!
            sleep(3)
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        """Terminate the web service process."""
        # interrupt and join for coverage
        self.p1.terminate()
        self.p1.join()


@pytest.fixture(scope="session")
def web_server_process(db_engine):
    """This starts the flask dev server in separate processes"""
    with WebServerProcess() as web:
        yield {"JOBMON_HOST": web.web_host, "JOBMON_PORT": web.web_port}


@pytest.fixture(scope="function")
def client_env(web_server_process, monkeypatch):
    monkeypatch.setenv(
        "JOBMON__HTTP__SERVICE_URL",
        f'http://{web_server_process["JOBMON_HOST"]}:{web_server_process["JOBMON_PORT"]}',
    )
    monkeypatch.setenv("JOBMON__HTTP__ROUTE_PREFIX", _api_prefix)
    monkeypatch.setenv("JOBMON__HTTP__STOP_AFTER_DELAY", "0")
    monkeypatch.setenv("JOBMON__HTTP__RETRIES_TIMEOUT", "0")
    monkeypatch.setenv("JOBMON__DISTRIBUTOR__POLL_INTERVAL", "1")
    monkeypatch.setenv("JOBMON__HEARTBEAT__WORKFLOW_RUN_INTERVAL", "1")
    monkeypatch.setenv("JOBMON__HEARTBEAT__TASK_INSTANCE_INTERVAL", "1")

    # This instance is thrown away, hence monkey-patching the defaults via the
    # environment variables
    requester = Requester.from_defaults()
    yield requester.url


@pytest.fixture(scope="function")
def requester_no_retry(client_env):
    return Requester(client_env, retries_timeout=0)


def get_task_template(tool, template_name):
    tool.get_task_template(
        template_name=template_name,
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )


# TODO: This tool and the subsequent fixtures should probably be session scoped
@pytest.fixture
def tool(client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tool.get_task_template(
        template_name="array_template",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    get_task_template(tool, "phase_1")
    get_task_template(tool, "phase_2")
    get_task_template(tool, "phase_3")
    return tool


@pytest.fixture
def task_template(tool):
    return tool.active_task_templates["simple_template"]


@pytest.fixture
def array_template(tool):
    return tool.active_task_templates["array_template"]
