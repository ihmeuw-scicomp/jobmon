import logging
import multiprocessing as mp
import os
import platform
import requests
import signal
import socket
import subprocess
import sys
from time import sleep
from types import TracebackType
from typing import Any, Optional


import pytest
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine

from jobmon.client.api import Tool
from jobmon.core import requester
from jobmon.core.requester import Requester
from jobmon.server.web.api import get_app, JobmonConfig, configure_logging
from jobmon.server.web.models import init_db

logger = logging.getLogger(__name__)


class WebServerProcess:
    """Context manager creates the Jobmon web server in a process and tears it down on exit."""

    def __init__(self, filepath: str) -> None:
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
        self.filepath = filepath

    def __enter__(self) -> Any:
        """Starts the web service process."""
        # jobmon_cli string
        database_uri = f"sqlite:///{self.filepath}"

        def run_server_with_handler() -> None:
            def sigterm_handler(_signo: int, _stack_frame: Any) -> None:
                # catch SIGTERM and shut down with 0 so pycov finalizers are run
                # Raises SystemExit(0):
                sys.exit(0)

            signal.signal(signal.SIGTERM, sigterm_handler)

            init_db(create_engine(database_uri))

            config = JobmonConfig(
                dict_config={"db": {"sqlalchemy_database_uri": database_uri}}
            )
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
            app = get_app(config)
            with app.app_context():
                app.run(host="0.0.0.0", port=self.web_port)

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
                r = requests.get(f"http://{self.web_host}:{self.web_port}/health")
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


@pytest.fixture(scope="session", autouse=True)
def set_mac_to_fork():
    """necessary for running tests on a mac with python 3.8 see:
    https://github.com/pytest-dev/pytest-flask/issues/104"""
    if platform.system() == "Darwin":
        import multiprocessing

        multiprocessing.set_start_method("fork")


@pytest.fixture(scope="session")
def sqlite_file(tmpdir_factory) -> str:
    file = str(tmpdir_factory.mktemp("db").join("tests.sqlite"))
    return file


@pytest.fixture(scope="session")
def web_server_process(sqlite_file):
    """This starts the flask dev server in separate processes"""
    with WebServerProcess(sqlite_file) as web:
        yield {"JOBMON_HOST": web.web_host, "JOBMON_PORT": web.web_port}


@pytest.fixture(scope="session")
def db_engine(sqlite_file) -> Engine:
    return sqlalchemy.create_engine(f"sqlite:///{sqlite_file}")


@pytest.fixture(scope="function")
def client_env(web_server_process, monkeypatch):
    monkeypatch.setenv(
        "JOBMON__HTTP__SERVICE_URL",
        f'http://{web_server_process["JOBMON_HOST"]}:{web_server_process["JOBMON_PORT"]}',
    )
    monkeypatch.setenv("JOBMON__HTTP__STOP_AFTER_DELAY", "0")

    # This instance is thrown away, hence monkey-patching the defaults via the
    # environment variables
    requester = Requester.from_defaults()
    yield requester.url


@pytest.fixture(scope="function")
def requester_no_retry(client_env):
    return Requester(client_env, stop_after_delay=0)


@pytest.fixture(scope="function")
def web_server_in_memory(sqlite_file, monkeypatch):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server.web.app_factory import AppFactory

    # The create_app call sets up database connections
    database_uri = f"sqlite:///{sqlite_file}"
    monkeypatch.setenv(
        "JOBMON__FLASK__SQLALCHEMY_DATABASE_URI", database_uri
    )
    config = JobmonConfig(
        dict_config={"db": {"sqlalchemy_database_uri": database_uri}}
    )
    app_factory = AppFactory(config=config)
    app = app_factory.get_app()
    app.config["TESTING"] = True
    with app.app_context():
        client = app.test_client()
        yield client, app_factory.engine


def get_test_content(response):
    """The function called by the no_request_jsm_jqs to query the fake
    test_client for a response
    """
    if "application/json" in response.headers.get("Content-Type"):
        content = response.json
    elif "text/html" in response.headers.get("Content-Type"):
        content = response.data
    else:
        content = response.content
    return response.status_code, content


@pytest.fixture(scope="function")
def requester_in_memory(monkeypatch, web_server_in_memory):
    """This function monkeypatches the requests library to use the
    test_client
    """

    monkeypatch.setenv("JOBMON__HTTP__SERVICE_URL", "1")

    app, engine = web_server_in_memory

    def get_in_mem(url, params, data, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return app.get(path=url, query_string=params, data=data, headers=headers)

    def post_in_mem(url, params, json, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return app.post(url, query_string=params, json=json, headers=headers)

    def put_in_mem(url, params, json, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return app.put(url, query_string=params, json=json, headers=headers)

    monkeypatch.setattr(requests, "get", get_in_mem)
    monkeypatch.setattr(requests, "post", post_in_mem)
    monkeypatch.setattr(requests, "put", put_in_mem)
    monkeypatch.setattr(requester, "get_content", get_test_content)


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


@pytest.fixture(scope='session')
def remote_sequential_distributor(web_server_process):

    cmd = [
        sys.executable,
        "-m",  # safest way to find the entrypoint
        "jobmon.distributor.cli",
        "start",
        "--cluster_name",
        "sequential"
    ]

    env = os.environ.copy()
    env.update(
        {"JOBMON__HTTP__SERVICE_URL":
            f'http://{web_server_process["JOBMON_HOST"]}:'
            f'{web_server_process["JOBMON_PORT"]}'
        }
    )

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    yield process

    # Clean up the subprocess
    process.terminate()
    process.wait()
