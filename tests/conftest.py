import logging
import multiprocessing as mp
import os
import pathlib
import platform
import signal
import sys
import tempfile
from time import sleep
from types import TracebackType
from typing import Any, Optional

import pytest
import requests
import uvicorn
from sqlalchemy.engine import Engine

# SET UP TEST ENVIRONMENT VARIABLES BEFORE ANY JOBMON IMPORTS
# This must happen before jobmon modules are imported because load_dotenv()
# runs at module level in jobmon.core.configuration

# Simple, elegant subprocess detection using process ID
_main_process_pid = os.getpid()


def _is_main_test_process():
    """Check if we're in the main test process (not a subprocess)."""
    return os.getpid() == _main_process_pid


def _setup_test_environment():
    """Set up complete test environment in pytest_sessionstart."""
    if not _is_main_test_process():
        print("Subprocess detected: Using test environment from parent process")
        return None

    # We're in the main process - create new database
    tmp_dir = tempfile.mkdtemp()
    sqlite_file = pathlib.Path(tmp_dir, "tests.sqlite").resolve()

    print("Setting up complete test environment")
    print(f"SQLite file created at: {sqlite_file}")

    # Set complete test environment variables
    complete_test_vars = {
        # Core database configuration
        "JOBMON__DB__SQLALCHEMY_DATABASE_URI": f"sqlite:////{sqlite_file}",
        "JOBMON__DB__SQLALCHEMY_CONNECT_ARGS": "{}",  # No SSL for SQLite
        # Essential test settings
        "JOBMON__AUTH__ENABLED": "false",
        "JOBMON__HTTP__ROUTE_PREFIX": "/api/v3",
        "JOBMON__SESSION__SECRET_KEY": "test",
        # Performance optimizations for faster tests
        "JOBMON__HTTP__STOP_AFTER_DELAY": "0",
        "JOBMON__HTTP__RETRIES_TIMEOUT": "0",
        "JOBMON__DISTRIBUTOR__POLL_INTERVAL": "1",
        "JOBMON__HEARTBEAT__WORKFLOW_RUN_INTERVAL": "1",
        "JOBMON__HEARTBEAT__TASK_INSTANCE_INTERVAL": "1",
    }

    os.environ.update(complete_test_vars)
    print(f"Set {len(complete_test_vars)} test environment variables")
    return sqlite_file


# Now safe to import jobmon modules - load_dotenv() is skipped during tests
_api_prefix = "/api/v3"

# Import jobmon modules
from jobmon.client.api import Tool
from jobmon.core.requester import Requester

logger = logging.getLogger(__name__)


def pytest_sessionstart(session):
    """Set up complete test environment and reset singletons for clean test state."""
    print("=== pytest_sessionstart: Setting up test environment ===")

    # Complete the test environment setup
    _setup_test_environment()

    print("=== Test environment setup complete ===")

    # Log current test configuration for debugging
    db_uri = os.environ.get("JOBMON__DB__SQLALCHEMY_DATABASE_URI", "NOT_SET")
    print(f"Using database: {db_uri}")
    print(f"Auth enabled: {os.environ.get('JOBMON__AUTH__ENABLED', 'NOT_SET')}")


@pytest.fixture(scope="session")
def api_prefix():
    return _api_prefix


@pytest.fixture(scope="session")
def db_engine() -> Engine:
    from jobmon.server.web.config import get_jobmon_config

    config = get_jobmon_config()
    from jobmon.server.web.db import init_db

    # Verify that our test environment setup worked
    db_uri = config.get("db", "sqlalchemy_database_uri")
    print(f"Database URI from config: {db_uri}")
    assert "sqlite" in db_uri, f"Expected SQLite URI but got: {db_uri}"

    init_db()  # Then initialize DB (runs migrations + metadata load)

    # verify db created
    from jobmon.server.web.db import get_engine

    eng = get_engine()  # Use the configured engine with WAL mode
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
        # Always use localhost for test server to avoid DNS/network issues
        # The test server runs locally regardless of platform
        self.web_host = "127.0.0.1"
        self.web_port = str(10_000 + os.getpid() % 30_000)
        self.api_prefix = _api_prefix

    def _run_server_with_handler(self) -> None:
        """Run the server with signal handlers - separate method for pickle compatibility."""

        def sigterm_handler(_signo: int, _stack_frame: Any) -> None:
            # catch SIGTERM and shut down with 0 so pycov finalizers are run
            # Raises SystemExit(0):
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)

        from jobmon.server.web import log_config
        from jobmon.server.web.api import get_app

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

        app = get_app(versions=["v3"])
        uvicorn.run(app, host="0.0.0.0", port=int(self.web_port))

    def __enter__(self) -> Any:
        """Starts the web service process."""
        # start server
        # Use spawn on macOS to avoid fork warnings in multi-threaded environment
        mp_method = "spawn" if platform.system() == "Darwin" else "fork"
        ctx = mp.get_context(mp_method)
        self.p1 = ctx.Process(target=self._run_server_with_handler)
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
    """Configure client to connect to the local test server."""
    # Set the dynamic service URL to point to the local test server
    service_url = f'http://{web_server_process["JOBMON_HOST"]}:{web_server_process["JOBMON_PORT"]}'
    monkeypatch.setenv("JOBMON__HTTP__SERVICE_URL", service_url)

    # Create requester instance that will use the test configuration
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
