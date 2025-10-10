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

# Import jobmon modules
from jobmon.client.api import Tool
from jobmon.core.requester import Requester

logger = logging.getLogger(__name__)

# Global API prefix
_api_prefix = "/api/v3"


def pytest_configure(config):
    """Configure pytest - set up environment variables before any imports happen."""
    # Only set up database config during test collection/execution
    # This ensures environment variables are available when modules are imported
    if not os.environ.get("JOBMON__DB__SQLALCHEMY_DATABASE_URI"):
        # Set a temporary database URI for test collection/imports
        # This will be updated later in setup_test_environment with proper worker isolation
        tmp_dir = tempfile.mkdtemp()
        temp_sqlite_file = pathlib.Path(tmp_dir, "temp_collection.sqlite").resolve()

        # Set minimal environment variables needed for module imports
        test_vars = {
            # Temporary database URI for collection - will be updated in session fixture
            "JOBMON__DB__SQLALCHEMY_DATABASE_URI": f"sqlite:////{temp_sqlite_file}",
            "JOBMON__DB__SQLALCHEMY_CONNECT_ARGS": "{}",  # No SSL for SQLite
            "JOBMON__DB__NEEDS_WORKER_SETUP": "true",  # Flag to update URI in session fixture
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

        os.environ.update(test_vars)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment with per-worker databases - runs automatically."""
    # Now worker ID should be properly set by pytest-xdist
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")

    # Set up unique database for this worker if needed
    if os.environ.get("JOBMON__DB__NEEDS_WORKER_SETUP"):
        # Create unique database per worker (different from temp collection DB)
        tmp_dir = tempfile.mkdtemp()
        sqlite_file = pathlib.Path(tmp_dir, f"tests_{worker_id}.sqlite").resolve()

        # Update the database URI now that we have the correct worker ID
        os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"] = f"sqlite:////{sqlite_file}"

        # Clean up the setup flag
        del os.environ["JOBMON__DB__NEEDS_WORKER_SETUP"]
    else:
        # Database URI was already set (non-xdist run or already configured)
        db_uri = os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"]
        sqlite_file = pathlib.Path(db_uri[11:])  # Remove 'sqlite:////' prefix

    print(f"Worker {worker_id}: Setting up test environment")
    print(f"Worker {worker_id}: SQLite file at {sqlite_file}")

    # Reset singletons for clean test state - important after URI change
    import jobmon.server.web.config as config_module

    config_module._jobmon_config = None

    import jobmon.server.web.db.engine as engine_module

    engine_module._engine = None
    engine_module._SessionMaker = None

    print(f"Worker {worker_id}: Environment setup complete")
    yield sqlite_file  # This becomes the fixture value
    print(f"Worker {worker_id}: Environment teardown")


@pytest.fixture(scope="session")
def api_prefix():
    return _api_prefix


@pytest.fixture(scope="session")
def db_engine(setup_test_environment) -> Engine:
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.server.web.db import init_db

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    config = get_jobmon_config()

    # Verify configuration
    db_uri = config.get("db", "sqlalchemy_database_uri")
    print(f"Worker {worker_id}: Database URI from config: {db_uri}")
    assert "sqlite" in db_uri, f"Expected SQLite URI but got: {db_uri}"

    # Initialize database - each worker has its own, so no race conditions
    print(f"Worker {worker_id}: Initializing database")
    init_db()

    # Create and return engine
    from jobmon.server.web.db import get_engine

    eng = get_engine()  # Use the configured engine with WAL mode

    # Verify database has expected tables
    from sqlalchemy.orm import Session

    with Session(eng) as session:
        from sqlalchemy import text

        res = session.execute(text("SELECT * from workflow_status")).fetchall()
        assert len(res) > 0, f"Worker {worker_id}: Database not properly initialized"

    print(f"Worker {worker_id}: Database ready with {len(res)} workflow statuses")
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

        from jobmon.core.config.logconfig_utils import configure_component_logging
        from jobmon.core.config.structlog_config import configure_structlog
        from jobmon.server.web.api import get_app

        # Configure logging using core utilities
        configure_component_logging("server")
        configure_structlog(component_name="server")

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
