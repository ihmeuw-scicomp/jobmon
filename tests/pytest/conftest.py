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
    """Configure pytest - set up environment variables before any imports happen.
    
    IMPORTANT for pytest-xdist:
    - Main process sets up placeholder environment for test collection
    - Each worker process inherits environment but needs its OWN database
    - Workers are identified by PYTEST_XDIST_WORKER env var (gw0, gw1, etc.)
    - Each worker creates a unique database in setup_test_environment fixture
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    is_worker = worker_id is not None
    
    # Workers ALWAYS need setup, even if they inherited env vars from main process
    # The key insight: workers inherit the main process's environment, but the
    # main process's database is NOT initialized (it's just a placeholder).
    # Each worker must create and initialize its own database.
    if is_worker:
        # Reset any cached config from main process
        try:
            import jobmon.server.web.config as config_module
            config_module._jobmon_config = None
        except ImportError:
            pass
        try:
            import jobmon.server.web.db.engine as engine_module
            engine_module._engine = None
            engine_module._SessionMaker = None
        except ImportError:
            pass
    
    # Set up environment: either first time (main) or reset for worker
    if not os.environ.get("JOBMON__DB__SQLALCHEMY_DATABASE_URI") or is_worker:
        # Create unique temp directory for this process
        tmp_dir = tempfile.mkdtemp()
        
        if is_worker:
            # Worker: use worker-specific database name
            temp_sqlite_file = pathlib.Path(tmp_dir, f"tests_{worker_id}.sqlite").resolve()
        else:
            # Main process: placeholder for test collection
            temp_sqlite_file = pathlib.Path(tmp_dir, "temp_collection.sqlite").resolve()

        # Set minimal environment variables needed for module imports
        test_vars = {
            "JOBMON__DB__SQLALCHEMY_DATABASE_URI": f"sqlite:////{temp_sqlite_file}",
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
        os.environ.update(test_vars)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment - runs automatically at session start.
    
    Database URI and singletons are already configured in pytest_configure.
    This fixture ensures singletons are clean for the session and provides
    the database path for other fixtures.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    db_uri = os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"]
    
    # Extract SQLite file path from URI (remove 'sqlite:////' prefix)
    sqlite_file = pathlib.Path(db_uri[11:])

    print(f"Worker {worker_id}: Setting up test environment")
    print(f"Worker {worker_id}: SQLite file at {sqlite_file}")

    # Reset singletons for clean test state (may have been set in pytest_configure,
    # but we reset again here to ensure clean state at fixture resolution time)
    import jobmon.server.web.config as config_module
    config_module._jobmon_config = None

    import jobmon.server.web.db.engine as engine_module
    engine_module._engine = None
    engine_module._SessionMaker = None

    print(f"Worker {worker_id}: Environment setup complete")
    yield sqlite_file
    print(f"Worker {worker_id}: Environment teardown")


@pytest.fixture(scope="session")
def api_prefix():
    return _api_prefix


@pytest.fixture(scope="session")
def db_engine(setup_test_environment) -> Engine:
    """Initialize and return the database engine.
    
    CRITICAL: Reset singletons before initialization to prevent stale
    references from earlier fixture resolution or other workers.
    """
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.server.web.db import init_db
    import jobmon.server.web.db.engine as engine_module
    import jobmon.server.web.config as config_module

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    
    # CRITICAL: Reset singletons to prevent stale references
    # This is necessary because fixtures might run in unexpected order
    # with pytest-xdist, and singletons could be initialized with wrong values
    config_module._jobmon_config = None
    engine_module._engine = None
    engine_module._SessionMaker = None
    
    config = get_jobmon_config()
    db_uri = config.get("db", "sqlalchemy_database_uri")
    assert "sqlite" in db_uri, f"Expected SQLite URI but got: {db_uri}"

    # Initialize database - each worker has its own, so no race conditions
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
