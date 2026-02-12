"""Root conftest.py for Jobmon test suite.

This file contains:
1. pytest_configure hook for environment setup (required here for xdist)
2. Fixture imports from the fixtures/ module

Fixtures are organized in:
- fixtures/database.py: Database engine and session fixtures
- fixtures/server.py: Web server process and client connection fixtures
- fixtures/workflows.py: Tool, task template, and workflow fixtures
"""

import os
import pathlib
import tempfile

# =============================================================================
# PYTEST CONFIGURATION HOOK
# =============================================================================
# This MUST stay in conftest.py - it's a pytest hook that runs before collection


def pytest_configure(config):
    """Configure pytest - set up environment variables before any imports happen.

    IMPORTANT for pytest-xdist:
    - Main process sets up placeholder environment for test collection
    - Each worker process inherits environment but needs its OWN database
    - Workers are identified by PYTEST_XDIST_WORKER env var (gw0, gw1, etc.)
    - Each worker creates a unique database in the db_engine fixture
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    is_worker = worker_id is not None

    # Set up environment: either first time (main) or reset for worker
    if not os.environ.get("JOBMON__DB__SQLALCHEMY_DATABASE_URI") or is_worker:
        # Create unique temp directory for this process
        tmp_dir = tempfile.mkdtemp()

        if is_worker:
            # Worker: use worker-specific database name
            temp_sqlite_file = pathlib.Path(
                tmp_dir, f"tests_{worker_id}.sqlite"
            ).resolve()
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


# =============================================================================
# FIXTURE IMPORTS
# =============================================================================
# Import all fixtures from the fixtures module to make them available to tests.
# Pytest automatically discovers fixtures in conftest.py and its imports.

from tests.fixtures.database import (
    db_engine,
    dbsession,
    setup_test_environment,
)
from tests.fixtures.server import (
    WebServerProcess,
    api_prefix,
    client_env,
    requester_no_retry,
    web_server_process,
)
from tests.fixtures.workflows import _get_task_template as get_task_template
from tests.fixtures.workflows import (
    array_template,
    task_template,
    tool,
)

# Re-export for backwards compatibility with any code that imports from conftest
__all__ = [
    # Database
    "setup_test_environment",
    "db_engine",
    "dbsession",
    # Server
    "WebServerProcess",
    "api_prefix",
    "web_server_process",
    "client_env",
    "requester_no_retry",
    # Workflows
    "tool",
    "task_template",
    "array_template",
    "get_task_template",
]
