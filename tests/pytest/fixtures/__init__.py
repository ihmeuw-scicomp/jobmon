"""Shared test fixtures for the Jobmon test suite.

This module organizes fixtures by category for better maintainability:

- database.py: Database engine and session fixtures
- server.py: Web server process and client connection fixtures
- workflows.py: Tool, task template, and workflow fixtures

Usage in conftest.py:
    from tests.pytest.fixtures.database import db_engine
    from tests.pytest.fixtures.server import web_server_process, client_env
    from tests.pytest.fixtures.workflows import tool, task_template

Or import all fixtures:
    from tests.pytest.fixtures import *
"""

# Re-export all fixtures for convenient importing
from tests.pytest.fixtures.database import (
    db_engine,
    setup_test_environment,
)
from tests.pytest.fixtures.server import (
    WebServerProcess,
    api_prefix,
    client_env,
    requester_no_retry,
    web_server_process,
)
from tests.pytest.fixtures.workflows import (
    _get_task_template,
    array_template,
    task_template,
    tool,
)

__all__ = [
    # Database
    "setup_test_environment",
    "db_engine",
    # Server
    "WebServerProcess",
    "web_server_process",
    "client_env",
    "api_prefix",
    "requester_no_retry",
    # Workflows
    "tool",
    "task_template",
    "array_template",
    "_get_task_template",
]
