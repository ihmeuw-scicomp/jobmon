"""Shared test fixtures for the Jobmon test suite.

This module organizes fixtures by category for better maintainability:

- database.py: Database engine and session fixtures
- server.py: Web server process and client connection fixtures
- workflows.py: Tool, task template, and workflow fixtures

Usage in conftest.py:
    from tests.fixtures.database import db_engine
    from tests.fixtures.server import web_server_process, client_env
    from tests.fixtures.workflows import tool, task_template

Or import all fixtures:
    from tests.fixtures import *
"""

# Re-export all fixtures for convenient importing
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
from tests.fixtures.workflows import (
    _get_task_template,
    array_template,
    task_template,
    tool,
)

__all__ = [
    # Database
    "setup_test_environment",
    "db_engine",
    "dbsession",
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
