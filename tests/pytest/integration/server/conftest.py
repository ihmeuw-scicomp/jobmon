"""Server integration test fixtures.

These fixtures provide in-memory web server testing capabilities
and logging utilities for server tests.
"""

import logging

import pytest
import structlog
from sqlalchemy.orm import sessionmaker


class StructlogJSONTestFormatter(logging.Formatter):
    """Custom JSON formatter for tests using structlog."""

    def __init__(self) -> None:
        """Initialize JSON formatter with structlog."""
        super().__init__()
        self._structlog_formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(), foreign_pre_chain=[]
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format using structlog JSON renderer."""
        return self._structlog_formatter.format(record)


@pytest.fixture(scope="function")
def web_server_in_memory(db_engine):
    """Create an in-memory FastAPI test client with database configured.

    This fixture creates a FastAPI TestClient with the database engine,
    sessionmaker, and dialect properly configured in app.state, matching
    what the db_lifespan would do in production.

    Args:
        db_engine: The database engine fixture from database.py

    Yields:
        Tuple[TestClient, Engine]: The test client and database engine
    """
    from fastapi.testclient import TestClient

    from jobmon.server.web.api import get_app

    app = get_app(versions=["v3"])

    # Set up app.state to match what db_lifespan does in production
    app.state.db_engine = db_engine
    app.state.db_dialect = db_engine.dialect.name.lower()
    app.state.db_sessionmaker = sessionmaker(
        bind=db_engine, autoflush=False, autocommit=False
    )

    client = TestClient(app)
    yield client, db_engine


@pytest.fixture(scope="function")
def json_log_file(tmp_path):
    """Fixture that sets up JSON logging to a temporary file.

    This fixture provides a flexible way to configure structured logging for tests
    that need to capture and verify log output from specific loggers.

    Args:
        loggers: Dict of logger names to log levels (e.g., {"jobmon.server.web": "INFO"})
        filename_suffix: String to use as part of the log filename (default: "test")

    Returns:
        A function that, when called, returns a Path object to the log file

    Usage Examples:
        # Basic usage - logs jobmon.server.web at INFO level
        def test_basic_logging(json_log_file):
            log_file_path = json_log_file()
            # ... run code that logs ...

        # Custom logger and level
        def test_db_logging(json_log_file):
            log_file_path = json_log_file(
                loggers={"jobmon.server.web.db": "DEBUG"}
            )

        # Multiple loggers with different levels
        def test_multiple_loggers(json_log_file):
            log_file_path = json_log_file(
                loggers={
                    "jobmon.server.web": "INFO",
                    "jobmon.server.web.db": "DEBUG",
                    "sqlalchemy": "WARNING"
                },
                filename_suffix="multi_logger"
            )

        # Reading the log file
        def test_log_content(json_log_file):
            log_file_path = json_log_file(loggers={"my.logger": "INFO"})
            # ... trigger logging ...

            import json
            with open(log_file_path, "r") as f:
                for line in f:
                    if line.strip():
                        log_entry = json.loads(line.strip())
                        assert "expected_message" in log_entry.get("event", "")
    """
    import logging.config

    from jobmon.core.config.structlog_config import configure_structlog

    def _setup_logging(loggers=None, filename_suffix="test"):
        if loggers is None:
            loggers = {"jobmon.server.web": "INFO"}

        log_file_path = tmp_path / f"{filename_suffix}.log"

        # Build loggers dict with proper structure
        logger_configs = {}
        for logger_name, level in loggers.items():
            logger_configs[logger_name] = {
                "handlers": ["test_file_handler"],
                "level": level,
                "propagate": False,
            }

        dict_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {},
            "handlers": {
                "test_file_handler": {
                    "class": "logging.FileHandler",
                    "filename": str(log_file_path),
                    "level": "INFO",
                }
            },
            "loggers": logger_configs,
        }

        # Apply the test logging configuration
        logging.config.dictConfig(dict_config)

        # Manually set the JSON formatter on the handler (avoids dictConfig import issues)
        for handler in logging.getLogger(list(loggers.keys())[0]).handlers:
            if isinstance(handler, logging.FileHandler):
                handler.setFormatter(StructlogJSONTestFormatter())
        configure_structlog(component_name="server")
        return log_file_path, None

    setups = []

    def setup_logging_wrapper(**kwargs):
        log_file_path, _ = _setup_logging(**kwargs)
        setups.append(log_file_path)
        return log_file_path

    yield setup_logging_wrapper

    # Cleanup: restore default logging configuration
    from jobmon.core.config.logconfig_utils import configure_component_logging

    configure_component_logging("server")
    configure_structlog(component_name="server")


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
def requester_in_memory(monkeypatch, web_server_in_memory, api_prefix):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests

    from jobmon.core import requester

    monkeypatch.setenv("JOBMON__HTTP__ROUTE_PREFIX", api_prefix)
    monkeypatch.setenv("JOBMON__HTTP__SERVICE_URL", "1")

    client, engine = web_server_in_memory

    def get_in_mem(url, params=None, data=None, headers=None, **kwargs):
        # Reformat the URL
        url = "/" + url.split(":")[-1].split("/", 1)[1]

        # FastAPI uses `params` for query strings
        return client.get(url, params=params, headers=headers)

    def post_in_mem(url, params=None, json=None, headers=None, **kwargs):
        # Reformat the URL
        url = "/" + url.split(":")[-1].split("/", 1)[1]

        # FastAPI uses `params` for query strings and `json` for JSON body
        return client.post(url, params=params, json=json, headers=headers)

    def put_in_mem(url, params=None, json=None, headers=None, **kwargs):
        # Reformat the URL
        url = "/" + url.split(":")[-1].split("/", 1)[1]

        # FastAPI uses `params` for query strings and `json` for JSON body
        return client.put(url, params=params, json=json, headers=headers)

    monkeypatch.setattr(requests, "get", get_in_mem)
    monkeypatch.setattr(requests, "post", post_in_mem)
    monkeypatch.setattr(requests, "put", put_in_mem)
    monkeypatch.setattr(requester, "get_content", get_test_content)
