"""Server request handling logging tests.

Tests that require server-specific fixtures (requester_in_memory, web_server_in_memory)
for testing logging during actual request handling.

Note: General server logging configuration tests are in tests/pytest/logging/.
"""

import json

import pytest

from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester
from jobmon.server.web import routes


@pytest.fixture(scope="function")
def log_file(web_server_in_memory, json_log_file):
    """Legacy fixture that uses the consolidated json_log_file fixture"""
    app, _ = web_server_in_memory
    app.get("/")  # trigger logging setup

    # Use the consolidated logging fixture
    filepath = json_log_file(
        loggers={"jobmon.server.web": "INFO"}, filename_suffix="server_logging"
    )

    yield str(filepath)


def test_add_structlog_context(requester_in_memory, log_file, api_prefix):
    """Test that structlog context is added to server logs during request handling."""
    requester = Requester("")
    added_context = {"foo": "bar", "baz": "qux"}
    requester.add_server_structlog_context(**added_context)
    requester._send_request(f"{api_prefix}/health", {}, "get")

    # Look for the health endpoint log entry that should contain the context
    found_health_log = False
    with open(log_file, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            if stripped_line:
                log_dict = json.loads(stripped_line)
                # Only check logs from the health endpoint (contains our message and path)
                if (
                    log_dict.get("event") == "Health check completed successfully"
                    and log_dict.get("path") == f"{api_prefix}/health"
                ):
                    found_health_log = True
                    for key in added_context.keys():
                        assert (
                            key in log_dict.keys()
                        ), f"Key '{key}' not found in health log: {log_dict}"
                    for val in added_context.values():
                        assert (
                            val in log_dict.values()
                        ), f"Value '{val}' not found in health log: {log_dict}"

    assert found_health_log, "Health endpoint log with context not found"


@pytest.mark.skip(reason="This test is not working")
def test_error_handling(requester_in_memory, log_file, monkeypatch, api_prefix):
    """Test that server logs exceptions correctly during request handling."""
    msg = "bad luck buddy"

    def raise_error():
        raise RuntimeError(msg)

    monkeypatch.setattr(routes, "_get_time", raise_error)

    captured_exception = False
    requester = Requester("")

    with pytest.raises(InvalidResponse):
        requester.send_request(f"{api_prefix}/health", {}, "get", tenacious=False)

    with open(log_file, "r", encoding="utf8") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            if "exception" in log_dict.keys():
                assert msg in log_dict["exception"]
                assert "Traceback" in log_dict["exception"]
                captured_exception = True

    assert captured_exception
