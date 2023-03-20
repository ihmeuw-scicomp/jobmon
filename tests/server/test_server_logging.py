import json

import pytest

from jobmon.core.requester import Requester
from jobmon.server.web import routes
from jobmon.server.web.api import configure_logging


@pytest.fixture(scope="function")
def log_config(web_server_in_memory, tmp_path):
    app, engine = web_server_in_memory
    app.get("/")  # trigger logging setup
    filepath = str(tmp_path) + ".log"

    # override base config
    handler_config = {
        "file_handler": {
            "class": "logging.FileHandler",
            "formatter": "json",
            "filename": filepath,
        }
    }
    logger_config = {
        "jobmon.server.web": {
            "handlers": ["file_handler"],
            "level": "INFO",
        },
    }
    configure_logging(loggers_dict=logger_config, handlers_dict=handler_config)
    yield filepath
    configure_logging()


def test_add_structlog_context(requester_in_memory, log_config):
    requester = Requester("")
    added_context = {"foo": "bar", "baz": "qux"}
    requester.add_server_structlog_context(**added_context)
    requester._send_request("/health", {}, "get")
    requester._send_request("/health", {}, "post")
    requester._send_request("/health", {}, "put")
    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            for key in added_context.keys():
                assert key in log_dict.keys()
            for val in added_context.values():
                assert val in log_dict.values()


def test_error_handling(requester_in_memory, log_config, monkeypatch):
    msg = "bad luck buddy"

    def raise_error():
        raise RuntimeError(msg)

    monkeypatch.setattr(routes, "_get_time", raise_error)

    captured_exception = False
    requester = Requester("")
    requester._send_request("/health", {}, "get")
    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            if "exception" in log_dict.keys():
                assert msg in log_dict["exception"]
                assert "Traceback" in log_dict["exception"]
                captured_exception = True

    assert captured_exception


def test_server_500(requester_in_memory):
    test_requester = Requester("")
    rc, resp = test_requester._send_request(
        app_route="/test_bad", message={}, request_type="get"
    )
    assert rc == 500
    assert "no such table" in resp["error"]["exception_message"]
