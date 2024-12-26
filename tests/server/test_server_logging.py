import json

import pytest

from jobmon.core.requester import Requester
from jobmon.core.exceptions import InvalidResponse
from jobmon.server.web import routes
from jobmon.server.web import log_config


@pytest.fixture(scope="function")
def log_file(web_server_in_memory, tmp_path):
    app, _ = web_server_in_memory
    app.get("/")  # trigger logging setup
    filepath = str(tmp_path) + ".log"

    dict_config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": log_config.default_formatters.copy(),
        "handlers": {
            "file_handler": {
                "class": "logging.FileHandler",
                "formatter": "json",
                "filename": filepath,
            }
        },
        "loggers": {
            "jobmon.server.web": {
                "handlers": ["file_handler"],
                "level": "INFO",
            },
        },
    }

    # override base config
    log_config.configure_logging(dict_config=dict_config)
    yield filepath
    log_config.configure_logging()


def test_add_structlog_context(requester_in_memory, log_file, api_prefix):
    requester = Requester("")
    added_context = {"foo": "bar", "baz": "qux"}
    requester.add_server_structlog_context(**added_context)
    requester._send_request(f"{api_prefix}/health", {}, "get")
    with open(log_file, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            for key in added_context.keys():
                assert key in log_dict.keys()
            for val in added_context.values():
                assert val in log_dict.values()


@pytest.mark.skip(reason="This test is not working")
def test_error_handling(requester_in_memory, log_file, monkeypatch, api_prefix):
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
