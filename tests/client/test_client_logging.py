import logging
from datetime import date

from jobmon.client.logging import JobmonLoggerConfig


def test_client_logging_default_format(client_env, capsys):
    JobmonLoggerConfig.attach_default_handler(logger_name="test.test")
    logger = logging.getLogger("test.test")
    logger.info("This is a test")
    captured = capsys.readouterr()
    logs = captured.out.split("\n")
    # should only contain two lines, one empty, one above log
    for log in logs:
        if log:
            # check format and message
            assert date.today().strftime("%Y-%m-%d") in log
            assert "test.test" in log
            assert "INFO" in log
            assert "This is a test" in log
