import time
from unittest import mock

import pytest
from tenacity import stop_after_attempt
from requests import ConnectionError

from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester


def test_server_502(client_env):
    """
    GBDSCI-1553

    We should be able to automatically retry if server returns 5XX
    status code. If we exceed retry budget, we should raise informative error
    """

    err_response = (
        502,
        b"<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body "
        b'bgcolor="white">\r\n<center><h1>502 Bad Gateway</h1></center>\r\n'
        b"<hr><center>nginx/1.13.12</center>\r\n</body>\r\n</html>\r\n",
    )
    good_response = (200, {"time": "2019-02-21 17:40:07"})

    test_requester = Requester(client_env)

    # mock requester.get_content to return 2 502s then 200
    with mock.patch("jobmon.core.requester.get_content") as m:
        # Docs: If side_effect is an iterable then each call to the mock
        # will return the next value from the iterable
        m.side_effect = [err_response] * 2 + [good_response] + [err_response] * 2

        test_requester.send_request("/time", {}, "get")  # fails at first

        # should have retried twice + one success
        retrier = test_requester._retry
        assert retrier.statistics["attempt_number"] == 3

        # if we end up stopping we should get an error
        with pytest.raises(RuntimeError, match="Status code was 502"):
            retrier.stop = stop_after_attempt(1)
            retrier.__call__(test_requester._send_request, "/time", {}, "get")


def test_connection_retry(client_env):
    """
    GBDSCI-3411

    We should automatically retry connection errors, not fail on first failure.
    """

    class RequesterMock(Requester):
        """
        Mock requester class to raise ConnectionErrors on first 2 attempts, then succeed
        """

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.time = time.time()

        def _send_request(self, *args, **kwargs):
            current_time = time.time()
            if current_time - self.time < 5:
                # If <5 seconds has elapsed, raise an error. Retry will catch it
                raise ConnectionError
            else:
                self.time = current_time
                return super()._send_request(*args, **kwargs)

    with pytest.raises(ConnectionError):
        # Set low backoff and max time limits, to force max retries error
        failed_requester = RequesterMock(client_env, max_retries=1, stop_after_delay=2)
        failed_requester.send_request("/time", {}, "get")

    # Use defaults of 10 second backoff, 2 min max wait
    good_requester = RequesterMock(client_env)
    rc, resp = good_requester.send_request(
        "/time", {}, "get"
    )  # No connectionerror raised
    assert rc == 200
    retrier = good_requester._retry
    assert retrier.statistics["attempt_number"] > 1


def test_fail_fast(client_env):
    """
    Use the client-env requestor that has max-retries == 0.
    """

    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as exc:
        requester.send_request("/no-route-should-fail", {}, "get")
        assert "Unexpected status code 404" in str(exc.value)
    tries = requester._retry.statistics["attempt_number"]
    assert tries == 1
