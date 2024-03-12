import time
import pytest
from unittest import mock
import requests
from tenacity import stop_after_attempt
from jobmon.core.exceptions import InvalidResponse, InvalidRequest
from jobmon.core.requester import Requester


@pytest.mark.parametrize(
    "initial_responses, final_response, expected_attempts, expected_exception, expected_msg",
    [
        (
                [(502, b"Some error message...")] * 2,
                (200, {"time": "2019-02-21 17:40:07"}),
                3,
                None,
                None,
        ),
        (
                [(502, b"Some error message...")] * 3,
                None,  # This won't be used since we've hit the retry limit
                3,
                RuntimeError,
                (
                        "Exceeded HTTP request retry budget due to: Request failed due to status code 502 "
                        "from GET request through route /time. Response content: b'Some error message..."
                ),
        ),
    ],
)
def test_retries(
        client_env,
        mocker,
        initial_responses,
        final_response,
        expected_attempts,
        expected_exception,
        expected_msg,
):
    responses = initial_responses + [final_response]
    mock_content = mocker.patch(
        "jobmon.core.requester.get_content", side_effect=responses
    )
    requester = Requester(client_env, retries_attempts=3)
    if expected_exception:
        with pytest.raises(expected_exception, match=expected_msg):
            requester.send_request("/time", {}, "get")
    else:
        requester.send_request("/time", {}, "get")

    # Instead of using the _retry attribute, check how many times the mocked method was called
    assert mock_content.call_count == expected_attempts


def test_connection_retry(client_env, mocker):
    """Test if connection retry occurs after a ConnectionError and succeeds after a given time."""

    class RequesterMock(Requester):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.time = time.time()

        def _send_request(self, app_route, message, request_type):
            current_time = time.time()
            if current_time - self.time < 5:
                raise requests.ConnectionError
            else:
                self.time = current_time
                return super()._send_request(app_route, message, request_type)

    good_requester = RequesterMock(client_env)
    start_time = time.time()
    rc, resp = good_requester.send_request("/time", {}, "get", tenacious=True)
    elapsed_time = time.time() - start_time

    assert rc == 200
    assert elapsed_time >= 5  # Ensure the time taken indicates retries were attempted.


def test_fail_fast(client_env, mocker):
    mock_content = mocker.patch(
        "jobmon.core.requester.get_content", side_effect=[(404, "Not Found")]
    )
    requester = Requester.from_defaults()

    with pytest.raises(InvalidRequest) as exc:
        requester.send_request("/no-route-should-fail", {}, "get")

    assert (
            "Client error with status code 404 from GET request through route "
            "/no-route-should-fail. Response content: Not Found" in str(exc.value)
    )
    assert mock_content.call_count == 1


def test_non_tenacious_request(client_env, mocker):
    mock_content = mocker.patch(
        "jobmon.core.requester.get_content", side_effect=[(500, "Server Error")]
    )
    requester = Requester(client_env)

    with pytest.raises(InvalidResponse) as exc:
        requester.send_request("/test_bad", {}, "get", tenacious=False)

    assert (
            "Request failed due to status code 500 from GET request through "
            "route /test_bad. Response content: Server Error" in str(exc.value)
    )
    assert mock_content.call_count == 1


@pytest.mark.parametrize(
    "tenacious, exception_type", [(False, TimeoutError), (True, RuntimeError)]
)
def test_connection_timeout(client_env, mocker, tenacious, exception_type):
    mocker.patch("requests.get", side_effect=TimeoutError)

    # Adjust the retries_timeout for the requester
    requester = Requester(client_env, request_timeout=1, retries_timeout=10)

    mock_send_request = mocker.patch.object(
        Requester, "_send_request", side_effect=requester._send_request
    )

    if tenacious:
        with pytest.raises(exception_type, match="Exceeded HTTP request retry budget"):
            requester.send_request("/some_route", {}, "get", tenacious=True)
        assert mock_send_request.call_count > 1
    else:
        with pytest.raises(exception_type):
            requester.send_request("/some_route", {}, "get", tenacious=False)
        assert mock_send_request.call_count == 1
