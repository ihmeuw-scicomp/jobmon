import time
import pytest
from unittest import mock
from requests import ConnectionError
import requests
from tenacity import stop_after_attempt
from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester


@pytest.mark.parametrize(
    "initial_responses, final_response, expected_attempts, expected_exception, expected_msg",
    [
        ([(502, b"Some error message...")] * 2, (200, {"time": "2019-02-21 17:40:07"}), 3, None, None),
        ([(502, b"Some error message...")] * 2, (502, b"Some error message..."), 3, RuntimeError, "Status code was 502")
    ]
)
def test_retries(client_env, mocker, initial_responses, final_response, expected_attempts, expected_exception, expected_msg):
    responses = initial_responses + [final_response]

    mock_content = mocker.patch("jobmon.core.requester.get_content", side_effect=responses)
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
                raise ConnectionError
            else:
                self.time = current_time
                return super()._send_request(app_route, message, request_type)

    good_requester = RequesterMock(client_env)
    start_time = time.time()
    rc, resp = good_requester.send_request("/time", {}, "get")
    elapsed_time = time.time() - start_time

    assert rc == 200
    assert elapsed_time >= 5  # Ensure the time taken indicates retries were attempted.


def test_fail_fast(client_env, mocker):
    mock_content = mocker.patch("jobmon.core.requester.get_content", side_effect=[(404, "Not Found")])
    requester = Requester.from_defaults()
    
    with pytest.raises(InvalidResponse) as exc:
        requester.send_request("/no-route-should-fail", {}, "get")

    assert "Unexpected status code 404" in str(exc.value)
    assert mock_content.call_count == 1


def test_tracing_enabled(client_env, mocker):
    """Validate that when the use_otlp flag is enabled, traces are captured."""
    mock_tracer = mocker.Mock()
    mock_tracer.start_as_current_span.return_value = mocker.MagicMock(
        __enter__=mocker.Mock(return_value=...), 
        __exit__=mocker.Mock(return_value=None)
    )
    mocker.patch('jobmon.core.otlp.OtlpAPI.get_tracer', return_value=mock_tracer)

    requester = Requester(client_env, use_otlp=True)
    requester.send_request("/time", {}, "get")

    mock_tracer.start_as_current_span.assert_called_once()


def test_non_tenacious_request(client_env, mocker):
    mock_content = mocker.patch("jobmon.core.requester.get_content", side_effect=[(500, "Server Error")])
    requester = Requester(client_env)

    with pytest.raises(InvalidResponse) as exc:
        requester.send_request("/some_route", {}, "get", tenacious=False)

    assert "Unexpected status code 500" in str(exc.value)
    assert mock_content.call_count == 1


@pytest.mark.parametrize("tenacious, exception_type", [(False, TimeoutError), (True, RuntimeError)])
def test_connection_timeout(client_env, mocker, tenacious, exception_type):
    mocker.patch("requests.get", side_effect=TimeoutError)

    # Adjust the retries_timeout for the requester
    requester = Requester(client_env, request_timeout=1, retries_timeout=10)

    mock_send_request = mocker.patch.object(Requester, "_send_request", side_effect=requester._send_request)

    if tenacious:
        with pytest.raises(exception_type, match="Exceeded HTTP request retry budget"):
            requester.send_request("/some_route", {}, "get", tenacious=True)
        assert mock_send_request.call_count > 1
    else:
        with pytest.raises(exception_type):
            requester.send_request("/some_route", {}, "get", tenacious=False)
        assert mock_send_request.call_count == 1
