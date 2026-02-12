import time

import aiohttp
import pytest
import requests

from jobmon.core.exceptions import InvalidRequest, InvalidResponse
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


# Async tests for the new async functionality


@pytest.mark.asyncio
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
                "from POST request through route /time. Response content: b'Some error message..."
            ),
        ),
    ],
)
async def test_async_retries(
    client_env,
    mocker,
    initial_responses,
    final_response,
    expected_attempts,
    expected_exception,
    expected_msg,
):
    """Test async retry functionality with tenacity."""
    responses = initial_responses + [final_response]
    mock_content = mocker.patch(
        "jobmon.core.requester.Requester._get_content_async", side_effect=responses
    )
    requester = Requester(client_env, retries_attempts=3)

    async with aiohttp.ClientSession() as session:
        if expected_exception:
            with pytest.raises(expected_exception, match=expected_msg):
                await requester.send_request_async(session, "/time", {}, "post")
        else:
            await requester.send_request_async(session, "/time", {}, "post")

    assert mock_content.call_count == expected_attempts


@pytest.mark.asyncio
async def test_async_connection_retry(client_env, mocker):
    """Test if async connection retry occurs after an aiohttp.ClientError and succeeds after a given time."""

    class AsyncRequesterMock(Requester):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.time = time.time()

        async def _send_request_async(self, session, app_route, message, request_type):
            current_time = time.time()
            if current_time - self.time < 3:
                raise aiohttp.ClientError("Connection failed")
            else:
                self.time = current_time
                return await super()._send_request_async(
                    session, app_route, message, request_type
                )

    good_requester = AsyncRequesterMock(client_env)
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        rc, resp = await good_requester.send_request_async(
            session, "/time", {}, "get", tenacious=True
        )

    elapsed_time = time.time() - start_time

    assert rc == 200
    assert elapsed_time >= 3  # Ensure the time taken indicates retries were attempted.


@pytest.mark.asyncio
async def test_async_fail_fast(client_env, mocker):
    """Test async fail fast behavior for 4xx errors."""
    mock_content = mocker.patch(
        "jobmon.core.requester.Requester._get_content_async",
        side_effect=[(404, "Not Found")],
    )
    requester = Requester.from_defaults()

    async with aiohttp.ClientSession() as session:
        with pytest.raises(InvalidRequest) as exc:
            await requester.send_request_async(
                session, "/no-route-should-fail", {}, "get"
            )

    assert (
        "Client error with status code 404 from GET request through route "
        "/no-route-should-fail. Response content: Not Found" in str(exc.value)
    )
    assert mock_content.call_count == 1


@pytest.mark.asyncio
async def test_async_non_tenacious_request(client_env, mocker):
    """Test async non-tenacious requests fail immediately without retries."""
    mock_content = mocker.patch(
        "jobmon.core.requester.Requester._get_content_async",
        side_effect=[(500, "Server Error")],
    )
    requester = Requester(client_env)

    async with aiohttp.ClientSession() as session:
        with pytest.raises(InvalidResponse) as exc:
            await requester.send_request_async(
                session, "/test_bad", {}, "get", tenacious=False
            )

    assert (
        "Request failed due to status code 500 from GET request through "
        "route /test_bad. Response content: Server Error" in str(exc.value)
    )
    assert mock_content.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tenacious, exception_type",
    [(False, aiohttp.ServerTimeoutError), (True, RuntimeError)],
)
async def test_async_timeout(client_env, mocker, tenacious, exception_type):
    """Test async timeout handling with and without retries."""
    # Mock the aiohttp session to raise timeout
    mock_post = mocker.patch.object(aiohttp.ClientSession, "post")
    mock_post.side_effect = aiohttp.ServerTimeoutError("Request timeout")

    requester = Requester(client_env, request_timeout=1, retries_timeout=5)

    async with aiohttp.ClientSession() as session:
        if tenacious:
            with pytest.raises(
                exception_type, match="Exceeded HTTP request retry budget"
            ):
                await requester.send_request_async(
                    session, "/some_route", {}, "post", tenacious=True
                )
            assert mock_post.call_count > 1
        else:
            with pytest.raises(exception_type):
                await requester.send_request_async(
                    session, "/some_route", {}, "post", tenacious=False
                )
            assert mock_post.call_count == 1


@pytest.mark.asyncio
async def test_async_get_content_json(client_env, mocker):
    """Test async content parsing for JSON responses."""
    requester = Requester(client_env)

    # Mock aiohttp response
    mock_response = mocker.MagicMock()
    mock_response.status = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json = mocker.AsyncMock(return_value={"test": "data"})

    status, content = await requester._get_content_async(mock_response)

    assert status == 200
    assert content == {"test": "data"}
    mock_response.json.assert_called_once()


@pytest.mark.asyncio
async def test_async_get_content_malformed_json(client_env, mocker):
    """Test async content parsing for malformed JSON responses."""
    requester = Requester(client_env)

    # Mock aiohttp response with malformed JSON
    mock_response = mocker.MagicMock()
    mock_response.status = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json = mocker.AsyncMock(
        side_effect=aiohttp.ContentTypeError(None, None)
    )
    mock_response.text = mocker.AsyncMock(return_value="malformed json")

    status, content = await requester._get_content_async(mock_response)

    assert status == 200
    assert content == "malformed json"
    mock_response.text.assert_called_once()


@pytest.mark.asyncio
async def test_async_get_content_non_json(client_env, mocker):
    """Test async content parsing for non-JSON responses."""
    requester = Requester(client_env)

    # Mock aiohttp response with non-JSON content
    mock_response = mocker.MagicMock()
    mock_response.status = 200
    mock_response.headers = {"Content-Type": "text/html"}
    mock_response.read = mocker.AsyncMock(return_value=b"<html>content</html>")

    status, content = await requester._get_content_async(mock_response)

    assert status == 200
    assert content == b"<html>content</html>"
    mock_response.read.assert_called_once()


@pytest.mark.asyncio
async def test_async_request_types(client_env, mocker):
    """Test async requests support different HTTP methods."""
    requester = Requester(client_env)

    # Mock successful response
    mock_response = mocker.MagicMock()
    mock_response.status = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json = mocker.AsyncMock(return_value={"success": True})

    # Mock the session context manager
    mock_session = mocker.MagicMock()
    mock_context = mocker.MagicMock()
    mock_context.__aenter__ = mocker.AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = mocker.AsyncMock(return_value=None)

    # Test POST
    mock_session.post.return_value = mock_context
    await requester._send_request_async(mock_session, "/test", {}, "post")
    mock_session.post.assert_called_once()

    # Test GET
    mock_session.get.return_value = mock_context
    await requester._send_request_async(mock_session, "/test", {}, "get")
    mock_session.get.assert_called_once()

    # Test PUT
    mock_session.put.return_value = mock_context
    await requester._send_request_async(mock_session, "/test", {}, "put")
    mock_session.put.assert_called_once()

    # Test invalid method
    with pytest.raises(ValueError, match="request_type must be one of"):
        await requester._send_request_async(mock_session, "/test", {}, "delete")
