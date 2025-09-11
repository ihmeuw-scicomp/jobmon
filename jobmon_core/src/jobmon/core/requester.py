"""Requester object to make HTTP requests to the Jobmon FastAPI services."""

from __future__ import annotations

import contextlib
import functools
import json
import logging
import logging.config
from typing import Any, Callable, Dict, Tuple, Type

import aiohttp
import requests
import tenacity
import urllib3

from jobmon.core import __version__
from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import InvalidRequest, InvalidResponse

logger = logging.getLogger(__name__)


def http_request_ok(status_code: int) -> bool:
    """Return True if HTTP return codes that are deemed ok."""
    return status_code in (200, 302, 307)


class Requester:
    """Handles HTTP requests to the jobmon server with configurable OTLP integration."""

    # Class-level attribute to store the OtlpManager instance
    _otlp_manager = None

    def __init__(
        self,
        service_url: str,
        retries_timeout: int = 300,
        retries_attempts: int = 10,
        request_timeout: int = 20,
        use_otlp: bool = False,
    ) -> None:
        """Initialize requester with optional OTLP support.

        Args:
            service_url: The jobmon server URL
            retries_timeout: Total timeout for retries in seconds
            retries_attempts: Number of retry attempts
            request_timeout: Individual request timeout in seconds
            use_otlp: Whether to enable OTLP instrumentation
        """
        self.service_url = service_url
        self.retries_timeout = retries_timeout
        self.retries_attempts = retries_attempts
        self.request_timeout = request_timeout

        if use_otlp and Requester._otlp_manager is None:
            self._init_otlp()

        self.server_structlog_context: Dict[str, str] = {}

    @classmethod
    def _init_otlp(cls: Type[Requester]) -> None:
        """Initialize OTLP tracing only - logging handled by client configuration."""
        try:
            from jobmon.core.otlp import (
                OTLP_AVAILABLE,
                JobmonOTLPManager,
                initialize_jobmon_otlp,
            )

            if not OTLP_AVAILABLE:
                return

            # Initialize minimal OTLP manager for traces only
            cls._otlp_manager = initialize_jobmon_otlp()

            # Instrument requests library for HTTP tracing
            JobmonOTLPManager.instrument_requests()

            # Note: Logging configuration is handled by the client's configure_client_logging()
            # No requester-specific logconfig needed

        except ImportError:
            # OTLP dependencies not available, continue without OTLP
            pass

    @classmethod
    def from_defaults(cls: Type[Requester]) -> Requester:
        """Instantiate a requester from default config values."""
        config = JobmonConfig()

        service_url = config.get("http", "service_url")
        route_prefix = config.get("http", "route_prefix")
        if route_prefix:
            service_url = f"{service_url.rstrip('/')}/{route_prefix.strip('/')}"

        retries_timeout = config.get_int("http", "retries_timeout")
        retries_attempts = config.get_int("http", "retries_attempts")
        request_timeout = config.get_int("http", "request_timeout")

        try:
            telemetry_section = config.get_section_coerced("telemetry")
            tracing_config = telemetry_section.get("tracing", {})
            use_otlp = tracing_config.get("requester_enabled", False)
        except Exception:
            use_otlp = False

        return cls(
            service_url=service_url,
            retries_timeout=retries_timeout,
            retries_attempts=retries_attempts,
            request_timeout=request_timeout,
            use_otlp=use_otlp,
        )

    @property
    def url(self) -> str:
        """Legacy property for backward compatibility."""
        return self.service_url

    def add_server_structlog_context(self, **kwargs: Any) -> None:
        """Add the structlogging context if it has been provided."""
        for key, value in kwargs.items():
            self.server_structlog_context[key] = value

    @contextlib.contextmanager
    def tracing_span(self, app_route: str, request_type: str) -> Any:
        if self._otlp_manager and hasattr(self._otlp_manager, "get_tracer"):
            tracer = self._otlp_manager.get_tracer("requester")
            if tracer:
                with tracer.start_as_current_span("send_request") as span:
                    span.set_attribute("http.method", request_type.upper())
                    span.set_attribute("http.url", self.service_url + app_route)
                    yield
                    return

        # If no OTLP or tracer not available, just yield without tracing
        yield

    def _maybe_trace(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Callable:
            with self.tracing_span(
                kwargs.get("app_route", "UNKNOWN"),
                kwargs.get("request_type", "UNKNOWN"),
            ):
                return func(*args, **kwargs)

        return wrapper

    def _should_retry_exception(self, exception: Any) -> bool:
        """Determine if an exception should trigger a retry."""
        logger.warning(f"Exception occurred: {exception}")

        # Do not retry for certain client errors.
        if isinstance(exception, InvalidRequest):
            return False

        # Retry for specific exceptions (sync and async compatible).
        return isinstance(
            exception,
            (
                InvalidResponse,
                TimeoutError,
                requests.ConnectionError,
                requests.adapters.MaxRetryError,
                requests.exceptions.ReadTimeout,
                urllib3.exceptions.NewConnectionError,
                urllib3.exceptions.MaxRetryError,
                aiohttp.ClientError,
                aiohttp.ServerTimeoutError,
                aiohttp.ClientConnectorError,
            ),
        )

    def _maybe_retry(self, func: Callable, tenacious: bool) -> Any:
        if not tenacious:
            return func

        def raise_if_exceed_retry(retry_state: tenacity.RetryCallState) -> Any:
            """If we trigger retry error, raise informative RuntimeError."""
            # Check if the retry outcome is an exception
            outcome = retry_state.outcome
            if outcome and outcome.exception():
                exception = outcome.exception()
                raise RuntimeError(
                    f"Exceeded HTTP request retry budget due to: {exception}"
                ) from exception

        retrying = tenacity.retry(
            stop=(
                tenacity.stop_after_attempt(self.retries_attempts)
                | tenacity.stop_after_delay(self.retries_timeout)
            ),
            wait=tenacity.wait_exponential_jitter(initial=1, exp_base=2, jitter=1),
            retry=tenacity.retry_if_exception(self._should_retry_exception),
            retry_error_callback=raise_if_exceed_retry,
        )(func)

        return retrying

    def _send_request(
        self,
        app_route: str,
        message: dict,
        request_type: str,
    ) -> Tuple[int, Any]:
        # Construct URL
        route = self.service_url + app_route
        logger.info(f"Route: {route}, message: {message}")

        # Add version to query parameters
        params = {"client_jobmon_version": __version__}
        if request_type == "get":
            params.update(message)

        # Set headers, including the custom header for structlog context
        headers = {
            "Content-Type": "application/json",
            "X-Server-Structlog-Context": json.dumps(self.server_structlog_context),
        }

        # Send the appropriate request
        if request_type == "post":
            response = requests.post(
                route,
                params=params,
                json=message,
                headers=headers,
                timeout=self.request_timeout,
            )
        elif request_type == "get":
            response = requests.get(
                route,
                params=params,
                headers=headers,
                timeout=self.request_timeout,
            )
        elif request_type == "put":
            response = requests.put(
                route,
                params=params,
                json=message,
                headers=headers,
                timeout=self.request_timeout,
            )
        else:
            raise ValueError(
                f"request_type must be one of 'get', 'post', or 'put'. Got {request_type}"
            )

        # Extract status code and content
        status_code, content = get_content(response)

        # Raise the InvalidResponse exception based on the logic from should_retry_result
        if 499 < status_code < 600 or status_code == 423:
            raise InvalidResponse(
                f"Request failed due to status code {status_code} from {request_type.upper()} "
                f"request through route {app_route}. Response content: {content}"
            )

        # Keep the logic for other status codes that might be encountered but
        # aren't in the retry condition.
        if 400 <= status_code < 500:
            raise InvalidRequest(
                f"Client error with status code {status_code} from {request_type.upper()} "
                f"request through route {app_route}. Response content: {content}"
            )

        return status_code, content

    def send_request(
        self, app_route: str, message: dict, request_type: str, tenacious: bool = True
    ) -> Tuple[int, Any]:
        """Send a request to the Jobmon server."""

        def send_fn(
            app_route: str, message: dict, request_type: str
        ) -> Tuple[int, Any]:
            return self._send_request(app_route, message, request_type)

        send_method = self._maybe_retry(send_fn, tenacious)
        send_with_trace = self._maybe_trace(send_method)
        res = send_with_trace(
            app_route=app_route, message=message, request_type=request_type
        )

        return res

    async def _send_request_async(
        self,
        session: aiohttp.ClientSession,
        app_route: str,
        message: dict,
        request_type: str,
    ) -> Tuple[int, Any]:
        """Async version of _send_request using aiohttp."""
        # Construct URL
        route = self.service_url + app_route
        logger.info(f"Route: {route}, message: {message}")

        # Add version to query parameters
        params = {"client_jobmon_version": __version__}
        if request_type == "get":
            params.update(message)

        # Set headers, including the custom header for structlog context
        headers = {
            "Content-Type": "application/json",
            "X-Server-Structlog-Context": json.dumps(self.server_structlog_context),
        }

        # Send the appropriate request
        method_map = {
            "post": session.post,
            "get": session.get,
            "put": session.put,
        }

        if request_type not in method_map:
            raise ValueError(
                f"request_type must be one of 'get', 'post', or 'put'. Got {request_type}"
            )

        method = method_map[request_type]

        # Send the request with appropriate parameters
        if request_type in ("post", "put"):
            async with method(
                route,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.request_timeout),
                json=message,
            ) as response:
                status_code, content = await self._get_content_async(response)
        else:  # GET
            async with method(
                route,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.request_timeout),
            ) as response:
                status_code, content = await self._get_content_async(response)

        # Raise the InvalidResponse exception based on the logic from should_retry_result
        if 499 < status_code < 600 or status_code == 423:
            raise InvalidResponse(
                f"Request failed due to status code {status_code} from {request_type.upper()} "
                f"request through route {app_route}. Response content: {content}"
            )

        # Keep the logic for other status codes that might be encountered but
        # aren't in the retry condition.
        if 400 <= status_code < 500:
            raise InvalidRequest(
                f"Client error with status code {status_code} from {request_type.upper()} "
                f"request through route {app_route}. Response content: {content}"
            )

        return status_code, content

    async def _get_content_async(
        self, response: aiohttp.ClientResponse
    ) -> Tuple[int, Any]:
        """Parse an aiohttp response, handling JSON and non-JSON content gracefully.

        Args:
            response: The aiohttp ClientResponse object to parse.

        Returns:
            Tuple of (status_code, content) where content is parsed JSON or raw text/bytes.
        """
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                content = await response.json()
            except (json.decoder.JSONDecodeError, ValueError, aiohttp.ContentTypeError):
                # For cases where the response body is empty or malformed JSON
                content = await response.text()
        else:
            content = await response.read()
        return response.status, content

    def _maybe_retry_async(self, func: Callable, tenacious: bool) -> Any:
        """Async version of _maybe_retry using tenacity async retry."""
        if not tenacious:
            return func

        def raise_if_exceed_retry(retry_state: tenacity.RetryCallState) -> Any:
            """If we trigger retry error, raise informative RuntimeError."""
            # Check if the retry outcome is an exception
            outcome = retry_state.outcome
            if outcome and outcome.exception():
                exception = outcome.exception()
                raise RuntimeError(
                    f"Exceeded HTTP request retry budget due to: {exception}"
                ) from exception

        retrying = tenacity.retry(
            stop=(
                tenacity.stop_after_attempt(self.retries_attempts)
                | tenacity.stop_after_delay(self.retries_timeout)
            ),
            wait=tenacity.wait_exponential_jitter(initial=1, exp_base=2, jitter=1),
            retry=tenacity.retry_if_exception(self._should_retry_exception),
            retry_error_callback=raise_if_exceed_retry,
        )(func)

        return retrying

    async def send_request_async(
        self,
        session: aiohttp.ClientSession,
        app_route: str,
        message: dict,
        request_type: str,
        tenacious: bool = True,
    ) -> Tuple[int, Any]:
        """Send an async request to the Jobmon server with sophisticated retry logic.

        This method provides the same robust retry capabilities as the sync version,
        using tenacity for exponential backoff with jitter, timeout protection,
        and comprehensive exception handling.

        Args:
            session: An active aiohttp ClientSession for making requests.
            app_route: The API route to request (will be appended to base URL).
            message: Dictionary containing the request payload.
            request_type: HTTP method - 'get', 'post', or 'put'.
            tenacious: Whether to enable retry logic (default: True).

        Returns:
            Tuple of (status_code, response_content).

        Raises:
            InvalidRequest: For 4xx client errors (no retry).
            InvalidResponse: For 5xx server errors after exhausting retries.
            RuntimeError: If retry budget is exceeded.
        """

        async def send_fn(
            session: aiohttp.ClientSession,
            app_route: str,
            message: dict,
            request_type: str,
        ) -> Tuple[int, Any]:
            return await self._send_request_async(
                session, app_route, message, request_type
            )

        send_method = self._maybe_retry_async(send_fn, tenacious)

        with self.tracing_span(app_route, request_type):
            res = await send_method(
                session=session,
                app_route=app_route,
                message=message,
                request_type=request_type,
            )

        return res


def get_content(response: Any) -> Tuple[int, Any]:
    """Parse the response."""
    if "application/json" in response.headers.get("Content-Type", ""):
        try:
            content = response.json()
        except TypeError:
            # for test_client, response.json is a dict not fn
            content = response.json
        except (json.decoder.JSONDecodeError, ValueError):
            # For cases where the response body is empty or malformed JSON
            content = response.text
    else:
        content = response.content
    return response.status_code, content
