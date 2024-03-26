"""Requester object to make HTTP requests to the Jobmon Flask services."""

from __future__ import annotations

import contextlib
import functools
import json
import logging
from typing import Any, Callable, Dict, Tuple, Type

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


class Requester(object):
    """Requester object to make HTTP requests to the Jobmon Flask services."""

    # Class-level attribute to store the OtlpAPI instance
    _otlp_api = None

    def __init__(
        self,
        url: str,
        route_prefix: str = "",
        request_timeout: int = 20,
        retries_timeout: int = 300,
        retries_attempts: int = 10,
        use_otlp: bool = False,
    ) -> None:
        """Initialize the Requester object with the url to make requests to."""
        self.base_url = url
        self.route_prefix = route_prefix
        self.request_timeout = request_timeout
        self.retries_timeout = retries_timeout
        self.retries_attempts = retries_attempts
        if use_otlp and Requester._otlp_api is None:
            self._init_otlp()
        self.server_structlog_context: Dict[str, str] = {}

    @classmethod
    def _init_otlp(cls: Type[Requester]) -> None:
        from jobmon.core.otlp import OtlpAPI

        # setup connections to backend
        otlp_instance = OtlpAPI()
        otlp_instance.instrument_requests()
        otlp_instance.correlate_logger("jobmon.core.requester")

        # setup tracer for Requester to use
        cls._otlp_api = otlp_instance

    @classmethod
    def from_defaults(cls: Type[Requester]) -> Requester:
        """Instantiate a requester from default config values."""
        config = JobmonConfig()
        service_url = config.get("http", "service_url")
        route_prefix = config.get("http", "route_prefix")
        request_timeout = config.get_int("http", "request_timeout")
        retries_timeout = config.get_int("http", "retries_timeout")
        retries_attempts = config.get_int("http", "retries_attempts")
        use_otlp = config.get_boolean("otlp", "http_enabled")
        return cls(
            service_url,
            route_prefix,
            request_timeout,
            retries_timeout,
            retries_attempts,
            use_otlp,
        )

    @property
    def url(self) -> str:
        """Return the base url for the requester."""
        return self.base_url + self.route_prefix

    def add_server_structlog_context(self, **kwargs: Any) -> None:
        """Add the structlogging context if it has been provided."""
        for key, value in kwargs.items():
            self.server_structlog_context[key] = value

    @contextlib.contextmanager
    def tracing_span(self, app_route: str, request_type: str) -> Any:
        if self._otlp_api:
            tracer = self._otlp_api.get_tracer("requester")
            with tracer.start_as_current_span("send_request") as span:
                span.set_attribute("http.method", request_type.upper())
                span.set_attribute("http.url", self.url + app_route)
                yield span
        else:
            yield None

    def _maybe_trace(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Callable:
            with self.tracing_span(
                kwargs.get("app_route", "UNKNOWN"),
                kwargs.get("request_type", "UNKNOWN"),
            ):
                return func(*args, **kwargs)

        return wrapper

    def _maybe_retry(self, func: Callable, tenacious: bool) -> Any:
        if not tenacious:
            return func

        def should_retry_exception(exception: Any) -> Any:
            """Return True if we should retry on the given exception."""
            logger.warning(f"Exception occurred: {exception}")

            # Do not retry for certain client errors.
            if isinstance(exception, InvalidRequest):
                return False

            # Retry for specific exceptions.
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
                ),
            )

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
            retry=tenacity.retry_if_exception(should_retry_exception),
            retry_error_callback=raise_if_exceed_retry,
        )(func)

        return retrying

    def _send_request(
        self,
        app_route: str,
        message: dict,
        request_type: str,
    ) -> Tuple[int, Any]:
        # construct url
        route = self.url + app_route
        logger.debug(f"Route: {route}, message: {message}")

        if request_type in ["post", "put"]:
            message["server_structlog_context"] = self.server_structlog_context
        else:
            {}

        # send request to server
        if request_type == "post":
            params = {"client_jobmon_version": __version__}
            response = requests.post(
                route,
                params=params,
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=self.request_timeout,
            )
        elif request_type == "get":
            params = message.copy()
            params["client_jobmon_version"] = __version__
            response = requests.get(
                route,
                params=params,
                data=json.dumps(self.server_structlog_context),
                headers={"Content-Type": "application/json"},
                timeout=self.request_timeout,
            )
        elif request_type == "put":
            params = {"client_jobmon_version": __version__}
            response = requests.put(
                route,
                params=params,
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=self.request_timeout,
            )
        else:
            raise ValueError(
                f"request_type must be one of 'get', 'post', or 'put'. Got {request_type}"
            )

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


def get_content(response: Any) -> Tuple[int, Any]:
    """Parse the response."""
    if "application/json" in response.headers.get("Content-Type", ""):
        try:
            content = response.json()
        except TypeError:  # for test_client, response.json is a dict not fn
            content = response.json
    else:
        content = response.content
    return response.status_code, content
