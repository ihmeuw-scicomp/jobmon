"""Requester object to make HTTP requests to the Jobmon Flask services."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Tuple, Type

import requests
import tenacity

from jobmon.core import __version__
from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import InvalidResponse

logger = logging.getLogger(__name__)


def http_request_ok(status_code: int) -> bool:
    """Return True if HTTP return codes that are deemed ok."""
    return status_code in (200, 302, 307)


class Requester(object):
    """Sends an HTTP messages via the Requests library to one of the running services.

    Either the JQS or the JSM, and returns the response from the server. A common use case is
    where the swarm of application jobs send status messages via a Requester to the
    JobStateManager or requests job status from the JobQueryServer.
    """

    def __init__(
        self, url: str, max_retries: int = 10, stop_after_delay: int = 120
    ) -> None:
        """Initialize the Requester object with the url to make requests to."""
        self.url = url
        self.max_retries = max_retries
        self.stop_after_delay = stop_after_delay
        self.server_structlog_context: Dict[str, str] = {}

    @classmethod
    def from_defaults(cls: Type[Requester]) -> Requester:
        """Instantiate a requester from default config values."""
        config = JobmonConfig()
        service_url = config.get("http", "service_url")
        max_retries = config.get_int("http", "max_retries")
        stop_after_delay = config.get_int("http", "stop_after_delay")

        return cls(service_url, max_retries, stop_after_delay)

    def add_server_structlog_context(self, **kwargs: Any) -> None:
        """Add the structlogging context if it has been provided."""
        for key, value in kwargs.items():
            self.server_structlog_context[key] = value

    def send_request(
        self,
        app_route: str,
        message: dict,
        request_type: str,
        tenacious: bool = True,
    ) -> Tuple[int, Any]:
        """Send request to server.

        If we get a 5XX status code, we will retry for up to 2 minutes using
        exponential backoff.

        Args:
            app_route:
                The specific end point with which you want to
                interact. The app_route must always start with a slash ('/') and
                must match one of the function decorations of @jsm.route or
                @jqs.route on the server side.
            message: The message dict to be sent to the server.
                Must contain any arguments the JSM/JQS route needs to operate.
                If the request is a 'GET', the value of the message dict will
                likely be parsed into the url. If the request is a 'POST' or 'PUT',
                the message dict will get stored in a dictionary that is parsed on
                the server side and passed into the work done by that route.
                For example, a valid message for a request to add a task_dag
                to the JSM might be:

                    {'name': 'my_name',
                     'user': 'my_user',
                     'dag_hash': 'my_dag_hash'}

            request_type: The type of request desired, either 'get', 'post, or 'put'
            tenacious: use tenacity for retries


        Returns:
            Server reply message

        Raises:
            RuntimeError if 500 errors occur for > 2 minutes

        """
        if tenacious:
            res = self._tenacious_send_request(app_route, message, request_type)
        else:
            res = self._send_request(app_route, message, request_type)

        if not http_request_ok(res[0]):
            raise InvalidResponse(
                f"Unexpected status code {res[0]} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {res[1]}"
            )

        return res

    def _tenacious_send_request(
        self,
        app_route: str,
        message: dict,
        request_type: str,
    ) -> Tuple[int, Any]:
        """Use tenacity to retry requests if they get an unsatisfactory return code."""

        def is_5XX(result: Tuple[int, dict]) -> bool:
            """Return True if get_content result has 5XX status."""
            status = result[0]
            content = str(result[1])
            is_bad = 499 < status < 600
            if is_bad:
                if "2013, 'Lost connection to MySQL server during query'" in content:
                    msg = (
                        "A 'Lost connection to MySQL server' event occurred, "
                        "for which a new db connection pool has been created "
                        "(usually due to a routine db hot cutover operation)"
                    )
                    logger.warning(
                        f"Got HTTP status_code={status} from server. app_route: {app_route}."
                        f" message: {msg}"
                    )
                else:
                    logger.warning(
                        f"Got HTTP status_code={status} from server. app_route: {app_route}."
                        f" message: {message}"
                    )
            return is_bad

        def is_423(result: Tuple[int, dict]) -> bool:
            """Return True if get_content result has 423 status.

            Indicates a retryable transaction on the server.
            """
            status = result[0]
            is_bad = status == 423
            if is_bad:
                logger.info(
                    f"Got HTTP status_code=423 from server. app_route: {app_route}. "
                    f"Retrying as per design."
                    f" message: {message}"
                )
            return is_bad

        def raise_if_exceed_retry(retry_state: tenacity.RetryCallState) -> None:
            """If we trigger retry error, raise informative RuntimeError."""
            logger.exception(f"Retry exceeded. {retry_state}")
            status, content = retry_state.outcome.result()  # type: ignore
            raise RuntimeError(
                f"Exceeded HTTP request retry budget. Status code was {status} "
                f"and content was {content}"
            )

        # so we can access it in tests
        self._retry = tenacity.Retrying(
            stop=tenacity.stop_after_delay(self.stop_after_delay),
            wait=tenacity.wait_exponential(self.max_retries),
            retry=(
                tenacity.retry_if_result(is_5XX)
                | tenacity.retry_if_result(is_423)
                | tenacity.retry_if_exception_type(requests.ConnectionError)
            ),
            retry_error_callback=raise_if_exceed_retry,
        )

        return self._retry.__call__(
            self._send_request, app_route, message, request_type
        )

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
            )
        elif request_type == "get":
            params = message.copy()
            params["client_jobmon_version"] = __version__
            response = requests.get(
                route,
                params=params,
                data=json.dumps(self.server_structlog_context),
                headers={"Content-Type": "application/json"},
            )
        elif request_type == "put":
            params = {"client_jobmon_version": __version__}
            response = requests.put(
                route,
                params=params,
                json=message,
                headers={"Content-Type": "application/json"},
            )
        else:
            raise ValueError(
                f"request_type must be one of 'get', 'post', or 'put'. Got {request_type}"
            )

        status_code, content = get_content(response)
        logger.debug(f"Route: {route}; status: {status_code}; content: {content}")
        return status_code, content


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
