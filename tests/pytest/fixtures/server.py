"""Server fixtures for Jobmon tests.

These fixtures handle web server process management and client configuration.

Key fixtures:
    api_prefix: The API route prefix (/api/v3)
    web_server_process: Starts Jobmon server in a subprocess
    client_env: Configures client to connect to test server
    requester_no_retry: Client requester with no retry logic
"""

import multiprocessing as mp
import os
import platform
import signal
import sys
from time import sleep
from types import TracebackType
from typing import Any, Optional

import pytest
import requests
import uvicorn

from jobmon.core.requester import Requester

# Global API prefix
_api_prefix = "/api/v3"


@pytest.fixture(scope="session")
def api_prefix():
    """Return the API route prefix.

    Returns:
        str: The API prefix (/api/v3)
    """
    return _api_prefix


class WebServerProcess:
    """Context manager that runs the Jobmon web server in a subprocess.

    Creates an isolated web server process for testing. Each test session
    gets its own server on a unique port (based on PID).

    Usage:
        with WebServerProcess() as web:
            url = f"http://{web.web_host}:{web.web_port}/api/v3/health"
            response = requests.get(url)

    Attributes:
        web_host: Server hostname (127.0.0.1)
        web_port: Server port (10000 + PID % 30000)
        api_prefix: API route prefix (/api/v3)
    """

    def __init__(self) -> None:
        """Initialize the web server process configuration.

        Uses localhost and a PID-based port to avoid conflicts.
        """
        # Always use localhost for test server to avoid DNS/network issues
        self.web_host = "127.0.0.1"
        self.web_port = str(10_000 + os.getpid() % 30_000)
        self.api_prefix = _api_prefix

    def _run_server_with_handler(self) -> None:
        """Run the server with signal handlers.

        Separate method for pickle compatibility with multiprocessing.
        Sets up SIGTERM handler for graceful shutdown.
        """

        def sigterm_handler(_signo: int, _stack_frame: Any) -> None:
            # Catch SIGTERM and shut down with 0 so pycov finalizers are run
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)

        from jobmon.core.config.logconfig_utils import configure_component_logging
        from jobmon.core.config.structlog_config import configure_structlog
        from jobmon.server.web.api import get_app

        # Configure logging using core utilities
        configure_component_logging("server")
        configure_structlog(component_name="server")

        app = get_app(versions=["v3"])
        uvicorn.run(app, host="0.0.0.0", port=int(self.web_port))

    def __enter__(self) -> "WebServerProcess":
        """Start the web service process.

        Spawns the server process and waits for it to be ready
        (responds to health check).

        Returns:
            WebServerProcess: Self for context manager usage

        Raises:
            TimeoutError: If server doesn't respond within 30 seconds
        """
        # Use spawn on macOS to avoid fork warnings in multi-threaded environment
        mp_method = "spawn" if platform.system() == "Darwin" else "fork"
        ctx = mp.get_context(mp_method)
        self.p1 = ctx.Process(target=self._run_server_with_handler)
        self.p1.start()

        # Wait for server to be up (max 10 tries, 3 seconds apart)
        status = 404
        count = 0
        max_tries = 10

        while status != 200 and count < max_tries:
            try:
                count += 1
                url = f"http://{self.web_host}:{self.web_port}{self.api_prefix}/health"
                print(url)
                r = requests.get(
                    url,
                    headers={"Content-Type": "application/json"},
                )
                status = r.status_code
            except Exception as e:
                if count >= max_tries:
                    raise TimeoutError(
                        f"Out-of-process jobmon services did not answer after "
                        f"{count} attempts, probably failed to start."
                    ) from e
            # Sleep outside try block
            sleep(3)

        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        """Terminate the web service process.

        Sends SIGTERM and waits for graceful shutdown.
        """
        self.p1.terminate()
        self.p1.join()


@pytest.fixture(scope="session")
def web_server_process(db_engine):
    """Start the Jobmon web server in a separate process.

    Session-scoped: server starts once and is shared by all tests.
    Depends on db_engine to ensure database is initialized first.

    Args:
        db_engine: The database engine fixture

    Yields:
        dict: Server connection info {"JOBMON_HOST": str, "JOBMON_PORT": str}
    """
    with WebServerProcess() as web:
        yield {"JOBMON_HOST": web.web_host, "JOBMON_PORT": web.web_port}


@pytest.fixture(scope="function")
def client_env(web_server_process, monkeypatch):
    """Configure client to connect to the local test server.

    Sets JOBMON__HTTP__SERVICE_URL environment variable and creates
    a requester instance that uses the test configuration.

    Function-scoped because monkeypatch is function-scoped.

    Args:
        web_server_process: The server connection info
        monkeypatch: pytest monkeypatch fixture

    Yields:
        str: The full service URL (e.g., http://127.0.0.1:12345/api/v3)
    """
    service_url = f'http://{web_server_process["JOBMON_HOST"]}:{web_server_process["JOBMON_PORT"]}'
    monkeypatch.setenv("JOBMON__HTTP__SERVICE_URL", service_url)

    # Create requester instance that will use the test configuration
    requester = Requester.from_defaults()
    yield requester.url


@pytest.fixture(scope="function")
def requester_no_retry(client_env):
    """Create a requester with no retry logic.

    Useful for tests that need to verify error handling without retries.

    Args:
        client_env: The client environment URL

    Returns:
        Requester: A requester instance with retries_timeout=0
    """
    return Requester(client_env, retries_timeout=0)
