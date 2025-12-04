"""Shared fixtures for logging tests.

This module provides parameterized fixtures for testing logging configuration
across all Jobmon components in a consistent manner.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional
from unittest.mock import MagicMock

import pytest


@dataclass
class ComponentConfig:
    """Configuration for a Jobmon component's logging tests."""

    name: str  # Component name (e.g., "client", "server")
    cli_class_path: str  # Full import path to CLI class
    logger_name: str  # Logger name (e.g., "jobmon.client")
    has_otlp_flush: bool = False  # Whether CLI uses otlp_flush_on_exit
    cli_run_method: Optional[str] = None  # Method to mock for CLI execution
    cli_main_args: str = "test"  # Default args for CLI.main()


# Component configurations - the source of truth for all component tests
COMPONENTS = [
    ComponentConfig(
        name="client",
        cli_class_path="jobmon.client.cli.ClientCLI",
        logger_name="jobmon.client",
        has_otlp_flush=False,
        cli_run_method=None,
        cli_main_args="version",
    ),
    ComponentConfig(
        name="server",
        cli_class_path="jobmon.server.cli.ServerCLI",
        logger_name="jobmon.server.web",
        has_otlp_flush=False,
        cli_run_method=None,  # Server CLI doesn't have a run_server method
        cli_main_args="test",
    ),
    ComponentConfig(
        name="distributor",
        cli_class_path="jobmon.distributor.cli.DistributorCLI",
        logger_name="jobmon.distributor",
        has_otlp_flush=True,
        cli_run_method="run_distributor",
        cli_main_args="start --cluster_name test --workflow_run_id 123",
    ),
    ComponentConfig(
        name="worker",
        cli_class_path="jobmon.worker_node.cli.WorkerNodeCLI",
        logger_name="jobmon.worker_node",
        has_otlp_flush=True,
        cli_run_method=None,
        cli_main_args="test",
    ),
]


@pytest.fixture(params=COMPONENTS, ids=lambda c: c.name)
def component(request) -> ComponentConfig:
    """Parameterized fixture providing component configuration.

    This fixture runs the test once for each component (client, server,
    distributor, worker), making it easy to verify consistent behavior.
    """
    return request.param


@pytest.fixture(params=[c for c in COMPONENTS if c.has_otlp_flush], ids=lambda c: c.name)
def component_with_otlp_flush(request) -> ComponentConfig:
    """Parameterized fixture for components that flush OTLP on exit.

    Only distributor and worker have OTLP flush on exit.
    """
    return request.param


def get_cli_class(component: ComponentConfig) -> type:
    """Dynamically import and return the CLI class for a component."""
    module_path, class_name = component.cli_class_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)


def create_mock_args(
    component: ComponentConfig, cli: Any, return_value: Any = None
) -> MagicMock:
    """Create mock args for CLI execution."""
    mock_args = MagicMock()

    if component.cli_run_method:
        # Get the method to mock
        run_method = getattr(cli, component.cli_run_method)
        mock_args.func = MagicMock(return_value=return_value)
    else:
        mock_args.func = lambda args: return_value

    return mock_args


def clear_logger_handlers(logger_name: str) -> logging.Logger:
    """Clear handlers from a logger and return it."""
    logger = logging.getLogger(logger_name)
    logger.handlers.clear()
    return logger


@pytest.fixture
def clean_logger(component: ComponentConfig) -> Callable[[], logging.Logger]:
    """Fixture that provides a function to get a clean logger for the component."""

    def get_clean_logger() -> logging.Logger:
        return clear_logger_handlers(component.logger_name)

    return get_clean_logger


@pytest.fixture
def template_content() -> str:
    """Standard logging template content for tests."""
    return """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(name)s - %(levelname)s - %(message)s"

handlers:
  test_handler:
    class: logging.StreamHandler
    level: INFO
    formatter: simple

loggers:
  {logger_name}:
    handlers: [test_handler]
    level: INFO
    propagate: false
"""


@pytest.fixture
def override_content() -> str:
    """Override logging configuration content for tests."""
    return """
version: 1
disable_existing_loggers: false

formatters:
  override:
    format: "OVERRIDE: %(message)s"

handlers:
  override_handler:
    class: logging.StreamHandler
    level: DEBUG
    formatter: override

loggers:
  {logger_name}:
    handlers: [override_handler]
    level: DEBUG
    propagate: false
"""

