"""Logging configuration for jobmon client applications.

This module provides the standard logging configuration used by
workflow.run(configure_logging=True).

The module supports both legacy dict-based configs and new template-based
configurations. Requester logs are automatically captured by OTLP when
enabled (handled by the Requester class itself).
"""

from __future__ import annotations

import os
import sys
from typing import Dict

# Legacy default configuration - used as fallback only
_DEFAULT_LOG_FORMAT = (
    "%(asctime)s [%(name)-12s] %(module)s %(levelname)-8s: %(message)s"
)

default_config: Dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": _DEFAULT_LOG_FORMAT, "datefmt": "%Y-%m-%d %H:%M:%S"}
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": sys.stdout,
        }
    },
}


# Legacy configuration - kept for backward compatibility if needed
# The primary interface is now configure_client_logging()


def configure_client_logging() -> None:
    """Configure client logging with template and user override support.

    This is the primary interface for configuring client logging. It supports:
    1. Default template-based configuration
    2. User file overrides via logging.client_logconfig_file
    3. User section overrides via logging.client.*
    4. Environment variable overrides

    Configuration precedence:
    1. Custom file (logging.client_logconfig_file)
    2. Section overrides (logging.client.formatters/handlers/loggers)
    3. Default template (logconfig_client.yaml)
    4. Basic fallback configuration

    Note: Requester OTLP is handled separately by the Requester class.
    """
    from jobmon.core.config.logconfig_utils import configure_logging_with_overrides

    # Get default template path
    current_dir = os.path.dirname(__file__)
    default_template_path = os.path.join(current_dir, "config/logconfig_client.yaml")

    # Configure Python logging with override support
    # Note: structlog is already configured in __init__.py
    configure_logging_with_overrides(
        default_template_path=default_template_path,
        config_section="client",
        fallback_config=default_config,
    )
