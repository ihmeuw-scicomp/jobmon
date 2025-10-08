"""Logging configuration for jobmon client applications.

This module provides the standard logging configuration used by
workflow.run(configure_logging=True).

The module supports both legacy dict-based configs and new template-based
configurations. Requester logs are automatically captured by OTLP when
enabled (handled by the Requester class itself).
"""

from __future__ import annotations

import logging
import logging.config
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
    try:
        from jobmon.core.config.logconfig_utils import configure_logging_with_overrides
        from jobmon.core.config.structlog_config import configure_structlog

        # Get default template path
        current_dir = os.path.dirname(__file__)
        default_template_path = os.path.join(
            current_dir, "config/logconfig_client.yaml"
        )

        # Configure Python logging with override support
        configure_logging_with_overrides(
            default_template_path=default_template_path,
            config_section="client",
            fallback_config=default_config,
        )

        # Configure structlog so requester logs use formatters
        configure_structlog(component_name="client")

    except Exception:
        # Fall back to basic configuration for any error:
        # - ImportError: core module not available
        # - FileNotFoundError: template file missing
        # - yaml.YAMLError: malformed YAML
        # - ValueError: template resolution errors
        # - PermissionError: file access issues
        # - OSError: other I/O errors
        # - logging configuration errors
        try:
            logging.config.dictConfig(default_config)
        except Exception:
            # If even the fallback config fails, set up minimal logging
            logging.basicConfig(
                level=logging.INFO,
                format=_DEFAULT_LOG_FORMAT,
                datefmt="%Y-%m-%d %H:%M:%S",
                stream=sys.stdout,
            )
