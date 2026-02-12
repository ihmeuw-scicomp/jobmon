"""Logging configuration for jobmon server applications.

This module provides the standard logging configuration used by
server startup and API initialization.

Configuration is generated programmatically with support for user overrides
via JobmonConfig. Server logs are automatically captured by OTLP when
enabled (handled by the server configuration overrides).
"""

from __future__ import annotations

import logging.config
import os
import sys
from typing import Any, Dict

from jobmon.core.config.logconfig_utils import (
    generate_component_logconfig,
    merge_logconfig_sections,
)
from jobmon.core.configuration import JobmonConfig

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


# Module-level flag to prevent duplicate configuration
_server_logging_configured = False


def configure_server_logging() -> None:
    """Configure server logging with programmatic generation and user override support.

    This is the primary interface for configuring server logging. It supports:
    1. User file overrides via logging.server_logconfig_file
    2. User section overrides via logging.server.*
    3. Environment variable overrides
    4. Programmatic base configuration

    Configuration precedence:
    1. Custom file (logging.server_logconfig_file) - complete replacement
    2. Section overrides (logging.server.*) - merged with base
    3. Programmatic base: generate_component_logconfig("server")

    Note: Server OTLP is handled separately by the server OTLP manager.
    """
    global _server_logging_configured

    # Prevent duplicate configuration in multi-worker environments
    if _server_logging_configured:
        return

    # Load configuration with override support
    logconfig_data = _load_server_logconfig_with_overrides()

    # Configure Python stdlib logging
    try:
        logging.config.dictConfig(logconfig_data)
    except Exception:
        # Fallback to basic config
        logging.config.dictConfig(default_config)

    # Configure structlog to integrate with stdlib loggers
    # This must come AFTER stdlib logging is configured
    from jobmon.core.config.structlog_config import configure_structlog

    configure_structlog(component_name="server")

    _server_logging_configured = True


def _load_server_logconfig_with_overrides() -> Dict[str, Any]:
    """Load server logconfig with file and section override support.

    Returns:
        Logconfig dictionary ready for logging.config.dictConfig()
    """
    from jobmon.core.config.template_loader import load_logconfig_with_templates

    try:
        config = JobmonConfig()

        # Check for file-based override first (highest precedence)
        try:
            custom_file = config.get("logging", "server_logconfig_file")
            if custom_file and os.path.exists(custom_file):
                logconfig_from_file = load_logconfig_with_templates(custom_file)
                logconfig_from_file["disable_existing_loggers"] = True
                return logconfig_from_file
        except Exception:
            pass  # No file override, continue

        # Generate programmatic base configuration
        logconfig_data = generate_component_logconfig("server")

        # Apply section-based overrides if present
        try:
            section_overrides = config.get_section_coerced("logging")
            component_overrides = section_overrides.get("server", {})

            if component_overrides:
                logconfig_data = merge_logconfig_sections(
                    logconfig_data, component_overrides
                )
        except Exception:
            pass  # No section overrides, use base config

        return logconfig_data

    except Exception:
        # Return programmatic base if all else fails
        return generate_component_logconfig("server")
