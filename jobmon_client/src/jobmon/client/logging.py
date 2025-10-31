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
import threading
from typing import Dict

# Lazy configuration state
_structlog_configured_lock = threading.Lock()
_structlog_configured_by_jobmon = False

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


# Lazy structlog configuration
def ensure_structlog_configured() -> None:
    """Ensure structlog is configured for Jobmon, using lazy initialization.

    This function is called at the start of key Jobmon operations (workflow.run(),
    workflow.bind(), etc.) to ensure structlog is configured. It uses a lock to
    ensure thread-safe initialization.

    Behavior:
    - If host has already configured structlog: prepend Jobmon processors
    - If structlog is unconfigured: configure it with Jobmon defaults
    - If already called: no-op (idempotent)

    This lazy approach ensures host applications always have the opportunity to
    configure structlog first, eliminating import-order dependencies.
    """
    global _structlog_configured_by_jobmon

    # Fast path: already configured
    if _structlog_configured_by_jobmon:
        return

    with _structlog_configured_lock:
        # Double-check after acquiring lock
        if _structlog_configured_by_jobmon:
            return

        from jobmon.core.config.structlog_config import (
            configure_structlog,
            is_structlog_configured,
            prepend_jobmon_processors_to_existing_config,
        )

        if is_structlog_configured():
            # Host app configured structlog - prepend our processors
            prepend_jobmon_processors_to_existing_config()
        else:
            # No one configured structlog yet - use Jobmon defaults
            configure_structlog(component_name="client")

        _structlog_configured_by_jobmon = True


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

    Adapts to host application logging architecture:
    - If host uses direct rendering (like FHS): sets up minimal stdlib handlers
      that pass through formatted output without double-processing
    - If host uses stdlib integration: sets up full Jobmon logging configuration

    Note: Requester OTLP is handled separately by the Requester class.
    """
    import structlog

    from jobmon.core.config.logconfig_utils import configure_logging_with_overrides

    # Ensure structlog is configured first (lazy initialization)
    ensure_structlog_configured()

    # Get default template path
    current_dir = os.path.dirname(__file__)
    default_template_path = os.path.join(current_dir, "config/logconfig_client.yaml")

    # Check host app's logging architecture
    current_config = structlog.get_config()
    logger_factory = current_config.get("logger_factory")
    host_uses_direct_rendering = hasattr(logger_factory, "__name__") and (
        "PrintLogger" in str(logger_factory) or "Print" in str(logger_factory)
    )

    if host_uses_direct_rendering:
        # Host app (like FHS) handles output directly - set up minimal passthrough handlers
        _configure_minimal_client_logging_for_direct_rendering()
    else:
        # Host app uses stdlib integration - set up full Jobmon logging
        configure_logging_with_overrides(
            default_template_path=default_template_path,
            config_section="client",
            fallback_config=default_config,
        )


def _configure_minimal_client_logging_for_direct_rendering() -> None:
    """Configure minimal stdlib logging when host app uses direct rendering.

    When host applications (like FHS) use direct structlog rendering that prints
    output directly, we set up minimal stdlib infrastructure that:
    1. Doesn't interfere with host app's direct output
    2. Provides logger hierarchy for Jobmon components
    3. Allows OTLP telemetry to work (data comes from thread-local, not handlers)

    Since FHS handles all console output through structlog rendering, we don't
    set up console handlers to avoid double-processing or conflicts.
    """
    import logging.config

    # Minimal configuration - just logger hierarchy, no handlers
    # OTLP telemetry works through thread-local data, not stdlib handlers
    minimal_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {
            # Define logger hierarchy without handlers (since FHS handles output)
            "jobmon.client": {
                "level": "INFO",
                "propagate": False,  # Don't interfere with FHS
            },
            "jobmon.core": {
                "level": "WARNING",
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(minimal_config)
