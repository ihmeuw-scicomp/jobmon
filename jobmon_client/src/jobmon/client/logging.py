"""Logging configuration for jobmon client applications.

This module provides the standard logging configuration used by
workflow.run(configure_logging=True).

Configuration is generated programmatically with support for user overrides
via JobmonConfig. Requester logs are automatically captured by OTLP when
enabled (handled by the Requester class itself).
"""

from __future__ import annotations

import logging
import logging.config
import sys
import threading
from typing import Any, Dict, Iterable, List, Optional

from jobmon.core.config.logconfig_utils import (
    generate_component_logconfig,
    merge_logconfig_sections,
)
from jobmon.core.config.structlog_config import _uses_stdlib_integration
from jobmon.core.configuration import JobmonConfig

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
    """Configure client logging with programmatic generation and user override support.

    This is the primary interface for configuring client logging. It supports:

    1. User file overrides via ``logging.client_logconfig_file``
    2. User section overrides via ``logging.client.*``
    3. Environment variable overrides
    4. Programmatic base configuration

    Configuration precedence:

    1. Custom file (``logging.client_logconfig_file``) - complete replacement
    2. Section overrides (``logging.client.*``) - merged with base
    3. Programmatic base: ``generate_component_logconfig("client")``

    Adapts to host application logging architecture:

    - If host uses direct rendering (like FHS): sets up minimal stdlib handlers
      that pass through formatted output without double-processing
    - If host uses stdlib integration: sets up full Jobmon logging configuration

    Note: Requester OTLP is handled separately by the Requester class.
    """
    import structlog

    # Ensure structlog is configured first (lazy initialization)
    ensure_structlog_configured()

    # Check host app's logging architecture
    current_config = structlog.get_config()
    logger_factory = current_config.get("logger_factory")
    wrapper_class = current_config.get("wrapper_class")

    if wrapper_class is None:
        wrapper_class = structlog.stdlib.BoundLogger

    host_uses_stdlib = _uses_stdlib_integration(logger_factory, wrapper_class)

    # Load configuration with override support
    logconfig_data = _load_client_logconfig_with_overrides()

    if host_uses_stdlib:
        # Standard stdlib integration - apply config as-is
        try:
            logging.config.dictConfig(logconfig_data)
        except Exception:
            # Fallback to basic config
            logging.config.dictConfig(default_config)
    else:
        # Direct rendering - strip non-OTLP handlers
        _configure_client_logging_for_direct_rendering(
            logconfig_data=logconfig_data,
            logger_factory=logger_factory,
            wrapper_class=wrapper_class,
        )


def _load_client_logconfig_with_overrides() -> Dict[str, Any]:
    """Load client logconfig with file and section override support.

    Returns:
        Logconfig dictionary ready for logging.config.dictConfig()
    """
    from jobmon.core.config.template_loader import load_logconfig_with_templates

    try:
        config = JobmonConfig()

        # Check for file-based override first (highest precedence)
        try:
            import os

            custom_file = config.get("logging", "client_logconfig_file")
            if custom_file and os.path.exists(custom_file):
                logconfig_from_file = load_logconfig_with_templates(custom_file)
                logconfig_from_file["disable_existing_loggers"] = True
                return logconfig_from_file
        except Exception:
            pass  # No file override, continue

        # Generate programmatic base configuration
        logconfig_data = generate_component_logconfig("client")

        # Apply section-based overrides if present
        try:
            section_overrides = config.get_section_coerced("logging")
            component_overrides = section_overrides.get("client", {})

            if component_overrides:
                logconfig_data = merge_logconfig_sections(
                    logconfig_data, component_overrides
                )
        except Exception:
            pass  # No section overrides, use base config

        return logconfig_data

    except Exception:
        # Return programmatic base if all else fails
        return generate_component_logconfig("client")


def _remove_non_jobmon_handlers(logconfig: Dict) -> List[str]:
    """Keep only Jobmon OTLP handlers when host renders output directly."""
    handlers = logconfig.get("handlers", {})

    # Identify OTLP handlers Jobmon manages
    jobmon_handlers = {
        name
        for name, cfg in handlers.items()
        if cfg.get("class", "").startswith("jobmon.core.otlp")
    }

    # Drop everything except Jobmon's handlers
    to_remove = set(handlers) - jobmon_handlers
    for name in to_remove:
        handlers.pop(name, None)

    # Update logger handler lists to only reference remaining handlers
    loggers = logconfig.get("loggers", {})
    jobmon_logger_names: List[str] = []

    for logger_name, logger_config in loggers.items():
        if isinstance(logger_name, str) and logger_name.startswith("jobmon."):
            jobmon_logger_names.append(logger_name)

        handler_list: List[str] = logger_config.get("handlers", [])
        if handler_list:
            logger_config["handlers"] = [h for h in handler_list if h in handlers]

    return jobmon_logger_names


def _configure_client_logging_for_direct_rendering(
    *,
    logconfig_data: Dict[str, Any],
    logger_factory: Any,
    wrapper_class: Any,
) -> None:
    """Configure logging for direct-rendering hosts while preserving telemetry.

    In direct rendering mode, the host application handles console output.
    We strip console handlers and only keep OTLP handlers for telemetry.
    """
    jobmon_logger_names = _remove_non_jobmon_handlers(logconfig_data)

    logging.config.dictConfig(logconfig_data)

    _ensure_jobmon_otlp_handlers(
        logconfig_data, jobmon_logger_names=jobmon_logger_names
    )


def _ensure_jobmon_otlp_handlers(
    logconfig_data: Dict[str, Any],
    jobmon_logger_names: Optional[Iterable[str]] = None,
) -> List[str]:
    """Attach OTLP handlers if jobmon loggers ended up without handlers."""
    attached: List[str] = []

    try:
        from jobmon.core.otlp import JobmonOTLPLoggingHandler
    except Exception:
        return attached

    if jobmon_logger_names is None:
        configured_loggers = logconfig_data.get("loggers", {})
        candidates = [
            name
            for name in configured_loggers
            if isinstance(name, str) and name.startswith("jobmon.")
        ]
    else:
        candidates = [name for name in jobmon_logger_names if isinstance(name, str)]

    for logger_name in candidates:
        logger = logging.getLogger(logger_name)

        if logger.handlers:
            continue

        try:
            handler = JobmonOTLPLoggingHandler()
        except Exception:
            continue

        logger.addHandler(handler)
        attached.append(logger_name)

    return attached
