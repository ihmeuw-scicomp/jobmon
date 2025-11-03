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
import threading
from typing import Any, Dict, List, Optional

from jobmon.core.config.logconfig_utils import (
    configure_logging_with_overrides,
    load_logconfig_with_overrides,
)
from jobmon.core.config.structlog_config import _uses_stdlib_integration

# Lazy configuration state
_structlog_configured_lock = threading.Lock()
_structlog_configured_by_jobmon = False

_telemetry_debug_enabled: Optional[bool] = None
_telemetry_debug_emitted = False

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

    # Ensure structlog is configured first (lazy initialization)
    ensure_structlog_configured()

    # Get default template path
    current_dir = os.path.dirname(__file__)
    default_template_path = os.path.join(current_dir, "config/logconfig_client.yaml")

    # Check host app's logging architecture
    current_config = structlog.get_config()
    logger_factory = current_config.get("logger_factory")
    wrapper_class = current_config.get("wrapper_class")

    if wrapper_class is None:
        wrapper_class = structlog.stdlib.BoundLogger

    host_uses_stdlib = _uses_stdlib_integration(logger_factory, wrapper_class)

    if host_uses_stdlib:
        configure_logging_with_overrides(
            default_template_path=default_template_path,
            config_section="client",
            fallback_config=default_config,
        )
    else:
        _configure_client_logging_for_direct_rendering(
            default_template_path=default_template_path,
            logger_factory=logger_factory,
            wrapper_class=wrapper_class,
        )


def _remove_non_jobmon_handlers(logconfig: Dict) -> None:
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
    for logger_config in loggers.values():
        handler_list: List[str] = logger_config.get("handlers", [])
        if handler_list:
            logger_config["handlers"] = [h for h in handler_list if h in handlers]


def _is_telemetry_debug_enabled() -> bool:
    """Return True when telemetry debugging is enabled via JobmonConfig."""
    global _telemetry_debug_enabled
    if _telemetry_debug_enabled is not None:
        return _telemetry_debug_enabled

    try:
        from jobmon.core.configuration import JobmonConfig

        telemetry_section = JobmonConfig().get_section_coerced("telemetry")
        _telemetry_debug_enabled = bool(telemetry_section.get("debug"))
    except Exception:
        _telemetry_debug_enabled = False

    return _telemetry_debug_enabled


def _emit_direct_rendering_debug(
    *,
    architecture: str,
    logger_factory: Any,
    wrapper_class: Any,
    logconfig_data: Dict[str, Any],
    fallback_attached: List[str],
) -> None:
    """Emit a single debug log describing the applied logging configuration."""
    global _telemetry_debug_emitted

    if _telemetry_debug_emitted or not _is_telemetry_debug_enabled():
        return

    try:
        telemetry_logger = logging.getLogger("jobmon.client.telemetry")

        configured_loggers = logconfig_data.get("loggers", {})
        runtime_handlers: Dict[str, List[str]] = {}
        for name in sorted(configured_loggers):
            handlers = logging.getLogger(name).handlers
            runtime_handlers[name] = [
                f"{handler.__class__.__module__}.{handler.__class__.__name__}"
                for handler in handlers
            ]

        wrapper_repr = None
        if isinstance(wrapper_class, type):
            wrapper_repr = f"{wrapper_class.__module__}.{wrapper_class.__name__}"
        else:
            wrapper_repr = repr(wrapper_class)

        payload = {
            "architecture": architecture,
            "logger_factory": repr(logger_factory),
            "wrapper_class": wrapper_repr,
            "configured_handlers": sorted(logconfig_data.get("handlers", {}).keys()),
            "configured_logger_handlers": {
                name: list(config.get("handlers", []))
                for name, config in configured_loggers.items()
            },
            "runtime_handlers": runtime_handlers,
            "fallback_attached": fallback_attached,
        }

        telemetry_logger.debug(
            "jobmon client logging configured",
            extra={"jobmon": payload},
        )
    except Exception:
        return

    _telemetry_debug_emitted = True


def _configure_client_logging_for_direct_rendering(
    *,
    default_template_path: str,
    logger_factory: Any,
    wrapper_class: Any,
) -> None:
    """Configure logging for direct-rendering hosts while preserving telemetry."""
    logconfig_data = load_logconfig_with_overrides(
        default_template_path=default_template_path,
        config_section="client",
    )

    _remove_non_jobmon_handlers(logconfig_data)

    logging.config.dictConfig(logconfig_data)

    fallback_attached = _ensure_jobmon_otlp_handlers(logconfig_data)

    _emit_direct_rendering_debug(
        architecture="direct",
        logger_factory=logger_factory,
        wrapper_class=wrapper_class,
        logconfig_data=logconfig_data,
        fallback_attached=fallback_attached,
    )


def _ensure_jobmon_otlp_handlers(logconfig_data: Dict[str, Any]) -> List[str]:
    """Attach OTLP handlers if jobmon loggers ended up without handlers."""
    attached: List[str] = []

    try:
        from jobmon.core.otlp import JobmonOTLPStructlogHandler
    except Exception:
        return attached

    configured_loggers = logconfig_data.get("loggers", {})

    for logger_name in configured_loggers:
        if not isinstance(logger_name, str) or not logger_name.startswith("jobmon."):
            continue

        logger = logging.getLogger(logger_name)

        if logger.handlers:
            continue

        try:
            handler = JobmonOTLPStructlogHandler()
        except Exception:
            continue

        logger.addHandler(handler)
        attached.append(logger_name)

    return attached
