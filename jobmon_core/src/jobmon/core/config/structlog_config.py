"""Shared structlog configuration for all jobmon components.

This module provides structlog configuration that enables:
1. Context variable merging (required for @bind_context decorator)
2. Standard log formatting (timestamps, log levels, etc.)
3. OpenTelemetry trace integration (optional)
4. Component identification in logs
"""

from __future__ import annotations

import os
import threading
from typing import Any, Callable, Dict, List, Optional

# Thread-local storage for event_dict
_thread_local = threading.local()


def _store_event_dict_for_otlp(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Store event_dict in thread-local before it gets rendered to JSON.

    This preserves the raw structured data for OTLP to extract as attributes.
    """
    # Store a copy of the event_dict
    _thread_local.last_event_dict = dict(event_dict)
    return event_dict




def configure_structlog(
    component_name: Optional[str] = None,
    extra_processors: Optional[List[Callable]] = None,
) -> None:
    """Configure structlog for jobmon components.

    This function sets up structlog with processors that:
    1. Merge context variables (from bind_contextvars and @bind_context decorator)
    2. Add logger name, log level, timestamps
    3. Format exception info
    4. Optionally integrate with OpenTelemetry traces

    IMPORTANT: This must be called before using the @bind_context decorator
    or any structlog.contextvars.bind_contextvars() calls.

    Args:
        component_name: Component name to add to all logs (e.g., "distributor")
        extra_processors: Additional processors to insert into the chain
                         (e.g., add_span_details_processor for OTLP)

    Example:
        >>> # Basic usage
        >>> configure_structlog(component_name="distributor")

        >>> # With OTLP trace integration
        >>> from jobmon.core.otlp import add_span_details_processor
        >>> configure_structlog(
        ...     component_name="distributor",
        ...     extra_processors=[add_span_details_processor]
        ... )
    """
    import structlog

    if extra_processors is None:
        extra_processors = []

    # Build processor chain
    processors: List[Callable] = [
        # CRITICAL: merge_contextvars enables @bind_context decorator
        # and manual bind_contextvars() calls
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        *extra_processors,  # OTLP processors, custom processors go here
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _store_event_dict_for_otlp,  # Store for OTLP after exception processing
        # REMOVED: ProcessorFormatter.wrap_for_formatter - causes duplicate emissions
        # Our OTLP handler reads directly from _thread_local.last_event_dict
    ]

    # Add component name to all logs if provided
    if component_name:
        processors.insert(1, _create_component_processor(component_name))

    # Configure structlog globally
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _create_component_processor(component_name: str) -> Callable:
    """Create a processor that adds component name to all log events.

    Args:
        component_name: Name of the component (e.g., "distributor", "worker")

    Returns:
        Processor function that adds component field to event_dict
    """

    def add_component(
        logger: Any, method_name: str, event_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        event_dict["component"] = component_name
        return event_dict

    return add_component


def configure_structlog_with_otlp(component_name: str) -> None:
    """Configure structlog with OTLP trace integration if enabled.

    Checks JobmonConfig for telemetry.tracing settings and configures
    structlog with OpenTelemetry trace processors if enabled.

    This is the recommended function to call from component CLIs as it
    automatically handles OTLP configuration based on settings.

    Args:
        component_name: Component name (e.g., "distributor", "worker")

    Example:
        >>> # In distributor CLI
        >>> configure_structlog_with_otlp(component_name="distributor")

        >>> # In worker CLI
        >>> configure_structlog_with_otlp(component_name="worker")
    """
    try:
        from jobmon.core.configuration import JobmonConfig

        config = JobmonConfig()
    except Exception:
        # Config not available, use basic configuration
        configure_structlog(component_name=component_name)
        return

    # Check if OTLP tracing is enabled for this component
    try:
        telemetry_section = config.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})

        # Component-specific enable flag (e.g., "distributor_enabled")
        component_key = f"{component_name}_enabled"
        otlp_enabled = tracing_config.get(component_key, False)
    except Exception:
        otlp_enabled = False

    if otlp_enabled:
        # OTLP tracing is enabled, add trace processors
        try:
            from jobmon.core.otlp import add_span_details_processor

            configure_structlog(
                component_name=component_name,
                extra_processors=[add_span_details_processor],
            )
        except ImportError:
            # OTLP module not available, configure without it
            configure_structlog(component_name=component_name)
    else:
        # OTLP not enabled, basic configuration
        configure_structlog(component_name=component_name)


def is_structlog_configured() -> bool:
    """Check if structlog has been configured.

    Returns:
        True if structlog is configured, False otherwise

    Example:
        >>> if not is_structlog_configured():
        ...     configure_structlog(component_name="my_component")
    """
    import structlog

    # Check if structlog has been configured by looking for processors
    return structlog.is_configured()
