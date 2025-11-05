"""Elegant OTLP integration for jobmon that prevents global log pollution."""

from __future__ import annotations

try:
    # Actually test if the required OpenTelemetry modules are available
    import opentelemetry.trace  # noqa: F401

    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False

from .formatters import JobmonOTLPFormatter
from .handlers import JobmonOTLPLoggingHandler, JobmonOTLPStructlogHandler
from .manager import (
    JobmonOTLPManager,
    create_log_exporter,
    get_logger,
    get_shared_logger_provider,
    initialize_jobmon_otlp,
    log_validation_results,
    otlp_flush_on_exit,
    validate_and_log_otlp_config,
    validate_logging_config_otlp,
    validate_otlp_exporter_config,
)
from .utils import add_span_details_processor, get_current_span_details

# Legacy alias for backward compatibility
OpenTelemetryLogFormatter = JobmonOTLPFormatter

__all__ = [
    "OTLP_AVAILABLE",
    "JobmonOTLPManager",
    "initialize_jobmon_otlp",
    "create_log_exporter",
    "get_logger",
    "get_shared_logger_provider",
    "JobmonOTLPLoggingHandler",
    "JobmonOTLPStructlogHandler",
    "JobmonOTLPFormatter",
    "otlp_flush_on_exit",
    "get_current_span_details",
    "add_span_details_processor",
    "OpenTelemetryLogFormatter",
    "log_validation_results",
    "validate_and_log_otlp_config",
    "validate_logging_config_otlp",
    "validate_otlp_exporter_config",
]
