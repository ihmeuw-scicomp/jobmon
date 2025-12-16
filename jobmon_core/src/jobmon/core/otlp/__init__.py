"""Elegant OTLP integration for jobmon that prevents global log pollution."""

from __future__ import annotations

# Import OTLP_AVAILABLE from isolated module to avoid cyclic imports
from ._compat import OTLP_AVAILABLE
from .formatters import JobmonOTLPFormatter
from .handlers import JobmonOTLPLoggingHandler, JobmonOTLPStructlogHandler
from .manager import (
    JobmonOTLPManager,
    create_log_exporter,
    get_logger,
    get_shared_logger_provider,
    initialize_jobmon_otlp,
    otlp_flush_on_exit,
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
    "validate_otlp_exporter_config",
]
