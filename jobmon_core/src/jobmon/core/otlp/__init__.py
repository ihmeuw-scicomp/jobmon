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
from .manager import JobmonOTLPManager, create_log_exporter, initialize_jobmon_otlp
from .utils import add_span_details_processor, get_current_span_details

# Legacy alias for backward compatibility
OpenTelemetryLogFormatter = JobmonOTLPFormatter

__all__ = [
    "OTLP_AVAILABLE",
    "JobmonOTLPManager",
    "initialize_jobmon_otlp",
    "create_log_exporter",
    "JobmonOTLPLoggingHandler",
    "JobmonOTLPStructlogHandler",
    "JobmonOTLPFormatter",
    "get_current_span_details",
    "add_span_details_processor",
    "OpenTelemetryLogFormatter",
]
