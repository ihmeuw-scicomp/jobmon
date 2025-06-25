"""OpenTelemetry utilities for the jobmon server.

This module contains server-specific OpenTelemetry functionality that works
with auto-instrumentation. For client code, use jobmon.core.otlp.OtlpAPI instead.
"""

import logging
import os
from typing import Any, Dict, Optional, Tuple

from opentelemetry import trace

from jobmon.core.configuration import JobmonConfig


def configure_otlp_exporters(config: JobmonConfig) -> None:
    """Configure OTLP exporters via environment variables for auto-instrumentation.

    Auto-instrumentation uses standard OpenTelemetry environment variables,
    so we translate our custom jobmon config to the expected env vars.
    """
    # Configure traces exporter
    span_exporter = config.get("otlp", "span_exporter")
    if span_exporter:
        try:
            span_config = config.get_section(span_exporter)
            endpoint = span_config.get("endpoint")
            if endpoint:
                # Set standard OpenTelemetry environment variables
                os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = f"https://{endpoint}"
                os.environ["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = "grpc"
        except Exception:
            pass  # Continue if span exporter config is missing

    # Configure logs exporter
    log_exporter = config.get("otlp", "log_exporter")
    if log_exporter:
        try:
            log_config = config.get_section(log_exporter)
            endpoint = log_config.get("endpoint")
            if endpoint:
                os.environ["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"] = f"https://{endpoint}"
                os.environ["OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"] = "grpc"
        except Exception:
            pass  # Continue if log exporter config is missing

    # Set service name and deployment environment
    try:
        deployment_env = config.get("otlp", "deployment_environment")
        os.environ["OTEL_SERVICE_NAME"] = "jobmon"
        os.environ["OTEL_DEPLOYMENT_ENVIRONMENT"] = deployment_env
        os.environ["OTEL_RESOURCE_ATTRIBUTES"] = (
            f"deployment.environment={deployment_env},service.name=jobmon"
        )
    except Exception:
        pass


def get_span_details() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Retrieve details of the current span.

    This is a standalone utility function that works with any OpenTelemetry setup.
    """
    span = trace.get_current_span()

    # Check if there's a valid span
    if not span or not span.is_recording():
        return None, None, None

    ctx = span.get_span_context()

    # Get parent span, but handle if it doesn't exist
    parent = getattr(span, "parent", None)

    span_id = format(ctx.span_id, "016x") if ctx and ctx.span_id else None
    trace_id = format(ctx.trace_id, "032x") if ctx and ctx.trace_id else None
    parent_span_id = format(parent.span_id, "016x") if parent else None

    return span_id, trace_id, parent_span_id


class OpenTelemetryLogFormatter(logging.Formatter):
    """Formatter that adds OpenTelemetry spans to log records."""

    def format(self, record: logging.LogRecord) -> str:
        span_id, trace_id, parent_span_id = get_span_details()
        record.span_id = span_id
        record.trace_id = trace_id
        record.parent_span_id = parent_span_id
        return super().format(record)


def add_span_details_processor(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Structlog processor to add OpenTelemetry span details to log entries.

    Args:
        logger: The logger instance (not used, but required by Structlog processor signature).
        method_name: The logging method name (e.g., "info", "debug").
        event_dict: The event dictionary representing the log entry.

    Returns:
        The modified event dictionary with OpenTelemetry span details added.
    """
    span_id, trace_id, parent_span_id = get_span_details()
    if trace_id or span_id or parent_span_id:
        event_dict.update(
            {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
            }
        )
    return event_dict
