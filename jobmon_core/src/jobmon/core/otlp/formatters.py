"""OTLP log formatters for jobmon."""

from __future__ import annotations

import logging

from .utils import get_current_span_details


class JobmonOTLPFormatter(logging.Formatter):
    """Formatter that adds OpenTelemetry span details to jobmon logs.

    Note: For handlers using _JobmonOTLPLoggingHandler, this formatter is not
    used for OTLP output (handler creates OTLPLogRecord directly). It only
    affects console/file output.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with OpenTelemetry span details."""
        span_id, trace_id, parent_span_id = get_current_span_details()
        record.span_id = span_id
        record.trace_id = trace_id
        record.parent_span_id = parent_span_id
        return super().format(record)
