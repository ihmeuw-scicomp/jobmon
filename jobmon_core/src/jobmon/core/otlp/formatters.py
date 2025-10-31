"""Compatibility layer for legacy OTLP formatter imports."""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from .utils import add_span_details_processor
from .utils import get_current_span_details as _get_current_span_details


def get_current_span_details() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Expose span detail helper for legacy import paths."""
    return _get_current_span_details()


class JobmonOTLPFormatter(logging.Formatter):
    """Formatter that adds OpenTelemetry span details to jobmon logs."""

    def format(self, record: logging.LogRecord) -> str:
        span_id, trace_id, parent_span_id = get_current_span_details()
        record.span_id = span_id
        record.trace_id = trace_id
        record.parent_span_id = parent_span_id
        return super().format(record)


__all__ = [
    "JobmonOTLPFormatter",
    "add_span_details_processor",
    "get_current_span_details",
]
