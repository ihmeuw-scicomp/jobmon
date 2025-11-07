"""Compatibility layer for legacy OTLP formatter imports."""

from __future__ import annotations

from .utils import (
    JobmonOTLPFormatter,
    add_span_details_processor,
    get_current_span_details,
)

__all__ = [
    "JobmonOTLPFormatter",
    "add_span_details_processor",
    "get_current_span_details",
]
