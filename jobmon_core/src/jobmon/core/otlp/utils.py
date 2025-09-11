"""OTLP utility functions for jobmon."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from . import OTLP_AVAILABLE

if OTLP_AVAILABLE:
    from opentelemetry import trace


def get_current_span_details() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Get details of the current OpenTelemetry span.

    Returns:
        Tuple of (span_id, trace_id, parent_span_id) as hex strings, or (None, None, None)
    """
    if not OTLP_AVAILABLE:
        return None, None, None

    try:
        span = trace.get_current_span()
        if not span or not span.is_recording():
            return None, None, None

        ctx = span.get_span_context()
        parent = getattr(span, "parent", None)

        span_id = format(ctx.span_id, "016x") if ctx and ctx.span_id else None
        trace_id = format(ctx.trace_id, "032x") if ctx and ctx.trace_id else None
        parent_span_id = format(parent.span_id, "016x") if parent else None

        return span_id, trace_id, parent_span_id
    except Exception:
        return None, None, None


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
    span_id, trace_id, parent_span_id = get_current_span_details()
    if trace_id or span_id or parent_span_id:
        event_dict.update(
            {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
            }
        )
    return event_dict
