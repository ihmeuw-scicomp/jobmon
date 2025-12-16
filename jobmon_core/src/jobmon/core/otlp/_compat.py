"""OTLP availability detection - isolated to avoid cyclic imports."""

from __future__ import annotations

try:
    # Test if the required OpenTelemetry modules are available
    import opentelemetry.trace  # noqa: F401

    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False

__all__ = ["OTLP_AVAILABLE"]
