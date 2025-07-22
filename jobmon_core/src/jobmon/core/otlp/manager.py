"""Core OTLP manager for jobmon-scoped telemetry."""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

from . import OTLP_AVAILABLE

if OTLP_AVAILABLE:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

from .resources import create_jobmon_resources


class JobmonOTLPManager:
    """Minimal OTLP manager for shared trace resources only.

    With pure separation, this manager only handles:
    - Trace provider setup (for distributed tracing)
    - Resource detection (shared across components)
    - Request instrumentation (shared utility)

    Log exporters are handled directly by handlers with pre-configured exporters.
    """

    _instance: Optional[JobmonOTLPManager] = None

    def __init__(self) -> None:
        """Initialize the minimal OTLP manager."""
        self.tracer_provider: Optional[Any] = None
        self._initialized = False

    @classmethod
    def get_instance(cls: Type[JobmonOTLPManager]) -> JobmonOTLPManager:
        """Get or create the singleton OTLP manager."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def initialize(self) -> None:
        """Initialize trace provider with jobmon resources."""
        if self._initialized or not OTLP_AVAILABLE:
            return

        try:
            # Create shared resources
            resource_group = create_jobmon_resources()

            # Only create trace provider (logging handled by pure separation)
            self.tracer_provider = TracerProvider(resource=resource_group)
            trace.set_tracer_provider(self.tracer_provider)

            self._initialized = True

        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to initialize OTLP: {e}")

    def get_tracer(self, name: str) -> Optional[Any]:
        """Get a tracer for distributed tracing."""
        if not OTLP_AVAILABLE or not self.tracer_provider:
            return None
        return self.tracer_provider.get_tracer(name)

    @classmethod
    def instrument_requests(cls: Type[JobmonOTLPManager]) -> None:
        """Instrument requests library for HTTP tracing."""
        if not OTLP_AVAILABLE:
            return

        try:
            from opentelemetry.instrumentation.requests import RequestsInstrumentor

            RequestsInstrumentor().instrument()
        except ImportError:
            pass

    def shutdown(self) -> None:
        """Shutdown trace provider."""
        if not self._initialized:
            return

        try:
            if self.tracer_provider and hasattr(self.tracer_provider, "shutdown"):
                self.tracer_provider.shutdown()
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error during OTLP shutdown: {e}")
        finally:
            self._initialized = False


def initialize_jobmon_otlp() -> JobmonOTLPManager:
    """Initialize minimal OTLP for shared resources (traces only).

    For log export, use create_log_exporter() to get pre-configured exporters
    that can be passed to JobmonOTLPLoggingHandler.

    Returns:
        The minimal OTLP manager instance
    """
    manager = JobmonOTLPManager.get_instance()
    manager.initialize()
    return manager


def create_log_exporter(**kwargs: Any) -> Optional[Any]:
    """Create a pre-configured log exporter for client applications.

    This factory function creates exporters that can be passed to
    JobmonOTLPLoggingHandler for pure separation.

    Args:
        **kwargs: Exporter configuration (endpoint, headers, etc.)

    Returns:
        Pre-configured OTLP log exporter, or None if unavailable

    Example:
        exporter = create_log_exporter(
            endpoint="otelcol.dev.aks:443",
            max_batch_size=8
        )
        handler = JobmonOTLPLoggingHandler(exporter=exporter)
    """
    if not OTLP_AVAILABLE:
        return None

    try:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

        # Default configuration for resource exhaustion prevention
        default_config = {
            "insecure": True,  # For internal development endpoints
        }

        # Merge with user configuration
        config = {**default_config, **kwargs}

        return OTLPLogExporter(**config)

    except Exception:
        return None
