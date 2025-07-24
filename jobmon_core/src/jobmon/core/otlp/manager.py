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
        """Initialize trace provider with jobmon resources and configure span exporters."""
        if self._initialized or not OTLP_AVAILABLE:
            return

        try:
            # Create shared resources
            resource_group = create_jobmon_resources()

            # Create trace provider
            self.tracer_provider = TracerProvider(resource=resource_group)

            # Configure span exporters from telemetry configuration
            self._configure_span_exporters()

            # Set the global tracer provider
            trace.set_tracer_provider(self.tracer_provider)

            self._initialized = True

        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to initialize OTLP: {e}")

    def _configure_span_exporters(self) -> None:
        """Configure span exporters from telemetry configuration."""
        if not OTLP_AVAILABLE or not self.tracer_provider:
            return

        try:
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            from jobmon.core.configuration import JobmonConfig

            config = JobmonConfig()
            telemetry_config = config.get_section_coerced("telemetry")
            tracing_config = telemetry_config.get("tracing", {})

            # Check if server tracing is enabled
            if not tracing_config.get("server_enabled", False):
                return

            # Get the configured span exporter
            span_exporter_name = tracing_config.get("span_exporter")
            if not span_exporter_name:
                return

            # Get the exporter configuration
            exporters_config = tracing_config.get("exporters", {})
            exporter_config = exporters_config.get(span_exporter_name)

            if not exporter_config:
                logging.getLogger(__name__).warning(
                    f"Span exporter '{span_exporter_name}' not found in configuration"
                )
                return

            # Create the exporter
            exporter = self._create_span_exporter(exporter_config)
            if exporter:
                # Add processor to tracer provider
                processor = BatchSpanProcessor(exporter)
                self.tracer_provider.add_span_processor(processor)
                logging.getLogger(__name__).info(
                    f"Configured span exporter: {span_exporter_name}"
                )
            else:
                logging.getLogger(__name__).warning(
                    f"Failed to create span exporter: {span_exporter_name}"
                )

        except Exception as e:
            logging.getLogger(__name__).warning(
                f"Failed to configure span exporters: {e}"
            )

    def _create_span_exporter(self, config: Any) -> Optional[Any]:
        """Create a span exporter from configuration dictionary."""
        try:
            import importlib

            module_name = config.get("module")
            class_name = config.get("class")

            if not module_name or not class_name:
                return None

            # Dynamically import and instantiate the exporter
            module = importlib.import_module(module_name)
            exporter_class = getattr(module, class_name)

            # Build exporter arguments
            exporter_args = {}

            # Common exporter arguments
            if "endpoint" in config:
                exporter_args["endpoint"] = config["endpoint"]
            if "headers" in config:
                exporter_args["headers"] = dict(config["headers"])
            if "timeout" in config:
                exporter_args["timeout"] = config["timeout"]
            if "compression" in config:
                # Handle compression parameter - convert string to grpc.Compression enum
                compression_str = config["compression"].lower()
                try:
                    import grpc  # type: ignore[import-untyped]

                    if compression_str == "gzip":
                        exporter_args["compression"] = grpc.Compression.Gzip
                    elif compression_str == "deflate":
                        exporter_args["compression"] = grpc.Compression.Deflate
                    elif compression_str in ("none", "nocompression"):
                        exporter_args["compression"] = grpc.Compression.NoCompression
                    else:
                        logging.getLogger(__name__).warning(
                            f"Unknown compression type: {compression_str}, "
                            "skipping compression"
                        )
                except ImportError:
                    logging.getLogger(__name__).warning(
                        "grpc not available, skipping compression parameter"
                    )
            if "insecure" in config:
                exporter_args["insecure"] = config["insecure"]
            if "options" in config:
                # Convert list of [key, value] pairs to list of tuples
                options_list = config["options"]
                exporter_args["options"] = [tuple(option) for option in options_list]

            return exporter_class(**exporter_args)

        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to create exporter: {e}")
            return None

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
