"""Core OTLP manager for jobmon-scoped telemetry."""

from __future__ import annotations

import atexit
import logging
import threading
from typing import Any, Optional, Type

from . import OTLP_AVAILABLE

if OTLP_AVAILABLE:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk._logs import LoggerProvider

from .resources import create_jobmon_resources

# Module-level singleton for shared logger provider with thread safety
_logger_provider: Optional[Any] = None
_logger_provider_lock = threading.Lock()


class JobmonOTLPManager:
    """OTLP manager for shared trace and log resources.

    This manager handles:
    - Trace provider setup (for distributed tracing)
    - Logger provider setup (for OTLP log export)
    - Resource detection (shared across components)
    - Request instrumentation (shared utility)

    All OTLP handlers should use the shared logger provider to avoid
    duplicate connections and log emissions.
    """

    _instance: Optional[JobmonOTLPManager] = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        """Initialize the OTLP manager."""
        self.tracer_provider: Optional[Any] = None
        self.logger_provider: Optional[Any] = None
        self._initialized = False
        self._log_processor_configured = False
        self._atexit_registered = False
        self._init_lock = threading.Lock()

    @classmethod
    def get_instance(cls: Type[JobmonOTLPManager]) -> JobmonOTLPManager:
        """Get or create the singleton OTLP manager with thread safety."""
        if cls._instance is None:
            with cls._instance_lock:
                # Double-check pattern: another thread might have created while we waited
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def initialize(self) -> None:
        """Initialize trace and log providers with jobmon resources."""
        if self._initialized or not OTLP_AVAILABLE:
            return

        with self._init_lock:
            # Double-check pattern: another thread might have initialized while we waited
            if self._initialized:
                return

            try:
                # Create shared resources
                resource_group = create_jobmon_resources()

                # Create trace provider
                self.tracer_provider = TracerProvider(resource=resource_group)

                # Create logger provider (shared across all OTLP handlers)
                self.logger_provider = LoggerProvider(resource=resource_group)

                # Configure log processor (single processor for all handlers)
                self._configure_log_processor()

                # Configure span exporters from telemetry configuration
                self._configure_span_exporters()

                # Set the global tracer provider
                trace.set_tracer_provider(self.tracer_provider)

                # Register atexit handler to ensure graceful shutdown on normal exit
                # This prevents telemetry loss when processes exit normally
                if not self._atexit_registered:
                    atexit.register(self._atexit_shutdown)
                    self._atexit_registered = True

                self._initialized = True

            except Exception:
                # Don't log here to avoid circular dependency during initialization
                pass

    def _configure_log_processor(self) -> None:
        """Configure log processor from telemetry configuration."""
        if not OTLP_AVAILABLE or not self.logger_provider:
            return

        # Guard against adding processor multiple times
        if self._log_processor_configured:
            # Don't log here to avoid circular dependency during initialization
            return

        try:
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            # Always set our new provider as the global one
            # This ensures that after shutdown+reinit, we use the new provider not the old one
            set_logger_provider(self.logger_provider)

            # Check if BatchLogRecordProcessor already exists on OUR provider
            # Processors are stored in _multi_log_record_processor._log_record_processors
            multi_processor = getattr(
                self.logger_provider, "_multi_log_record_processor", None
            )
            if multi_processor:
                existing_processors = getattr(
                    multi_processor, "_log_record_processors", []
                )
                if any(
                    isinstance(p, BatchLogRecordProcessor) for p in existing_processors
                ):
                    # Processor already exists, don't add another
                    self._log_processor_configured = True
                    return

            from jobmon.core.configuration import JobmonConfig

            config = JobmonConfig()
            telemetry_config = config.get_section_coerced("telemetry")
            logging_config = telemetry_config.get("logging", {})

            # Check if logging is enabled
            enabled = logging_config.get("enabled", False)
            if not enabled:
                return

            # Get the configured log exporter
            log_exporter_name = logging_config.get("log_exporter")
            if not log_exporter_name:
                return

            # Get the exporter configuration
            exporters_config = logging_config.get("exporters", {})
            exporter_config = exporters_config.get(log_exporter_name)

            if not exporter_config:
                # Don't log here to avoid circular dependency during initialization
                return

            # Create the exporter
            exporter = self._create_log_exporter(exporter_config)
            if exporter:
                # Use BatchLogRecordProcessor for efficient batching
                processor = BatchLogRecordProcessor(exporter)

                self.logger_provider.add_log_record_processor(
                    processor
                )  # type: ignore[attr-defined]
                self._log_processor_configured = True
            else:
                # Don't log here to avoid circular dependency during initialization
                pass

        except Exception:
            # Don't log here to avoid circular dependency during initialization
            pass

    def _create_log_exporter(self, config: Any) -> Optional[Any]:
        """Create a log exporter from configuration dictionary."""
        try:
            import importlib

            module_name = config.get("module")
            class_name = config.get("class")

            if not module_name or not class_name:
                return None

            # Dynamically import and instantiate the exporter
            module = importlib.import_module(module_name)
            exporter_class = getattr(module, class_name)

            # Build exporter arguments (same as span exporter)
            exporter_args = {}

            if "endpoint" in config:
                exporter_args["endpoint"] = config["endpoint"]
            if "headers" in config:
                exporter_args["headers"] = dict(config["headers"])
            if "timeout" in config:
                exporter_args["timeout"] = config["timeout"]
            if "compression" in config:
                compression_str = config["compression"].lower()
                try:
                    import grpc  # type: ignore[import-untyped]

                    if compression_str == "gzip":
                        exporter_args["compression"] = grpc.Compression.Gzip
                    elif compression_str == "deflate":
                        exporter_args["compression"] = grpc.Compression.Deflate
                    elif compression_str in ("none", "nocompression"):
                        exporter_args["compression"] = grpc.Compression.NoCompression
                except ImportError:
                    pass
            if "insecure" in config:
                exporter_args["insecure"] = config["insecure"]
            if "options" in config:
                options_list = config["options"]
                exporter_args["options"] = [tuple(option) for option in options_list]

            return exporter_class(**exporter_args)

        except Exception:
            # Don't log here to avoid circular dependency during initialization
            return None

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

            # Check if either server or requester tracing is enabled
            server_enabled = tracing_config.get("server_enabled", False)
            requester_enabled = tracing_config.get("requester_enabled", False)
            if not (server_enabled or requester_enabled):
                return

            # Get the configured span exporter
            span_exporter_name = tracing_config.get("span_exporter")
            if not span_exporter_name:
                return

            # Get the exporter configuration
            exporters_config = tracing_config.get("exporters", {})
            exporter_config = exporters_config.get(span_exporter_name)

            if not exporter_config:
                # Don't log here to avoid circular dependency during initialization
                return

            # Create the exporter
            exporter = self._create_span_exporter(exporter_config)
            if exporter:
                # Add processor to tracer provider
                processor = BatchSpanProcessor(exporter)
                self.tracer_provider.add_span_processor(processor)
                # Don't log here to avoid circular dependency during initialization
            else:
                # Don't log here to avoid circular dependency during initialization
                pass

        except Exception:
            # Don't log here to avoid circular dependency during initialization
            pass

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
                        # Don't log here to avoid circular dependency during initialization
                        pass
                except ImportError:
                    # Don't log here to avoid circular dependency during initialization
                    pass
            if "insecure" in config:
                exporter_args["insecure"] = config["insecure"]
            if "options" in config:
                # Convert list of [key, value] pairs to list of tuples
                options_list = config["options"]
                exporter_args["options"] = [tuple(option) for option in options_list]

            return exporter_class(**exporter_args)

        except Exception:
            # Don't log here to avoid circular dependency during initialization
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

    def force_flush(self, timeout_millis: int = 5000) -> bool:
        """Force flush pending telemetry without shutting down.

        This is useful for ensuring telemetry is exported before process exit
        or during signal handling, without disrupting ongoing telemetry collection.

        Args:
            timeout_millis: Maximum time to wait for flush (default 5 seconds)

        Returns:
            True if flush succeeded, False otherwise
        """
        if not self._initialized:
            return False

        try:
            if self.tracer_provider and hasattr(self.tracer_provider, "force_flush"):
                self.tracer_provider.force_flush(timeout_millis=timeout_millis)
            if self.logger_provider and hasattr(self.logger_provider, "force_flush"):
                self.logger_provider.force_flush(timeout_millis=timeout_millis)
            return True
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error flushing OTLP: {e}")
            return False

    def _atexit_shutdown(self) -> None:
        """Shutdown hook called automatically on normal process exit.

        This ensures that pending telemetry is flushed before the process terminates.
        Uses a short timeout since the process is already exiting.
        """
        if not self._initialized:
            return

        try:
            # Use shutdown() with explicit flush and timeout
            # The force_flush=True parameter ensures telemetry is exported before shutdown
            self.shutdown(timeout_millis=5000, force_flush=True)
        except Exception:
            # Silently ignore shutdown errors - don't block process exit
            pass

    def shutdown(self, timeout_millis: int = 5000, force_flush: bool = True) -> None:
        """Shutdown trace and log providers and clear global caches.

        This ensures that after shutdown (e.g., for fork-safety), subsequent
        reinitialization will create fresh providers rather than reusing stale ones.

        Args:
            timeout_millis: Maximum time to wait for flush operations (default 5 seconds)
            force_flush: Whether to flush pending telemetry before shutdown (default True)
        """
        if not self._initialized:
            return

        # First, flush any pending telemetry to ensure it's exported
        if force_flush:
            self.force_flush(timeout_millis=timeout_millis)

        # Now shutdown the providers
        try:
            if self.tracer_provider and hasattr(self.tracer_provider, "shutdown"):
                self.tracer_provider.shutdown()
            if self.logger_provider and hasattr(self.logger_provider, "shutdown"):
                self.logger_provider.shutdown()
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error during OTLP shutdown: {e}")
        finally:
            # Clear the global OpenTelemetry providers to prevent stale references
            # This is critical for fork-safety: after shutdown, reinitialization must
            # create fresh providers and exporters, not reuse shut-down ones
            try:
                from opentelemetry._logs import set_logger_provider
                from opentelemetry.sdk._logs import LoggerProvider as NoOpLoggerProvider

                # Reset to a no-op provider so get_logger_provider() doesn't return
                # the shut-down provider on next initialization
                set_logger_provider(NoOpLoggerProvider())
            except Exception:
                pass

            # Clear the module-level logger provider cache so handlers will fetch the new one
            # after reinitialization
            global _logger_provider
            with _logger_provider_lock:
                _logger_provider = None

            # Clear instance state and provider references
            self.tracer_provider = None
            self.logger_provider = None
            self._initialized = False
            self._log_processor_configured = False
            # Note: Don't clear _atexit_registered - atexit handlers can't be unregistered


def initialize_jobmon_otlp() -> JobmonOTLPManager:
    """Initialize OTLP for shared resources (traces and logs).

    This creates shared TracerProvider and LoggerProvider instances that
    should be used by all OTLP handlers to avoid duplicate connections.

    Returns:
        The OTLP manager instance with shared providers
    """
    manager = JobmonOTLPManager.get_instance()
    manager.initialize()
    return manager


def get_shared_logger_provider() -> Optional[Any]:
    """Get the shared logger provider, initializing if needed.

    This function provides a clean interface for handlers to access the
    shared LoggerProvider without dealing with manager instances directly.
    Uses double-checked locking to prevent race conditions in multi-threaded
    environments like Kubernetes with multiple workers.

    Returns:
        The shared LoggerProvider instance, or None if unavailable
    """
    global _logger_provider
    if _logger_provider is None:
        with _logger_provider_lock:
            # Double-check pattern: another thread might have initialized while we waited
            if _logger_provider is None:
                try:
                    manager = JobmonOTLPManager.get_instance()
                    manager.initialize()
                    _logger_provider = manager.logger_provider
                except Exception:
                    pass
    return _logger_provider


def get_logger(name: str) -> Optional[Any]:
    """Get a logger from the shared provider.

    This is the cleanest way for handlers to get OTLP loggers without
    dealing with manager instances or initialization complexity.

    Args:
        name: Logger name (typically __name__)

    Returns:
        OTLP logger instance, or None if unavailable
    """
    provider = get_shared_logger_provider()
    return provider.get_logger(name) if provider else None


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
