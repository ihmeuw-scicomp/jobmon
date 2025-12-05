"""Core OTLP manager for jobmon-scoped telemetry."""

from __future__ import annotations

import atexit
import importlib
import logging
import os
import signal
import threading
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Optional, Tuple, Type

from ._compat import OTLP_AVAILABLE

if OTLP_AVAILABLE:

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk._logs import LoggerProvider

from .resources import create_jobmon_resources

# Module-level singleton for shared logger provider with thread safety
_logger_provider: Optional[Any] = None
_logger_provider_lock = threading.Lock()

_shutdown_lock = threading.Lock()
_shutdown_invoked = False
_signal_handlers_installed = False


def _build_exporter_args(config: Dict[str, Any], module_name: str) -> Dict[str, Any]:
    exporter_args: Dict[str, Any] = {}

    if "endpoint" in config:
        exporter_args["endpoint"] = config["endpoint"]

    if "headers" in config:
        exporter_args["headers"] = dict(config["headers"])

    if "timeout" in config:
        exporter_args["timeout"] = config["timeout"]

    if "insecure" in config:
        exporter_args["insecure"] = config["insecure"]

    compression = config.get("compression")
    if compression is not None:
        compression_str = str(compression).lower()
        is_grpc = ".grpc." in module_name

        if is_grpc:
            try:
                import grpc  # type: ignore[import-untyped]

                mapping = {
                    "gzip": grpc.Compression.Gzip,
                    "deflate": grpc.Compression.Deflate,
                    "none": grpc.Compression.NoCompression,
                    "nocompression": grpc.Compression.NoCompression,
                }
                if compression_str in mapping:
                    exporter_args["compression"] = mapping[compression_str]
            except ImportError:
                pass
        elif compression_str not in {"none", "nocompression"}:
            exporter_args["compression"] = compression_str

    if "options" in config:
        options_list = config["options"]
        exporter_args["options"] = [tuple(option) for option in options_list]

    return exporter_args


def _normalize_exporter_config(
    config: Any, defaults: Optional[Dict[str, Any]] = None
) -> Tuple[Optional[Type[Any]], Dict[str, Any]]:
    if not isinstance(config, dict):
        return None, {}

    module_name = config.get("module")
    class_name = config.get("class")

    if not module_name or not class_name:
        return None, {}

    try:
        module = importlib.import_module(module_name)
        exporter_class = getattr(module, class_name)
    except Exception:
        return None, {}

    exporter_args = dict(defaults or {})
    exporter_args.update(_build_exporter_args(config, module_name))
    return exporter_class, exporter_args


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
            from opentelemetry._logs import get_logger_provider, set_logger_provider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            # Explicit check: ensure we have a proper LoggerProvider and no existing processors
            global_lp = get_logger_provider()
            if not isinstance(global_lp, LoggerProvider):
                # Set our provider as the global one
                set_logger_provider(self.logger_provider)
                global_lp = self.logger_provider

            # Check if BatchLogRecordProcessor already exists
            # Processors are stored in _multi_log_record_processor._log_record_processors
            multi_processor = getattr(global_lp, "_multi_log_record_processor", None)
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

                global_lp.add_log_record_processor(processor)  # type: ignore[attr-defined]
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
            exporter_class, exporter_args = _normalize_exporter_config(config)
            if exporter_class is None:
                return None
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
            exporter_class, exporter_args = _normalize_exporter_config(config)
            if exporter_class is None:
                return None
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

    def shutdown(self) -> None:
        """Shutdown trace and log providers."""
        if not self._initialized:
            return

        try:
            if self.tracer_provider and hasattr(self.tracer_provider, "shutdown"):
                self.tracer_provider.shutdown()
            if self.logger_provider and hasattr(self.logger_provider, "shutdown"):
                self.logger_provider.shutdown()
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error during OTLP shutdown: {e}")
        finally:
            self._initialized = False

    def flush_and_shutdown(self) -> None:
        """Flush pending OTLP telemetry and shut down providers."""
        if not OTLP_AVAILABLE:
            return

        self._force_flush_logger_provider()
        self._force_flush_tracer_provider()

        self.shutdown()

    def _force_flush_logger_provider(self) -> None:
        provider = self.logger_provider
        if not provider:
            return

        flush = getattr(provider, "force_flush", None)
        if callable(flush):
            try:
                flush()
            except Exception:
                pass

    def _force_flush_tracer_provider(self) -> None:
        provider = self.tracer_provider
        if not provider:
            return

        flush = getattr(provider, "force_flush", None)
        if callable(flush):
            try:
                flush()
            except Exception:
                pass


# Configuration validation utilities


def _flush_otlp_once(reason: str = "atexit") -> None:
    """Flush OTLP telemetry a single time per process."""
    if not OTLP_AVAILABLE:
        return

    global _shutdown_invoked
    with _shutdown_lock:
        if _shutdown_invoked:
            return
        _shutdown_invoked = True

    try:
        manager = JobmonOTLPManager.get_instance()
        manager.flush_and_shutdown()
    except Exception:
        # Best-effort shutdown; suppress failures during interpreter teardown
        pass


def _register_atexit_hook() -> None:
    """Register the OTLP flush hook for interpreter shutdown."""
    if not OTLP_AVAILABLE:
        return

    atexit.register(_flush_otlp_once)


def _register_signal_handlers() -> None:
    """Install signal handlers that flush OTLP before termination."""
    if not OTLP_AVAILABLE:
        return

    global _signal_handlers_installed
    if _signal_handlers_installed:
        return

    def _make_handler(previous_handler: Any, signum: int) -> Callable[[int, Any], None]:
        def _handler(signum_inner: int, frame: Any) -> None:
            _flush_otlp_once(f"signal:{signum_inner}")

            if callable(previous_handler):
                try:
                    previous_handler(signum_inner, frame)
                except Exception:
                    pass
            elif previous_handler == signal.SIG_DFL:
                try:
                    signal.signal(signum_inner, signal.SIG_DFL)
                    if hasattr(signal, "raise_signal"):
                        signal.raise_signal(signum_inner)
                    else:
                        os.kill(os.getpid(), signum_inner)
                except Exception:
                    pass
            # Ignore SIG_IGN (do nothing)

        return _handler

    for signum in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)):
        if signum is None:
            continue

        try:
            previous = signal.getsignal(signum)
            handler = _make_handler(previous, signum)
            signal.signal(signum, handler)
        except Exception:
            continue

    _signal_handlers_installed = True


def _install_lifecycle_hooks() -> None:
    """Ensure lifecycle hooks are installed exactly once."""
    _register_atexit_hook()
    _register_signal_handlers()


_install_lifecycle_hooks()


@contextmanager
def otlp_flush_on_exit() -> Generator[Optional[JobmonOTLPManager], None, None]:
    """Context manager that guarantees OTLP flush when exiting."""
    if not OTLP_AVAILABLE:
        yield None
        return

    manager = JobmonOTLPManager.get_instance()
    try:
        yield manager
    finally:
        manager.flush_and_shutdown()


def register_otlp_shutdown_event(app: Any) -> None:
    """Register a FastAPI shutdown hook that flushes OTLP telemetry."""
    if not OTLP_AVAILABLE:
        return

    add_event_handler = getattr(app, "add_event_handler", None)
    if callable(add_event_handler):

        async def _flush_otlp_on_shutdown() -> None:  # pragma: no cover - integration
            _flush_otlp_once("fastapi-shutdown")

        add_event_handler("shutdown", _flush_otlp_on_shutdown)
        return

    on_event = getattr(app, "on_event", None)
    if callable(on_event):

        @on_event("shutdown")
        async def _flush_otlp_on_shutdown() -> (
            None
        ):  # pragma: no cover - exercised in integration
            _flush_otlp_once("fastapi-shutdown")


def validate_otlp_exporter_config(config: Any, exporter_type: str = "log") -> list[str]:
    """Validate OTLP exporter configuration and return list of issues.

    Args:
        config: Exporter configuration dictionary
        exporter_type: Type of exporter ('log', 'trace', 'metric')

    Returns:
        List of validation error messages. Empty list if valid.
    """
    issues = []

    # Check required fields
    if not config.get("module"):
        issues.append("Missing required field: module")
    if not config.get("class"):
        issues.append("Missing required field: class")

    # Define supported parameters by exporter type
    SUPPORTED_PARAMS = {
        "log": {
            "endpoint",
            "headers",
            "timeout",
            "compression",
            "insecure",
            "max_export_batch_size",
            "export_timeout_millis",
            "schedule_delay_millis",
            "max_queue_size",
        },
        "trace": {
            "endpoint",
            "headers",
            "timeout",
            "compression",
            "insecure",
            "options",
            "max_export_batch_size",
            "export_timeout_millis",
            "schedule_delay_millis",
            "max_queue_size",
        },
        "metric": {
            "endpoint",
            "headers",
            "timeout",
            "compression",
            "insecure",
            "options",
            "aggregation_temporality",
            "max_export_batch_size",
            "export_timeout_millis",
            "schedule_delay_millis",
            "max_queue_size",
        },
    }

    supported = SUPPORTED_PARAMS.get(exporter_type, SUPPORTED_PARAMS["log"])

    # Check for unsupported parameters
    config_keys = set(config.keys()) - {"module", "class"}  # Exclude metadata fields
    unsupported = config_keys - supported

    if unsupported:
        issues.append(
            f"Unsupported parameters for {exporter_type} exporter: {sorted(unsupported)}"
        )

    # Specific validation for known problematic parameters
    if "options" in config and exporter_type == "log":
        issues.append(
            "'options' parameter is not supported by OTLPLogExporter. Remove this parameter."
        )

    # Validate endpoint format
    endpoint = config.get("endpoint")
    if endpoint:
        if not isinstance(endpoint, str):
            issues.append("'endpoint' must be a string")
        elif not (endpoint.startswith("http://") or endpoint.startswith("https://")):
            issues.append("'endpoint' must start with http:// or https://")

    # Validate timeout
    timeout = config.get("timeout")
    if timeout is not None:
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            issues.append("'timeout' must be a positive number")

    # Validate batch size parameters
    for param in ["max_export_batch_size", "max_queue_size"]:
        value = config.get(param)
        if value is not None:
            if not isinstance(value, int) or value <= 0:
                issues.append(f"'{param}' must be a positive integer")

    # Validate timing parameters
    for param in ["export_timeout_millis", "schedule_delay_millis"]:
        value = config.get(param)
        if value is not None:
            if not isinstance(value, int) or value < 0:
                issues.append(f"'{param}' must be a non-negative integer")

    return issues


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
                manager = JobmonOTLPManager.get_instance()
                manager.initialize()
                _logger_provider = manager.logger_provider
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

    Example::

        exporter = create_log_exporter(
            endpoint="otelcol.dev.aks:443",
            max_batch_size=8
        )
        handler = JobmonOTLPLoggingHandler(exporter=exporter)
    """
    if not OTLP_AVAILABLE:
        return None

    try:
        config: Dict[str, Any] = {
            "module": "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
            "class": "OTLPLogExporter",
        }
        config.update(kwargs)

        exporter_class, exporter_args = _normalize_exporter_config(
            config, defaults={"insecure": True}
        )
        if exporter_class is None:
            return None

        return exporter_class(**exporter_args)

    except Exception:
        return None
