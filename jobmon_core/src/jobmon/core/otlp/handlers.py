"""Custom OTLP logging handlers that prevent global log pollution."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from jobmon.core.configuration import JobmonConfig

from . import OTLP_AVAILABLE
from .formatters import JobmonOTLPFormatter


class JobmonOTLPLoggingHandler(logging.Handler):
    """Universal OTLP logging handler with lazy initialization and attribute extraction.

    This handler extracts attributes from structlog's thread-local event_dict and
    supports flexible configuration patterns:

    1. Inline dict configuration (server pattern):
        handlers:
          otlp_logs:
            class: jobmon.core.otlp.JobmonOTLPLoggingHandler
            level: INFO
            exporter:
              module: opentelemetry.exporter.otlp.proto.grpc._log_exporter
              class: OTLPLogExporter
              endpoint: otelcol.dev.aks.scicomp.ihme.washington.edu:443
              options: [["grpc.max_send_message_length", 16777216]]
              max_export_batch_size: 8

    2. Pre-configured exporter instance:
        handler = JobmonOTLPLoggingHandler(exporter=my_exporter)

    3. Direct use with logger_provider (for testing):
        handler = JobmonOTLPLoggingHandler(logger_provider=my_provider)
    """

    # Map log level to OTLP severity
    from opentelemetry._logs.severity import SeverityNumber

    _SEVERITY_MAP = {
        "DEBUG": SeverityNumber.DEBUG,
        "INFO": SeverityNumber.INFO,
        "WARNING": SeverityNumber.WARN,
        "ERROR": SeverityNumber.ERROR,
        "CRITICAL": SeverityNumber.FATAL,
    }

    # Shared logger instance to prevent duplicate emissions
    _shared_logger: Optional[Any] = None
    _shared_logger_provider: Optional[Any] = None
    _class_initialized: bool = False  # Class-level initialization flag

    def __init__(
        self,
        level: int = logging.NOTSET,
        exporter: Optional[Union[Any, Dict]] = None,
        logger_provider: Optional[Any] = None,
    ) -> None:
        """Initialize with optional exporter config or pre-configured logger provider.

        Args:
            level: Logging level for this handler
            exporter: Either a dict configuration or pre-configured OTLP exporter instance
            logger_provider: Optional pre-configured logger provider (for testing/direct use)
        """
        super().__init__(level)
        self._exporter_config = exporter
        self._logger_provider = logger_provider
        self._logger: Optional[Any] = None
        self._initialized = False

        # Simple debug mode for troubleshooting
        config = JobmonConfig()
        self._debug_mode = config.get_boolean("telemetry", "debug")

        self.setFormatter(JobmonOTLPFormatter())

        # If logger_provider is provided directly, initialize immediately
        if logger_provider:
            self._logger = logger_provider.get_logger(__name__)
            self._initialized = True

    def _ensure_initialized(self) -> bool:
        """Ensure logger provider is initialized. Returns True if ready."""
        # Use class-level initialization to prevent multiple handlers from initializing
        if JobmonOTLPLoggingHandler._class_initialized:
            self._logger_provider = JobmonOTLPLoggingHandler._shared_logger_provider
            self._logger = JobmonOTLPLoggingHandler._shared_logger
            self._initialized = True
            return True

        if not OTLP_AVAILABLE:
            return False

        # Try to initialize from manager (only once per class)
        try:
            from .manager import JobmonOTLPManager

            manager = JobmonOTLPManager.get_instance()
            provider = manager.logger_provider

            if provider:
                # Set class-level shared resources
                JobmonOTLPLoggingHandler._shared_logger_provider = provider
                JobmonOTLPLoggingHandler._shared_logger = provider.get_logger(__name__)
                JobmonOTLPLoggingHandler._class_initialized = True

                # Set instance-level references
                self._logger_provider = provider
                self._logger = JobmonOTLPLoggingHandler._shared_logger
                self._initialized = True

                if self._debug_mode:
                    logging.getLogger("jobmon.otlp.debug").info(
                        "OTLP handler initialized successfully"
                    )
                return True

        except Exception as e:
            if self._debug_mode:
                logging.getLogger("jobmon.otlp.debug").exception(
                    f"OTLP handler initialization failed: {e}"
                )

        return False

    def _extract_attributes(self, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OTLP attributes from event_dict."""
        attributes = {}
        for key, value in event_dict.items():
            if key.startswith("_") or key in ("event", "timestamp"):
                continue
            if isinstance(value, (str, int, float, bool, type(None))):
                attributes[key] = value
            elif isinstance(value, (list, dict)):
                attributes[key] = str(value)
        return attributes

    def _parse_trace_context(
        self, event_dict: Optional[Dict[str, Any]]
    ) -> tuple[int, int]:
        """Parse trace and span IDs from event_dict or current span."""
        trace_id_int, span_id_int = 0, 0

        if not event_dict:
            return trace_id_int, span_id_int

        # Try parsing from structlog context
        try:
            if "trace_id" in event_dict:
                trace_id_int = int(event_dict["trace_id"], 16)
            if "span_id" in event_dict:
                span_id_int = int(event_dict["span_id"], 16)
        except (ValueError, TypeError):
            # Fallback to current span
            from opentelemetry.trace import get_current_span

            span = get_current_span()
            if span:
                ctx = span.get_span_context()
                if ctx and ctx.is_valid:
                    trace_id_int = ctx.trace_id
                    span_id_int = ctx.span_id

        return trace_id_int, span_id_int

    def emit(self, record: logging.LogRecord) -> None:
        """Emit log record to OTLP with extracted attributes."""
        # Lazy initialization on first emit
        if not self._ensure_initialized():
            return

        # Type guard: _ensure_initialized() guarantees these are not None
        assert self._logger_provider is not None
        assert self._logger is not None

        try:
            from opentelemetry.sdk._logs import LogRecord as OTLPLogRecord
            from opentelemetry.trace import TraceFlags, get_current_span

            from jobmon.core.config.structlog_config import _thread_local

            # Get event_dict from thread-local
            event_dict = getattr(_thread_local, "last_event_dict", None)

            # Extract message and attributes
            message = record.getMessage()
            attributes = {}
            if event_dict:
                attributes = self._extract_attributes(event_dict)
                message = event_dict.get("event", message)

            # Get trace context
            trace_id_int, span_id_int = self._parse_trace_context(event_dict)

            # Create OTLP log record
            severity_number = self._SEVERITY_MAP.get(
                record.levelname, self._SEVERITY_MAP["INFO"]
            )
            otlp_record = OTLPLogRecord(
                timestamp=int(record.created * 1e9),
                severity_text=record.levelname,
                severity_number=severity_number,
                body=message,
                resource=self._logger_provider.resource,
                attributes=attributes,
            )

            # Set trace context (defaults to 0 per OTLP spec)
            otlp_record.trace_id = trace_id_int
            otlp_record.span_id = span_id_int
            otlp_record.trace_flags = TraceFlags(0)

            # Update trace flags from current span if available
            if trace_id_int and span_id_int:
                span = get_current_span()
                if span:
                    ctx = span.get_span_context()
                    if ctx and ctx.is_valid:
                        otlp_record.trace_flags = ctx.trace_flags

            self._logger.emit(otlp_record)
        except Exception as e:
            if self._debug_mode:
                logging.getLogger("jobmon.otlp.debug").error(f"OTLP emit failed: {e}")


class JobmonOTLPStructlogHandler(JobmonOTLPLoggingHandler):
    """OTLP logging handler for structlog.

    Identical to JobmonOTLPLoggingHandler - uses the same custom handler that
    extracts attributes from thread-local event_dict. This class exists for
    clarity in configuration (to indicate structlog support) but functionally
    is the same as the parent class.
    """

    pass  # No need to override anything
