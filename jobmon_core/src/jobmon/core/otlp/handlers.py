"""Custom OTLP logging handlers that prevent global log pollution."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

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

    # Map log level to OTLP severity (lazy import)
    _SEVERITY_MAP = None

    @classmethod
    def _get_severity_map(cls: type["JobmonOTLPLoggingHandler"]) -> Dict[str, Any]:
        """Get severity map with lazy import."""
        if cls._SEVERITY_MAP is None:
            if OTLP_AVAILABLE:
                from opentelemetry._logs.severity import SeverityNumber

                cls._SEVERITY_MAP = {
                    "DEBUG": SeverityNumber.DEBUG,
                    "INFO": SeverityNumber.INFO,
                    "WARNING": SeverityNumber.WARN,
                    "ERROR": SeverityNumber.ERROR,
                    "CRITICAL": SeverityNumber.FATAL,
                }
            else:
                cls._SEVERITY_MAP = {}
        return cls._SEVERITY_MAP

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

        self.setFormatter(JobmonOTLPFormatter())

        # If logger_provider is provided directly, initialize immediately
        if logger_provider:
            self._logger = logger_provider.get_logger(__name__)
            self._initialized = True

    def _ensure_initialized(self) -> bool:
        """Ensure logger provider is initialized. Returns True if ready."""
        if self._initialized:
            return True

        if not OTLP_AVAILABLE:
            return False

        try:
            from .manager import get_logger

            # Get logger from shared provider (handles initialization automatically)
            self._logger = get_logger(__name__)
            if self._logger:
                # Get the provider for resource access
                from .manager import get_shared_logger_provider

                self._logger_provider = get_shared_logger_provider()
                self._initialized = True

                return True

        except Exception:
            # Silently ignore initialization failures
            pass

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
            import hashlib
            import os

            from opentelemetry.sdk._logs import LogRecord as OTLPLogRecord
            from opentelemetry.trace import TraceFlags, get_current_span

            from jobmon.core.config.structlog_config import _thread_local

            # DEBUG: Count handlers attached to this logger
            logger = logging.getLogger(record.name)
            handler_count = len(logger.handlers)
            handler_classes = [h.__class__.__name__ for h in logger.handlers]
            
            # Only debug for workflow_run logger to avoid spam
            if "workflow_run" in record.name:
                print(f"[HANDLER_DEBUG] Logger: {record.name}, Handlers: {handler_count}, Classes: {handler_classes}, Handler ID: {id(self)}")
                print(f"[HANDLER_DEBUG] Record ID: {id(record)}, Message: {record.getMessage()[:50]}...")
                print(f"[HANDLER_DEBUG] Record created: {record.created}, Thread: {record.thread}")

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

            # Add deduplication metadata
            # Generate stable event_id: hash of (timestamp, message, trace_id, span_id, pid)
            event_key = (
                f"{record.created}:{message}:{trace_id_int}:{span_id_int}:{os.getpid()}"
            )
            event_id = hashlib.sha256(event_key.encode()).hexdigest()[:16]

            attributes["jobmon.event_id"] = event_id
            attributes["jobmon.process_id"] = os.getpid()
            attributes["jobmon.thread_id"] = record.thread
            attributes["jobmon.handler_class"] = self.__class__.__name__
            attributes["jobmon.handler_id"] = id(self)
            attributes["jobmon.handler_count"] = handler_count

            # Create OTLP log record
            severity_map = self._get_severity_map()
            severity_number = severity_map.get(
                record.levelname, severity_map.get("INFO", 0)
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
        except Exception:
            # Silently ignore OTLP emission failures to avoid circular logging
            pass


class JobmonOTLPStructlogHandler(JobmonOTLPLoggingHandler):
    """OTLP logging handler for structlog.

    Identical to JobmonOTLPLoggingHandler - uses the same custom handler that
    extracts attributes from thread-local event_dict. This class exists for
    clarity in configuration (to indicate structlog support) but functionally
    is the same as the parent class.
    """

    pass  # No need to override anything
