"""Custom OTLP logging handlers that prevent global log pollution."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from jobmon.core.configuration import JobmonConfig

from . import OTLP_AVAILABLE
from .formatters import JobmonOTLPFormatter


class _JobmonOTLPLoggingHandler(logging.Handler):
    """Custom OTLP LoggingHandler that extracts attributes from thread-local event_dict.

    Retrieves the raw structlog event_dict from thread-local storage (stored before
    JSON rendering) to extract clean attributes for OTLP.
    """

    def __init__(self, level: int, logger_provider: Any) -> None:
        """Initialize with logger provider."""
        super().__init__(level)
        self._logger_provider = logger_provider
        self._logger = logger_provider.get_logger(__name__)

    def emit(self, record: logging.LogRecord) -> None:
        """Emit log record to OTLP with extracted attributes."""
        try:
            from opentelemetry.sdk._logs import LogRecord as OTLPLogRecord
            from opentelemetry.trace import get_current_span

            # Extract attributes from thread-local event_dict (before formatting)
            from jobmon.core.config.structlog_config import _thread_local

            attributes = {}
            message = record.getMessage()

            # Try to get event_dict from thread-local
            event_dict = getattr(_thread_local, "last_event_dict", None)

            if event_dict:
                # Extract all fields as OTLP attributes
                for key, value in event_dict.items():
                    if not key.startswith("_") and key not in (
                        "event",
                        "timestamp",
                    ):
                        if isinstance(value, (str, int, float, bool, type(None))):
                            attributes[key] = value
                        elif isinstance(value, (list, dict)):
                            attributes[key] = str(value)

                # Use event field as clean message
                message = event_dict.get("event", message)

            # Use trace IDs from structlog context (stable) instead of current span (unstable)
            # This ensures consistent trace correlation
            trace_id_int = 0
            span_id_int = 0

            if event_dict:
                # Parse hex trace_id/span_id from structlog context to integers
                try:
                    if "trace_id" in event_dict:
                        trace_id_int = int(event_dict["trace_id"], 16)
                    if "span_id" in event_dict:
                        span_id_int = int(event_dict["span_id"], 16)
                except (ValueError, TypeError):
                    # If parsing fails, try current span as fallback
                    span = get_current_span()
                    if span:
                        ctx = span.get_span_context()
                        if ctx and ctx.is_valid:
                            trace_id_int = ctx.trace_id
                            span_id_int = ctx.span_id

            # Create OTLP log record with stable trace IDs
            otlp_record = OTLPLogRecord(
                timestamp=int(record.created * 1e9),
                severity_text=record.levelname,
                severity_number=None,
                body=message,
                resource=self._logger_provider.resource,
                attributes=attributes,
            )

            # Set trace context from structlog (stable across duplicates)
            if trace_id_int:
                otlp_record.trace_id = trace_id_int
            if span_id_int:
                otlp_record.span_id = span_id_int

            self._logger.emit(otlp_record)
        except Exception:
            pass


class JobmonOTLPLoggingHandler(logging.Handler):
    """Universal OTLP logging handler supporting dict configs and pre-configured exporters.

    This handler follows the principle of single responsibility while being flexible enough
    to work with different configuration patterns:

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
    """

    def __init__(
        self, level: int = logging.NOTSET, exporter: Optional[Union[Any, Dict]] = None
    ) -> None:
        """Initialize with either a dict configuration or pre-configured exporter instance.

        Args:
            level: Logging level for this handler
            exporter: Either a dict configuration or pre-configured OTLP exporter instance
        """
        super().__init__(level)
        self._exporter_config = exporter
        self._otlp_handler: Optional[logging.Handler] = None

        # Simple debug mode for troubleshooting
        config = JobmonConfig()
        self._debug_mode = config.get_boolean("telemetry", "debug")

        self.setFormatter(JobmonOTLPFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to OTLP."""
        # Create handler on first use
        if (
            not self._otlp_handler
            and self._exporter_config is not None
            and OTLP_AVAILABLE
        ):
            try:
                self._otlp_handler = self._create_handler()
                if self._debug_mode and self._otlp_handler:
                    logging.getLogger("jobmon.otlp.debug").info(
                        "OTLP handler initialized successfully"
                    )
            except Exception as e:
                if self._debug_mode:
                    logging.getLogger("jobmon.otlp.debug").error(
                        f"OTLP handler initialization failed: {e}", exc_info=True
                    )

        # Emit to OTLP if handler is available
        if self._otlp_handler:
            try:
                # Call emit directly (no filters needed with thread-local approach)
                self._otlp_handler.emit(record)
            except Exception as e:
                if self._debug_mode:
                    logging.getLogger("jobmon.otlp.debug").error(
                        f"OTLP emit failed: {e}"
                    )

    def _create_handler(self) -> Optional[logging.Handler]:
        """Create OTLP handler using the shared logger provider.

        The shared logger provider is configured once in JobmonOTLPManager with
        a single processor/exporter to avoid duplicate log emissions.
        """
        try:
            from .manager import JobmonOTLPManager

            # Get shared logger provider from manager
            # The manager has already configured the processor/exporter
            manager = JobmonOTLPManager.get_instance()

            logger_provider = manager.logger_provider
            if not logger_provider:
                # If not initialized yet, we can't create handler
                # This is expected during early logging config
                return None

            # Use CUSTOM OTLP handler that properly extracts attributes
            # Standard LoggingHandler doesn't extract custom record.__dict__ fields
            handler = _JobmonOTLPLoggingHandler(
                level=self.level, logger_provider=logger_provider
            )
            handler.setFormatter(self.formatter)
            return handler

        except Exception:
            return None


class JobmonOTLPStructlogHandler(JobmonOTLPLoggingHandler):
    """OTLP logging handler for structlog.

    Identical to JobmonOTLPLoggingHandler - uses the same custom handler that
    extracts attributes from thread-local event_dict. This class exists for
    clarity in configuration (to indicate structlog support) but functionally
    is the same as the parent class.
    """

    pass  # No need to override anything
