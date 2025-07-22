"""Server-specific OTLP logging handlers for structured logging."""

from __future__ import annotations

import logging
from typing import Any, Optional

try:
    import structlog

    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False


class ServerOTLPStructlogHandler(logging.Handler):
    """Minimal server OTLP handler with structlog formatting.

    This handler combines structlog formatting with OTLP export,
    accepting a pre-configured exporter instance for simplicity.

    Usage in logconfig:
        handlers:
          otlp_structlog:
            class: jobmon.server.web.otlp.ServerOTLPStructlogHandler
            level: INFO
            exporter: !otlp_exporter_factory  # Pre-configured exporter instance
    """

    def __init__(
        self, level: int = logging.NOTSET, exporter: Optional[Any] = None
    ) -> None:
        """Initialize with a pre-configured exporter instance.

        Args:
            level: Logging level for this handler
            exporter: Pre-configured OTLP exporter instance
        """
        super().__init__(level)
        self._exporter = exporter
        self._otlp_handler: Optional[logging.Handler] = None

        # Set up structlog formatting if available
        if STRUCTLOG_AVAILABLE:
            try:
                self.setFormatter(
                    structlog.stdlib.ProcessorFormatter(
                        processor=structlog.processors.JSONRenderer(),
                        foreign_pre_chain=[],
                    )
                )
            except Exception:
                self._set_fallback_formatter()
        else:
            self._set_fallback_formatter()

    def _set_fallback_formatter(self) -> None:
        """Set fallback formatter when structlog is not available."""
        try:
            from jobmon.core.otlp.formatters import JobmonOTLPFormatter

            self.setFormatter(JobmonOTLPFormatter())
        except ImportError:
            self.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a structured log record to OTLP."""
        if not self._otlp_handler and self._exporter and OTLP_AVAILABLE:
            try:
                self._otlp_handler = self._create_handler()
                if self._otlp_handler:
                    self._otlp_handler.setFormatter(self.formatter)
            except Exception:
                # Fail silently if handler creation fails to avoid breaking application logging
                pass

        if self._otlp_handler:
            try:
                self._otlp_handler.emit(record)
            except Exception:
                # Fail silently to avoid breaking application logging
                pass

    def _create_handler(self) -> Optional[logging.Handler]:
        """Create OTLP handler with the provided exporter."""
        try:
            from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            from jobmon.core.otlp.resources import create_jobmon_resources

            # Create isolated provider with jobmon resources
            resource_group = create_jobmon_resources()
            logger_provider = LoggerProvider(resource=resource_group)

            # Use the provided exporter with a simple processor
            if self._exporter is None:
                return None
            processor = BatchLogRecordProcessor(self._exporter)
            logger_provider.add_log_record_processor(processor)

            # Create and return the handler
            handler = LoggingHandler(level=self.level, logger_provider=logger_provider)
            return handler

        except Exception:
            return None
