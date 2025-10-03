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
                        "logger",
                        "level",
                    ):
                        if isinstance(value, (str, int, float, bool, type(None))):
                            attributes[key] = value
                        elif isinstance(value, (list, dict)):
                            attributes[key] = str(value)

                # Use event field as clean message
                message = event_dict.get("event", message)

            # Get trace context
            span = get_current_span()
            trace_context = span.get_span_context() if span else None

            # Create OTLP log record (set trace context after creation to avoid deprecation)
            otlp_record = OTLPLogRecord(
                timestamp=int(record.created * 1e9),
                severity_text=record.levelname,
                severity_number=None,  # Type issue: Let SDK determine from severity_text
                body=record.getMessage(),
                resource=self._logger_provider.resource,
                attributes=attributes,
            )

            # Set trace context if available
            if trace_context and trace_context.is_valid:
                otlp_record.trace_id = trace_context.trace_id
                otlp_record.span_id = trace_context.span_id
                otlp_record.trace_flags = trace_context.trace_flags

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
        if not self._otlp_handler and self._exporter_config and OTLP_AVAILABLE:
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
        """Create OTLP handler by processing the exporter configuration."""
        try:
            from opentelemetry.sdk._logs import LoggerProvider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            from .resources import create_jobmon_resources

            # Create isolated provider with jobmon resources
            resource_group = create_jobmon_resources()
            logger_provider = LoggerProvider(resource=resource_group)

            # Determine if we have a dict config or pre-configured exporter
            # Handle both dict and ConvertingDict (from logging config)
            if hasattr(self._exporter_config, "get") and hasattr(
                self._exporter_config, "keys"
            ):
                # Handle inline dict configuration (server pattern)
                exporter = self._create_exporter_from_dict(self._exporter_config)
                processor = self._create_processor_from_dict(
                    exporter, self._exporter_config
                )
            else:
                # Handle pre-configured exporter instance
                exporter = self._exporter_config
                if not exporter:
                    return None
                processor = BatchLogRecordProcessor(exporter)

            if not exporter:
                return None

            logger_provider.add_log_record_processor(processor)

            # Use CUSTOM OTLP handler that properly extracts attributes
            # Standard LoggingHandler doesn't extract custom record.__dict__ fields
            handler = _JobmonOTLPLoggingHandler(
                level=self.level, logger_provider=logger_provider
            )
            handler.setFormatter(self.formatter)
            return handler

        except Exception:
            return None

    def _create_exporter_from_dict(self, config: Any) -> Optional[Any]:
        """Create an OTLP exporter from dictionary configuration (handles ConvertingDict)."""
        try:
            # Extract exporter configuration
            module_name = config.get("module")
            class_name = config.get("class")

            if not module_name or not class_name:
                return None

            # Dynamically import and instantiate the exporter
            import importlib

            module = importlib.import_module(module_name)
            exporter_class = getattr(module, class_name)

            # Build exporter arguments
            exporter_args = {}

            # Common exporter arguments
            if "endpoint" in config:
                exporter_args["endpoint"] = config["endpoint"]
            if "headers" in config:
                exporter_args["headers"] = dict(
                    config["headers"]
                )  # Convert ConvertingDict to dict
            if "timeout" in config:
                exporter_args["timeout"] = config["timeout"]
            if "compression" in config:
                exporter_args["compression"] = config["compression"]
            if "insecure" in config:
                exporter_args["insecure"] = config["insecure"]
            if "options" in config:
                # Convert list of [key, value] pairs to list of tuples
                options_list = config["options"]
                exporter_args["options"] = [tuple(option) for option in options_list]

            return exporter_class(**exporter_args)

        except Exception:
            return None

    def _create_processor_from_dict(self, exporter: Any, config: Any) -> Any:
        """Create a batch processor with configuration from dict (handles ConvertingDict)."""
        try:
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            # Extract batch processor configuration
            processor_args = {}

            if "max_export_batch_size" in config:
                processor_args["max_export_batch_size"] = config[
                    "max_export_batch_size"
                ]
            if "export_timeout_millis" in config:
                processor_args["export_timeout_millis"] = config[
                    "export_timeout_millis"
                ]
            if "schedule_delay_millis" in config:
                processor_args["schedule_delay_millis"] = config[
                    "schedule_delay_millis"
                ]
            if "max_queue_size" in config:
                processor_args["max_queue_size"] = config["max_queue_size"]

            return BatchLogRecordProcessor(exporter, **processor_args)

        except Exception:
            # Fallback to basic processor
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            return BatchLogRecordProcessor(exporter)


class JobmonOTLPStructlogHandler(JobmonOTLPLoggingHandler):
    """OTLP logging handler for structlog.

    Identical to JobmonOTLPLoggingHandler - uses the same custom handler that
    extracts attributes from thread-local event_dict. This class exists for
    clarity in configuration (to indicate structlog support) but functionally
    is the same as the parent class.
    """

    pass  # No need to override anything
