"""Custom OTLP logging handlers that prevent global log pollution."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from . import OTLP_AVAILABLE
from .formatters import JobmonOTLPFormatter


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
        self.setFormatter(JobmonOTLPFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to OTLP."""
        if not self._otlp_handler and self._exporter_config and OTLP_AVAILABLE:
            try:
                self._otlp_handler = self._create_handler()
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
        """Create OTLP handler by processing the exporter configuration."""
        try:
            from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            from .resources import create_jobmon_resources

            # Create isolated provider with jobmon resources
            resource_group = create_jobmon_resources()
            logger_provider = LoggerProvider(resource=resource_group)

            # Determine if we have a dict config or pre-configured exporter
            if isinstance(self._exporter_config, dict):
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

            # Create and return the handler
            handler = LoggingHandler(level=self.level, logger_provider=logger_provider)
            handler.setFormatter(self.formatter)
            return handler

        except Exception:
            return None

    def _create_exporter_from_dict(self, config: Dict) -> Optional[Any]:
        """Create an OTLP exporter from dictionary configuration."""
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
                exporter_args["headers"] = config["headers"]
            if "timeout" in config:
                exporter_args["timeout"] = config["timeout"]
            if "compression" in config:
                exporter_args["compression"] = config["compression"]
            if "options" in config:
                # Convert list of [key, value] pairs to list of tuples
                exporter_args["options"] = [
                    tuple(option) for option in config["options"]
                ]

            return exporter_class(**exporter_args)

        except Exception:
            return None

    def _create_processor_from_dict(self, exporter: Any, config: Dict) -> Any:
        """Create a batch processor with configuration from dict."""
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
    """OTLP logging handler with structlog formatting for structured logs.

    This handler extends JobmonOTLPLoggingHandler to provide structured logging
    using structlog formatting before sending to OTLP.
    """

    def __init__(
        self, level: int = logging.NOTSET, exporter: Optional[Union[Any, Dict]] = None
    ) -> None:
        """Initialize with structlog formatter."""
        super().__init__(level, exporter)

        # Set up structlog formatting if available
        try:
            import structlog

            self.setFormatter(
                structlog.stdlib.ProcessorFormatter(
                    processor=structlog.processors.JSONRenderer(), foreign_pre_chain=[]
                )
            )
        except ImportError:
            # Fall back to regular OTLP formatter if structlog not available
            pass
