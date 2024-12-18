from __future__ import annotations

import getpass
import logging
import os
import socket
import sys
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Type, Union


from opentelemetry import _logs, trace
from opentelemetry.sdk import resources
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer

from jobmon.core import __version__
from jobmon.core.configuration import JobmonConfig


class OtlpAPI:
    """OpenTelemetry API."""

    _instance = None
    _initialized = False
    _sqlalchemy_instrumented = False
    _requests_instrumented = False

    def __new__(cls: Type[OtlpAPI], *args: Any, **kwargs: Any) -> OtlpAPI:
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self, extra_detectors: Optional[List[resources.ResourceDetector]] = None
    ) -> None:
        """Initialize the OtlpAPI object."""
        if OtlpAPI._initialized:
            return

        if extra_detectors is None:
            extra_detectors = []

        self._configure_resources(extra_detectors)
        self._configure_providers()

        OtlpAPI._initialized = True

    def _configure_resources(
        self, extra_detectors: List[resources.ResourceDetector]
    ) -> None:
        self._detectors = [
            _ProcessResourceDetector(),
            _HostResourceDetector(),
            _ServiceResourceDetector(),
        ]

        if extra_detectors:
            self._detectors.extend(extra_detectors)

        resource_group = resources.get_aggregated_resources(self._detectors)

        # Store the SDK TracerProvider instance
        self.tracer_provider = TracerProvider(resource=resource_group)
        trace.set_tracer_provider(self.tracer_provider)

        # Store the SDK LoggerProvider instance
        self.logger_provider = LoggerProvider(resource=resource_group)
        _logs.set_logger_provider(self.logger_provider)

    def _configure_providers(self) -> None:
        config = JobmonConfig()

        span_exporter = config.get("otlp", "span_exporter")
        if span_exporter:
            span_kwargs = config.get_section(span_exporter)
            self._set_exporter(
                span_kwargs,
                self.tracer_provider.add_span_processor,  # Use the SDK instance
                BatchSpanProcessor,
            )

        log_exporter = config.get("otlp", "log_exporter")
        if log_exporter:
            log_kwargs = config.get_section(log_exporter)
            self._set_exporter(
                log_kwargs,
                self.logger_provider.add_log_record_processor,  # Use the SDK instance
                BatchLogRecordProcessor,
            )

    def _set_exporter(
        self,
        kwargs: Any,
        add_processor_func: Callable[[Any], None],
        batch_processor_class: Type[Any],
    ) -> None:
        module_name = kwargs["module"]
        class_name = kwargs["class"]
        module = __import__(module_name, fromlist=[class_name])
        ExporterClass = getattr(module, class_name)
        processor = batch_processor_class(
            ExporterClass(
                **{k: v for k, v in kwargs.items() if k not in ["module", "class"]}
            )
        )
        add_processor_func(processor)

    @classmethod
    def instrument_sqlalchemy(cls: Type[OtlpAPI]) -> None:
        """Instrument SQLAlchemy."""
        if not cls._sqlalchemy_instrumented:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            SQLAlchemyInstrumentor().instrument()
            cls._sqlalchemy_instrumented = True

    @classmethod
    def instrument_app(cls: Type[OtlpAPI], app: Any) -> None:
        """Instrument FastAPI app."""
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        # Instrument FastAPI with OpenTelemetry
        FastAPIInstrumentor().instrument_app(app)

    @classmethod
    def instrument_requests(cls: Type[OtlpAPI]) -> None:
        """Instrument requests."""
        if not cls._requests_instrumented:
            from opentelemetry.instrumentation.requests import RequestsInstrumentor

            RequestsInstrumentor().instrument()
            cls._requests_instrumented = True

    def get_tracer(self, name: str) -> Tracer:
        """Get a tracer from the SDK TracerProvider."""
        return self.tracer_provider.get_tracer(name)

    def get_logger_provider(self) -> LoggerProvider:
        """Get the logger provider."""
        return self.logger_provider

    @staticmethod
    def get_span_details() -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Retrieve details of the current span."""
        span = trace.get_current_span()

        # Check if there's a valid span
        if not span or not span.is_recording():
            return None, None, None

        ctx = span.get_span_context()

        # Get parent span, but handle if it doesn't exist
        parent = getattr(span, "parent", None)

        span_id = format(ctx.span_id, "016x") if ctx and ctx.span_id else None
        trace_id = format(ctx.trace_id, "032x") if ctx and ctx.trace_id else None
        parent_span_id = format(parent.span_id, "016x") if parent else None

        return span_id, trace_id, parent_span_id


class OpenTelemetryLogFormatter(logging.Formatter):
    """Formatter that adds OpenTelemetry spans to log records."""

    def format(self, record: logging.LogRecord) -> str:
        span_id, trace_id, parent_span_id = OtlpAPI.get_span_details()
        record.span_id = span_id
        record.trace_id = trace_id
        record.parent_span_id = parent_span_id
        return super().format(record)


def add_span_details_processor(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Structlog processor to add OpenTelemetry span details to log entries.

    Args:
        logger: The logger instance (not used, but required by Structlog processor signature).
        method_name: The logging method name (e.g., "info", "debug").
        event_dict: The event dictionary representing the log entry.

    Returns:
        The modified event dictionary with OpenTelemetry span details added.
    """
    span_id, trace_id, parent_span_id = OtlpAPI.get_span_details()
    if trace_id or span_id or parent_span_id:
        event_dict.update(
            {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
            }
        )
    return event_dict


class _ProcessResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        attrs: Mapping[str, Union[str, bool, int, float]] = {
            str(resources.PROCESS_PID): int(os.getpid()),  # Explicit cast to int
            str(resources.PROCESS_RUNTIME_NAME): str(
                sys.implementation.name
            ),  # Explicit cast to str
            str(resources.PROCESS_OWNER): str(
                getpass.getuser()
            ),  # Explicit cast to str
        }
        return resources.Resource(attrs)


class _ServiceResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        attrs = {
            resources.SERVICE_NAME: "jobmon",
            resources.SERVICE_VERSION: __version__,
        }
        return resources.Resource(attrs)


class _HostResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        attrs = {resources.HOST_NAME: socket.gethostname()}
        return resources.Resource(attrs)
