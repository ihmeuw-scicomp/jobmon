from __future__ import annotations

import getpass
import logging
import logging.config
import os
import socket
import sys
from typing import Any, Callable, List, Optional, Tuple, Type

from flask import Flask
from opentelemetry import _logs
from opentelemetry import trace
from opentelemetry.sdk import resources
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer


from jobmon.core import __version__
from jobmon.core.configuration import JobmonConfig


def get_resource(raise_on_error: bool) -> resources.Resource:
    """Gather data on the currently running process to define an opentelemetry resource.

    Args:
        raise_on_error: if True, will raise if an exception is encountered

    Returns:
        opentelemetry.sdk.resources.Resource
    """
    detectors = [
        _ServiceResourceDetector(raise_on_error=raise_on_error),
        _ProcessResourceDetector(raise_on_error=raise_on_error),
        _HostResourceDetector(raise_on_error=raise_on_error),
    ]
    detected_resources = resources.get_aggregated_resources(detectors)
    return detected_resources


class _ProcessResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        """Returns a Resource related to the process."""
        attrs = {
            resources.PROCESS_PID: os.getpid(),
            resources.PROCESS_RUNTIME_NAME: sys.implementation.name,
            resources.PROCESS_OWNER: getpass.getuser(),
        }
        return resources.Resource(attrs)


class _ServiceResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        """Returns a Resource related to the instrumentation library itself."""
        attrs = {
            resources.SERVICE_NAME: "jobmon",
            resources.SERVICE_VERSION: __version__,
        }
        return resources.Resource(attrs)


class _HostResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        """Returns a Resource related to the host machine."""
        attrs = {resources.HOST_NAME: socket.gethostname()}
        return resources.Resource(attrs)


class _ClusterResourceDetector(resources.ResourceDetector):
    def detect(self) -> resources.Resource:
        """Returns a Resource related to the cluster job (if applicable)."""
        attrs = {}
        env_variables = ["SLURM_JOBID", "SLURM_JOB_ACCOUNT", "SLURM_JOB_NAME", "JOB_ID"]
        for variable_str in env_variables:
            variable_val = os.getenv(variable_str)
            if variable_val:
                attrs[variable_str.lower()] = variable_val
        return resources.Resource(attrs)


class OtlpAPI:
    """OpenTelemetry API."""

    _instance = None
    _initialized = False
    _sqlalchemy_instrumented = False
    _requests_instrumented = False
    _log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "root": {"level": "INFO", "handlers": []},
        "formatters": {
            "otel_jobmon": {
                "class": "jobmon.core.otlp.OpenTelemetryLogFormatter",
                "format": "%(asctime)s [%(levelname)s] [trace_id=%(trace_id)s,"
                " span_id=%(span_id)s, parent_span_id=%(parent_span_id)s]"
                " - %(message)s",
            }
        },
        "handlers": {
            "otel_jobmon": {
                "class": "opentelemetry.sdk._logs.LoggingHandler",
                "formatter": "otel_jobmon",
            },
        },
    }

    def __new__(cls: Type[OtlpAPI], *args: Any, **kwargs: Any) -> OtlpAPI:
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, extra_detectors: List[resources.ResourceDetector] = []) -> None:
        """Initialize the OtlpAPI object."""
        if OtlpAPI._initialized:
            return

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
        trace.set_tracer_provider(TracerProvider(resource=resource_group))
        _logs.set_logger_provider(LoggerProvider(resource=resource_group))

    def _configure_providers(self) -> None:
        config = JobmonConfig()

        span_exporter = config.get("otlp", "span_exporter")
        if span_exporter:
            span_kwargs = config.get_section(span_exporter)
            self._set_exporter(
                span_kwargs,
                trace.get_tracer_provider().add_span_processor,
                BatchSpanProcessor,
            )

        log_exporter = config.get("otlp", "log_exporter")
        if log_exporter:
            log_kwargs = config.get_section(log_exporter)
            self._set_exporter(
                log_kwargs,
                _logs.get_logger_provider().add_log_record_processor,
                BatchLogRecordProcessor,
            )

    def _set_exporter(
        self, kwargs: Any, add_processor_func: Callable, batch_processor: Any
    ) -> None:
        module_name = kwargs["module"]
        class_name = kwargs["class"]
        module = __import__(module_name, fromlist=[class_name])
        ExporterClass = getattr(module, class_name)
        processor = batch_processor(
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
    def instrument_app(cls: Type[OtlpAPI], app: Flask) -> None:
        """Instrument Flask app."""
        from opentelemetry.instrumentation.flask import FlaskInstrumentor

        FlaskInstrumentor().instrument_app(app)

    @classmethod
    def instrument_requests(cls: Type[OtlpAPI]) -> None:
        """Instrument requests."""
        if not cls._requests_instrumented:
            from opentelemetry.instrumentation.requests import RequestsInstrumentor

            RequestsInstrumentor().instrument()
            cls._requests_instrumented = True

    def get_tracer(self, name: str) -> Tracer:
        """Get a tracer."""
        return trace.get_tracer(name)

    def get_logger_provider(self) -> LoggerProvider:
        """Get the logger provider."""
        return _logs.get_logger_provider()

    def correlate_logger(self, logger_name: str, level: int = logging.INFO) -> None:
        """Correlate a logger with the current span."""
        log_config = self._log_config.copy()
        log_config.update(
            {
                "loggers": {
                    logger_name: {
                        "handlers": ["otel_jobmon"],
                        "level": level,
                    },
                },
            }
        )
        logging.config.dictConfig(log_config)

    @staticmethod
    def get_span_details() -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Retrieve details of the current span."""
        span = trace.get_current_span()

        # Check if there's a valid span
        if not span or not span.is_recording():
            return None, None, None

        ctx = span.get_span_context()

        # Get parent span, but handle if it doesn't exist
        parent = None
        if hasattr(span, "parent"):
            parent = span.parent

        span_id = hex(ctx.span_id) if ctx and ctx.span_id else None
        trace_id = hex(ctx.trace_id) if ctx and ctx.trace_id else None
        parent_span_id = None if not parent else hex(parent.span_id)

        return span_id, trace_id, parent_span_id


class OpenTelemetryLogFormatter(logging.Formatter):
    """Formatter that adds OpenTelemetry spans to log records."""

    def format(self, record: Any) -> Any:
        span_id, trace_id, parent_span_id = OtlpAPI.get_span_details()
        record.span_id = span_id
        record.trace_id = trace_id
        record.parent_span_id = parent_span_id
        return super().format(record)
