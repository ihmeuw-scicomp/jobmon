import functools

import structlog

from jobmon.core.config.structlog_config import _ensure_logger_name  # noqa: PLC0414
from jobmon.core.config.structlog_config import (
    _build_structlog_processor_chain,
    _forward_event_to_logging_handlers,
    _store_event_dict_for_otlp,
    _uses_stdlib_integration,
)


def test_direct_detection_with_printlogger_subclass() -> None:
    class CustomPrintLoggerFactory(structlog.PrintLoggerFactory):
        pass

    factory = CustomPrintLoggerFactory()

    assert not _uses_stdlib_integration(factory, structlog.PrintLogger)


def test_direct_detection_with_wrapped_factory_attribute() -> None:
    inner_factory = structlog.PrintLoggerFactory()

    class Wrapper:
        def __init__(self, wrapped: structlog.PrintLoggerFactory) -> None:  # type: ignore[type-arg]
            self.wrapped_factory = wrapped

        def __call__(
            self, *args, **kwargs
        ):  # noqa: ANN001, D401 - structlog signature passthrough
            return self.wrapped_factory(*args, **kwargs)

    factory = Wrapper(inner_factory)

    assert not _uses_stdlib_integration(factory, structlog.PrintLogger)


def test_direct_detection_with_partial_wrapping() -> None:
    inner_factory = structlog.PrintLoggerFactory()
    partial_factory = functools.partial(inner_factory.__call__)

    assert not _uses_stdlib_integration(partial_factory, structlog.PrintLogger)


def test_stdlib_detection_still_true() -> None:
    factory = structlog.stdlib.LoggerFactory()

    assert _uses_stdlib_integration(factory, structlog.stdlib.BoundLogger)


def test_build_processor_chain_for_stdlib_integration() -> None:
    processors = _build_structlog_processor_chain(
        uses_stdlib_integration=True,
        component_name="client",
        telemetry_logger_prefixes=["jobmon."],
        extra_processors=[],
        include_store_for_otlp=True,
        include_wrap_for_formatter=True,
    )

    assert structlog.contextvars.merge_contextvars in processors
    assert structlog.stdlib.filter_by_level in processors
    assert structlog.stdlib.add_logger_name in processors
    assert structlog.stdlib.add_log_level in processors
    assert processors[-2] is _store_event_dict_for_otlp
    assert processors[-1] is structlog.stdlib.ProcessorFormatter.wrap_for_formatter

    # Component processor should inject component field
    component_processor = processors[1]
    event_dict: dict[str, object] = {}
    component_processor(structlog.get_logger("jobmon.client"), "info", event_dict)
    assert event_dict.get("component") == "client"


def test_build_processor_chain_for_direct_rendering() -> None:
    processors = _build_structlog_processor_chain(
        uses_stdlib_integration=False,
        component_name=None,
        telemetry_logger_prefixes=["jobmon."],
        extra_processors=[],
        include_store_for_otlp=True,
        include_wrap_for_formatter=False,
    )

    assert _ensure_logger_name in processors
    assert structlog.stdlib.filter_by_level not in processors
    assert structlog.stdlib.add_logger_name not in processors
    # For direct rendering: OTLP store, then add_log_level, then forward to handlers
    assert processors[-3] is _store_event_dict_for_otlp
    assert processors[-2] is structlog.stdlib.add_log_level
    assert processors[-1] is _forward_event_to_logging_handlers
