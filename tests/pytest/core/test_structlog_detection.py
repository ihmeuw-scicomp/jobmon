import functools

import structlog

from jobmon.core.config.structlog_config import _uses_stdlib_integration


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
