"""Shared structlog configuration for all jobmon components.

This module provides structlog configuration that enables:

1. Context variable merging (required for ``@bind_context`` decorator)
2. Basic stdlib metadata decoration (logger name, level) while deferring
   rendering/formatting to the host application
3. Optional Jobmon telemetry isolation and OTLP capture
4. Optional component identification in logs
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
)

from structlog._output import PrintLogger, PrintLoggerFactory

from jobmon.core.logging.context import get_jobmon_context


class _NamedPrintLogger:
    """Wrapper that adds a ``name`` attribute to ``PrintLogger``."""

    __slots__ = ("_logger", "name")

    def __init__(self, logger_name: str, base_logger: PrintLogger) -> None:
        self._logger = base_logger
        self.name = logger_name

    def __getattr__(self, item: str) -> Any:
        return getattr(self._logger, item)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<_NamedPrintLogger name={self.name!r} logger={self._logger!r}>"


class _NamedPrintLoggerFactory:
    """Factory that decorates ``PrintLogger`` instances with a name."""

    __jobmon_named_factory__ = True

    def __init__(self, base_factory: PrintLoggerFactory) -> None:
        self._base_factory = base_factory

    def __call__(self, *args: Any) -> _NamedPrintLogger:
        base_logger = self._base_factory(*args)
        logger_name: Optional[str] = None
        if args and isinstance(args[0], str):
            logger_name = args[0]
        return _NamedPrintLogger(logger_name or "jobmon.client", base_logger)


# Thread-local storage for event_dict captured by OTLP handlers.
_thread_local = threading.local()
# Reference count so multiple OTLP handlers can co-exist safely.
_otlp_handler_count = 0
_otlp_capture_lock = threading.Lock()


def _store_event_dict_for_otlp(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Store ``event_dict`` in thread-local storage for OTLP exporters."""
    if _otlp_handler_count == 0:
        return event_dict

    # Keep a shallow copy so handlers can mutate safely.
    _thread_local.last_event_dict = dict(event_dict)
    return event_dict


def _forward_event_to_logging_handlers(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Forward structlog events to stdlib handlers when hosts render directly."""
    log_name = getattr(logger, "name", event_dict.get("logger", "jobmon.client"))
    level = logging._nameToLevel.get(method_name.upper(), logging.INFO)

    target_logger = logging.getLogger(log_name)

    if not target_logger.handlers:
        parent_name = log_name
        while not target_logger.handlers and target_logger.propagate:
            parent_name, _, _ = parent_name.rpartition(".")
            if not parent_name:
                break
            target_logger = logging.getLogger(parent_name)

    if not target_logger.handlers:
        return event_dict

    event_dict.setdefault("logger", log_name)
    if hasattr(_thread_local, "last_event_dict") and isinstance(
        _thread_local.last_event_dict, dict
    ):
        _thread_local.last_event_dict.setdefault("logger", log_name)

    record = logging.LogRecord(
        name=log_name,
        level=level,
        pathname="jobmon/core/config/structlog_config.py",
        lineno=0,
        msg=event_dict.get("event", ""),
        args=(),
        exc_info=_extract_exc_info(event_dict),
    )

    target_logger.handle(record)

    return _prune_event_dict_for_console(event_dict)


def _prune_event_dict_for_console(
    event_dict: Dict[str, Any],
) -> Dict[str, Any]:
    """Strip Jobmon-bound metadata while leaving host formatting intact.

    Removes all keys starting with 'telemetry_' prefix along with helper fields
    added for OTLP forwarding (``logger`` and ``level``). This keeps console output
    clean while preserving the full context for OTLP exports.
    """
    cleaned = dict(event_dict)

    for key in list(cleaned.keys()):
        if key.startswith("telemetry_"):
            cleaned.pop(key, None)

    cleaned.pop("level", None)
    cleaned.pop("logger", None)

    return cleaned


def _ensure_logger_name(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Ensure the event dict contains a ``logger`` field."""
    if "logger" in event_dict and isinstance(event_dict["logger"], str):
        return event_dict

    candidate = getattr(logger, "name", None)
    if not candidate:
        factory_args = getattr(logger, "_logger_factory_args", ())
        if isinstance(factory_args, tuple) and factory_args:
            candidate = factory_args[0]

    event_dict["logger"] = candidate or "jobmon.client"
    return event_dict


def _wrap_print_logger_factory(factory: Any) -> Any:
    """Decorate PrintLogger factories so produced loggers expose ``name``."""
    if isinstance(factory, PrintLoggerFactory) and not getattr(
        factory, "__jobmon_named_factory__", False
    ):
        return _NamedPrintLoggerFactory(factory)
    return factory


def _wrap_wrapper_class_for_otlp(wrapper_class: Any) -> Any:
    """Ensure filtered log levels still execute processors for OTLP capture."""
    if not isinstance(wrapper_class, type):
        return wrapper_class

    if getattr(wrapper_class, "__jobmon_otlp_passthrough__", False):
        return wrapper_class

    # Detect filtered methods via structlog's _nop sentinel when available.
    filtered_methods = set()

    _nop = None
    for module in (
        "structlog._native",
        "structlog._log_levels",
        "structlog._config",
    ):
        if _nop is not None:
            break
        try:
            _nop = __import__(module, fromlist=["_nop"])._nop  # type: ignore[attr-defined]
        except (ImportError, AttributeError):
            continue

    if _nop is not None:
        filtered_methods = {
            name
            for name in ("debug", "trace", "verbose")
            if name in wrapper_class.__dict__ and wrapper_class.__dict__[name] is _nop
        }
    else:
        # Final fallback: inspect the method object name. Some structlog versions
        # expose the sentinel as a regular function named "_nop" even if the import
        # path changes. This keeps detection narrow without instantiating wrappers.
        filtered_methods = {
            name
            for name in ("debug", "trace", "verbose")
            if name in wrapper_class.__dict__
            and getattr(wrapper_class.__dict__[name], "__name__", None) == "_nop"
        }

    if not filtered_methods:
        return wrapper_class

    try:
        from structlog.exceptions import DropEvent
    except Exception:  # pragma: no cover - defensive import

        class DropEvent(Exception):  # type: ignore[no-redef]
            """Fallback DropEvent placeholder when structlog is unavailable."""

    def _jobmon_process_filtered_event(
        self: Any, method_name: str, event: Any, args: Sequence[Any], kw: Dict[str, Any]
    ) -> None:
        level = logging._nameToLevel.get(method_name.upper(), logging.INFO)
        log_name = getattr(getattr(self, "_logger", None), "name", None)
        if not log_name:
            log_name = kw.get("logger") or kw.get("name")

        if not log_name:
            return

        target_logger = logging.getLogger(log_name)
        if not target_logger.isEnabledFor(level):
            return

        rendered_event = event
        if args:
            try:
                rendered_event = event % tuple(args)
            except Exception:
                rendered_event = event

        event_kw = dict(kw)
        try:
            self._process_event(method_name, rendered_event, event_kw)
        except DropEvent:
            return
        except Exception:
            return

    def _make_method(name: str) -> Callable[..., Any]:
        def _method(self: Any, event: Any, *args: Any, **kw: Any) -> Any:
            _jobmon_process_filtered_event(self, name, event, args, kw)
            return None

        return _method

    def _make_async_method(name: str) -> Callable[..., Any]:
        async def _amethod(self: Any, event: Any, *args: Any, **kw: Any) -> Any:
            _jobmon_process_filtered_event(self, name, event, args, kw)
            return None

        return _amethod

    attrs: Dict[str, Any] = {
        "__jobmon_otlp_passthrough__": True,
        "_jobmon_process_filtered_event": _jobmon_process_filtered_event,
    }

    for method_name in filtered_methods:
        attrs[method_name] = _make_method(method_name)
        async_name = f"a{method_name}"
        if wrapper_class.__dict__.get(async_name) is not None:
            attrs[async_name] = _make_async_method(method_name)

    jobmon_wrapper = type(
        f"JobmonOTLPPassthrough{wrapper_class.__name__}",
        (wrapper_class,),
        attrs,
    )

    return jobmon_wrapper


def _build_structlog_processor_chain(
    *,
    uses_stdlib_integration: bool,
    component_name: Optional[str],
    telemetry_logger_prefixes: Optional[Sequence[str]],
    extra_processors: Iterable[Callable],
    include_store_for_otlp: bool,
    include_wrap_for_formatter: bool,
    include_telemetry_isolation: bool = True,
) -> List[Callable]:
    """Compose the processor chain shared across Jobmon structlog setup paths.

    Args:
        uses_stdlib_integration: If True, configures for stdlib logging integration.
                                If False, configures for direct rendering (e.g., PrintLogger).
        component_name: Optional component name to add to all logs.
        telemetry_logger_prefixes: Logger name prefixes for telemetry isolation.
        extra_processors: Additional processors to insert after telemetry isolation.
        include_store_for_otlp: Whether to include OTLP event storage processor.
        include_wrap_for_formatter: Whether to include ProcessorFormatter wrapper (for stdlib).
        include_telemetry_isolation: Whether to include telemetry isolation processor.
    """
    import structlog

    processors: List[Callable] = [structlog.contextvars.merge_contextvars]

    if component_name:
        processors.append(_create_component_processor(component_name))

    if uses_stdlib_integration:
        # Standard library logging integration path
        processors.append(structlog.stdlib.filter_by_level)
        processors.append(structlog.stdlib.add_logger_name)
        processors.append(structlog.stdlib.add_log_level)
    else:
        # Direct rendering path (e.g., PrintLogger)
        processors.append(_ensure_logger_name)

    if include_telemetry_isolation:
        prefixes = list(telemetry_logger_prefixes or ["jobmon."])
        processors.append(create_telemetry_isolation_processor(prefixes))

    if include_store_for_otlp:
        processors.append(_store_event_dict_for_otlp)

    processors.extend(list(extra_processors))

    if not uses_stdlib_integration:
        # For direct rendering, add log level and forward to stdlib handlers for OTLP
        processors.append(structlog.stdlib.add_log_level)
        processors.append(_forward_event_to_logging_handlers)

    if include_wrap_for_formatter:
        processors.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)

    return processors


def _extract_exc_info(event_dict: Dict[str, Any]) -> Optional[Any]:
    """Translate structlog exception metadata into logging exc_info tuple."""
    if "exc_info" in event_dict:
        exc_info = event_dict["exc_info"]
        if isinstance(exc_info, tuple):
            return exc_info
        if isinstance(exc_info, BaseException):
            return (exc_info.__class__, exc_info, exc_info.__traceback__)
        if exc_info:
            try:
                return tuple(exc_info)  # type: ignore[arg-type]
            except TypeError:
                pass

    if event_dict.get("exc_text"):
        return (None, event_dict["exc_text"], None)

    return None


def enable_structlog_otlp_capture() -> None:
    """Enable thread-local capture for OTLP handlers."""
    global _otlp_handler_count
    with _otlp_capture_lock:
        _otlp_handler_count += 1


def disable_structlog_otlp_capture() -> None:
    """Disable OTLP capture (used for tests or when handlers are removed)."""
    global _otlp_handler_count
    with _otlp_capture_lock:
        if _otlp_handler_count > 0:
            _otlp_handler_count -= 1
        if _otlp_handler_count == 0 and hasattr(_thread_local, "last_event_dict"):
            delattr(_thread_local, "last_event_dict")


@contextmanager
def structlog_otlp_capture_enabled() -> Iterator[None]:
    """Context manager to enable OTLP capture temporarily.

    Ensures the reference count is decremented even if an exception is raised.
    Particularly useful in tests that need to toggle capture around assertions.
    """
    enable_structlog_otlp_capture()
    try:
        yield
    finally:
        disable_structlog_otlp_capture()


def configure_structlog(
    component_name: Optional[str] = None,
    *,
    extra_processors: Optional[Iterable[Callable]] = None,
) -> None:
    """Configure structlog for jobmon components.

    This function sets up structlog with processors that:

    1. Merge context variables (from bind_contextvars and ``@bind_context`` decorator)
    2. Add logger metadata (logger name, log level)
    3. Optionally add a component field to the event_dict
    4. Isolate Jobmon telemetry metadata to jobmon.* loggers
    5. Capture the raw event_dict for OTLP handlers
    6. Optionally include extra processors supplied by the caller

    IMPORTANT: This must be called before using the ``@bind_context`` decorator
    or any ``structlog.contextvars.bind_contextvars()`` calls.

    PROCESSOR CHAIN ORDER (after configuration):

    1. merge_contextvars (Jobmon context)
    2. component processor (optional, when component_name is provided)
    3. filter_by_level (stdlib)
    4. add_logger_name (stdlib)
    5. add_log_level (stdlib)
    6. telemetry isolation processor
    7. ``_store_event_dict_for_otlp`` (Jobmon OTLP capture)
    8. Extra processors supplied via ``extra_processors`` (if any)
    9. ProcessorFormatter.wrap_for_formatter (stdlib - keeps stdlib handlers working)

    Args:
        component_name: Component name to add to all logs (e.g., "distributor")
        extra_processors: Additional structlog processors to append after Jobmon's
            defaults (e.g., custom formatting or telemetry processors)

    Example::

        # Basic usage
        configure_structlog(component_name="distributor")
    """
    import structlog

    processors = _build_structlog_processor_chain(
        uses_stdlib_integration=True,
        component_name=component_name,
        telemetry_logger_prefixes=["jobmon."],
        extra_processors=list(extra_processors or []),
        include_store_for_otlp=True,
        include_wrap_for_formatter=True,
    )

    # Configure structlog globally
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _create_component_processor(component_name: str) -> Callable:
    """Create a processor that adds component name to all log events.

    Args:
        component_name: Name of the component (e.g., "distributor", "worker")

    Returns:
        Processor function that adds component field to event_dict
    """

    def add_component(
        logger: Any, method_name: str, event_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        event_dict["component"] = component_name
        return event_dict

    return add_component


def create_telemetry_isolation_processor(
    telemetry_prefixes: List[str],
) -> Callable[[Any, str, Dict[str, Any]], Dict[str, Any]]:
    """Create a processor that isolates telemetry metadata to specific logger prefixes.

    Args:
        telemetry_prefixes: List of logger name prefixes that should receive telemetry
                           metadata (e.g., ["jobmon.", "myapp.telemetry"]).

    Returns:
        A structlog processor function that isolates telemetry metadata.
    """

    def isolate_telemetry_processor(
        logger: Any, method_name: str, event_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Isolate telemetry metadata to configured logger prefixes.

        Injects telemetry metadata only into logs from configured prefixes.
        Removes telemetry metadata from logs from other namespaces.
        """
        metadata = get_jobmon_context()
        if not metadata:
            return event_dict

        logger_name = event_dict.get("logger")
        if not logger_name and hasattr(logger, "name"):
            logger_name = getattr(logger, "name")

        # Check if this logger should get telemetry metadata
        should_include_telemetry = isinstance(logger_name, str) and any(
            logger_name.startswith(prefix) for prefix in telemetry_prefixes
        )

        if should_include_telemetry:
            for key, value in metadata.items():
                event_dict.setdefault(key, value)
        else:
            for key in metadata:
                event_dict.pop(key, None)

        return event_dict

    isolation_key = tuple(telemetry_prefixes)
    # Tag the processor so we can detect duplicate registrations later.
    setattr(
        isolate_telemetry_processor, "__jobmon_telemetry_isolation__", isolation_key
    )

    return isolate_telemetry_processor


def configure_structlog_with_otlp(
    component_name: str,
) -> None:
    """Configure structlog with OTLP trace integration if enabled.

    Checks JobmonConfig for telemetry.tracing settings and configures
    structlog with OpenTelemetry trace processors if enabled.

    This is the recommended function to call from component CLIs as it
    automatically handles OTLP configuration based on settings.

    Args:
        component_name: Component name (e.g., "distributor", "worker")

    Example:
        >>> # In distributor CLI
        >>> configure_structlog_with_otlp(component_name="distributor")

        >>> # In worker CLI
        >>> configure_structlog_with_otlp(component_name="worker")
    """
    # Check if OTLP tracing is enabled for this component
    otlp_enabled = False
    try:
        from jobmon.core.configuration import JobmonConfig

        config = JobmonConfig()
        telemetry_section = config.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})

        # Component-specific enable flag (e.g., "distributor_enabled")
        component_key = f"{component_name}_enabled"
        otlp_enabled = tracing_config.get(component_key, False)
    except Exception:
        otlp_enabled = False

    # Build processor list with optional OTLP trace processor
    extra_processors: List[Callable] = []
    if otlp_enabled:
        try:
            from jobmon.core.otlp import add_span_details_processor

            extra_processors = [add_span_details_processor]
        except ImportError:
            pass

    configure_structlog(
        component_name=component_name,
        extra_processors=extra_processors,
    )


def prepend_jobmon_processors_to_existing_config() -> None:
    """Prepend Jobmon processors to an existing structlog configuration.

    This is called when Jobmon is used as a library and the host application
    has already configured structlog. It intelligently prepends Jobmon's processors
    to the existing processor chain to enable telemetry context management and isolation
    while preserving the host application's final rendering.

    Adapts to the host app's logging architecture:

    - Stdlib integration adds ``merge_contextvars``, ``filter_by_level``,
      ``add_logger_name``, and telemetry isolation
    - Direct rendering (like FHS) adds ``merge_contextvars`` and telemetry isolation
    - Host processors remain untouched so final rendering is preserved
    """
    import structlog

    # Get current configuration
    current_config = structlog.get_config()
    existing_processors = list(current_config.get("processors", []))
    logger_factory = current_config.get("logger_factory")
    wrapper_class = current_config.get("wrapper_class")

    # Check if host app uses stdlib logging integration
    uses_stdlib_integration = _uses_stdlib_integration(logger_factory, wrapper_class)

    prefixes = ["jobmon."]
    prefixes_tuple = tuple(prefixes)

    # Build list of Jobmon processors to prepend
    jobmon_processors: List[Callable] = []

    # Always need context merging
    jobmon_processors.append(structlog.contextvars.merge_contextvars)

    # Add stdlib integration processors (but not add_log_level, host may have it)
    if uses_stdlib_integration:
        jobmon_processors.append(structlog.stdlib.filter_by_level)
        jobmon_processors.append(structlog.stdlib.add_logger_name)
    else:
        # Direct rendering path
        jobmon_processors.append(_ensure_logger_name)

    # Add telemetry isolation
    if not _has_isolation_processor(existing_processors, prefixes_tuple):
        jobmon_processors.append(create_telemetry_isolation_processor(prefixes))

    # Add OTLP capture
    jobmon_processors.append(_store_event_dict_for_otlp)

    # For direct rendering, add forwarding to stdlib handlers
    if not uses_stdlib_integration:
        jobmon_processors.append(structlog.stdlib.add_log_level)
        jobmon_processors.append(_forward_event_to_logging_handlers)

    # Remove Jobmon processors from existing chain to avoid duplicates
    def _remove_processor(processors: List[Callable], processor: Callable) -> None:
        """Remove first occurrence of processor from list."""
        try:
            processors.remove(processor)
        except ValueError:
            pass  # Not present, that's fine

    for processor in jobmon_processors:
        _remove_processor(existing_processors, processor)

    if not uses_stdlib_integration:
        logger_factory = _wrap_print_logger_factory(logger_factory)
        wrapper_class = _wrap_wrapper_class_for_otlp(wrapper_class)

    # Combine: Jobmon processors first (in correct order), then remaining host processors
    new_processors = jobmon_processors + existing_processors

    # Update configuration with combined processors
    new_config = current_config.copy()
    new_config["processors"] = new_processors
    new_config["logger_factory"] = logger_factory
    if wrapper_class is not None:
        new_config["wrapper_class"] = wrapper_class

    structlog.configure(**new_config)


def is_structlog_configured() -> bool:
    """Check if structlog has been configured.

    Returns:
        True if structlog is configured, False otherwise

    Example:
        >>> if not is_structlog_configured():
        ...     configure_structlog(component_name="my_component")
    """
    import structlog

    # Check if structlog has been configured by looking for processors
    return structlog.is_configured()


def _iter_factory_candidates(factory: Any) -> Iterator[Any]:
    """Yield the given factory and any nested factories exposed via common attrs."""
    if factory is None:
        return

    stack = [factory]
    seen: Set[int] = set()

    while stack:
        current = stack.pop()
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)

        yield current

        for attr in ("__wrapped__", "wrapped_factory", "factory", "inner_factory"):
            nested = getattr(current, attr, None)
            if nested is not None:
                stack.append(nested)

        func = getattr(current, "func", None)  # functools.partial
        if func is not None:
            stack.append(func)


def _looks_like_direct_rendering_factory(factory: Any) -> bool:
    """Best-effort detection of structlog PrintLogger-based factories."""
    import structlog

    print_factory_cls = getattr(structlog, "PrintLoggerFactory", None)
    print_logger_cls = getattr(structlog, "PrintLogger", None)

    if not print_factory_cls and not print_logger_cls:
        return False

    for candidate in _iter_factory_candidates(factory):
        try:
            if print_factory_cls and (
                isinstance(candidate, print_factory_cls)
                or (
                    isinstance(candidate, type)
                    and issubclass(candidate, print_factory_cls)
                )
            ):
                return True

            candidate_cls = getattr(candidate, "__class__", None)
            if (
                print_factory_cls
                and isinstance(candidate_cls, type)
                and issubclass(candidate_cls, print_factory_cls)
            ):
                return True

            if print_logger_cls and (
                isinstance(candidate, print_logger_cls)
                or (
                    isinstance(candidate, type)
                    and issubclass(candidate, print_logger_cls)
                )
            ):
                return True
        except Exception:  # noqa: BLE001 - heuristic guard
            continue

    return False


def _wrapper_indicates_direct_rendering(wrapper_class: Any) -> bool:
    """Detect wrapper classes that imply direct rendering (e.g., PrintLogger)."""
    import structlog

    print_logger_cls = getattr(structlog, "PrintLogger", None)
    if not print_logger_cls or not isinstance(wrapper_class, type):
        return False

    try:
        return issubclass(wrapper_class, print_logger_cls)
    except Exception:  # noqa: BLE001 - guard against atypical wrapper types
        return False


def _uses_stdlib_integration(logger_factory: Any, wrapper_class: Any) -> bool:
    """Best-effort detection of stdlib integration vs direct rendering."""
    import structlog

    if _looks_like_direct_rendering_factory(logger_factory):
        return False

    if _wrapper_indicates_direct_rendering(wrapper_class):
        return False

    logger_factory_cls = getattr(structlog.stdlib, "LoggerFactory", None)
    if logger_factory_cls and isinstance(logger_factory, logger_factory_cls):
        return True

    bound_logger_cls = getattr(structlog.stdlib, "BoundLogger", None)
    if (
        isinstance(wrapper_class, type)
        and bound_logger_cls
        and issubclass(wrapper_class, bound_logger_cls)  # type: ignore[arg-type]
    ):
        return True

    if _looks_like_direct_rendering_factory(getattr(logger_factory, "__class__", None)):
        return False

    factory_repr = repr(logger_factory)
    if "PrintLogger" in factory_repr:
        return False

    # Fallback: default to stdlib integration. Safer than assuming direct rendering
    # when the host provides a custom factory that we cannot introspect.
    return True


def _processor_present(processors: Iterable[Callable], processor: Callable) -> bool:
    """Return True when the exact processor function is already present."""
    return any(p is processor for p in processors)


def _has_isolation_processor(
    processors: Iterable[Callable], prefixes: tuple[str, ...]
) -> bool:
    """Check whether an isolation processor for the given prefixes already exists."""
    for proc in processors:
        registered = getattr(proc, "__jobmon_telemetry_isolation__", None)
        if registered == prefixes:
            return True
    return False
