"""Shared structlog configuration for all jobmon components.

This module provides structlog configuration that enables:
1. Context variable merging (required for @bind_context decorator)
2. Basic stdlib metadata decoration (logger name, level) while deferring
   rendering/formatting to the host application
3. Optional Jobmon telemetry isolation and OTLP capture
4. Optional component identification in logs
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set

from jobmon.core.logging.context import get_jobmon_context

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
        return event_dict

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
    return event_dict


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
    extra_processors: Optional[List[Callable]] = None,
    enable_jobmon_context: bool = True,
    telemetry_logger_prefixes: Optional[List[str]] = None,
) -> None:
    """Configure structlog for jobmon components.

    This function sets up structlog with processors that:
    1. Merge context variables (from bind_contextvars and @bind_context decorator)
    2. Add logger metadata (logger name, log level)
    3. Optionally add a component field to the event_dict
    4. Optionally isolate Jobmon telemetry metadata to configured logger prefixes
    5. Capture the raw event_dict for OTLP handlers

    IMPORTANT: This must be called before using the @bind_context decorator
    or any structlog.contextvars.bind_contextvars() calls.

    PROCESSOR CHAIN ORDER (after configuration):
    1. merge_contextvars (Jobmon context)
    2. component processor (optional, when component_name is provided)
    3. filter_by_level (stdlib)
    4. add_logger_name (stdlib)
    5. add_log_level (stdlib)
    6. telemetry isolation processor (optional, controlled by enable_jobmon_context)
    7. extra_processors (host processors, if provided)
    8. _store_event_dict_for_otlp (Jobmon OTLP capture)
    9. ProcessorFormatter.wrap_for_formatter (stdlib - keeps stdlib handlers working)

    COLLISION AVOIDANCE:
    - Host processors in extra_processors run AFTER telemetry isolation
    - This prevents telemetry metadata from leaking into host rendering
    - Avoid duplicate processors (timestamps, exception formatting, etc.) in extra_processors
    - Use unique event_dict keys to avoid overwrites
    - Jobmon telemetry keys: workflow_run_id, task_instance_id, array_id, etc.
      (see jobmon.core.logging.context.JOBMON_METADATA_KEYS)
    - Standard keys to avoid duplicating: timestamp, level, logger, event, exc_info

    Args:
        component_name: Component name to add to all logs (e.g., "distributor")
        extra_processors: Additional processors to insert into the chain AFTER
                         telemetry isolation. Avoid duplicate processors (timestamps,
                         log levels).
        enable_jobmon_context: If True, install telemetry isolation processor to isolate
                              telemetry metadata (workflow_run_id, task_instance_id, etc.)
                              to configured logger prefixes. Defaults to True for backward
                              compatibility.
        telemetry_logger_prefixes: List of logger name prefixes that should receive
                                  telemetry metadata. Defaults to ["jobmon."]. Only used
                                  when enable_jobmon_context=True.

    Example:
        >>> # Basic usage
        >>> configure_structlog(component_name="distributor")

        >>> # With OTLP trace integration
        >>> from jobmon.core.otlp import add_span_details_processor
        >>> configure_structlog(
        ...     component_name="distributor",
        ...     extra_processors=[add_span_details_processor]
        ... )

        >>> # Disable jobmon context isolation (for host applications)
        >>> configure_structlog(enable_jobmon_context=False)

        >>> # Custom telemetry prefixes
        >>> configure_structlog(
        ...     telemetry_logger_prefixes=["myapp.telemetry", "jobmon."]
        ... )
    """
    import structlog

    if extra_processors is None:
        extra_processors = []

    # Build processor chain
    processors: List[Callable] = [
        # CRITICAL: merge_contextvars enables @bind_context decorator
        # and manual bind_contextvars() calls
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
    ]

    # Run isolation before any downstream processors so host processors never
    # observe Jobmon-only telemetry fields unless explicitly opted in.
    if enable_jobmon_context:
        prefixes = telemetry_logger_prefixes or ["jobmon."]
        processor = create_telemetry_isolation_processor(prefixes)
        processors.append(processor)

    processors.extend(
        [
            *extra_processors,
            _store_event_dict_for_otlp,  # Capture raw event_dict for OTLP handlers
            # Required so stdlib logging handlers using ProcessorFormatter keep working.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]
    )

    # Add component name to all logs if provided
    if component_name:
        processors.insert(1, _create_component_processor(component_name))

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
    enable_jobmon_context: bool = True,
) -> None:
    """Configure structlog with OTLP trace integration if enabled.

    Checks JobmonConfig for telemetry.tracing settings and configures
    structlog with OpenTelemetry trace processors if enabled.

    This is the recommended function to call from component CLIs as it
    automatically handles OTLP configuration based on settings.

    Args:
        component_name: Component name (e.g., "distributor", "worker")
        enable_jobmon_context: If True, install jobmon_context_processor to isolate
                              telemetry metadata to jobmon.* loggers only. Defaults to True.

    Example:
        >>> # In distributor CLI
        >>> configure_structlog_with_otlp(component_name="distributor")

        >>> # In worker CLI
        >>> configure_structlog_with_otlp(component_name="worker")

        >>> # Disable jobmon context isolation (for host applications)
        >>> configure_structlog_with_otlp(
        ...     component_name="distributor",
        ...     enable_jobmon_context=False,
        ... )
    """
    try:
        from jobmon.core.configuration import JobmonConfig

        config = JobmonConfig()
    except Exception:
        # Config not available, use basic configuration
        configure_structlog(
            component_name=component_name, enable_jobmon_context=enable_jobmon_context
        )
        return

    # Check if OTLP tracing is enabled for this component
    try:
        telemetry_section = config.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})

        # Component-specific enable flag (e.g., "distributor_enabled")
        component_key = f"{component_name}_enabled"
        otlp_enabled = tracing_config.get(component_key, False)
    except Exception:
        otlp_enabled = False

    if otlp_enabled:
        # OTLP tracing is enabled, add trace processors
        try:
            from jobmon.core.otlp import add_span_details_processor

            configure_structlog(
                component_name=component_name,
                extra_processors=[add_span_details_processor],
                enable_jobmon_context=enable_jobmon_context,
            )
        except ImportError:
            # OTLP module not available, configure without it
            configure_structlog(
                component_name=component_name,
                enable_jobmon_context=enable_jobmon_context,
            )
    else:
        # OTLP not enabled, basic configuration
        configure_structlog(
            component_name=component_name, enable_jobmon_context=enable_jobmon_context
        )


def prepend_jobmon_processors_to_existing_config(
    telemetry_logger_prefixes: Optional[List[str]] = None,
) -> None:
    """Prepend Jobmon processors to an existing structlog configuration.

    This is called when Jobmon is used as a library and the host application
    has already configured structlog. It intelligently prepends Jobmon's processors
    to the existing processor chain to enable telemetry context management and isolation
    while preserving the host application's final rendering.

    Adapts to the host app's logging architecture:
    - Stdlib integration adds merge_contextvars, filter_by_level,
      add_logger_name, and telemetry isolation
    - Direct rendering (like FHS) adds merge_contextvars and telemetry isolation
    - Host processors remain untouched so final rendering is preserved

    Args:
        telemetry_logger_prefixes: Logger prefixes that should retain Jobmon telemetry
            metadata. Defaults to ["jobmon."] to match configure_structlog().
    """
    import structlog

    # Get current configuration
    current_config = structlog.get_config()
    existing_processors = list(current_config.get("processors", []))
    logger_factory = current_config.get("logger_factory")
    wrapper_class = current_config.get("wrapper_class")

    # Check if host app uses stdlib logging integration
    uses_stdlib_integration = _uses_stdlib_integration(logger_factory, wrapper_class)

    # Build Jobmon processors ensuring correct ordering: merge contextvars must
    # always run first.
    jobmon_processors: List[Callable] = []

    # Always ensure merge_contextvars runs first
    merge_contextvars = structlog.contextvars.merge_contextvars
    jobmon_processors.append(merge_contextvars)

    if uses_stdlib_integration:
        for processor in (
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
        ):
            jobmon_processors.append(processor)

    prefixes = telemetry_logger_prefixes or ["jobmon."]
    prefixes_tuple = tuple(prefixes)
    if not _has_isolation_processor(existing_processors, prefixes_tuple):
        jobmon_processors.append(create_telemetry_isolation_processor(prefixes))

    if not _processor_present(existing_processors, _store_event_dict_for_otlp):
        jobmon_processors.append(_store_event_dict_for_otlp)

    if not uses_stdlib_integration and not _processor_present(
        existing_processors, _forward_event_to_logging_handlers
    ):
        jobmon_processors.append(_forward_event_to_logging_handlers)

    # Remove Jobmon processors from existing chain to avoid duplicates
    def _remove_processor(processors: List[Callable], processor: Callable) -> None:
        """Remove first occurrence of processor from list."""
        try:
            processors.remove(processor)
        except ValueError:
            pass  # Not present, that's fine

    _remove_processor(existing_processors, merge_contextvars)
    if uses_stdlib_integration:
        _remove_processor(existing_processors, structlog.stdlib.filter_by_level)
        _remove_processor(existing_processors, structlog.stdlib.add_logger_name)
    _remove_processor(existing_processors, _store_event_dict_for_otlp)
    if not uses_stdlib_integration:
        _remove_processor(existing_processors, _forward_event_to_logging_handlers)

    # Combine: Jobmon processors first (in correct order), then remaining host processors
    new_processors = jobmon_processors + existing_processors

    # Update configuration with combined processors
    new_config = current_config.copy()
    new_config["processors"] = new_processors

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
