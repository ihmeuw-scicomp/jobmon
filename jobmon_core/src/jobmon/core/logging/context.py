"""Telemetry context helpers for Jobmon structlog instrumentation.

All Jobmon telemetry metadata is namespaced with the 'telemetry_' prefix.
This clearly indicates data that is exported to OTLP but stripped from
console output, without requiring explicit key registries.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterator, Sequence

import structlog

# Ensure structlog contextvars support is available.
if not hasattr(structlog, "contextvars") or not hasattr(
    structlog.contextvars, "get_contextvars"
):  # pragma: no cover - import-time check
    raise RuntimeError(
        "structlog.contextvars must provide get_contextvars() for Jobmon telemetry"
    )


def get_jobmon_context() -> Dict[str, Any]:
    """Return a copy of all active Jobmon telemetry metadata.

    Returns all context variables with the 'telemetry_' prefix.
    """
    ctx = structlog.contextvars.get_contextvars()
    return {k: v for k, v in ctx.items() if k.startswith("telemetry_")}


def clear_jobmon_context() -> None:
    """Remove all Jobmon telemetry metadata from the current context."""
    metadata = get_jobmon_context()
    if metadata:
        structlog.contextvars.unbind_contextvars(*metadata.keys())


def set_jobmon_context(*, allow_non_jobmon_keys: bool = False, **metadata: Any) -> None:
    """Bind telemetry metadata to the current structlog context.

    All keys are automatically prefixed with 'telemetry_' unless allow_non_jobmon_keys is True.

    Args:
        allow_non_jobmon_keys: If True, bind keys as-is without adding telemetry_ prefix.
                              Used for server-side context propagation and decorators.
        **metadata: Key-value pairs to bind to context.
    """
    if allow_non_jobmon_keys:
        # Allow arbitrary keys without prefixing (for server context propagation)
        filtered = {k: v for k, v in metadata.items() if v is not None}
    else:
        # Ensure all keys have telemetry_ prefix (for Jobmon telemetry)
        filtered = {
            (k if k.startswith("telemetry_") else f"telemetry_{k}"): v
            for k, v in metadata.items()
            if v is not None
        }

    if not filtered:
        return

    structlog.contextvars.bind_contextvars(**filtered)


def unset_jobmon_context(*keys: str, allow_non_jobmon_keys: bool = False) -> None:
    """Remove telemetry metadata keys from the current context.

    Keys are automatically prefixed with 'telemetry_' unless allow_non_jobmon_keys is True.

    Args:
        *keys: Keys to remove from context.
        allow_non_jobmon_keys: If True, remove keys as-is without adding telemetry_ prefix.
                              Used for server-side context propagation and decorators.
    """
    if not keys:
        return

    if allow_non_jobmon_keys:
        # Remove arbitrary keys without prefixing
        filtered: Sequence[str] = tuple(key for key in keys if key)
    else:
        # Ensure all keys have telemetry_ prefix
        filtered = tuple(
            k if k.startswith("telemetry_") else f"telemetry_{k}" for k in keys if k
        )

    if not filtered:
        return

    structlog.contextvars.unbind_contextvars(*filtered)


@contextmanager
def bind_jobmon_context(**metadata: Any) -> Iterator[None]:
    """Context manager that binds Jobmon telemetry metadata temporarily.

    All keys are automatically prefixed with 'telemetry_' if not already.
    """
    # Ensure all keys have telemetry_ prefix
    filtered = {
        (k if k.startswith("telemetry_") else f"telemetry_{k}"): v
        for k, v in metadata.items()
        if v is not None
    }

    if not filtered:
        yield
        return

    contextvars_module = structlog.contextvars
    current = contextvars_module.get_contextvars()
    previous = {k: current[k] for k in filtered if k in current}

    structlog.contextvars.bind_contextvars(**filtered)
    try:
        yield
    finally:
        structlog.contextvars.unbind_contextvars(*filtered.keys())
        if previous:
            contextvars_module.bind_contextvars(**previous)


def register_jobmon_metadata_keys(*keys: str) -> None:
    """No-op for backward compatibility.

    Metadata keys no longer need registration - any key with 'telemetry_' prefix
    is automatically treated as Jobmon telemetry metadata.
    """
