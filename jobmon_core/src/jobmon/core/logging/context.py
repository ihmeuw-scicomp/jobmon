"""Telemetry context helpers for Jobmon structlog instrumentation.

All Jobmon telemetry metadata is namespaced with the ``telemetry_`` prefix.
This clearly indicates data that is exported to OTLP but stripped from
console output, without requiring explicit key registries.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, Mapping, Sequence

import structlog

# Ensure structlog contextvars support is available.
if not hasattr(structlog, "contextvars") or not hasattr(
    structlog.contextvars, "get_contextvars"
):  # pragma: no cover - import-time check
    raise RuntimeError(
        "structlog.contextvars must provide get_contextvars() for Jobmon telemetry"
    )


_TELEMETRY_PREFIX = "telemetry_"


def _normalize_context_metadata(
    metadata: Mapping[str, Any], *, allow_non_jobmon_keys: bool
) -> Dict[str, Any]:
    """Filter ``None`` values and apply telemetry prefixing rules."""
    normalized: Dict[str, Any] = {}

    for raw_key, value in metadata.items():
        if value is None:
            continue

        key = _normalize_context_key(
            raw_key, allow_non_jobmon_keys=allow_non_jobmon_keys
        )
        if key:
            normalized[key] = value

    return normalized


def _normalize_context_keys(
    keys: Iterable[str], *, allow_non_jobmon_keys: bool
) -> Sequence[str]:
    """Apply telemetry prefixing rules to context keys."""
    normalized: list[str] = []

    for raw_key in keys:
        if not raw_key:
            continue

        key = _normalize_context_key(
            raw_key, allow_non_jobmon_keys=allow_non_jobmon_keys
        )
        if key:
            normalized.append(key)

    return tuple(normalized)


def _normalize_context_key(key: str, *, allow_non_jobmon_keys: bool) -> str:
    """Return a context key that honours telemetry prefixing rules."""
    if allow_non_jobmon_keys or key.startswith(_TELEMETRY_PREFIX):
        return key

    return f"{_TELEMETRY_PREFIX}{key}"


def get_jobmon_context() -> Dict[str, Any]:
    """Return a copy of all active Jobmon telemetry metadata.

    Returns all context variables with the ``telemetry_`` prefix.
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

    All keys are automatically prefixed with ``telemetry_`` unless
    allow_non_jobmon_keys is True.

    Args:
        allow_non_jobmon_keys: **INTERNAL USE ONLY**. If True, bind keys as-is
            without adding ``telemetry_`` prefix. Used internally by the
            bind_context decorator and server middleware. External callers
            should not use this flag.
        **metadata: Key-value pairs to bind to context.
    """
    filtered = _normalize_context_metadata(
        metadata, allow_non_jobmon_keys=allow_non_jobmon_keys
    )

    if not filtered:
        return

    structlog.contextvars.bind_contextvars(**filtered)


def unset_jobmon_context(*keys: str, allow_non_jobmon_keys: bool = False) -> None:
    """Remove telemetry metadata keys from the current context.

    Keys are automatically prefixed with ``telemetry_`` unless
    allow_non_jobmon_keys is True.

    Args:
        *keys: Keys to remove from context.
        allow_non_jobmon_keys: **INTERNAL USE ONLY**. If True, remove keys as-is
            without adding ``telemetry_`` prefix. Used internally by the
            bind_context decorator and server middleware. External callers
            should not use this flag.
    """
    if not keys:
        return

    filtered = _normalize_context_keys(
        keys, allow_non_jobmon_keys=allow_non_jobmon_keys
    )

    if not filtered:
        return

    structlog.contextvars.unbind_contextvars(*filtered)


@contextmanager
def bind_jobmon_context(**metadata: Any) -> Iterator[None]:
    """Context manager that binds Jobmon telemetry metadata temporarily.

    All keys are automatically prefixed with ``telemetry_`` if not already.
    """
    filtered = _normalize_context_metadata(metadata, allow_non_jobmon_keys=False)

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
