"""Telemetry context helpers for Jobmon structlog instrumentation."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Mapping, Sequence, Set

import structlog

# Ensure structlog contextvars support is available.
if not hasattr(structlog, "contextvars") or not hasattr(
    structlog.contextvars, "get_contextvars"
):  # pragma: no cover - import-time check
    raise RuntimeError(
        "structlog.contextvars must provide get_contextvars() for Jobmon telemetry"
    )

# Metadata keys that Jobmon treats as telemetry fields.
JOBMON_METADATA_KEYS: Set[str] = {
    "array_id",
    "batch_number",
    "cluster_name",
    "dag_hash",
    "dag_id",
    "error",
    "method",
    "path",
    "request_id",
    "task_hash",
    "task_id",
    "task_instance_id",
    "task_resources_id",
    "task_template_id",
    "task_template_version_id",
    "tool_id",
    "tool_version_id",
    "workflow_args_hash",
    "workflow_id",
    "workflow_run_id",
}

_METADATA_LOCK = threading.RLock()


def get_jobmon_context() -> Dict[str, Any]:
    """Return a copy of the active Jobmon telemetry metadata."""
    ctx = structlog.contextvars.get_contextvars()
    with _METADATA_LOCK:
        keys = tuple(JOBMON_METADATA_KEYS)
    return {k: ctx[k] for k in keys if k in ctx}


def clear_jobmon_context() -> None:
    """Remove all Jobmon telemetry metadata from the current context."""
    metadata = get_jobmon_context()
    if metadata:
        structlog.contextvars.unbind_contextvars(*metadata.keys())


def set_jobmon_context(*, allow_non_jobmon_keys: bool = False, **metadata: Any) -> None:
    """Bind telemetry metadata to the current structlog context."""
    if allow_non_jobmon_keys:
        filtered = {k: v for k, v in metadata.items() if v is not None}
    else:
        filtered = _filter_jobmon_metadata(metadata)

    if not filtered:
        return

    structlog.contextvars.bind_contextvars(**filtered)


def unset_jobmon_context(*keys: str, allow_non_jobmon_keys: bool = False) -> None:
    """Remove telemetry metadata keys from the current context."""
    if not keys:
        return

    if allow_non_jobmon_keys:
        filtered: Sequence[str] = tuple(key for key in keys if key)
    else:
        with _METADATA_LOCK:
            allowed = set(JOBMON_METADATA_KEYS)
        filtered = tuple(key for key in keys if key in allowed)

    if not filtered:
        return

    structlog.contextvars.unbind_contextvars(*filtered)


@contextmanager
def bind_jobmon_context(**metadata: Any) -> Iterator[None]:
    """Context manager that binds Jobmon telemetry metadata temporarily."""
    filtered = _filter_jobmon_metadata(metadata)

    if not filtered:
        yield
        return

    contextvars_module = structlog.contextvars
    current = contextvars_module.get_contextvars()
    previous = {k: current[k] for k in filtered if k in current}

    set_jobmon_context(**filtered)
    try:
        yield
    finally:
        unset_jobmon_context(*filtered.keys())
        if previous:
            contextvars_module.bind_contextvars(**previous)


def register_jobmon_metadata_keys(*keys: str) -> None:
    """Register additional telemetry metadata keys at runtime."""
    if not keys:
        return

    with _METADATA_LOCK:
        JOBMON_METADATA_KEYS.update(keys)


def _filter_jobmon_metadata(metadata: Mapping[str, Any]) -> Dict[str, Any]:
    with _METADATA_LOCK:
        allowed = set(JOBMON_METADATA_KEYS)
    return {k: v for k, v in metadata.items() if k in allowed}
