"""Utilities for structured logging with context binding."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional, TypeVar

import structlog

F = TypeVar("F", bound=Callable[..., Any])


def bind_context(*param_names: str, **renames: str) -> Callable[[F], F]:
    """Decorator to automatically bind function/method parameters to structlog context.

    This decorator extracts specified parameters from a function call and binds them
    to the structlog context for the duration of the function execution. After the
    function completes (successfully or with an exception), the context is cleaned up.

    Args:
        *param_names: Names of parameters to bind to context using their original names.
        **renames: Mapping of parameter names to custom context keys
            (e.g., ti_id="task_instance_id").

    Returns:
        Decorated function with automatic context binding.

    Example:
        >>> @bind_context("workflow_run_id", "cluster_name")
        ... def process_workflow(workflow_run_id: int, cluster_name: str):
        ...     logger.info("processing")  # Includes workflow_run_id, cluster

        >>> @bind_context(
        ...     "task_instance_id", wf_id="workflow_run_id"
        ... )
        ... def launch_task(task_instance_id: int, workflow_run_id: int):
        ...     logger.info("launching")

        >>> # Works with methods too
        ... class Service:
        ...     @bind_context("status", "timeout")
        ...     def process_status(self, status: str, timeout: int):
        ...         logger.info("processing_status")  # Includes status and timeout

        >>> # Bind nested attributes
        ... @bind_context(
        ...     task_id="task_instance.task_instance_id"
        ... )
        ... def process_task(task_instance: TaskInstance):
        ...     logger.info("processing")  # Includes task_id extracted from task_instance
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Get function signature to map args to parameter names
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Collect values to bind
            context_data = {}

            # Process positional param_names (use original parameter name as key)
            for param_name in param_names:
                value = _extract_value(bound_args.arguments, param_name)
                if value is not None:
                    context_data[param_name] = value

            # Process renamed parameters (custom context key)
            for context_key, param_path in renames.items():
                value = _extract_value(bound_args.arguments, param_path)
                if value is not None:
                    context_data[context_key] = value

            # Bind context and execute function
            if context_data:
                structlog.contextvars.bind_contextvars(**context_data)
                try:
                    return func(*args, **kwargs)
                finally:
                    # Clean up context
                    structlog.contextvars.unbind_contextvars(*context_data.keys())
            else:
                # No context to bind, just execute
                return func(*args, **kwargs)

        return wrapper  # type: ignore

    return decorator


def _extract_value(arguments: dict, param_path: str) -> Optional[Any]:
    """Extract a value from function arguments using dot notation.

    Args:
        arguments: Dictionary of bound function arguments.
        param_path: Parameter path (e.g., "task_instance" or "task_instance.task_instance_id").

    Returns:
        The extracted value, or None if not found.

    Example:
        >>> args = {"task_instance": TaskInstance(task_instance_id=123)}
        >>> _extract_value(args, "task_instance.task_instance_id")
        123
    """
    parts = param_path.split(".")
    value = arguments.get(parts[0])

    if value is None:
        return None

    # Navigate nested attributes
    for part in parts[1:]:
        try:
            value = getattr(value, part)
        except AttributeError:
            return None

    return value


def bind_method_context(*param_names: str, **renames: str) -> Callable[[F], F]:
    """Decorator for binding context in methods with self/cls awareness.

    This is an alias for bind_context that makes it explicit when decorating methods.
    The decorator automatically handles 'self' and 'cls' parameters.

    Args:
        *param_names: Names of parameters to bind (excluding self/cls).
        **renames: Mapping of parameter names to custom context keys.

    Returns:
        Decorated method with automatic context binding.

    Example:
        >>> class DistributorService:
        ...     @bind_method_context("task_instance", wf_id="workflow_run.workflow_run_id")
        ...     def launch_task_instance(self, task_instance: DistributorTaskInstance):
        ...         logger.info("launching")  # Includes task_instance and wf_id from self
    """
    return bind_context(*param_names, **renames)
