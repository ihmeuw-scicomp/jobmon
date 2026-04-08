from __future__ import annotations

import asyncio
from typing import Any, Callable

import structlog

logger = structlog.get_logger(__name__)


class DistributorCommand:
    def __init__(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """A command to be run by the distributor service.

        Args:
            func: a callable which does work and optionally modifies task instance state.
                  Can be sync or async — async callables are awaited automatically.
            *args: positional args to be passed into func.
            **kwargs: kwargs to be passed into func.
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self.error_raised = False

    async def __call__(self, raise_on_error: bool = False) -> None:
        try:
            result = self._func(*self._args, **self._kwargs)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            self.error_raised = True
            if raise_on_error:
                raise
            else:
                logger.exception(
                    "Distributor command failed",
                    command=getattr(self._func, "__qualname__", str(self._func)),
                    error_type=type(e).__name__,
                    error=str(e),
                    args=str(self._args)[:200],
                )
