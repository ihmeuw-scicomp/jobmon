from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


class DistributorCommand:
    def __init__(self, func: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        """A command to be run by the distributor service.

        Args:
            func: a callable which does work and optionally modifies task instance state.
            *args: positional args to be passed into func.
            **kwargs: kwargs to be passed into func.
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self.error_raised = False

    def __call__(self, raise_on_error: bool = False) -> None:
        try:
            self._func(*self._args, **self._kwargs)
        except Exception as e:
            if raise_on_error:
                raise
            else:
                logger.warning(e)
