"""Custom logging formatters for jobmon."""

from __future__ import annotations

import logging
from typing import Union

try:
    import structlog
    from structlog.stdlib import ProcessorFormatter

    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

    # Create dummy class for type hints when structlog is not available
    class ProcessorFormatter:  # type: ignore
        pass


class JobmonStructlogConsoleFormatter(logging.Formatter):
    """Structlog-based console formatter with fallback to basic logging."""

    def __init__(self) -> None:
        """Initialize console formatter with structlog or fallback."""
        super().__init__()

        if STRUCTLOG_AVAILABLE:
            # Create the structlog formatter with console renderer
            self._structlog_formatter: Union[ProcessorFormatter, logging.Formatter] = (
                structlog.stdlib.ProcessorFormatter(
                    processor=structlog.dev.ConsoleRenderer(), foreign_pre_chain=[]
                )
            )
        else:
            # Fall back to basic formatter if structlog not available
            self._structlog_formatter = logging.Formatter(
                "%(levelname)s [%(name)s] %(message)s"
            )

    def format(self, record: logging.LogRecord) -> str:
        """Format using structlog console renderer."""
        return self._structlog_formatter.format(record)


class JobmonStructlogJSONFormatter(logging.Formatter):
    """Structlog-based JSON formatter with fallback to basic JSON logging."""

    def __init__(self) -> None:
        """Initialize JSON formatter with structlog or fallback."""
        super().__init__()

        if STRUCTLOG_AVAILABLE:
            # Create the structlog formatter with JSON renderer
            self._structlog_formatter: Union[ProcessorFormatter, logging.Formatter] = (
                structlog.stdlib.ProcessorFormatter(
                    processor=structlog.processors.JSONRenderer(), foreign_pre_chain=[]
                )
            )
        else:
            # Fall back to basic JSON formatter if structlog not available
            json_format = (
                '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                '"logger": "%(name)s", "message": "%(message)s"}'
            )
            self._structlog_formatter = logging.Formatter(json_format)

    def format(self, record: logging.LogRecord) -> str:
        """Format using structlog JSON renderer."""
        return self._structlog_formatter.format(record)
