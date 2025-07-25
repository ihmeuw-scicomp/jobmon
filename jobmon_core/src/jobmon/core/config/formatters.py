"""Custom logging formatters for jobmon."""

from __future__ import annotations

import logging

import structlog


class JobmonStructlogConsoleFormatter(logging.Formatter):
    """Structlog-based console formatter."""

    def __init__(self) -> None:
        """Initialize console formatter with structlog."""
        super().__init__()
        self._structlog_formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(), foreign_pre_chain=[]
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format using structlog console renderer."""
        return self._structlog_formatter.format(record)


class JobmonStructlogJSONFormatter(logging.Formatter):
    """Structlog-based JSON formatter."""

    def __init__(self) -> None:
        """Initialize JSON formatter with structlog."""
        super().__init__()
        self._structlog_formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(), foreign_pre_chain=[]
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format using structlog JSON renderer."""
        return self._structlog_formatter.format(record)
