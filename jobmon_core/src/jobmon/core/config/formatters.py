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


class JobmonStructlogEventOnlyFormatter(logging.Formatter):
    """Formatter that shows traditional log format with just the event message.

    Parses structlog JSON output and extracts just the event field,
    hiding all the context/label fields. Shows timestamp, level, logger, and event.
    Context is still sent to OTLP, but console shows clean traditional log messages.

    Format: YYYY-MM-DD HH:MM:SS [LEVEL] [logger.name] Event message
    """

    def __init__(self) -> None:
        """Initialize with traditional log format."""
        super().__init__(fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        """Format record showing only the event message with standard fields."""
        try:
            import ast
            import json

            msg = record.getMessage()

            # Check if this is structlog JSON output
            if msg.startswith("{"):
                try:
                    event_dict = json.loads(msg)
                except (json.JSONDecodeError, ValueError):
                    event_dict = ast.literal_eval(msg)

                # Replace message with just the event
                record.msg = event_dict.get("event", msg)
                record.args = ()
        except Exception:
            pass

        # Format with standard fields (timestamp, level, logger, message)
        return super().format(record)
