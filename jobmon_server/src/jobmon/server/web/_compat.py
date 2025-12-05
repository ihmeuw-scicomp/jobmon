"""Compatibility utilities for time-based SQL operations.

This module provides dialect-aware SQL functions for time manipulation.
The functions accept a dialect parameter to support proper operation
with the lifespan-based database configuration.
"""

from typing import Any

from sqlalchemy.sql import func

from jobmon.server.web.server_side_exception import ServerError


def add_time(next_report_increment: float, dialect: str) -> Any:
    """Create a SQL expression to add time to the current timestamp.

    Args:
        next_report_increment: Number of seconds to add
        dialect: The database dialect (mysql, sqlite)

    Returns:
        A SQLAlchemy expression appropriate for the dialect

    Raises:
        ServerError: If the dialect is not supported
    """
    if dialect == "mysql":
        return func.ADDTIME(func.now(), func.SEC_TO_TIME(next_report_increment))
    if dialect == "sqlite":
        return func.datetime(func.now(), f"+{next_report_increment} seconds")
    raise ServerError(
        f"Invalid SQL dialect. Only (mysql, sqlite) are supported. Got {dialect}"
    )


def subtract_time(next_report_increment: float, dialect: str) -> Any:
    """Create a SQL expression to subtract time from the current timestamp.

    Args:
        next_report_increment: Number of seconds to subtract
        dialect: The database dialect (mysql, sqlite)

    Returns:
        A SQLAlchemy expression appropriate for the dialect

    Raises:
        ServerError: If the dialect is not supported
    """
    if dialect == "mysql":
        return func.SUBTIME(func.now(), func.SEC_TO_TIME(next_report_increment))
    if dialect == "sqlite":
        return func.datetime(func.now(), f"-{next_report_increment} seconds")
    raise ServerError(
        f"Invalid SQL dialect. Only (mysql, sqlite) are supported. Got {dialect}"
    )
