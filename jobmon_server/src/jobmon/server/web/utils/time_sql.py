"""Time-based SQL expression utilities."""

from typing import Any

from sqlalchemy.sql import func

from jobmon.server.web.db import get_dialect_name
from jobmon.server.web.server_side_exception import ServerError

DIALECT = get_dialect_name()


def add_time_expr(delta: float) -> Any:
    """Create a SQL expression to add time (in seconds) to the current timestamp.

    Args:
        delta: Number of seconds to add

    Returns:
        A SQLAlchemy expression appropriate for the current dialect

    Raises:
        ServerError: If the dialect is not supported
    """
    if DIALECT == "mysql":
        return func.ADDTIME(func.now(), func.SEC_TO_TIME(delta))
    if DIALECT == "sqlite":
        return func.datetime(func.now(), f"+{delta} seconds")
    raise ServerError(f"Unsupported SQL dialect '{DIALECT}'")


def subtract_time_expr(delta: float) -> Any:
    """Create a SQL expression to subtract time (in seconds) from the current timestamp.

    Args:
        delta: Number of seconds to subtract

    Returns:
        A SQLAlchemy expression appropriate for the current dialect

    Raises:
        ServerError: If the dialect is not supported
    """
    if DIALECT == "mysql":
        return func.SUBTIME(func.now(), func.SEC_TO_TIME(delta))
    if DIALECT == "sqlite":
        return func.datetime(func.now(), f"-{delta} seconds")
    raise ServerError(f"Unsupported SQL dialect '{DIALECT}'")
