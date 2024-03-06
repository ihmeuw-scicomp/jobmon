from typing import Any

from sqlalchemy.sql import func

from jobmon.server.web import session_factory
from jobmon.server.web.server_side_exception import ServerError


def add_time(next_report_increment: float) -> Any:
    """Adds the next report increment time, and then returns the new time."""
    session = session_factory()
    if session is not None and session.bind is not None:
        if session.bind.dialect.name == "mysql":
            add_time_func = func.ADDTIME(
                func.now(), func.SEC_TO_TIME(next_report_increment)
            )
        elif session.bind.dialect.name == "sqlite":
            add_time_func = func.datetime(
                func.now(), f"+{next_report_increment} seconds"
            )
        else:
            raise ServerError(
                f"Invalid SQL dialect. Only (mysql, sqlite) are supported. "
                f"Got {session.bind.dialect.name}"
            )
    else:
        raise ServerError("Invalid SQL session factory")

    return add_time_func


def subtract_time(next_report_increment: float) -> Any:
    """Subtracts the next report increment time, and then returns the new time."""
    session = session_factory()
    if session is not None and session.bind is not None:
        if session.bind.dialect.name == "mysql":
            sub_time_func = func.SUBTIME(
                func.now(), func.SEC_TO_TIME(next_report_increment)
            )
        elif session.bind.dialect.name == "sqlite":
            sub_time_func = func.datetime(
                func.now(), f"-{next_report_increment} seconds"
            )
        else:
            raise ServerError(
                f"Invalid SQL dialect. Only (mysql, sqlite) are supported. "
                f"Got {session.bind.dialect.name}"
            )
    else:
        raise ServerError("Invalid SQL session factory")

    return sub_time_func
