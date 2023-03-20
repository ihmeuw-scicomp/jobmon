from sqlalchemy.sql import func

from jobmon.server.web import session_factory
from jobmon.server.web.server_side_exception import ServerError


def add_time(next_report_increment: float) -> func:
    """Adds the next report increment time, and then returns the new time."""
    if session_factory().bind.dialect.name == "mysql":
        add_time_func = func.ADDTIME(
            func.now(), func.SEC_TO_TIME(next_report_increment)
        )
    elif session_factory().bind.dialect.name == "sqlite":
        add_time_func = func.datetime(func.now(), f"+{next_report_increment} seconds")

    else:
        raise ServerError(
            "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
            + session_factory.bind.dialect.name
        )
    return add_time_func


def subtract_time(next_report_increment: float) -> func:
    """Subtracts the next report increment time, and then returns the new time."""
    if session_factory().bind.dialect.name == "mysql":
        add_time_func = func.SUBTIME(
            func.now(), func.SEC_TO_TIME(next_report_increment)
        )
    elif session_factory().bind.dialect.name == "sqlite":
        add_time_func = func.datetime(func.now(), f"-{next_report_increment} seconds")

    else:
        raise ServerError(
            "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
            + session_factory.bind.dialect.name
        )
    return add_time_func
