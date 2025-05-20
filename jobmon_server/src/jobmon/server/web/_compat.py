from typing import Any

from sqlalchemy.sql import func

from jobmon.server.web.db import get_dialect_name, get_sessionmaker
from jobmon.server.web.server_side_exception import ServerError

SessionMaker = get_sessionmaker()
DIALECT = get_dialect_name()


def add_time(next_report_increment: float) -> Any:
    """Adds the next report increment time, and then returns the new time."""
    with SessionMaker() as session:
        if session:
            if DIALECT == "mysql":
                add_time_func = func.ADDTIME(
                    func.now(), func.SEC_TO_TIME(next_report_increment)
                )
            elif DIALECT == "sqlite":
                add_time_func = func.datetime(
                    func.now(), f"+{next_report_increment} seconds"
                )
            else:
                raise ServerError(
                    f"Invalid SQL dialect. Only (mysql, sqlite) are supported. "
                    f"Got {DIALECT}"
                )
        else:
            raise ServerError("Invalid SQL session factory")

        return add_time_func


def subtract_time(next_report_increment: float) -> Any:
    """Subtracts the next report increment time, and then returns the new time."""
    with SessionMaker() as session:
        if session:
            if DIALECT == "mysql":
                sub_time_func = func.SUBTIME(
                    func.now(), func.SEC_TO_TIME(next_report_increment)
                )
            elif DIALECT == "sqlite":
                sub_time_func = func.datetime(
                    func.now(), f"-{next_report_increment} seconds"
                )
            else:
                raise ServerError(
                    f"Invalid SQL dialect. Only (mysql, sqlite) are supported. "
                    f"Got {DIALECT}"
                )
        else:
            raise ServerError("Invalid SQL session factory")

        return sub_time_func
