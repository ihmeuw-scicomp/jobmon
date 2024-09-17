from typing import Any

from sqlalchemy.sql import func

from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.server_side_exception import ServerError

SessionLocal = get_session_local()
_CONFIG = get_jobmon_config()


def add_time(next_report_increment: float) -> Any:
    """Adds the next report increment time, and then returns the new time."""
    with SessionLocal() as session:
        if session:
            if "mysql" in _CONFIG.get("db", "sqlalchemy_database_uri"):
                add_time_func = func.ADDTIME(
                    func.now(), func.SEC_TO_TIME(next_report_increment)
                )
            elif "sqlite" in _CONFIG.get("db", "sqlalchemy_database_uri"):
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
    with SessionLocal() as session:
        if session:
            if "mysql" in _CONFIG.get("db", "sqlalchemy_database_uri"):
                sub_time_func = func.SUBTIME(
                    func.now(), func.SEC_TO_TIME(next_report_increment)
                )
            elif "sqlite" in _CONFIG.get("db", "sqlalchemy_database_uri"):
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
