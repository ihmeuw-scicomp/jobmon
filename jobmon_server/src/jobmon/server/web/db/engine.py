# jobmon/server/db/engine.py
from __future__ import annotations

import logging

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db.dns import get_dns_engine

log = logging.getLogger(__name__)

_engine: Engine | None = None
_SessionMaker: sessionmaker | None = None


def get_engine() -> Engine:
    """Get the SQLAlchemy engine singleton."""
    global _engine
    if _engine is None:
        cfg = get_jobmon_config()
        _engine = get_dns_engine(cfg.get("db", "sqlalchemy_database_uri"))
    return _engine


def get_sessionmaker() -> sessionmaker:
    """Get the SQLAlchemy sessionmaker singleton."""
    global _SessionMaker
    if _SessionMaker is None:
        _SessionMaker = sessionmaker(
            bind=get_engine(), autoflush=False, autocommit=False
        )
    return _SessionMaker


# helpers for tests
def _reset_singletons() -> None:  # called by tests that patch JobmonConfig
    global _engine, _SessionMaker
    _engine = _SessionMaker = None


def get_dialect_name() -> str:
    """Lower-case dialect string, e.g. 'mysql', 'sqlite', 'postgresql'."""
    return get_engine().dialect.name.lower()


def is_mysql() -> bool:
    """Check if the current database dialect is MySQL."""
    return get_dialect_name() == "mysql"


def is_sqlite() -> bool:
    """Check if the current database dialect is SQLite."""
    return get_dialect_name() == "sqlite"
