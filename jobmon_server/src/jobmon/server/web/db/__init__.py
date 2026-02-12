# jobmon/server/db/__init__.py
"""Database module for Jobmon server.

This module provides:
- Database engine lifecycle management via db_lifespan
- FastAPI dependency injection for sessions via get_db
- Migration utilities via init_db, apply_migrations
"""
from jobmon.server.web.db.deps import DB, Dialect, get_db, get_dialect
from jobmon.server.web.db.engine import (
    create_engine_from_config,
    db_lifespan,
    is_mysql_dialect,
    is_sqlite_dialect,
)
from jobmon.server.web.db.migrate import apply_migrations, init_db, terminate_db

__all__ = [
    # Lifespan management
    "db_lifespan",
    "create_engine_from_config",
    # FastAPI dependencies
    "get_db",
    "get_dialect",
    "DB",
    "Dialect",
    # Dialect helpers
    "is_mysql_dialect",
    "is_sqlite_dialect",
    # Migration utilities
    "apply_migrations",
    "init_db",
    "terminate_db",
]
