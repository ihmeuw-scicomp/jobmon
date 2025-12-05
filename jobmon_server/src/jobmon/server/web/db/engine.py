# jobmon/server/db/engine.py
"""Database engine management using FastAPI lifespan pattern.

This module provides database engine creation and lifecycle management
without global singletons. The engine is stored in FastAPI's app.state
and managed through the db_lifespan context manager.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from jobmon.core.configuration import ConfigError
from jobmon.server.web.config import get_jobmon_config

if TYPE_CHECKING:
    from fastapi import FastAPI

log = logging.getLogger(__name__)


def create_engine_from_config() -> tuple[Engine, str, Dict[str, Any]]:
    """Create a SQLAlchemy engine from the current configuration.

    Returns:
        Tuple of (engine, dialect_name, config_info) where config_info contains
        the pool settings and connect args used for debugging.
    """
    cfg = get_jobmon_config()
    uri = cfg.get("db", "sqlalchemy_database_uri")

    # Get database configuration with automatic type coercion
    connect_args = None
    pool_kwargs: Dict[str, Any] = {}

    try:
        db_config = cfg.get_section_coerced("db")
        connect_args = db_config.get("sqlalchemy_connect_args")

        # Get pool settings - ensure pool_config is always a dict
        pool_config = db_config.get("pool") or {}
        if not isinstance(pool_config, dict):
            pool_config = {}

        pool_param_mapping = {
            "recycle": "pool_recycle",
            "pre_ping": "pool_pre_ping",
            "timeout": "pool_timeout",
            "size": "pool_size",
            "max_overflow": "max_overflow",
        }

        for config_key, sqlalchemy_param in pool_param_mapping.items():
            if config_key in pool_config:
                pool_kwargs[sqlalchemy_param] = pool_config[config_key]

    except (ConfigError, ValueError):
        pass

    log.debug("DATABASE URI: %s", uri)
    log.debug("CONNECT ARGS: %s", connect_args)
    log.debug("POOL SETTINGS: %s", pool_kwargs)

    engine = (
        create_engine(uri, connect_args=connect_args, **pool_kwargs)
        if connect_args
        else create_engine(uri, **pool_kwargs)
    )

    dialect_name = engine.dialect.name.lower()
    log.info("Created SQLAlchemy database engine (dialect=%s)", dialect_name)

    # Instrument the engine with OpenTelemetry if enabled
    try:
        telemetry_section = cfg.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})
        use_otel = tracing_config.get("server_enabled", False)
        if use_otel:
            from jobmon.server.web.otlp import ServerOTLPManager

            ServerOTLPManager.instrument_engine(engine)
            log.debug("Instrumented database engine with OpenTelemetry")
    except Exception as e:
        # Don't fail engine creation if instrumentation fails
        log.warning("Failed to instrument database engine with OpenTelemetry: %s", e)

    config_info = {"connect_args": connect_args, "pool_kwargs": pool_kwargs}
    return engine, dialect_name, config_info


@asynccontextmanager
async def db_lifespan(app: "FastAPI") -> AsyncIterator[None]:
    """Manage database engine lifecycle via FastAPI lifespan.

    Creates the database engine and sessionmaker on startup, stores them
    in app.state, and properly disposes of the engine on shutdown.

    Usage::

        app = FastAPI(lifespan=db_lifespan)

        # In route handlers:
        def get_db(request: Request):
            SessionLocal = request.app.state.db_sessionmaker
            ...
    """
    # Startup: create engine and sessionmaker
    engine, dialect_name, _ = create_engine_from_config()

    app.state.db_engine = engine
    app.state.db_dialect = dialect_name
    app.state.db_sessionmaker = sessionmaker(
        bind=engine, autoflush=False, autocommit=False
    )

    log.info("Database engine initialized (dialect=%s)", dialect_name)

    yield  # Application runs here

    # Shutdown: dispose of engine
    log.info("Disposing database engine")
    engine.dispose()


# =============================================================================
# Helper functions for routes that need dialect info
# =============================================================================
# These are convenience functions that can be used when you have access to
# the app or request object. For route handlers, prefer using get_dialect()
# from deps.py which uses dependency injection.


def is_mysql_dialect(dialect: str) -> bool:
    """Check if the dialect is MySQL."""
    return dialect == "mysql"


def is_sqlite_dialect(dialect: str) -> bool:
    """Check if the dialect is SQLite."""
    return dialect == "sqlite"
