# jobmon/server/db/engine.py
from __future__ import annotations

import logging

from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from jobmon.core.configuration import ConfigError
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db.dns import get_dns_engine

log = logging.getLogger(__name__)

_engine: Engine | None = None
_SessionMaker: sessionmaker | None = None


def get_engine() -> Engine:
    """Return the lazily-initialised SQLAlchemy engine."""
    global _engine
    if _engine is not None:
        return _engine

    cfg = get_jobmon_config()
    uri = cfg.get("db", "sqlalchemy_database_uri")

    # Get database configuration with automatic type coercion
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

        pool_kwargs = {}
        for config_key, sqlalchemy_param in pool_param_mapping.items():
            if config_key in pool_config:
                pool_kwargs[sqlalchemy_param] = pool_config[config_key]

    except (ConfigError, ValueError):
        connect_args = None
        pool_kwargs = {}

    log.debug("DATABASE URI: %s", uri)
    log.debug("CONNECT ARGS: %s", connect_args)
    log.debug("POOL SETTINGS: %s", pool_kwargs)

    # DNS-aware engine controls
    use_dns_engine = True
    dns_timeout = 12
    dns_nameservers = None
    dns_grace_ttl = 30
    dns_fallback = True
    dns_max_retries = 3
    dns_extend_grace = True

    try:
        use_dns_engine = bool(db_config.get("use_dns_engine", True))
        dns_timeout = int(db_config.get("dns_timeout", 12))
        dns_nameservers = db_config.get("dns_nameservers")
        if dns_nameservers is not None and not isinstance(dns_nameservers, list):
            dns_nameservers = None
        dns_grace_ttl = int(db_config.get("dns_grace_ttl", 30))
        dns_fallback = bool(db_config.get("dns_fallback", True))
        dns_max_retries = int(db_config.get("dns_max_retries", 3))
        dns_extend_grace = bool(db_config.get("dns_extend_grace", True))
    except Exception:
        # Keep defaults on any config parsing issue
        pass

    if use_dns_engine:
        _engine = (
            get_dns_engine(
                uri,
                connect_args=connect_args,
                dns_timeout=dns_timeout,
                dns_nameservers=dns_nameservers,
                dns_grace_ttl=dns_grace_ttl,
                dns_fallback=dns_fallback,
                dns_max_retries=dns_max_retries,
                dns_extend_grace=dns_extend_grace,
                **pool_kwargs,
            )
            if connect_args
            else get_dns_engine(
                uri,
                dns_timeout=dns_timeout,
                dns_nameservers=dns_nameservers,
                dns_grace_ttl=dns_grace_ttl,
                dns_fallback=dns_fallback,
                dns_max_retries=dns_max_retries,
                dns_extend_grace=dns_extend_grace,
                **pool_kwargs,
            )
        )
        log.info(
            "Using DNS-aware database engine (timeout=%ss, "
            "fallback=%s, retries=%s, extend_grace=%s)",
            dns_timeout,
            dns_fallback,
            dns_max_retries,
            dns_extend_grace,
        )
    else:
        _engine = (
            sqlalchemy_create_engine(uri, connect_args=connect_args, **pool_kwargs)
            if connect_args
            else sqlalchemy_create_engine(uri, **pool_kwargs)
        )
        log.info("Using standard SQLAlchemy database engine (DNS resolution disabled)")

    # Instrument the engine with OpenTelemetry if enabled
    try:
        telemetry_section = cfg.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})
        use_otel = tracing_config.get("server_enabled", False)
        if use_otel:
            from jobmon.server.web.otlp import ServerOTLPManager

            ServerOTLPManager.instrument_engine(_engine)
            log.debug("Instrumented database engine with OpenTelemetry")
    except Exception as e:
        # Don't fail engine creation if instrumentation fails
        log.warning("Failed to instrument database engine with OpenTelemetry: %s", e)

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
