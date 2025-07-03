# jobmon/server/db/engine.py
from __future__ import annotations

import ast
import json
import logging
from collections.abc import Mapping, Sequence

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from jobmon.core.configuration import ConfigError
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db.dns import get_dns_engine

log = logging.getLogger(__name__)

_engine: Engine | None = None
_SessionMaker: sessionmaker | None = None


_TRUE = {"true", "t", "1", "yes"}
_FALSE = {"false", "f", "0", "no"}


def _coerce(val: object) -> object:
    """Recursively coerce strings to Python types.

    • 'true', 'false', etc. → bool
    • JSON / Python literals → parsed objects
    • Dict / list containers → recurse element-wise
    • Anything else → returned unchanged
    """
    # Already a non-string, recurse if container
    if isinstance(val, Mapping):
        return {k: _coerce(v) for k, v in val.items()}
    if isinstance(val, Sequence) and not isinstance(val, (str, bytes, bytearray)):
        return [_coerce(v) for v in val]

    if not isinstance(val, str):
        return val

    s_val = val.strip()
    lower_s_val = s_val.lower()

    # Cheap bool path first
    if lower_s_val in _TRUE:
        return True
    if lower_s_val in _FALSE:
        return False

    # Try JSON, then Python literal, fall back to raw string
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(s_val)
            return parsed
        except Exception:  # noqa: BLE001
            pass

    return s_val


def get_engine() -> Engine:
    """Return the lazily-initialised SQLAlchemy engine."""
    global _engine
    if _engine is not None:
        return _engine

    cfg = get_jobmon_config()
    uri = cfg.get("db", "sqlalchemy_database_uri")

    # Grab the raw value exactly once
    try:
        raw_ca = cfg.get_section("db").get("sqlalchemy_connect_args")
    except ConfigError:
        raw_ca = None

    connect_args = _coerce(raw_ca) if raw_ca else None

    # Get pool settings from configuration
    pool_kwargs = {
        "pool_recycle": cfg.get_int("db", "pool_recycle"),
        "pool_pre_ping": cfg.get_boolean("db", "pool_pre_ping"),
        "pool_timeout": cfg.get_int("db", "pool_timeout"),
    }

    log.debug("DATABASE URI: %s", uri)
    log.debug("CONNECT ARGS: %s", connect_args)
    log.debug("POOL SETTINGS: %s", pool_kwargs)

    _engine = (
        get_dns_engine(uri, connect_args=connect_args, **pool_kwargs)
        if connect_args
        else get_dns_engine(uri, **pool_kwargs)
    )

    # Instrument the engine with OpenTelemetry if enabled
    try:
        use_otel = cfg.get_boolean("otlp", "web_enabled")
        if use_otel:
            from jobmon.core.otlp import OtlpAPI

            OtlpAPI.instrument_engine(_engine)
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
