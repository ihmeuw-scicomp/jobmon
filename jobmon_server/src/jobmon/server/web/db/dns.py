"""dns_pool.py — generic SQLAlchemy engine factory with DNS-aware pooling.

`get_dns_engine()` behaves exactly like :pyfunc:`sqlalchemy.create_engine`, **but**
ensures that every connection the pool hands out is opened against the *current*
IP address for the hostname in your database URL.  When the DNS A-record
changes, stale sockets are invalidated and new ones are created transparently.

Key points:
~~~~~~~~~~~
* Works with any **synchronous** SQLAlchemy dialect – no hard dependency on a
  particular DB-API.
* Accepts the full argument surface of :pyfunc:`sqlalchemy.create_engine`.
  The function injects its own `creator`, `pool_pre_ping`, and `pool_recycle`
  arguments; supplying *your own* `creator` will raise a clear
  `ValueError`, because a custom creator would defeat the point of DNS-aware
  pooling.
* Opens **zero** extra database connections while building the engine.
* Includes ``clear_dns_cache()`` for tests.

Example:
~~~~~~~~
>>> from dns_pool import get_dns_engine, clear_dns_cache
>>> engine = get_dns_engine(
...     "postgresql+psycopg2://user:pass@db.internal/mydb",
...     echo=True, pool_size=10, pool_timeout=20
... )

If the host's IP address changes at runtime, the very next checkout from the
pool raises ``DisconnectionError``; SQLAlchemy automatically retries and a new
connection is opened to the fresh IP.
"""

from __future__ import annotations

import functools
import importlib
import logging
import threading
import time
from types import ModuleType
from typing import Any, Dict, Tuple, Type

from dns import resolver
from sqlalchemy import create_engine, event, exc
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.interfaces import DBAPIConnection, Dialect
from sqlalchemy.pool import _ConnectionRecord

__all__ = ["get_dns_engine", "clear_dns_cache"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DNS-resolution helpers
# ---------------------------------------------------------------------------
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}
_CACHE_LOCK = threading.RLock()
_DEFAULT_MAX_TTL = 300  # seconds


def _resolve(host: str) -> Tuple[str, int]:
    """Return ``(ip, ttl_seconds)`` – fail fast (2 s timeout)."""
    ans = resolver.resolve(host, "A", lifetime=2)
    ttl = getattr(ans.rrset, "ttl", None) or _DEFAULT_MAX_TTL
    return ans[0].address, int(ttl)


def _cached_ip(host: str) -> Tuple[str, int]:
    now = time.time()
    with _CACHE_LOCK:
        ip, exp = _DNS_CACHE.get(host, (None, 0.0))
        if ip and exp > now:
            return ip, int(exp - now)

    try:
        ip, ttl = _resolve(host)
    except Exception as err:  # pragma: no cover
        logger.warning("DNS resolve failed for %s: %s", host, err, exc_info=err)
        if ip:
            return ip, 30  # grace period
        raise

    with _CACHE_LOCK:
        _DNS_CACHE[host] = (ip, now + min(ttl, _DEFAULT_MAX_TTL))
    return ip, ttl


def clear_dns_cache() -> None:
    """Flush the local DNS cache (useful in unit tests)."""
    with _CACHE_LOCK:
        _DNS_CACHE.clear()


# ---------------------------------------------------------------------------
# Dialect helpers – *zero* extra DB connections
# ---------------------------------------------------------------------------
_DIALECT_CACHE: Dict[str, Type[Dialect]] = {}


def _get_dialect_cls(url: URL) -> Type[Dialect]:
    if url.drivername not in _DIALECT_CACHE:
        _DIALECT_CACHE[url.drivername] = url.get_dialect()  # no I/O
    return _DIALECT_CACHE[url.drivername]


def _import_explicit_driver(drivername: str) -> ModuleType | None:
    if "+" not in drivername:
        return None
    _dialect, driver = drivername.split("+", 1)
    try:
        return importlib.import_module(driver)
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            f"Requested driver '{driver}' for dialect '{_dialect}' is not installed"
        ) from exc


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------
@functools.lru_cache(maxsize=4)
def get_dns_engine(  # noqa: C901
    uri: str | URL, *engine_args: Any, **engine_kwargs: Any
) -> Engine:
    """Return a SQLAlchemy ``Engine`` that respects dynamic DNS.

    Parameters
    ----------
    uri : str | sqlalchemy.engine.URL
        Full database URL.
    *engine_args / **engine_kwargs
        Forwarded verbatim to :pyfunc:`sqlalchemy.create_engine` *except* the
        reserved keywords ``creator``, ``pool_pre_ping`` and ``pool_recycle``,
        which are managed internally.  Passing ``creator`` will raise
        ``ValueError``.
    """
    url: URL = URL.create(uri) if isinstance(uri, str) else uri

    # SQLite – passthrough - use the original URI string directly
    if url.drivername.startswith("sqlite"):
        # str(url) can mangle absolute paths, pass the original uri string
        return create_engine(uri, *engine_args, future=True, **engine_kwargs)

    host = url.host
    if host is None:
        raise ValueError("URI must include a hostname for DNS-aware pooling")

    if "creator" in engine_kwargs:
        raise ValueError("'creator' cannot be supplied – it would bypass DNS logic")

    # Reasonable defaults that the caller may override *except* creator
    _, ttl = _cached_ip(host)
    engine_kwargs.setdefault("pool_pre_ping", True)
    engine_kwargs.setdefault("pool_recycle", max(min(ttl, 60), 30))

    dbapi_module = _import_explicit_driver(url.drivername)
    dialect_cls = _get_dialect_cls(url)
    minimal_dialect = dialect_cls()

    # ---------------------------------------------------------------
    # Creator (runs for each new physical connection)
    # ---------------------------------------------------------------
    def creator() -> DBAPIConnection:  # noqa: D401
        ip_now, _ = _cached_ip(host)
        connect_url = url.set(host=ip_now)
        cargs, cparams = minimal_dialect.create_connect_args(connect_url)
        module = dbapi_module or minimal_dialect.dbapi
        if module is None:
            raise RuntimeError(
                f"Could not determine DBAPI module for dialect '{url.drivername}'. "
                f"Ensure the driver is installed or explicitly specified in the URL "
                f"(e.g., 'postgresql+psycopg2')."
            )
        return module.connect(*cargs, **cparams)

    engine_kwargs["creator"] = creator

    # Prevent SQLAlchemy from doing its own DNS look-up.
    placeholder = url.set(host="127.0.0.1", port=url.port or 1)

    engine = create_engine(
        str(placeholder),
        *engine_args,
        future=True,
        **engine_kwargs,
    )

    # ---------------------------------------------------------------
    # Pool listeners: invalidate when the A-record changes
    # ---------------------------------------------------------------
    @event.listens_for(engine, "connect")
    def _store_peer_ip(
        dbapi_conn: DBAPIConnection, record: _ConnectionRecord
    ) -> None:  # type: ignore[func-returns-value]
        record.info["peer_ip"] = _cached_ip(host)[0]

    @event.listens_for(engine, "checkout")
    def _ensure_ip_fresh(
        dbapi_conn: DBAPIConnection, record: _ConnectionRecord, proxy: Any
    ) -> None:  # type: ignore[func-returns-value]
        current_ip = _cached_ip(host)[0]
        if record.info.get("peer_ip") != current_ip:
            record.invalidate()
            raise exc.DisconnectionError(
                f"DNS changed; reconnecting ({record.info.get('peer_ip')} → {current_ip})"
            )

    return engine
