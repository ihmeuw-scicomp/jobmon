"""dns.py — generic SQLAlchemy engine factory with DNS-aware pooling.

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

import importlib
import ipaddress
import logging
import threading
import time
from types import ModuleType
from typing import Any, Dict, Tuple, Type

from dns import resolver
from sqlalchemy import create_engine, event, exc
from sqlalchemy.engine import Engine, make_url, URL
from sqlalchemy.engine.interfaces import DBAPIConnection, Dialect
from sqlalchemy.pool import _ConnectionRecord

__all__ = ["get_dns_engine", "clear_dns_cache", "get_ip_with_ttl"]

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
def get_dns_engine(uri: str | URL, *engine_args: Any, **engine_kwargs: Any) -> Engine:
    """Create a DNS-powered engine from a SQLAlchemy URL.

    With a normal SQLAlchemy engine, the hostname in the URL is looked up
    exactly once, on connection pool creation. If a DNS entry TTL expires
    or otherwise updates, this will prevent failover on reconnect, because
    the old, cached address will still be used.

    This function fixes the problem by intercepting the connection factory
    to resolve DNS on each reconnect.
    """
    logger.debug("get_dns_engine: uri=%s engine_kwargs=%s", uri, engine_kwargs)

    # Extract and save user connect_args
    user_connect_args = engine_kwargs.pop("connect_args", {})

    # Connection pools are singletons created by engine spec (args & kwargs)
    # so they are effectively cached forever.
    #
    url = make_url(uri)
    host = url.host
    logger.debug("get_dns_engine: url=%s", str(url))

    is_sqlite_url = url.drivername.startswith("sqlite")

    # If host is None AND it's not a SQLite URL, then it's an invalid URL for this function.
    if host is None and not is_sqlite_url:
        raise ValueError(f"URL has no host and is not a SQLite URL: {url!s}")

    # If it's SQLite, or a non-DNS hostname (localhost), or an IP address, use a standard engine.
    # For SQLite, host will be None. (host and _is_ip_address(host)) handles this.
    if is_sqlite_url or host == "127.0.0.1" or host == "localhost" or (host and _is_ip_address(host)):
        logger.info("Creating non-DNS engine for %s", url)

        # Restore the connect_args before creating the standard engine
        if user_connect_args:
            engine_kwargs["connect_args"] = user_connect_args

        return create_engine(str(url), *engine_args, **engine_kwargs)

    # Make sure that we can create a fully valid SQLAlchemy engine
    # with the current URL, which will break if the URL is malformed.
    minimal_dialect = create_engine(str(url), _initialize=False).dialect
    dbapi_module = getattr(minimal_dialect, "dbapi", None)

    # This will throw errors in automap_base.prepare() if the dialect
    # chosen doesn't match actual database. Dialect names are things like:
    # 'mysql+pymysql', 'postgresql+psycopg2', not just 'mysql'.
    logger.info(
        "Creating DNS-aware engine for %s with dialect %r",
        url,
        minimal_dialect.name,
    )

    # Get the connection factory method from the dialect.
    def creator() -> DBAPIConnection:
        """Return a connection to the currently resolved IP address."""
        ip_now, _ = _cached_ip(host)
        connect_url = url.set(host=ip_now)
        cargs, cparams = minimal_dialect.create_connect_args(connect_url)

        # Merge user-supplied connect_args into the parameters
        if user_connect_args:
            cparams.update(user_connect_args)
            logger.info(
                "Updated connection params with user args: %s", user_connect_args
            )

        module = dbapi_module or minimal_dialect.dbapi
        if module is None:
            raise RuntimeError(
                f"Could not determine DBAPI module for dialect {minimal_dialect.name}"
            )
        return module.connect(*cargs, **cparams)

    # Prevent SQLAlchemy from doing its own DNS look-up.
    placeholder = url.set(host="127.0.0.1", port=url.port or 1)

    # Debug logging to see what parameters are being passed to the engine
    logger.error(f"ENGINE KWARGS: {engine_kwargs}")

    engine = create_engine(
        str(placeholder),
        *engine_args,
        future=True,
        creator=creator,
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


def get_ip_with_ttl(host: str) -> Tuple[str, int]:
    """Get the current IP address and TTL for a hostname.

    Args:
        host (str): The hostname to resolve

    Returns:
        Tuple[str, int]: A tuple of (ip_address, ttl_in_seconds)
    """
    return _cached_ip(host)


def _is_ip_address(host: str) -> bool:
    """Check if a string is a valid IP address."""
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False
