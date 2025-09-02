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
  The function injects its own `creator` for DNS-aware pooling; supplying
  *your own* `creator` will raise a clear `ValueError`, because a custom
  creator would defeat the point of DNS-aware pooling.
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
import random
import threading
import time
from types import ModuleType
from typing import Any, Dict, Optional, Tuple, Type

from dns import resolver
from sqlalchemy import create_engine, event, exc
from sqlalchemy.engine import URL, Engine, make_url
from sqlalchemy.engine.interfaces import DBAPIConnection, Dialect
from sqlalchemy.pool import _ConnectionRecord

__all__ = ["get_dns_engine", "clear_dns_cache", "get_ip_with_ttl"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DNS-resolution helpers
# ---------------------------------------------------------------------------
_DNS_CACHE: Dict[str, Tuple[str, float, int]] = (
    {}
)  # host -> (ip, expiry, failure_count)
_CACHE_LOCK = threading.RLock()
_RESOLVER_POOL = threading.local()  # Thread-local resolver instances
_DEFAULT_MAX_TTL = 300  # seconds


def _get_thread_local_resolver(
    nameservers: Optional[list[str]] = None,
) -> resolver.Resolver:
    """Get a thread-local DNS resolver instance to avoid recreation overhead.

    This reduces the overhead of creating new resolver instances for every DNS query
    and provides more consistent behavior under high concurrency.
    """
    if not hasattr(_RESOLVER_POOL, "resolver") or not hasattr(
        _RESOLVER_POOL, "nameservers"
    ):
        r = resolver.Resolver()  # honors /etc/resolv.conf by default
        if nameservers:
            r.nameservers = list(nameservers)
        # Configure for better performance under load
        r.timeout = 3  # Per-server timeout (reduced from default)
        r.lifetime = 12  # Total query timeout
        _RESOLVER_POOL.resolver = r
        _RESOLVER_POOL.nameservers = nameservers
    elif _RESOLVER_POOL.nameservers != nameservers:
        # Nameservers changed, update resolver
        if nameservers:
            _RESOLVER_POOL.resolver.nameservers = list(nameservers)
        else:
            # Reset to system defaults
            _RESOLVER_POOL.resolver = resolver.Resolver()
        _RESOLVER_POOL.nameservers = nameservers

    return _RESOLVER_POOL.resolver


def _resolve_with_retry(
    host: str,
    timeout_seconds: int,
    nameservers: Optional[list[str]] = None,
    max_retries: int = 3,
) -> Tuple[str, int]:
    """Resolve host with exponential backoff retry logic.

    This addresses transient DNS failures by implementing:
    - Exponential backoff with jitter
    - Thread-local resolver instances
    - Configurable retry attempts
    """
    r = _get_thread_local_resolver(nameservers)

    last_exception = None
    for attempt in range(max_retries):
        try:
            ans = r.resolve(host, "A", lifetime=timeout_seconds, search=False)
            ttl = getattr(ans.rrset, "ttl", None) or _DEFAULT_MAX_TTL
            if attempt > 0:
                logger.info(
                    f"DNS resolution succeeded for {host} on retry {attempt + 1}/{max_retries}"
                )
            return ans[0].address, int(ttl)
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                # Exponential backoff with jitter to avoid thundering herd
                delay = (2**attempt) * 0.1 + random.uniform(0, 0.1)
                logger.debug(
                    f"DNS retry {attempt + 1}/{max_retries} for {host} "
                    f"after {delay:.2f}s (error: {e})"
                )
                time.sleep(delay)
            else:
                logger.warning(
                    f"DNS resolution failed for {host} after {max_retries} attempts: {e}"
                )

    if last_exception is not None:
        raise last_exception
    else:
        raise RuntimeError("DNS resolution failed for unknown reason")


def _resolve(
    host: str, timeout_seconds: int, nameservers: list[str] | None
) -> Tuple[str, int]:
    """Resolve host to (ip, ttl_seconds) with configurable timeout and nameservers.

    Uses a single total lifetime budget; dnspython will iterate configured
    nameservers within that lifetime. Search domains are disabled.

    This function now uses the enhanced retry logic by default.
    """
    return _resolve_with_retry(host, timeout_seconds, nameservers, max_retries=3)


def _cached_ip(
    host: str,
    dns_timeout: int = 12,
    dns_nameservers: list[str] | None = None,
    dns_grace_ttl: int = 30,
    dns_max_retries: int = 3,
    dns_extend_grace: bool = True,
) -> Tuple[str, int]:
    """Enhanced DNS resolution with improved caching and retry logic.

    New features:
    - Retry with exponential backoff
    - Extended grace period on repeated failures
    - Thread-local resolver instances
    - Failure count tracking
    """
    now = time.time()

    # Check cache first
    with _CACHE_LOCK:
        cache_entry = _DNS_CACHE.get(host, (None, 0.0, 0))
        cached_ip, exp, failure_count = cache_entry

        if cached_ip and exp > now:
            return cached_ip, int(exp - now)

    # Attempt DNS resolution with retry
    try:
        ip, ttl = _resolve_with_retry(
            host,
            timeout_seconds=dns_timeout,
            nameservers=dns_nameservers,
            max_retries=dns_max_retries,
        )

        # Success - reset failure count and cache the result
        with _CACHE_LOCK:
            _DNS_CACHE[host] = (ip, now + min(ttl, _DEFAULT_MAX_TTL), 0)

        if failure_count > 0:
            logger.info(
                f"DNS resolution recovered for {host} -> {ip} (TTL: {ttl}s) "
                f"after {failure_count} failures"
            )
        else:
            logger.debug(f"DNS resolved {host} -> {ip} (TTL: {ttl}s)")
        return ip, ttl

    except Exception as err:
        logger.warning(f"DNS resolve failed for {host}: {err}", exc_info=True)

        # Enhanced fallback logic
        if cached_ip:
            # Calculate extended grace period based on failure history
            base_grace = dns_grace_ttl
            if dns_extend_grace and failure_count > 0:
                # Extend grace period for repeated failures (max 5 minutes)
                extended_grace = min(base_grace * (2 ** min(failure_count, 4)), 300)
                logger.info(
                    f"Using cached IP {cached_ip} for {host} with extended grace period "
                    f"{extended_grace}s (failure #{failure_count + 1})"
                )
                grace_period = extended_grace
            else:
                grace_period = base_grace
                logger.info(
                    f"Using cached IP {cached_ip} for {host} with {grace_period}s grace period"
                )

            # Update failure count in cache
            with _CACHE_LOCK:
                _DNS_CACHE[host] = (cached_ip, now + grace_period, failure_count + 1)

            return cached_ip, int(grace_period)

        # No cached IP available
        raise


def clear_dns_cache() -> None:
    """Flush the local DNS cache and reset failure counts (useful in unit tests)."""
    with _CACHE_LOCK:
        _DNS_CACHE.clear()
    # Also clear thread-local resolver instances
    if hasattr(_RESOLVER_POOL, "resolver"):
        delattr(_RESOLVER_POOL, "resolver")
    if hasattr(_RESOLVER_POOL, "nameservers"):
        delattr(_RESOLVER_POOL, "nameservers")


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
def get_dns_engine(
    uri: str | URL,
    *engine_args: Any,
    dns_timeout: int = 12,
    dns_nameservers: list[str] | None = None,
    dns_grace_ttl: int = 30,
    dns_fallback: bool = True,
    dns_max_retries: int = 3,
    dns_extend_grace: bool = True,
    **engine_kwargs: Any,
) -> Engine:
    """Create a DNS-powered engine from a SQLAlchemy URL.

    With a normal SQLAlchemy engine, the hostname in the URL is looked up
    exactly once, on connection pool creation. If a DNS entry TTL expires
    or otherwise updates, this will prevent failover on reconnect, because
    the old, cached address will still be used.

    This function fixes the problem by intercepting the connection factory
    to resolve DNS on each reconnect.
    """
    logger.debug(
        "get_dns_engine: "
        "uri=%s engine_kwargs=%s dns_timeout=%s "
        "dns_fallback=%s dns_max_retries=%s "
        "dns_extend_grace=%s",
        uri,
        engine_kwargs,
        dns_timeout,
        dns_fallback,
        dns_max_retries,
        dns_extend_grace,
    )

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

    # For SQLite, or a non-DNS hostname (localhost), or an IP address, use a standard engine.
    # For SQLite, host will be None. (host and _is_ip_address(host)) handles this.
    if (
        is_sqlite_url
        or host == "127.0.0.1"
        or host == "localhost"
        or (host and _is_ip_address(host))
    ):
        logger.info("Creating non-DNS engine for %s", url)

        # Restore the connect_args before creating the standard engine
        if user_connect_args:
            engine_kwargs["connect_args"] = user_connect_args

        return create_engine(str(url), *engine_args, **engine_kwargs)

    # If we've reached here, it's a DNS-aware case, and host should not be None.
    assert host is not None, "Host should be a string at this point for DNS resolution"

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
        try:
            ip_now, _ = _cached_ip(
                host,
                dns_timeout=dns_timeout,
                dns_nameservers=dns_nameservers,
                dns_grace_ttl=dns_grace_ttl,
                dns_max_retries=dns_max_retries,
                dns_extend_grace=dns_extend_grace,
            )
            connect_url = url.set(host=ip_now)
        except Exception:
            if dns_fallback:
                # Fall back to original hostname
                connect_url = url
                logger.warning(
                    "DNS resolution failed; falling back to original hostname for %s",
                    host,
                )
            else:
                raise
        cargs, cparams = minimal_dialect.create_connect_args(connect_url)

        # Merge user-supplied connect_args into the parameters
        if user_connect_args:
            cparams.update(user_connect_args)
            logger.debug(
                "Updated connection params with user args: %s", user_connect_args
            )

        module = dbapi_module or minimal_dialect.dbapi
        if module is None:
            raise RuntimeError(
                f"Could not determine DBAPI module for dialect {minimal_dialect.name}"
            )
        logger.info(f"Successfully created DNS-aware engine for {url}.")
        if user_connect_args:
            logger.debug(f"Applied user_connect_args: {user_connect_args}")
        return module.connect(*cargs, **cparams)

    # No need for placeholder URL when using custom creator - just use the original URL
    # This allows OpenTelemetry to properly read the real connection details

    engine = create_engine(
        str(url),
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
        try:
            record.info["peer_ip"] = _cached_ip(
                host,
                dns_timeout=dns_timeout,
                dns_nameservers=dns_nameservers,
                dns_grace_ttl=dns_grace_ttl,
                dns_max_retries=dns_max_retries,
                dns_extend_grace=dns_extend_grace,
            )[0]
        except Exception:
            # If DNS fails here, keep existing info or store hostname marker
            record.info["peer_ip"] = record.info.get("peer_ip", host)

    # This makes sure the DB connection's IP matches current DNS. If it has changed,
    # drop the connection. The insert=True guarantees this listener is first so it
    # runs before any others on checkout.
    @event.listens_for(engine, "checkout", insert=True)
    def _ensure_ip_fresh(
        dbapi_conn: DBAPIConnection, record: _ConnectionRecord, proxy: Any
    ) -> None:  # type: ignore[func-returns-value]
        try:
            current_ip = _cached_ip(
                host,
                dns_timeout=dns_timeout,
                dns_nameservers=dns_nameservers,
                dns_grace_ttl=dns_grace_ttl,
                dns_max_retries=dns_max_retries,
                dns_extend_grace=dns_extend_grace,
            )[0]
        except Exception:
            # If DNS refresh fails during checkout, keep the connection
            return
        if record.info.get("peer_ip") != current_ip:
            record.invalidate()
            raise exc.DisconnectionError(
                f"DNS changed; reconnecting ({record.info.get('peer_ip')} → {current_ip})"
            )

    return engine


def get_ip_with_ttl(
    host: str,
    dns_timeout: int = 12,
    dns_nameservers: list[str] | None = None,
    dns_grace_ttl: int = 30,
    dns_max_retries: int = 3,
    dns_extend_grace: bool = True,
) -> Tuple[str, int]:
    """Get the current IP address and TTL for a hostname.

    Args:
        host (str): The hostname to resolve
        dns_timeout (int): DNS query timeout in seconds
        dns_nameservers (list[str] | None): Custom nameservers to use
        dns_grace_ttl (int): Grace period for cached IPs during failures
        dns_max_retries (int): Maximum retry attempts for DNS resolution
        dns_extend_grace (bool): Whether to extend grace period on repeated failures

    Returns:
        Tuple[str, int]: A tuple of (ip_address, ttl_in_seconds)
    """
    return _cached_ip(
        host,
        dns_timeout=dns_timeout,
        dns_nameservers=dns_nameservers,
        dns_grace_ttl=dns_grace_ttl,
        dns_max_retries=dns_max_retries,
        dns_extend_grace=dns_extend_grace,
    )


def _is_ip_address(host: str) -> bool:
    """Check if a string is a valid IP address."""
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False
