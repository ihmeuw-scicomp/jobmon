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
import socket
import time
from types import ModuleType
from typing import Any, Dict, Type

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine, make_url
from sqlalchemy.engine.interfaces import DBAPIConnection, Dialect

__all__ = ["get_dns_engine", "clear_dns_cache"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DNS-resolution helpers
# ---------------------------------------------------------------------------
# Note: DNS caching removed - using direct hostname resolution instead


def clear_dns_cache() -> None:
    """Flush the local DNS cache and reset failure counts (useful in unit tests)."""
    # No-op since we're not using DNS cache anymore


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


def _get_private_azure_hostname(host: str) -> str:
    """Get the private azure hostname by inserting a .privatelink after the host.

    For example, for scicomp-mysql-db-d01.mysql.database.azure.com,
    it returns scicomp-mysql-db-d01.privatelink.mysql.database.azure.com.
    """
    if host.endswith(".mysql.database.azure.com"):
        return host.replace(
            ".mysql.database.azure.com", ".privatelink.mysql.database.azure.com"
        )
    else:
        return host


def _resolve_host_with_retries(target_host: str) -> bool:
    """Verify hostname is reachable with 5 retries and increasing sleep."""
    for retry in range(5):
        try:
            socket.gethostbyname(target_host)
            return True  # Just validate availability, don't return IP
        except Exception as e:
            if retry < 4:  # Don't sleep on last attempt
                sleep_time = (retry + 1) * 0.2  # 0.2s, 0.4s, 0.6s, 0.8s
                logger.debug(
                    f"DNS resolution failed for {target_host}, "
                    f"retry {retry+1}/5, sleeping {sleep_time}s"
                )
                time.sleep(sleep_time)
            else:
                logger.warning(
                    f"DNS resolution failed for {target_host} after 5 attempts: {e}"
                )
    return False  # Return False instead of None for clarity


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
            # First check if original hostname is available
            if _resolve_host_with_retries(host):
                target_host = host
                logger.debug(f"Using original hostname: {target_host}")
            else:
                # Try private Azure hostname as fallback
                private_host = _get_private_azure_hostname(host)
                if private_host != host and _resolve_host_with_retries(private_host):
                    target_host = private_host
                    logger.info(
                        f"Original hostname {host} failed, \
                            using private hostname: {target_host}"
                    )
                else:
                    raise RuntimeError(
                        f"DNS resolution failed for both {host} and {private_host}"
                    )

            # Create new URL with the resolved hostname
            connect_url = url.set(host=target_host)
            logger.debug(f"Connecting to resolved hostname: {target_host}")

        except Exception as e:
            logger.error(f"DNS resolution failed: {e}")
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

    # Event listeners removed since we're not tracking IP addresses
    # Each connection will resolve the hostname dynamically without IP caching

    return engine


def _is_ip_address(host: str) -> bool:
    """Check if a string is a valid IP address."""
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False
