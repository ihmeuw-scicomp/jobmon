import functools
from importlib.resources import files
import logging
import threading
import time
from typing import Dict, Tuple, cast

from alembic import command
from alembic.config import Config
from dns import resolver
from sqlalchemy import create_engine, event, exc
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import _ConnectionRecord
from sqlalchemy_utils import create_database, database_exists, drop_database
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


from jobmon.server.web.config import get_jobmon_config

logger = logging.getLogger(__name__)

# DNS Cache Internals
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}  # hostname -> (ip, expires_epoch)
_CACHE_LOCK = threading.RLock()  # Thread-safe lock for cache
_DEFAULT_MAX_TTL = 300  # 5 min max TTL to prevent stale IPs


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(Exception),
)
def _resolve_from_dns(hostname: str) -> Tuple[str, int]:
    """Resolve hostname to IP with retries. Returns (ip, ttl_seconds)."""
    answers = resolver.resolve(hostname, "A", lifetime=2)  # Fail-fast after 2 seconds
    ttl = getattr(answers.rrset, 'ttl', None) or _DEFAULT_MAX_TTL
    return answers[0].address, ttl


def get_ip_with_ttl(hostname: str) -> Tuple[str, int]:
    """Get the current IP and TTL for a hostname using a TTL cache.

    - Returns cached IP and TTL if valid.
    - Resolves DNS and updates cache if expired or missing.
    - Falls back to last known IP if DNS resolution fails.
    """
    now = time.time()

    with _CACHE_LOCK:
        ip, expires_at = _DNS_CACHE.get(hostname, (None, 0.0))
        if ip and expires_at > now:
            # Calculate remaining TTL
            remaining_ttl = int(expires_at - now)
            return ip, remaining_ttl

    try:
        ip, ttl = _resolve_from_dns(hostname)
        ttl = min(ttl, _DEFAULT_MAX_TTL)  # Enforce max TTL
        with _CACHE_LOCK:
            _DNS_CACHE[hostname] = (ip, now + ttl)
        return ip, ttl
    except Exception as err:
        logger.warning("DNS resolve failed for %s: %s", hostname, err, exc_info=err)
        if ip:  # Use last known IP if DNS fails
            # Assume a short TTL to force a retry soon
            return ip, 30  # 30 seconds
        raise


# Database Functions
def apply_migrations(sqlalchemy_database_uri: str, revision: str = "head") -> None:
    """Apply database migrations using Alembic."""
    config_path = files("jobmon.server").joinpath("alembic.ini")
    migration_path = files("jobmon.server").joinpath("web/migrations")
    alembic_cfg = Config(str(config_path))
    alembic_cfg.set_main_option("sqlalchemy.url", sqlalchemy_database_uri)
    alembic_cfg.set_main_option("script_location", str(migration_path))
    command.upgrade(alembic_cfg, revision)


@functools.lru_cache(maxsize=4)
def get_engine_from_config(uri: str) -> Engine:
    """Create a SQLAlchemy Engine whose connections always point to the current IP.

    The connections always point to the *current* IP of `uri`'s host, with
    automated cache-and-recycle driven by the DNS record's TTL.
    """
    parsed = make_url(uri)
    hostname = parsed.host
    
    # Add null check for hostname
    if hostname is None:
        raise ValueError("URI must have a hostname")

    # Do an initial lookup so we can set the starting pool_recycle
    _, initial_ttl = get_ip_with_ttl(hostname)
    recycle_interval = min(initial_ttl, _DEFAULT_MAX_TTL)

    def _creator() -> DBAPIConnection:
        """This is called once per new DBAPI connection.

        We:
          1. resolve the host (getting fresh TTL)
          2. bump pool_recycle so old sockets get closed at the right time
          3. open a raw driver connection straight to that IP
        """
        if hostname is None:
            raise ValueError("Hostname cannot be None")
            
        ip_now, ttl_now = get_ip_with_ttl(hostname)
        # Adjust recycle so connections don't live past their DNS validity
        engine.pool._recycle = min(ttl_now, _DEFAULT_MAX_TTL)
        conn = create_engine(
            parsed.set(host=ip_now), connect_args={"connect_timeout": 2}
        ).raw_connection()
        
        # Explicitly cast the return value to DBAPIConnection
        return cast(DBAPIConnection, conn)

    # We need *some* dummy URL here so SQLAlchemy knows which dialect/driver
    placeholder = parsed.set(host="127.0.0.1", port=1)

    engine = create_engine(
        str(placeholder),
        creator=_creator,
        pool_size=20,
        max_overflow=10,
        pool_recycle=recycle_interval,
        pool_pre_ping=True,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _on_connect(
        dbapi_conn: DBAPIConnection, conn_record: _ConnectionRecord
    ) -> None:
        """Stash the IP we used when this connection was first made."""
        if hostname is None:
            raise ValueError("Hostname cannot be None")
            
        ip_used, _ = get_ip_with_ttl(hostname)
        conn_record.info["peer_ip"] = ip_used

    @event.listens_for(engine, "checkout")
    def _on_checkout(
        dbapi_conn: DBAPIConnection,
        conn_record: _ConnectionRecord,
        conn_proxy: Connection,
    ) -> None:
        """Every time the pool gives out a connection, check IP matches current DNS IP.

        If the IP doesn't match the current DNS IP, drop the connection.
        """
        if hostname is None:
            raise ValueError("Hostname cannot be None")
            
        old_ip = conn_record.info.get("peer_ip")
        current_ip, _ = get_ip_with_ttl(hostname)
        if old_ip != current_ip:
            # Fix: Use None instead of dbapi_conn for the first argument
            conn_record.invalidate(None)  # kill the socket
            raise exc.DisconnectionError(
                f"Host IP changed from {old_ip} → {current_ip}; reconnecting"
            )

    return engine


@functools.lru_cache(maxsize=4)
def _get_session_factory(uri: str) -> sessionmaker:
    """Cached session factory for database sessions."""
    return sessionmaker(
        bind=get_engine_from_config(uri), autoflush=False, autocommit=False
    )


def get_session_local() -> sessionmaker:
    """Get or create a sessionmaker for database sessions."""
    config = get_jobmon_config()
    uri = config.get("db", "sqlalchemy_database_uri")
    return _get_session_factory(uri)


def init_db() -> None:
    """Initialize database and apply migrations."""
    config = get_jobmon_config()
    sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
    add_metadata = False
    if not database_exists(sqlalchemy_database_uri):
        add_metadata = True
        create_database(sqlalchemy_database_uri)
    apply_migrations(sqlalchemy_database_uri)
    if add_metadata:
        from jobmon.server.web.models import load_metadata

        load_metadata(get_engine_from_config(sqlalchemy_database_uri))


def terminate_db(sqlalchemy_database_uri: str) -> None:
    """Drop the database if it exists."""
    if database_exists(sqlalchemy_database_uri):
        drop_database(sqlalchemy_database_uri)
