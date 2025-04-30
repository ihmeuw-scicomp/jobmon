import functools
from importlib.resources import files
import logging
import threading
import time
from typing import cast, Dict, Tuple

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

from MySQLdb.connections import Connection as MySQLConnection
from jobmon.server.web.config import get_jobmon_config
import MySQLdb

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
    ttl = getattr(answers.rrset, "ttl", None) or _DEFAULT_MAX_TTL
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

    if parsed.drivername.startswith("sqlite"):
        # SQLite: No DNS resolution or advanced pooling needed
        engine = create_engine(
            uri,
            connect_args={"check_same_thread": False},  # Allow multi-threaded use
            future=True,
        )
        return engine

    # Non-SQLite (networked databases)
    hostname = parsed.host
    if hostname is None:
        raise ValueError("URI must have a hostname for non-SQLite databases")

    # Initial DNS lookup for pool_recycle
    _, initial_ttl = get_ip_with_ttl(hostname)
    recycle_interval = min(initial_ttl, _DEFAULT_MAX_TTL)

    def _creator() -> MySQLConnection:
        """This is called once per new DBAPI connection using MySQLdb."""
        if hostname is None:
            raise ValueError("Hostname cannot be None")

        ip_now, ttl_now = get_ip_with_ttl(hostname)
        # Adjust recycle so connections don't outlive DNS TTL
        # Note: As mentioned before, accessing _recycle might be fragile.
        # You might rely solely on the checkout check instead.
        # engine.pool._recycle = min(ttl_now, _DEFAULT_MAX_TTL)

        # Get connection parameters from the parsed URL
        db_user = parsed.username
        db_password = parsed.password
        db_name = parsed.database
        db_port = parsed.port or 3306 # Default MySQL port if not specified

        try:
            conn = MySQLdb.connect(
                host=ip_now,
                port=db_port,
                user=db_user,
                passwd=db_password, # Note: parameter name is passwd for MySQLdb
                db=db_name,         # Note: parameter name is db for MySQLdb
                connect_timeout=2,
                ssl_mode=parsed.get("ssl_mode", "REQUIRED"),
                # Add other necessary MySQLdb connection parameters if needed
            )
            # MySQLdb connection objects are the DBAPIConnection type expected
            return conn
        except MySQLdb.Error as e: # Catch specific MySQL errors
            # Log the error appropriately
            print(f"Error connecting with MySQLdb: {e}")
            raise # Re-raise the exception so SQLAlchemy knows creation failed
        except Exception as e: # Catch other potential errors
            print(f"An unexpected error occurred during connection: {e}")
            raise

    # Placeholder URL for dialect/driver detection
    placeholder = parsed.set(host="127.0.0.1", port=1)

    engine = create_engine(
        str(placeholder),
        creator=_creator,
        pool_size=2,
        max_overflow=3,
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
        conn_proxy: MySQLConnection,
    ) -> None:
        """Every time the pool gives out a connection, check IP matches current DNS IP.

        If the IP doesn't match the current DNS IP, drop the connection.
        """
        if hostname is None:
            raise ValueError("Hostname cannot be None")

        old_ip = conn_record.info.get("peer_ip")
        current_ip, _ = get_ip_with_ttl(hostname)
        if old_ip != current_ip:
            conn_record.invalidate(None)  # Kill the socket
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
