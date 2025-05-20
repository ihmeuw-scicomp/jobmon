# jobmon/server/db/migrate.py
from __future__ import annotations

from importlib.resources import files

from alembic import command
from alembic.config import Config
from sqlalchemy_utils import create_database, database_exists, drop_database

from jobmon.server.web.config import get_jobmon_config


def apply_migrations(uri: str, revision: str = "head") -> None:
    """Apply Alembic migrations to the database."""
    cfg_path = files("jobmon.server").joinpath("alembic.ini")
    mig_path = files("jobmon.server").joinpath("web/migrations")
    cfg = Config(str(cfg_path))
    cfg.set_main_option("sqlalchemy.url", uri)
    cfg.set_main_option("script_location", str(mig_path))
    command.upgrade(cfg, revision)


def init_db() -> None:
    """Initialize the database: create if needed, apply migrations."""
    cfg = get_jobmon_config()
    uri = cfg.get("db", "sqlalchemy_database_uri")
    fresh = False
    if not database_exists(uri):
        create_database(uri)
        fresh = True
    apply_migrations(uri)

    if fresh:
        from jobmon.server.web.models import load_metadata, load_model

        load_model()
        load_metadata()


def terminate_db(uri: str) -> None:
    """Drop the database if it exists."""
    if database_exists(uri):
        drop_database(uri)
