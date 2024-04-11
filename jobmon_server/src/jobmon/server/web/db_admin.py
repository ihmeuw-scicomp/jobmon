try:
    # Python 3.9 and newer
    from importlib.resources import files  # type: ignore
except ImportError:
    # Python 3.8 and older, requires 'importlib_resources' to be installed
    from importlib_resources import files  # type: ignore

from alembic import command
from alembic.config import Config
from sqlalchemy_utils import create_database, database_exists, drop_database


def apply_migrations(sqlalchemy_database_uri: str, revision: str = "head") -> None:
    """Apply migrations to the database."""
    # Adjust the package path to where alembic.ini is located within your package
    # Note: Ensure that 'jobmon.server' is a valid package and 'web/migrations' is accessible
    config_path = files("jobmon.server").joinpath("alembic.ini")
    migration_path = files("jobmon.server").joinpath("web/migrations")

    # Set the path to your Alembic configuration file
    alembic_cfg = Config(str(config_path))
    # Set the SQLAlchemy URL directly in the Alembic config
    alembic_cfg.set_main_option("sqlalchemy.url", sqlalchemy_database_uri)
    alembic_cfg.set_main_option("script_location", str(migration_path))

    # Invoke the upgrade command programmatically
    command.upgrade(alembic_cfg, revision)


def init_db(sqlalchemy_database_uri: str) -> None:
    """Create database and apply migrations."""
    # create a fresh database
    add_metadata = False
    if not database_exists(sqlalchemy_database_uri):
        add_metadata = True
        create_database(sqlalchemy_database_uri)

    apply_migrations(sqlalchemy_database_uri)

    # load metadata if db is new
    if add_metadata:
        from jobmon.server.web.models import load_metadata

        load_metadata(sqlalchemy_database_uri)


def terminate_db(sqlalchemy_database_uri: str) -> None:
    """Terminate/drop a database."""
    if database_exists(sqlalchemy_database_uri):
        drop_database(sqlalchemy_database_uri)
