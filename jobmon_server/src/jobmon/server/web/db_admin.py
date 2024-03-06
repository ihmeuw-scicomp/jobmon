from pkg_resources import resource_filename

from alembic.config import Config
from alembic import command
from sqlalchemy_utils import create_database, database_exists, drop_database


def apply_migrations(sqlalchemy_database_uri: str, revision: str = "head") -> None:
    """Apply migrations to the database."""
    # Adjust the package path to where alembic.ini is located within your package
    config_path = resource_filename("jobmon.server", "alembic.ini")
    migration_path = resource_filename("jobmon.server", "web/migrations")

    # Set the path to your Alembic configuration file
    alembic_cfg = Config(config_path)
    # Set the SQLAlchemy URL directly in the Alembic config
    alembic_cfg.set_main_option("sqlalchemy.url", sqlalchemy_database_uri)
    alembic_cfg.set_main_option("script_location", migration_path)

    # Invoke the upgrade command programmatically
    command.upgrade(alembic_cfg, revision)


def init_db(sqlalchemy_database_uri: str) -> None:
    """Create database and apply migrations."""
    # create a fresh database
    load_metadata = False
    if not database_exists(sqlalchemy_database_uri):
        load_metadata = True
        create_database(sqlalchemy_database_uri)

    apply_migrations(sqlalchemy_database_uri)

    # load metadata if db is new
    if load_metadata:
        from jobmon.server.web.models import load_metadata
        load_metadata(sqlalchemy_database_uri)

def terminate_db(sqlalchemy_database_uri: str) -> None:
    """Terminate/drop a database."""
    if database_exists(sqlalchemy_database_uri):
        drop_database(sqlalchemy_database_uri)
