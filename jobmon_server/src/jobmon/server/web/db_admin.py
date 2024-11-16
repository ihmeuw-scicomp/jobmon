from importlib.resources import files  # type: ignore

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from jobmon.server.web.config import get_jobmon_config


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


def get_engine_from_config() -> Engine:
    """Create a SQLAlchemy engine from a URI."""
    connect_args = {}
    config = get_jobmon_config()
    sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
    if "sqlite" in sqlalchemy_database_uri:
        connect_args["check_same_thread"] = False

    _engine_instance = create_engine(
        sqlalchemy_database_uri,
        connect_args=connect_args,
        pool_recycle=200,
        future=True,
    )

    return _engine_instance


def init_db() -> None:
    """Create database and apply migrations."""
    # get db url from config
    config = get_jobmon_config()
    sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
    # create a fresh database
    add_metadata = False
    if not database_exists(sqlalchemy_database_uri):
        add_metadata = True
        create_database(sqlalchemy_database_uri)

    apply_migrations(sqlalchemy_database_uri)

    # get the engine
    engine = get_engine_from_config()

    # load metadata if db is new
    if add_metadata:
        from jobmon.server.web.models import load_metadata

        load_metadata(engine)


def terminate_db(sqlalchemy_database_uri: str) -> None:
    """Terminate/drop a database."""
    if database_exists(sqlalchemy_database_uri):
        drop_database(sqlalchemy_database_uri)


# create a singleton holder so that it gets created after config
_session_local = None
_db_url = None


def get_session_local() -> sessionmaker:
    """Get a session local object."""
    global _session_local
    global _db_url
    config = get_jobmon_config()
    url_from_config = config.get("db", "sqlalchemy_database_uri")
    # if _session_local is not set, or db_url has changed, create a new
    # this is to compromise to the behavior when running tests under a directory
    # somehow, when running tests under a dir, it initializes the default config anyway
    # this doesn't happen when running prod or a test file
    """TODO:
       move to a .env file
    """
    if _session_local is None or url_from_config != _db_url:
        # backdoor for conftest without touching the existing JobmonConfig
        _session_local = sessionmaker(
            autocommit=False, autoflush=False, bind=get_engine_from_config()
        )
    # reset the db_url from config
    _db_url = url_from_config
    return _session_local
