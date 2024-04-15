import logging
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, event, Index, pool
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.schema import ForeignKeyConstraint

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.models import add_string_length_constraint, Base, load_model

logger = logging.getLogger("alembic")


_CONFIG = JobmonConfig()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


# Check if 'sqlalchemy.url' is already set in the Alembic configuration
sqlalchemy_url = config.get_main_option("sqlalchemy.url")

# If 'sqlalchemy.url' is not set, update it with the value from JobmonConfig
if not sqlalchemy_url:
    sqlalchemy_url = _CONFIG.get("db", "sqlalchemy_database_uri")
    config.set_main_option("sqlalchemy.url", sqlalchemy_url)


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# for 'autogenerate' support
# remove sqlite check constraints from the model before loading it
logger.info(sqlalchemy_url)
if not sqlalchemy_url.startswith("sqlite"):
    event.remove(Base, "instrument_class", add_string_length_constraint)
load_model()


# swap foreign keys for indices
def swap_foreign_keys_for_indices(metadata: MetaData) -> None:
    """Swap all foreign keys in metadata for indices."""
    for table in metadata.tables.values():
        for constraint in list(table.constraints):
            # Identify foreign key constraints
            if isinstance(constraint, ForeignKeyConstraint):
                # Create an index for each foreign key constraint
                for column in constraint.columns:
                    index_name = f"fkidx_{table.name}_{column.name}"
                    # Check if index already exists to avoid duplication
                    if not any(index.name == index_name for index in table.indexes):
                        Index(index_name, column)

                # Remove the foreign key constraint from the table
                table.constraints.remove(constraint)


swap_foreign_keys_for_indices(Base.metadata)
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
