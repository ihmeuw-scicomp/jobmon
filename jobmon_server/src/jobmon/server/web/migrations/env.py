from logging.config import fileConfig

from alembic import context
# from alembic.operations import Operations, MigrateOperation, CreateTableOp
from sqlalchemy import engine_from_config, event, Index
from sqlalchemy import pool
# from sqlalchemy.schema import ForeignKeyConstraint

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.models import Base, load_model, add_string_length_constraint


_CONFIG = JobmonConfig()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


# Check if 'sqlalchemy.url' is already set in the Alembic configuration
existing_sqlalchemy_url = config.get_main_option("sqlalchemy.url")

# If 'sqlalchemy.url' is not set, update it with the value from JobmonConfig
if not existing_sqlalchemy_url:
    sqlalchemy_database_uri = _CONFIG.get("web", "sqlalchemy_database_uri")
    config.set_main_option("sqlalchemy.url", sqlalchemy_database_uri)


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# for 'autogenerate' support
# remove sqlite check constraints from the model before loading it
event.remove(Base, "instrument_class", add_string_length_constraint)
load_model()

# swap foreign keys for indices
def swap_foreign_keys_for_indices(metadata):
    """Swap all foreign keys in metadata for indices."""
    for table in metadata.tables.values():
        for constraint in list(table.constraints):
            # Identify foreign key constraints
            if constraint.__visit_name__ == 'foreign_key_constraint':
                # Create an index for each foreign key constraint
                for column in constraint.columns:
                    index_name = f"ix_{table.name}_{column.name}"
                    # Check if index already exists to avoid duplication
                    if not any(index.name == index_name for index in table.indexes):
                        Index(index_name, column)
                
                # Remove the foreign key constraint from the table
                table.constraints.remove(constraint)
                # Also, remove from metadata's global constraint list if necessary
                # This might require additional handling depending on SQLAlchemy version and use case

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
