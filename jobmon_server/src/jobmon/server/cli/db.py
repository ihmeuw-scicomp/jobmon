"""Database management commands for Jobmon Server."""

import click


@click.group()
def db() -> None:
    r"""Database management commands.

    \b
    Commands for initializing, upgrading, and terminating the Jobmon database.

    \b
    Examples:
      jobmon-server db init
      jobmon-server db upgrade --revision head
      jobmon-server db terminate --yes
    """


@db.command()
@click.option(
    "--db-uri",
    type=str,
    default="",
    help="SQLAlchemy database URI (defaults to config file setting).",
)
def init(db_uri: str) -> None:
    r"""Initialize a new Jobmon database.

    Creates the database schema and applies all migrations.

    \b
    Examples:
      # Use config file settings
      jobmon-server db init

      # Override with specific URI
      jobmon-server db init --db-uri "mysql://user:pass@host/jobmon"
    """
    from jobmon.core.configuration import JobmonConfig
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.server.web.db import init_db

    sqlalchemy_database_uri = db_uri
    if not sqlalchemy_database_uri:
        config = JobmonConfig()
        sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
    else:
        # Override the config singleton with CLI-provided database URI
        config = JobmonConfig(
            dict_config={"db": {"sqlalchemy_database_uri": sqlalchemy_database_uri}}
        )
        get_jobmon_config(config)

    init_db(sqlalchemy_database_uri)
    click.echo(f"Database initialized: {sqlalchemy_database_uri}")


@db.command()
@click.option(
    "--db-uri",
    type=str,
    default="",
    help="SQLAlchemy database URI (defaults to config file setting).",
)
@click.option(
    "--revision",
    type=str,
    default="head",
    show_default=True,
    help="Target revision to upgrade to.",
)
def upgrade(db_uri: str, revision: str) -> None:
    r"""Apply database migrations.

    Upgrades the database schema to the specified revision.

    \b
    Examples:
      # Upgrade to latest
      jobmon-server db upgrade

      # Upgrade to specific revision
      jobmon-server db upgrade --revision abc123
    """
    from jobmon.core.configuration import JobmonConfig
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.server.web.db import apply_migrations

    sqlalchemy_database_uri = db_uri
    if not sqlalchemy_database_uri:
        config = JobmonConfig()
        sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
    else:
        config = JobmonConfig(
            dict_config={"db": {"sqlalchemy_database_uri": sqlalchemy_database_uri}}
        )
        get_jobmon_config(config)

    apply_migrations(sqlalchemy_database_uri, revision)
    click.echo(f"Database upgraded to revision: {revision}")


@db.command()
@click.option(
    "--db-uri",
    type=str,
    default="",
    help="SQLAlchemy database URI (defaults to config file setting).",
)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt.",
)
def terminate(db_uri: str, yes: bool) -> None:
    r"""Terminate/drop a Jobmon database.

    WARNING: This is a destructive operation that will delete all data!

    \b
    Examples:
      # With confirmation prompt
      jobmon-server db terminate

      # Skip confirmation (for scripts)
      jobmon-server db terminate --yes
    """
    from jobmon.core.configuration import JobmonConfig
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.server.web.db import terminate_db

    sqlalchemy_database_uri = db_uri
    if not sqlalchemy_database_uri:
        config = JobmonConfig()
        sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
    else:
        config = JobmonConfig(
            dict_config={"db": {"sqlalchemy_database_uri": sqlalchemy_database_uri}}
        )
        get_jobmon_config(config)

    if not yes:
        click.confirm(
            f"Are you sure you want to terminate the database at {sqlalchemy_database_uri}?",
            abort=True,
        )

    terminate_db(sqlalchemy_database_uri)
    click.echo(f"Database terminated: {sqlalchemy_database_uri}")
