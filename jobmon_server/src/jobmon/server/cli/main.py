"""Main CLI entry point for Jobmon Server administration."""

import logging
import sys
from typing import Optional

import click

from jobmon.server.cli.db import db
from jobmon.server.cli.reaper import reaper


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx: click.Context, debug: bool) -> None:
    r"""Jobmon server administration CLI.

    Administrative commands for managing the Jobmon server, database,
    and background services.

    \b
    Examples:
      jobmon-server db init
      jobmon-server db upgrade
      jobmon-server reaper start

    For more information on a specific command group:
      jobmon-server <group> --help
    """
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug

    if debug:
        logging.basicConfig(level=logging.DEBUG)


# Register command groups
cli.add_command(db)
cli.add_command(reaper)


def main(args: Optional[list] = None) -> None:
    """Main entry point for the server CLI."""
    try:
        cli(args)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
