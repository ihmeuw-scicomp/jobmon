"""Main CLI entry point for Jobmon."""

import logging
import sys
from typing import Optional

import click

from jobmon.client.cli.config import config
from jobmon.client.cli.legacy import register_legacy_commands
from jobmon.client.cli.task import task
from jobmon.client.cli.workflow import workflow


def configure_client_logging(debug: bool = False) -> None:
    """Configure logging for the client CLI.

    Uses the same logging infrastructure as other Jobmon components.
    """
    from jobmon.core.config.logconfig_utils import configure_component_logging
    from jobmon.core.config.structlog_config import configure_structlog_with_otlp

    # Configure standard logging (handlers, formatters)
    configure_component_logging("client")

    # Configure structlog (processors, context merging, OTLP integration)
    configure_structlog_with_otlp(component_name="client")

    if debug:
        logging.getLogger("jobmon.client").setLevel(logging.DEBUG)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool) -> None:
    r"""Jobmon workflow management CLI.

    Jobmon is a workflow orchestration tool for managing computational pipelines
    on HPC clusters.

    \b
    Examples:
      jobmon workflow status -u myuser
      jobmon workflow tasks -w 12345 -s FATAL
      jobmon task status -t 123 -t 456
      jobmon config set http.retries_attempts 15

    For more information on a specific command group:
      jobmon <group> --help
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["debug"] = debug

    # Configure client logging with proper infrastructure
    configure_client_logging(debug=debug)


# Register command groups
cli.add_command(workflow)
cli.add_command(task)
cli.add_command(config)


@cli.command()
@click.option(
    "--components",
    is_flag=True,
    help="Show all component versions (core, client, server)",
)
def version(components: bool) -> None:
    """Show version information."""
    from jobmon.client import __version__

    click.echo(f"jobmon {__version__}")

    if components:
        try:
            from jobmon.core import __version__ as core_version

            click.echo(f"jobmon_core {core_version}")
        except ImportError:
            pass

        try:
            from jobmon.server import __version__ as server_version

            click.echo(f"jobmon_server {server_version}")
        except ImportError:
            pass


# Register legacy commands with deprecation warnings
register_legacy_commands(cli)


def main(args: Optional[list] = None) -> None:
    """Main entry point for the CLI."""
    try:
        cli(args)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
