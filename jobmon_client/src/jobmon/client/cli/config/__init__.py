"""Config command group for Jobmon CLI."""

import click

from jobmon.client.cli.config.set import set_config
from jobmon.client.cli.config.show import show


@click.group()
def config() -> None:
    r"""Configuration management.

    \b
    Commands for viewing and updating Jobmon configuration.

    \b
    Examples:
      jobmon config show
      jobmon config set http.retries_attempts 15
    """


# Register config subcommands
config.add_command(show)
config.add_command(set_config, name="set")
