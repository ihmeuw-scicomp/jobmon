"""Config set command."""

from typing import Optional

import click


@click.command("set")
@click.argument("key", type=str)
@click.argument("value", type=str)
@click.option(
    "--config-file",
    type=click.Path(exists=False),
    default=None,
    help="Specific config file to update (defaults to system config).",
)
def set_config(key: str, value: str, config_file: Optional[str]) -> None:
    r"""Update a configuration value.

    Updates a configuration value in the config file using dot notation.

    \b
    Arguments:
      KEY    Configuration key in dot notation (e.g., 'http.retries_attempts')
      VALUE  New value to set

    \b
    Examples:
      # Update HTTP retries
      jobmon config set http.retries_attempts 15

      # Update distributor poll interval
      jobmon config set distributor.poll_interval 5

      # Update specific config file
      jobmon config set http.retries_attempts 15 --config-file ~/my_config.yaml
    """
    from jobmon.client.commands.config import update_config_value

    try:
        result = update_config_value(
            key=key,
            value=value,
            config_file=config_file,
        )
        click.echo(result)
    except ValueError as e:
        raise click.ClickException(str(e))
