"""Config show command."""

from typing import Optional

import click


@click.command()
@click.option(
    "--section",
    type=str,
    default=None,
    help="Show only a specific section of the configuration.",
)
@click.option(
    "--key",
    type=str,
    default=None,
    help="Show only a specific key (requires --section).",
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(["yaml", "json"]),
    default="yaml",
    show_default=True,
    help="Output format.",
)
def show(section: Optional[str], key: Optional[str], output: str) -> None:
    r"""Display current configuration.

    Shows the current Jobmon configuration from the active config file.

    \b
    Examples:
      # Show all configuration
      jobmon config show

      # Show specific section
      jobmon config show --section http

      # Show specific value
      jobmon config show --section http --key retries_attempts

      # Output as JSON
      jobmon config show -o json
    """
    import json

    import yaml

    from jobmon.core.configuration import JobmonConfig

    if key and not section:
        raise click.UsageError("--key requires --section to be specified.")

    config = JobmonConfig()

    if section:
        if key:
            # Show specific key
            try:
                value = config.get(section, key)
                if output == "json":
                    click.echo(json.dumps({section: {key: value}}, indent=2))
                else:
                    click.echo(f"{section}.{key}: {value}")
            except Exception as e:
                raise click.ClickException(f"Key not found: {e}")
        else:
            # Show entire section
            try:
                section_data = config.get_section(section)
                if output == "json":
                    click.echo(json.dumps({section: section_data}, indent=2))
                else:
                    click.echo(
                        yaml.safe_dump(
                            {section: section_data}, default_flow_style=False
                        )
                    )
            except Exception as e:
                raise click.ClickException(f"Section not found: {e}")
    else:
        # Show all configuration
        if output == "json":
            click.echo(json.dumps(config._config, indent=2))
        else:
            click.echo(yaml.safe_dump(config._config, default_flow_style=False))

    # Show config file path
    click.echo(f"\n(Config file: {config._filepath})", err=True)
