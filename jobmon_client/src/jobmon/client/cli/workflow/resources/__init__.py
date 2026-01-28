"""Workflow resources command group."""

import click

from jobmon.client.cli.workflow.resources.usage import usage
from jobmon.client.cli.workflow.resources.yaml import yaml


@click.group()
def resources() -> None:
    r"""Resource usage and YAML generation.

    \b
    Commands for querying resource usage statistics and generating
    resource configuration YAML files.

    \b
    Examples:
      jobmon workflow resources usage -t 456
      jobmon workflow resources yaml -w 12345 --stdout
    """


# Register resources subcommands
resources.add_command(usage)
resources.add_command(yaml)
