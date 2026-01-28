"""Task command group for Jobmon CLI."""

import click

from jobmon.client.cli.task.dependencies import dependencies
from jobmon.client.cli.task.status import status
from jobmon.client.cli.task.update import update


@click.group()
def task() -> None:
    r"""Task operations.

    \b
    Commands for viewing task status, updating task states,
    and exploring task dependencies.

    \b
    Examples:
      jobmon task status -t 123
      jobmon task update -t 123 -w 12345 -s DONE
      jobmon task dependencies -t 123
    """


# Register task subcommands
task.add_command(status)
task.add_command(update)
task.add_command(dependencies)
