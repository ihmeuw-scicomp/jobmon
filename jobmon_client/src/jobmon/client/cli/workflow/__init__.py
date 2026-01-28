"""Workflow command group for Jobmon CLI."""

import click

from jobmon.client.cli.workflow.concurrency import concurrency
from jobmon.client.cli.workflow.logs import logs
from jobmon.client.cli.workflow.reset import reset
from jobmon.client.cli.workflow.resources import resources
from jobmon.client.cli.workflow.resume import resume
from jobmon.client.cli.workflow.status import status
from jobmon.client.cli.workflow.tasks import tasks


@click.group()
def workflow() -> None:
    r"""Workflow operations.

    \b
    Commands for viewing workflow status, managing workflow state,
    and querying resource usage.

    \b
    Examples:
      jobmon workflow status -w 12345
      jobmon workflow tasks -w 12345 -s FATAL
      jobmon workflow resume -w 12345 -c slurm
      jobmon workflow resources usage -t 456
    """


# Register workflow subcommands
workflow.add_command(status)
workflow.add_command(tasks)
workflow.add_command(reset)
workflow.add_command(resume)
workflow.add_command(concurrency)
workflow.add_command(logs)
workflow.add_command(resources)
