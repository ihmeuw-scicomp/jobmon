"""Workflow tasks command."""

from typing import Tuple

import click


@click.command()
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    required=True,
    help="Workflow ID to query tasks for.",
)
@click.option(
    "-s",
    "--status",
    type=click.Choice(
        ["PENDING", "RUNNING", "DONE", "FATAL", "pending", "running", "done", "fatal"],
        case_sensitive=False,
    ),
    multiple=True,
    help="Filter by task status. Can be specified multiple times.",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    default=5,
    show_default=True,
    help="Maximum number of results to return.",
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
    help="Output format.",
)
def tasks(
    workflow_id: int,
    status: Tuple[str, ...],
    limit: int,
    output: str,
) -> None:
    r"""View tasks in a workflow.

    \b
    Examples:
      # Show tasks in workflow
      jobmon workflow tasks -w 12345

      # Show only failed tasks
      jobmon workflow tasks -w 12345 -s FATAL

      # Show pending and running tasks
      jobmon workflow tasks -w 12345 -s PENDING -s RUNNING -l 50
    """
    from tabulate import tabulate

    from jobmon.client.commands.workflow import workflow_tasks as workflow_tasks_cmd

    # Convert tuple to list, empty means None
    statuses = list(status) if status else None

    df = workflow_tasks_cmd(
        workflow_id=workflow_id,
        status=statuses,
        json=(output == "json"),
        limit=limit,
    )

    if output == "json":
        click.echo(df)
    else:
        click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
