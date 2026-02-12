"""Workflow status command."""

from typing import Tuple

import click


@click.command()
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    multiple=True,
    help="Workflow ID(s) to query. Can be specified multiple times.",
)
@click.option(
    "-u",
    "--user",
    type=str,
    multiple=True,
    help="Username(s) to filter by. Defaults to current user.",
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
def status(
    workflow_id: Tuple[int, ...],
    user: Tuple[str, ...],
    limit: int,
    output: str,
) -> None:
    r"""View workflow status summary.

    \b
    Examples:
      # Show recent workflows for current user
      jobmon workflow status

      # Show specific workflow
      jobmon workflow status -w 12345

      # Show multiple workflows as JSON
      jobmon workflow status -w 12345 -w 67890 -o json

      # Show all workflows for a user
      jobmon workflow status -u jsmith -l 20
    """
    from tabulate import tabulate

    from jobmon.client.commands.workflow import workflow_status as workflow_status_cmd

    # Convert tuples to lists, empty tuple means None
    workflow_ids = list(workflow_id) if workflow_id else None
    users = list(user) if user else None

    df = workflow_status_cmd(
        workflow_id=workflow_ids,
        user=users,
        json=(output == "json"),
        limit=limit,
    )

    if output == "json":
        click.echo(df)
    else:
        click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
