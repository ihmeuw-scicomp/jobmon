"""Task status command."""

from typing import Tuple

import click


@click.command()
@click.option(
    "-t",
    "--task-id",
    type=int,
    multiple=True,
    required=True,
    help="Task ID(s) to query. Can be specified multiple times.",
)
@click.option(
    "-s",
    "--status",
    "filter_status",
    type=click.Choice(
        ["PENDING", "RUNNING", "DONE", "FATAL", "pending", "running", "done", "fatal"],
        case_sensitive=False,
    ),
    multiple=True,
    help="Filter by task instance status. Can be specified multiple times.",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    default=None,
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
    task_id: Tuple[int, ...],
    filter_status: Tuple[str, ...],
    limit: int,
    output: str,
) -> None:
    r"""View task instance details.

    Shows the status and metadata for task instances associated with
    the specified task ID(s).

    \b
    Examples:
      # Show task status
      jobmon task status -t 123

      # Show multiple tasks
      jobmon task status -t 123 -t 456 -t 789

      # Filter by status
      jobmon task status -t 123 -s DONE -s FATAL

      # Output as JSON
      jobmon task status -t 123 -o json
    """
    from tabulate import tabulate

    from jobmon.client.commands.task import task_status as task_status_cmd

    # Convert tuples to lists
    task_ids = list(task_id)
    statuses = list(filter_status) if filter_status else None

    click.echo(f"\nTASK_IDS: {task_ids}")

    df = task_status_cmd(
        task_ids=task_ids,
        status=statuses,
        json=(output == "json"),
    )

    if output == "json":
        click.echo(df)
    else:
        click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
