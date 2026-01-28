"""Workflow logs command (get log file paths)."""

import click


@click.command()
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    required=True,
    help="Workflow ID to get log paths for.",
)
@click.option(
    "-a",
    "--array",
    type=str,
    default="",
    help="Filter by array name.",
)
@click.option(
    "-j",
    "--job",
    type=str,
    default="",
    help="Filter by job name.",
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
def logs(
    workflow_id: int,
    array: str,
    job: str,
    limit: int,
    output: str,
) -> None:
    r"""Get log file paths for tasks in a workflow.

    Returns stdout/stderr file paths for task instances, useful for
    debugging failed tasks.

    \b
    Examples:
      # Get log paths for workflow
      jobmon workflow logs -w 12345

      # Filter by array name
      jobmon workflow logs -w 12345 -a "processing_array" -l 20

      # Filter by job name as JSON
      jobmon workflow logs -w 12345 -j "task_1" -o json
    """
    from tabulate import tabulate

    from jobmon.client.commands.workflow import get_filepaths

    df = get_filepaths(
        workflow_id=workflow_id,
        array_name=array,
        job_name=job,
        limit=limit,
    )

    if output == "json":
        # get_filepaths returns dict in json mode
        import json

        click.echo(json.dumps(df, indent=2))
    else:
        click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
