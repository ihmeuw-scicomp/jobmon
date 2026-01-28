"""Workflow concurrency command."""

import click


def validate_max_tasks(ctx: click.Context, param: click.Parameter, value: int) -> int:
    """Validate that max_tasks is non-negative."""
    if value < 0:
        raise click.BadParameter("Must be at least 0.")
    return value


@click.command()
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    required=True,
    help="Workflow ID to adjust concurrency for.",
)
@click.option(
    "-m",
    "--max-tasks",
    type=int,
    required=True,
    callback=validate_max_tasks,
    help="Maximum number of concurrent tasks (0 to pause workflow).",
)
def concurrency(workflow_id: int, max_tasks: int) -> None:
    r"""Dynamically adjust concurrent task limit.

    Change the maximum number of tasks that can run concurrently for a
    running workflow. Setting to 0 effectively pauses the workflow.

    \b
    Examples:
      # Set concurrency limit to 100
      jobmon workflow concurrency -w 12345 -m 100

      # Pause workflow (set to 0)
      jobmon workflow concurrency -w 12345 -m 0
    """
    from jobmon.client.commands.workflow import (
        concurrency_limit as concurrency_limit_cmd,
    )

    result = concurrency_limit_cmd(
        workflow_id=workflow_id,
        max_tasks=max_tasks,
    )
    click.echo(result)
