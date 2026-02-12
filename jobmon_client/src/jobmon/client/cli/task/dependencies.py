"""Task dependencies command."""

import click


@click.command()
@click.option(
    "-t",
    "--task-id",
    type=int,
    required=True,
    help="Task ID to query dependencies for.",
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
    help="Output format.",
)
def dependencies(task_id: int, output: str) -> None:
    r"""View task upstream/downstream dependencies.

    Shows all upstream (parent) and downstream (child) tasks for the
    specified task, along with their current status.

    \b
    Examples:
      # Show dependencies
      jobmon task dependencies -t 123

      # As JSON
      jobmon task dependencies -t 123 -o json
    """
    import json

    from jobmon.client.commands.task import get_task_dependencies

    result = get_task_dependencies(task_id)
    up = result["up"]
    down = result["down"]

    if output == "json":
        click.echo(json.dumps(result, indent=2))
    else:
        click.echo("Upstream Tasks:\n")
        click.echo("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in up:
            tid = item["id"]
            status = item["status"]
            click.echo("{:<8} {:<15} {:<15}".format("", tid, status))

        click.echo("\nDownstream Tasks:\n")
        click.echo("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in down:
            tid = item["id"]
            status = item["status"]
            click.echo("{:<8} {:<15} {:<15}".format("", tid, status))
