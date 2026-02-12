"""Task update command."""

from typing import Tuple

import click


@click.command()
@click.option(
    "-t",
    "--task-id",
    type=int,
    multiple=True,
    required=True,
    help="Task ID(s) to update. Can be specified multiple times.",
)
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    required=True,
    help="Parent workflow ID for the tasks.",
)
@click.option(
    "-s",
    "--status",
    "new_status",
    type=click.Choice(["D", "G", "DONE", "REGISTERED"], case_sensitive=False),
    required=True,
    help="New status: D/DONE (complete) or G/REGISTERED (reset for re-run).",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="Allow all source statuses and all workflow statuses.",
)
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    default=False,
    help=(
        "Apply recursively (requires --force). "
        "Upstream for DONE, downstream for REGISTERED."
    ),
)
def update(
    task_id: Tuple[int, ...],
    workflow_id: int,
    new_status: str,
    force: bool,
    recursive: bool,
) -> None:
    r"""Manually update task statuses.

    Change task status to DONE (complete) or REGISTERED (reset for re-run).
    This is useful for manual intervention when tasks need to be skipped
    or re-run.

    \b
    Status values:
      D or DONE       - Mark task as completed
      G or REGISTERED - Reset task to pending (will re-run)

    \b
    Examples:
      # Mark task as done
      jobmon task update -t 123 -w 12345 -s DONE

      # Reset task to re-run
      jobmon task update -t 123 -w 12345 -s REGISTERED

      # Force update with recursive downstream reset
      jobmon task update -t 123 -w 12345 -s G --force --recursive

      # Update multiple tasks
      jobmon task update -t 123 -t 456 -w 12345 -s D
    """
    from jobmon.client.commands.task import update_task_status

    # Normalize status value
    status_map = {"D": "D", "DONE": "D", "G": "G", "REGISTERED": "G"}
    normalized_status = status_map[new_status.upper()]

    task_ids = list(task_id)

    result = update_task_status(
        task_ids=task_ids,
        workflow_id=workflow_id,
        new_status=normalized_status,
        force=force,
        recursive=recursive,
    )
    click.echo(f"Response: {result}")
