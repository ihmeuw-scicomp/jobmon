"""Workflow reset command."""

import click


@click.command()
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    required=True,
    help="Workflow ID to reset.",
)
@click.option(
    "-p",
    "--partial",
    is_flag=True,
    default=False,
    help="Keep DONE tasks unchanged (partial reset).",
)
def reset(workflow_id: int, partial: bool) -> None:
    r"""Reset a workflow to REGISTERED state.

    This allows a failed workflow to be re-run. Only works on workflows
    in ERROR state and you must be the workflow owner.

    \b
    Examples:
      # Full reset - all tasks reset
      jobmon workflow reset -w 12345

      # Partial reset - keep completed tasks
      jobmon workflow reset -w 12345 --partial
    """
    from jobmon.client.commands.workflow import workflow_reset as workflow_reset_cmd

    result = workflow_reset_cmd(
        workflow_id=workflow_id,
        partial_reset=partial,
    )
    click.echo(result)
