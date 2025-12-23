"""Workflow resume command."""

import click


@click.command()
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    required=True,
    help="Workflow ID to resume.",
)
@click.option(
    "-c",
    "--cluster",
    type=str,
    required=True,
    help="Cluster name to run on (e.g., 'slurm', 'slurm_test', 'dummy').",
)
@click.option(
    "--reset-running",
    is_flag=True,
    default=False,
    help="Kill currently running jobs before resuming (cold resume).",
)
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=180,
    show_default=True,
    help="Timeout in seconds to wait for workflow to become resumable.",
)
@click.option(
    "--execution-timeout",
    type=int,
    default=36000,
    show_default=True,
    help="Timeout in seconds for workflow execution after resume.",
)
@click.option(
    "--keep-resources",
    is_flag=True,
    default=False,
    help="Don't auto-increase resources for tasks that failed with resource errors.",
)
def resume(
    workflow_id: int,
    cluster: str,
    reset_running: bool,
    timeout: int,
    execution_timeout: int,
    keep_resources: bool,
) -> None:
    r"""Resume a failed workflow.

    Resumes execution of a workflow that was previously interrupted or failed.
    By default, resources are automatically increased for tasks that failed
    due to resource errors.

    \b
    Examples:
      # Basic resume
      jobmon workflow resume -w 12345 -c slurm

      # Cold resume (kill running jobs)
      jobmon workflow resume -w 12345 -c slurm --reset-running

      # Resume with custom timeouts
      jobmon workflow resume -w 12345 -c slurm -t 300 --execution-timeout 72000

      # Resume without resource increase
      jobmon workflow resume -w 12345 -c slurm --keep-resources
    """
    from jobmon.client.commands.workflow import resume_workflow_from_id

    resume_workflow_from_id(
        workflow_id=workflow_id,
        cluster_name=cluster,
        reset_if_running=reset_running,
        timeout=timeout,
        seconds_until_timeout=execution_timeout,
        increase_resource=not keep_resources,
    )
