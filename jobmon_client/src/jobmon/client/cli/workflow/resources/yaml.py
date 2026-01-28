"""Workflow resources yaml command."""

from typing import Any, Dict, List, Optional, Tuple

import click


class MutuallyExclusiveOption(click.Option):
    """Custom option class for mutually exclusive options."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the option with mutually exclusive options list."""
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        super().__init__(*args, **kwargs)

    def handle_parse_result(
        self, ctx: click.Context, opts: Dict[str, Any], args: List[str]
    ) -> Tuple[Any, List[str]]:
        """Handle parse result and check for mutually exclusive options."""
        current_opt = self.name in opts

        for mutex_opt in self.mutually_exclusive:
            if mutex_opt in opts:
                if current_opt:
                    raise click.UsageError(
                        f"Options --{self.name.replace('_', '-')} and "
                        f"--{mutex_opt.replace('_', '-')} are mutually exclusive."
                    )

        return super().handle_parse_result(ctx, opts, args)


@click.command("yaml")
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    default=None,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["task_id"],
    help="Workflow ID to generate YAML for (mutually exclusive with -t).",
)
@click.option(
    "-t",
    "--task-id",
    type=int,
    default=None,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["workflow_id"],
    help="Task ID to generate YAML for (mutually exclusive with -w).",
)
@click.option(
    "--mem-strategy",
    type=click.Choice(["avg", "min", "max"]),
    default="avg",
    show_default=True,
    help="Strategy for calculating memory values.",
)
@click.option(
    "--core-strategy",
    type=click.Choice(["avg", "min", "max"]),
    default="avg",
    show_default=True,
    help="Strategy for calculating core values.",
)
@click.option(
    "--runtime-strategy",
    type=click.Choice(["avg", "min", "max"]),
    default="max",
    show_default=True,
    help="Strategy for calculating runtime values.",
)
@click.option(
    "-c",
    "--cluster",
    type=str,
    multiple=True,
    default=["slurm"],
    show_default=True,
    help="Target cluster(s) for YAML. Can be specified multiple times.",
)
@click.option(
    "-f",
    "--file",
    "output_file",
    type=click.Path(),
    default=None,
    help="Output file path to write YAML to.",
)
@click.option(
    "--stdout",
    is_flag=True,
    default=False,
    help="Print YAML to stdout.",
)
def yaml(
    workflow_id: Optional[int],
    task_id: Optional[int],
    mem_strategy: str,
    core_strategy: str,
    runtime_strategy: str,
    cluster: Tuple[str, ...],
    output_file: Optional[str],
    stdout: bool,
) -> None:
    r"""Generate resource YAML from historical usage.

    Creates a resource configuration YAML file based on actual resource
    usage from previous runs. Requires either --workflow-id or --task-id.

    \b
    Examples:
      # Generate YAML for workflow, print to stdout
      jobmon workflow resources yaml -w 12345 --stdout

      # Generate YAML for specific task, save to file
      jobmon workflow resources yaml -t 789 -f resources.yaml

      # Use max values for all metrics
      jobmon workflow resources yaml -w 12345 --mem-strategy max \\
          --core-strategy max --runtime-strategy max -f resources.yaml

      # Generate for multiple clusters
      jobmon workflow resources yaml -w 12345 -c slurm -c dummy --stdout
    """
    from jobmon.client.commands.resources import create_resource_yaml

    # Validate that exactly one of workflow_id or task_id is provided
    if workflow_id is None and task_id is None:
        raise click.UsageError("Must provide either --workflow-id or --task-id.")

    # Validate that at least one output method is specified
    if not stdout and output_file is None:
        raise click.UsageError("Must specify either --stdout or --file.")

    yaml_content = create_resource_yaml(
        wfid=workflow_id,
        tid=task_id,
        v_mem=mem_strategy,
        v_core=core_strategy,
        v_runtime=runtime_strategy,
        clusters=list(cluster),
    )

    if stdout:
        click.echo(yaml_content)

    if output_file:
        with open(output_file, "w") as f:
            f.write(yaml_content)
        click.echo(f"Resource YAML written to {output_file}")
