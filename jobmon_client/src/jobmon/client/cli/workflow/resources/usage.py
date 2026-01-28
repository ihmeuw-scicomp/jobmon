"""Workflow resources usage command."""

import json as json_module
from typing import Optional, Tuple

import click


@click.command()
@click.option(
    "-t",
    "--template-version",
    type=int,
    required=True,
    help="Task template version ID to query resource usage for.",
)
@click.option(
    "-w",
    "--workflow-id",
    type=int,
    multiple=True,
    help="Filter by workflow ID(s). Can be specified multiple times.",
)
@click.option(
    "-a",
    "--node-args",
    type=str,
    default=None,
    help="Filter by node arguments (JSON string, e.g., '{\"location_id\": 1}').",
)
def usage(
    template_version: int,
    workflow_id: Tuple[int, ...],
    node_args: Optional[str],
) -> None:
    r"""Query resource usage for a task template version.

    Returns aggregate resource usage statistics for a task template,
    optionally filtered by workflow or node arguments.

    \b
    Examples:
      # Get resource usage for template
      jobmon workflow resources usage -t 456

      # Filter by workflows
      jobmon workflow resources usage -t 456 -w 12345 -w 67890

      # Filter by node arguments
      jobmon workflow resources usage -t 456 -a '{"location_id": 1}'
    """
    from jobmon.client.commands.resources import task_template_resources

    # Parse node_args JSON if provided
    parsed_node_args = None
    if node_args:
        try:
            parsed_node_args = json_module.loads(node_args)
        except json_module.JSONDecodeError as e:
            raise click.BadParameter(f"Invalid JSON for node-args: {e}")

    # Convert tuple to list
    workflow_ids = list(workflow_id) if workflow_id else None

    result = task_template_resources(
        task_template_version=template_version,
        workflows=workflow_ids,
        node_args=parsed_node_args,
    )

    if result is not None:
        click.echo(result)
