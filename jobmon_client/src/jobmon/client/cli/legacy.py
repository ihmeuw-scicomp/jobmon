"""Legacy command aliases with deprecation warnings.

This module provides backward-compatible aliases for the old CLI commands.
Each legacy command emits a deprecation warning and delegates to the new
Click-based command.
"""

from typing import Any, Callable, Dict, List, Tuple

import click

# ANSI color codes for terminal output
YELLOW = "\033[93m"
RESET = "\033[0m"

# Mapping of old commands to new commands
# Format: old_command -> (new_command_path, [(old_arg, new_arg), ...])
LEGACY_COMMAND_MAP: Dict[str, Tuple[str, List[Tuple[str, str]]]] = {
    # old_command: (new_command_path, [old_args_to_new_args_mapping])
    "workflow_status": ("workflow status", []),
    "workflow_tasks": ("workflow tasks", []),
    "workflow_reset": ("workflow reset", [("--partial_reset", "--partial")]),
    "workflow_resume": (
        "workflow resume",
        [
            ("--cluster_name", "--cluster"),
            ("--reset-running-jobs", "--reset-running"),
            ("--seconds-until-timeout", "--execution-timeout"),
            ("--use-original-resources", "--keep-resources"),
        ],
    ),
    "task_status": ("task status", [("--task_ids", "--task-id")]),
    "update_task_status": (
        "task update",
        [
            ("--task_ids", "--task-id"),
            ("--new_status", "--status"),
        ],
    ),
    "task_dependencies": ("task dependencies", []),
    "concurrency_limit": (
        "workflow concurrency",
        [("--max_tasks", "--max-tasks")],
    ),
    "get_filepaths": (
        "workflow logs",
        [
            ("--array_name", "--array"),
            ("--job_name", "--job"),
        ],
    ),
    "task_template_resources": (
        "workflow resources usage",
        [("--task_template_version", "--template-version")],
    ),
    "create_resource_yaml": (
        "workflow resources yaml",
        [
            ("--value_mem", "--mem-strategy"),
            ("--value_core", "--core-strategy"),
            ("--value_runtime", "--runtime-strategy"),
            ("--print", "--stdout"),
            ("--clusters", "--cluster"),
        ],
    ),
    "update_config": ("config set", []),
}


def emit_deprecation_warning(old_cmd: str, new_cmd: str) -> None:
    """Emit a deprecation warning to stderr."""
    msg = (
        f"{YELLOW}⚠️  DEPRECATION WARNING: 'jobmon {old_cmd}' is deprecated.\n"
        f"   Use 'jobmon {new_cmd}' instead.\n"
        f"   Legacy commands will be removed in version 3.0.{RESET}\n"
    )
    click.echo(msg, err=True)


def create_legacy_command(
    old_name: str,
    new_path: str,
    arg_mappings: List[Tuple[str, str]],
) -> Callable:
    """Create a legacy command that wraps the new command with deprecation warning.

    Args:
        old_name: The old command name (e.g., 'workflow_status')
        new_path: The new command path (e.g., 'workflow status')
        arg_mappings: List of (old_arg, new_arg) tuples for argument translation
    """

    @click.command(
        old_name,
        context_settings=dict(
            ignore_unknown_options=True,
            allow_extra_args=True,
            allow_interspersed_args=False,
        ),
        add_help_option=False,
    )
    @click.pass_context
    def legacy_cmd(ctx: click.Context) -> None:
        """Legacy command - deprecated."""
        emit_deprecation_warning(old_name, new_path)

        # Transform arguments
        args = list(ctx.args)
        for old_arg, new_arg in arg_mappings:
            args = [new_arg if a == old_arg else a for a in args]

        # Navigate to the new command
        parts = new_path.split()
        current_cmd = ctx.parent.command if ctx.parent else ctx.command

        # Find the target command group and subcommand
        for part in parts[:-1]:
            if hasattr(current_cmd, "commands"):
                current_cmd = current_cmd.commands.get(part)
            else:
                break

        if hasattr(current_cmd, "commands"):
            target_cmd = current_cmd.commands.get(parts[-1])
            if target_cmd:
                # Invoke the new command with transformed args
                ctx.invoke(target_cmd, **_parse_legacy_args(target_cmd, args))
                return

        # Fallback: show help message
        click.echo(
            f"Run: jobmon {new_path} {' '.join(args)}",
            err=True,
        )

    # Set help text
    legacy_cmd.help = f"[DEPRECATED] Use 'jobmon {new_path}' instead."

    return legacy_cmd


def _parse_legacy_args(cmd: click.Command, args: List[str]) -> Dict[str, Any]:
    """Parse legacy arguments into kwargs for the new command.

    This is a simplified parser - for complex cases, users should migrate
    to the new command syntax.
    """
    # For now, we'll use Click's built-in parsing via context
    # This function exists for potential future enhancement
    return {}


def register_legacy_commands(cli: click.Group) -> None:
    """Register all legacy commands with the CLI.

    Each legacy command will emit a deprecation warning when invoked.
    """
    # Import here to avoid circular imports

    # Create wrapper commands that delegate to new commands
    # These use a different approach - they use the old argparse-based implementation
    # but emit deprecation warnings

    @cli.command("workflow_status")
    @click.option("-w", "--workflow_id", type=int, multiple=True)
    @click.option("-u", "--user", type=str, multiple=True)
    @click.option("-n", "--json", "use_json", is_flag=True)
    @click.option("-l", "--limit", type=int, default=5)
    @click.pass_context
    def workflow_status(ctx, workflow_id, user, use_json, limit):
        """[DEPRECATED] Use 'jobmon workflow status' instead."""
        emit_deprecation_warning("workflow_status", "workflow status")
        from tabulate import tabulate

        from jobmon.client.status_commands import workflow_status as workflow_status_cmd

        workflow_ids = list(workflow_id) if workflow_id else None
        users = list(user) if user else None
        df = workflow_status_cmd(workflow_ids, users, use_json, limit)
        if use_json:
            click.echo(df)
        else:
            click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @cli.command("workflow_tasks")
    @click.option("-w", "--workflow_id", type=int, required=True)
    @click.option("-s", "--status", type=str, multiple=True)
    @click.option("-n", "--json", "use_json", is_flag=True)
    @click.option("-l", "--limit", type=int, default=5)
    @click.pass_context
    def workflow_tasks(ctx, workflow_id, status, use_json, limit):
        """[DEPRECATED] Use 'jobmon workflow tasks' instead."""
        emit_deprecation_warning("workflow_tasks", "workflow tasks")
        from tabulate import tabulate

        from jobmon.client.status_commands import workflow_tasks as workflow_tasks_cmd

        statuses = list(status) if status else None
        df = workflow_tasks_cmd(workflow_id, statuses, use_json, limit)
        if use_json:
            click.echo(df)
        else:
            click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @cli.command("workflow_reset")
    @click.option("-w", "--workflow_id", type=int, required=True)
    @click.option("-p", "--partial_reset", is_flag=True)
    @click.pass_context
    def workflow_reset(ctx, workflow_id, partial_reset):
        """[DEPRECATED] Use 'jobmon workflow reset' instead."""
        emit_deprecation_warning("workflow_reset", "workflow reset")
        from jobmon.client.status_commands import workflow_reset as workflow_reset_cmd

        result = workflow_reset_cmd(workflow_id, partial_reset)
        click.echo(result)

    @cli.command("workflow_resume")
    @click.option("-w", "--workflow_id", type=int, required=True)
    @click.option("-c", "--cluster_name", type=str, required=True)
    @click.option("--reset-running-jobs", is_flag=True)
    @click.option("-t", "--timeout", type=int, default=180)
    @click.option("--seconds-until-timeout", type=int, default=36000)
    @click.option("--use-original-resources", is_flag=True)
    @click.option("--force-cleanup", is_flag=True)
    @click.pass_context
    def workflow_resume(
        ctx,
        workflow_id,
        cluster_name,
        reset_running_jobs,
        timeout,
        seconds_until_timeout,
        use_original_resources,
        force_cleanup,
    ):
        """[DEPRECATED] Use 'jobmon workflow resume' instead."""
        emit_deprecation_warning("workflow_resume", "workflow resume")
        from jobmon.client.status_commands import resume_workflow_from_id

        resume_workflow_from_id(
            workflow_id=workflow_id,
            cluster_name=cluster_name,
            reset_if_running=reset_running_jobs,
            timeout=timeout,
            seconds_until_timeout=seconds_until_timeout,
            increase_resource=not use_original_resources,
            force_cleanup=force_cleanup,
        )

    @cli.command("task_status")
    @click.option("-t", "--task_ids", type=int, multiple=True, required=True)
    @click.option("-s", "--status", type=str, multiple=True)
    @click.option("-n", "--json", "use_json", is_flag=True)
    @click.pass_context
    def task_status(ctx, task_ids, status, use_json):
        """[DEPRECATED] Use 'jobmon task status' instead."""
        emit_deprecation_warning("task_status", "task status")
        from tabulate import tabulate

        from jobmon.client.status_commands import task_status as task_status_cmd

        task_ids_list = list(task_ids)
        statuses = list(status) if status else None
        click.echo(f"\nTASK_IDS: {task_ids_list}")
        df = task_status_cmd(task_ids_list, statuses, use_json)
        if use_json:
            click.echo(df)
        else:
            click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @cli.command("update_task_status")
    @click.option("-t", "--task_ids", type=int, multiple=True, required=True)
    @click.option("-w", "--workflow_id", type=int, required=True)
    @click.option("-s", "--new_status", type=click.Choice(["D", "G"]), required=True)
    @click.option("-f", "--force", is_flag=True)
    @click.option("-r", "--recursive", is_flag=True)
    @click.pass_context
    def update_task_status(ctx, task_ids, workflow_id, new_status, force, recursive):
        """[DEPRECATED] Use 'jobmon task update' instead."""
        emit_deprecation_warning("update_task_status", "task update")
        from jobmon.client.status_commands import update_task_status as update_task_cmd

        result = update_task_cmd(
            list(task_ids), workflow_id, new_status, force, recursive
        )
        click.echo(f"Response is: {result}")

    @cli.command("task_dependencies")
    @click.option("-t", "--task_id", type=int, required=True)
    @click.pass_context
    def task_dependencies(ctx, task_id):
        """[DEPRECATED] Use 'jobmon task dependencies' instead."""
        emit_deprecation_warning("task_dependencies", "task dependencies")
        from jobmon.client.status_commands import get_task_dependencies

        result = get_task_dependencies(task_id)
        up = result["up"]
        down = result["down"]
        click.echo("Upstream Tasks:\n")
        click.echo("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in up:
            click.echo("{:<8} {:<15} {:<15}".format("", item["id"], item["status"]))
        click.echo("\nDownstream Tasks:\n")
        click.echo("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in down:
            click.echo("{:<8} {:<15} {:<15}".format("", item["id"], item["status"]))

    @cli.command("concurrency_limit")
    @click.option("-w", "--workflow_id", type=int, required=True)
    @click.option("-m", "--max_tasks", type=int, required=True)
    @click.pass_context
    def concurrency_limit(ctx, workflow_id, max_tasks):
        """[DEPRECATED] Use 'jobmon workflow concurrency' instead."""
        emit_deprecation_warning("concurrency_limit", "workflow concurrency")
        from jobmon.client.status_commands import concurrency_limit as concurrency_cmd

        result = concurrency_cmd(workflow_id, max_tasks)
        click.echo(result)

    @cli.command("get_filepaths")
    @click.option("-w", "--workflow_id", type=int, required=True)
    @click.option("-a", "--array_name", type=str, default="")
    @click.option("-j", "--job_name", type=str, default="")
    @click.option("-l", "--limit", type=int, default=5)
    @click.option("-n", "--json", "use_json", is_flag=True)
    @click.pass_context
    def get_filepaths(ctx, workflow_id, array_name, job_name, limit, use_json):
        """[DEPRECATED] Use 'jobmon workflow logs' instead."""
        emit_deprecation_warning("get_filepaths", "workflow logs")
        from tabulate import tabulate

        from jobmon.client.status_commands import get_filepaths as get_filepaths_cmd

        df = get_filepaths_cmd(workflow_id, array_name, job_name, limit)
        if use_json:
            click.echo(df)
        else:
            click.echo(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @cli.command("task_template_resources")
    @click.option("-t", "--task_template_version", type=int, required=True)
    @click.option("-w", "--workflows", type=int, multiple=True)
    @click.option("-a", "--node_args", type=str, default=None)
    @click.pass_context
    def task_template_resources(ctx, task_template_version, workflows, node_args):
        """[DEPRECATED] Use 'jobmon workflow resources usage' instead."""
        emit_deprecation_warning("task_template_resources", "workflow resources usage")
        import json

        from jobmon.client.status_commands import (
            task_template_resources as tt_resources,
        )

        parsed_node_args = None
        if node_args:
            parsed_node_args = json.loads(node_args)
        workflow_list = list(workflows) if workflows else None
        result = tt_resources(task_template_version, workflow_list, parsed_node_args)
        click.echo(result)

    @cli.command("create_resource_yaml")
    @click.option("-w", "--workflow_id", type=int, default=None)
    @click.option("-t", "--task_id", type=int, default=None)
    @click.option(
        "--value_mem", type=click.Choice(["avg", "min", "max"]), default="avg"
    )
    @click.option(
        "--value_core", type=click.Choice(["avg", "min", "max"]), default="avg"
    )
    @click.option(
        "--value_runtime", type=click.Choice(["avg", "min", "max"]), default="max"
    )
    @click.option("-f", "--file", "output_file", type=str, default=None)
    @click.option("-p", "--print", "print_output", is_flag=True)
    @click.option("-c", "--clusters", type=str, multiple=True, default=["slurm"])
    @click.pass_context
    def create_resource_yaml(
        ctx,
        workflow_id,
        task_id,
        value_mem,
        value_core,
        value_runtime,
        output_file,
        print_output,
        clusters,
    ):
        """[DEPRECATED] Use 'jobmon workflow resources yaml' instead."""
        emit_deprecation_warning("create_resource_yaml", "workflow resources yaml")
        from jobmon.client.status_commands import create_resource_yaml as create_yaml

        if (workflow_id is None) == (task_id is None):
            click.echo("Please provide a value for either -w or -t but not both.")
            return

        result = create_yaml(
            workflow_id, task_id, value_mem, value_core, value_runtime, list(clusters)
        )
        if print_output:
            click.echo(result)
        if output_file:
            with open(output_file, "w") as f:
                f.write(result)

    @cli.command("update_config")
    @click.argument("key")
    @click.argument("value")
    @click.option("--config-file", type=str, default=None)
    @click.pass_context
    def update_config(ctx, key, value, config_file):
        """[DEPRECATED] Use 'jobmon config set' instead."""
        emit_deprecation_warning("update_config", "config set")
        from jobmon.client.status_commands import update_config_value

        try:
            result = update_config_value(key, value, config_file)
            click.echo(result)
        except ValueError as e:
            click.echo(f"Error: {e}")
            ctx.exit(1)

    # Note: No legacy version command - the new 'jobmon version' command
    # is sufficient and adds the --components flag
