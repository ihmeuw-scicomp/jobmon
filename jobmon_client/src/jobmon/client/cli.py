"""Client command line interface for workflow/task status and concurrency limiting."""

import argparse
import json
from typing import Any, Optional

from jobmon.client.status_commands import get_task_dependencies
from jobmon.core.cli import CLI


class _HelpAction(argparse._HelpAction):
    """To show help for all subparsers in one place."""

    def __call__(
        self,
        parser: Any,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Optional[str] = None,
    ) -> None:
        """Add subparsers' help info when jobmon --help is called."""
        print(parser.format_help())
        subparsers_actions = [
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ]
        print("Jobmon Usage Options:")
        for sub_action in subparsers_actions:
            for choice, subparser in sub_action.choices.items():
                print(
                    f"{choice.upper()} (for more information specify 'jobmon {choice} "
                    f"--help'):"
                )
                print(subparser.format_usage())
        parser.exit()


class ClientCLI(CLI):
    """Client command line interface for workflow/task status and concurrency limiting."""

    def __init__(self) -> None:
        """Initialization of client CLI."""
        self.parser = argparse.ArgumentParser("Jobmon Client CLI", add_help=False)
        self.parser.add_argument(
            "--help", action=_HelpAction, help="Help if you need Help"
        )
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=argparse.ArgumentParser
        )

        self._add_workflow_status_subparser()
        self._add_workflow_tasks_subparser()
        self._add_task_template_resources_subparser()
        self._add_task_status_subparser()
        self._add_update_task_status_subparser()
        self._add_concurrency_limit_subparser()
        self._add_version_subparser()
        self._add_task_dependencies_subparser()
        self._add_workflow_reset_subparser()
        self._add_create_resource_yaml_subparser()
        self._add_get_filepaths_subparser()
        self._add_resume_workflow_parser()

    @staticmethod
    def limit_checker(limit: Any) -> int:
        """Coerce to int and check that the limit is greater than 0."""
        limit = int(limit)
        if limit <= 0:
            raise argparse.ArgumentTypeError(
                f"Limit value must be greater than 0. The limit "
                f"value passed in was: {limit}"
            )
        return limit

    @staticmethod
    def workflow_status(args: argparse.Namespace) -> None:
        """Workflow status checking options."""
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_status as workflow_status_cmd

        df = workflow_status_cmd(args.workflow_id, args.user, args.json, args.limit)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @staticmethod
    def workflow_tasks(args: argparse.Namespace) -> None:
        """Check the tasks for a given workflow."""
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_tasks as workflow_tasks_cmd

        df = workflow_tasks_cmd(args.workflow_id, args.status, args.json, args.limit)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @staticmethod
    def task_template_resources(args: argparse.Namespace) -> None:
        """Aggregates the resource usage for a given TaskTemplateVersion."""
        from jobmon.client.status_commands import task_template_resources

        resources = task_template_resources(
            args.task_template_version, args.workflows, args.node_args
        )
        print(resources)

    @staticmethod
    def task_status(args: argparse.Namespace) -> None:
        """Check task status."""
        from tabulate import tabulate
        from jobmon.client.status_commands import task_status as task_status_cmd

        df = task_status_cmd(args.task_ids, args.status, args.json)
        print(f"\nTASK_IDS: {args.task_ids}")
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @staticmethod
    def update_task_status(args: argparse.Namespace) -> None:
        """Manually update task status for resumes, reruns, etc."""
        from jobmon.client.status_commands import update_task_status

        response = update_task_status(
            args.task_ids,
            args.workflow_id,
            args.new_status,
            args.force,
            args.recursive,
        )
        print(f"Response is: {response}")

    @staticmethod
    def concurrency_limit(args: argparse.Namespace) -> None:
        """Set a limit for the number of tasks that can run concurrently."""
        from jobmon.client.status_commands import (
            concurrency_limit as concurrency_limit_cmd,
        )

        response = concurrency_limit_cmd(args.workflow_id, args.max_tasks)
        print(response)

    @staticmethod
    def task_dependencies(args: argparse.Namespace) -> None:
        """Get task's upstream and downstream tasks and their status."""
        r = get_task_dependencies(args.task_id)
        up = r["up"]
        down = r["down"]
        """Format output that should look like:
        Upstream Tasks:

             Task ID         Status
             1               D

        Downstream Tasks:

            Task ID         Status
            3               D
            4               D
        """
        print("Upstream Tasks:\n")
        print("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in up:
            task_id = item["id"]
            status = item["status"]
            print("{:<8} {:<15} {:<15}".format("", task_id, status))
        print("\nDownstream Tasks:\n")
        print("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in down:
            task_id = item["id"]
            status = item["status"]
            print("{:<8} {:<15} {:<15}".format("", task_id, status))

    @staticmethod
    def workflow_reset(args: argparse.Namespace) -> None:
        """Manually reset a workflow."""
        from jobmon.client.status_commands import workflow_reset

        response = workflow_reset(
            workflow_id=args.workflow_id, partial_reset=args.partial_reset
        )
        print(f"Response is: {response}")

    @staticmethod
    def jobmon_version(args: argparse.Namespace) -> None:
        """Return the jobmon version."""
        from jobmon.client import __version__

        print(__version__)

    @staticmethod
    def resource_yaml(args: argparse.Namespace) -> None:
        """Create resource yaml."""
        from jobmon.client.status_commands import create_resource_yaml

        # input check
        if (args.workflow_id is None) ^ (args.task_id is None):
            r = create_resource_yaml(
                args.workflow_id,
                args.task_id,
                args.value_mem,
                args.value_core,
                args.value_runtime,
                args.clusters,
            )
            if args.print:
                print(r)
            if args.file:
                f = open(args.file, "w")
                f.write(r)
                f.close()
        else:
            print("Please provide a value for either -w or -t but not both.")

    @staticmethod
    def resume_workflow(args: argparse.Namespace) -> None:
        """Resume a workflow from a workflow ID."""
        from jobmon.client.status_commands import resume_workflow_from_id

        resume_workflow_from_id(
            workflow_id=args.workflow_id,
            cluster_name=args.cluster_name,
            reset_if_running=args.reset_running_jobs,
            timeout=args.timeout,
        )

    @staticmethod
    def get_filepaths(args: argparse.Namespace) -> None:
        from tabulate import tabulate
        from jobmon.client.status_commands import get_filepaths

        df = get_filepaths(
            workflow_id=args.workflow_id,
            array_name=args.array_name,
            job_name=args.job_name,
            limit=args.limit,
        )
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def _add_version_subparser(self) -> None:
        version_parser = self._subparsers.add_parser("version")
        version_parser.set_defaults(func=self.jobmon_version)

    def _add_workflow_status_subparser(self) -> None:
        workflow_status_parser = self._subparsers.add_parser("workflow_status")
        workflow_status_parser.set_defaults(func=self.workflow_status)
        workflow_status_parser.add_argument(
            "-w",
            "--workflow_id",
            nargs="*",
            help="list of workflow_ids",
            required=False,
            type=int,
            action="append",
            default=[],
        )
        workflow_status_parser.add_argument(
            "-u", "--user", nargs="*", help="list of users", required=False, type=str
        )
        workflow_status_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )
        workflow_status_parser.add_argument(
            "-l",
            "--limit",
            default=5,
            help="limit the number of returning records; default is 5",
            required=False,
            type=self.limit_checker,
        )

    def _add_workflow_tasks_subparser(self) -> None:
        workflow_tasks_parser = self._subparsers.add_parser("workflow_tasks")
        workflow_tasks_parser.set_defaults(func=self.workflow_tasks)
        workflow_tasks_parser.add_argument(
            "-w",
            "--workflow_id",
            help="workflow_id to get task statuses for",
            required=True,
            type=int,
        )
        workflow_tasks_parser.add_argument(
            "-s",
            "--status",
            nargs="*",
            help="limit tasks to a status (PENDING, RUNNING, DONE, FATAL)",
            choices=[
                "PENDING",
                "RUNNING",
                "DONE",
                "FATAL",
                "pending",
                "running",
                "done",
                "fatal",
            ],
            required=False,
        )
        workflow_tasks_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )
        workflow_tasks_parser.add_argument(
            "-l",
            "--limit",
            default=5,
            help="limit the number of returning records; default is 5",
            required=False,
            type=self.limit_checker,
        )

    def _add_task_template_resources_subparser(self) -> None:
        tt_resources_parser = self._subparsers.add_parser("task_template_resources")
        tt_resources_parser.set_defaults(func=self.task_template_resources)
        tt_resources_parser.add_argument(
            "-t",
            "--task_template_version",
            help="TaskTemplateVersion ID to get resource usage for",
            required=True,
            type=int,
        )
        tt_resources_parser.add_argument(
            "-w",
            "--workflows",
            nargs="*",
            help="list of workflow IDs to query by",
            required=False,
            type=int,
        )
        tt_resources_parser.add_argument(
            "-a",
            "--node_args",
            help="dictionary of node arguments to query by",
            required=False,
            type=json.loads,
        )

    def _add_task_status_subparser(self) -> None:
        task_status_parser = self._subparsers.add_parser("task_status")
        task_status_parser.set_defaults(func=self.task_status)
        task_status_parser.add_argument(
            "-t",
            "--task_ids",
            nargs="+",
            help="task_ids to get task statuses for",
            required=True,
            type=int,
        )
        task_status_parser.add_argument(
            "-s",
            "--status",
            nargs="*",
            help="limit task instances to statuses (PENDING, RUNNING, DONE, FATAL)",
            choices=[
                "PENDING",
                "RUNNING",
                "DONE",
                "FATAL",
                "pending",
                "running",
                "done",
                "fatal",
            ],
            required=False,
        )
        task_status_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )

    def _add_update_task_status_subparser(self) -> None:
        update_task_parser = self._subparsers.add_parser("update_task_status")
        update_task_parser.set_defaults(func=self.update_task_status)
        update_task_parser.add_argument(
            "-t",
            "--task_ids",
            nargs="+",
            help="task_ids to reset",
            required=True,
            type=int,
        )
        update_task_parser.add_argument(
            "-w",
            "--workflow_id",
            help="workflow_id of the tasks to reset",
            required=True,
            type=int,
        )
        update_task_parser.add_argument(
            "-s",
            "--new_status",
            help='Status to set to. "D" for DONE; "G" for REGISTERED/(pending).',
            choices=["D", "G"],
            type=str,
        )
        update_task_parser.add_argument(
            "-f",
            "--force",
            help="If set, allow all source statuses and all workflow statuses.",
            default=False,
            action="store_true",
        )
        update_task_parser.add_argument(
            "-r",
            "--recursive",
            help="If used with --force, Jobmon will apply recursive update_status downstream "
            "or upstream depending on new_status ",
            default=False,
            action="store_true",
        )

    def _add_concurrency_limit_subparser(self) -> None:
        concurrency_limit_parser = self._subparsers.add_parser("concurrency_limit")
        concurrency_limit_parser.set_defaults(func=self.concurrency_limit)
        concurrency_limit_parser.add_argument(
            "-w",
            "--workflow_id",
            required=True,
            type=int,
            help="Workflow ID of the workflow to be adjusted",
        )

        # Define a custom function to validate the user's input.
        def _validate_ntasks(x: Any) -> int:
            try:
                x = int(x)
            except ValueError:
                raise argparse.ArgumentTypeError(f"{x} is not coercible to an integer.")
            if x < 0:
                raise argparse.ArgumentTypeError(
                    "Max concurrent tasks must be at least 0."
                )
            return x

        concurrency_limit_parser.add_argument(
            "-m",
            "--max_tasks",
            required=True,
            type=_validate_ntasks,
            help="Number of concurrent tasks to allow. Must be at least 1.",
        )

    def _add_task_dependencies_subparser(self) -> None:
        task_dependencies_parser = self._subparsers.add_parser("task_dependencies")
        task_dependencies_parser.set_defaults(func=self.task_dependencies)
        task_dependencies_parser.add_argument(
            "-t", "--task_id", help="list of task dependencies", required=True, type=int
        )

    def _add_workflow_reset_subparser(self) -> None:
        workflow_reset_parser = self._subparsers.add_parser("workflow_reset")
        workflow_reset_parser.set_defaults(func=self.workflow_reset)
        workflow_reset_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id to reset", required=True, type=int
        )
        workflow_reset_parser.add_argument(
            "-p",
            "--partial_reset",
            help="Set to indicate Done tasks will not be reset",
            required=False,
            action="store_true",
        )

    def _add_create_resource_yaml_subparser(self) -> None:
        create_resource_yaml_parser = self._subparsers.add_parser(
            "create_resource_yaml"
        )
        create_resource_yaml_parser.set_defaults(func=self.resource_yaml)
        create_resource_yaml_parser.add_argument(
            "-w",
            "--workflow_id",
            help="The workflow id to generate resource YAML. "
            "Must provide either -w or -t.",
            required=False,
            type=int,
        )
        create_resource_yaml_parser.add_argument(
            "-t",
            "--task_id",
            help="The workflow id to generate resource YAML. "
            "Must provide either -w or -t.",
            required=False,
            type=int,
        )
        create_resource_yaml_parser.add_argument(
            "--value_mem",
            help="The algorithm to get memory usage. Default avg.",
            choices=["avg", "max", "min"],
            required=False,
            default="avg",
            type=str,
        )
        create_resource_yaml_parser.add_argument(
            "--value_core",
            help="The algorithm to get core requested. Default avg.",
            choices=["avg", "max", "min"],
            required=False,
            default="avg",
            type=str,
        )
        create_resource_yaml_parser.add_argument(
            "--value_runtime",
            help="The algorithm to get runtime. Default max.",
            choices=["avg", "max", "min"],
            required=False,
            default="max",
            type=str,
        )
        create_resource_yaml_parser.add_argument(
            "-f",
            "--file",
            help="The file to save the YAML.",
            required=False,
            default=None,
            type=str,
        )
        create_resource_yaml_parser.add_argument(
            "-p",
            "--print",
            help="Print the result YAMl to standard output.",
            required=False,
            default=False,
            action="store_true",
        )
        create_resource_yaml_parser.add_argument(
            "-c",
            "--clusters",
            nargs="+",
            help="The clusters for the YAML.",
            required=False,
            default=["slurm"],
            type=str,
        )

    def _add_get_filepaths_subparser(self) -> None:
        get_filepaths_parser = self._subparsers.add_parser("get_filepaths")
        get_filepaths_parser.set_defaults(func=self.get_filepaths)
        get_filepaths_parser.add_argument(
            "-w",
            "--workflow_id",
            help="workflow_id to filter by",
            required=True,
            type=int,
        )
        get_filepaths_parser.add_argument(
            "-a",
            "--array_name",
            help="array name to filter by",
            required=False,
            default="",
            type=str,
        )
        get_filepaths_parser.add_argument(
            "-j",
            "--job_name",
            help="job name to filter by",
            required=False,
            default="",
            type=str,
        )
        get_filepaths_parser.add_argument(
            "-l",
            "--limit",
            default=5,
            help="Limit the number of returning records; default is 5",
            required=False,
            type=self.limit_checker,
        )
        get_filepaths_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )

    def _add_resume_workflow_parser(self) -> None:
        workflow_resume_parser = self._subparsers.add_parser("workflow_resume")
        workflow_resume_parser.set_defaults(func=self.resume_workflow)
        workflow_resume_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id to resume", required=True, type=int
        )
        # TODO: perhaps provide a mechanism to infer the last cluster this
        # workflow was run on
        workflow_resume_parser.add_argument(
            "-c",
            "--cluster_name",
            help="cluster to run this workflow on, e.g. 'slurm', 'slurm_test', 'dummy'",
            required=True,
        )
        workflow_resume_parser.add_argument(
            "--reset-running-jobs",
            help="whether to reset running jobs or not",
            required=False,
            action="store_true",
        )
        workflow_resume_parser.add_argument(
            "-t",
            "--timeout",
            help="timeout for resume command",
            required=False,
            default=180,
        )


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ClientCLI()
    cli.main(argstr)
