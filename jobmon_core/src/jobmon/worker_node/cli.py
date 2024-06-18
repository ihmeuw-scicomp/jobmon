"""Command line interface for Execution."""

import argparse
import ast
import importlib
import importlib.machinery
import importlib.util
import logging
import os
import sys
from typing import Optional

from jobmon.core.cli import CLI
from jobmon.core.task_generator import TaskGenerator

logger = logging.getLogger(__name__)


class WorkerNodeCLI(CLI):
    """Command line interface for WorkderNode."""

    def __init__(self) -> None:
        """Initialization of the worker node CLI."""
        self.parser = argparse.ArgumentParser("jobmon worker_node CLI")
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=argparse.ArgumentParser
        )

        self._add_worker_node_job_parser()
        self._add_worker_node_array_parser()
        self._add_run_task_generator_parser()

    def run_task_instance_job(self, args: argparse.Namespace) -> int:
        """Configuration for the jobmon worker node."""
        from jobmon.core.exceptions import ReturnCodes
        from jobmon.worker_node import __version__
        from jobmon.worker_node.worker_node_factory import WorkerNodeFactory

        if __version__ != args.expected_jobmon_version:
            msg = (
                f"Your expected Jobmon version is {args.expected_jobmon_version} and your "
                f"worker node is using {__version__}. Please check your bash profile "
            )
            logger.error(msg)
            sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

        worker_node_factory = WorkerNodeFactory(cluster_name=args.cluster_name)
        worker_node_task_instance = worker_node_factory.get_job_task_instance(
            task_instance_id=args.task_instance_id
        )
        worker_node_task_instance.configure_logging()
        try:
            worker_node_task_instance.run()
        except Exception as e:
            logger.error(e)
            sys.exit(ReturnCodes.WORKER_NODE_CLI_FAILURE)

        return worker_node_task_instance.command_returncode

    def run_task_instance_array(self, args: argparse.Namespace) -> int:
        """Configuration for the jobmon worker node."""
        from jobmon.core.exceptions import ReturnCodes
        from jobmon.worker_node import __version__
        from jobmon.worker_node.worker_node_factory import WorkerNodeFactory

        if __version__ != args.expected_jobmon_version:
            msg = (
                f"Your expected Jobmon version is {args.expected_jobmon_version} and your "
                f"worker node is using {__version__}. Please check your bash profile "
            )
            logger.error(msg)
            sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

        worker_node_factory = WorkerNodeFactory(cluster_name=args.cluster_name)
        worker_node_task_instance = worker_node_factory.get_array_task_instance(
            array_id=args.array_id,
            batch_number=args.batch_number,
        )
        worker_node_task_instance.configure_logging()

        try:
            worker_node_task_instance.run()
        except Exception as e:
            logger.error(e)
            sys.exit(ReturnCodes.WORKER_NODE_CLI_FAILURE)

        return worker_node_task_instance.command_returncode

    def run_task_generator(self, args: argparse.Namespace) -> int:
        from jobmon.core.exceptions import ReturnCodes
        from jobmon.worker_node import __version__

        # if the user used the --args flag, parse the args and run the task generator
        if args.args:
            arg_dict = {}
            pairs = args.args.split(";")

            for pair in pairs:
                key, value = pair.split("=")
                if value.startswith("[") and value.endswith("]"):
                    value = ast.literal_eval(value)
                arg_dict[key] = value

        if __version__ != args.expected_jobmon_version:
            msg = (
                f"Your expected Jobmon version is {args.expected_jobmon_version} and your "
                f"worker node is using {__version__}. Please check your bash profile "
            )
            logger.error(msg)
            sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

        # Import the module and get the task generator we've been pointed to, raise an error
        # if it's not a TaskGenerator
        # if the user used the --module_dir flag, add the module directory to the path
        if args.module_source_path:
            # Create a module spec from the source file
            loader = importlib.machinery.SourceFileLoader(
                args.module_name, os.path.expanduser(args.module_source_path)
            )
            spec = importlib.util.spec_from_loader(loader.name, loader)
            # Create a new module based on the spec
            mod = importlib.util.module_from_spec(spec)  # type: ignore
            # Add the module to sys.modules
            sys.modules[args.module_name] = mod
            loader.exec_module(mod)
        else:
            mod = importlib.import_module(args.module_name)
        task_generator = getattr(mod, args.func_name)
        if not isinstance(task_generator, TaskGenerator):
            raise ValueError(
                f"{args.module_name}:{args.func_name} doesn't point to a runnable jobmon task."
            )

        # if the user used the --arghelp flag, print the help message for the task generator
        if args.arghelp:
            print(task_generator.help())
            return ReturnCodes.OK
        task_generator.run(arg_dict)
        return ReturnCodes.OK

    def _add_run_task_generator_parser(self) -> None:
        generator_parser = self._subparsers.add_parser("task_generator")
        generator_parser.set_defaults(func=self.run_task_generator)
        generator_parser.add_argument(
            "--module_name",
            help="name of the module containing the TaskGenerator",
            required=True,
        )
        generator_parser.add_argument(
            "--func_name",
            type=str,
            help="the name of the function which has been turned into a TaskGenerator",
            required=True,
        )
        generator_parser.add_argument(
            "--args",
            type=str,
            help="Pair the args with the params of the function, seperated by `;`. "
            'For example: --args "arg1=1; arg2=[2, 3]"',
            required=False,
        )
        generator_parser.add_argument(
            "--arghelp",
            type=str,
            help="Show the help message for the task generator. For example: --arghelp",
            required=False,
        )
        generator_parser.add_argument(
            "--expected_jobmon_version",
            type=str,
            help="expected_jobmon_version of the work node.",
            required=True,
        )
        generator_parser.add_argument(
            "--module_source_path",
            type=str,
            help="The directory the module source code located; "
            "you do not need this if the module is installed in your system.",
            required=False,
        )

    def _add_worker_node_job_parser(self) -> None:
        job_parser = self._subparsers.add_parser("worker_node_job")
        job_parser.set_defaults(func=self.run_task_instance_job)
        job_parser.add_argument(
            "--task_instance_id",
            help="task_instance_id of the work node.",
            required=False,
        )
        job_parser.add_argument(
            "--cluster_name",
            type=str,
            help="cluster_name of the work node.",
            required=True,
        )
        job_parser.add_argument(
            "--expected_jobmon_version",
            type=str,
            help="expected_jobmon_version of the work node.",
            required=True,
        )

    def _add_worker_node_array_parser(self) -> None:
        array_parser = self._subparsers.add_parser("worker_node_array")
        array_parser.set_defaults(func=self.run_task_instance_array)
        array_parser.add_argument(
            "--array_id",
            help="array_id of the worker node if this is an array task.",
            required=False,
        )
        array_parser.add_argument(
            "--batch_number",
            help="batch number of the array this task instance is associated with.",
            required=False,
        )
        array_parser.add_argument(
            "--cluster_name",
            type=str,
            help="cluster_name of the work node.",
            required=True,
        )
        array_parser.add_argument(
            "--expected_jobmon_version",
            type=str,
            help="expected_jobmon_version of the work node.",
            required=True,
        )


def run(argstr: Optional[str] = None) -> None:
    """Entrypoint to create WorkerNode CLI."""
    cli = WorkerNodeCLI()
    cli.main(argstr)
