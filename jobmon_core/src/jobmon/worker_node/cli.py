"""Command line interface for Execution."""
import argparse
import logging
import sys
from typing import Optional

from jobmon.core.cli import CLI

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
