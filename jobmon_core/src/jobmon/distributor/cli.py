"""Command line interface for Execution."""

import argparse
from typing import Optional

import structlog

from jobmon.core.cli import CLI
from jobmon.core.cluster import Cluster
from jobmon.distributor.api import DistributorService

logger = structlog.get_logger(__name__)


class DistributorCLI(CLI):
    """Command line interface for Distributor with automatic logging."""

    def __init__(self) -> None:
        """Initialization of distributor CLI with automatic component logging."""
        # Enable automatic logging for distributor component
        super().__init__(component_name="distributor")

        self.parser = argparse.ArgumentParser()
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=argparse.ArgumentParser
        )

        self._add_distributor_parser()

    @staticmethod
    def run_distributor(args: argparse.Namespace) -> None:
        """Start the distributor service for a workflow run."""
        # Bind global context for this distributor instance
        structlog.contextvars.bind_contextvars(workflow_run_id=args.workflow_run_id)

        logger.info("Distributor starting")

        cluster = Cluster.get_cluster(args.cluster_name)
        cluster_interface = cluster.get_distributor()

        distributor_service = DistributorService(cluster_interface)
        distributor_service.set_workflow_run(args.workflow_run_id)
        distributor_service.run()

    def _add_distributor_parser(self) -> None:
        distributor_parser = self._subparsers.add_parser("start")
        distributor_parser.set_defaults(func=self.run_distributor)
        distributor_parser.add_argument(
            "--cluster_name",
            type=str,
            help="cluster_name to distribute jobs onto.",
            required=True,
        )
        distributor_parser.add_argument(
            "--workflow_run_id",
            type=int,
            help="workflow_run_id to distribute jobs for.",
            required=True,
        )


def main(argstr: Optional[str] = None) -> None:
    """Entrypoint to create Executor CLI."""
    cli = DistributorCLI()
    cli.main(argstr)


if __name__ == "__main__":
    main()
