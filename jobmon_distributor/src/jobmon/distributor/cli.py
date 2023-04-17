"""Command line interface for Execution."""
import argparse
from typing import Optional

from jobmon.core.cli import CLI
from jobmon.core.cluster import Cluster
from jobmon.distributor.api import DistributorInstance


class DistributorCLI(CLI):
    """Command line interface for Distributor."""

    def __init__(self) -> None:
        """Initialization of distributor CLI."""
        self.parser = argparse.ArgumentParser()
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=argparse.ArgumentParser
        )

        self._add_distributor_parser()

    @staticmethod
    def run_distributor(args: argparse.Namespace) -> None:
        """Configuration for the jobmon worker node."""
        distributor_service = DistributorInstance(
            cluster_name=args.cluster_name,
            workflow_run_id=args.workflow_run_id,
            )
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
        # TODO: Enforce that helm does not provide this argument
        distributor_parser.add_argument(
            "--workflow_run_id",
            type=int,
            help="workflow_run_id to distribute jobs for.",
            required=False,
            default=None,
        )


def main(argstr: Optional[str] = None) -> None:
    """Entrypoint to create Executor CLI."""
    cli = DistributorCLI()
    cli.main(argstr)


if __name__ == "__main__":
    main()
