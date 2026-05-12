"""Command line interface for Execution."""

import argparse
import sys
from typing import Optional

import structlog

from jobmon.core.cli import CLI
from jobmon.core.cluster import Cluster
from jobmon.core.logging import set_jobmon_context
from jobmon.distributor.api import DistributorService

logger = structlog.get_logger(__name__)


def _phase(name: str) -> None:
    """Emit a startup-phase breadcrumb on stderr.

    The orchestrator's ``DistributorContext.wait_for_startup_signal`` watches
    stderr for the ``ALIVE`` token and, on timeout, captures the full stderr
    buffer into the ``DistributorStartupTimeout`` exception body. These
    breadcrumbs land in that buffer and identify which startup step the
    distributor reached before hanging — invaluable when OTLP context isn't
    bound yet (e.g. shared-filesystem stalls during Python imports or
    ``shutil.which`` in the cluster plugin constructor) so the failure is
    invisible to ES.

    Carefully avoids the substring ``ALIVE`` so the orchestrator's signal
    detector cannot be fooled into thinking startup succeeded prematurely.
    """
    sys.stderr.write(f"JOBMON_PHASE: {name}\n")
    sys.stderr.flush()


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
        _phase("cli_entered")

        # Bind global context for this distributor instance
        set_jobmon_context(workflow_run_id=args.workflow_run_id)
        _phase("context_bound")

        logger.info("Distributor starting")

        # First HTTP roundtrip to the jobmon server — fetches cluster
        # metadata. If this hangs the distributor is blocked on network or
        # on Cluster construction (notably ``shutil.which`` over PATH, which
        # can stall on a slow shared filesystem).
        cluster = Cluster.get_cluster(args.cluster_name)
        _phase("cluster_bound")

        cluster_interface = cluster.get_distributor()
        _phase("cluster_interface_built")

        distributor_service = DistributorService(cluster_interface)
        # Second HTTP roundtrip: transition_to_instantiated (workflow_run
        # B -> I). Hangs here implicate the server-side WFR transition path.
        distributor_service.set_workflow_run(args.workflow_run_id)
        _phase("workflow_run_set")

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
