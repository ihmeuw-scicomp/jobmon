"""Set up server specific CLI config."""
import argparse
import logging
import sys
from typing import Optional

from jobmon.core.cli import CLI

logger = logging.getLogger(__name__)


class ServerCLI(CLI):
    """CLI for Server only."""

    def __init__(self) -> None:
        """Initialize ServerCLI with subcommands."""
        self.parser = argparse.ArgumentParser("jobmon server")
        self._subparsers = self.parser.add_subparsers(dest="sub_command")

        # now add specific sub parsers
        self._add_workflow_reaper_subparser()
        self._add_init_db_subparser()
        self._add_terminate_db_subparser()

    def workflow_reaper(self, args: argparse.Namespace) -> None:
        """Workflow reaper entrypoint logic."""
        from jobmon.server.workflow_reaper.api import start_workflow_reaper

        logging.basicConfig(stream=sys.stdout, level=logging.INFO)
        if args.command == "start":
            start_workflow_reaper(
                service_url=args.service_url,
                slack_api_url=args.slack_api_url,
                slack_token=args.slack_token,
                slack_channel_default=args.slack_channel_default,
                poll_interval_minutes=args.poll_interval_minutes,
            )
        else:
            raise ValueError(
                "Invalid command choice. Options are (start), got " f"({args.command})"
            )

    def init_db(self, args: argparse.Namespace) -> None:
        """Entrypoint to initialize new Jobmon database."""
        import sqlalchemy
        from jobmon.core.configuration import JobmonConfig
        from jobmon.server.web.models import init_db

        sqlalchemy_database_uri = args.sqlalchemy_database_uri
        if not sqlalchemy_database_uri:
            config = JobmonConfig()
            sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
        engine = sqlalchemy.create_engine(sqlalchemy_database_uri)
        init_db(engine)

    def terminate_db(self, args: argparse.Namespace) -> None:
        """Entrypoint to terminate a Jobmon database."""
        import sqlalchemy
        from jobmon.core.configuration import JobmonConfig
        from jobmon.server.web.models import terminate_db

        sqlalchemy_database_uri = args.sqlalchemy_database_uri
        if not sqlalchemy_database_uri:
            config = JobmonConfig()
            sqlalchemy_database_uri = config.get("db", "sqlalchemy_database_uri")
        engine = sqlalchemy.create_engine(sqlalchemy_database_uri)
        terminate_db(engine)

    def _add_workflow_reaper_subparser(self) -> None:
        reaper_parser = self._subparsers.add_parser("workflow_reaper")
        reaper_parser.set_defaults(func=self.workflow_reaper)
        reaper_parser.add_argument(
            "command",
            type=str,
            choices=["start"],
            help=(
                "The workflow_reaper sub-command to run: (start). Start command runs "
                "workflow_reaper.monitor_forever() method."
            ),
        )
        reaper_parser.add_argument(
            "--service_url",
            type=str,
            help="Jobmon web service URL",
            required=False,
            default="",
        )
        reaper_parser.add_argument(
            "--slack_api_url",
            type=str,
            help="URL to post notifications",
            required=False,
            default="",
        )
        reaper_parser.add_argument(
            "--slack_token",
            type=str,
            help="Authentication token for posting updates to slack",
            required=False,
            default="",
        )
        reaper_parser.add_argument(
            "--slack_channel_default",
            type=str,
            help="Default channel to post updates to",
            required=False,
            default="",
        )
        reaper_parser.add_argument(
            "--poll_interval_minutes",
            type=int,
            help="Duration in minutes to sleep between reaper loops",
            required=False,
            default=None,
        )

    def _add_init_db_subparser(self) -> None:
        init_db_parser = self._subparsers.add_parser("init_db")
        init_db_parser.set_defaults(func=self.init_db)
        init_db_parser.add_argument(
            "--sqlalchemy_database_uri",
            type=str,
            help="The connection string for sqlalchemy to use when running the server.",
            required=False,
            default="",
        )

    def _add_terminate_db_subparser(self) -> None:
        terminate_db_parser = self._subparsers.add_parser("terminate_db")
        terminate_db_parser.set_defaults(func=self.terminate_db)
        terminate_db_parser.add_argument(
            "--sqlalchemy_database_uri",
            type=str,
            help="The connection string for sqlalchemy to use when running the server.",
            required=False,
            default="",
        )


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ServerCLI()
    cli.main(argstr)
