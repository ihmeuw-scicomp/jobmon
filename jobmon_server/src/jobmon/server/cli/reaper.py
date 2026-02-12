"""Workflow reaper commands for Jobmon Server."""

from typing import Optional

import click


@click.group()
def reaper() -> None:
    r"""Workflow reaper service commands.

    \b
    Commands for managing the workflow reaper background service.

    \b
    Examples:
      jobmon-server reaper start
      jobmon-server reaper start --poll-interval 10
    """


@reaper.command()
@click.option(
    "--service-url",
    type=str,
    default="",
    help="Jobmon web service URL.",
)
@click.option(
    "--poll-interval",
    type=int,
    default=None,
    help="Minutes between reaper loops.",
)
@click.option(
    "--slack-api-url",
    type=str,
    default="",
    help="Slack webhook URL for notifications.",
)
@click.option(
    "--slack-token",
    type=str,
    default="",
    help="Slack authentication token.",
)
@click.option(
    "--slack-channel",
    type=str,
    default="",
    help="Default Slack channel for notifications.",
)
def start(
    service_url: str,
    poll_interval: Optional[int],
    slack_api_url: str,
    slack_token: str,
    slack_channel: str,
) -> None:
    r"""Start the workflow reaper daemon.

    The workflow reaper monitors workflows and performs cleanup tasks
    such as marking stalled workflows as failed.

    \b
    Examples:
      # Use config file settings
      jobmon-server reaper start

      # With custom poll interval
      jobmon-server reaper start --poll-interval 10

      # With Slack notifications
      jobmon-server reaper start --slack-token $SLACK_TOKEN --slack-channel "#jobmon-alerts"
    """
    from jobmon.server.web.logging import configure_server_logging
    from jobmon.server.workflow_reaper.api import start_workflow_reaper

    # Use the same logging configuration as the web server
    configure_server_logging()

    click.echo("Starting workflow reaper...")
    start_workflow_reaper(
        service_url=service_url,
        slack_api_url=slack_api_url,
        slack_token=slack_token,
        slack_channel_default=slack_channel,
        poll_interval_minutes=poll_interval,
    )
