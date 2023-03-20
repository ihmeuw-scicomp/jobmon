"""Start up workflow reaper service."""
from typing import Callable, Optional


from jobmon.core.exceptions import ConfigError
from jobmon.core.requester import Requester
from jobmon.server.workflow_reaper.notifiers import SlackNotifier
from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper


def start_workflow_reaper(
    service_url: str = "",
    slack_api_url: str = "",
    slack_token: str = "",
    slack_channel_default: str = "",
    poll_interval_minutes: Optional[int] = None,
) -> None:
    """Start monitoring for lost workflow runs."""
    # build slack notifier
    wf_sink: Optional[Callable[[str, Optional[str]], None]] = None
    if slack_api_url or slack_token or slack_channel_default:
        try:
            wf_notifier = SlackNotifier(
                api_url=slack_api_url,
                token=slack_token,
                channel_default=slack_channel_default,
            )
            wf_sink = wf_notifier.send
        except ConfigError:
            pass

    # construct requester
    requester: Optional[Requester] = None
    if service_url:
        requester = Requester(service_url)

    poll_interval_seconds: Optional[int] = None
    if poll_interval_minutes is not None:
        poll_interval_seconds = poll_interval_minutes * 60

    reaper = WorkflowReaper(
        poll_interval_seconds=poll_interval_seconds,
        requester=requester,
        wf_notification_sink=wf_sink,
    )
    reaper.monitor_forever()
