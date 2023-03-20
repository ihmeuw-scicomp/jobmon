"""Places to notify upon certain events (ex. slack to notify of unhealthy workflow)."""
import logging
from typing import Optional

import requests

from jobmon.core.configuration import JobmonConfig

logger = logging.getLogger(__name__)


class SlackNotifier(object):
    """Send notifications via slack."""

    def __init__(
        self, api_url: str = "", token: str = "", channel_default: str = ""
    ) -> None:
        """Container for connection with Slack.

        Args:
            api_url (str): url to Slack.
            token (str): token gotten from your app in api.slack.com.
            channel_default (str): name of channel to which you want to post.
        """
        config = JobmonConfig()
        if not api_url:
            api_url = config.get("reaper", "slack_api_url")
        if not token:
            token = config.get("reaper", "slack_token")
        if not channel_default:
            channel_default = config.get("reaper", "slack_channel_default")

        self.api_url = api_url
        self._token = token
        self.channel_default = channel_default

    def send(self, msg: str, channel: Optional[str] = None) -> None:
        """Send message to Slack using requests.post."""
        if channel is None:
            channel = self.channel_default
        resp = requests.post(
            self.api_url,
            headers={"Authorization": "Bearer {}".format(self._token)},
            json={"channel": channel, "text": msg},
        )
        logger.debug(resp)
        if resp.status_code != requests.codes.OK:
            error = "Could not send Slack message. {!r}".format(resp.content)
            # To raise an exception here causes the docker container stop, and
            # becomes hard to restart.
            # Log the error instead. So we can enter the container to fix
            # issues when necessary.
            # Log the status code so that it's easier to identify the cause.
            logger.error(resp.status_code)
            logger.error(error)
