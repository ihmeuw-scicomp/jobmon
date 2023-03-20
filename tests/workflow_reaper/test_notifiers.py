from jobmon.server.workflow_reaper.notifiers import SlackNotifier

import pytest


def test_no_raise():
    notifier = SlackNotifier(
        token="fake_token",
        channel_default="jobmon-alerts",
        api_url="https://slack.com/apis/chat.postMessage",
    )
    try:
        notifier.send(msg="This should fail because of fake link")
    except Exception as e:
        print(str(e))
        pytest.fail("There should be no exception")
