from jobmon.core.constants import TaskInstanceStatus
from jobmon.plugins.sequential.seq_distributor import (
    SequentialDistributor,
    SequentialWorkerNode,
)


def test_seq_kill_self_state():
    """
    mock the error status
    """

    expected_words = "job was in kill self state"
    executor = SequentialDistributor("sequential")
    executor._exit_info = {1: 199}
    r_value, r_msg = executor.get_remote_exit_info(1)
    assert r_value == TaskInstanceStatus.UNKNOWN_ERROR
    assert expected_words in r_msg


def test_get_queueing_errors_returns_empty():
    """get_queueing_errors should return an empty dict, not raise."""
    executor = SequentialDistributor("sequential")
    result = executor.get_queueing_errors(["1", "2", "3"])
    assert result == {}


def test_get_submitted_or_running_returns_empty_set():
    """Sequential tasks complete synchronously, so nothing is running."""
    executor = SequentialDistributor("sequential")
    result = executor.get_submitted_or_running()
    assert result == set()
    assert isinstance(result, set)


def test_get_usage_stats_returns_expected_keys():
    """get_usage_stats should return server-ready keys."""
    worker = SequentialWorkerNode()
    stats = worker.get_usage_stats()
    assert "maxrss" in stats
    assert "cpu" in stats
    assert "usage_str" in stats
    assert isinstance(stats["maxrss"], str)
    assert isinstance(stats["cpu"], str)
    assert isinstance(stats["usage_str"], str)
