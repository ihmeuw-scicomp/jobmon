import pytest

from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import RemoteExitInfoNotAvailable
from jobmon.plugins.multiprocess.multiproc_distributor import (
    MultiprocessDistributor,
    MultiprocessWorkerNode,
)


@pytest.fixture
def distributor():
    d = MultiprocessDistributor("multiprocess", parallelism=2)
    d.start()
    yield d
    d.stop()


def test_get_remote_exit_info_kill_self(distributor):
    """Exit code 199 should return UNKNOWN_ERROR with kill self message."""
    distributor._exit_info["1"] = 199
    status, msg = distributor.get_remote_exit_info("1")
    assert status == TaskInstanceStatus.UNKNOWN_ERROR
    assert "kill self" in msg


def test_get_remote_exit_info_nonzero(distributor):
    """Non-zero exit code should return UNKNOWN_ERROR with the code."""
    distributor._exit_info["2"] = 1
    status, msg = distributor.get_remote_exit_info("2")
    assert status == TaskInstanceStatus.UNKNOWN_ERROR
    assert "1" in msg


def test_get_remote_exit_info_zero(distributor):
    """Zero exit code should still return UNKNOWN_ERROR (triaging path)."""
    distributor._exit_info["3"] = 0
    status, msg = distributor.get_remote_exit_info("3")
    assert status == TaskInstanceStatus.UNKNOWN_ERROR
    assert "0" in msg


def test_get_remote_exit_info_missing(distributor):
    """Missing distributor_id should raise RemoteExitInfoNotAvailable."""
    with pytest.raises(RemoteExitInfoNotAvailable):
        distributor.get_remote_exit_info("nonexistent")


def test_exit_info_from_real_subprocess(distributor):
    """Submit a real command and verify exit code is captured."""
    distributor._run_task("10", "true", {})
    assert distributor._exit_info["10"] == 0

    distributor._run_task("11", "false", {})
    assert distributor._exit_info["11"] != 0


def test_get_queueing_errors_captures_failure(distributor):
    """A command that fails to Popen should be captured as a queueing error."""
    distributor._run_task("20", "/nonexistent/binary", {})
    errors = distributor.get_queueing_errors(["20"])
    assert "20" in errors
    assert "No such file" in errors["20"] or "not found" in errors["20"]


def test_get_queueing_errors_empty_for_success(distributor):
    """Successful commands should not appear in queueing errors."""
    distributor._run_task("21", "true", {})
    errors = distributor.get_queueing_errors(["21"])
    assert errors == {}


def test_get_queueing_errors_consumes_entry(distributor):
    """Queueing errors should be consumed (popped) on retrieval."""
    distributor._queueing_errors["30"] = "some error"
    errors = distributor.get_queueing_errors(["30"])
    assert "30" in errors
    # Second call should return empty
    errors = distributor.get_queueing_errors(["30"])
    assert errors == {}


def test_get_usage_stats():
    """get_usage_stats should return server-ready keys."""
    worker_node = MultiprocessWorkerNode()
    stats = worker_node.get_usage_stats()
    assert "maxrss" in stats
    assert "cpu" in stats
    assert "usage_str" in stats
    assert isinstance(stats["maxrss"], str)
    assert isinstance(stats["cpu"], str)
    assert isinstance(stats["usage_str"], str)
