from jobmon.core.constants import TaskInstanceStatus
from jobmon.plugins.sequential.seq_distributor import SequentialDistributor


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
