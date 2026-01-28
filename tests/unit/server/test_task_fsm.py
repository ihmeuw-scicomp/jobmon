"""Unit tests for TaskFSM class."""

from jobmon.core.constants import TaskInstanceStatus, TaskStatus
from jobmon.server.web.services.task_fsm import TaskFSM


class TestTaskFSM:
    """Test TaskFSM centralized finite state machine."""

    def test_valid_transitions_defined_for_all_statuses(self) -> None:
        """All TaskStatus values should have a transition definition."""
        all_statuses = [
            TaskStatus.REGISTERING,
            TaskStatus.QUEUED,
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
            TaskStatus.DONE,
            TaskStatus.ERROR_RECOVERABLE,
            TaskStatus.ADJUSTING_RESOURCES,
            TaskStatus.ERROR_FATAL,
        ]
        for status in all_statuses:
            assert (
                status in TaskFSM.VALID_TRANSITIONS
            ), f"Missing transition definition for {status}"

    def test_terminal_states(self) -> None:
        """DONE and ERROR_FATAL should be terminal states."""
        assert TaskFSM.is_terminal(TaskStatus.DONE)
        assert TaskFSM.is_terminal(TaskStatus.ERROR_FATAL)

    def test_non_terminal_states(self) -> None:
        """Non-terminal states should have outgoing transitions."""
        non_terminal = [
            TaskStatus.REGISTERING,
            TaskStatus.QUEUED,
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
            TaskStatus.ERROR_RECOVERABLE,
            TaskStatus.ADJUSTING_RESOURCES,
        ]
        for status in non_terminal:
            assert not TaskFSM.is_terminal(status), f"{status} should not be terminal"

    def test_registering_to_queued(self) -> None:
        """REGISTERING can only transition to QUEUED."""
        assert TaskFSM.is_valid_transition(TaskStatus.REGISTERING, TaskStatus.QUEUED)
        # Invalid transitions from REGISTERING
        for invalid_target in [
            TaskStatus.RUNNING,
            TaskStatus.DONE,
            TaskStatus.LAUNCHED,
        ]:
            assert not TaskFSM.is_valid_transition(
                TaskStatus.REGISTERING, invalid_target
            ), f"REGISTERING->{invalid_target} should be invalid"

    def test_running_to_done(self) -> None:
        """RUNNING can transition to DONE."""
        assert TaskFSM.is_valid_transition(TaskStatus.RUNNING, TaskStatus.DONE)

    def test_running_to_error_states(self) -> None:
        """RUNNING can transition directly to error handling states."""
        # ERROR_RECOVERABLE (normal path)
        assert TaskFSM.is_valid_transition(
            TaskStatus.RUNNING, TaskStatus.ERROR_RECOVERABLE
        )
        # Direct to REGISTERING (skip ERROR_RECOVERABLE)
        assert TaskFSM.is_valid_transition(TaskStatus.RUNNING, TaskStatus.REGISTERING)
        # Direct to ADJUSTING_RESOURCES (skip ERROR_RECOVERABLE)
        assert TaskFSM.is_valid_transition(
            TaskStatus.RUNNING, TaskStatus.ADJUSTING_RESOURCES
        )
        # Direct to ERROR_FATAL (skip ERROR_RECOVERABLE)
        assert TaskFSM.is_valid_transition(TaskStatus.RUNNING, TaskStatus.ERROR_FATAL)

    def test_launched_to_error_fatal_for_kill(self) -> None:
        """LAUNCHED can transition to ERROR_FATAL for kill operations."""
        assert TaskFSM.is_valid_transition(TaskStatus.LAUNCHED, TaskStatus.ERROR_FATAL)

    def test_get_valid_sources(self) -> None:
        """get_valid_sources returns all statuses that can transition TO a target."""
        queued_sources = TaskFSM.get_valid_sources(TaskStatus.QUEUED)
        assert TaskStatus.REGISTERING in queued_sources
        assert TaskStatus.ADJUSTING_RESOURCES in queued_sources
        assert TaskStatus.ERROR_RECOVERABLE in queued_sources

    def test_get_valid_sources_for_done(self) -> None:
        """DONE can only be reached from RUNNING."""
        done_sources = TaskFSM.get_valid_sources(TaskStatus.DONE)
        assert done_sources == {TaskStatus.RUNNING}

    def test_get_valid_sources_for_nonexistent(self) -> None:
        """get_valid_sources returns empty set for unknown status."""
        assert TaskFSM.get_valid_sources("INVALID") == set()


class TestTaskFSMTaskInstanceMapping:
    """Test TaskInstance -> Task status mapping."""

    def test_ti_to_task_direct_mapping(self) -> None:
        """Non-error TI statuses map directly to Task statuses."""
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.RUNNING, 0, 3)
            == TaskStatus.RUNNING
        )
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.DONE, 0, 3)
            == TaskStatus.DONE
        )
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.LAUNCHED, 0, 3)
            == TaskStatus.LAUNCHED
        )

    def test_ti_error_max_attempts_exceeded(self) -> None:
        """Error TI with max attempts exceeded -> ERROR_FATAL."""
        # num_attempts >= max_attempts
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.ERROR, 3, 3)
            == TaskStatus.ERROR_FATAL
        )
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.UNKNOWN_ERROR, 5, 3)
            == TaskStatus.ERROR_FATAL
        )

    def test_ti_error_can_retry(self) -> None:
        """Error TI with attempts remaining -> REGISTERING (retry)."""
        # num_attempts < max_attempts
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.ERROR, 1, 3)
            == TaskStatus.REGISTERING
        )
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.NO_DISTRIBUTOR_ID, 0, 3)
            == TaskStatus.REGISTERING
        )

    def test_ti_resource_error_can_retry(self) -> None:
        """RESOURCE_ERROR TI with attempts remaining -> ADJUSTING_RESOURCES."""
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.RESOURCE_ERROR, 1, 3)
            == TaskStatus.ADJUSTING_RESOURCES
        )

    def test_ti_resource_error_max_attempts(self) -> None:
        """RESOURCE_ERROR TI with max attempts -> ERROR_FATAL."""
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.RESOURCE_ERROR, 3, 3)
            == TaskStatus.ERROR_FATAL
        )

    def test_ti_no_transition_for_triaging(self) -> None:
        """TRIAGING and other internal TI states return None."""
        assert TaskFSM.get_task_status_for_ti(TaskInstanceStatus.TRIAGING, 0, 3) is None
        assert (
            TaskFSM.get_task_status_for_ti(TaskInstanceStatus.KILL_SELF, 0, 3) is None
        )
