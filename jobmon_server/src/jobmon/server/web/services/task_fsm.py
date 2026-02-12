"""Centralized Task finite state machine."""

from typing import Dict, Optional, Set, Type

from jobmon.core.constants import TaskInstanceStatus, TaskStatus


class TaskFSM:
    """Centralized finite state machine for Task status transitions.

    Extracts and centralizes the FSM logic currently embedded in Task model.
    Includes direct transitions that skip ERROR_RECOVERABLE intermediate state.
    """

    # Valid transitions: from_status -> set of valid to_statuses
    # Note: Uses constants from jobmon.core.constants (single-char strings)
    VALID_TRANSITIONS: Dict[str, Set[str]] = {
        TaskStatus.REGISTERING: {TaskStatus.QUEUED},
        TaskStatus.ADJUSTING_RESOURCES: {TaskStatus.QUEUED, TaskStatus.ERROR_FATAL},
        TaskStatus.QUEUED: {TaskStatus.INSTANTIATING},
        TaskStatus.INSTANTIATING: {
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,  # Race condition: worker reports before distributor
            TaskStatus.ERROR_RECOVERABLE,
        },
        TaskStatus.LAUNCHED: {
            TaskStatus.RUNNING,
            TaskStatus.ERROR_RECOVERABLE,
            TaskStatus.ERROR_FATAL,  # Kill operation
        },
        TaskStatus.RUNNING: {
            TaskStatus.DONE,
            TaskStatus.ERROR_RECOVERABLE,
            # Direct transitions (skip ERROR_RECOVERABLE for audit simplicity):
            TaskStatus.REGISTERING,  # Retry
            TaskStatus.ADJUSTING_RESOURCES,  # Resource error retry
            TaskStatus.ERROR_FATAL,  # Max attempts exceeded
        },
        TaskStatus.ERROR_RECOVERABLE: {
            TaskStatus.REGISTERING,
            TaskStatus.ADJUSTING_RESOURCES,
            TaskStatus.ERROR_FATAL,
            TaskStatus.QUEUED,
        },
        # Terminal states - no transitions out
        TaskStatus.DONE: set(),
        TaskStatus.ERROR_FATAL: set(),
    }

    # Auto-generate reverse lookup at class load time
    VALID_SOURCES: Dict[str, Set[str]] = {}

    # TaskInstance status -> Task status mapping
    TI_TO_TASK_STATUS: Dict[str, str] = {
        TaskInstanceStatus.QUEUED: TaskStatus.QUEUED,
        TaskInstanceStatus.INSTANTIATED: TaskStatus.INSTANTIATING,
        TaskInstanceStatus.LAUNCHED: TaskStatus.LAUNCHED,
        TaskInstanceStatus.RUNNING: TaskStatus.RUNNING,
        TaskInstanceStatus.DONE: TaskStatus.DONE,
        TaskInstanceStatus.ERROR_FATAL: TaskStatus.ERROR_FATAL,
        # Error states -> determined by retry logic (see get_task_status_for_ti_error)
    }

    @classmethod
    def _build_valid_sources(cls: Type["TaskFSM"]) -> None:
        """Build reverse lookup: to_status -> set of valid from_statuses."""
        cls.VALID_SOURCES = {}
        for from_status, to_statuses in cls.VALID_TRANSITIONS.items():
            for to_status in to_statuses:
                if to_status not in cls.VALID_SOURCES:
                    cls.VALID_SOURCES[to_status] = set()
                cls.VALID_SOURCES[to_status].add(from_status)

    @classmethod
    def is_valid_transition(
        cls: Type["TaskFSM"], from_status: str, to_status: str
    ) -> bool:
        """Check if a transition is valid per FSM rules."""
        return to_status in cls.VALID_TRANSITIONS.get(from_status, set())

    @classmethod
    def get_valid_sources(cls: Type["TaskFSM"], to_status: str) -> Set[str]:
        """Get all statuses that can transition TO the given status."""
        if not cls.VALID_SOURCES:
            cls._build_valid_sources()
        return cls.VALID_SOURCES.get(to_status, set())

    @classmethod
    def is_terminal(cls: Type["TaskFSM"], status: str) -> bool:
        """Check if a status is terminal (no outgoing transitions)."""
        return len(cls.VALID_TRANSITIONS.get(status, set())) == 0

    @classmethod
    def get_task_status_for_ti(
        cls: Type["TaskFSM"],
        ti_status: str,
        task_num_attempts: int,
        task_max_attempts: int,
    ) -> Optional[str]:
        """Get Task status for a TaskInstance status change.

        Skips ERROR_RECOVERABLE, returns final state directly.
        Returns None if no Task transition needed.
        """
        # Direct mapping for non-error states
        if ti_status in cls.TI_TO_TASK_STATUS:
            return cls.TI_TO_TASK_STATUS[ti_status]

        # Error states: determine final status (skip ERROR_RECOVERABLE)
        if ti_status in (
            TaskInstanceStatus.NO_DISTRIBUTOR_ID,
            TaskInstanceStatus.ERROR,
            TaskInstanceStatus.UNKNOWN_ERROR,
            TaskInstanceStatus.RESOURCE_ERROR,
        ):
            if task_num_attempts >= task_max_attempts:
                return TaskStatus.ERROR_FATAL
            elif ti_status == TaskInstanceStatus.RESOURCE_ERROR:
                return TaskStatus.ADJUSTING_RESOURCES
            else:
                return TaskStatus.REGISTERING

        # No task transition for other TI states (TRIAGING, KILL_SELF, etc.)
        return None


# Build reverse lookup on module import
TaskFSM._build_valid_sources()
