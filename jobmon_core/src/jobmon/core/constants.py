"""Constants declared for different statuses, types and codes throughout Jobmon."""
from enum import Enum


class ArgType:
    NODE_ARG = 1
    TASK_ARG = 2
    OP_ARG = 3


class TaskResourcesType:
    """Constant Types for Task Resources."""

    ORIGINAL = "O"
    VALIDATED = "V"
    ADJUSTED = "A"


class TaskInstanceStatus:
    """Statuses used for Task Instances."""

    DONE = "D"
    ERROR = "E"
    ERROR_FATAL = "F"
    INSTANTIATED = "I"
    KILL_SELF = "K"
    LAUNCHED = "O"
    QUEUED = "Q"
    RUNNING = "R"
    TRIAGING = "T"

    UNKNOWN_ERROR = "U"
    NO_DISTRIBUTOR_ID = "W"
    RESOURCE_ERROR = "Z"


class TaskStatus:
    """Statuses used for Tasks."""

    REGISTERING = "G"
    QUEUED = "Q"
    INSTANTIATING = "I"
    LAUNCHED = "O"
    RUNNING = "R"
    DONE = "D"
    ERROR_RECOVERABLE = "E"
    ADJUSTING_RESOURCES = "A"
    ERROR_FATAL = "F"

    LABEL_DICT = {
        "G": "REGISTERING",
        "Q": "QUEUED",
        "I": "INSTANTIATING",
        "O": "LAUNCHED",
        "R": "RUNNING",
        "D": "DONE",
        "E": "ERROR_RECOVERABLE",
        "A": "ADJUSTING_RESOURCES",
        "F": "ERROR_FATAL",
    }


class WorkflowRunStatus:
    """Statuses used for Workflow Runs."""

    REGISTERED = "G"
    LINKING = "L"
    BOUND = "B"
    ABORTED = "A"
    RUNNING = "R"
    DONE = "D"
    STOPPED = "S"
    ERROR = "E"
    COLD_RESUME = "C"
    HOT_RESUME = "H"
    TERMINATED = "T"
    INSTANTIATED = "I"
    LAUNCHED = "O"


class WorkflowStatus:
    """Statuses used for Workflows."""

    REGISTERING = "G"
    QUEUED = "Q"
    ABORTED = "A"
    INSTANTIATING = "I"
    LAUNCHED = "O"
    RUNNING = "R"
    DONE = "D"
    HALTED = "H"
    FAILED = "F"


class Direction(Enum):
    """A generic utility class.

    Used to represent one-dimensional direction,
    such as upstream/downstream.
    """

    UP = "up"
    DOWN = "down"


class SpecialChars:
    """A generic utility class.

    Used to define special chars.
    """

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\" "


class ExecludeTTVs:
    """A hard-coded list.

    Used to exclude task template versions with huge tasks that cause DB crash.
    """

    EXECLUDE_TTVS = {1}  # bashtask


class MaxConcurrentlyRunning:
    """A hard limit of array concurrency.

    Currently set to max int, but can change as INFRA requests.
    """

    import sys

    MAXCONCURRENTLYRUNNING = 2_147_483_647  # mysql int(11) max
