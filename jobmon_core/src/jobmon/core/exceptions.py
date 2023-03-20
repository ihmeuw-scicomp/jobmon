"""Custom Exceptions used throughout Jobmon."""


class ReturnCodes(object):
    """Bash return codes used in distributor wrapper."""

    OK = 0
    WORKER_NODE_ENV_FAILURE = 198
    WORKER_NODE_CLI_FAILURE = 199


class InvalidResponse(Exception):
    """Invalid Response type Exception."""

    pass


class RemoteExitInfoNotAvailable(Exception):
    """Exception raised when Exit Info is not available for different executor types."""

    pass


class CallableReturnedInvalidObject(Exception):
    """Invalid Object got returned."""

    pass


class WorkflowAlreadyExists(Exception):
    """Workflow with the same workflow args already exists."""

    pass


class WorkflowAlreadyComplete(Exception):
    """This Workflow is already done."""

    pass


class WorkflowNotResumable(Exception):
    """This Workflow is not set to be resumed."""

    pass


class EmptyWorkflowError(Exception):
    """This Workflow is empty."""

    pass


class DistributorStartupTimeout(Exception):
    """Distributor was not able to start in time."""

    pass


class DistributorNotAlive(Exception):
    """The Distributor is not running."""

    pass


class DistributorUnexpected(Exception):
    """Unexpected situation in Distributor."""

    pass


class WorkflowRunStateError(Exception):
    """Error with the Workflow Run status."""

    pass


class ResumeSet(Exception):
    """Resume Exception."""

    pass


class NodeDependencyNotExistError(Exception):
    """Dependency does not exist."""

    pass


class DuplicateNodeArgsError(Exception):
    """Multiple nodes with the same args for the same TaskTemplate not allowed."""

    pass


class InvalidMemoryFormat(Exception):
    """Memory input invalid."""

    pass


class InvalidMemoryUnit(Exception):
    """Memory convert unit invalid."""

    pass


class ConfigError(Exception):
    """No configuration found for server."""

    pass


class InvalidStateTransition(Exception):
    """Invalid State Transition implementation."""

    def __init__(self, model: str, id: str, old_state: str, new_state: str) -> None:
        """Initialize Exception."""
        msg = "Cannot transition {} id: {} from {} to {}".format(
            model, id, old_state, new_state
        )
        super(InvalidStateTransition, self).__init__(self, msg)


class TransitionError(Exception):
    """Transition failed."""

    pass


class WorkflowTestError(Exception):
    """Workflow Run encountered and error."""

    pass


class DistributorInterruptedError(Exception):
    """raised when signal is sent to distributor."""

    pass
