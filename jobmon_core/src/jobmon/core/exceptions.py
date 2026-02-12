"""Custom Exceptions used throughout Jobmon."""


class ReturnCodes(object):
    """Bash return codes used in distributor wrapper."""

    OK = 0
    WORKER_NODE_ENV_FAILURE = 198
    WORKER_NODE_CLI_FAILURE = 199


class InvalidResponse(Exception):
    """Invalid Response type Exception."""


class InvalidRequest(Exception):
    """Invalid Request type Exception."""


class RemoteExitInfoNotAvailable(Exception):
    """Exception raised when Exit Info is not available for different executor types."""


class CallableReturnedInvalidObject(Exception):
    """Invalid Object got returned."""


class WorkflowAlreadyExists(Exception):
    """Workflow with the same workflow args already exists."""


class WorkflowAlreadyComplete(Exception):
    """This Workflow is already done."""


class WorkflowNotResumable(Exception):
    """This Workflow is not set to be resumed."""


class EmptyWorkflowError(Exception):
    """This Workflow is empty."""


class DistributorStartupTimeout(Exception):
    """Distributor was not able to start in time."""


class DistributorNotAlive(Exception):
    """The Distributor is not running."""


class DistributorUnexpected(Exception):
    """Unexpected situation in Distributor."""


class WorkflowRunStateError(Exception):
    """Error with the Workflow Run status."""


class ResumeSet(Exception):
    """Resume Exception."""


class NodeDependencyNotExistError(Exception):
    """Dependency does not exist."""


class DuplicateNodeArgsError(Exception):
    """Multiple nodes with the same args for the same TaskTemplate not allowed."""


class InvalidMemoryFormat(Exception):
    """Memory input invalid."""


class InvalidMemoryUnit(Exception):
    """Memory convert unit invalid."""


class ConfigError(Exception):
    """No configuration found for server."""


class InvalidStateTransition(Exception):
    """Invalid State Transition implementation."""

    def __init__(self, model: str, id: int, old_state: str, new_state: str) -> None:
        """Initialize Exception."""
        msg = f"Cannot transition {model} id: {id} from {old_state} to {new_state}"
        super(InvalidStateTransition, self).__init__(self, msg)


class TransitionError(Exception):
    """Transition failed."""


class WorkflowTestError(Exception):
    """Workflow Run encountered and error."""


class DistributorInterruptedError(Exception):
    """raised when signal is sent to distributor."""


class CyclicGraphError(Exception):
    """Cyclic graph detected."""
