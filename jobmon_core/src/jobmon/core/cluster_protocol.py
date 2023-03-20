from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# the following try-except is to accommodate Python versions on both >=3.8 and 3.7.
# The Protocol was officially introduced in 3.8, with typing_extensions slapped on 3.7.
try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

from jobmon.core import __version__
from jobmon.core.exceptions import RemoteExitInfoNotAvailable


class ClusterQueue(Protocol):
    """The protocol class for queues on a cluster."""

    @abstractmethod
    def __init__(self, queue_id: int, queue_name: str, parameters: Dict) -> None:
        """Initialization of ClusterQueue."""
        raise NotImplementedError

    @abstractmethod
    def validate_resources(
        self, strict: bool = False, **kwargs: Union[str, int, float]
    ) -> Tuple[bool, str]:
        """Ensures that requested resources aren't greater than what's available."""
        raise NotImplementedError

    @abstractmethod
    def coerce_resources(self, **kwargs: Union[str, int, float]) -> Dict:
        """Ensures that requested resources aren't greater than what's available."""
        raise NotImplementedError

    @property
    @abstractmethod
    def parameters(self) -> Dict:
        """Returns the dictionary of parameters."""
        raise NotImplementedError

    @property
    @abstractmethod
    def queue_name(self) -> str:
        """Returns the name of the queue."""
        raise NotImplementedError

    @property
    @abstractmethod
    def queue_id(self) -> int:
        """Returns the ID of the queue."""
        raise NotImplementedError

    @property
    @abstractmethod
    def required_resources(self) -> List:
        """Returns the list of resources that are required."""
        raise NotImplementedError


class ClusterDistributor(Protocol):
    """The protocol class for cluster distributors."""

    @abstractmethod
    def __init__(self, cluster_name: str, *args: str, **kwargs: str) -> None:
        """Initialization of ClusterQueue."""
        raise NotImplementedError

    @property
    @abstractmethod
    def worker_node_entry_point(self) -> str:
        """Path to jobmon worker_node_entry_point."""
        raise NotImplementedError

    @property
    @abstractmethod
    def cluster_name(self) -> str:
        """Return the name of the cluster type."""
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        """Start the distributor."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stop the distributor."""
        raise NotImplementedError

    @abstractmethod
    def get_queueing_errors(self, distributor_ids: List[str]) -> Dict[str, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    @abstractmethod
    def get_submitted_or_running(
        self, distributor_ids: Optional[List[str]] = None
    ) -> Set[str]:
        """Check which task instances are active.

        Returns: a set strings
        """
        raise NotImplementedError

    @abstractmethod
    def terminate_task_instances(self, distributor_ids: List[str]) -> None:
        """Terminate task instances.

        If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    @abstractmethod
    def get_remote_exit_info(self, distributor_id: str) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable

    @abstractmethod
    def submit_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
    ) -> str:
        """Submit the command on the cluster technology and return a distributor_id.

        The distributor_id can be used to identify the associated TaskInstance, terminate
        it, monitor for missingness, or collect usage statistics. If an exception is raised by
        this method the task instance will move to "W" state and the exception will be logged
        in the database under the task_instance_error_log table.

        Args:
            command: command to be run
            name: name of task
            requested_resources: resource requests sent to distributor API

        Returns:
            A tuple indicating the distributor id, the full output file location,
            and full error location.
        """
        raise NotImplementedError

    def submit_array_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
        array_length: int,
    ) -> Dict[int, str]:
        """Submit an array task to the underlying distributor and return a distributor_id.

        The distributor ID represents the ID of the overall array job, sub-tasks will have
        their own associated IDs.

        Args:
            command: the array worker node command to run
            name: name of the array
            requested_resources: resources with which to run the array
            array_length: how many tasks associated with the array
        Return:
            a mapping of array_step_id to distributor_id, output location, and error location.
        """
        raise NotImplementedError

    def build_worker_node_command(
        self,
        task_instance_id: Optional[int] = None,
        array_id: Optional[int] = None,
        batch_number: Optional[int] = None,
    ) -> str:
        """Build a command that can be executed by the worker_node.

        Args:
            task_instance_id: id for the given instance of this task
            array_id: id for the array if using an array strategy
            batch_number: if array strategy is used, the submission counter index to use

        Returns:
            (str) unwrappable command
        """
        wrapped_cmd = []
        if task_instance_id is not None:
            wrapped_cmd.extend(
                ["worker_node_job", "--task_instance_id", str(task_instance_id)]
            )
        elif array_id is not None and batch_number is not None:
            wrapped_cmd.extend(
                [
                    "worker_node_array",
                    "--array_id",
                    str(array_id),
                    "--batch_number",
                    str(batch_number),
                ]
            )
        else:
            raise ValueError(
                "Must specify either task_instance_id or array_id and batch_number. Got "
                f"task_instance_id={task_instance_id}, array_id={array_id}, "
                f"batch_number={batch_number}"
            )
        wrapped_cmd.extend(
            [
                "--expected_jobmon_version",
                __version__,
                "--cluster_name",
                self.cluster_name,
            ]
        )
        str_cmd = " ".join([str(i) for i in wrapped_cmd])
        return str_cmd


class ClusterWorkerNode(Protocol):
    """Base class defining interface for gathering executor info in the execution_wrapper.

    While not required, implementing get_usage_stats() will allow collection
    of CPU/memory utilization stats for each job.

    Get exit info is used to determine the error type if the task hits a
    system error of some variety.
    """

    @property
    @abstractmethod
    def distributor_id(self) -> Optional[str]:
        """Executor specific id assigned to a task instance."""
        raise NotImplementedError

    @abstractmethod
    def get_usage_stats(self) -> Dict:
        """Usage information specific to the exector."""
        raise NotImplementedError

    @abstractmethod
    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        """Error and exit code info from the executor."""
        raise NotImplementedError

    @abstractmethod
    def initialize_logfile(self, log_type: str, log_dir: str, name: str) -> str:
        """Error and exit code info from the executor."""
        raise NotImplementedError

    @property
    def array_step_id(self) -> Optional[int]:
        """The step id in each batch.

        For each array task instance, array_id, array_batch_num, and array_step_id
        should uniquely identify a subtask_id.

        It depends on the plug in whether you can generate the subtask_id using
        array_step_id.
        """
        raise NotImplementedError
