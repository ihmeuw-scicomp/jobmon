"""Sequential distributor that runs one task at a time."""
from collections import OrderedDict
import logging
import os
import shutil
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from jobmon.core.cluster_protocol import ClusterDistributor, ClusterWorkerNode
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import RemoteExitInfoNotAvailable, ReturnCodes
from jobmon.worker_node.cli import WorkerNodeCLI


logger = logging.getLogger(__name__)


class LimitedSizeDict(OrderedDict):
    """Dictionary for exit info."""

    def __init__(self, *args: int, **kwds: int) -> None:
        """Initialization of LimitedSizeDict."""
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key: Any, value: Any) -> None:
        """Set item in dict."""
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self) -> None:
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)


class SequentialDistributor(ClusterDistributor):
    """Executor to run tasks one at a time."""

    def __init__(
        self,
        cluster_name: str,
        exit_info_queue_size: int = 1000,
        *args: tuple,
        **kwargs: dict,
    ) -> None:
        """Initialization of the sequential distributor.

        Args:
            cluster_name (str): name of the cluster
            exit_info_queue_size (int): how many exit codes to retain
        """
        self.started = False

        self._cluster_name = cluster_name
        worker_node_entry_point = shutil.which("worker_node_entry_point")
        if not worker_node_entry_point:
            raise ValueError("worker_node_entry_point can't be found.")
        self._worker_node_entry_point = worker_node_entry_point

        self._next_distributor_id = 1
        self._exit_info = LimitedSizeDict(size_limit=exit_info_queue_size)

    @property
    def worker_node_entry_point(self) -> str:
        """Path to jobmon worker_node_entry_point."""
        return self._worker_node_entry_point

    @property
    def cluster_name(self) -> str:
        """Return the name of the cluster type."""
        return self._cluster_name

    def start(self) -> None:
        """Start the distributor."""
        self.started = True

    def stop(self) -> None:
        """Stop the distributor."""
        self.started = False

    def get_queueing_errors(self, distributor_ids: List[str]) -> Dict[str, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    def get_array_queueing_errors(
        self, distributor_id: Union[int, str]
    ) -> Dict[Union[int, str], str]:
        raise NotImplementedError

    def get_remote_exit_info(self, distributor_id: str) -> Tuple[str, str]:
        """Get exit info from task instances that have run."""
        try:
            exit_code = self._exit_info[distributor_id]
            if exit_code == 199:
                msg = "job was in kill self state"
                return TaskInstanceStatus.UNKNOWN_ERROR, msg
            else:
                return TaskInstanceStatus.UNKNOWN_ERROR, f"found {exit_code}"
        except KeyError:
            raise RemoteExitInfoNotAvailable

    def get_submitted_or_running(
        self, distributor_ids: Optional[List[str]] = None
    ) -> Set[str]:
        """Check status of running task."""
        running = os.environ.get("JOB_ID", "")
        return {running}

    def terminate_task_instances(self, distributor_ids: List[str]) -> None:
        """Terminate task instances.

        If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        logger.warning(
            "terminate_task_instances not implemented by ClusterDistributor: "
            f"{self.__class__.__name__}"
        )

    def submit_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
    ) -> str:
        """Execute sequentially."""
        # add an executor id to the environment
        os.environ["JOB_ID"] = str(self._next_distributor_id)
        distributor_id = str(self._next_distributor_id)
        self._next_distributor_id += 1

        # run the job and log the exit code
        try:
            # run command
            cli = WorkerNodeCLI()
            args = cli.parse_args(command)
            exit_code = cli.run_task_instance_job(args)

        except SystemExit as e:
            if e.code == ReturnCodes.WORKER_NODE_CLI_FAILURE:
                exit_code = ReturnCodes.WORKER_NODE_CLI_FAILURE
            else:
                raise

        self._exit_info[distributor_id] = exit_code
        return str(distributor_id)


class SequentialWorkerNode(ClusterWorkerNode):
    """Get Executor Info for a Task Instance."""

    def __init__(self) -> None:
        """Initialization of the sequential executor worker node."""
        self._distributor_id: Optional[str] = None
        self._logfile_template = {
            "stdout": "{root}/{name}.o{job_id}",
            "stderr": "{root}/{name}.e{job_id}",
        }

    @property
    def distributor_id(self) -> Optional[str]:
        """Distributor id of the task."""
        if self._distributor_id is None:
            jid = os.environ.get("JOB_ID")
            if jid:
                self._distributor_id = jid
        return self._distributor_id

    def initialize_logfile(self, log_type: str, log_dir: str, name: str) -> str:
        if log_dir:
            logpath = self._logfile_template[log_type].format(
                root=log_dir, name=name, job_id=self.distributor_id
            )
        else:
            logpath = "/dev/null"
        return logpath

    @staticmethod
    def get_exit_info(exit_code: int, error_msg: str) -> Tuple[str, str]:
        """Exit info, error message."""
        return TaskInstanceStatus.ERROR, error_msg

    @staticmethod
    def get_usage_stats() -> Dict:
        """Usage information specific to the exector."""
        return {}
