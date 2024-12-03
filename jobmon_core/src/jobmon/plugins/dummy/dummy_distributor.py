"""Dummy distributor that runs one task at a time."""

from collections import OrderedDict
import logging
import os
import random
import shutil
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from jobmon.core.cluster_protocol import ClusterDistributor, ClusterWorkerNode
from jobmon.core.constants import TaskInstanceStatus
from jobmon.worker_node.cli import WorkerNodeCLI
from jobmon.worker_node.worker_node_factory import WorkerNodeFactory


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


class DummyDistributor(ClusterDistributor):
    """Executor to run tasks one at a time."""

    def __init__(
        self,
        cluster_name: str,
        exit_info_queue_size: int = 1000,
        *args: tuple,
        **kwargs: dict,
    ) -> None:
        """Initialization of the dummy distributor.

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
        return TaskInstanceStatus.UNKNOWN_ERROR, "Whatever"

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
        """Run a fake execution of the task.

        In a real executor, this is where submission to the cluster would happen.
        Here, since it's a dummy executor, we just get a random number and empty
        file paths.
        """
        logger.debug("This is the Dummy Distributor")
        # even number for non array tasks
        distributor_id = random.randint(1, int(1e6)) * 2
        os.environ["JOB_ID"] = str(distributor_id)

        cli = WorkerNodeCLI()
        args = cli.parse_args(command)

        worker_node_factory = WorkerNodeFactory(cluster_name=args.cluster_name)
        # Do not do ANY logging at all
        worker_node_task_instance = worker_node_factory.get_job_task_instance(
            task_instance_id=args.task_instance_id
        )
        # Log running, log done, and exit
        worker_node_task_instance.log_running()
        worker_node_task_instance.set_command_output(0, "", "")
        worker_node_task_instance.log_done()

        return str(distributor_id)


class DummyWorkerNode(ClusterWorkerNode):
    """Get Executor Info for a Task Instance."""

    def __init__(self) -> None:
        """Initialization of the dummy executor worker node."""
        self._distributor_id: Optional[str] = None
        self._logfile_template = {
            "stdout": "{root}/{name}.o{job_id}",
            "stderr": "{root}/{name}.e{job_id}",
        }

    @property
    def distributor_id(self) -> Optional[str]:
        """Distributor id of the task."""
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
