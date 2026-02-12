"""Multiprocess executes tasks in parallel using a thread pool."""

import logging
import os
import platform
import resource
import shlex
import shutil
import subprocess
import sys
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set, Tuple

import psutil

from jobmon.core.cluster_protocol import ClusterDistributor, ClusterWorkerNode
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import RemoteExitInfoNotAvailable

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


class MultiprocessDistributor(ClusterDistributor):
    """Executes tasks locally in parallel using a ThreadPoolExecutor.

    Each submitted task runs in a thread that spawns a subprocess via Popen.
    Shared memory dicts track processes, futures, exit codes, and errors.
    """

    def __init__(
        self,
        cluster_name: str,
        parallelism: int = 3,
        *args: tuple,
        **kwargs: dict,
    ) -> None:
        """Initialization of the multiprocess distributor.

        Args:
            cluster_name: the name of the cluster.
            parallelism: how many parallel jobs to distribute at a time.
        """
        self.started = False
        self._cluster_name = cluster_name

        # Find worker_node_entry_point in the same environment as the
        # running Python. This avoids version mismatches when multiple
        # jobmon installations exist (e.g., conda base vs project .venv).
        bin_dir = os.path.dirname(sys.executable)
        candidate_path = os.path.join(bin_dir, "worker_node_entry_point")
        worker_node_entry_point: Optional[str]
        if os.path.exists(candidate_path):
            worker_node_entry_point = candidate_path
        else:
            worker_node_entry_point = shutil.which("worker_node_entry_point")
        if not worker_node_entry_point or not os.path.exists(worker_node_entry_point):
            raise ValueError("worker_node_entry_point can't be found.")
        self._worker_node_entry_point = worker_node_entry_point

        logger.info("Initializing {}".format(self.__class__.__name__))

        self._parallelism = parallelism
        self._next_job_id = 1
        self._executor = ThreadPoolExecutor(max_workers=self._parallelism)

        # Shared state (thread-safe via GIL for simple dict ops)
        self._processes: Dict[str, subprocess.Popen] = {}
        self._futures: Dict[str, Future] = {}
        self._exit_info: LimitedSizeDict = LimitedSizeDict(size_limit=1000)
        self._queueing_errors: Dict[str, str] = {}

    @property
    def worker_node_entry_point(self) -> str:
        """Path to jobmon worker_node_entry_point."""
        return self._worker_node_entry_point

    @property
    def cluster_name(self) -> str:
        """Return the name of the cluster type."""
        return self._cluster_name

    def _get_subtask_id(self, distributor_id: int, array_step_id: int) -> str:
        """Get the subtask_id based on distributor_id and array_step_id."""
        return str(distributor_id) + "_" + str(array_step_id)

    def start(self) -> None:
        """Start the thread pool executor."""
        if not self.started:
            self._executor = ThreadPoolExecutor(max_workers=self._parallelism)
            self.started = True

    def stop(self) -> None:
        """Terminate all running tasks and shut down the executor."""
        actual = self.get_submitted_or_running()
        if actual:
            self.terminate_task_instances(list(actual))
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._executor = ThreadPoolExecutor(max_workers=self._parallelism)
        self.started = False

    def _run_task(self, distributor_id: str, command: str, env: Dict[str, str]) -> None:
        """Run a single task in a thread.

        Args:
            distributor_id: the id assigned to this task.
            command: the full shell command to execute.
            env: environment variables for the subprocess.
        """
        try:
            proc = subprocess.Popen(shlex.split(command), env=env)
            self._processes[distributor_id] = proc
            proc.communicate()
            self._exit_info[distributor_id] = proc.returncode
        except Exception as e:
            self._queueing_errors[distributor_id] = str(e)
            logger.exception(f"Error running task {distributor_id}: {e}")
        finally:
            self._processes.pop(distributor_id, None)
            self._futures.pop(distributor_id, None)

    def submit_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
    ) -> str:
        """Submit a single task to the thread pool."""
        distributor_id = str(self._next_job_id)
        self._next_job_id += 1

        env = os.environ.copy()
        env["JOB_ID"] = distributor_id
        full_command = self.worker_node_entry_point + " " + command

        future = self._executor.submit(
            self._run_task, distributor_id, full_command, env
        )
        self._futures[distributor_id] = future
        return distributor_id

    def submit_array_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
        array_length: int,
    ) -> Dict[int, str]:
        """Submit an array task to the thread pool.

        Return: a mapping of array_step_id to distributor_id.
        """
        job_id = self._next_job_id
        self._next_job_id += 1

        mapping: Dict[int, str] = {}
        for array_step_id in range(0, array_length):
            distributor_id = self._get_subtask_id(job_id, array_step_id)
            mapping[array_step_id] = distributor_id

            env = os.environ.copy()
            env["JOB_ID"] = str(job_id)
            env["ARRAY_STEP_ID"] = str(array_step_id)
            full_command = self.worker_node_entry_point + " " + command

            future = self._executor.submit(
                self._run_task, distributor_id, full_command, env
            )
            self._futures[distributor_id] = future

        return mapping

    def get_submitted_or_running(
        self, distributor_ids: Optional[List[str]] = None
    ) -> Set[str]:
        """Get tasks that are active."""
        return set(self._futures.keys())

    def terminate_task_instances(self, distributor_ids: List[str]) -> None:
        """Terminate task instances.

        Kills subprocesses and cancels futures for the given IDs.

        Args:
            distributor_ids: A list of distributor IDs.
        """
        logger.debug(f"Going to terminate: {distributor_ids}")

        for did in distributor_ids:
            proc = self._processes.get(did)
            if proc is not None:
                try:
                    parent = psutil.Process(proc.pid)
                    for child in parent.children(recursive=True):
                        child.kill()
                    parent.kill()
                except psutil.NoSuchProcess:
                    pass

            future = self._futures.pop(did, None)
            if future is not None:
                future.cancel()
            self._processes.pop(did, None)

    def get_queueing_errors(self, distributor_ids: List[str]) -> Dict[str, str]:
        """Get the task instances that have errored during queueing."""
        errors = {}
        for did in distributor_ids:
            if did in self._queueing_errors:
                errors[did] = self._queueing_errors.pop(did)
        return errors

    def get_remote_exit_info(self, distributor_id: str) -> Tuple[str, str]:
        """Get the exit info about the task instance once done."""
        try:
            exit_code = self._exit_info[distributor_id]
            if exit_code == 199:
                return (
                    TaskInstanceStatus.UNKNOWN_ERROR,
                    "job was in kill self state",
                )
            return (
                TaskInstanceStatus.UNKNOWN_ERROR,
                f"Process exited with code {exit_code}",
            )
        except KeyError:
            raise RemoteExitInfoNotAvailable


class MultiprocessWorkerNode(ClusterWorkerNode):
    """Task instance info for the Multiprocess distributor."""

    def __init__(self) -> None:
        """Initialization of the multiprocess distributor worker node."""
        self._distributor_id: Optional[str] = None
        self._array_step_id: Optional[int] = None
        self._subtask_id: Optional[str] = None
        self._logfile_template = {
            "stdout": "{root}/{name}.o{job_id}",
            "stderr": "{root}/{name}.e{job_id}",
        }

    @property
    def distributor_id(self) -> Optional[str]:
        """The id from the distributor."""
        if self._distributor_id is None:
            jid = os.environ.get("JOB_ID")
            if jid:
                self._distributor_id = f"{jid}_{self.array_step_id}"
        return self._distributor_id

    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        """Exit code and message."""
        msg = f"Got exit_code: {exit_code}. " f"Error message was: {error_msg}"
        return TaskInstanceStatus.ERROR, msg

    def get_usage_stats(self) -> Dict:
        """Usage information specific to the distributor."""
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        maxrss = usage.ru_maxrss
        if platform.system() != "Darwin":
            maxrss = maxrss * 1024  # KB -> bytes on Linux
        return {
            "maxrss_bytes": maxrss,
            "user_time_sec": usage.ru_utime,
            "system_time_sec": usage.ru_stime,
        }

    def initialize_logfile(self, log_type: str, log_dir: str, name: str) -> str:
        if log_dir:
            logpath = self._logfile_template[log_type].format(
                root=log_dir, name=name, job_id=self.distributor_id
            )
        else:
            logpath = "/dev/null"
        return logpath

    @property
    def array_step_id(self) -> Optional[int]:
        """Return array_step_id ."""
        if self._array_step_id is None:
            atid = os.environ.get("ARRAY_STEP_ID")
            if atid:
                self._array_step_id = int(atid)
        return self._array_step_id
