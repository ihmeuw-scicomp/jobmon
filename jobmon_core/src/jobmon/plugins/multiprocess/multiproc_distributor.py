"""Multiprocess executes tasks in parallel if multiple threads are available."""
import logging
from multiprocessing import JoinableQueue, Process, Queue
import os
import queue
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Set, Tuple

import psutil

from jobmon.core.cluster_protocol import ClusterDistributor, ClusterWorkerNode
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import RemoteExitInfoNotAvailable

logger = logging.getLogger(__name__)


class PickableTask:
    """Object passed between processes."""

    def __init__(
        self,
        distributor_id: str,
        command: str,
        task_type: str = "array",
    ) -> None:
        """Initialization of PickableTask.

        array_step_id: is only meaningful and has int value when for array
        """
        self.distributor_id = distributor_id
        self.command = command
        self.task_type = task_type


class Consumer(Process):
    """Consumes the tasks to be run."""

    def __init__(self, task_queue: JoinableQueue, response_queue: Queue) -> None:
        """Consume work sent from LocalExecutor through multiprocessing queue.

        this class is structured based on
        https://pymotw.com/2/multiprocessing/communication.html

            task_queue:
                a (multiprocessing.JoinableQueue[Optional[PickableTask]]) object
                created by LocalExecutor used to retrieve work from the
                distributor.
            response_queue:
                A (Queue[Tuple[int, Optional[int], Optional[int]]]) object,
                that will hold information with Queue:
                Tuple[distributor_id, array_step_id if applicable, pid]
        """
        super().__init__()

        # consumer communication
        self.task_queue: JoinableQueue[Optional[PickableTask]] = task_queue
        self.response_queue: Queue[Tuple[str, Optional[int]]] = response_queue

    def run(self) -> None:
        """Wait for work, the execute it."""
        logger.info(f"consumer alive. pid={os.getpid()}")

        while True:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:
                    logger.info("Received poison pill. Shutting down")
                    # Received poison pill, no more tasks to run
                    self.task_queue.task_done()
                    break

                else:
                    logger.info(f"consumer received {task.command}")

                    # run the job
                    env = os.environ.copy()

                    if task.task_type == "array":
                        job_id, array_step_id = task.distributor_id.split("_")
                        env["JOB_ID"] = job_id
                        env["ARRAY_STEP_ID"] = array_step_id
                    else:
                        env["JOB_ID"] = task.distributor_id

                    proc = subprocess.Popen(task.command, env=env, shell=True)

                    # log the pid with the distributor class
                    self.response_queue.put((task.distributor_id, proc.pid))

                    # wait till the process finishes
                    proc.communicate()

                    logger.info(f"consumer finished processing {task.distributor_id}")

                    # tell the queue this job is done so it can be shut down
                    # someday
                    self.response_queue.put((task.distributor_id, None))
                    self.task_queue.task_done()

            except queue.Empty:
                pass
            except Exception as e:
                logger.exception(e)


class MultiprocessDistributor(ClusterDistributor):
    """Executes tasks locally in parallel.

    It uses the multiprocessing Python library and queues to parallelize the execution of
    tasks. The subprocessing pattern looks like this:
        LocalExec
        --> consumer1
        ----> subconsumer1
        --> consumer2
        ----> subconsumer2
        ...
        --> consumerN
        ----> subconsumerN
    """

    def __init__(
        self, cluster_name: str, parallelism: int = 3, *args: tuple, **kwargs: dict
    ) -> None:
        """Initialization of the multiprocess distributor.

        Args:
            cluster_name: the name of the cluster.
            parallelism (int, optional): how many parallel jobs to distribute at a
                time
        """
        self.temp_dir: Optional[str] = None
        self.started = False
        self._cluster_name = cluster_name

        worker_node_entry_point = shutil.which("worker_node_entry_point")
        if not worker_node_entry_point:
            raise ValueError("worker_node_entry_point can't be found.")
        self._worker_node_entry_point = worker_node_entry_point

        logger.info("Initializing {}".format(self.__class__.__name__))

        self._parallelism = parallelism
        self._next_job_id = 1

        # mapping of Tuple[distributor_id, optinal array_step_id] to pid.
        # if pid is None then it is queued
        self._running_or_submitted: Dict[str, Optional[int]] = {}

        # ipc queues
        self.task_queue: JoinableQueue[Optional[PickableTask]] = JoinableQueue()
        self.response_queue: Queue[Tuple[str, Optional[int]]] = Queue()

        # workers
        self.consumers: List[Consumer] = []

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
        """Fire up N task consuming processes using Multiprocessing.

        Number of consumers is controlled by parallelism.
        """
        # set jobmon command if provided
        if not self.started:
            self.consumers = [
                Consumer(task_queue=self.task_queue, response_queue=self.response_queue)
                for i in range(self._parallelism)
            ]
            for w in self.consumers:
                w.start()

            """Start the default."""
            self.started = True

    def stop(self) -> None:
        """Terminate consumers and call sync 1 final time."""
        actual = self.get_submitted_or_running()
        self.terminate_task_instances(list(actual))

        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # Wait for commands to finish
        self.task_queue.join()

        self.started = False

    def _update_internal_states(self) -> None:
        while not self.response_queue.empty():
            distributor_id, pid = self.response_queue.get()
            if pid is not None:
                self._running_or_submitted.update({distributor_id: pid})
            else:
                self._running_or_submitted.pop(distributor_id)

    def terminate_task_instances(self, distributor_ids: List[str]) -> None:
        """Terminate task instances.

        Only terminate the task instances that are running, not going to kill the jobs that
        are actually still in a waiting or a transitioning state.

        Args:
            distributor_ids: A list of distributor IDs.
        """
        logger.debug(f"Going to terminate: {distributor_ids}")

        # first drain the work queue so there are no race conditions with the
        # workers
        current_work: List[Optional[PickableTask]] = []
        work_order: Dict[int, PickableTask] = {}
        dist_ids_work_order: Set[str] = set()
        i = 0
        while not self.task_queue.empty():
            current_work.append(self.task_queue.get())
            self.task_queue.task_done()
            # create a dictionary of the work indices for quick removal later
            if current_work[-1] is not None:
                work_order[i] = current_work[-1]
                dist_ids_work_order.add(current_work[-1].distributor_id)
            i += 1

        # no need to worry about race conditions because there are no state
        # changes in the FSM caused by this method

        # now update our internal state tracker
        self._update_internal_states()

        # now terminate any running jobs and remove from state tracker
        # for distributor_id in distributor_ids:
        for w in work_order.values():
            if w.distributor_id in distributor_ids:
                execution_pid = self._running_or_submitted.get(w.distributor_id)
                if execution_pid is not None:
                    # kill the process and remove it from the state tracker
                    parent = psutil.Process(execution_pid)
                    for child in parent.children(recursive=True):
                        child.kill()

        unexpected_distributor_ids = set(distributor_ids).difference(
            dist_ids_work_order
        )
        for distributor_id in unexpected_distributor_ids:
            logger.error(
                f"distributor_id {distributor_id} was requested to be terminated"
                " but is not submitted or running"
            )

        # if not running remove from queue and state tracker
        for index in sorted(work_order.keys(), reverse=True):
            w = work_order[index]
            if w.distributor_id in distributor_ids:
                del current_work[index]
                del self._running_or_submitted[w.distributor_id]

        # put remaining work back on queue
        for task in current_work:
            self.task_queue.put(task)

    def get_submitted_or_running(
        self, distributor_ids: Optional[List[str]] = None
    ) -> Set[str]:
        """Get tasks that are active."""
        self._update_internal_states()
        return set(self._running_or_submitted.keys())

    def submit_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
    ) -> str:
        distributor_id = str(self._next_job_id)
        self._next_job_id += 1

        task = PickableTask(
            distributor_id, self.worker_node_entry_point + " " + command, "job"
        )
        self.task_queue.put(task)
        self._running_or_submitted.update({distributor_id: None})
        return distributor_id

    def submit_array_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
        array_length: int,
    ) -> Dict[int, str]:
        """Submit an array task to the multiprocess cluster.

        Return: a mapping of array_step_id to distributor_id, output path, and error path
        """
        job_id = self._next_job_id
        self._next_job_id += 1

        mapping: Dict[int, str] = {}
        for array_step_id in range(0, array_length):
            distributor_id = self._get_subtask_id(job_id, array_step_id)
            mapping[array_step_id] = distributor_id

            task = PickableTask(
                distributor_id, self.worker_node_entry_point + " " + command, "array"
            )
            self.task_queue.put(task)
            self._running_or_submitted.update({distributor_id: None})

        return mapping

    def get_queueing_errors(self, distributor_ids: List[str]) -> Dict[str, str]:
        """Get the task instances that have errored out."""
        return {}

    def get_remote_exit_info(self, distributor_id: str) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable


class MultiprocessWorkerNode(ClusterWorkerNode):
    """Task instance info for an instance run with the Multiprocessing distributor."""

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
        msg = f"Got exit_code: {exit_code}. Error message was: {error_msg}"
        return TaskInstanceStatus.ERROR, msg

    def get_usage_stats(self) -> Dict:
        """Usage information specific to the distributor."""
        return {}

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
