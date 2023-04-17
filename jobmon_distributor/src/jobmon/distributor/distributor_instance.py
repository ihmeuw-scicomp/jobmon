from __future__ import annotations

import itertools as it
import logging
import signal
import sys
import time
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Set,
    Union,
)

from jobmon.core.cluster import Cluster
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import (
    DistributorInterruptedError, InvalidResponse, RemoteExitInfoNotAvailable
)
from jobmon.core.requester import Requester
from jobmon.distributor.batch import Batch
from jobmon.distributor.distributor_command import DistributorCommand
from jobmon.distributor.distributor_task_instance import DistributorTaskInstance

logger = logging.getLogger(__name__)


class DistributorInstance:
    def __init__(
        self,
        cluster_name: str,
        requester: Optional[Requester] = None,
        workflow_run_heartbeat_interval: Optional[int] = None,
        task_instance_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
        distributor_poll_interval: Optional[int] = None,
        workflow_run_id: Optional[int] = None,
        raise_on_error: bool = False,
    ) -> None:
        """Initialization of DistributorService."""
        # operational args
        config = JobmonConfig()
        if workflow_run_heartbeat_interval is None:
            self._workflow_run_heartbeat_interval = config.get_int(
                "heartbeat", "workflow_run_interval"
            )
        else:
            self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        if task_instance_heartbeat_interval is None:
            self._task_instance_heartbeat_interval = config.get_int(
                "heartbeat", "task_instance_interval"
            )
        else:
            self._task_instance_heartbeat_interval = task_instance_heartbeat_interval
        if heartbeat_report_by_buffer is None:
            self._heartbeat_report_by_buffer = config.get_float(
                "heartbeat", "report_by_buffer"
            )
        else:
            self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        if distributor_poll_interval is None:
            self._distributor_poll_interval = config.get_int(
                "distributor", "poll_interval"
            )
        else:
            self._distributor_poll_interval = distributor_poll_interval

        self.raise_on_error = raise_on_error
        self._distributor_instance_id = None

        # Optionally allow the distributor to only scan for tasks from a single workflowrun.
        # Necessary for cluster protocols that execute in the same memory space, e.g.
        # sequential or multiprocess builtins.
        self._workflow_run_id = workflow_run_id

        # Store allowed cluster distributor objects
        cluster = Cluster.get_cluster(cluster_name)
        self._cluster_interface = cluster.get_distributor()
        self._cluster_id = cluster.id

        # indexing of task instance by associated id
        self._task_instances: Dict[int, DistributorTaskInstance] = {}
        self._batches: Dict[int, Batch] = {}

        # work queue
        self._distributor_commands: Iterator[DistributorCommand] = it.chain([])

        # indexing of task instances by status
        self._task_instance_status_map: Dict[str, Set[DistributorTaskInstance]] = {
            TaskInstanceStatus.QUEUED: set(),
            TaskInstanceStatus.LAUNCHED: set(),
            TaskInstanceStatus.RUNNING: set(),
            TaskInstanceStatus.TRIAGING: set(),
            TaskInstanceStatus.KILL_SELF: set(),
        }
        # order through which we processes work
        self._command_generator_map: Dict[str, Callable[..., Generator[DistributorCommand, None, None]]] = {
            TaskInstanceStatus.QUEUED: self._check_queued_for_work,
            TaskInstanceStatus.TRIAGING: self._check_triaging_for_work,
            TaskInstanceStatus.KILL_SELF: self._check_kill_self_for_work,
        }

        # synchronization timings
        self._last_heartbeat_time = time.time()

        # web service API
        if requester is None:
            self.requester = Requester.from_defaults()
        else:
            self.requester = requester

    @property
    def _next_report_increment(self) -> float:
        return self._heartbeat_report_by_buffer * self._task_instance_heartbeat_interval

    def _initialize_signal_handlers(self) -> None:
        def handle_sighup(signal: int, frame: Any) -> None:
            raise DistributorInterruptedError("Got signal SIGHUP.")

        def handle_sigterm(signal: int, frame: Any) -> None:
            raise DistributorInterruptedError("Got signal SIGTERM.")

        def handle_sigint(signal: int, frame: Any) -> None:
            pass

        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGHUP, handle_sighup)
        signal.signal(signal.SIGINT, handle_sigint)

    @property
    def distributor_instance_id(self):
        if self._distributor_instance_id is None:
            raise ValueError(
                "distributor_instance_id not available until DistributorInstance registers "
                " with the server via DistributorInstance.register()"
            )
        return self._distributor_instance_id

    def register(self) -> None:
        """Register this DistributorInstance with the database. Get an ID"""
        app_route = "/distributor_instance/register"
        params = {"cluster_id": self._cluster_id}
        if self._workflow_run_id:
            params.update({'workflow_run_id': self._workflow_run_id})
        _, result = self.requester.send_request(
            app_route=app_route,
            message=params,
            request_type="post",
        )
        self._distributor_instance_id = result["distributor_instance_id"]

    def run(self) -> None:
        # start the cluster
        try:
            self._initialize_signal_handlers()
            self._cluster_interface.start()

            # signal via pipe that we are alive
            sys.stderr.write(f"ALIVE: distributor_instance_id={self._distributor_instance_id}")
            sys.stderr.flush()

            todo = it.cycle([
                TaskInstanceStatus.QUEUED,
                TaskInstanceStatus.LAUNCHED,
                TaskInstanceStatus.RUNNING,
                TaskInstanceStatus.TRIAGING,
                TaskInstanceStatus.KILL_SELF,
            ])
            while self._not_expunged():

                self.log_task_instance_report_by_date()

                # loop through all statuses and do as much work as we can till the heartbeat
                time_till_next_heartbeat = self._workflow_run_heartbeat_interval - (
                    time.time() - self._last_heartbeat_time
                )

                while todo and time_till_next_heartbeat > 0:
                    # log when this status started
                    start_time = time.time()

                    status = next(todo)

                    # refresh internal state from db
                    self.refresh_status_from_db(status)

                    # how long the heartbeat took
                    refresh_time = time.time()
                    time_till_next_heartbeat -= refresh_time - start_time

                    if status in self._command_generator_map.keys():
                        # process any work
                        self.process_status(status, time_till_next_heartbeat)
                        # how long the full status took
                        end_time = time.time()
                        time_till_next_heartbeat -= end_time - refresh_time

                    else:
                        end_time = refresh_time

                    logger.info(
                        f"Status processing for status={status} took "
                        f"{int((end_time - start_time))}s."
                    )

                if time_till_next_heartbeat > 0:
                    time.sleep(time_till_next_heartbeat)

        except DistributorInterruptedError:
            logger.info("Interrupt received!")
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            # stop distributor
            self._cluster_interface.stop()

            # signal via pipe that we are shutdown
            sys.stderr.write("SHUTDOWN")
            sys.stderr.flush()

    def process_status(self, status: str, timeout: Union[int, float] = -1) -> None:
        """Processes commands until all work is done or timeout is reached.

        Args:
            status: which status to process work for.
            timeout: time until we stop processing. -1 means process till no more work
        """
        start = time.time()

        # generate new distributor commands from this status
        command_generator_callable = self._command_generator_map[status]
        command_generator = command_generator_callable()
        self._distributor_commands = it.chain(command_generator)

        # this way we always process at least 1 command
        keep_iterating = True
        while keep_iterating:
            # run commands
            try:
                # get next command
                distributor_command = next(self._distributor_commands)
                distributor_command(self.raise_on_error)

                # if we need a status sync close the main generator. we will process remaining
                # transactions, but nothing new from the generator
                if not ((time.time() - start) < timeout or timeout == -1):
                    command_generator.close()

            except StopIteration:
                # stop processing commands if we are out of commands
                keep_iterating = False

        # update the state map
        task_instances = self._task_instance_status_map.pop(status)
        self._task_instance_status_map[status] = set()
        for task_instance in task_instances:
            if task_instance.status in self._task_instance_status_map:
                self._task_instance_status_map[task_instance.status].add(task_instance)
            else:
                self._task_instances.pop(task_instance.task_instance_id)

    def launch_task_instance_batch(
        self, task_instance_batch: Batch
    ) -> None:

        task_instance_batch.prepare_task_instance_batch_for_launch()

        # build worker node command
        command = self._cluster_interface.build_worker_node_command(
            task_instance_id=None,
            array_id=task_instance_batch.array_id,
            batch_id=task_instance_batch.batch_id,
        )
        distributor_commands: List[DistributorCommand] = []

        try:
            # submit array to distributor
            distributor_id_map = (
                self._cluster_interface.submit_array_to_batch_distributor(
                    command=command,
                    name=task_instance_batch.submission_name,
                    requested_resources=task_instance_batch.requested_resources,
                    array_length=len(task_instance_batch.task_instances),
                )
            )
            task_instance_batch.set_distributor_ids(distributor_id_map)

        except NotImplementedError:
            # create DistributorCommands to submit the launch if array isn't implemented
            for task_instance in task_instance_batch.task_instances:
                distributor_command = DistributorCommand(
                    self.launch_task_instance,
                    task_instance,
                )
                distributor_commands.append(distributor_command)

        except Exception as e:
            # if other error, transition to No ID status
            logger.exception(e)
            for task_instance in task_instance_batch.task_instances:
                distributor_command = DistributorCommand(
                    task_instance.transition_to_no_distributor_id, no_id_err_msg=str(e)
                )
                distributor_commands.append(distributor_command)

        else:

            launch_command = DistributorCommand(
                task_instance_batch.transition_to_launched, self._next_report_increment
            )
            # Log the distributor IDs
            log_distributor_ids_command = DistributorCommand(
                task_instance_batch.log_distributor_ids
            )

            distributor_commands.append(launch_command)
            distributor_commands.append(log_distributor_ids_command)

        finally:
            # Remove batch from registry
            self._batches.pop(task_instance_batch.batch_id)
            self._distributor_commands = it.chain(
                distributor_commands, self._distributor_commands
            )

    def launch_task_instance(self, task_instance: DistributorTaskInstance) -> None:
        """Submits a task instance on a given distributor.

        Adds the new task instance to self.submitted_or_running_task_instances.
        """
        # load resources
        try:
            requested_resources = task_instance.batch.requested_resources
        except AttributeError:
            task_instance.batch.load_requested_resources()
            requested_resources = task_instance.batch.requested_resources

        # Fetch the worker node command
        command = self._cluster_interface.build_worker_node_command(
            task_instance_id=task_instance.task_instance_id
        )

        # Submit to batch distributor
        try:
            distributor_id = self._cluster_interface.submit_to_batch_distributor(
                command=command,
                name=task_instance.submission_name,
                requested_resources=requested_resources,
            )
        except Exception as e:
            logger.exception(e)
            task_instance.transition_to_no_distributor_id(no_id_err_msg=str(e))

        else:
            # move from register queue to launch queue
            task_instance.transition_to_launched(
                distributor_id, self._next_report_increment
            )

    def triage_error(self, task_instances: set[DistributorTaskInstance]) -> None:
        """
        We can potentially have two cases of task instances in Triaging:
        1) Task instance was lost from squeue. Distributor does not log a heartbeat,
              active swarm moves to triaging. We want to get the error message and log it,
              move to an appropriate error state e.g. Z, U,
        2) Task instance is still in squeue, but distributor dies so can't log heartbeats.
              a) If new distributor instance picks up the launched TI before it misses a heartbeat
                    - no problem. New distributor puts it in the L register,
                        logs a heartbeat next round
              b) New distributor instance does not pick up the launched TI in time. Misses a heartbeat
                    - swarm will move it into Triaging state. Ideal behavior is to move back to launched
                    - Can use presence/lack of in sacct call to determine whether a TI gets moved
                        to an error state or back to launched
        """
        for task_instance in task_instances:
            try:
                r_value, r_msg = self._cluster_interface.get_remote_exit_info(
                    task_instance.distributor_id
                )
                task_instance.transition_to_error(r_msg, r_value)
            except RemoteExitInfoNotAvailable:
                # The most likely cause is that the job is still active, no db entry yet.
                # Move back to launched? Assume that it's lively?
                # TODO: check that get_remote_exit_info in jobmon_slurm works as expected.
                task_instance.transition_to_launched(
                    distributor_id=task_instance.distributor_id,
                    next_report_increment=self._next_report_increment
                )

    def kill_self(self, task_instance: DistributorTaskInstance) -> None:
        """Cancel a running task instance that is no longer logging heartbeats."""
        self._cluster_interface.terminate_task_instances([task_instance.distributor_id])
        task_instance.transition_to_error(
            "Task instance was self-killed.", TaskInstanceStatus.ERROR_FATAL
        )

    def log_task_instance_report_by_date(self) -> None:
        """Log the heartbeat to show that the task instance is still alive."""
        task_instances_launched = self._task_instance_status_map[
            TaskInstanceStatus.LAUNCHED
        ]
        submitted_or_running = self._cluster_interface.get_submitted_or_running(
            [x.distributor_id for x in task_instances_launched]
        )

        task_instance_ids_to_heartbeat: List[int] = []
        for task_instance_launched in task_instances_launched:
            if task_instance_launched.distributor_id in submitted_or_running:
                task_instance_ids_to_heartbeat.append(
                    task_instance_launched.task_instance_id
                )

        logger.debug(
            f"Logging heartbeat for task_instance {task_instance_ids_to_heartbeat}"
        )
        message: Dict = {
            "next_report_increment": self._next_report_increment,
            "task_instance_ids": task_instance_ids_to_heartbeat,
        }
        app_route = "/task_instance/log_report_by/batch"
        _ = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        self._last_heartbeat_time = time.time()

    def log_heartbeat(self):
        self.requester.send_request(f'/distributor_instance/{self._distributor_instance_id}/heartbeat',
                                    {}, 'post')

    def refresh_status_from_db(self, status: str) -> None:
        """Got to DB to check the list tis status."""
        active_task_instance_ids = {
            task_instance.task_instance_id
            for task_instance in self._task_instance_status_map[status]
        }
        message = {
            "task_instance_ids": list(active_task_instance_ids),
            "status": status,
        }
        if self._workflow_run_id:
            message['workflow_run_id'] = self._workflow_run_id
        app_route = f"/distributor_instance/{self._distributor_instance_id}/sync_status"
        return_code, result = self.requester.send_request(
            app_route=app_route, message=message, request_type="post"
        )

        # mutate the statuses and update the status map
        status_updates: List[tuple[int, str]] = result["status_updates"]
        new_task_instance_ids = []
        for task_instance_id, new_status in status_updates:
            # Discard is safe, no KeyError if task instance ID is not present
            active_task_instance_ids.discard(task_instance_id)
            try:
                task_instance = self._task_instances[task_instance_id]
            except KeyError:
                new_task_instance_ids.append(task_instance_id)
            else:
                # remove from old status set
                previous_status = task_instance.status
                self._task_instance_status_map[previous_status].remove(
                    task_instance
                )

                # change to new status and move to new set
                task_instance.status = new_status

                try:
                    self._task_instance_status_map[task_instance.status].add(
                        task_instance
                    )
                except KeyError:
                    # If the task instance is in a terminal state, e.g. D, E, etc.,
                    # expire it from the distributor
                    if task_instance_id in self._task_instances:
                        self._task_instances.pop(task_instance_id)

        if any(new_task_instance_ids):

            self._distributor_commands = it.chain(
                # TODO: Put this at the front of the end of the queue?
                # TODO: won't be evaluated until subsequent process_status call
                self._distributor_commands,
                self._generate_add_task_instance_callables(new_task_instance_ids, status)
            )

    def expire_inactive_task_instances(self):
        """Purge remaining task instances that belong to inactive WFRs."""
        # TODO: Call this in the main run loop independently
        # Alternative: workflow reaper updated to move task instances in reaped WFRs to
        # terminal states
        _, resp = self.requester.send_request(
            app_route=f"/batch/get_expired_batches",
            message={'batch_ids': list(self._batches.keys())},
            request_type='post'
        )

        inactive_batch_ids = resp['inactive_batch_ids']
        for batch_id in inactive_batch_ids:
            batch = self._batches.pop(batch_id)
            # Assumes that task instance status attribute is set properly
            # Could lead to a memory leak if a task instance's status and the register it's in
            # are not consistent
            # Alternative: For safety, just loop through all registers and try a discard
            for task_instance in batch.task_instances:
                self._task_instances.pop(task_instance.task_instance_id)
                if task_instance.status in self._task_instance_status_map:
                    self._task_instance_status_map[task_instance.status].discard(task_instance)

    def _generate_add_task_instance_callables(
        self, new_task_instance_ids: list[int], status: str, chunk_size: int = 50
    ) -> Generator:
        # Chain together, naive chunking
        for chunk_number in range(len(new_task_instance_ids) % chunk_size):
            start_idx = chunk_number * chunk_size
            end_idx = start_idx + chunk_size
            yield DistributorCommand(
                self._add_task_instances,
                new_task_instance_ids[start_idx:end_idx],
                status
            )

    def _add_task_instances(self, task_instance_ids: list[int], status: str):
        # Fetch metadata and create new task instances

        task_instance_ids = list(set(task_instance_ids) - set(self._task_instances.keys()))

        if not task_instance_ids:
            return
        _, resp = self.requester.send_request(
            app_route='/task_instance/get_task_instance_metadata',
            message={'task_instance_ids': task_instance_ids},
            request_type='post'
        )

        for task_instance_id, batch_id, distributor_id, cluster_id, workflow_run_id \
                in resp['task_instance_metadata']:
            if task_instance_id not in self._task_instances \
                    and status in self._task_instance_status_map:

                task_instance = DistributorTaskInstance(
                    task_instance_id=task_instance_id,
                    batch_id=batch_id,
                    cluster_id=cluster_id,
                    workflow_run_id=workflow_run_id,
                    status=status,
                    requester=self.requester
                )
                self._task_instances[task_instance_id] = task_instance
                self._task_instance_status_map[status].add(task_instance)

    def expunge(self):
        # TODO: Call this in an independent timing loop somewhere
        # Acts as a pseudo reaper. Multiple distributor instances might try to update the same
        # expired instance, but shouldn't be a concern - one will win, rest will fail
        self.requester.send_request(
            '/distributor_instance/expunge',
            {},
            'put'
        )

    def _not_expunged(self) -> bool:
        pass

    def _create_batches(
        self, queued_task_instances: Set[DistributorTaskInstance]
    ) -> Set[Batch]:
        """Only needed in Q state. Once in launched/running/triaging/etc.,
        batches are not needed. Monitoring done at task instance level
        """
        batch_ids = {ti.batch_id for ti in queued_task_instances}
        batch_map = self._get_batches(batch_ids)

        for task_instance in queued_task_instances:
            batch = batch_map[task_instance.batch_id]
            batch.add_task_instance(task_instance)

        # Register the batches with the instance
        self._batches.update(batch_map)
        return set(batch_map.values())

    def _get_batches(self, batch_ids: set[int]) -> Dict[int, Batch]:
        # Consider chunking
        if not any(batch_ids):
            return {}

        batch_map = {}
        _, batches = self.requester.send_request(
            app_route="/batch/get_batches",
            message={'batch_ids': list(batch_ids)},
            request_type='post'
        )
        for batch_id, task_resources_id, array_id, array_name in batches['batches']:
            new_batch = Batch(
                batch_id=batch_id,
                task_resources_id=task_resources_id,
                array_id=array_id,
                array_name=array_name,
                requester=self.requester
            )
            batch_map[batch_id] = new_batch
        return batch_map

    def _check_queued_for_work(self) -> Generator[DistributorCommand, None, None]:

        queued_task_instances = self._task_instance_status_map[TaskInstanceStatus.QUEUED]

        # Create batches on queued TIs
        # TODO: needs to be distributor command? To log HBs?
        # Could consider consolidating creating a batch and launching it, but would require
        # multiple trips to server for batch metadata
        batches = self._create_batches(queued_task_instances)
        for batch in batches:
            yield DistributorCommand(self.launch_task_instance_batch, batch)

    def _check_triaging_for_work(self) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in TRIAGING state.

        For TaskInstances with TRIAGING status, check the nature of no heartbeat,
        and change the statuses accordingly.
        """
        triaging_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.TRIAGING
        ]

        yield DistributorCommand(self.triage_error, triaging_task_instances)

    def _check_kill_self_for_work(self) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in KILL_SELF state.

        For TaskInstances with KILL_SELF status, terminate it and
        transition it to error accordingly.
        """
        kill_self_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.KILL_SELF
        ]

        for task_instance in kill_self_task_instances:
            yield DistributorCommand(self.kill_self, task_instance)
