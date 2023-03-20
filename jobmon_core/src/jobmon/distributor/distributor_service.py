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
    Tuple,
    Union,
)

from jobmon.core.cluster_protocol import ClusterDistributor
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import DistributorInterruptedError, InvalidResponse
from jobmon.core.requester import http_request_ok, Requester
from jobmon.core.serializers import SerializeTaskInstanceBatch
from jobmon.distributor.distributor_command import DistributorCommand
from jobmon.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.distributor.task_instance_batch import TaskInstanceBatch


logger = logging.getLogger(__name__)


class DistributorService:
    def __init__(
        self,
        cluster_interface: ClusterDistributor,
        requester: Optional[Requester] = None,
        workflow_run_heartbeat_interval: Optional[int] = None,
        task_instance_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
        distributor_poll_interval: Optional[int] = None,
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

        # indexing of task instance by associated id
        self._task_instances: Dict[int, DistributorTaskInstance] = {}
        self._task_instance_batches: Dict[Tuple[int, int], TaskInstanceBatch] = {}

        # work queue
        self._distributor_commands: Iterator[DistributorCommand] = it.chain([])

        # indexing of task instances by status
        self._task_instance_status_map: Dict[str, Set[DistributorTaskInstance]] = {
            TaskInstanceStatus.QUEUED: set(),
            TaskInstanceStatus.INSTANTIATED: set(),
            TaskInstanceStatus.LAUNCHED: set(),
            TaskInstanceStatus.RUNNING: set(),
            TaskInstanceStatus.TRIAGING: set(),
            TaskInstanceStatus.KILL_SELF: set(),
        }
        # order through which we processes work
        gen_map: Dict[str, Callable[..., Generator[DistributorCommand, None, None]]] = {
            TaskInstanceStatus.QUEUED: self._check_queued_for_work,
            TaskInstanceStatus.INSTANTIATED: self._check_instantiated_for_work,
            TaskInstanceStatus.TRIAGING: self._check_triaging_for_work,
            TaskInstanceStatus.KILL_SELF: self._check_kill_self_for_work,
        }
        self._command_generator_map = gen_map

        # syncronization timings
        self._last_heartbeat_time = time.time()

        # cluster API
        self.cluster_interface = cluster_interface

        # web service API
        if requester is None:
            self.requester = Requester.from_defaults()
        else:
            self.requester = requester

    @property
    def _next_report_increment(self) -> float:
        return self._heartbeat_report_by_buffer * self._task_instance_heartbeat_interval

    def set_workflow_run(self, workflow_run_id: int) -> None:
        workflow_run = DistributorWorkflowRun(workflow_run_id, self.requester)
        self.workflow_run = workflow_run
        self.workflow_run.transition_to_instantiated()

    def run(self) -> None:
        # start the cluster
        try:
            self._initialize_signal_handlers()
            self.cluster_interface.start()
            self.workflow_run.transition_to_launched()

            # signal via pipe that we are alive
            sys.stderr.write("ALIVE")
            sys.stderr.flush()

            done: List[str] = []
            todo = [
                TaskInstanceStatus.QUEUED,
                TaskInstanceStatus.INSTANTIATED,
                TaskInstanceStatus.LAUNCHED,
                TaskInstanceStatus.RUNNING,
                TaskInstanceStatus.TRIAGING,
                TaskInstanceStatus.KILL_SELF,
            ]
            while True:
                # loop through all statuses and do as much work as we can till the heartbeat
                time_till_next_heartbeat = self._workflow_run_heartbeat_interval - (
                    time.time() - self._last_heartbeat_time
                )

                while todo and time_till_next_heartbeat > 0:
                    # log when this status started
                    start_time = time.time()

                    # remove status from todo and add to done
                    status = todo.pop(0)

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

                    done.append(status)
                    logger.info(
                        f"Status processing for status={status} took "
                        f"{int((end_time - start_time))}s."
                    )

                # append done work to the end of the work order
                todo += done
                done = []

                if time_till_next_heartbeat > 0:
                    time.sleep(time_till_next_heartbeat)

                self.log_task_instance_report_by_date()

        except DistributorInterruptedError:
            logger.info("Interrupt received!")
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            # stop distributor
            self.cluster_interface.stop()

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
            self._task_instance_status_map[task_instance.status].add(task_instance)

    def instantiate_task_instances(
        self, task_instances: List[DistributorTaskInstance]
    ) -> None:
        app_route = "/task_instance/instantiate_task_instances"
        return_code, result = self.requester.send_request(
            app_route=app_route,
            message={
                "task_instance_ids": [
                    task_instance.task_instance_id for task_instance in task_instances
                ]
            },
            request_type="post",
        )
        if not http_request_ok(return_code):
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {result}"
            )

        # construct batch. associations are made inside batch init
        for batch in result["task_instance_batches"]:
            task_instance_batch_kwargs = SerializeTaskInstanceBatch.kwargs_from_wire(
                batch
            )

            array_id = task_instance_batch_kwargs["array_id"]
            batch_number = task_instance_batch_kwargs["array_batch_num"]
            try:
                task_instance_batch = self._task_instance_batches[
                    (array_id, batch_number)
                ]
            except KeyError:
                task_instance_batch = TaskInstanceBatch(
                    array_id=array_id,
                    array_name=task_instance_batch_kwargs["array_name"],
                    array_batch_num=batch_number,
                    task_resources_id=task_instance_batch_kwargs["task_resources_id"],
                    requester=self.requester,
                )
                self._task_instance_batches[
                    (array_id, batch_number)
                ] = task_instance_batch

            for task_instance_id in task_instance_batch_kwargs["task_instance_ids"]:
                task_instance = self._task_instances[task_instance_id]
                task_instance.status = TaskInstanceStatus.INSTANTIATED
                task_instance_batch.add_task_instance(task_instance)

    def launch_task_instance_batch(
        self, task_instance_batch: TaskInstanceBatch
    ) -> None:
        self._task_instance_batches.pop(
            (task_instance_batch.array_id, task_instance_batch.batch_number)
        )
        # record batch info in db
        task_instance_batch.prepare_task_instance_batch_for_launch()

        # build worker node command
        command = self.cluster_interface.build_worker_node_command(
            task_instance_id=None,
            array_id=task_instance_batch.array_id,
            batch_number=task_instance_batch.batch_number,
        )
        distributor_commands: List[DistributorCommand] = []

        try:
            # submit array to distributor
            distributor_id_map = (
                self.cluster_interface.submit_array_to_batch_distributor(
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
            # if successful log a transition to launched
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
        command = self.cluster_interface.build_worker_node_command(
            task_instance_id=task_instance.task_instance_id
        )

        # Submit to batch distributor
        try:
            distributor_id = self.cluster_interface.submit_to_batch_distributor(
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

    def triage_error(self, task_instance: DistributorTaskInstance) -> None:
        r_value, r_msg = self.cluster_interface.get_remote_exit_info(
            task_instance.distributor_id
        )
        task_instance.transition_to_error(r_msg, r_value)

    def kill_self(self, task_instance: DistributorTaskInstance) -> None:
        self.cluster_interface.terminate_task_instances([task_instance.distributor_id])
        task_instance.transition_to_error(
            "Task instance was self-killed.", TaskInstanceStatus.ERROR_FATAL
        )

    def log_task_instance_report_by_date(self) -> None:
        """Log the heartbeat to show that the task instance is still alive."""
        task_instances_launched = self._task_instance_status_map[
            TaskInstanceStatus.LAUNCHED
        ]
        submitted_or_running = self.cluster_interface.get_submitted_or_running(
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
        return_code, result = self.requester.send_request(
            app_route="/task_instance/log_report_by/batch",
            message=message,
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"{app_route} Returned={return_code}. Message={message}"
            )
        self._last_heartbeat_time = time.time()

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

    def refresh_status_from_db(self, status: str) -> None:
        """Got to DB to check the list tis status."""
        message = {
            "task_instance_ids": [
                task_instance.task_instance_id
                for task_instance in self._task_instance_status_map[status]
            ],
            "status": status,
        }
        app_route = f"/workflow_run/{self.workflow_run.workflow_run_id}/sync_status"
        return_code, result = self.requester.send_request(
            app_route=app_route, message=message, request_type="post"
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"{app_route} Returned={return_code}. Message={message}"
            )

        # mutate the statuses and update the status map
        status_updates: Dict[str, List[int]] = result["status_updates"]
        for new_status, task_instance_ids in status_updates.items():
            for task_instance_id in task_instance_ids:
                try:
                    task_instance = self._task_instances[task_instance_id]

                except KeyError:
                    task_instance = DistributorTaskInstance(
                        task_instance_id,
                        self.workflow_run.workflow_run_id,
                        new_status,
                        self.requester,
                    )
                    self._task_instance_status_map[task_instance.status].add(
                        task_instance
                    )
                    self._task_instances[task_instance.task_instance_id] = task_instance

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
                        continue

    def _check_queued_for_work(self) -> Generator[DistributorCommand, None, None]:
        queued_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.QUEUED]
        )
        queued_task_instances.sort()
        chunk_size = 500
        while queued_task_instances:
            ti_list = queued_task_instances[:chunk_size]
            queued_task_instances = queued_task_instances[chunk_size:]
            yield DistributorCommand(self.instantiate_task_instances, ti_list)

    def _check_instantiated_for_work(self) -> Generator[DistributorCommand, None, None]:
        # compute the task_instances that can be launched
        instantiated_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.INSTANTIATED]
        )
        task_instance_batches = set(
            [task_instance.batch for task_instance in instantiated_task_instances]
        )

        for batch in task_instance_batches:
            yield DistributorCommand(self.launch_task_instance_batch, batch)

    def _check_triaging_for_work(self) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in TRIAGING state.

        For TaskInstances with TRIAGING status, check the nature of no heartbeat,
        and change the statuses accordingly.
        """
        triaging_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.TRIAGING
        ]
        for task_instance in triaging_task_instances:
            yield DistributorCommand(self.triage_error, task_instance)

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
