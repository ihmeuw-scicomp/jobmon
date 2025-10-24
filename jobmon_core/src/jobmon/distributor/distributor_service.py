from __future__ import annotations

import asyncio
import itertools as it
import signal
import sys
import time
import traceback
from collections import defaultdict
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

import aiohttp
import structlog

from jobmon.core.cluster_protocol import ClusterDistributor
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import DistributorInterruptedError
from jobmon.core.requester import Requester
from jobmon.core.serializers import SerializeTaskInstanceBatch
from jobmon.core.structlog_utils import bind_context
from jobmon.distributor.distributor_command import DistributorCommand
from jobmon.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.distributor.task_instance_batch import TaskInstanceBatch

logger = structlog.get_logger(__name__)


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
        # Bind distributor instance context
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
            TaskInstanceStatus.NO_HEARTBEAT: set(),
        }
        # order through which we processes work
        gen_map: Dict[str, Callable[..., Generator[DistributorCommand, None, None]]] = {
            TaskInstanceStatus.QUEUED: self._check_queued_for_work,
            TaskInstanceStatus.INSTANTIATED: self._check_instantiated_for_work,
            TaskInstanceStatus.TRIAGING: self._check_triaging_for_work,
            TaskInstanceStatus.KILL_SELF: self._check_kill_self_for_work,
            TaskInstanceStatus.NO_HEARTBEAT: self._check_no_heartbeat_for_work,
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
        """Set the workflow run for this distributor service."""
        structlog.contextvars.bind_contextvars(workflow_run_id=workflow_run_id)
        workflow_run = DistributorWorkflowRun(workflow_run_id, requester=self.requester)
        self.workflow_run = workflow_run
        self.workflow_run.transition_to_instantiated()

        logger.info("Workflow run initialized")

    def run(self) -> None:
        """Main distributor run loop."""
        logger.info("Distributor running")

        # start the cluster
        try:
            self._initialize_signal_handlers()
            self.cluster_interface.start()
            self.workflow_run.transition_to_launched()

            # Send simple startup signal
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
                TaskInstanceStatus.NO_HEARTBEAT,
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
                    duration = int(end_time - start_time)
                    if duration > 5:  # Only log if took significant time
                        logger.info(
                            f"Status processing completed in {duration}s",
                            status=status,
                            duration_seconds=duration,
                        )

                # append done work to the end of the work order
                todo += done
                done = []

                logger.info(
                    f"Distributor service time_till_next_heartbeat: {time_till_next_heartbeat}"
                )
                if time_till_next_heartbeat > 0:
                    time.sleep(time_till_next_heartbeat)

                self.log_task_instance_report_by_date()

        except DistributorInterruptedError as e:
            logger.info(f"Distributor interrupted: {e}")
        except Exception as e:
            logger.exception("Distributor error", error=str(e))
            raise
        finally:
            logger.info("Distributor stopping")
            # stop distributor
            self.cluster_interface.stop()

            # Send simple shutdown signal
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
        task_instance_ids = [ti.task_instance_id for ti in task_instances]

        # Log each task instance ID for traceability (info level - state transition)
        for ti in task_instances:
            logger.info(
                f"Task instance {ti.task_instance_id} queued for instantiation",
                task_instance_id=ti.task_instance_id,
            )

        logger.debug(
            f"Requesting instantiation of {len(task_instances)} task instances",
            num_tasks=len(task_instances),
        )

        app_route = "/task_instance/instantiate_task_instances"
        _, result = self.requester.send_request(
            app_route=app_route,
            message={"task_instance_ids": task_instance_ids},
            request_type="post",
        )

        # construct batch. associations are made inside batch init
        num_batches = len(result["task_instance_batches"])
        logger.debug(
            "Batch instantiation completed",
            num_batches=num_batches,
            num_tasks=len(task_instances),
        )

        for batch in result["task_instance_batches"]:
            task_instance_batch_kwargs = SerializeTaskInstanceBatch.kwargs_from_wire(
                batch
            )

            array_id = task_instance_batch_kwargs["array_id"]
            batch_number = task_instance_batch_kwargs["array_batch_num"]
            logger.debug(
                "Distributor processing instantiated batch",
                array_id=array_id,
                array_batch_num=batch_number,
                batch_size=len(task_instance_batch_kwargs["task_instance_ids"]),
            )
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
                self._task_instance_batches[(array_id, batch_number)] = (
                    task_instance_batch
                )

            for task_instance_id in task_instance_batch_kwargs["task_instance_ids"]:
                task_instance = self._task_instances[task_instance_id]
                task_instance.status = TaskInstanceStatus.INSTANTIATED
                task_instance_batch.add_task_instance(task_instance)

    @bind_context(
        array_id="task_instance_batch.array_id",
        batch_number="task_instance_batch.batch_number",
    )
    def launch_task_instance_batch(
        self, task_instance_batch: TaskInstanceBatch
    ) -> None:
        self._task_instance_batches.pop(
            (task_instance_batch.array_id, task_instance_batch.batch_number)
        )

        batch_size = len(task_instance_batch.task_instances)
        logger.debug(
            "Distributor preparing batch for launch",
            array_id=task_instance_batch.array_id,
            array_batch_num=task_instance_batch.batch_number,
            batch_size=batch_size,
        )

        # Log each task instance (info level - state transition)
        for ti in task_instance_batch.task_instances:
            logger.info(
                f"Task instance {ti.task_instance_id} preparing for launch",
                task_instance_id=ti.task_instance_id,
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
            # submit array to batch distributor
            logger.debug(
                "Submitting batch to cluster",
                array_id=task_instance_batch.array_id,
                array_batch_num=task_instance_batch.batch_number,
                batch_size=batch_size,
                submission_name=task_instance_batch.submission_name,
            )
            distributor_id_map = (
                self.cluster_interface.submit_array_to_batch_distributor(
                    command=command,
                    name=task_instance_batch.submission_name,
                    requested_resources=task_instance_batch.requested_resources,
                    array_length=batch_size,
                )
            )
            task_instance_batch.set_distributor_ids(distributor_id_map)
            logger.info(
                "Batch submitted to cluster successfully",
                array_id=task_instance_batch.array_id,
                array_batch_num=task_instance_batch.batch_number,
                batch_size=batch_size,
            )

        except NotImplementedError:
            # create DistributorCommands to submit the launch if array isn't implemented
            logger.debug(
                "Array submission not supported, launching individually",
                batch_size=batch_size,
            )
            for task_instance in task_instance_batch.task_instances:
                distributor_command = DistributorCommand(
                    self.launch_task_instance,
                    task_instance,
                )
                distributor_commands.append(distributor_command)

        except Exception as e:
            # if other error, transition to No ID status
            stack_trace = traceback.format_exc()
            logger.exception(
                "Batch launch failed",
                error=str(e),
                batch_size=batch_size,
            )
            for task_instance in task_instance_batch.task_instances:
                distributor_command = DistributorCommand(
                    task_instance.transition_to_no_distributor_id,
                    no_id_err_msg=stack_trace,
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

    @bind_context(task_instance_id="task_instance.task_instance_id")
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
            logger.debug("Task instance launched", distributor_id=distributor_id)
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.exception("Task instance launch failed", error=str(e))
            task_instance.transition_to_no_distributor_id(no_id_err_msg=stack_trace)

        else:
            # move from register queue to launch queue
            task_instance.transition_to_launched(
                distributor_id, self._next_report_increment
            )

    @bind_context(task_instance_id="task_instance.task_instance_id")
    def triage_error(self, task_instance: DistributorTaskInstance) -> None:
        """Triage a running task instance that has missed a heartbeat.

        Allowed transitions are (R, U, Z, F)
        """
        logger.info(
            "Distributor triaging task instance error",
            distributor_id=task_instance.distributor_id,
        )

        r_value, r_msg = self.cluster_interface.get_remote_exit_info(
            task_instance.distributor_id
        )
        logger.info(
            "Retrieved exit info from cluster",
            return_code=r_value,
            error_message=(
                r_msg[:100] if r_msg else None
            ),  # Truncate for log readability
        )

        task_instance.transition_to_error(r_msg, r_value)

        logger.info(
            "Task instance triage completed",
            new_status=task_instance.status,
            error_state=task_instance.error_state,
        )

    @bind_context(
        array_id="task_instance_batch.array_id",
        batch_number="task_instance_batch.batch_number",
    )
    def kill_self_batch(self, task_instance_batch: TaskInstanceBatch) -> None:
        """Terminate all TIs in this batch.

        Args:
            task_instance_batch: The batch of task instances to terminate.
        """
        batch_size = len(task_instance_batch.task_instances)
        logger.info(
            "Distributor terminating KILL_SELF batch",
            batch_size=batch_size,
        )

        # 1) Collect the distributor IDs to terminate
        distributor_ids = [
            ti.distributor_id
            for ti in task_instance_batch.task_instances
            if ti.distributor_id is not None
        ]

        # 2) If there are jobs to terminate, call the cluster
        if distributor_ids:
            logger.info(
                "Sending termination signal to cluster",
                num_tasks=len(distributor_ids),
                distributor_ids=distributor_ids[:10],  # Log first 10
            )
            self.cluster_interface.terminate_task_instances(distributor_ids)
            logger.info(
                "Cluster termination completed",
                num_tasks=len(distributor_ids),
            )

        # 3) Mark them as killed in the DB
        task_instance_batch.transition_to_killed()

    @bind_context(task_instance_id="task_instance.task_instance_id")
    def no_heartbeat_error(self, task_instance: DistributorTaskInstance) -> None:
        """Move a task instance in NO_HEARTBEAT state to a recoverable error state.

        This signal is sent from the swarm in the event a task instance in LAUNCHED state
        fails to log a heartbeat, either due to the distributor failing to log a heartbeat
        batch or due to the worker node failing to start up properly.

        ERROR state allows for a retry, so that a new task instance can attempt to run.
        """
        logger.info(
            "Distributor processing NO_HEARTBEAT task instance",
            distributor_id=task_instance.distributor_id,
        )

        task_instance.transition_to_error(
            "Task instance never reported a heartbeat after scheduling. Will retry. "
            "May be caused by distributor heartbeat failure or worker startup issue often due "
            "to cluster node problem. If the retry fails, resume the task with Slurm logs "
            "enabled by setting 'standard_error' and 'standard_output' in your compute "
            "resources dictionary.",
            TaskInstanceStatus.ERROR,
        )

        logger.info(
            "Task instance transitioned NO_HEARTBEAT → ERROR",
            new_status=task_instance.status,
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

        if any(task_instance_ids_to_heartbeat):
            # Create batches of task instance IDs
            chunk_size = 500
            task_instance_batches = [
                task_instance_ids_to_heartbeat[i : i + chunk_size]
                for i in range(0, len(task_instance_ids_to_heartbeat), chunk_size)
            ]

            # Send heartbeat for each batch
            logger.info(
                f"Sending heartbeats for {len(task_instance_ids_to_heartbeat)} task instances",
                num_tasks=len(task_instance_ids_to_heartbeat),
                num_batches=len(task_instance_batches),
            )
            asyncio.run(self._log_heartbeats(task_instance_batches))

        self._last_heartbeat_time = time.time()

    async def _log_heartbeats(self, task_instance_batches: List[List[int]]) -> None:
        """Create a task for each batch of task instances to send heartbeat."""
        async with aiohttp.ClientSession() as session:
            heartbeat_tasks = [
                asyncio.create_task(self._log_heartbeat_by_batch(session, batch))
                for batch in task_instance_batches
            ]
            await asyncio.gather(*heartbeat_tasks)

    async def _log_heartbeat_by_batch(
        self, session: aiohttp.ClientSession, task_instance_ids_to_heartbeat: List[int]
    ) -> None:
        """Send heartbeat for a batch of task instances using sophisticated retry logic."""
        message: Dict = {
            "next_report_increment": self._next_report_increment,
            "task_instance_ids": task_instance_ids_to_heartbeat,
        }
        app_route = "/task_instance/log_report_by/batch"

        # Use the sophisticated async requester with tenacity retry logic
        await self.requester.send_request_async(
            session=session,
            app_route=app_route,
            message=message,
            request_type="post",
            tenacious=True,
        )

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
        _, result = self.requester.send_request(
            app_route=app_route, message=message, request_type="post"
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

                    try:
                        self._task_instance_status_map[new_status].add(task_instance)
                        # change to new status and move to new set
                        task_instance.status = new_status

                    except KeyError:
                        # If the task instance is in a terminal state, e.g. D, E, etc.,
                        # expire it from the distributor
                        del self._task_instances[task_instance_id]

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

        if triaging_task_instances:
            logger.info(
                "Distributor processing TRIAGING task instances",
                num_task_instances=len(triaging_task_instances),
            )

        for task_instance in triaging_task_instances:
            yield DistributorCommand(self.triage_error, task_instance)

    def _check_kill_self_for_work(self) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in KILL_SELF state, grouped by their TaskInstanceBatch."""
        kill_self_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.KILL_SELF]
        )

        if kill_self_task_instances:
            logger.info(
                "Distributor processing KILL_SELF task instances",
                num_task_instances=len(kill_self_task_instances),
            )

            # Log each task instance being killed (info level - state transition)
            for ti in kill_self_task_instances:
                logger.info(
                    "Task instance marked for termination",
                    task_instance_id=ti.task_instance_id,
                    distributor_id=ti.distributor_id,
                )

        # Group TIs by their batch
        batch_map = defaultdict(list)
        for ti in kill_self_task_instances:
            batch_map[ti.batch].append(ti)

        for batch_obj, _ in batch_map.items():
            # If you'd like to verify they still have KILL_SELF status, etc., do it here.
            yield DistributorCommand(self.kill_self_batch, batch_obj)

    def _check_no_heartbeat_for_work(self) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in NO_HEARTBEAT state.

        For TaskInstances with NO_HEARTBEAT status, move to an error recoverable state
        """
        no_heartbeat_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.NO_HEARTBEAT
        ]

        if no_heartbeat_task_instances:
            logger.info(
                "Distributor processing NO_HEARTBEAT task instances",
                num_task_instances=len(no_heartbeat_task_instances),
            )

        for task_instance in no_heartbeat_task_instances:
            yield DistributorCommand(self.no_heartbeat_error, task_instance)
