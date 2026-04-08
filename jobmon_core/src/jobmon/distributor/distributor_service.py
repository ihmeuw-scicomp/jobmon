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
from jobmon.core.logging import set_jobmon_context
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
        gen_map: Dict[
            str,
            Callable[..., Generator[DistributorCommand, None, None]],
        ] = {
            TaskInstanceStatus.QUEUED: self._check_queued_for_work,
            TaskInstanceStatus.INSTANTIATED: self._check_instantiated_for_work,
            TaskInstanceStatus.TRIAGING: self._check_triaging_for_work,
            TaskInstanceStatus.KILL_SELF: self._check_kill_self_for_work,
            TaskInstanceStatus.NO_HEARTBEAT: (self._check_no_heartbeat_for_work),
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

        # Flag for signaling the heartbeat task to stop
        self._stopping = False

    @property
    def _next_report_increment(self) -> float:
        return self._heartbeat_report_by_buffer * self._task_instance_heartbeat_interval

    def set_workflow_run(self, workflow_run_id: int) -> None:
        """Set the workflow run for this distributor service."""
        set_jobmon_context(workflow_run_id=workflow_run_id)
        workflow_run = DistributorWorkflowRun(workflow_run_id, requester=self.requester)
        self.workflow_run = workflow_run
        self.workflow_run.transition_to_instantiated()

        logger.info("Workflow run initialized")

    def run(self) -> None:
        """Main distributor entry point.

        Sets up signal handlers and delegates to the async run loop.
        """
        logger.info("Distributor running")

        try:
            self._initialize_signal_handlers()
            self.cluster_interface.start()
            self.workflow_run.transition_to_launched()

            # Send simple startup signal
            sys.stderr.write("ALIVE")
            sys.stderr.flush()

            asyncio.run(self._async_run())

        except DistributorInterruptedError as e:
            logger.info(f"Distributor interrupted: {e}")
        finally:
            logger.info("Distributor stopping")
            self._stopping = True
            self.cluster_interface.stop()

            # Send simple shutdown signal
            sys.stderr.write("SHUTDOWN")
            sys.stderr.flush()

    async def _async_run(self) -> None:
        """Async main loop with independent heartbeat task."""
        # Start heartbeat as an independent background task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
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
            last_cycle_time = time.time()
            while True:
                time_till_next_cycle = self._workflow_run_heartbeat_interval - (
                    time.time() - last_cycle_time
                )

                while todo and time_till_next_cycle > 0:
                    start_time = time.time()

                    status = todo.pop(0)

                    # refresh internal state from db
                    try:
                        await self.refresh_status_from_db(status)
                    except DistributorInterruptedError:
                        raise
                    except Exception as e:
                        logger.warning(
                            "Status refresh failed, will retry next cycle",
                            status=status,
                            error=str(e),
                        )
                        done.append(status)
                        time_till_next_cycle -= time.time() - start_time
                        continue

                    refresh_time = time.time()
                    time_till_next_cycle -= refresh_time - start_time

                    if status in self._command_generator_map.keys():
                        await self.process_status(status, time_till_next_cycle)
                        end_time = time.time()
                        time_till_next_cycle -= end_time - refresh_time
                    else:
                        end_time = refresh_time

                    done.append(status)
                    duration = int(end_time - start_time)
                    if duration > 5:
                        logger.info(
                            f"Status processing completed in {duration}s",
                            status=status,
                            duration_seconds=duration,
                        )

                # append done work to the end of the work order
                todo += done
                done = []

                # Sleep until next cycle, yielding to heartbeat task
                if time_till_next_cycle > 0:
                    await asyncio.sleep(time_till_next_cycle)

                last_cycle_time = time.time()

        finally:
            self._stopping = True
            heartbeat_task.cancel()
            await self.requester.close_async_session()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self) -> None:
        """Background task that sends heartbeats on a fixed interval.

        Runs independently of the main processing loop so heartbeats
        are never starved by long-running triage or status processing.
        """
        while not self._stopping:
            try:
                await self.log_task_instance_report_by_date()
            except DistributorInterruptedError:
                raise
            except Exception as e:
                logger.warning(
                    "Heartbeat logging failed, will retry next cycle",
                    error=str(e),
                )
            await asyncio.sleep(self._workflow_run_heartbeat_interval)

    # Statuses whose commands are independent per-TI and can run
    # concurrently via asyncio.gather().
    PARALLELIZABLE_STATUSES = frozenset(
        {TaskInstanceStatus.TRIAGING, TaskInstanceStatus.NO_HEARTBEAT}
    )

    # Max concurrent commands for parallel statuses. Limits load on
    # the cluster API and jobmon server during large triage batches.
    PARALLEL_CONCURRENCY = 20

    async def process_status(
        self, status: str, timeout: Union[int, float] = -1
    ) -> None:
        """Process commands for a status.

        For TRIAGING and NO_HEARTBEAT, commands are independent per-TI
        and run concurrently via asyncio.gather() with a concurrency
        limit. For other statuses, commands run sequentially to
        preserve ordering.

        Args:
            status: which status to process work for.
            timeout: time until we stop processing. -1 means process
                     till no more work.
        """
        start = time.time()

        command_generator_callable = self._command_generator_map[status]
        command_generator = command_generator_callable()

        if status in self.PARALLELIZABLE_STATUSES:
            commands = list(command_generator)
            if commands:
                sem = asyncio.Semaphore(self.PARALLEL_CONCURRENCY)

                async def limited(cmd: DistributorCommand) -> None:
                    async with sem:
                        await cmd(self.raise_on_error)

                results = await asyncio.gather(
                    *(limited(cmd) for cmd in commands),
                    return_exceptions=True,
                )
                for i, result in enumerate(results):
                    if isinstance(result, DistributorInterruptedError):
                        raise result
                    if isinstance(result, Exception):
                        commands[i].error_raised = True
                        if self.raise_on_error:
                            raise result
        else:
            self._distributor_commands = it.chain(command_generator)
            keep_iterating = True
            while keep_iterating:
                try:
                    distributor_command = next(self._distributor_commands)
                    await distributor_command(self.raise_on_error)

                    if not ((time.time() - start) < timeout or timeout == -1):
                        command_generator.close()

                except StopIteration:
                    keep_iterating = False

        # update the state map
        task_instances = self._task_instance_status_map.pop(status)
        self._task_instance_status_map[status] = set()
        for task_instance in task_instances:
            try:
                self._task_instance_status_map[task_instance.status].add(task_instance)
            except KeyError:
                del self._task_instances[task_instance.task_instance_id]

    async def instantiate_task_instances(
        self, task_instances: List[DistributorTaskInstance]
    ) -> None:
        task_instance_ids = [ti.task_instance_id for ti in task_instances]

        for ti in task_instances:
            logger.info(
                f"Task instance {ti.task_instance_id} queued for " f"instantiation",
                task_instance_id=ti.task_instance_id,
            )

        logger.debug(
            f"Requesting instantiation of " f"{len(task_instances)} task instances",
            num_tasks=len(task_instances),
        )

        app_route = "/task_instance/instantiate_task_instances"
        _, result = await self.requester.async_request(
            app_route=app_route,
            message={"task_instance_ids": task_instance_ids},
            request_type="post",
        )

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
    async def launch_task_instance_batch(
        self, task_instance_batch: TaskInstanceBatch
    ) -> None:
        batch_key = (
            task_instance_batch.array_id,
            task_instance_batch.batch_number,
        )
        if self._task_instance_batches.pop(batch_key, None) is None:
            logger.warning(
                "Batch already removed from tracking, skipping launch",
                array_id=task_instance_batch.array_id,
                array_batch_num=task_instance_batch.batch_number,
            )
            return

        batch_size = len(task_instance_batch.task_instances)
        logger.debug(
            "Distributor preparing batch for launch",
            array_id=task_instance_batch.array_id,
            array_batch_num=task_instance_batch.batch_number,
            batch_size=batch_size,
        )

        for ti in task_instance_batch.task_instances:
            logger.info(
                f"Task instance {ti.task_instance_id} preparing for launch",
                task_instance_id=ti.task_instance_id,
            )

        await task_instance_batch.prepare_task_instance_batch_for_launch()

        command = self.cluster_interface.build_worker_node_command(
            task_instance_id=None,
            array_id=task_instance_batch.array_id,
            batch_number=task_instance_batch.batch_number,
        )
        distributor_commands: List[DistributorCommand] = []

        try:
            logger.debug(
                "Submitting batch to cluster",
                array_id=task_instance_batch.array_id,
                array_batch_num=task_instance_batch.batch_number,
                batch_size=batch_size,
                submission_name=task_instance_batch.submission_name,
            )
            distributor_id_map = (
                await (
                    self.cluster_interface.submit_array_to_batch_distributor_async(
                        command=command,
                        name=task_instance_batch.submission_name,
                        requested_resources=(task_instance_batch.requested_resources),
                        array_length=batch_size,
                    )
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
            launch_command = DistributorCommand(
                task_instance_batch.transition_to_launched,
                self._next_report_increment,
            )
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
    async def launch_task_instance(
        self, task_instance: DistributorTaskInstance
    ) -> None:
        """Submits a task instance on a given distributor."""
        try:
            requested_resources = task_instance.batch.requested_resources
        except AttributeError:
            await task_instance.batch.load_requested_resources()
            requested_resources = task_instance.batch.requested_resources

        command = self.cluster_interface.build_worker_node_command(
            task_instance_id=task_instance.task_instance_id
        )

        try:
            distributor_id = (
                await self.cluster_interface.submit_to_batch_distributor_async(
                    command=command,
                    name=task_instance.submission_name,
                    requested_resources=requested_resources,
                )
            )
            logger.debug("Task instance launched", distributor_id=distributor_id)
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.exception("Task instance launch failed", error=str(e))
            await task_instance.transition_to_no_distributor_id(
                no_id_err_msg=stack_trace
            )

        else:
            await task_instance.transition_to_launched(
                distributor_id, self._next_report_increment
            )

    # Maximum wall-clock time to wait for cluster accounting to
    # finalize before proceeding with potentially inaccurate metadata.
    MAX_TRIAGE_DURATION: float = 300.0  # 5 minutes

    @bind_context(task_instance_id="task_instance.task_instance_id")
    async def triage_error(self, task_instance: DistributorTaskInstance) -> None:
        """Triage a running task instance that has missed a heartbeat.

        Queries the cluster for exit info asynchronously. If accounting
        has not finalized yet, defers the transition and retries on the
        next distributor cycle.
        """
        now = time.time()
        if task_instance.first_triage_time is None:
            task_instance.first_triage_time = now
        task_instance.triage_attempts += 1

        logger.info(
            "Distributor triaging task instance error",
            distributor_id=task_instance.distributor_id,
            triage_attempt=task_instance.triage_attempts,
        )

        result = await self.cluster_interface.get_remote_exit_info_async(
            task_instance.distributor_id
        )

        # Support legacy plugins returning (error_state, error_message)
        if isinstance(result, tuple):
            r_value, r_msg = result
            finalized = True
        else:
            r_value = result.error_state
            r_msg = result.error_message
            finalized = result.finalized

        logger.info(
            "Retrieved exit info from cluster",
            return_code=r_value,
            error_message=(r_msg[:100] if r_msg else None),
            finalized=finalized,
        )

        if not finalized:
            elapsed = now - task_instance.first_triage_time
            if elapsed < self.MAX_TRIAGE_DURATION:
                logger.info(
                    "Cluster accounting not finalized, "
                    "deferring triage to next cycle",
                    distributor_id=task_instance.distributor_id,
                    triage_attempts=task_instance.triage_attempts,
                    elapsed_seconds=round(elapsed, 1),
                    max_duration=self.MAX_TRIAGE_DURATION,
                )
                return

            logger.warning(
                "Cluster accounting still unfinalized after timeout, "
                "proceeding with available metadata",
                distributor_id=task_instance.distributor_id,
                triage_attempts=task_instance.triage_attempts,
                elapsed_seconds=round(elapsed, 1),
            )

        await task_instance.transition_to_error(r_msg, r_value)

        logger.info(
            "Task instance triage completed",
            new_status=task_instance.status,
            error_state=task_instance.error_state,
        )

    @bind_context(
        array_id="task_instance_batch.array_id",
        batch_number="task_instance_batch.batch_number",
    )
    async def kill_self_batch(self, task_instance_batch: TaskInstanceBatch) -> None:
        """Terminate all TIs in this batch."""
        batch_size = len(task_instance_batch.task_instances)
        logger.info(
            "Distributor terminating KILL_SELF batch",
            batch_size=batch_size,
        )

        distributor_ids = [
            ti.distributor_id
            for ti in task_instance_batch.task_instances
            if ti.distributor_id is not None
        ]

        if distributor_ids:
            logger.info(
                "Sending termination signal to cluster",
                num_tasks=len(distributor_ids),
                distributor_ids=distributor_ids[:10],
            )
            await self.cluster_interface.terminate_task_instances_async(distributor_ids)
            logger.info(
                "Cluster termination completed",
                num_tasks=len(distributor_ids),
            )

        await task_instance_batch.transition_to_killed()

    @bind_context(task_instance_id="task_instance.task_instance_id")
    async def no_heartbeat_error(self, task_instance: DistributorTaskInstance) -> None:
        """Move a NO_HEARTBEAT task instance to a recoverable error state."""
        logger.info(
            "Distributor processing NO_HEARTBEAT task instance",
            distributor_id=task_instance.distributor_id,
        )

        await task_instance.transition_to_error(
            "Task instance never reported a heartbeat after scheduling. "
            "Will retry. May be caused by distributor heartbeat failure "
            "or worker startup issue often due to cluster node problem. "
            "If the retry fails, resume the task with Slurm logs "
            "enabled by setting 'standard_error' and 'standard_output' "
            "in your compute resources dictionary.",
            TaskInstanceStatus.ERROR,
        )

        logger.info(
            "Task instance transitioned NO_HEARTBEAT → ERROR",
            new_status=task_instance.status,
        )

    async def log_task_instance_report_by_date(self) -> None:
        """Log heartbeats for launched task instances.

        Uses the async cluster interface to check which TIs are still
        active, then sends heartbeat batches to the server.
        """
        task_instances_launched = self._task_instance_status_map[
            TaskInstanceStatus.LAUNCHED
        ]
        try:
            submitted_or_running = (
                await self.cluster_interface.get_submitted_or_running_async(
                    [x.distributor_id for x in task_instances_launched]
                )
            )
        except DistributorInterruptedError:
            raise
        except Exception as e:
            logger.warning(
                "Failed to query submitted/running jobs, " "skipping heartbeat",
                error=str(e),
            )
            self._last_heartbeat_time = time.time()
            return

        task_instance_ids_to_heartbeat: List[int] = []
        for task_instance_launched in task_instances_launched:
            if task_instance_launched.distributor_id in submitted_or_running:
                task_instance_ids_to_heartbeat.append(
                    task_instance_launched.task_instance_id
                )

        if any(task_instance_ids_to_heartbeat):
            chunk_size = 500
            task_instance_batches = [
                task_instance_ids_to_heartbeat[i : i + chunk_size]
                for i in range(0, len(task_instance_ids_to_heartbeat), chunk_size)
            ]

            logger.info(
                f"Sending heartbeats for "
                f"{len(task_instance_ids_to_heartbeat)} task instances",
                num_tasks=len(task_instance_ids_to_heartbeat),
                num_batches=len(task_instance_batches),
            )
            await self._log_heartbeats(task_instance_batches)

        self._last_heartbeat_time = time.time()

    async def _log_heartbeats(self, task_instance_batches: List[List[int]]) -> None:
        """Send heartbeat batches concurrently."""
        async with aiohttp.ClientSession() as session:
            heartbeat_tasks = [
                asyncio.create_task(self._log_heartbeat_by_batch(session, batch))
                for batch in task_instance_batches
            ]
            results = await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, DistributorInterruptedError):
                    raise result
                if isinstance(result, Exception):
                    logger.warning(
                        "Heartbeat batch failed",
                        batch_index=i,
                        error=str(result),
                    )

    async def _log_heartbeat_by_batch(
        self,
        session: aiohttp.ClientSession,
        task_instance_ids_to_heartbeat: List[int],
    ) -> None:
        """Send heartbeat for a batch of task instances."""
        message: Dict = {
            "next_report_increment": self._next_report_increment,
            "task_instance_ids": task_instance_ids_to_heartbeat,
        }
        app_route = "/task_instance/log_report_by/batch"

        await self.requester.send_request_async(
            session=session,
            app_route=app_route,
            message=message,
            request_type="post",
            tenacious=True,
        )

    def run_next_status_cycle(self, *statuses: str) -> None:
        """Drive statuses through refresh + process synchronously.

        Convenience method for tests and scripts. In production,
        _async_run handles this via the async event loop.
        """

        async def _cycle() -> None:
            for status in statuses:
                await self.refresh_status_from_db(status)
                if status in self._command_generator_map:
                    await self.process_status(status)

        asyncio.run(_cycle())

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

    async def refresh_status_from_db(self, status: str) -> None:
        """Check DB for status changes via async HTTP."""
        message = {
            "task_instance_ids": [
                task_instance.task_instance_id
                for task_instance in self._task_instance_status_map[status]
            ],
            "status": status,
        }
        app_route = f"/workflow_run/{self.workflow_run.workflow_run_id}/sync_status"
        _, result = await self.requester.async_request(
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
                    previous_status = task_instance.status
                    self._task_instance_status_map[previous_status].remove(
                        task_instance
                    )

                    try:
                        self._task_instance_status_map[new_status].add(task_instance)
                        task_instance.status = new_status

                    except KeyError:
                        del self._task_instances[task_instance_id]

    def _check_queued_for_work(
        self,
    ) -> Generator[DistributorCommand, None, None]:
        queued_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.QUEUED]
        )
        queued_task_instances.sort()
        chunk_size = 500
        while queued_task_instances:
            ti_list = queued_task_instances[:chunk_size]
            queued_task_instances = queued_task_instances[chunk_size:]
            yield DistributorCommand(self.instantiate_task_instances, ti_list)

    def _check_instantiated_for_work(
        self,
    ) -> Generator[DistributorCommand, None, None]:
        instantiated_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.INSTANTIATED]
        )
        task_instance_batches = set(
            [task_instance.batch for task_instance in instantiated_task_instances]
        )

        for batch in task_instance_batches:
            yield DistributorCommand(self.launch_task_instance_batch, batch)

    def _check_triaging_for_work(
        self,
    ) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in TRIAGING state.

        Each triage command uses async get_remote_exit_info_async,
        so multiple triage calls can yield to the event loop between
        each other and allow heartbeats to proceed.
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

    def _check_kill_self_for_work(
        self,
    ) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in KILL_SELF state, grouped by TaskInstanceBatch."""
        kill_self_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.KILL_SELF]
        )

        if kill_self_task_instances:
            logger.info(
                "Distributor processing KILL_SELF task instances",
                num_task_instances=len(kill_self_task_instances),
            )

            for ti in kill_self_task_instances:
                logger.info(
                    "Task instance marked for termination",
                    task_instance_id=ti.task_instance_id,
                    distributor_id=ti.distributor_id,
                )

        batch_map = defaultdict(list)
        for ti in kill_self_task_instances:
            batch_map[ti.batch].append(ti)

        for batch_obj, _ in batch_map.items():
            yield DistributorCommand(self.kill_self_batch, batch_obj)

    def _check_no_heartbeat_for_work(
        self,
    ) -> Generator[DistributorCommand, None, None]:
        """Handle TIs in NO_HEARTBEAT state."""
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
