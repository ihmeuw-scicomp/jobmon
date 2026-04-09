"""The Task Instance Object once it has been submitted to run on a worker node."""

import asyncio
import os
import signal
import socket
from time import time
from typing import Dict, List, Optional

import aiofiles
import aiofiles.os
import aiohttp
import structlog

from jobmon.core.cluster_protocol import ClusterWorkerNode
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exceptions import ReturnCodes, TransitionError
from jobmon.core.requester import Requester
from jobmon.core.serializers import SerializeTaskInstance
from jobmon.core.structlog_utils import bind_method_context

logger = structlog.get_logger(__name__)


class WorkerNodeTaskInstance:
    """The Task Instance object once it has been submitted to run on a worker node."""

    def __init__(
        self,
        cluster_interface: ClusterWorkerNode,
        task_instance_id: int,
        task_instance_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
        command_interrupt_timeout: Optional[int] = None,
        requester: Optional[Requester] = None,
    ) -> None:
        """A mechanism whereby a running task_instance can communicate back to the JSM.

         Logs its status, errors, usage details, etc.

        Args:
            cluster_interface: interface that gathers executor info in the execution_wrapper.
            task_instance_id: the id of the TaskInstance that is reporting back.
            task_instance_heartbeat_interval: how ofter to log a report by with the db
            heartbeat_report_by_buffer: multiplier for report by date in case we miss a few.
            command_interrupt_timeout: the amount of time to wait for the child process to
                terminate.
            requester: communicate with the flask services.
        """
        # identity attributes
        self._task_instance_id = task_instance_id

        # service API
        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

        # cluster API
        self.cluster_interface = cluster_interface

        # get distributor id from executor
        self._distributor_id = self.cluster_interface.distributor_id

        # config
        config = JobmonConfig()
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
        if command_interrupt_timeout is None:
            self._command_interrupt_timeout = config.get_int(
                "worker_node", "command_interrupt_timeout"
            )
        else:
            self._command_interrupt_timeout = command_interrupt_timeout

        # attrs set by log running
        self._status: Optional[str] = None
        self._command: Optional[str] = None
        self._command_add_env: Optional[Dict[str, str]] = None
        self._stdout: Optional[str] = None
        self._stderr: Optional[str] = None

        # set last heartbeat
        self.last_heartbeat_time = time()

    @property
    def task_instance_id(self) -> int:
        """Returns a task instance ID if it's been bound."""
        if self._task_instance_id is None:
            raise AttributeError("Cannot access task_instance_id because it is None.")
        return self._task_instance_id

    @property
    def distributor_id(self) -> Optional[str]:
        """Executor id given from the executor it is being run on."""
        return self._distributor_id

    @property
    def nodename(self) -> Optional[str]:
        """Node it is being run on."""
        if not hasattr(self, "_nodename"):
            self._nodename = socket.getfqdn()
        return self._nodename

    @property
    def process_group_id(self) -> Optional[int]:
        """Process group to track parent and child processes."""
        if not hasattr(self, "_process_group_id"):
            self._process_group_id = os.getpid()
        return self._process_group_id

    @property
    def status(self) -> str:
        """Returns the last known status of the task instance."""
        if self._status is None:
            raise AttributeError(
                "Cannot access status until log_running() has been called."
            )
        return self._status

    @property
    def stdout(self) -> str:
        if self._stdout is None:
            raise AttributeError(
                "Cannot access stdout until log_running() has been called."
            )
        return self._stdout

    @property
    def stderr(self) -> str:
        if self._stderr is None:
            raise AttributeError(
                "Cannot access stderr until log_running() has been called."
            )
        return self._stderr

    @property
    def command(self) -> str:
        """Returns the command this task instance will run."""
        if self._command is None:
            raise AttributeError(
                "Cannot access command until log_running() has been called."
            )
        return self._command

    @property
    def command_add_env(self) -> Dict[str, str]:
        """Returns the command this task instance will run."""
        if self._command_add_env is None:
            raise AttributeError(
                "Cannot access command_add_env until log_running() has been called."
            )
        return self._command_add_env

    @property
    def command_returncode(self) -> int:
        """Returns the exit code of the command that was run."""
        if not hasattr(self, "_proc_returncode"):
            raise AttributeError(
                "Cannot access command_returncode until run() has been called"
            )
        return self._proc_returncode

    @property
    def command_stdout(self) -> str:
        """Returns the last 10k characters of the commands stdout."""
        if not hasattr(self, "_proc_stdout"):
            raise AttributeError(
                "Cannot access command_stdout until run() has been called"
            )
        return self._proc_stdout

    @property
    def command_stderr(self) -> str:
        """Returns the last 10k characters of the commands stderr."""
        if not hasattr(self, "_proc_stderr"):
            raise AttributeError(
                "Cannot access command_stderr until run() has been called"
            )
        return self._proc_stderr

    @bind_method_context(task_instance_id="_task_instance_id")
    def log_done(self) -> None:
        """Tell the JobStateManager that this task_instance is done."""
        logger.info(
            f"Task instance {self.task_instance_id} marked as DONE",
            task_instance_id=self.task_instance_id,
            return_code=self.command_returncode,
        )

        message = {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "stdout_log": self.command_stdout,
            "stderr_log": self.command_stderr,
            "nodename": self.nodename,
            "distributor_id": self.distributor_id,
        }

        # Collect resource usage from cluster plugin
        try:
            usage_stats = self.cluster_interface.get_usage_stats()
        except Exception:
            usage_stats = {}

        if usage_stats:
            message.update(usage_stats)

        if hasattr(self, "_run_start_time"):
            message["wallclock"] = str(round(time() - self._run_start_time, 2))

        app_route = f"/task_instance/{self.task_instance_id}/log_done"
        _, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        self._status = response["status"]
        if self.status != TaskInstanceStatus.DONE:
            logger.error(
                "Task instance failed to transition to DONE",
                expected_status=TaskInstanceStatus.DONE,
                actual_status=self.status,
            )
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.DONE} status. Current status is {self.status}."
            )

        logger.info(
            "Task instance successfully transitioned to DONE",
            status=self.status,
        )

    @bind_method_context(task_instance_id="_task_instance_id")
    def log_error(self, error_state: str, description: str) -> None:
        """Tell the JobStateManager that this task_instance has errored."""
        logger.info(
            "Worker node logging task instance error",
            error_state=error_state,
            error_description=description[:200],  # Truncate for readability
            nodename=self.nodename,
            distributor_id=self.distributor_id,
        )

        message = {
            "error_state": error_state,
            "error_description": description,
            "stdout_log": self.command_stdout,
            "stderr_log": self.command_stderr,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "nodename": self.nodename,
            "distributor_id": self.distributor_id,
        }

        # Collect resource usage so error instances appear in
        # the usage scatter plot alongside successful ones.
        try:
            usage_stats = self.cluster_interface.get_usage_stats()
        except Exception:
            usage_stats = {}

        if usage_stats:
            message.update(usage_stats)

        if hasattr(self, "_run_start_time"):
            message["wallclock"] = str(round(time() - self._run_start_time, 2))

        app_route = f"/task_instance/{self.task_instance_id}/log_error_worker_node"
        _, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        self._status = response["status"]
        if self.status != error_state:
            logger.error(
                "Task instance failed to transition to error state",
                expected_status=error_state,
                actual_status=self.status,
            )
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {error_state} status. Current status is {self.status}."
            )

        logger.info(
            "Task instance successfully transitioned to error state",
            error_state=self.status,
        )

    @bind_method_context(
        task_instance_id="_task_instance_id",
        nodename="nodename",
        distributor_id="distributor_id",
    )
    def log_running(self) -> None:
        """Tell the JobStateManager that this task_instance is running.

        Update the report_by_date to be further in the future in case it gets reconciled
        immediately.
        """
        logger.info(
            f"Task instance {self.task_instance_id} started running",
            task_instance_id=self.task_instance_id,
        )
        message = {
            "nodename": self.nodename,
            "process_group_id": str(self.process_group_id),
            "next_report_increment": (
                self._task_instance_heartbeat_interval
                * self._heartbeat_report_by_buffer
            ),
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.warning("No distributor_id in worker environment")

        app_route = f"/task_instance/{self.task_instance_id}/log_running"
        _, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )

        kwargs = SerializeTaskInstance.kwargs_from_wire_worker_node(
            response["task_instance"]
        )
        self._status = kwargs.pop("status")
        self._command = kwargs.pop("command")
        task_name = kwargs.pop("name")
        self._stdout = self.cluster_interface.initialize_logfile(
            "stdout", kwargs.pop("stdout_dir"), task_name
        )
        self._stderr = self.cluster_interface.initialize_logfile(
            "stderr", kwargs.pop("stderr_dir"), task_name
        )
        self._command_add_env = {
            f"JOBMON_{k.upper()}": str(v) for k, v in kwargs.items()
        }
        self.last_heartbeat_time = time()
        if self.status != TaskInstanceStatus.RUNNING:
            logger.error(
                "Task instance failed to transition to RUNNING",
                expected_status=TaskInstanceStatus.RUNNING,
                actual_status=self.status,
            )
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.RUNNING} status. Current status is {self.status}."
            )

        logger.info(
            "Task instance successfully transitioned to RUNNING",
            status=self.status,
        )

    @bind_method_context(task_instance_id="_task_instance_id")
    async def log_report_by(self, session: aiohttp.ClientSession) -> None:
        """Log the heartbeat to show that the task instance is still alive.

        Uses the async requester with an aiohttp ClientSession so heartbeats
        can run concurrently with other asyncio work (subprocess pipe drain,
        lazy log-directory creation in ``_communicate``). Running on the event
        loop instead of the default thread-pool avoids the executor pressure
        that ``asyncio.to_thread`` would add when multiple slow I/O paths are
        in flight at the same time.
        """
        logger.info("Worker node sending heartbeat")
        message: Dict = {
            "next_report_increment": (
                self._task_instance_heartbeat_interval
                * self._heartbeat_report_by_buffer
            ),
            "stdout": self.stdout,
            "stderr": self.stderr,
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No distributor_id was found in the sbatch env at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_report_by"
        _, response = await self.requester.send_request_async(
            session=session,
            app_route=app_route,
            message=message,
            request_type="post",
        )

        self._status = response["status"]
        self.last_heartbeat_time = time()

        if self.status != TaskInstanceStatus.RUNNING:
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.RUNNING} status. Current status is {self.status}."
            )

    def run(self) -> None:
        """This script executes on the target node and wraps the target application.

        Could be in any language, anything that can execute on linux. Similar to a stub or a
        container set ENV variables in case tasks need to access them.

        """
        # If it logs running and is not able to transition it raises TransitionError
        self.log_running()
        self._run_start_time = time()

        try:
            # run the command in a subprocess
            asyncio.run(self._run_cmd())

        # some other deployment unit transitioned task instance out of R state
        except RuntimeError as e:
            if isinstance(e.__cause__, TransitionError):
                msg = (
                    f"TaskInstance is in status '{self.status}'. Expected status 'R'."
                    f" Terminating command {self.command}."
                )
                logger.error("Task in unexpected state", current_status=self.status)

                # log an error with db if we are in K state
                if self.status == TaskInstanceStatus.KILL_SELF:
                    msg = (
                        f"Command: '{self.command}' got KILL_SELF event. Process shut down"
                        f" with exit code: '{self.command_returncode}'"
                    )
                    logger.error("Task killed", exit_code=self.command_returncode)
                    self.log_error(TaskInstanceStatus.ERROR_FATAL, msg)

                # otherwise raise the error cause we are in trouble
                else:
                    raise e.__cause__
            else:
                raise

        # normal happy path
        else:
            if self.command_returncode == ReturnCodes.OK:
                logger.info(
                    f"Task instance {self.task_instance_id} completed successfully",
                    task_instance_id=self.task_instance_id,
                    return_code=self.command_returncode,
                )
                self.log_done()
            else:
                logger.info(
                    "Command exited with non-zero code",
                    return_code=self.command_returncode,
                )
                error_state, msg = self.cluster_interface.get_exit_info(
                    self.command_returncode, self.command_stderr
                )
                self.log_error(error_state, msg)

    def set_command_output(self, returncode: int, stdout: str, stderr: str) -> None:
        self._proc_returncode = returncode
        self._proc_stdout = stdout
        self._proc_stderr = stderr

    @staticmethod
    async def _communicate(
        async_stream: asyncio.StreamReader,
        output_path: str,
        chunk_size: int = 64,
    ) -> str:
        mem_buffer = ""
        try:
            async with aiofiles.open(output_path, "w") as output_stream:
                while True:
                    data_chunk = await async_stream.read(chunk_size)
                    if not data_chunk:
                        break  # EOF reached

                    try:
                        data_chunk_str = data_chunk.decode()
                        await output_stream.write(data_chunk_str)
                        await output_stream.flush()
                        mem_buffer += data_chunk_str
                        mem_buffer = mem_buffer[-10000:]
                    except UnicodeDecodeError:
                        pass
        except Exception as e:
            logger.exception("Stream reading error", error=str(e))
            mem_buffer += "\n[Error reading stream: {}]".format(e)
        finally:
            return mem_buffer

    async def _heartbeat_loop(self, session: aiohttp.ClientSession) -> None:
        """Periodically send heartbeats for the entire duration of ``_run_cmd``.

        Runs concurrently with pre-subprocess log-directory creation, the
        subprocess itself, and the stream-drain phase. The caller in
        ``_run_cmd`` races this task against the subprocess work tasks via
        ``asyncio.wait(FIRST_COMPLETED)``, so a ``TransitionError`` raised
        from ``log_report_by`` (e.g. the server moved the TI out of R)
        triggers immediate subprocess cleanup instead of being discovered
        only when the subprocess finishes on its own.

        Transient network/server errors are logged and swallowed so they
        don't abort the whole task — ``log_report_by`` only raises after
        the sync requester's tenacity retry budget is exhausted, and even
        then we want to keep ticking in case the next attempt recovers.
        """
        while True:
            await asyncio.sleep(self._task_instance_heartbeat_interval)
            try:
                await self.log_report_by(session)
            except TransitionError:
                raise
            except Exception as e:
                logger.warning(
                    "Worker node heartbeat failed, retrying next tick",
                    error=str(e),
                )

    async def _ensure_logfile_parent_dir(self, output_path: str) -> None:
        """Create the parent directory of a logfile path if needed.

        Uses ``aiofiles.os.makedirs`` so the underlying syscall runs on the
        event loop's default executor while the coroutine awaits the
        future. The event loop stays responsive throughout, so a concurrent
        ``_heartbeat_loop`` task keeps firing heartbeats even if the
        filesystem stalls for minutes.
        """
        if not output_path or output_path == "/dev/null":
            return
        parent = os.path.dirname(output_path)
        if not parent:
            return
        await aiofiles.os.makedirs(parent, exist_ok=True)

    async def _run_cmd(self) -> None:
        """Run the user's command under a concurrent heartbeat task.

        The heartbeat loop runs for the entire lifetime of this method —
        from pre-subprocess log-dir creation through subprocess exit and
        stream drain — so a slow filesystem or long subprocess can't
        starve the TI's ``report_by_date`` on the server.
        """
        async with aiohttp.ClientSession() as session:
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(session))
            try:
                await self._run_subprocess_with_heartbeat_race(heartbeat_task)
            finally:
                await self._stop_heartbeat_task(heartbeat_task)

    async def _run_subprocess_with_heartbeat_race(
        self, heartbeat_task: "asyncio.Task[None]"
    ) -> None:
        """Pre-create log dirs, spawn the subprocess, and race it.

        All failures — pre-create ``OSError``, heartbeat
        ``TransitionError``, stream drain errors, subprocess spawn
        errors — are caught and re-raised wrapped in ``RuntimeError`` via
        ``raise ... from e``. The outer ``run()`` method's
        ``except RuntimeError`` block depends on
        ``isinstance(e.__cause__, TransitionError)`` to route KILL_SELF
        handling, so the wrap must happen here regardless of whether the
        subprocess ever actually started.
        """
        process: Optional[asyncio.subprocess.Process] = None
        stdout_task: Optional["asyncio.Task[str]"] = None
        stderr_task: Optional["asyncio.Task[str]"] = None
        process_wait_task: Optional["asyncio.Task[int]"] = None
        try:
            env = os.environ.copy()
            env.update(self.command_add_env)

            # Pre-create log parent directories. If the shared filesystem
            # stalls here, ``heartbeat_task`` keeps the TI alive on the
            # server. Because we wait to spawn the subprocess until the
            # directories exist, the subprocess's stdout/stderr pipes can
            # never fill while we're blocked on a metadata op.
            await self._ensure_logfile_parent_dir(self.stdout)
            await self._ensure_logfile_parent_dir(self.stderr)

            # Surface an early heartbeat failure (e.g. KILL_SELF) before
            # we spawn a subprocess we'll immediately have to kill.
            if heartbeat_task.done():
                heartbeat_task.result()

            process = await asyncio.create_subprocess_shell(
                self.command,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            assert process.stdout is not None
            assert process.stderr is not None

            stdout_task = asyncio.create_task(
                self._communicate(process.stdout, self.stdout)
            )
            stderr_task = asyncio.create_task(
                self._communicate(process.stderr, self.stderr)
            )
            process_wait_task = asyncio.create_task(process.wait())

            await self._race_work_tasks(
                heartbeat_task, [stdout_task, stderr_task, process_wait_task]
            )

        except Exception as e:
            await self._abort_subprocess(
                process, [stdout_task, stderr_task, process_wait_task]
            )
            logger.exception(
                "Task instance command execution failed",
                error=str(e),
                command=self.command,
            )
            raise RuntimeError(
                f"Failed to execute command '{self.command}': {e}"
            ) from e

        finally:
            await self._record_final_command_output(process, stdout_task, stderr_task)

    @staticmethod
    async def _race_work_tasks(
        heartbeat_task: "asyncio.Task[None]",
        work_tasks: List["asyncio.Task"],
    ) -> None:
        """Wait for work tasks while watching the heartbeat.

        Uses ``asyncio.wait(FIRST_COMPLETED)`` so a heartbeat failure wakes
        us immediately — we want to cancel the subprocess the moment the
        server says the TI is no longer ours, not after it runs to
        completion. Any task exception (heartbeat ``TransitionError``,
        stream drain errors) surfaces via ``task.result()``; on the normal
        happy path no task has a stored exception and this loop is a
        no-op. After the first completion, the remaining work tasks are
        awaited — typically ``process_wait_task`` finishes first and we
        wait for the stdout/stderr drains to hit EOF.
        """
        done, _pending = await asyncio.wait(
            [heartbeat_task, *work_tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                task.result()
        still_pending = [t for t in work_tasks if not t.done()]
        if still_pending:
            await asyncio.gather(*still_pending)

    async def _abort_subprocess(
        self,
        process: Optional[asyncio.subprocess.Process],
        work_tasks: List[Optional["asyncio.Task"]],
    ) -> None:
        """Cancel work tasks and gracefully terminate the subprocess.

        Safe to call even if the subprocess never started. The escalation
        is SIGINT → wait up to ``_command_interrupt_timeout`` → SIGKILL.
        """
        tasks_to_cancel = [t for t in work_tasks if t is not None and not t.done()]
        for task in tasks_to_cancel:
            task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        if process is not None and process.returncode is None:
            process.send_signal(signal.SIGINT)
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=self._command_interrupt_timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

    async def _record_final_command_output(
        self,
        process: Optional[asyncio.subprocess.Process],
        stdout_task: Optional["asyncio.Task[str]"],
        stderr_task: Optional["asyncio.Task[str]"],
    ) -> None:
        """Collect the final command output and store it on ``self``.

        Gathers the stdout/stderr tails from the drain tasks, awaits the
        subprocess return code, and calls ``set_command_output``. If the
        subprocess never spawned (e.g. a ``TransitionError`` during
        pre-create), falls back to a sentinel return code so the outer
        ``run()`` method's KILL_SELF handler can still access
        ``command_returncode`` without ``AttributeError``.
        """
        stdout_result = self._get_drain_result(stdout_task)
        stderr_result = self._get_drain_result(stderr_task)

        if process is not None:
            returncode = await process.wait()
        else:
            returncode = -1

        self.set_command_output(
            returncode=returncode,
            stdout=stdout_result,
            stderr=stderr_result,
        )

    @staticmethod
    def _get_drain_result(task: Optional["asyncio.Task[str]"]) -> str:
        """Return a stream drain task's accumulated output, or empty.

        Returns an empty string if the task is None, still running,
        cancelled, or raised an exception.
        """
        if task is None or not task.done() or task.cancelled():
            return ""
        try:
            return task.result()
        except Exception:
            return ""

    @staticmethod
    async def _stop_heartbeat_task(
        heartbeat_task: "asyncio.Task[None]",
    ) -> None:
        """Cancel and consume the heartbeat task.

        Called from an outer ``finally`` — swallows the task's final
        exception (``TransitionError``, ``CancelledError``, etc.) so it
        can't mask the real in-flight error. The relevant exception has
        already been surfaced by ``_race_work_tasks`` or the early
        ``heartbeat_task.result()`` check in
        ``_run_subprocess_with_heartbeat_race``.
        """
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        try:
            await heartbeat_task
        except BaseException:
            pass
