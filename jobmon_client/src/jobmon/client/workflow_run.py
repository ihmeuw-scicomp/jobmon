"""The workflow run is an instance of a workflow."""

from __future__ import annotations

import getpass
import time
from typing import Optional

import structlog

from jobmon.client import __version__
from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import WorkflowNotResumable
from jobmon.core.requester import Requester

logger = structlog.get_logger(__name__)


class WorkflowRunFactory:
    """A utility class responsible for instantiating workflow run objects.

    This class sends the appropriate resume signals so that the parent workflow
    object is in a state where the newly created workflowrun is ready to run, either on
    resume or not.
    """

    # TODO: workflow run factory is for now mostly a placeholder for various functions
    # useful for instantiating workflow runs either from a workflow or from the CLI.
    # Might want to consider unifying the resume API more, think about how to handle
    # task resource caching and creation as well in order to resume.

    def __init__(self, workflow_id: int, requester: Optional[Requester] = None) -> None:
        """Initialization of client WorkflowRun."""
        self.workflow_id = workflow_id
        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester
        self.workflow_is_resumable = False

    def set_workflow_resume(
        self,
        reset_running_jobs: bool = True,
        resume_timeout: int = 300,
        force_cleanup: bool = False,
    ) -> None:
        """Set statuses of the given workflow ID's workflow to a resumable state.

        Move active workflow runs to hot/cold resume states, depending on reset_running_jobs.

        Args:
            reset_running_jobs: If True (cold resume), also terminate running tasks.
                               If False (hot resume), let running tasks finish.
            resume_timeout: Maximum time to wait for workflow to become resumable.
            force_cleanup: If True and timeout is reached with pending KILL_SELF
                          task instances, force cleanup and proceed. Use this when
                          jobs have been externally terminated (scancel, node failure).
        """
        if self.workflow_is_resumable:
            return
        app_route = f"/workflow/{self.workflow_id}/set_resume"
        self.requester.send_request(
            app_route=app_route,
            message={"reset_running_jobs": reset_running_jobs},
            request_type="post",
        )
        # Wait for the workflow to become resumable
        self.wait_for_workflow_resume(resume_timeout, force_cleanup=force_cleanup)

    def wait_for_workflow_resume(
        self, resume_timeout: int = 300, force_cleanup: bool = False
    ) -> None:
        """Wait for workflow to become resumable.

        Args:
            resume_timeout: Maximum time to wait in seconds
            force_cleanup: If True and timeout is reached with pending KILL_SELF
                          task instances, force cleanup and proceed
        """
        wait_start = time.time()
        pending_kill_self = 0
        warned_about_kill_self = False

        while not self.workflow_is_resumable:
            elapsed = time.time() - wait_start
            remaining = resume_timeout - elapsed

            app_route = f"/workflow/{self.workflow_id}/is_resumable"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get"
            )

            self.workflow_is_resumable = bool(response.get("workflow_is_resumable"))
            pending_kill_self = response.get("pending_kill_self", 0)

            # Warn user about pending KILL_SELF task instances
            if pending_kill_self > 0 and not warned_about_kill_self:
                logger.warning(
                    f"Waiting for {pending_kill_self} task instance(s) in KILL_SELF "
                    f"state to be cleaned up by workers. If these jobs were externally "
                    f"terminated (scancel, node failure), use force_cleanup=True or "
                    f"call force_cleanup_kill_self() to proceed."
                )
                warned_about_kill_self = True

            if pending_kill_self > 0:
                logger.info(
                    f"Waiting for resume. {pending_kill_self} KILL_SELF task(s) pending. "
                    f"Timeout in {round(remaining, 1)}s"
                )
            else:
                logger.info(f"Waiting for resume. Timeout in {round(remaining, 1)}s")

            if elapsed > resume_timeout:
                if force_cleanup and pending_kill_self > 0:
                    logger.warning(
                        f"Timeout reached with {pending_kill_self} KILL_SELF task(s). "
                        f"Force cleanup enabled - cleaning up stuck task instances."
                    )
                    self.force_cleanup_kill_self()
                    # Re-check resumability after cleanup
                    continue
                else:
                    msg = (
                        "workflow_run timed out waiting for previous "
                        "workflow_run to exit."
                    )
                    if pending_kill_self > 0:
                        msg += (
                            f" {pending_kill_self} task instance(s) stuck in KILL_SELF "
                            f"state. Jobs may have been externally terminated. "
                            f"Use force_cleanup=True to force cleanup."
                        )
                    raise WorkflowNotResumable(msg)
            else:
                sleep_time = round(float(resume_timeout) / 10.0, 1)
                time.sleep(sleep_time)

    def force_cleanup_kill_self(self) -> int:
        """Force cleanup of stuck KILL_SELF task instances.

        Use this when jobs have been externally terminated (e.g., scancel, node failure)
        and the workflow is stuck waiting for cleanup that will never happen.

        Returns:
            Number of task instances cleaned up
        """
        logger.info(
            f"Force cleanup of KILL_SELF task instances for workflow {self.workflow_id}"
        )
        app_route = f"/workflow/{self.workflow_id}/force_cleanup_kill_self"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="post"
        )
        num_cleaned = response.get("num_cleaned_up", 0)
        logger.info(f"Cleaned up {num_cleaned} KILL_SELF task instance(s)")
        return num_cleaned

    def reset_task_statuses(
        self, reset_if_running: bool = True, force_cleanup: bool = False
    ) -> None:
        """Sets the tasks associated with a workflow to the appropriate states.

        Args:
            reset_if_running: If True, also reset running tasks.
            force_cleanup: If True and workflow not resumable due to stuck KILL_SELF
                          task instances, force cleanup and proceed.
        """
        self.wait_for_workflow_resume(force_cleanup=force_cleanup)

        self.requester.send_request(
            app_route=f"/task/{self.workflow_id}/set_resume_state",
            message={"reset_if_running": reset_if_running},
            request_type="post",
        )

    def create_workflow_run(
        self,
        workflow_run_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
    ) -> WorkflowRun:
        """Workflow should at least have signalled for a resume at this point."""
        # create workflow run
        client_wfr = WorkflowRun(
            workflow_id=self.workflow_id,
            requester=self.requester,
            workflow_run_heartbeat_interval=workflow_run_heartbeat_interval,
            heartbeat_report_by_buffer=heartbeat_report_by_buffer,
        )
        client_wfr.bind()

        return client_wfr


class WorkflowRun(object):
    """WorkflowRun enables tracking for multiple runs of a single Workflow.

    A Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(
        self,
        workflow_id: int,
        requester: Optional[Requester] = None,
        workflow_run_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
    ) -> None:
        """Initialize client WorkflowRun."""
        # set attrs
        self.workflow_id = workflow_id
        self.user = getpass.getuser()

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

        # set values from config
        config = JobmonConfig()
        if workflow_run_heartbeat_interval is None:
            heartbeat_interval = config.get_int("heartbeat", "workflow_run_interval")
        else:
            heartbeat_interval = int(workflow_run_heartbeat_interval)
        self.heartbeat_interval = heartbeat_interval
        if heartbeat_report_by_buffer is None:
            report_by_buffer = config.get_float("heartbeat", "report_by_buffer")
        else:
            report_by_buffer = float(heartbeat_report_by_buffer)
        self.heartbeat_report_by_buffer = report_by_buffer

        self._workflow_run_id = None
        self._status: Optional[str] = None

    @property
    def workflow_run_id(self) -> int:
        if not self._workflow_run_id:
            raise WorkflowNotResumable(
                "This workflow run was not yet bound successfully, "
                "cannot access workflow run id attribute."
            )
        return self._workflow_run_id

    @property
    def status(self) -> Optional[str]:
        if not self._status:
            raise WorkflowNotResumable(
                "This workflow run was not bound successfully, "
                "cannot access status attribute."
            )
        return self._status

    def bind(self) -> None:
        """Link this workflow run with the workflow and add all tasks."""
        if self._workflow_run_id:
            return  # WorkflowRun already bound

        next_report_increment = (
            self.heartbeat_interval * self.heartbeat_report_by_buffer
        )

        # bind to database
        app_route = "/workflow_run"
        _, resp = self.requester.send_request(
            app_route=app_route,
            message={
                "workflow_id": self.workflow_id,
                "user": self.user,
                "jobmon_version": __version__,
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        workflow_run_id = resp.get("workflow_run_id")
        if not workflow_run_id:
            raise WorkflowNotResumable(resp.get("err_msg"))
        self._workflow_run_id = workflow_run_id
        self._status = resp.get("status")

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        self.requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
        )
        self._status = status

    def __repr__(self) -> str:
        """A representation string for a client WorkflowRun instance."""
        return (
            f"WorkflowRun(workflow_id={self.workflow_id}, "
            f"workflow_run_id={self.workflow_run_id}"
        )
