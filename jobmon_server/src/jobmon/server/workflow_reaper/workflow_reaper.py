"""Service to monitor and reap dead workflows."""
import logging
from time import sleep
from typing import Callable, List, Optional, Tuple

from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core.exceptions import ConfigError, InvalidResponse
from jobmon.core.requester import http_request_ok, Requester
from jobmon.server import __version__
from jobmon.server.workflow_reaper.notifiers import SlackNotifier
from jobmon.server.workflow_reaper.reaper_workflow_run import ReaperWorkflowRun

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    """Monitoring and reaping dead workflows."""

    _version = __version__

    # starting point of F-D inconsistency query
    _current_starting_row = 0

    _reaper_message = {
        WorkflowRunStatus.ERROR: (
            "{__version__} Workflow Reaper transitioned a Workflow to FAILED state and "
            "associated Workflow Run to ERROR state.\n"
            "Workflow ID: {workflow_id}\n"
            "Workflow Name: {workflow_name}\n"
            "Workflow Args: {workflow_args}\n"
            "WorkflowRun ID: {workflow_run_id}"
        ),
        WorkflowRunStatus.TERMINATED: (
            "{__version__} Workflow Reaper transitioned a Workflow to HALTED state and "
            "associated Workflow Run to TERMINATED state.\n"
            "Workflow ID: {workflow_id}\n"
            "Workflow Name: {workflow_name}\n"
            "Workflow Args: {workflow_args}\n"
            "WorkflowRun ID: {workflow_run_id}"
        ),
        WorkflowRunStatus.ABORTED: (
            "{__version__} Workflow Reaper transitioned a Workflow to ABORTED state and "
            "associated Workflow Run to ABORTED state.\n"
            "Workflow ID: {workflow_id}\n"
            "Workflow Name: {workflow_name}\n"
            "Workflow Args: {workflow_args}\n"
            "WorkflowRun ID: {workflow_run_id}"
        ),
    }

    def __init__(
        self,
        poll_interval_seconds: Optional[int] = None,
        requester: Optional[Requester] = None,
        wf_notification_sink: Optional[Callable[..., None]] = None,
    ) -> None:
        """Initializes WorkflowReaper class with specified poll interval and slack info.

        Args:
            poll_interval_seconds(int): how often the WorkflowReaper should check the
                database and reap workflows. Using seconds, rather than minutes, makes
                the tests run faster.
            requester (Requester): requester to communicate with Flask.
            wf_notification_sink (Callable): Slack notifier send().
        """
        config = JobmonConfig()

        # get poll interval from config
        if poll_interval_seconds is None:
            poll_interval_seconds = (
                config.get_int("reaper", "poll_interval_minutes") * 60
            )
        if requester is None:
            requester = Requester.from_defaults()
        if wf_notification_sink is None:
            try:
                wf_notifier = SlackNotifier()
                wf_notification_sink = wf_notifier.send
            except ConfigError:
                pass

        logger.info(
            f"WorkflowReaper initializing with: poll_interval_minutes={poll_interval_seconds},"
            f"requester_url={requester.url}"
        )

        self._poll_interval_seconds = poll_interval_seconds
        self._requester = requester
        self._wf_notification_sink = wf_notification_sink

    def monitor_forever(self) -> None:
        """The main part of the Worklow Reaper.

        Check if workflow runs should be in ABORTED, SUSPENDED, or ERROR state. Wait and do
        it again.
        """
        logger.info("Monitoring forever...")

        if self._wf_notification_sink is not None:
            self._wf_notification_sink(msg=f"Workflow Reaper v{__version__} is alive")
        try:
            while True:
                self._halted_state()
                self._aborted_state()
                self._error_state()
                # The chunk size for the _inconsistent_status query is small so that each
                # query takes 100-400 mS. Therefore run several, a few seconds apart. We want
                # to be able to clean the whole database every 12 hours, but also not lock
                # the database.
                for i in range(5):
                    self._inconsistent_status(100)
                    sleep(2)
                sleep(self._poll_interval_seconds)
        except RuntimeError as e:
            logger.debug(f"Error in monitor_forever() in workflow reaper: {e}")

    def _get_wf_name_args(self, workflow_id: int) -> Tuple[str, str]:
        """Return the workflow name and args associated with a specific workflow_id."""
        logger.info(
            f"Checking the DB for workflow name and args of WF_ID: {workflow_id}"
        )
        app_route = f"/workflow/{workflow_id}/workflow_name_and_args"
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={"workflow_id": workflow_id},
            request_type="get",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {result}"
            )
        return result["workflow_name"], result["workflow_args"]

    def _get_lost_workflow_runs(self, status: List[str]) -> List[ReaperWorkflowRun]:
        """Return all workflows that are in a specific state."""
        logger.info(f"Checking the database for workflow runs of status: {status}")
        app_route = "/lost_workflow_run"
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={"status": status, "version": self._version},
            request_type="get",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {result}"
            )
        workflow_runs = []
        for wfr in result["workflow_runs"]:
            workflow_runs.append(ReaperWorkflowRun.from_wire(wfr, self._requester))

        if workflow_runs:
            logger.info(f"Found workflow runs: {workflow_runs}")
        return workflow_runs

    def _halted_state(self) -> Optional[str]:
        """Check if a workflow_run needs to be transitioned to terminated state."""
        # Get workflow_runs in H and C state
        workflow_runs = self._get_lost_workflow_runs(["C", "H"])

        # Transition workflows to HALTED
        target_status = WorkflowRunStatus.TERMINATED
        messages = ""
        for wfr in workflow_runs:
            status = wfr.reap()
            if status == target_status and self._wf_notification_sink is not None:
                wf_name, wf_args = self._get_wf_name_args(wfr.workflow_id)
                message = self._reaper_message[status].format(
                    __version__=self._version,
                    workflow_id=wfr.workflow_id,
                    workflow_run_id=wfr.workflow_run_id,
                    workflow_name=wf_name,
                    workflow_args=wf_args,
                )
                self._wf_notification_sink(msg=message)
                messages += message
        return messages

    def _error_state(self) -> Optional[str]:
        """Get lost workflows and register them as error."""
        workflow_runs = self._get_lost_workflow_runs(["R"])

        # Transitions workflow to FAILED state and workflow run to ERROR
        target_status = WorkflowRunStatus.ERROR
        messages = ""
        for wfr in workflow_runs:
            status = wfr.reap()
            if status == target_status and self._wf_notification_sink is not None:
                wf_name, wf_args = self._get_wf_name_args(wfr.workflow_id)
                message = self._reaper_message[status].format(
                    __version__=self._version,
                    workflow_id=wfr.workflow_id,
                    workflow_run_id=wfr.workflow_run_id,
                    workflow_name=wf_name,
                    workflow_args=wf_args,
                )
                self._wf_notification_sink(msg=message)
                messages += message
        return messages

    def _aborted_state(self) -> Optional[str]:
        """Find workflows that should be in aborted state.

        Get all workflow runs in G state and validate if they should be in A state. Get all
        lost wfr in L state and set it to A
        """
        # Get all lost wfr in L
        workflow_runs = self._get_lost_workflow_runs(["L"])

        # Transitions workflow to A state and workflow run to A
        target_status = WorkflowRunStatus.ABORTED
        messages = ""
        for wfr in workflow_runs:
            status = wfr.reap()
            if status == target_status and self._wf_notification_sink is not None:
                wf_name, wf_args = self._get_wf_name_args(wfr.workflow_id)
                message = self._reaper_message[status].format(
                    __version__=self._version,
                    workflow_id=wfr.workflow_id,
                    workflow_run_id=wfr.workflow_run_id,
                    workflow_name=wf_name,
                    workflow_args=wf_args,
                )
                self._wf_notification_sink(msg=message)
                messages += message
        return messages

    def _inconsistent_status(self, step_size: int) -> None:
        """Find wf in F with all tasks in D and fix them."""
        logger.debug("Find wf in state F but all tasks in D and fix them.")

        app_route = (
            f"/workflow/{WorkflowReaper._current_starting_row}/fix_status_inconsistency"
        )
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={"increase_step": step_size},
            request_type="put",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {result}"
            )
        WorkflowReaper._current_starting_row = int(result["wfid"])
