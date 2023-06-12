"""Workflow run database table."""
import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import structlog

from jobmon.core.exceptions import InvalidStateTransition
from jobmon.core.serializers import SerializeWorkflowRun
from jobmon.server import __version__
from jobmon.server.web._compat import add_time
from jobmon.server.web.models import Base
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.models.workflow_status import WorkflowStatus


logger = structlog.get_logger(__name__)


class WorkflowRun(Base):
    """Database table for recording Workflow Runs."""

    __tablename__ = "workflow_run"

    def to_wire_as_reaper_workflow_run(self) -> tuple:
        """Serialize workflow run."""
        serialized = SerializeWorkflowRun.to_wire(
            id=self.id, workflow_id=self.workflow_id
        )
        return serialized

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey("workflow.id"))
    user = Column(String(150))
    jobmon_version = Column(String(150), default="UNKNOWN")
    jobmon_server_version = Column(String(150), default=__version__)
    status = Column(
        String(1),
        ForeignKey("workflow_run_status.id"),
        default=WorkflowRunStatus.REGISTERED,
    )

    created_date = Column(DateTime, default=func.now())
    status_date = Column(DateTime, default=func.now())
    heartbeat_date = Column(DateTime, default=func.now())

    workflow = relationship("Workflow", back_populates="workflow_runs", lazy=True)

    __table_args__ = (
        Index("ix_status_version", "status", "jobmon_version", "jobmon_server_version"),
    )

    valid_transitions = [
        # a workflow run is created normally. claimed control of workflow
        (WorkflowRunStatus.REGISTERED, WorkflowRunStatus.LINKING),
        # a workflow run is created normally. All tasks are updated in the db
        # and the workflow run can move to bound state
        (WorkflowRunStatus.LINKING, WorkflowRunStatus.BOUND),
        # a workflow run is created normally. Something goes wrong while the
        # tasks are binding and the workflow run moves to error state
        (WorkflowRunStatus.LINKING, WorkflowRunStatus.ABORTED),
        # a workflow run is bound and then moves to instantiating
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.INSTANTIATED),
        # a workflow run moves from instantiating to launched
        (WorkflowRunStatus.INSTANTIATED, WorkflowRunStatus.LAUNCHED),
        # a workflow run moves from launched to running
        (WorkflowRunStatus.LAUNCHED, WorkflowRunStatus.RUNNING),
        # a workflow run can't be launched for some reason. TODO: implement triaging
        (WorkflowRunStatus.INSTANTIATED, WorkflowRunStatus.ERROR),
        (WorkflowRunStatus.LAUNCHED, WorkflowRunStatus.ERROR),
        # a workflow run is bound and then an error occurs before it starts
        # running
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.ERROR),
        # a workflow run is bound and then a new workflow run is created
        # before the old workflow run moves into running state
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.COLD_RESUME),
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.HOT_RESUME),
        # the workflow starts running normally and finishes successfully
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.DONE),
        # the workflow starts running normally and the user stops it via a
        # keyboard interrupt
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.STOPPED),
        # the workflow is running and then a new workflow run is created
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.COLD_RESUME),
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.HOT_RESUME),
        # the workflow is running and then it's tasks hit errors
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.ERROR),
        # the workflow is set to resume and then it successfully shuts down
        (WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.TERMINATED),
        (WorkflowRunStatus.HOT_RESUME, WorkflowRunStatus.TERMINATED),
    ]

    untimely_transitions = [
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.RUNNING),
        (WorkflowRunStatus.LINKING, WorkflowRunStatus.LINKING),
    ]

    bound_error_states = [WorkflowRunStatus.STOPPED, WorkflowRunStatus.ERROR]

    active_states = [
        WorkflowRunStatus.BOUND,
        WorkflowRunStatus.RUNNING,
        WorkflowRunStatus.COLD_RESUME,
        WorkflowRunStatus.HOT_RESUME,
    ]

    @property
    def is_alive(self) -> bool:
        """Workflow run is in a state that should be registering heartbeats."""
        return self.status in [
            WorkflowRunStatus.LINKING,
            WorkflowRunStatus.BOUND,
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.COLD_RESUME,
            WorkflowRunStatus.HOT_RESUME,
        ]

    @property
    def is_active(self) -> bool:
        """Statuses where Workflow Run is active (bound or running)."""
        return self.status in [WorkflowRunStatus.BOUND, WorkflowRunStatus.RUNNING]

    def terminable(self, current_time: datetime.datetime) -> bool:
        """Whether a workflowrun can be terminated.

        A workflowrun can be terminated if it is in Cold/Hot resume state and has missed
        the last reporting heartbeat.
        """
        return (
            (self.heartbeat_date <= current_time) and
            (self.status in (WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.HOT_RESUME))
        )

    def heartbeat(
        self,
        next_report_increment: float,
        transition_status: str = WorkflowRunStatus.RUNNING,
    ) -> None:
        """Register a heartbeat for the Workflow Run to show it is still alive."""
        self.transition(transition_status)
        self.heartbeat_date = add_time(next_report_increment)

    def reap(self) -> None:
        """Transition dead workflow runs to a terminal state."""
        structlog.contextvars.bind_contextvars(
            workflow_run_id=self.id, workflow_id=self.workflow_id
        )
        logger.info("Dead workflow_run will be reaped.")
        if self.status == WorkflowRunStatus.LINKING:
            logger.debug(f"Transitioning wfr {self.id} to ABORTED")
            self.transition(WorkflowRunStatus.ABORTED)
        if self.status in [WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.HOT_RESUME]:
            logger.debug(f"Transitioning wfr {self.id} to TERMINATED")
            self.transition(WorkflowRunStatus.TERMINATED)
        if self.status == WorkflowRunStatus.RUNNING:
            logger.debug(f"Transitioning wfr {self.id} to ERROR")
            self.transition(WorkflowRunStatus.ERROR)
        logger.info(f"Transitioned workflow to {self.status}")

    def transition(self, new_state: str) -> None:
        """Transition the Workflow Run's state."""
        structlog.contextvars.bind_contextvars(
            workflow_run_id=self.id, workflow_id=self.workflow_id
        )
        logger.info(f"Transitioning workflow_run from {self.status} to {new_state}")
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            self.status = new_state
            self.status_date = func.now()
            if new_state == WorkflowRunStatus.LINKING:
                self.workflow.transition(WorkflowStatus.REGISTERING)
            elif new_state == WorkflowRunStatus.BOUND:
                self.workflow.transition(WorkflowStatus.QUEUED)
            elif new_state == WorkflowRunStatus.ABORTED:
                self.workflow.transition(WorkflowStatus.ABORTED)
            elif new_state == WorkflowRunStatus.RUNNING:
                self.workflow.transition(WorkflowStatus.RUNNING)
            elif new_state == WorkflowRunStatus.DONE:
                self.workflow.transition(WorkflowStatus.DONE)
            elif new_state == WorkflowRunStatus.TERMINATED:
                self.workflow.transition(WorkflowStatus.HALTED)
            elif new_state in self.bound_error_states:
                self.workflow.transition(WorkflowStatus.FAILED)
            elif new_state == WorkflowRunStatus.INSTANTIATED:
                self.workflow.transition(WorkflowStatus.INSTANTIATING)
            elif new_state == WorkflowRunStatus.LAUNCHED:
                self.workflow.transition(WorkflowStatus.LAUNCHED)

    def hot_reset(self) -> None:
        """Set Workflow Run to Hot Resume."""
        structlog.contextvars.bind_contextvars(
            workflow_run_id=self.id, workflow_id=self.workflow_id
        )
        logger.info("Transitioning workflow_run to HOT_RESUME.")
        self.transition(WorkflowRunStatus.HOT_RESUME)

    def cold_reset(self) -> None:
        """Set Workflow Run to Cold Resume."""
        structlog.contextvars.bind_contextvars(
            workflow_run_id=self.id, workflow_id=self.workflow_id
        )
        logger.info("Transitioning workflow_run to COLD_RESUME.")
        self.transition(WorkflowRunStatus.COLD_RESUME)

    def _validate_transition(self, new_state: str) -> None:
        """Ensure the Job state transition is valid."""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition("WorkflowRun", self.id, self.status, new_state)

    def _is_timely_transition(self, new_state: str) -> bool:
        """Check if the transition is invalid due to a race condition."""
        structlog.contextvars.bind_contextvars(
            workflow_run_id=self.id, workflow_id=self.workflow_id
        )
        if (self.status, new_state) in self.untimely_transitions:
            logger.info(
                f"Ignoring transition of workflow_run from {self.status} to {new_state}"
            )
            return False
        else:
            logger.debug(
                f"No race condition when transitioning workflow_run from "
                f"{self.status} to {new_state}"
            )
            return True
