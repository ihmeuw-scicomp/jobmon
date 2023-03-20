"""Workflow Run Status Database Table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

from jobmon.core.constants import WorkflowRunStatus as Statuses
from jobmon.server.web.models import Base


class WorkflowRunStatus(Base):
    """Workflow Run Status Database Table."""

    __tablename__ = "workflow_run_status"

    REGISTERED = Statuses.REGISTERED
    LINKING = Statuses.LINKING
    BOUND = Statuses.BOUND
    ABORTED = Statuses.ABORTED
    RUNNING = Statuses.RUNNING
    DONE = Statuses.DONE
    STOPPED = Statuses.STOPPED
    ERROR = Statuses.ERROR
    COLD_RESUME = Statuses.COLD_RESUME
    HOT_RESUME = Statuses.HOT_RESUME
    TERMINATED = Statuses.TERMINATED
    INSTANTIATED = Statuses.INSTANTIATED
    LAUNCHED = Statuses.LAUNCHED

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)
    description = Column(String(150))


def add_workflow_run_statuses(session: Session) -> None:
    """Populate the workflow_run_status table in the database."""
    statuses = [
        WorkflowRunStatus(
            id="A",
            label="ABORTED",
            description="WorkflowRun encountered problems while binding so it stopped.",
        ),
        WorkflowRunStatus(
            id="B",
            label="BOUND",
            description="WorkflowRun has been bound to the database.",
        ),
        WorkflowRunStatus(
            id="C",
            label="COLD_RESUME",
            description="WorkflowRun is set to resume as soon all existing tasks are killed.",
        ),
        WorkflowRunStatus(
            id="D",
            label="DONE",
            description="WorkflowRun is Done, it successfully completed.",
        ),
        WorkflowRunStatus(
            id="E",
            label="ERROR",
            description="WorkflowRun did not complete successfully, either some Tasks "
            "failed or (rarely) an internal Jobmon error.",
        ),
        WorkflowRunStatus(
            id="G", label="REGISTERED", description="WorkflowRun has been validated."
        ),
        WorkflowRunStatus(
            id="H",
            label="HOT_RESUME",
            description="WorkflowRun was set to hot-resume while tasks are still running, "
            "they will continue running.",
        ),
        WorkflowRunStatus(
            id="I",
            label="INSTANTIATED",
            description="Scheduler is instantiating a WorkflowRun on the distributor.",
        ),
        WorkflowRunStatus(
            id="L",
            label="LINKING",
            description="WorkflowRun completed successfully, updating the Workflow.",
        ),
        WorkflowRunStatus(
            id="O",
            label="LAUNCHED",
            description="Instantiation complete. Distributor is controlling Tasks or waiting "
            "for scheduling loop.",
        ),
        WorkflowRunStatus(
            id="R", label="RUNNING", description="WorkflowRun is currently running."
        ),
        WorkflowRunStatus(
            id="S",
            label="STOPPED",
            description="WorkflowRun was deliberately stopped, probably due to keyboard "
            "interrupt from user.",
        ),
        WorkflowRunStatus(
            id="T",
            label="TERMINATED",
            description="This WorkflowRun is being replaced by a new WorkflowRun created "
            "to pick up remaining Tasks, this WFR is terminating.",
        ),
    ]
    session.add_all(statuses)
