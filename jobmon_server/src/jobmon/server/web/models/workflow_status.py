"""Workflow status database table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

from jobmon.core.constants import WorkflowStatus as Statuses
from jobmon.server.web.models import Base


class WorkflowStatus(Base):
    """Workflow Status database table."""

    __tablename__ = "workflow_status"

    REGISTERING = Statuses.REGISTERING
    QUEUED = Statuses.QUEUED
    ABORTED = Statuses.ABORTED
    RUNNING = Statuses.RUNNING
    HALTED = Statuses.HALTED
    FAILED = Statuses.FAILED
    DONE = Statuses.DONE
    INSTANTIATING = Statuses.INSTANTIATING
    LAUNCHED = Statuses.LAUNCHED

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)
    description = Column(String(150))


def add_workflow_statuses(session: Session) -> None:
    """Populate the workflow_status in the database."""
    statuses = [
        WorkflowStatus(
            id="A",
            label="ABORTED",
            description="Workflow encountered an error before a WorkflowRun was created.",
        ),
        WorkflowStatus(
            id="D",
            label="DONE",
            description="Workflow has completed, it finished successfully.",
        ),
        WorkflowStatus(
            id="F",
            label="FAILED",
            description="Workflow unsuccessful in one or more WorkflowRuns, "
            "no runs finished successfully as DONE.",
        ),
        WorkflowStatus(
            id="G", label="REGISTERING", description="Workflow is being validated."
        ),
        WorkflowStatus(
            id="H",
            label="HALTED",
            description="Resume was set and Workflow is shut down or the controller died "
            "and therefore Workflow was reaped.",
        ),
        WorkflowStatus(
            id="I",
            label="INSTANTIATING",
            description="Jobmon Scheduler is creating a Workflow on the distributor.",
        ),
        WorkflowStatus(
            id="O",
            label="LAUNCHED",
            description="Workflow has been created. Distributor is now controlling tasks, "
            "or waiting for scheduling loop.",
        ),
        WorkflowStatus(
            id="Q",
            label="QUEUED",
            description="Jobmon client has updated the Jobmon database, "
            "and signalled Scheduler to create Workflow.",
        ),
        WorkflowStatus(
            id="R",
            label="RUNNING",
            description="Workflow has a WorkflowRun that is running.",
        ),
    ]
    session.add_all(statuses)
