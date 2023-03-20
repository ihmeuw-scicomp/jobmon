"""Task Instance Status Database table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskStatus as Statuses
from jobmon.server.web.models import Base


class TaskStatus(Base):
    """The table in the database that holds on the possible statuses for a Task."""

    __tablename__ = "task_status"

    REGISTERING = Statuses.REGISTERING
    QUEUED = Statuses.QUEUED
    INSTANTIATING = Statuses.INSTANTIATING
    LAUNCHED = Statuses.LAUNCHED
    RUNNING = Statuses.RUNNING
    ERROR_RECOVERABLE = Statuses.ERROR_RECOVERABLE
    ADJUSTING_RESOURCES = Statuses.ADJUSTING_RESOURCES
    ERROR_FATAL = Statuses.ERROR_FATAL
    DONE = Statuses.DONE

    id = Column(String(1), primary_key=True)
    label = Column(String(150))
    description = Column(String(150))


def add_task_statuses(session: Session) -> None:
    """Populate the task_status table in the database."""
    statuses = [
        TaskStatus(
            id="A",
            label="ADJUSTING_RESOURCES",
            description="Task errored with a resource error, the resources will be "
            "adjusted before retrying.",
        ),
        TaskStatus(
            id="D",
            label="DONE",
            description="Task is Done, it ran successfully to completion; "
            "it has a TaskInstance that successfully completed.",
        ),
        TaskStatus(
            id="E",
            label="ERROR_RECOVERABLE",
            description="Task has errored out but has more attempts so it will be retried.",
        ),
        TaskStatus(
            id="F",
            label="ERROR_FATAL",
            description="Task errored out and has used all of the attempts, therefore has "
            "failed for this WorkflowRun. It can be resumed in a new WFR.",
        ),
        TaskStatus(
            id="G", label="REGISTERING", description="Task is bound to the database."
        ),
        TaskStatus(
            id="I", label="INSTANTIATING", description="Task is created within Jobmon."
        ),
        TaskStatus(
            id="O",
            label="LAUNCHED",
            description="Task instance submitted to the cluster normally, "
            "part of a Job Array.",
        ),
        TaskStatus(
            id="Q",
            label="QUEUED",
            description="Task's dependencies have successfully completed, task can be run "
            "when the scheduler is ready.",
        ),
        TaskStatus(
            id="R",
            label="RUNNING",
            description="Task is running on the specified distributor.",
        ),
    ]
    session.add_all(statuses)
