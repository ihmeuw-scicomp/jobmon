"""Task Instance Status Table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskInstanceStatus as Statuses
from jobmon.server.web.models import Base


class TaskInstanceStatus(Base):
    """The table in the database that holds on the possible statuses for TaskInstance."""

    __tablename__ = "task_instance_status"

    QUEUED = Statuses.QUEUED
    INSTANTIATED = Statuses.INSTANTIATED
    NO_DISTRIBUTOR_ID = Statuses.NO_DISTRIBUTOR_ID
    LAUNCHED = Statuses.LAUNCHED
    RUNNING = Statuses.RUNNING
    TRIAGING = Statuses.TRIAGING
    RESOURCE_ERROR = Statuses.RESOURCE_ERROR
    UNKNOWN_ERROR = Statuses.UNKNOWN_ERROR
    ERROR = Statuses.ERROR
    DONE = Statuses.DONE
    KILL_SELF = Statuses.KILL_SELF
    ERROR_FATAL = Statuses.ERROR_FATAL

    id = Column(String(1), primary_key=True)
    label = Column(String(150))
    description = Column(String(150))


def add_task_instance_statuses(session: Session) -> None:
    """Populate the task_instance_status table in the database, in alphabetical order."""
    statuses = [
        TaskInstanceStatus(
            id="D", label="DONE", description="Task instance finished successfully."
        ),
        TaskInstanceStatus(
            id="E",
            label="ERROR",
            description="Task instance stopped with an application error "
            "(non-zero return code).",
        ),
        TaskInstanceStatus(
            id="F",
            label="ERROR_FATAL",
            description="Task instance killed itself as part of a cold workflow resume, "
            "and cannot be retried.",
        ),
        TaskInstanceStatus(
            id="I",
            label="INSTANTIATED",
            description="Task instance is created within Jobmon, but not queued for "
            "submission to the cluster.",
        ),
        TaskInstanceStatus(
            id="K",
            label="KILL_SELF",
            description="Task instance has been ordered to kill itself if it is still alive, "
            "as part of a cold workflow resume.",
        ),
        TaskInstanceStatus(
            id="O",
            label="QUEUED",
            description="Task instance submitted to the cluster normally, "
            "part of a Job Array.",
        ),
        TaskInstanceStatus(
            id="Q",
            label="QUEUED",
            description="TaskInstance is queued for submission to the cluster.",
        ),
        TaskInstanceStatus(
            id="R",
            label="RUNNING",
            description="Task instance has started running normally.",
        ),
        TaskInstanceStatus(
            id="T",
            label="TRIAGING",
            description="Task instance has errored, Jobmon "
            "is determining the category of error.",
        ),
        TaskInstanceStatus(
            id="U",
            label="UNKNOWN_ERROR",
            description="Task instance stopped reporting that it was alive "
            "for an unknown reason.",
        ),
        TaskInstanceStatus(
            id="W",
            label="NO_DISTRIBUTOR_ID",
            description="Task instance submission within Jobmon failed – "
            "did not receive a distributor_id from the cluster.",
        ),
        TaskInstanceStatus(
            id="Z",
            label="RESOURCE_ERROR",
            description="Task instance died because of insufficient resource request, "
            "i.e. insufficient memory or runtime.",
        ),
    ]
    session.add_all(statuses)
