"""Task Resources Type Database Table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskResourcesType as Types
from jobmon.server.web.models import Base


class TaskResourcesType(Base):
    """The table in the database that holds the possible statuses for the TaskResources."""

    __tablename__ = "task_resources_type"
    ORIGINAL = Types.ORIGINAL
    VALIDATED = Types.VALIDATED
    ADJUSTED = Types.ADJUSTED

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)


def add_task_resources_types(session: Session) -> None:
    """Populate the task_resources_type table in the database."""
    task_resources_type = [
        TaskResourcesType(id="O", label="ORIGINAL"),
        TaskResourcesType(id="V", label="VALIDATED"),
        TaskResourcesType(id="A", label="ADJUSTED"),
    ]
    session.add_all(task_resources_type)
