"""Task Instance Error Log."""

from typing import Tuple

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from jobmon.core.serializers import SerializeTaskInstanceErrorLog
from jobmon.server.web.models import Base


class TaskInstanceErrorLog(Base):
    """The table in the database that logs the error messages for task_instances."""

    __tablename__ = "task_instance_error_log"

    def to_wire(self) -> Tuple:
        """Serialize task instance error log object."""
        return SerializeTaskInstanceErrorLog.to_wire(
            self.id, self.error_time, self.description
        )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_instance_id = Column(Integer, ForeignKey("task_instance.id"), nullable=False)
    error_time = mapped_column(DateTime, default=func.now())
    description: Mapped[str] = mapped_column(Text)

    task_instance = relationship("TaskInstance", back_populates="errors")
