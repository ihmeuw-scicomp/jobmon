"""Task Instance Error Log."""
from typing import Tuple

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship
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

    id = Column(Integer, primary_key=True)
    task_instance_id = Column(Integer, ForeignKey("task_instance.id"))
    error_time = Column(DateTime, default=func.now())
    description = Column(Text)

    task_instance = relationship("TaskInstance", back_populates="errors")
