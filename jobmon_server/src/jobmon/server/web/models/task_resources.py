"""Task Resources Database Table."""
from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from jobmon.core.serializers import SerializeTaskResources
from jobmon.server.web.models import Base
from jobmon.server.web.models.task_resources_type import TaskResourcesType  # noqa F401


class TaskResources(Base):
    """The table in the database that holds all task specific resources.

    Task specific resources:
        queue_id - designated queue
        requested_resources
    """

    __tablename__ = "task_resources"

    def to_wire_as_task_resources(self) -> tuple:
        """Serialize executor task object."""
        serialized = SerializeTaskResources.to_wire(
            task_resources_id=self.id,
            queue_name=self.queue.name,
            task_resources_type_id=self.task_resources_type_id,
            requested_resources=self.requested_resources,
        )
        return serialized

    id = Column(Integer, primary_key=True)
    queue_id = Column(Integer, ForeignKey("queue.id"))
    task_resources_type_id = Column(String(1), ForeignKey("task_resources_type.id"))

    requested_resources = Column(Text, default=None)

    # ORM relationships
    queue = relationship("Queue", foreign_keys=[queue_id])
    task_resources_type = relationship(
        "TaskResourcesType", foreign_keys=[task_resources_type_id]
    )
