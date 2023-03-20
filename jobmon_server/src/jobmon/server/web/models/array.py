"""Array Table for the Database."""

from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from jobmon.core.serializers import SerializeDistributorArray
from jobmon.server.web.models import Base


class Array(Base):
    """Array Database object."""

    __tablename__ = "array"

    def to_wire_as_distributor_array(self) -> tuple:
        """Serialize executor task object."""
        serialized = SerializeDistributorArray.to_wire(
            array_id=self.id,
            max_concurrently_running=self.max_concurrently_running,
            name=self.name,
        )
        return serialized

    id = Column(Integer, primary_key=True)
    name = Column(String(255), index=True)
    task_template_version_id = Column(Integer, index=True)
    workflow_id = Column(Integer, index=True)
    max_concurrently_running = Column(Integer)
    created_date = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "task_template_version_id",
            "workflow_id",
            name="uc_task_template_version_id_workflow_id",
        ),
    )
