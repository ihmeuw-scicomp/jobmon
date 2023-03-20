"""Tool version db table."""
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from jobmon.core.serializers import SerializeClientToolVersion
from jobmon.server.web.models import Base


class ToolVersion(Base):
    """Tool version db table."""

    __tablename__ = "tool_version"

    def to_wire_as_client_tool_version(self) -> tuple:
        """Serialize tool version object."""
        serialized = SerializeClientToolVersion.to_wire(
            id=self.id, tool_id=self.tool_id
        )
        return serialized

    id = Column(Integer, primary_key=True)
    tool_id = Column(Integer, ForeignKey("tool.id"), nullable=False)

    # ORM relationships
    tool = relationship("Tool", back_populates="tool_versions")
    task_templates = relationship("TaskTemplate", back_populates="tool_versions")
