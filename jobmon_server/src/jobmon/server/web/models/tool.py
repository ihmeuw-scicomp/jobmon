"""Tool DB Table."""

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from jobmon.core.serializers import SerializeClientTool
from jobmon.server.web.models import Base


class Tool(Base):
    """Tool DB Table."""

    __tablename__ = "tool"

    def to_wire_as_client_tool(self) -> tuple:
        """Serialize tool object."""
        serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        return serialized

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    tool_versions = relationship("ToolVersion", back_populates="tool")
