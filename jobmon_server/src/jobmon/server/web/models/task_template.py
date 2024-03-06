"""Task Template database table."""

from sqlalchemy import ForeignKey, Integer, UniqueConstraint, VARCHAR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from jobmon.core.serializers import SerializeClientTaskTemplate
from jobmon.server.web.models import Base


class TaskTemplate(Base):
    """Task Template database table."""

    __tablename__ = "task_template"

    def to_wire_as_client_task_template(self) -> tuple:
        """Serialize Task Template."""
        # serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        # return serialized
        return SerializeClientTaskTemplate.to_wire(
            self.id, self.tool_version_id, self.name
        )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tool_version_id: Mapped[int] = mapped_column(Integer, ForeignKey("tool_version.id"))
    name: Mapped[str] = mapped_column(VARCHAR(255))

    # orm relationship
    tool_versions = relationship("ToolVersion", back_populates="task_templates")
    task_template_versions = relationship(
        "TaskTemplateVersion", back_populates="task_template"
    )

    __table_args__ = (
        UniqueConstraint("tool_version_id", "name", name="uc_tool_version_id_name"),
    )
