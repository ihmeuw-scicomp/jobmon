"""Template arg map table."""
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from jobmon.server.web.models import Base


class TemplateArgMap(Base):
    """Template Arg Map table."""

    __tablename__ = "template_arg_map"

    task_template_version_id = Column(
        Integer, ForeignKey("task_template_version.id"), primary_key=True
    )
    arg_id = Column(Integer, ForeignKey("arg.id"), primary_key=True)
    arg_type_id = Column(Integer, ForeignKey("arg_type.id"), primary_key=True)

    task_template_version = relationship(
        "TaskTemplateVersion", back_populates="template_arg_map"
    )
    argument = relationship("Arg", back_populates="template_arg_map")
    argument_type = relationship("ArgType", back_populates="template_arg_map")
