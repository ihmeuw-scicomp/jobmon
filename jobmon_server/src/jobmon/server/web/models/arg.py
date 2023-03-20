"""Arg table in the database."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from jobmon.server.web.models import Base


class Arg(Base):
    """Arg table in the database."""

    __tablename__ = "arg"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)

    template_arg_map = relationship("TemplateArgMap", back_populates="argument")
