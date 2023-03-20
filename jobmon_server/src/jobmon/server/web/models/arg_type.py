"""ArgType table in the database."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship, Session

from jobmon.core import constants
from jobmon.server.web.models import Base


class ArgType(Base):
    """ArgType table in the database."""

    __tablename__ = "arg_type"

    NODE_ARG = constants.ArgType.NODE_ARG
    TASK_ARG = constants.ArgType.TASK_ARG
    OP_ARG = constants.ArgType.OP_ARG

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)

    template_arg_map = relationship("TemplateArgMap", back_populates="argument_type")


def add_arg_types(session: Session) -> None:
    """Populate the arg_type table in the database."""
    types = [
        ArgType(id=1, name="NODE_ARG"),
        ArgType(id=2, name="TASK_ARG"),
        ArgType(id=3, name="OP_ARG"),
    ]
    session.add_all(types)
