"""Task Arg table."""
from sqlalchemy import Column, Integer, String


from jobmon.server.web.models import Base


class TaskArg(Base):
    """Task arg table."""

    __tablename__ = "task_arg"

    task_id = Column(Integer, primary_key=True)
    arg_id = Column(Integer, primary_key=True)
    val = Column(String(2048))
