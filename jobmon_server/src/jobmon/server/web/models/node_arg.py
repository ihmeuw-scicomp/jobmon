"""Node arg db table."""
from sqlalchemy import Column, Integer, String

from jobmon.server.web.models import Base


class NodeArg(Base):
    """Node arg db table."""

    __tablename__ = "node_arg"

    node_id = Column(Integer, primary_key=True)
    arg_id = Column(Integer, primary_key=True)
    val = Column(String(2048))
