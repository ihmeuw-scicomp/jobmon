"""DAG Database Table."""
from sqlalchemy import Column, DateTime, Integer, VARCHAR
from sqlalchemy.orm import relationship

from jobmon.server.web.models import Base


class Dag(Base):
    """DAG Database Table."""

    __tablename__ = "dag"

    id = Column(Integer, primary_key=True)
    hash = Column(VARCHAR(50), unique=True)
    created_date = Column(DateTime, default=None)

    workflow = relationship("Workflow", back_populates="dag", lazy=True)
