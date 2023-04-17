"""Cluster Table in the Database."""
from sqlalchemy import Boolean, Column, DateTime, Integer
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, Session
from typing import Optional

from jobmon.server.web._compat import add_time
from jobmon.server.web.models import Base


class DistributorInstance(Base):
    """Distributor Instance Table in the Database."""

    __tablename__ = "distributor_instance"

    id = Column(Integer, primary_key=True)
    report_by_date = Column(DateTime, default=func.now())
    expunged = Column(Boolean, default=False)
    cluster_id = Column(Integer)
    workflow_run_id = Column(Integer, default=None)

    def heartbeat(self, next_report_increment: float) -> None:
        """Register a heartbeat for the Workflow Run to show it is still alive."""
        self.report_by_date = add_time(next_report_increment)

    def expunge(self) -> None:
        self.expunged = True


def add_distributor_instance(
    cluster_id: int, session: Session, workflow_run_id: Optional[int] = None,
) -> DistributorInstance:
    """Create a DistributorInstance with the relevant cluster relationships.

    Mostly for unit testing purposes, but could be useful for initializing a new instance.
    """

    instance = DistributorInstance(cluster_id=cluster_id, workflow_run_id=workflow_run_id)
    session.add(instance)
    session.flush()

    return instance
