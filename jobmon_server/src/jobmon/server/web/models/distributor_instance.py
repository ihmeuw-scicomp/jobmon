"""Cluster Table in the Database."""
from sqlalchemy import Boolean, Column, DateTime, Integer
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, Session

from jobmon.server.web._compat import add_time
from jobmon.server.web.models import Base
from jobmon.server.web.models.distributor_instance_cluster import DistributorInstanceCluster


class DistributorInstance(Base):
    """Distributor Instance Table in the Database."""

    __tablename__ = "distributor_instance"

    id = Column(Integer, primary_key=True)
    report_by_date = Column(DateTime, default=func.now())
    expunged = Column(Boolean, default=False)

    cluster_links = relationship(
        "DistributorInstanceCluster",
        back_populates="distributor_instances"
    )

    def heartbeat(self, next_report_increment: float) -> None:
        """Register a heartbeat for the Workflow Run to show it is still alive."""
        self.report_by_date = add_time(next_report_increment)

    def expunge(self) -> None:
        self.expunged = True


def add_distributor_instances_with_associations(
    cluster_ids: list[int], session: Session
) -> DistributorInstance:
    """Create a DistributorInstance with the relevant cluster relationships.

    Mostly for unit testing purposes, but could be useful for initializing a new instance.
    """

    instance = DistributorInstance()
    session.add(instance)
    session.flush()

    for cluster_id in cluster_ids:
        cluster_instance = DistributorInstanceCluster(
            distributor_instance_id=instance.id,
            cluster_id=cluster_id
        )
        session.add(cluster_instance)

    return instance
