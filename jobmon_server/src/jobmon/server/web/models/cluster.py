"""Cluster Table in the Database."""
from typing import Tuple

from sqlalchemy import Column, ForeignKey, Integer, select, String
from sqlalchemy.orm import relationship, Session

from jobmon.core.serializers import SerializeCluster
from jobmon.server.web.models import Base
from jobmon.server.web.models.cluster_type import ClusterType


class Cluster(Base):
    """Cluster Table in the Database."""

    __tablename__ = "cluster"

    def to_wire_as_requested_by_client(self) -> Tuple:
        """Serialize cluster object."""
        return SerializeCluster.to_wire(
            self.id,
            self.name,
            self.cluster_type.name,
            self.connection_parameters,
        )

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)
    cluster_type_id = Column(Integer, ForeignKey("cluster_type.id"))
    connection_parameters = Column(String(2500))

    # ORM relationships
    cluster_type = relationship("ClusterType", back_populates="clusters")
    queues = relationship("Queue", back_populates="cluster")


def add_clusters(session: Session) -> None:
    """Populate the cluster table in the database."""
    for cluster_type_name in ["dummy", "sequential", "multiprocess"]:
        cluster_type = (
            session.execute(
                select(ClusterType).where(ClusterType.name == cluster_type_name)
            )
            .scalars()
            .one()
        )
        cluster = Cluster(
            name=cluster_type_name,
            cluster_type_id=cluster_type.id,
            connection_parameters=None,
        )
        session.add(cluster)
