"""Cluster Table in the Database."""
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from jobmon.server.web.models import Base


class DistributorInstanceCluster(Base):
    """Association table between DistributorInstance and Cluster."""

    __tablename__ = "distributor_instance_cluster"

    distributor_instance_id = Column(
        Integer, ForeignKey("distributor_instance.id"), primary_key=True
    )
    cluster_id = Column(
        Integer, ForeignKey("cluster.id"), primary_key=True
    )

    # ORM relationships
    distributor_instances = relationship("DistributorInstance", back_populates="cluster_links")
    clusters = relationship("Cluster", back_populates="distributor_clusters")
