"""ClusterType table in the database."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship, Session

from jobmon.server.web.models import Base


class ClusterType(Base):
    """ClusterType table in the database."""

    __tablename__ = "cluster_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)
    logfile_templates = Column(String(500))

    # ORM relationships
    clusters = relationship("Cluster", back_populates="cluster_type")


def add_cluster_types(session: Session) -> None:
    """Populate the cluster_type table in the database."""
    cluster_types = [
        ClusterType(name="dummy"),
        ClusterType(name="sequential"),
        ClusterType(name="multiprocess"),
    ]
    session.add_all(cluster_types)
