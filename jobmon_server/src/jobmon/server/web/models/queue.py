"""Queue Table in the Database."""
from sqlalchemy import Column, ForeignKey, Integer, select, String, UniqueConstraint
from sqlalchemy.orm import relationship, Session

from jobmon.core.serializers import SerializeQueue
from jobmon.server.web.models import Base
from jobmon.server.web.models.cluster import Cluster


class Queue(Base):
    """Queue Table in the Database."""

    __tablename__ = "queue"

    def to_wire_as_requested_by_client(self) -> tuple:
        """Serialize cluster object."""
        return SerializeQueue.to_wire(self.id, self.name, self.parameters)

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    cluster_id = Column(Integer, ForeignKey("cluster.id"))
    parameters = Column(String(2500))

    # ORM relationships
    cluster = relationship("Cluster", back_populates="queues")

    __table_args__ = (
        UniqueConstraint("name", "cluster_id", name="uc_name_cluster_id"),
    )


def add_queues(session: Session) -> None:
    """Populate the queue table in the database."""
    for cluster_name in ["dummy", "sequential", "multiprocess"]:
        cluster = (
            session.execute(select(Cluster).where(Cluster.name == cluster_name))
            .scalars()
            .one()
        )
        if cluster_name == "multiprocess":
            parameters = '{"cores": (1,20)}'
        else:
            parameters = "{}"
        session.add(Queue(name="null.q", cluster_id=cluster.id, parameters=parameters))
