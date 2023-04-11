"""Cluster Table in the Database."""
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

# from jobmon.core.serializers import SerializeBatch
from jobmon.server.web.models import Base


class Batch(Base):
    """A Batch Table in the Database.

    Batches represent groups of task instances that share the same array ID and task resources,
    that are ready to run at the same point in time.
    """

    __tablename__ = "batch"

    id = Column(Integer, primary_key=True)
    cluster_id = Column(Integer, ForeignKey("cluster.id"))
    task_resources_id = Column(Integer, ForeignKey("task_resources.id"))
    distributor_instance_id = Column(Integer, ForeignKey("distributor_instance.id"))
    workflow_run_id = Column(Integer, ForeignKey("workflow_run.id"))
    array_id = Column(Integer, ForeignKey("array.id"), default=None)

    task_resources = relationship("TaskResources", foreign_keys=[task_resources_id])
