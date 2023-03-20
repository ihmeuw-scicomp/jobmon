"""Node Table in the Database."""
from sqlalchemy import Column, Integer, UniqueConstraint, VARCHAR

from jobmon.server.web.models import Base


class Node(Base):
    """Node Table in the Database."""

    __tablename__ = "node"

    id = Column(Integer, primary_key=True)
    task_template_version_id = Column(Integer, index=True)
    node_args_hash = Column(VARCHAR(50))

    __table_args__ = (
        UniqueConstraint(
            "task_template_version_id",
            "node_args_hash",
            name="uc_task_template_version_id_node_args_hash",
        ),
    )
