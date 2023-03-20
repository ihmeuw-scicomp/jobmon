"""Workflow Attribute Database Table."""
from typing import Any, Dict

from sqlalchemy import Column, ForeignKey, Integer, String

from jobmon.server.web.models import Base


class WorkflowAttribute(Base):
    """Workflow Attribute Database Table."""

    __tablename__ = "workflow_attribute"

    workflow_id = Column(Integer, ForeignKey("workflow.id"), primary_key=True)
    workflow_attribute_type_id = Column(
        Integer, ForeignKey("workflow_attribute_type.id"), primary_key=True
    )
    value = Column(String(255))

    @classmethod
    def from_wire(cls: Any, dct: Dict) -> Any:
        """Create class object from dict."""
        return cls(
            workflow_id=dct["workflow_id"],
            workflow_attribute_type_id=dct["workflow_attribute_type_id"],
            value=dct["value"],
        )

    def to_wire(self) -> Dict:
        """Send data."""
        return {
            "workflow_id": self.workflow_id,
            "workflow_attribute_type_id": self.workflow_attribute_type_id,
            "value": self.value,
        }
