"""Task Attribute Table."""
from typing import Any, Dict

from sqlalchemy import Column, ForeignKey, Integer, String

from jobmon.server.web.models import Base


class TaskAttribute(Base):
    """Task Attribute Table."""

    __tablename__ = "task_attribute"

    task_id = Column(Integer, ForeignKey("task.id"), primary_key=True)
    task_attribute_type_id = Column(
        Integer, ForeignKey("task_attribute_type.id"), primary_key=True
    )
    value = Column(String(255))

    @classmethod
    def from_wire(cls: Any, dct: Dict) -> Any:
        """Task Attribute object created from dict."""
        return cls(
            id=dct["task_attribute_id"],
            task_id=dct["task_id"],
            attribute_type_id=dct["attribute_type_id"],
            value=dct["value"],
        )

    def to_wire(self) -> Dict:
        """Task attribute attributes to dict."""
        return {
            "task_attribute_id": self.id,
            "task_id": self.task_id,
            "attribute_type_id": self.attribute_type_id,
            "value": self.value,
        }
