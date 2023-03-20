"""Task Attribute Type table."""
from typing import Any, Dict

from sqlalchemy import Column, Integer, String

from jobmon.server.web.models import Base


class TaskAttributeType(Base):
    """Task Attribute Type Table."""

    __tablename__ = "task_attribute_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(255))

    @classmethod
    def from_wire(cls: Any, dct: Dict) -> Any:
        """Task Attribute Type object parsed from dict."""
        return cls(id=dct["task_attribute_type_id"], name=dct["name"])

    def to_wire(self) -> Dict:
        """Returns dict of TaskAttributeType attributes."""
        return {"task_attribute_type_id": self.id, "name": self.name}
