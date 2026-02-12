"""Task Status Audit Database Table."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from jobmon.server.web.models import Base


class TaskStatusAudit(Base):
    """Audit log for task status transitions - supports timeseries queries.

    Each record tracks when a task entered a status (entered_at) and when it
    exited that status (exited_at). This allows efficient queries for concurrent
    task counts without needing window functions.
    """

    __tablename__ = "task_status_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[int] = mapped_column(Integer, ForeignKey("task.id"), nullable=False)
    workflow_id: Mapped[int] = mapped_column(Integer, nullable=False)  # Denormalized
    previous_status: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    new_status: Mapped[str] = mapped_column(String(1), nullable=False)
    entered_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )
    exited_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_task_status_audit_workflow_time", "workflow_id", "entered_at"),
        Index("ix_task_status_audit_task_time", "task_id", "entered_at"),
    )
