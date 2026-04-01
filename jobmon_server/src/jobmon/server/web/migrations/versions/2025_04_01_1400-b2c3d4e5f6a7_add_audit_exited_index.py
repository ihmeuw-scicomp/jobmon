"""Add (task_id, exited_at) index to task_status_audit.

Speeds up the bulk audit close query in create_audit_records_bulk:
  UPDATE task_status_audit SET exited_at = NOW()
  WHERE task_id IN (...) AND exited_at IS NULL

Safe to run on a live database — MySQL 8.0 builds secondary indexes
online (ALGORITHM=INPLACE, LOCK=NONE) without blocking reads or writes.

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2025-04-01
"""

from typing import Sequence, Union

from alembic import op

revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add composite index for bulk audit close queries."""
    op.create_index(
        "ix_task_status_audit_task_exited",
        "task_status_audit",
        ["task_id", "exited_at"],
        mysql_algorithm="INPLACE",
        mysql_lock="NONE",
    )


def downgrade() -> None:
    """Remove the (task_id, exited_at) index."""
    op.drop_index(
        "ix_task_status_audit_task_exited",
        table_name="task_status_audit",
    )
