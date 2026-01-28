"""Add task_status_audit table for timeseries view.

Revision ID: a1b2c3d4e5f6
Revises: 4762c850f79c
Create Date: 2025-01-28

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "4762c850f79c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create task_status_audit table for Task Concurrency Timeline feature."""
    op.create_table(
        "task_status_audit",
        sa.Column("id", sa.Integer(), nullable=False, primary_key=True),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("workflow_id", sa.Integer(), nullable=False),
        sa.Column("previous_status", sa.String(1), nullable=True),
        sa.Column("new_status", sa.String(1), nullable=False),
        sa.Column(
            "entered_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("exited_at", sa.DateTime(), nullable=True),
    )

    # Indexes for timeseries queries (workflow + time range)
    op.create_index(
        "ix_task_status_audit_workflow_time",
        "task_status_audit",
        ["workflow_id", "entered_at"],
    )
    op.create_index(
        "ix_task_status_audit_task_time",
        "task_status_audit",
        ["task_id", "entered_at"],
    )

    # Foreign key as index (matches existing pattern - no FK constraint for performance)
    op.create_index(
        "fkidx_task_status_audit_task_id",
        "task_status_audit",
        ["task_id"],
    )


def downgrade() -> None:
    """Remove task_status_audit table."""
    op.drop_table("task_status_audit")
