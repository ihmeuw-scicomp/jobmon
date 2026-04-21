"""Add covering index to task_instance for TT detail queries.

The task-template aggregate and viz endpoints join through
task_instance and read ``wallclock``, ``maxrss``, and
``task_resources_id``. None of those columns live in a secondary
index, so each row required a random PK heap lookup — ~78K lookups
per query on large templates (pixel: ~6-7s of random I/O).

Adding ``(task_id, id, wallclock, maxrss, task_resources_id)`` makes
the hot path index-only: MySQL serves the join from the secondary
index alone, with zero heap reads. Expected impact:

  - aggregate endpoint on pixel:    ~6-7s → ~1-2s
  - viz page-0 on pixel:            ~8s   → ~3-4s

Safe to run on a live database — MySQL 8.0 builds secondary
indexes online (ALGORITHM=INPLACE, LOCK=NONE) without blocking
reads or writes. Estimated ~4 GB additional storage at 110M rows.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-04-21
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add covering index to eliminate heap lookups on TT queries."""
    bind = op.get_bind()
    if bind.dialect.name == "mysql":
        op.execute(
            sa.text(
                "ALTER TABLE task_instance "
                "ADD INDEX ix_ti_task_id_covering "
                "(task_id, id, wallclock, maxrss, task_resources_id), "
                "ALGORITHM=INPLACE, LOCK=NONE"
            )
        )
    else:
        op.create_index(
            "ix_ti_task_id_covering",
            "task_instance",
            ["task_id", "id", "wallclock", "maxrss", "task_resources_id"],
        )


def downgrade() -> None:
    """Remove the covering index."""
    op.drop_index("ix_ti_task_id_covering", table_name="task_instance")
