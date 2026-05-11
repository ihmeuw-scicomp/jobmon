"""Match SciComp prod schema: relax Pattern-A NOT NULL and add task.submitted_date.

Brings the Infra (jobmon_dev) schema in line with the SciComp prod (docker)
schema. See dev/schema-diff-2026-05-08.md for the full audit.

Two changes:
1. Relax NOT NULL -> NULL on the "Pattern A" columns that prod leaves
   nullable but Infra currently enforces non-null. Required so prod data
   can be imported into Infra without IntegrityError on rows where prod
   accepted NULL.
2. Add task.submitted_date as a NULL datetime column. Prod has it as a
   denormalization; the ORM does not declare it. Carrying the column on
   Infra preserves any operational analytics that reference it.

Safe to run live on MySQL 8.0: every ALTER uses ALGORITHM=INSTANT or
INPLACE with LOCK=NONE. Relaxing NOT NULL -> NULL is a metadata-only
operation on InnoDB 8.0+, so this migration is fast even on large
tables (no full table scan required).

Downgrade re-tightens NOT NULL and drops task.submitted_date. Re-tightening
DOES require a full-table validation scan and will FAIL if any row carries
NULL in those columns at downgrade time -- backfill first if running on a
populated database.

Revision ID: d4e5f6a7b8c9
Revises: b2c3d4e5f6a7
Create Date: 2026-05-11
"""

from typing import List, Sequence, Tuple, Union

import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (table, column, mysql_column_type, sqlalchemy_type)
_NULLABLE_RELAX: List[Tuple[str, str, str, sa.types.TypeEngine]] = [
    ("array", "max_concurrently_running", "INT", sa.Integer()),
    ("task", "command", "TEXT", sa.Text()),
    ("task", "max_attempts", "INT", sa.Integer()),
    ("task", "num_attempts", "INT", sa.Integer()),
    ("task_instance", "array_id", "INT", sa.Integer()),
    ("task_instance", "array_batch_num", "INT", sa.Integer()),
    ("task_instance", "array_step_id", "INT", sa.Integer()),
    ("task_instance", "task_resources_id", "INT", sa.Integer()),
    ("task_instance", "workflow_run_id", "INT", sa.Integer()),
    ("task_instance_error_log", "description", "TEXT", sa.Text()),
    ("task_resources", "requested_resources", "TEXT", sa.Text()),
    ("task_template_version", "command_template", "TEXT", sa.Text()),
    ("workflow", "dag_id", "INT", sa.Integer()),
    ("workflow", "max_concurrently_running", "INT", sa.Integer()),
    ("workflow_run", "workflow_id", "INT", sa.Integer()),
]


def upgrade() -> None:
    """Apply Pattern-A nullability relaxation and add task.submitted_date."""
    bind = op.get_bind()
    is_mysql = bind.dialect.name == "mysql"

    for table, column, mysql_type, sa_type in _NULLABLE_RELAX:
        if is_mysql:
            op.execute(
                sa.text(
                    f"ALTER TABLE {table} "
                    f"MODIFY COLUMN {column} {mysql_type} NULL, "
                    "ALGORITHM=INPLACE, LOCK=NONE"
                )
            )
        else:
            with op.batch_alter_table(table) as batch_op:
                batch_op.alter_column(column, existing_type=sa_type, nullable=True)

    if is_mysql:
        op.execute(
            sa.text(
                "ALTER TABLE task "
                "ADD COLUMN submitted_date DATETIME NULL, "
                "ALGORITHM=INSTANT"
            )
        )
    else:
        op.add_column("task", sa.Column("submitted_date", sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Re-tighten NOT NULL and drop task.submitted_date.

    Re-tightening fails if any row carries NULL in the affected columns.
    Backfill before running this on a populated database.
    """
    bind = op.get_bind()
    is_mysql = bind.dialect.name == "mysql"

    if is_mysql:
        op.execute(sa.text("ALTER TABLE task DROP COLUMN submitted_date"))
    else:
        op.drop_column("task", "submitted_date")

    for table, column, mysql_type, sa_type in reversed(_NULLABLE_RELAX):
        if is_mysql:
            op.execute(
                sa.text(
                    f"ALTER TABLE {table} "
                    f"MODIFY COLUMN {column} {mysql_type} NOT NULL, "
                    "ALGORITHM=INPLACE, LOCK=NONE"
                )
            )
        else:
            with op.batch_alter_table(table) as batch_op:
                batch_op.alter_column(column, existing_type=sa_type, nullable=False)
