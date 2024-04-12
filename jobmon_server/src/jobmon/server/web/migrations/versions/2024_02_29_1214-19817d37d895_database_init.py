"""database init.

Revision ID: 19817d37d895
Revises:
Create Date: 2024-02-29 12:14:14.344720
"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "19817d37d895"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade the database."""
    pass


def downgrade() -> None:
    """Downgrade the database."""
    pass
