"""v7 - Add index on name columns in dim_exchange_id and dim_crypto_id

Revision ID: c6016d1e5963
Revises: 62c26ff5ae6b
Create Date: 2025-06-03 22:06:46.212355

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c6016d1e5963'
down_revision: Union[str, None] = '62c26ff5ae6b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
