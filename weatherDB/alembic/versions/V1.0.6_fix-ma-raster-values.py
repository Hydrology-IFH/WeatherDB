"""Remove station_ma_raster values because they were faulty.

Revision ID: V1.0.6
Revises: V1.0.2
Create Date: 2024-12-06 14:33:30.129005

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'V1.0.6'
down_revision: Union[str, None] = 'V1.0.2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.text(
        """
        DELETE FROM station_ma_raster
        WHERE True;
        """))

def downgrade() -> None:
    op.execute(sa.text(
        """
        DELETE FROM station_ma_raster
        WHERE True;
        """))
