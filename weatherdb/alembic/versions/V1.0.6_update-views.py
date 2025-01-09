"""Rebew the views

Revision ID: V1.0.6
Revises: V1.0.5
Create Date: 2024-12-06 14:33:30.129005

"""
from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = 'V1.0.6'
down_revision: Union[str, None] = 'V1.0.5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass

def downgrade() -> None:
    pass