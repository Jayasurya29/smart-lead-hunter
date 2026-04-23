"""add_address_to_potential_leads

Revision ID: 016
Revises: 015
Create Date: 2026-04-23

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '016'
down_revision: Union[str, None] = '015_add_project_type'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('potential_leads', sa.Column('address', sa.Text(), nullable=True))
    op.add_column('potential_leads', sa.Column('zip_code', sa.String(20), nullable=True))


def downgrade() -> None:
    op.drop_column('potential_leads', 'zip_code')
    op.drop_column('potential_leads', 'address')
