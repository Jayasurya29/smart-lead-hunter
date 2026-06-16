"""add buying_signal_products (what the buyer is purchasing)

Level-2 of the buyer-intelligence summary: the engine's extract_products() reads
the thread for the actual garments/items being discussed (aprons, polos, dress,
vest, FOH uniform program, ...). Stored comma-joined so the contact drawer can
say "is an active buyer of aprons, polos" instead of a bare "active buyer".

  buying_signal_products  TEXT  (comma-joined, specific garments first, <=4)

Revision ID: 039
Revises: 038
Create Date: 2026-06-16
"""

from alembic import op
import sqlalchemy as sa

revision = "039"
down_revision = "038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("buying_signal_products", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("contacts", "buying_signal_products")
