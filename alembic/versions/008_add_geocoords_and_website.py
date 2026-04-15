"""add_geocoords_and_website_to_potential_leads

Adds latitude, longitude for map display of pre-opening leads,
and hotel_website_verified flag to track auto-found vs manually set websites.

Revision ID: 008
Revises: 007
Create Date: 2026-04-14
"""

from alembic import op
import sqlalchemy as sa

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade():
    # Add geocoords to potential_leads
    op.add_column("potential_leads", sa.Column("latitude", sa.Float(), nullable=True))
    op.add_column("potential_leads", sa.Column("longitude", sa.Float(), nullable=True))
    # Track whether website was auto-discovered or manually entered
    op.add_column("potential_leads", sa.Column("website_verified", sa.Boolean(), nullable=True, server_default="false"))
    # Index for map queries (leads with coords)
    op.create_index("ix_potential_leads_coords", "potential_leads",
                    ["latitude", "longitude"],
                    postgresql_where=sa.text("latitude IS NOT NULL AND longitude IS NOT NULL"))


def downgrade():
    op.drop_index("ix_potential_leads_coords", table_name="potential_leads")
    op.drop_column("potential_leads", "website_verified")
    op.drop_column("potential_leads", "longitude")
    op.drop_column("potential_leads", "latitude")
