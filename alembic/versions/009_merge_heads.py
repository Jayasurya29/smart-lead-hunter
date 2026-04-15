"""merge_sap_clients_and_geocoords

Merges two heads:
  - sap_clients_001 (add_sap_clients_table)
  - 008 (add_geocoords_and_website)

Revision ID: 009_merge
Revises: sap_clients_001, 008
Create Date: 2026-04-14
"""

from alembic import op

revision = "009_merge"
down_revision = ("sap_clients_001", "008")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
