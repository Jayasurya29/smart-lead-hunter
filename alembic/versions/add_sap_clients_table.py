"""add sap_clients table

Revision ID: sap_clients_001
Revises: 007
Create Date: 2026-04-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "sap_clients_001"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sap_clients",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("customer_code", sa.String(50), nullable=False, unique=True),
        sa.Column("customer_name", sa.String(300), nullable=False),
        sa.Column("customer_name_normalized", sa.String(300)),
        sa.Column("customer_group", sa.String(100)),
        sa.Column("customer_type", sa.String(50), server_default="unknown"),
        sa.Column("is_hotel", sa.Boolean(), server_default="false"),
        sa.Column("phone", sa.String(100)),
        sa.Column("email", sa.String(255)),
        sa.Column("contact_person", sa.String(200)),
        sa.Column("street", sa.String(300)),
        sa.Column("city", sa.String(100)),
        sa.Column("state", sa.String(10)),
        sa.Column("zip_code", sa.String(20)),
        sa.Column("country", sa.String(10), server_default="US"),
        sa.Column("revenue_current_year", sa.Float(), server_default="0"),
        sa.Column("revenue_last_year", sa.Float(), server_default="0"),
        sa.Column("revenue_lifetime", sa.Float(), server_default="0"),
        sa.Column("total_invoices", sa.Integer(), server_default="0"),
        sa.Column("customer_since", sa.String(20)),
        sa.Column("last_order_date", sa.String(20)),
        sa.Column("days_since_last_order", sa.Integer()),
        sa.Column("sales_rep", sa.String(100)),
        sa.Column("brand", sa.String(100)),
        sa.Column("brand_tier", sa.String(50)),
        sa.Column("room_count", sa.Integer()),
        sa.Column("hotel_website", sa.String(500)),
        sa.Column("latitude", sa.Float()),
        sa.Column("longitude", sa.Float()),
        sa.Column("matched_lead_id", sa.Integer()),
        sa.Column("import_batch", sa.String(50)),
        sa.Column("last_imported_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("notes", sa.Text()),
        sa.Column("extra_data", postgresql.JSONB(), server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index("ix_sap_clients_customer_code", "sap_clients", ["customer_code"], unique=True)
    op.create_index("ix_sap_clients_customer_group", "sap_clients", ["customer_group"])
    op.create_index("ix_sap_clients_state", "sap_clients", ["state"])
    op.create_index("ix_sap_clients_city_state", "sap_clients", ["city", "state"])
    op.create_index("ix_sap_clients_revenue_lifetime", "sap_clients", ["revenue_lifetime"])
    op.create_index("ix_sap_clients_days_since_last_order", "sap_clients", ["days_since_last_order"])
    op.create_index("ix_sap_clients_sales_rep", "sap_clients", ["sales_rep"])
    op.create_index("ix_sap_clients_is_hotel", "sap_clients", ["is_hotel"])


def downgrade() -> None:
    op.drop_index("ix_sap_clients_is_hotel", table_name="sap_clients")
    op.drop_index("ix_sap_clients_sales_rep", table_name="sap_clients")
    op.drop_index("ix_sap_clients_days_since_last_order", table_name="sap_clients")
    op.drop_index("ix_sap_clients_revenue_lifetime", table_name="sap_clients")
    op.drop_index("ix_sap_clients_city_state", table_name="sap_clients")
    op.drop_index("ix_sap_clients_state", table_name="sap_clients")
    op.drop_index("ix_sap_clients_customer_group", table_name="sap_clients")
    op.drop_index("ix_sap_clients_customer_code", table_name="sap_clients")
    op.drop_table("sap_clients")
