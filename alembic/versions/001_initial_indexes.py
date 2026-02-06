"""Apply schema.sql indexes and extensions

Revision ID: 001_initial_indexes
Revises: None
Create Date: 2026-02-06
"""
from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "001_initial_indexes"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(connection, table, column):
    result = connection.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = :table AND column_name = :column"
    ), {"table": table, "column": column})
    return result.fetchone() is not None


def _table_exists(connection, table):
    result = connection.execute(text(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_name = :table"
    ), {"table": table})
    return result.fetchone() is not None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    conn = op.get_bind()

    if _table_exists(conn, "potential_leads"):
        for col, idx in [
            ("status", "CREATE INDEX IF NOT EXISTS idx_leads_status ON potential_leads(status)"),
            ("lead_score", "CREATE INDEX IF NOT EXISTS idx_leads_score ON potential_leads(lead_score DESC)"),
            ("created_at", "CREATE INDEX IF NOT EXISTS idx_leads_created ON potential_leads(created_at DESC)"),
            ("hotel_name", "CREATE INDEX IF NOT EXISTS idx_leads_hotel_name_trgm ON potential_leads USING gin(hotel_name gin_trgm_ops)"),
            ("brand", "CREATE INDEX IF NOT EXISTS idx_leads_brand ON potential_leads(brand)"),
            ("state", "CREATE INDEX IF NOT EXISTS idx_leads_state ON potential_leads(state)"),
            ("country", "CREATE INDEX IF NOT EXISTS idx_leads_country ON potential_leads(country)"),
            ("opening_status", "CREATE INDEX IF NOT EXISTS idx_leads_opening_status ON potential_leads(opening_status)"),
        ]:
            if _column_exists(conn, "potential_leads", col):
                op.execute(idx)

    if _table_exists(conn, "scrape_logs"):
        for col, idx in [
            ("source_id", "CREATE INDEX IF NOT EXISTS idx_scrape_logs_source ON scrape_logs(source_id)"),
            ("started_at", "CREATE INDEX IF NOT EXISTS idx_scrape_logs_started ON scrape_logs(started_at DESC)"),
        ]:
            if _column_exists(conn, "scrape_logs", col):
                op.execute(idx)

    if _table_exists(conn, "lead_sources"):
        for col, idx in [
            ("is_active", "CREATE INDEX IF NOT EXISTS idx_sources_active ON lead_sources(is_active)"),
            ("priority", "CREATE INDEX IF NOT EXISTS idx_sources_priority ON lead_sources(priority DESC)"),
        ]:
            if _column_exists(conn, "lead_sources", col):
                op.execute(idx)


def downgrade() -> None:
    for idx in [
        "idx_leads_status", "idx_leads_score", "idx_leads_created",
        "idx_leads_hotel_name_trgm", "idx_leads_brand", "idx_leads_state",
        "idx_leads_country", "idx_leads_opening_status",
        "idx_scrape_logs_source", "idx_scrape_logs_started",
        "idx_sources_active", "idx_sources_priority",
    ]:
        op.execute(f"DROP INDEX IF EXISTS {idx}")