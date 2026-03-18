"""Add users and pending_registrations tables

Revision ID: 004_add_user_tables
Revises: 003_add_city_index
Create Date: 2026-03-17
"""

from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "004_add_user_tables"
down_revision: Union[str, None] = "003_add_city_index"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(connection, table):
    result = connection.execute(
        text("SELECT 1 FROM information_schema.tables WHERE table_name = :table"),
        {"table": table},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    conn = op.get_bind()

    if not _table_exists(conn, "users"):
        op.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) NOT NULL DEFAULT 'sales',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                last_login TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"
        )

    if not _table_exists(conn, "pending_registrations"):
        op.execute("""
            CREATE TABLE pending_registrations (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                role VARCHAR(20) NOT NULL DEFAULT 'sales',
                password_hash VARCHAR(255) NOT NULL,
                otp_hash VARCHAR(255) NOT NULL,
                otp_attempts INTEGER DEFAULT 0,
                otp_expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_email "
            "ON pending_registrations(email)"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pending_registrations")
    op.execute("DROP TABLE IF EXISTS users")
