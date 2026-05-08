"""add password-reset OTP fields to users

Created 2026-05-08.

Adds three columns supporting both flows:
  1. Self-serve forgot-password (POST /auth/forgot-password → email OTP →
     POST /auth/reset-password)
  2. Admin-triggered reset (POST /auth/users/{id}/reset-password → emails
     OTP to that user)

Mirrors the OTP storage pattern already used for PendingRegistration:
  - password_reset_otp_hash:        bcrypt hash of the 6-digit OTP
  - password_reset_otp_expires_at:  10-min expiry
  - password_reset_attempts:        cap at 5 attempts before requiring re-issue

Revision ID: 024
Revises: 023
Create Date: 2026-05-08
"""

from alembic import op
import sqlalchemy as sa


revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("password_reset_otp_hash", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "users",
        sa.Column(
            "password_reset_otp_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "users",
        sa.Column(
            "password_reset_attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("users", "password_reset_attempts")
    op.drop_column("users", "password_reset_otp_expires_at")
    op.drop_column("users", "password_reset_otp_hash")
