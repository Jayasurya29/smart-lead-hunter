"""Add research_history table for outreach pipeline output.

Revision ID: 019
Revises: 018

Stores the output of the 5-agent LangGraph outreach pipeline (ported
from PitchIQ). One row per generated outreach. Tracks approval workflow
(pending/approved/rejected/sent) so sales can review and triage.

Per-Phase 1 design: this table records that an outreach was generated
+ approved, but no automated email send is wired yet. `sent_at` flips
when sales clicks "Mark as Sent" after manually sending from their own
Gmail/Outlook.

Foreign keys are SET NULL on delete so deleting a lead/hotel/contact
preserves the outreach record (snapshot fields keep the data readable).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY


# revision identifiers
revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "research_history",
        sa.Column("id", sa.Integer(), primary_key=True),
        # Source linkage — nullable so deletes preserve the record
        sa.Column(
            "lead_id",
            sa.Integer(),
            sa.ForeignKey("potential_leads.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "existing_hotel_id",
            sa.Integer(),
            sa.ForeignKey("existing_hotels.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "lead_contact_id",
            sa.Integer(),
            sa.ForeignKey("lead_contacts.id", ondelete="SET NULL"),
            nullable=True,
        ),
        # Snapshot inputs
        sa.Column("contact_name", sa.String(255), nullable=False),
        sa.Column("contact_title", sa.String(255), nullable=True),
        sa.Column("hotel_name", sa.String(255), nullable=False),
        sa.Column("hotel_location", sa.String(255), nullable=True),
        sa.Column("linkedin_url", sa.String(500), nullable=True),
        sa.Column("email", sa.String(255), nullable=True),
        # Researcher output
        sa.Column("company_summary", sa.Text(), nullable=True),
        sa.Column("contact_summary", sa.Text(), nullable=True),
        sa.Column("pain_points", ARRAY(sa.Text()), nullable=True),
        sa.Column("signals", ARRAY(sa.Text()), nullable=True),
        sa.Column("outreach_angle", sa.String(500), nullable=True),
        sa.Column("personalization_hook", sa.Text(), nullable=True),
        sa.Column("hotel_tier", sa.String(50), nullable=True),
        sa.Column("hiring_signals", ARRAY(sa.Text()), nullable=True),
        sa.Column("recent_news", ARRAY(sa.Text()), nullable=True),
        # Analyst output
        sa.Column("fit_score", sa.Integer(), nullable=True),
        sa.Column("value_props", ARRAY(sa.Text()), nullable=True),
        # Writer output
        sa.Column("email_subject", sa.String(500), nullable=True),
        sa.Column("email_body", sa.Text(), nullable=True),
        sa.Column("linkedin_message", sa.Text(), nullable=True),
        # Critic output
        sa.Column("quality_approved", sa.Boolean(), nullable=True),
        sa.Column("quality_feedback", sa.Text(), nullable=True),
        # Scheduler output
        sa.Column("send_time", sa.String(255), nullable=True),
        sa.Column("follow_up_sequence", ARRAY(sa.Text()), nullable=True),
        # Workflow
        sa.Column(
            "approval_status",
            sa.String(50),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("approval_notes", sa.Text(), nullable=True),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Indexes for the dashboard queries
    op.create_index(
        "ix_research_history_lead_id", "research_history", ["lead_id"]
    )
    op.create_index(
        "ix_research_history_existing_hotel_id",
        "research_history",
        ["existing_hotel_id"],
    )
    op.create_index(
        "ix_research_history_lead_contact_id",
        "research_history",
        ["lead_contact_id"],
    )
    op.create_index(
        "ix_research_history_approval_status",
        "research_history",
        ["approval_status"],
    )
    op.create_index(
        "ix_research_history_fit_score", "research_history", ["fit_score"]
    )
    op.create_index(
        "ix_research_history_status_created",
        "research_history",
        ["approval_status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_research_history_status_created", table_name="research_history")
    op.drop_index("ix_research_history_fit_score", table_name="research_history")
    op.drop_index("ix_research_history_approval_status", table_name="research_history")
    op.drop_index("ix_research_history_lead_contact_id", table_name="research_history")
    op.drop_index(
        "ix_research_history_existing_hotel_id", table_name="research_history"
    )
    op.drop_index("ix_research_history_lead_id", table_name="research_history")
    op.drop_table("research_history")
