"""024_create_contacts_tables

Phase 2: Inbox contact sync infrastructure.

Creates two tables:
  - contacts: master contact directory from email signature extraction,
    enriched with BrandRegistry + procurement priority.
  - mailbox_sync_state: per-mailbox Gmail History API checkpoint + run stats
    so we only sync deltas after the first scan.

Both tables use idempotent CREATE INDEX IF NOT EXISTS per the audit-2026-05-05
convention to avoid DuplicateTableError on re-runs.

Revision ID: 024_create_contacts_tables
Revises: 023_post_audit_fixes
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── CONTACTS table ────────────────────────────────────────────────
    op.create_table(
        "contacts",
        sa.Column("id", sa.Integer, primary_key=True),
        # Identity
        sa.Column("email", sa.Text, nullable=False, unique=True),
        sa.Column("first_name", sa.Text),
        sa.Column("last_name", sa.Text),
        sa.Column("display_name", sa.Text),
        sa.Column("title", sa.Text),
        sa.Column("organization", sa.Text),
        sa.Column("phone", sa.Text),  # E.164 normalized
        sa.Column("address", sa.Text),
        sa.Column("linkedin_url", sa.Text),
        # Provenance
        sa.Column("org_source", sa.Text),  # signature/saved_contacts/domain_inferred
        sa.Column(
            "has_signature",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("confidence", sa.Float),
        # Hospitality enrichment
        sa.Column("parent_company", sa.Text),
        sa.Column("brand_tier", sa.Text),
        sa.Column("operating_model", sa.Text),
        sa.Column("gpo", sa.Text),
        sa.Column(
            "procurement_priority",
            sa.Text,
            nullable=False,
            server_default="P_unknown",
        ),
        sa.Column("priority_reason", sa.Text),
        sa.Column("opportunity_level", sa.Text),
        sa.Column("opportunity_score", sa.Float),
        sa.Column("management_company", sa.Text),
        # Interaction tracking
        sa.Column(
            "interaction_count",
            sa.Integer,
            nullable=False,
            server_default="1",
        ),
        sa.Column(
            "source_mailboxes",
            postgresql.ARRAY(sa.Text),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("first_seen", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_seen", sa.DateTime(timezone=True), server_default=sa.func.now()),
        # Workflow
        sa.Column(
            "approval_status",
            sa.Text,
            nullable=False,
            server_default="pending",
        ),  # pending / approved / pushed_to_insightly
        sa.Column("insightly_contact_id", sa.Text),
        sa.Column("pushed_to_insightly_at", sa.DateTime(timezone=True)),
        # Pipeline linkage
        sa.Column(
            "matched_lead_id",
            sa.Integer,
            sa.ForeignKey("potential_leads.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "matched_hotel_id",
            sa.Integer,
            sa.ForeignKey("existing_hotels.id", ondelete="SET NULL"),
        ),
        # Audit
        sa.Column(
            "sync_history",
            postgresql.JSONB,
            nullable=False,
            server_default="[]",
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        # Constraints
        sa.CheckConstraint(
            "approval_status IN ('pending','approved','pushed_to_insightly')",
            name="contacts_approval_status_check",
        ),
        sa.CheckConstraint(
            "procurement_priority IN ('P1','P2','P3','P4','P_unknown')",
            name="contacts_priority_check",
        ),
    )

    # Idempotent indexes per audit-2026-05-05 convention
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_email ON contacts(email)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_priority ON contacts(procurement_priority)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_brand_tier ON contacts(brand_tier)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_approval_status ON contacts(approval_status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_last_seen ON contacts(last_seen DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_org ON contacts(organization)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_matched_lead ON contacts(matched_lead_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_matched_hotel ON contacts(matched_hotel_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_opportunity_score ON contacts(opportunity_score DESC)")

    # GIN index on source_mailboxes array (for "which contacts has Ugarcia seen?" queries)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_source_mailboxes "
        "ON contacts USING GIN(source_mailboxes)"
    )

    # ── MAILBOX_SYNC_STATE table ──────────────────────────────────────
    op.create_table(
        "mailbox_sync_state",
        sa.Column("mailbox", sa.Text, primary_key=True),
        sa.Column("last_history_id", sa.Text),  # Gmail History API cursor
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
        sa.Column("last_run_status", sa.Text),  # success / error / partial
        sa.Column("last_run_contacts_found", sa.Integer),
        sa.Column("last_run_new_contacts", sa.Integer),
        sa.Column("last_run_updated_contacts", sa.Integer),
        sa.Column("last_run_messages_scanned", sa.Integer),
        sa.Column("last_run_error", sa.Text),
        sa.Column(
            "consecutive_errors",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "is_active",
            sa.Boolean,
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint(
            "last_run_status IN ('success','error','partial','running') OR last_run_status IS NULL",
            name="mailbox_sync_state_status_check",
        ),
    )

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_mailbox_sync_state_active "
        "ON mailbox_sync_state(is_active)"
    )

    # ── Trigger to keep updated_at fresh on contacts UPDATE ───────────
    op.execute(
        """
        CREATE OR REPLACE FUNCTION contacts_set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute("DROP TRIGGER IF EXISTS contacts_updated_at_trigger ON contacts")
    op.execute(
        """
        CREATE TRIGGER contacts_updated_at_trigger
        BEFORE UPDATE ON contacts
        FOR EACH ROW EXECUTE FUNCTION contacts_set_updated_at()
        """
    )
    op.execute(
        "DROP TRIGGER IF EXISTS mailbox_sync_state_updated_at_trigger ON mailbox_sync_state"
    )
    op.execute(
        """
        CREATE TRIGGER mailbox_sync_state_updated_at_trigger
        BEFORE UPDATE ON mailbox_sync_state
        FOR EACH ROW EXECUTE FUNCTION contacts_set_updated_at()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS contacts_updated_at_trigger ON contacts")
    op.execute(
        "DROP TRIGGER IF EXISTS mailbox_sync_state_updated_at_trigger "
        "ON mailbox_sync_state"
    )
    op.execute("DROP FUNCTION IF EXISTS contacts_set_updated_at()")
    op.drop_table("mailbox_sync_state")
    op.drop_table("contacts")
