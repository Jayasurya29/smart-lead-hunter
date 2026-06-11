"""034: add contact_affiliations (typed person<->account edges).

Phase 1 of the contact-affiliation rework. Replaces the single-valued
"where does this person belong" model (contacts.matched_hotel_id /
lead_contacts.{lead_id|existing_hotel_id}) with a many-to-many edge that tells
the real story: a person has ONE employer and ZERO-OR-MORE coverage ties.

A portfolio buyer (e.g. a Crescent VP of Procurement) is `employed_by` Crescent
with scope='portfolio'; the 20 hotels they buy for are DERIVED (every hotel with
management_company='Crescent'), not stored. Only exceptions (a regional director
over a specific cluster) get explicit `covers` rows. An embedded GM is
`employed_by` the operator with scope='property' + one `stationed_at` edge.

Polymorphic on BOTH ends so it spans the two contact stores and all account
kinds. Management companies are a name (no table), so account_name carries them.

This migration is purely additive: it creates the table and indexes. The
existing matched_* / lead_id / existing_hotel_id columns are left untouched as a
fast denormalized cache — nothing downstream breaks. Backfill is a separate
script (backfill_affiliations.py).

Revision ID: 034
Revises: 033
"""

import sqlalchemy as sa
from alembic import op

revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "contact_affiliations",
        sa.Column("id", sa.Integer, primary_key=True),
        # ── Person (polymorphic: inbox `contacts` + lead-gen `lead_contacts`) ──
        sa.Column("person_type", sa.Text, nullable=False),  # contact | lead_contact
        sa.Column("person_id", sa.Integer, nullable=False),
        # ── Account (polymorphic) ──
        sa.Column("account_type", sa.Text, nullable=False),
        sa.Column("account_id", sa.Integer, nullable=True),  # hotel/lead id
        sa.Column("account_name", sa.Text, nullable=True),  # mgmt company (no table)
        # ── Relationship semantics ──
        sa.Column("relationship", sa.Text, nullable=False),
        sa.Column("scope", sa.Text, nullable=True),  # property|cluster|regional|portfolio
        sa.Column(
            "is_primary", sa.Boolean, nullable=False, server_default=sa.false()
        ),
        sa.Column("confidence", sa.Float),
        sa.Column("source", sa.Text),  # signature|lead_generator|manual|derived
        sa.Column("notes", sa.Text),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "person_type IN ('contact','lead_contact')",
            name="ck_affil_person_type",
        ),
        sa.CheckConstraint(
            "account_type IN ('existing_hotel','potential_lead','management_company')",
            name="ck_affil_account_type",
        ),
        sa.CheckConstraint(
            "relationship IN ('employed_by','stationed_at','covers','former')",
            name="ck_affil_relationship",
        ),
        sa.CheckConstraint(
            "scope IS NULL OR scope IN ('property','cluster','regional','portfolio')",
            name="ck_affil_scope",
        ),
        # A hotel/lead edge needs account_id; a management-company edge needs a name.
        sa.CheckConstraint(
            "(account_type = 'management_company' AND account_name IS NOT NULL) "
            "OR (account_type IN ('existing_hotel','potential_lead') "
            "AND account_id IS NOT NULL)",
            name="ck_affil_account_identity",
        ),
    )

    # One edge per (person, account, relationship). COALESCE handles the nullable
    # account_id / account_name so the unique index works for both account kinds
    # and lets the backfill use ON CONFLICT DO NOTHING (idempotent re-runs).
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_affiliations "
        "ON contact_affiliations (person_type, person_id, account_type, "
        "COALESCE(account_id, -1), COALESCE(lower(account_name), ''), relationship)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_affil_person "
        "ON contact_affiliations (person_type, person_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_affil_account "
        "ON contact_affiliations (account_type, account_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_affil_account_name "
        "ON contact_affiliations (lower(account_name))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_affil_relationship "
        "ON contact_affiliations (relationship)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_affil_relationship")
    op.execute("DROP INDEX IF EXISTS ix_affil_account_name")
    op.execute("DROP INDEX IF EXISTS ix_affil_account")
    op.execute("DROP INDEX IF EXISTS ix_affil_person")
    op.execute("DROP INDEX IF EXISTS uq_contact_affiliations")
    op.drop_table("contact_affiliations")
