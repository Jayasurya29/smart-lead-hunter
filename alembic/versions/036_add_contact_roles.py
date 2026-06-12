"""add contact_roles dictionary (role-name intelligence)

The role dictionary: one row per DISTINCT normalized role string ever seen on a
contact, with its learned classification. This is the lookup table that makes
role classification fast, vertical-aware, and CUMULATIVE -- a title is judged
once and remembered forever, instead of being re-guessed (or thrown away as
"unknown") on every scrape.

Why a table and not more hardcoded keyword lists:
  * Coverage -- JA sells beyond hotels (parking, healthcare, education, grocery).
    The in-code lists only know hospitality vocabulary, so every non-hotel
    procurement/ops title fell through to P_unknown. The table holds all
    verticals and grows as new ones appear.
  * Memory -- an unmatched title gets classified once (rule or LLM) and stored.
    The next contact with that title is an instant lookup.
  * Reviewability -- a human can see the high-frequency roles, correct a
    mapping, mark a role relevant/irrelevant, and that decision sticks (and is
    never overwritten by an automated pass).

Columns
  role_raw         a representative original-cased example (for display)
  role_normalized  lowercase/trimmed key -- UNIQUE; what we look up on
  vertical         hospitality|parking_valet|education|healthcare|grocery|
                   corporate|other|unknown  (the buying context the role implies)
  priority         P1|P2|P3|P4|P_unknown    (same ladder as procurement_priority)
  is_relevant      whether this role is worth contacting at all (a janitorial
                   uniform buyer = yes; a software sales AE at a vendor = no)
  seniority        c_suite|director|manager|staff|unknown (advisory)
  source           'rule' | 'llm' | 'human'  -- provenance of the current mapping
  reviewed         a human has confirmed/edited this row (locks it)
  contact_count    how many contacts currently carry this role (mining stat;
                   drives "label the frequent ones first")
  confidence       0..1 for rule/llm mappings
  notes            free text (reason / examples)

Revision ID: 036
Revises: 035
Create Date: 2026-06-12
"""

from alembic import op
import sqlalchemy as sa

revision = "036"
down_revision = "035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "contact_roles",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("role_raw", sa.Text, nullable=False),
        sa.Column("role_normalized", sa.Text, nullable=False),
        sa.Column("vertical", sa.String(32), nullable=False, server_default="unknown"),
        sa.Column("priority", sa.String(12), nullable=False, server_default="P_unknown"),
        sa.Column("is_relevant", sa.Boolean, nullable=True),
        sa.Column("seniority", sa.String(16), nullable=True),
        sa.Column("source", sa.String(12), nullable=False, server_default="rule"),
        sa.Column("reviewed", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("contact_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("confidence", sa.Float, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint(
            "priority IN ('P1','P2','P3','P4','P_unknown')",
            name="ck_contact_roles_priority",
        ),
        sa.CheckConstraint(
            "vertical IN ('hospitality','parking_valet','education','healthcare',"
            "'grocery','corporate','other','unknown')",
            name="ck_contact_roles_vertical",
        ),
        sa.CheckConstraint(
            "source IN ('rule','llm','human')",
            name="ck_contact_roles_source",
        ),
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_roles_normalized "
        "ON contact_roles(role_normalized)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contact_roles_priority "
        "ON contact_roles(priority)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contact_roles_count "
        "ON contact_roles(contact_count DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contact_roles_count")
    op.execute("DROP INDEX IF EXISTS ix_contact_roles_priority")
    op.execute("DROP INDEX IF EXISTS ux_contact_roles_normalized")
    op.drop_table("contact_roles")
