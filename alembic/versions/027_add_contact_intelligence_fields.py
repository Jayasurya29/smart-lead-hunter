"""027_add_contact_intelligence_fields

Adds AI-enrichment persistence to the contacts table so a contact's inferred
role / seniority / background can be stored consistently, with provenance and a
freshness timestamp (the "consistent, persistent, accurate" requirement).

These are written by the contact-intelligence enrichers:
  - Tier 1 (signals): role/seniority/department/is_decision_maker from email,
    domain, org, signature — cheap, runs on everyone.
  - Tier 2 (grounded): background/linkedin via grounded Gemini — for the
    contacts that matter.

Every enriched field is paired with confidence + source + a *_enriched_at
timestamp so a later pass never overwrites good data with a weaker guess and
stale rows can be refreshed.

Revision ID: 027
Revises: 026
"""

from alembic import op
import sqlalchemy as sa

revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None


# (name, type) — added only if missing, so re-running is safe.
_COLUMNS = [
    # Relevance verdict from the intelligence engine.
    ("relevance_verdict", sa.Text),        # relevant | junk | unknown
    ("relevance_score", sa.Integer),       # 0-100
    ("relevance_reason", sa.Text),
    # Inferred identity / role (Tier 1).
    ("inferred_role", sa.Text),            # e.g. "Purchasing", "General Manager"
    ("seniority", sa.Text),                # c_suite | director | manager | staff | unknown
    ("department", sa.Text),               # procurement | operations | housekeeping | f&b | ...
    ("is_decision_maker", sa.Boolean),
    # Background (Tier 2, grounded).
    ("background", sa.Text),               # short synthesized bio
    ("enrichment_source", sa.Text),        # signals | grounded | manual
    ("enrichment_confidence", sa.Float),   # 0.0-1.0
    ("enriched_at", sa.DateTime(timezone=True)),
    ("enrichment_model", sa.Text),         # e.g. "gemini-2.5-flash"
    # Linkage into the pipeline.
    ("linked_lead_id", sa.Integer),
    ("linked_existing_hotel_id", sa.Integer),
]


def upgrade() -> None:
    conn = op.get_bind()
    existing = {
        r[0]
        for r in conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'contacts'"
            )
        )
    }
    for name, type_ in _COLUMNS:
        if name not in existing:
            op.add_column("contacts", sa.Column(name, type_, nullable=True))

    # Index the verdict so the Contacts page can default to relevant cheaply.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_relevance "
        "ON contacts(relevance_verdict)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_enriched_at "
        "ON contacts(enriched_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contacts_relevance")
    op.execute("DROP INDEX IF EXISTS ix_contacts_enriched_at")
    for name, _ in _COLUMNS:
        op.execute(f"ALTER TABLE contacts DROP COLUMN IF EXISTS {name}")
