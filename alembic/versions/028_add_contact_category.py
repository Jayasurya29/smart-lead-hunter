"""028_add_contact_category

Adds the six-way contact_category to contacts:
  client | prospect | competitor | vendor | personal | junk

CLIENT/PROSPECT/COMPETITOR/PERSONAL are derived deterministically (SAP match,
competitor list, personal-domain). VENDOR/JUNK come from the LLM. The column is
re-derived on each enrichment pass so a prospect that becomes a SAP client is
auto-promoted to CLIENT.

Revision ID: 028
Revises: 027
"""

from alembic import op
import sqlalchemy as sa

revision = "028"
down_revision = "027"
branch_labels = None
depends_on = None


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
    if "contact_category" not in existing:
        op.add_column("contacts", sa.Column("contact_category", sa.Text, nullable=True))
    if "category_source" not in existing:
        # 'sap' | 'competitor_list' | 'personal_rule' | 'llm' | 'manual'
        op.add_column("contacts", sa.Column("category_source", sa.Text, nullable=True))
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_category "
        "ON contacts(contact_category)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contacts_category")
    op.execute("ALTER TABLE contacts DROP COLUMN IF EXISTS contact_category")
    op.execute("ALTER TABLE contacts DROP COLUMN IF EXISTS category_source")
