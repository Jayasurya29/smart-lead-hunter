"""add organization_normalized to contacts (property grouping key)

Adds a normalized org-name column used to group inbox contacts into real
properties on the Contacts "By hotel" view, so spelling/formatting variants
("Hilton Miami", "The Hilton Miami, LLC", "Hilton  Miami") collapse into one
group. Backfills existing rows and indexes the column for the grouped query.

Normalization is shared with the app via app.services.org_normalize so the
backfill and every future write produce identical keys.

Revision ID: 029
Revises: 028
"""

from alembic import op
import sqlalchemy as sa

from app.services.org_normalize import normalize_organization

# revision identifiers, used by Alembic.
revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Add the column only if missing (re-runnable, per 027/028 convention).
    existing = {
        r[0]
        for r in conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'contacts'"
            )
        )
    }
    if "organization_normalized" not in existing:
        op.add_column(
            "contacts",
            sa.Column("organization_normalized", sa.Text, nullable=True),
        )

    # 2) Backfill from existing organization values (only rows missing it).
    rows = conn.execute(
        sa.text(
            "SELECT id, organization FROM contacts "
            "WHERE organization IS NOT NULL "
            "AND (organization_normalized IS NULL OR organization_normalized = '')"
        )
    ).fetchall()

    updated = 0
    for cid, org in rows:
        norm = normalize_organization(org)
        if norm:
            conn.execute(
                sa.text(
                    "UPDATE contacts SET organization_normalized = :norm WHERE id = :id"
                ),
                {"norm": norm, "id": cid},
            )
            updated += 1
    print(f"[029] backfilled organization_normalized for {updated} contacts")

    # 3) Index for the grouped query (idempotent).
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_org_normalized "
        "ON contacts(organization_normalized)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contacts_org_normalized")
    op.drop_column("contacts", "organization_normalized")
