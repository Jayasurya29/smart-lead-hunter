"""047: trigram (pg_trgm) GIN indexes for fast fuzzy contact search.

Server-side search matches contacts by fuzzy similarity (typo-tolerant, e.g.
"jasom rodrigeez" -> "Jason Rodriguez") AND exact substring, across name/email/
org/title/role/company. Without trigram indexes, similarity() over ~43k rows is
a full scan on every keystroke. These GIN indexes on gin_trgm_ops make both the
`%` similarity operator and ILIKE '%term%' fast.

pg_trgm is already installed (verified). Indexes are created IF NOT EXISTS and
the migration is idempotent. CONCURRENTLY is NOT used (Alembic runs in a txn);
these build quickly on 43k rows.
"""
from alembic import op

revision = "047"
down_revision = "046"
branch_labels = None
depends_on = None


# Columns we fuzzy/substring search. One GIN trigram index each.
_TRGM_COLS = [
    "display_name",
    "first_name",
    "last_name",
    "organization",
    "title",
    "email",
    "inferred_role",
    "management_company",
    "parent_company",
]


def upgrade() -> None:
    # Safety: ensure the extension is present (no-op if already installed).
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    for col in _TRGM_COLS:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS ix_contacts_{col}_trgm "
            f"ON contacts USING gin ({col} gin_trgm_ops)"
        )


def downgrade() -> None:
    for col in _TRGM_COLS:
        op.execute(f"DROP INDEX IF EXISTS ix_contacts_{col}_trgm")
