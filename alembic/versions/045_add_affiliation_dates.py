"""045: add dated tenure to contact_affiliations (title, start_date, end_date).

Foundation for coverage-freshness Phase 2 (follow the person). Lets a contact's
work history be stored as dated role rows instead of a single org string:

    org                    start_date   end_date     => meaning
    Great Wolf Lodge       2024-11-01   2026-03-01   => FORMER (ended)
    The Tides Inn          2022-05-01   2024-05-01   => FORMER (ended)
    Marriott Downtown      2026-04-01   NULL         => CURRENT (open / "Present")

Semantic: end_date IS NULL  <=>  the role is current/open. When every found role
has an end_date, the person's current employer is UNKNOWN (do not guess).

All columns nullable; existing rows are untouched. Idempotent (IF NOT EXISTS).
"""
from alembic import op

revision = "045"
down_revision = "044"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE contact_affiliations "
        "ADD COLUMN IF NOT EXISTS title text, "
        "ADD COLUMN IF NOT EXISTS start_date date, "
        "ADD COLUMN IF NOT EXISTS end_date date"
    )
    # Partial index: quickly find a person's CURRENT (open) roles.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_affiliations_open "
        "ON contact_affiliations (person_type, person_id) "
        "WHERE end_date IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_affiliations_open")
    op.execute(
        "ALTER TABLE contact_affiliations "
        "DROP COLUMN IF EXISTS end_date, "
        "DROP COLUMN IF EXISTS start_date, "
        "DROP COLUMN IF EXISTS title"
    )
