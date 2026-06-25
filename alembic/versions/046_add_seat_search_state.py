"""046: seat-search state on the vacated-seat ('former') affiliation.

Phase 3 v2 needs to remember what happened when it searched a vacated seat, so
it doesn't re-search forever and can distinguish outcomes:

    seat_status:
        NULL              -> never searched
        'filled'          -> a successor was found + filed
        'vacant'          -> seat is open / actively hiring (no current holder)
        'searched_unknown'-> searched, no clear holder found
        'ambiguous'       -> multi-location brand with no specific location to pin
    seat_searched_at      -> last time we ran the search
    seat_search_attempts  -> how many times we've tried (cap retries)

Stored on the 'former' affiliation row (which already represents the vacated
seat: account_name = old org, title = old role). All nullable, idempotent.
"""
from alembic import op

revision = "046"
down_revision = "045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE contact_affiliations "
        "ADD COLUMN IF NOT EXISTS seat_status text, "
        "ADD COLUMN IF NOT EXISTS seat_searched_at timestamptz, "
        "ADD COLUMN IF NOT EXISTS seat_search_attempts integer NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE contact_affiliations "
        "DROP CONSTRAINT IF EXISTS ck_affil_seat_status"
    )
    op.execute(
        "ALTER TABLE contact_affiliations "
        "ADD CONSTRAINT ck_affil_seat_status CHECK ("
        "seat_status IS NULL OR seat_status IN "
        "('filled','vacant','searched_unknown','ambiguous'))"
    )
    # Find seats due for a (re)search: former edges not yet resolved.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_affil_seat_unresolved "
        "ON contact_affiliations (person_type, person_id) "
        "WHERE relationship='former' AND seat_status IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_affil_seat_unresolved")
    op.execute("ALTER TABLE contact_affiliations DROP CONSTRAINT IF EXISTS ck_affil_seat_status")
    op.execute(
        "ALTER TABLE contact_affiliations "
        "DROP COLUMN IF EXISTS seat_search_attempts, "
        "DROP COLUMN IF EXISTS seat_searched_at, "
        "DROP COLUMN IF EXISTS seat_status"
    )
