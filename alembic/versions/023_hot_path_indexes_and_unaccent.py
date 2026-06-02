"""hot-path indexes + unaccent + pg_trgm + last_user_review_at

Created 2026-05-06 (audit fixes CRIT-3 + HIGH-2 / HV-3 + HV-4).

CRIT-3: Adds indexes on columns that are filtered, sorted, or used as
dedup keys on every save / list / map request:
  - potential_leads.hotel_name_normalized   — dedup exact-match
  - existing_hotels.hotel_name_normalized   — dedup exact-match + transfer
  - potential_leads.timeline_label          — Pipeline tab filter
  - potential_leads.brand_tier              — list filter
  - existing_hotels.brand_tier              — list filter
  - potential_leads.zone                    — list / map filter
  - existing_hotels.zone                    — list / map filter
  - potential_leads.opening_year            — year filter

HIGH-2 / HV-3: Enables `unaccent` and `pg_trgm` Postgres extensions and
adds expression indexes on `unaccent(lower(hotel_name))` so the search
filters in /leads and /api/existing-hotels can do diacritic-insensitive
matching ("café" finds "cafe") and trigram similarity matching ("Sandls"
still finds "Sandals").

HV-4: Adds `last_user_review_at` timestamp on potential_leads. Set by
the dashboard edit/approve/reject/restore handlers. Lets sales filter
"haven't reviewed in N days" leads — critical for the 6-month window.

Revision ID: 023
Revises: 022
Create Date: 2026-05-06

NOTE (2026-05-06 fix): Several indexes created here were already created
by migration 005 (ix_potential_leads_timeline_label, ix_potential_leads_brand_tier)
or by SQLAlchemy model-level index=True flags. All op.create_index() calls now
use if_not_exists=True and all op.drop_index() calls use if_exists=True to make
this migration fully idempotent.
"""

from alembic import op


revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Postgres extensions (HIGH-2 / HV-3) ──
    # Idempotent — IF NOT EXISTS keeps the migration safe to re-run on
    # any DB where the extension is already enabled (production has
    # pgvector enabled the same way).
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # ── HV-4: last_user_review_at on potential_leads ──
    # Use raw SQL so we get IF NOT EXISTS (Alembic's add_column has no
    # equivalent guard and would crash if the column already exists from
    # a prior create_all() or partial run).
    op.execute(
        "ALTER TABLE potential_leads "
        "ADD COLUMN IF NOT EXISTS last_user_review_at TIMESTAMP WITH TIME ZONE"
    )
    op.create_index(
        "ix_potential_leads_last_user_review_at",
        "potential_leads",
        ["last_user_review_at"],
        if_not_exists=True,
    )

    # ── CRIT-3: hot-path indexes on potential_leads ──
    # NOTE: ix_potential_leads_timeline_label and ix_potential_leads_brand_tier
    # were already created by migration 005. ix_potential_leads_hotel_name_normalized,
    # zone, and opening_year may exist from model index=True + prior create_all().
    # if_not_exists=True on all calls makes this safe regardless of DB history.
    op.create_index(
        "ix_potential_leads_hotel_name_normalized",
        "potential_leads",
        ["hotel_name_normalized"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_potential_leads_timeline_label",
        "potential_leads",
        ["timeline_label"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_potential_leads_brand_tier",
        "potential_leads",
        ["brand_tier"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_potential_leads_zone",
        "potential_leads",
        ["zone"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_potential_leads_opening_year",
        "potential_leads",
        ["opening_year"],
        if_not_exists=True,
    )

    # ── CRIT-3: hot-path indexes on existing_hotels ──
    op.create_index(
        "ix_existing_hotels_hotel_name_normalized",
        "existing_hotels",
        ["hotel_name_normalized"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_existing_hotels_brand_tier",
        "existing_hotels",
        ["brand_tier"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_existing_hotels_zone",
        "existing_hotels",
        ["zone"],
        if_not_exists=True,
    )

    # ── HIGH-2 / HV-3: trigram + unaccent expression indexes ──
    # PostgreSQL's built-in unaccent() is STABLE, not IMMUTABLE, so it
    # cannot be used directly in an index expression.  We create a thin
    # IMMUTABLE wrapper that delegates to unaccent() — this is the
    # standard PostgreSQL pattern for this problem.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION immutable_unaccent(text)
        RETURNS text AS $$
            SELECT unaccent($1)
        $$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
        """
    )

    # GIN trigram index on immutable_unaccent(lower(name)). Enables
    # diacritic-insensitive ILIKE and similarity() queries to hit the
    # index. Used by /leads + /api/existing-hotels search.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_potential_leads_hotel_name_unaccent_trgm
        ON potential_leads
        USING gin (immutable_unaccent(lower(hotel_name)) gin_trgm_ops)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_existing_hotels_hotel_name_unaccent_trgm
        ON existing_hotels
        USING gin (immutable_unaccent(lower(coalesce(hotel_name, name))) gin_trgm_ops)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_existing_hotels_hotel_name_unaccent_trgm")
    op.execute("DROP INDEX IF EXISTS ix_potential_leads_hotel_name_unaccent_trgm")
    op.execute("DROP FUNCTION IF EXISTS immutable_unaccent(text)")
    op.drop_index("ix_existing_hotels_zone", table_name="existing_hotels", if_exists=True)
    op.drop_index("ix_existing_hotels_brand_tier", table_name="existing_hotels", if_exists=True)
    op.drop_index(
        "ix_existing_hotels_hotel_name_normalized", table_name="existing_hotels", if_exists=True
    )
    op.drop_index("ix_potential_leads_opening_year", table_name="potential_leads", if_exists=True)
    op.drop_index("ix_potential_leads_zone", table_name="potential_leads", if_exists=True)
    op.drop_index("ix_potential_leads_brand_tier", table_name="potential_leads", if_exists=True)
    op.drop_index("ix_potential_leads_timeline_label", table_name="potential_leads", if_exists=True)
    op.drop_index(
        "ix_potential_leads_hotel_name_normalized", table_name="potential_leads", if_exists=True
    )
    op.drop_index(
        "ix_potential_leads_last_user_review_at", table_name="potential_leads", if_exists=True
    )
    # Use raw SQL for drop column to mirror the idempotent add above
    op.execute(
        "ALTER TABLE potential_leads DROP COLUMN IF EXISTS last_user_review_at"
    )
    # Leave the unaccent / pg_trgm extensions in place. Other application
    # code may rely on them and dropping is destructive across DBs.
