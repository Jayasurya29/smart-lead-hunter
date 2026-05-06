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
"""

from alembic import op
import sqlalchemy as sa


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
    op.add_column(
        "potential_leads",
        sa.Column(
            "last_user_review_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_potential_leads_last_user_review_at",
        "potential_leads",
        ["last_user_review_at"],
    )

    # ── CRIT-3: hot-path indexes on potential_leads ──
    op.create_index(
        "ix_potential_leads_hotel_name_normalized",
        "potential_leads",
        ["hotel_name_normalized"],
    )
    op.create_index(
        "ix_potential_leads_timeline_label",
        "potential_leads",
        ["timeline_label"],
    )
    op.create_index(
        "ix_potential_leads_brand_tier",
        "potential_leads",
        ["brand_tier"],
    )
    op.create_index(
        "ix_potential_leads_zone",
        "potential_leads",
        ["zone"],
    )
    op.create_index(
        "ix_potential_leads_opening_year",
        "potential_leads",
        ["opening_year"],
    )

    # ── CRIT-3: hot-path indexes on existing_hotels ──
    op.create_index(
        "ix_existing_hotels_hotel_name_normalized",
        "existing_hotels",
        ["hotel_name_normalized"],
    )
    op.create_index(
        "ix_existing_hotels_brand_tier",
        "existing_hotels",
        ["brand_tier"],
    )
    op.create_index(
        "ix_existing_hotels_zone",
        "existing_hotels",
        ["zone"],
    )

    # ── HIGH-2 / HV-3: trigram + unaccent expression indexes ──
    # GIN trigram index on unaccent(lower(name)). Enables both
    # diacritic-insensitive ILIKE and similarity('a', 'b') queries to
    # use the index. Used by /leads + /api/existing-hotels search.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_potential_leads_hotel_name_unaccent_trgm
        ON potential_leads
        USING gin (unaccent(lower(hotel_name)) gin_trgm_ops)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_existing_hotels_hotel_name_unaccent_trgm
        ON existing_hotels
        USING gin (unaccent(lower(coalesce(hotel_name, name))) gin_trgm_ops)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_existing_hotels_hotel_name_unaccent_trgm")
    op.execute("DROP INDEX IF EXISTS ix_potential_leads_hotel_name_unaccent_trgm")
    op.drop_index("ix_existing_hotels_zone", table_name="existing_hotels")
    op.drop_index("ix_existing_hotels_brand_tier", table_name="existing_hotels")
    op.drop_index(
        "ix_existing_hotels_hotel_name_normalized", table_name="existing_hotels"
    )
    op.drop_index("ix_potential_leads_opening_year", table_name="potential_leads")
    op.drop_index("ix_potential_leads_zone", table_name="potential_leads")
    op.drop_index("ix_potential_leads_brand_tier", table_name="potential_leads")
    op.drop_index("ix_potential_leads_timeline_label", table_name="potential_leads")
    op.drop_index(
        "ix_potential_leads_hotel_name_normalized", table_name="potential_leads"
    )
    op.drop_index(
        "ix_potential_leads_last_user_review_at", table_name="potential_leads"
    )
    op.drop_column("potential_leads", "last_user_review_at")
    # Leave the unaccent / pg_trgm extensions in place. Other application
    # code may rely on them and dropping is destructive across DBs.
