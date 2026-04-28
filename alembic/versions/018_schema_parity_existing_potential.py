"""schema_parity_existing_potential

Revision ID: 018
Revises: 017

Goal: existing_hotels schema = potential_leads schema, identical, EXCEPT
no timeline_label (which is meaningless for an already-opened hotel).

Everything else stays in both — opening_date, opening_year, project_type
are historical facts that remain useful after opening (when did the hotel
open, was it new build / renovation / conversion). The "timeline_label"
field is the only timing-specific computation that doesn't apply to open
hotels.

Approach: ADD new columns matching potential_leads names, BACKFILL data
from existing's old column names where applicable, but DON'T drop the
old columns yet. That way:

  - Old code keeps working (reads `existing.name`, `existing.gm_email`)
  - New code can use canonical names (`hotel_name`, `contact_email`)
  - A future migration 019 drops the old columns once all callers updated

Field renames (existing → potential canonical):
  name           → hotel_name
  property_type  → hotel_type
  website        → hotel_website
  gm_name        → contact_name
  gm_title       → contact_title
  gm_email       → contact_email
  gm_phone       → contact_phone
  gm_linkedin    → DROPPED (LinkedIn now lives in lead_contacts.linkedin
                  per-contact; populated by Iter 4 of enrichment)

New columns added to existing_hotels (parity with potential_leads):
  hotel_name_normalized, location_type, website_verified, description,
  key_insights, management_company, developer, owner, search_name,
  former_names, score_breakdown, estimated_revenue, source_id,
  source_site, source_urls, source_extractions, scraped_at, claimed_by,
  claimed_at, notes, insightly_lead_ids, synced_at, sync_error,
  embedding (if pgvector), duplicate_of_id, similarity_score, raw_data,
  opening_date, opening_year, project_type

Symmetry — added to potential_leads (existing already had these):
  zone           Territory routing for map page (was existing-only).
  chain          Brand parent company (Hilton Worldwide). Distinct from
                 brand (Hilton) and management_company (operator).
  data_source    Pipeline that created the row ("manual", "google_places",
                 "chain_directory", "scraping_run"). Distinct from
                 source_site (article domain).

Dropped from existing_hotels:
  phone          General property/front-desk phone number. Redundant
                 with contact_phone (primary contact's phone) and
                 lead_contacts.phone (per-contact). Nothing in the
                 sales workflow used the front-desk number.

lead_contacts dual FK:
  Add existing_hotel_id (nullable). Relax lead_id NOT NULL. Add CHECK
  enforcing exactly one of (lead_id, existing_hotel_id) is set. This
  enables contact enrichment on existing hotels — same Iter 1-6 pipeline,
  same scoring, same Smart Fill, just without timeline-based queries.

All new columns are NULLABLE. Migration is non-breaking. Existing data
preserved exactly. Backfill copies values into new columns so the new
fields are populated for every existing row.

Create Date: 2026-04-27
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = '018'
down_revision: Union[str, None] = '017'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_pgvector(bind) -> bool:
    """Match the L2 pgvector availability pattern from potential_lead.py."""
    try:
        result = bind.execute(
            sa.text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        ).fetchone()
        return result is not None
    except Exception:
        return False


def upgrade() -> None:
    bind = op.get_bind()
    has_vector = _has_pgvector(bind)

    # ═════════════════════════════════════════════════════════════════
    # 1. EXISTING_HOTELS — add all potential_leads columns
    # ═════════════════════════════════════════════════════════════════

    # ── Renames (add new column, backfill, leave old in place) ──
    # Old columns stay populated and readable. New code uses new names.
    # Migration 019 (later) will drop the old columns once callers updated.

    op.add_column(
        'existing_hotels',
        sa.Column('hotel_name', sa.String(255), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('hotel_type', sa.String(50), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('hotel_website', sa.String(500), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('contact_name', sa.String(200), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('contact_title', sa.String(100), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('contact_email', sa.String(255), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('contact_phone', sa.String(50), nullable=True),
    )

    # ── Backfill renamed fields from old columns ──
    op.execute("""
        UPDATE existing_hotels SET
            hotel_name     = name,
            hotel_type     = property_type,
            hotel_website  = website,
            contact_name   = gm_name,
            contact_title  = gm_title,
            contact_email  = gm_email,
            contact_phone  = gm_phone
        WHERE hotel_name IS NULL
    """)

    # Now make hotel_name NOT NULL (matches potential_leads.hotel_name)
    op.alter_column('existing_hotels', 'hotel_name', nullable=False)
    op.create_index(
        'ix_existing_hotels_hotel_name',
        'existing_hotels',
        ['hotel_name'],
    )

    # ── New parity columns (no rename, just additions) ──
    op.add_column(
        'existing_hotels',
        sa.Column('hotel_name_normalized', sa.String(255), nullable=True),
    )
    op.create_index(
        'ix_existing_hotels_hotel_name_normalized',
        'existing_hotels',
        ['hotel_name_normalized'],
    )

    op.add_column(
        'existing_hotels',
        sa.Column('location_type', sa.String(20), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('website_verified', sa.String(10), nullable=True),
    )

    # Opening / project — kept (per user: "we need all details like
    # opening date and all stuff"). Only timeline_label is dropped.
    op.add_column(
        'existing_hotels',
        sa.Column('opening_date', sa.String(50), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('opening_year', sa.Integer, nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('project_type', sa.String(30), nullable=True),
    )

    # Description + insights
    op.add_column(
        'existing_hotels',
        sa.Column('description', sa.Text, nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('key_insights', sa.Text, nullable=True),
    )

    # Stakeholders
    # `chain` already exists on existing_hotels (brand parent like
    # "Hilton Worldwide"). It's added to potential_leads below for parity.
    # `management_company` is the operator (Crescent, New Waterloo, HEI) —
    # different concept from `chain`. Both kept.
    op.add_column(
        'existing_hotels',
        sa.Column('management_company', sa.String(200), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('developer', sa.String(200), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('owner', sa.String(200), nullable=True),
    )

    # Name intelligence
    op.add_column(
        'existing_hotels',
        sa.Column('search_name', sa.String(255), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column(
            'former_names',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )

    # Scoring detail
    op.add_column(
        'existing_hotels',
        sa.Column(
            'score_breakdown',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('estimated_revenue', sa.Integer, nullable=True),
    )

    # Source provenance — matches potential_leads exactly.
    # `source_url` already exists on existing_hotels (single URL).
    # `source_urls` (plural, JSONB list) is the accumulated history.
    op.add_column(
        'existing_hotels',
        sa.Column(
            'source_id', sa.Integer, sa.ForeignKey('sources.id'), nullable=True
        ),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('source_site', sa.String(100), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column(
            'source_urls',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        'existing_hotels',
        sa.Column(
            'source_extractions',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('scraped_at', sa.DateTime(timezone=True), nullable=True),
    )

    # Workflow ownership (claim/release pattern)
    op.add_column(
        'existing_hotels',
        sa.Column('claimed_by', sa.String(100), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('claimed_at', sa.DateTime(timezone=True), nullable=True),
    )

    # General notes — distinct from existing's `client_notes` (SAP-specific).
    # If `client_notes` has any content, copy it into `notes` so users
    # don't lose context when looking at the new field.
    op.add_column(
        'existing_hotels',
        sa.Column('notes', sa.Text, nullable=True),
    )
    op.execute("""
        UPDATE existing_hotels SET notes = client_notes
        WHERE notes IS NULL AND client_notes IS NOT NULL
    """)

    # Insightly CRM sync
    op.add_column(
        'existing_hotels',
        sa.Column(
            'insightly_lead_ids',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('synced_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('sync_error', sa.Text, nullable=True),
    )

    # Dedup
    op.add_column(
        'existing_hotels',
        sa.Column(
            'duplicate_of_id',
            sa.Integer,
            sa.ForeignKey('existing_hotels.id'),
            nullable=True,
        ),
    )
    op.add_column(
        'existing_hotels',
        sa.Column('similarity_score', sa.Numeric(5, 4), nullable=True),
    )

    # Raw extraction snapshot
    op.add_column(
        'existing_hotels',
        sa.Column(
            'raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )

    # pgvector embedding for cross-table dedup
    if has_vector:
        op.execute('CREATE EXTENSION IF NOT EXISTS vector')
        op.execute(
            'ALTER TABLE existing_hotels ADD COLUMN embedding vector(384)'
        )

    # ═════════════════════════════════════════════════════════════════
    # 2. POTENTIAL_LEADS — symmetry additions
    # ═════════════════════════════════════════════════════════════════
    # Closing the gap the other way. These fields existed on existing_hotels
    # but were missing from potential_leads. Now they live on both.
    op.add_column(
        'potential_leads', sa.Column('zone', sa.String(50), nullable=True)
    )
    op.add_column(
        'potential_leads', sa.Column('chain', sa.String(150), nullable=True)
    )
    op.add_column(
        'potential_leads', sa.Column('data_source', sa.String(50), nullable=True)
    )

    # ── Drop existing_hotels.phone ──
    # `phone` (general property phone, front desk) is redundant. Contact
    # phone numbers belong per-contact in lead_contacts.phone, or as the
    # primary contact_phone on the lead row. Dropping here so the schema
    # stays minimal and parallel between the two tables.
    op.drop_column('existing_hotels', 'phone')

    # ═════════════════════════════════════════════════════════════════
    # 3. LEAD_CONTACTS — dual FK so contacts attach to either parent
    # ═════════════════════════════════════════════════════════════════
    op.add_column(
        'lead_contacts',
        sa.Column(
            'existing_hotel_id',
            sa.Integer,
            sa.ForeignKey('existing_hotels.id', ondelete='CASCADE'),
            nullable=True,
        ),
    )
    op.create_index(
        'ix_lead_contacts_existing_hotel_id',
        'lead_contacts',
        ['existing_hotel_id'],
    )

    # Relax lead_id NOT NULL — contacts can now belong to either parent.
    op.alter_column('lead_contacts', 'lead_id', nullable=True)

    # CHECK: exactly one parent FK is set. Enforced in DB, not just app code.
    op.create_check_constraint(
        'ck_lead_contacts_exactly_one_parent',
        'lead_contacts',
        '(lead_id IS NOT NULL AND existing_hotel_id IS NULL) OR '
        '(lead_id IS NULL AND existing_hotel_id IS NOT NULL)',
    )


def downgrade() -> None:
    bind = op.get_bind()
    has_vector = _has_pgvector(bind)

    # 3. lead_contacts
    op.drop_constraint(
        'ck_lead_contacts_exactly_one_parent', 'lead_contacts', type_='check'
    )
    op.alter_column('lead_contacts', 'lead_id', nullable=False)
    op.drop_index(
        'ix_lead_contacts_existing_hotel_id', table_name='lead_contacts'
    )
    op.drop_column('lead_contacts', 'existing_hotel_id')

    # 2. potential_leads symmetry
    op.drop_column('potential_leads', 'data_source')
    op.drop_column('potential_leads', 'chain')
    op.drop_column('potential_leads', 'zone')

    # ── Restore existing_hotels.phone (was dropped in upgrade) ──
    op.add_column(
        'existing_hotels', sa.Column('phone', sa.String(50), nullable=True)
    )

    # 1. existing_hotels
    if has_vector:
        op.execute(
            'ALTER TABLE existing_hotels DROP COLUMN IF EXISTS embedding'
        )

    op.drop_column('existing_hotels', 'raw_data')
    op.drop_column('existing_hotels', 'similarity_score')
    op.drop_column('existing_hotels', 'duplicate_of_id')
    op.drop_column('existing_hotels', 'sync_error')
    op.drop_column('existing_hotels', 'synced_at')
    op.drop_column('existing_hotels', 'insightly_lead_ids')
    op.drop_column('existing_hotels', 'notes')
    op.drop_column('existing_hotels', 'claimed_at')
    op.drop_column('existing_hotels', 'claimed_by')
    op.drop_column('existing_hotels', 'scraped_at')
    op.drop_column('existing_hotels', 'source_extractions')
    op.drop_column('existing_hotels', 'source_urls')
    op.drop_column('existing_hotels', 'source_site')
    op.drop_column('existing_hotels', 'source_id')
    op.drop_column('existing_hotels', 'estimated_revenue')
    op.drop_column('existing_hotels', 'score_breakdown')
    op.drop_column('existing_hotels', 'former_names')
    op.drop_column('existing_hotels', 'search_name')
    op.drop_column('existing_hotels', 'owner')
    op.drop_column('existing_hotels', 'developer')
    op.drop_column('existing_hotels', 'management_company')
    op.drop_column('existing_hotels', 'key_insights')
    op.drop_column('existing_hotels', 'description')
    op.drop_column('existing_hotels', 'project_type')
    op.drop_column('existing_hotels', 'opening_year')
    op.drop_column('existing_hotels', 'opening_date')
    op.drop_column('existing_hotels', 'website_verified')
    op.drop_column('existing_hotels', 'location_type')

    op.drop_index(
        'ix_existing_hotels_hotel_name_normalized', table_name='existing_hotels'
    )
    op.drop_column('existing_hotels', 'hotel_name_normalized')

    # Rename-back columns
    op.drop_index(
        'ix_existing_hotels_hotel_name', table_name='existing_hotels'
    )
    op.drop_column('existing_hotels', 'contact_phone')
    op.drop_column('existing_hotels', 'contact_email')
    op.drop_column('existing_hotels', 'contact_title')
    op.drop_column('existing_hotels', 'contact_name')
    op.drop_column('existing_hotels', 'hotel_website')
    op.drop_column('existing_hotels', 'hotel_type')
    op.drop_column('existing_hotels', 'hotel_name')
