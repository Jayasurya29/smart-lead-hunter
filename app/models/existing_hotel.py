"""
Smart Lead Hunter — Existing Hotels Model
==========================================

Schema parity with potential_leads (migration 018, 2026-04-27).

Existing hotels are operating properties, not pipeline leads. The data
model is IDENTICAL to potential_leads with one exception:

  - timeline_label is omitted — meaningless for already-opened hotels.

Everything else (opening_date, opening_year, project_type, contacts,
score_breakdown, source provenance, embedding) is preserved because:

  - opening_date / opening_year are historical facts (when did it open).
  - project_type tells you HOW the hotel came to market (renovation,
    rebrand, new build) — useful for understanding the property forever.
  - All enrichment, scoring, and source-tracking fields work the same
    way as for potential_leads.

LEGACY COLUMNS (kept until migration 019)
-----------------------------------------
  name           — backed by hotel_name
  property_type  — backed by hotel_type
  website        — backed by hotel_website
  gm_name        — backed by contact_name
  gm_title       — backed by contact_title
  gm_email       — backed by contact_email
  gm_phone       — backed by contact_phone
  gm_linkedin    — DROPPED in code; lives in lead_contacts.linkedin per
                   contact (populated by Iter 4 of enrichment).

Migration 018 added the new columns and backfilled them. Old columns
remain populated and readable so old code keeps working. Migration 019
will drop them once all callers updated.

CONTACTS (post-migration 018)
------------------------------
lead_contacts.existing_hotel_id is a nullable FK alongside lead_id.
A contact row attaches to EITHER a potential_lead OR an existing_hotel,
never both (CHECK constraint enforces this in DB). Contact enrichment
(Iter 1-6 + Smart Fill) works identically for existing hotels — when
called with an existing_hotel_id, contacts are saved with
existing_hotel_id set and lead_id NULL.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
    Text,
    CheckConstraint,
    Numeric,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone

# L2 FIX: Guard pgvector import — same pattern as potential_lead.py
try:
    from pgvector.sqlalchemy import Vector
except ImportError:
    Vector = None

from app.database import Base


class ExistingHotel(Base):
    """Existing/operating hotels — full parity with potential_leads."""

    __tablename__ = "existing_hotels"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Hotel Information ─────────────────────────────────────────────
    hotel_name = Column(String(255), nullable=False, index=True)
    hotel_name_normalized = Column(String(255))
    brand = Column(String(150))
    chain = Column(String(150))  # Brand parent (Hilton Worldwide, Marriott Intl)
    brand_tier = Column(String(50))
    hotel_type = Column(String(50))  # resort, hotel, boutique, all-inclusive
    hotel_website = Column(String(500))

    # ── Location ──────────────────────────────────────────────────────
    address = Column(String(500))
    city = Column(String(100), index=True)
    state = Column(String(100), index=True)
    country = Column(String(100), default="USA")
    zip_code = Column(String(20))
    location_type = Column(String(20))  # florida/caribbean/usa/international
    latitude = Column(Float)
    longitude = Column(Float)
    zone = Column(String(50))  # South Florida, Orlando, Tampa Bay, etc.
    website_verified = Column(String(10))  # "auto" | "manual" | None

    # ── Contact Information ───────────────────────────────────────────
    # Single primary contact summary. Full contact graph lives in
    # lead_contacts table (existing_hotel_id FK).
    contact_name = Column(String(200))
    contact_title = Column(String(100))
    contact_email = Column(String(255))
    contact_phone = Column(String(50))

    # ── Hotel Details ─────────────────────────────────────────────────
    # Opening fields kept — historical facts that remain useful.
    # Only timeline_label is omitted (it's a "how soon will this open"
    # computed label that's meaningless for already-opened hotels).
    opening_date = Column(String(50))
    opening_year = Column(Integer)
    project_type = Column(String(30))
    room_count = Column(Integer)
    revenue_opening = Column(Float)
    revenue_annual = Column(Float)
    description = Column(Text)
    key_insights = Column(Text)

    # ── Stakeholders ──────────────────────────────────────────────────
    management_company = Column(String(200))  # Operator (Crescent, HEI, etc.)
    developer = Column(String(200))
    owner = Column(String(200))

    # ── Name intelligence ─────────────────────────────────────────────
    search_name = Column(String(255))
    former_names = Column(JSONB)

    # ── Scoring ───────────────────────────────────────────────────────
    lead_score = Column(
        Integer, CheckConstraint("lead_score >= 0 AND lead_score <= 100")
    )
    score_breakdown = Column(JSONB)
    estimated_revenue = Column(Integer)

    # ── Client Status (existing-only, SAP integration) ────────────────
    is_client = Column(Boolean, default=False, nullable=False, index=True)
    sap_bp_code = Column(String(20))
    client_notes = Column(Text)

    # ── Source Tracking ───────────────────────────────────────────────
    data_source = Column(String(50))  # sap_import / google_places / etc.
    source_id = Column(Integer, ForeignKey("sources.id"))
    source_url = Column(String(500))
    source_site = Column(String(100))
    source_urls = Column(JSONB, default=[])
    source_extractions = Column(JSONB, default={})
    scraped_at = Column(DateTime(timezone=True))
    last_verified_at = Column(DateTime(timezone=True))

    # ── Workflow Status ───────────────────────────────────────────────
    status = Column(String(20), default="new", index=True)
    claimed_by = Column(String(100))
    claimed_at = Column(DateTime(timezone=True))
    rejection_reason = Column(String(100))
    notes = Column(Text)

    # ── Insightly CRM Sync ────────────────────────────────────────────
    insightly_id = Column(Integer)
    insightly_lead_ids = Column(JSONB, default=list)
    synced_at = Column(DateTime(timezone=True))
    sync_error = Column(Text)

    # ── Deduplication ─────────────────────────────────────────────────
    embedding = Column(Vector(384)) if Vector is not None else Column(Text)
    duplicate_of_id = Column(Integer, ForeignKey("existing_hotels.id"))
    similarity_score = Column(Numeric(5, 4))

    # ── Atlist Map Integration (existing-only) ────────────────────────
    atlist_marker_id = Column(String(100))
    pushed_to_map = Column(Boolean, default=False)
    pushed_at = Column(DateTime(timezone=True))

    # ── Raw extraction snapshot ───────────────────────────────────────
    raw_data = Column(JSONB)

    # ── Timestamps ────────────────────────────────────────────────────
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # ═══════════════════════════════════════════════════════════════════
    # LEGACY COLUMNS (will be dropped in migration 019)
    # ═══════════════════════════════════════════════════════════════════
    # These exist in the DB and are still readable, but new code should
    # use the canonical names above. Backfill in migration 018 ensures
    # the new columns mirror these for every existing row.
    name = Column(String(300))  # → hotel_name
    property_type = Column(String(50))  # → hotel_type
    website = Column(String(500))  # → hotel_website
    gm_name = Column(String(200))  # → contact_name
    gm_title = Column(String(200))  # → contact_title
    gm_email = Column(String(255))  # → contact_email
    gm_phone = Column(String(50))  # → contact_phone
    gm_linkedin = Column(String(500))  # DROPPED — see lead_contacts.linkedin

    def __repr__(self):
        client_tag = " [CLIENT]" if self.is_client else ""
        return (
            f"<ExistingHotel(id={self.id}, hotel_name='{self.hotel_name}', "
            f"city='{self.city}'{client_tag})>"
        )

    def to_dict(self):
        """Convert to dict for API responses. Uses canonical field names."""
        return {
            "id": self.id,
            # Hotel info
            "hotel_name": self.hotel_name,
            "hotel_name_normalized": self.hotel_name_normalized,
            "brand": self.brand,
            "chain": self.chain,
            "brand_tier": self.brand_tier,
            "hotel_type": self.hotel_type,
            "hotel_website": self.hotel_website,
            # Location
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "zip_code": self.zip_code,
            "location_type": self.location_type,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "zone": self.zone,
            "website_verified": self.website_verified,
            # Contact
            "contact_name": self.contact_name,
            "contact_title": self.contact_title,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            # Hotel details
            "opening_date": self.opening_date,
            "opening_year": self.opening_year,
            "project_type": self.project_type,
            "room_count": self.room_count,
            "revenue_opening": self.revenue_opening,
            "revenue_annual": self.revenue_annual,
            "description": self.description,
            "key_insights": self.key_insights,
            # Stakeholders
            "management_company": self.management_company,
            "developer": self.developer,
            "owner": self.owner,
            # Name intelligence
            "search_name": self.search_name,
            "former_names": self.former_names,
            # Scoring
            "lead_score": self.lead_score,
            "score_breakdown": self.score_breakdown,
            "estimated_revenue": self.estimated_revenue,
            # Client status
            "is_client": self.is_client,
            "sap_bp_code": self.sap_bp_code,
            "client_notes": self.client_notes,
            # Source
            "data_source": self.data_source,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "source_site": self.source_site,
            "source_urls": self.source_urls,
            "source_extractions": self.source_extractions,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "last_verified_at": (
                self.last_verified_at.isoformat() if self.last_verified_at else None
            ),
            # Workflow
            "status": self.status,
            "claimed_by": self.claimed_by,
            "claimed_at": self.claimed_at.isoformat() if self.claimed_at else None,
            "rejection_reason": self.rejection_reason,
            "notes": self.notes,
            # Insightly
            "insightly_id": self.insightly_id,
            "insightly_lead_ids": self.insightly_lead_ids,
            "synced_at": self.synced_at.isoformat() if self.synced_at else None,
            # Atlist
            "atlist_marker_id": self.atlist_marker_id,
            "pushed_to_map": self.pushed_to_map,
            "pushed_at": self.pushed_at.isoformat() if self.pushed_at else None,
            # Timestamps
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
