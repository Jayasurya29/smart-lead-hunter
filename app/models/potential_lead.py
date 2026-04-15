"""
Potential Lead model - stores scraped leads before pushing to Insightly
"""

from sqlalchemy import (
    Column,
    Float,
    String,
    Integer,
    DateTime,
    Text,
    CheckConstraint,
    Numeric,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone

# L2 FIX: Guard pgvector import — makes it optional for dev/test environments
try:
    from pgvector.sqlalchemy import Vector
except ImportError:
    Vector = None

from app.database import Base


class PotentialLead(Base):
    """Scraped leads waiting for review"""

    __tablename__ = "potential_leads"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Hotel Information
    hotel_name = Column(String(255), nullable=False)
    hotel_name_normalized = Column(String(255))  # Lowercase, no special chars for dedup
    brand = Column(String(100))
    brand_tier = Column(
        String(20)
    )  # tier1_ultra_luxury, tier2_luxury, tier3_upper_upscale, tier4_upscale, tier5_skip
    hotel_type = Column(String(50))  # resort, hotel, boutique, all-inclusive
    hotel_website = Column(String(500))

    # Location
    city = Column(String(100))
    state = Column(String(100))
    country = Column(String(100), default="USA")
    location_type = Column(String(20))  # florida, caribbean, usa, international

    # Geocoordinates (for map display)
    latitude = Column(Float)
    longitude = Column(Float)
    website_verified = Column(String(10))  # "auto" | "manual" | None

    # Contact Information
    contact_name = Column(String(200))
    contact_title = Column(String(100))
    contact_email = Column(String(255))
    contact_phone = Column(String(50))

    # Hotel Details
    opening_date = Column(String(50))  # Flexible: "Q2 2026", "June 2026", "2026"
    opening_year = Column(Integer)  # Extracted year for filtering
    timeline_label = Column(
        String(10)
    )  # HOT, URGENT, WARM, COOL, LATE, EXPIRED, TBD (computed on save/rescore)
    room_count = Column(Integer)
    revenue_opening = Column(Float)  # Pre-calculated: new opening JA addressable
    revenue_annual = Column(Float)  # Pre-calculated: annual recurring JA addressable
    description = Column(Text)

    # Key Insights - IMPORTANT for sales team!
    key_insights = Column(Text)  # Bullet points of important info

    # Stakeholders
    management_company = Column(String(200))
    developer = Column(String(200))
    owner = Column(String(200))

    # Scoring (0-100)
    lead_score = Column(
        Integer, CheckConstraint("lead_score >= 0 AND lead_score <= 100")
    )
    score_breakdown = Column(JSONB)  # {"location": 30, "brand": 25, "timing": 20, ...}
    estimated_revenue = Column(Integer)  # Estimated uniform revenue in dollars

    # Source Tracking
    source_id = Column(Integer, ForeignKey("sources.id"))
    source_url = Column(Text)
    source_site = Column(String(100))
    source_urls = Column(JSONB, default=[])
    source_extractions = Column(JSONB, default={})
    scraped_at = Column(DateTime(timezone=True))

    # Workflow Status
    status = Column(
        String(20), default="new", index=True
    )  # new, claimed, approved, rejected, pushed
    claimed_by = Column(String(100))
    claimed_at = Column(DateTime(timezone=True))
    rejection_reason = Column(
        String(100)
    )  # duplicate, budget_brand, international, old_opening, bad_data
    notes = Column(Text)

    # Insightly CRM Sync
    insightly_id = Column(Integer)  # First/primary Insightly Lead ID (legacy compat)
    insightly_lead_ids = Column(JSONB, default=list)  # ALL pushed Insightly Lead IDs
    synced_at = Column(DateTime(timezone=True))
    sync_error = Column(Text)

    # Deduplication — L2 FIX: Only create Vector column if pgvector is installed
    embedding = Column(Vector(384)) if Vector is not None else Column(Text)
    duplicate_of_id = Column(Integer, ForeignKey("potential_leads.id"))
    similarity_score = Column(Numeric(5, 4))  # How similar to duplicate (0.0-1.0)

    # Raw data
    raw_data = Column(JSONB)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        return f"<PotentialLead(id={self.id}, hotel_name='{self.hotel_name}', score={self.lead_score}, status='{self.status}')>"

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            # Identity
            "id": self.id,
            "hotel_name": self.hotel_name,
            "brand": self.brand,
            "brand_tier": self.brand_tier,
            "hotel_type": self.hotel_type,
            "hotel_website": self.hotel_website,
            # Location
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "location_type": self.location_type,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "website_verified": self.website_verified,
            # Project Details
            "opening_date": self.opening_date,
            "opening_year": self.opening_year,
            "timeline_label": self.timeline_label,
            "room_count": self.room_count,
            "description": self.description,
            # Key Insights - THE IMPORTANT STUFF!
            "key_insights": self.key_insights,
            # Stakeholders
            "management_company": self.management_company,
            "developer": self.developer,
            "owner": self.owner,
            # Contact
            "contact_name": self.contact_name,
            "contact_title": self.contact_title,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            # Scoring
            "lead_score": self.lead_score,
            "score_breakdown": self.score_breakdown,
            "estimated_revenue": self.estimated_revenue,
            "revenue_opening": self.revenue_opening,
            "revenue_annual": self.revenue_annual,
            # Source
            "source_url": self.source_url,
            "source_site": self.source_site,
            "source_id": self.source_id,
            # Workflow
            "status": self.status,
            "claimed_by": self.claimed_by,
            "rejection_reason": self.rejection_reason,
            "notes": self.notes,
            # CRM
            "insightly_id": self.insightly_id,
            # Timestamps
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
        }
