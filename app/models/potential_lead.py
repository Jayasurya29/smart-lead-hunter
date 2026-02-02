"""
Potential Lead model - stores scraped leads before pushing to Insightly
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, CheckConstraint, Numeric, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
from datetime import datetime, timezone

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
    brand_tier = Column(String(20))  # tier1_ultra_luxury, tier2_luxury, tier3_upper_upscale, tier4_upscale, tier5_skip
    hotel_type = Column(String(50))  # resort, hotel, boutique, all-inclusive
    hotel_website = Column(String(500))
    
    # Location
    city = Column(String(100))
    state = Column(String(100))
    country = Column(String(100), default="USA")
    location_type = Column(String(20))  # florida, caribbean, usa, international
    
    # Contact Information
    contact_name = Column(String(200))
    contact_title = Column(String(100))
    contact_email = Column(String(255))
    contact_phone = Column(String(50))
    
    # Hotel Details
    opening_date = Column(String(50))  # Flexible: "Q2 2026", "June 2026", "2026"
    opening_year = Column(Integer)  # Extracted year for filtering
    room_count = Column(Integer)
    description = Column(Text)
    
    # Scoring (0-100)
    lead_score = Column(Integer, CheckConstraint('lead_score >= 0 AND lead_score <= 100'))
    score_breakdown = Column(JSONB)  # {"location": 30, "brand": 25, "timing": 20, ...}
    estimated_revenue = Column(Integer)  # Estimated uniform revenue in dollars
    
    # Source Tracking
    source_id = Column(Integer, ForeignKey('sources.id'))
    source_url = Column(Text)
    source_site = Column(String(100))
    scraped_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Workflow Status
    status = Column(String(20), default="new")  # new, claimed, approved, rejected, pushed
    claimed_by = Column(String(100))
    claimed_at = Column(DateTime(timezone=True))
    rejection_reason = Column(String(100))  # duplicate, budget_brand, international, old_opening, bad_data
    notes = Column(Text)
    
    # Insightly CRM Sync
    insightly_id = Column(Integer)
    synced_at = Column(DateTime(timezone=True))
    sync_error = Column(Text)
    
    # Deduplication
    embedding = Column(Vector(384))  # For semantic similarity
    duplicate_of_id = Column(Integer, ForeignKey('potential_leads.id'))
    similarity_score = Column(Numeric(5, 4))  # How similar to duplicate (0.0-1.0)
    
    # Raw data
    raw_data = Column(JSONB)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    def __repr__(self):
        return f"<PotentialLead(id={self.id}, hotel_name='{self.hotel_name}', score={self.lead_score}, status='{self.status}')>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "hotel_name": self.hotel_name,
            "brand": self.brand,
            "brand_tier": self.brand_tier,
            "hotel_type": self.hotel_type,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "location_type": self.location_type,
            "opening_date": self.opening_date,
            "opening_year": self.opening_year,
            "room_count": self.room_count,
            "lead_score": self.lead_score,
            "score_breakdown": self.score_breakdown,
            "contact_name": self.contact_name,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "status": self.status,
            "source_url": self.source_url,
            "source_site": self.source_site,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }