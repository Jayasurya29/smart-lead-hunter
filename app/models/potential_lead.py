"""
Potential Lead model - stores scraped leads before pushing to Insightly
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, Date, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from pgvector.sqlalchemy import Vector
from datetime import datetime
import uuid

from app.database import Base


class PotentialLead(Base):
    """Scraped leads waiting for review"""
    
    __tablename__ = "potential_leads"
    
    # Primary key
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Hotel Information
    hotel_name = Column(String(255), nullable=False)
    brand = Column(String(100))
    hotel_website = Column(String(500))
    
    # Location
    city = Column(String(100))
    state = Column(String(100))
    country = Column(String(100), default="USA")
    
    # Contact Information
    contact_first_name = Column(String(100))
    contact_last_name = Column(String(100))
    contact_title = Column(String(100))
    contact_email = Column(String(255))
    contact_phone = Column(String(50))
    
    # Hotel Details
    projected_opening_date = Column(Date)
    room_count = Column(Integer)
    description = Column(Text)
    
    # Scoring
    lead_score = Column(Integer, CheckConstraint('lead_score >= 0 AND lead_score <= 100'))
    score_breakdown = Column(JSONB)
    
    # Source Tracking
    source_id = Column(UUID(as_uuid=True))
    source_url = Column(Text, nullable=False)
    source_site = Column(String(100), nullable=False)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    
    # Workflow Status
    status = Column(String(20), default="New")
    claimed_by = Column(String(100))
    claimed_at = Column(DateTime)
    rejection_reason = Column(String(50))
    rejection_notes = Column(Text)
    
    # Insightly Sync
    insightly_lead_id = Column(Integer)
    pushed_to_insightly_at = Column(DateTime)
    
    # Deduplication
    embedding = Column(Vector(384))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<PotentialLead(hotel_name='{self.hotel_name}', status='{self.status}')>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": str(self.id),
            "hotel_name": self.hotel_name,
            "brand": self.brand,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "contact_first_name": self.contact_first_name,
            "contact_last_name": self.contact_last_name,
            "contact_title": self.contact_title,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "hotel_website": self.hotel_website,
            "projected_opening_date": str(self.projected_opening_date) if self.projected_opening_date else None,
            "room_count": self.room_count,
            "description": self.description,
            "lead_score": self.lead_score,
            "score_breakdown": self.score_breakdown,
            "source_url": self.source_url,
            "source_site": self.source_site,
            "status": self.status,
            "claimed_by": self.claimed_by,
            "rejection_reason": self.rejection_reason,
            "created_at": str(self.created_at) if self.created_at else None
        }