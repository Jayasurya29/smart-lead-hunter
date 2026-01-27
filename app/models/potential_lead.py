"""
Potential Lead model - stores scraped leads before pushing to Insightly
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, CheckConstraint
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
    hotel_name_normalized = Column(String(255))
    brand = Column(String(100))
    hotel_type = Column(String(50))
    hotel_website = Column(String(500))
    
    # Location
    city = Column(String(100))
    state = Column(String(100))
    country = Column(String(100), default="USA")
    
    # Contact Information
    contact_name = Column(String(200))
    contact_title = Column(String(100))
    contact_email = Column(String(255))
    contact_phone = Column(String(50))
    
    # Hotel Details
    opening_date = Column(String(50))
    room_count = Column(Integer)
    description = Column(Text)
    
    # Scoring
    lead_score = Column(Integer, CheckConstraint('lead_score >= 0 AND lead_score <= 100'))
    score_breakdown = Column(JSONB)
    
    # Source Tracking
    source_id = Column(Integer)
    source_url = Column(Text)
    source_site = Column(String(100))
    scraped_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Workflow Status
    status = Column(String(20), default="new")
    claimed_by = Column(String(100))
    claimed_at = Column(DateTime(timezone=True))
    notes = Column(Text)
    
    # Insightly Sync
    insightly_id = Column(Integer)
    synced_at = Column(DateTime(timezone=True))
    
    # Deduplication
    embedding = Column(Vector(384))
    duplicate_of_id = Column(Integer)
    
    # Raw data
    raw_data = Column(JSONB)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    def __repr__(self):
        return f"<PotentialLead(id={self.id}, hotel_name='{self.hotel_name}', status='{self.status}')>"