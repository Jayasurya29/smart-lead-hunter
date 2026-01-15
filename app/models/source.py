"""
Source model - stores websites we scrape for hotel leads
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Numeric
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from app.database import Base


class Source(Base):
    """Websites we scrape for hotel openings"""
    
    __tablename__ = "sources"
    
    # Primary key
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Source Information
    name = Column(String(100), nullable=False)
    url = Column(String(500), nullable=False)
    
    # Scraping Settings
    scrape_frequency = Column(String(20), default="daily")  # daily, weekly, hourly
    is_active = Column(Boolean, default=True)
    
    # Statistics
    last_scraped_at = Column(DateTime)
    leads_found = Column(Integer, default=0)
    success_rate = Column(Numeric(5, 2))  # e.g., 95.50%
    
    # Notes
    notes = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<Source(name='{self.name}', active={self.is_active})>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": str(self.id),
            "name": self.name,
            "url": self.url,
            "scrape_frequency": self.scrape_frequency,
            "is_active": self.is_active,
            "last_scraped_at": str(self.last_scraped_at) if self.last_scraped_at else None,
            "leads_found": self.leads_found,
            "success_rate": float(self.success_rate) if self.success_rate else None,
            "notes": self.notes,
            "created_at": str(self.created_at) if self.created_at else None
        }