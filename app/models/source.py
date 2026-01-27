"""
Source model - stores websites we scrape for hotel leads
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Numeric, ARRAY
from datetime import datetime, timezone

from app.database import Base


class Source(Base):
    """Websites we scrape for hotel openings"""
    
    __tablename__ = "sources"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Source Information
    name = Column(String(100), nullable=False)
    base_url = Column(String(500), nullable=False)
    source_type = Column(String(50), default="aggregator")
    priority = Column(Integer, default=5)
    entry_urls = Column(ARRAY(Text))
    
    # Scraping Settings
    scrape_frequency = Column(String(20), default="daily")
    max_depth = Column(Integer, default=2)
    use_playwright = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    
    # Statistics
    last_scraped_at = Column(DateTime(timezone=True))
    leads_found = Column(Integer, default=0)
    success_rate = Column(Numeric(5, 2))
    
    # Notes
    notes = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    def __repr__(self):
        return f"<Source(name='{self.name}', active={self.is_active})>"
    
    # Property to support both 'url' and 'base_url' access
    @property
    def url(self):
        """Alias for base_url for backward compatibility"""
        return self.base_url
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "name": self.name,
            "url": self.base_url,  # API uses 'url'
            "base_url": self.base_url,
            "source_type": self.source_type,
            "priority": self.priority,
            "entry_urls": self.entry_urls,
            "scrape_frequency": self.scrape_frequency,
            "max_depth": self.max_depth,
            "use_playwright": self.use_playwright,
            "is_active": self.is_active,
            "last_scraped_at": str(self.last_scraped_at) if self.last_scraped_at else None,
            "leads_found": self.leads_found,
            "success_rate": float(self.success_rate) if self.success_rate else None,
            "notes": self.notes,
            "created_at": str(self.created_at) if self.created_at else None
        }