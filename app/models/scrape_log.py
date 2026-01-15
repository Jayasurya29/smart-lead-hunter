"""
Scrape Log model - tracks history of scraping runs
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from app.database import Base


class ScrapeLog(Base):
    """History of scraping runs for monitoring"""
    
    __tablename__ = "scrape_logs"
    
    # Primary key
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Source reference
    source_id = Column(UUID(as_uuid=True), ForeignKey("sources.id"))
    
    # Timing
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime)
    
    # Results
    pages_crawled = Column(Integer, default=0)
    leads_found = Column(Integer, default=0)
    leads_new = Column(Integer, default=0)
    leads_duplicate = Column(Integer, default=0)
    
    # Status
    status = Column(String(20), default="running")  # running, completed, failed
    errors = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<ScrapeLog(source_id='{self.source_id}', status='{self.status}')>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": str(self.id),
            "source_id": str(self.source_id) if self.source_id else None,
            "started_at": str(self.started_at) if self.started_at else None,
            "completed_at": str(self.completed_at) if self.completed_at else None,
            "pages_crawled": self.pages_crawled,
            "leads_found": self.leads_found,
            "leads_new": self.leads_new,
            "leads_duplicate": self.leads_duplicate,
            "status": self.status,
            "errors": self.errors
        }