"""
Scrape Log model - tracks history of scraping runs
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone

from app.database import Base


class ScrapeLog(Base):
    """History of scraping runs for monitoring"""
    
    __tablename__ = "scrape_logs"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Source reference
    source_id = Column(Integer, ForeignKey("sources.id"))
    
    # Timing
    started_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True))
    
    # Results
    urls_scraped = Column(Integer, default=0)
    pages_crawled = Column(Integer, default=0)
    leads_found = Column(Integer, default=0)
    leads_new = Column(Integer, default=0)
    leads_duplicate = Column(Integer, default=0)
    leads_skipped = Column(Integer, default=0)  # Skipped due to filters (budget, international)
    
    # Status
    status = Column(String(20), default="running")  # running, success, failed, partial
    error_message = Column(Text)  # Primary error message
    errors = Column(JSONB)  # Array of error details
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    def __repr__(self):
        return f"<ScrapeLog(id={self.id}, source_id={self.source_id}, status='{self.status}', leads={self.leads_found})>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "urls_scraped": self.urls_scraped,
            "pages_crawled": self.pages_crawled,
            "leads_found": self.leads_found,
            "leads_new": self.leads_new,
            "leads_duplicate": self.leads_duplicate,
            "leads_skipped": self.leads_skipped,
            "status": self.status,
            "error_message": self.error_message,
            "errors": self.errors
        }
    
    def mark_complete(self, leads_new: int = 0, leads_duplicate: int = 0, leads_skipped: int = 0):
        """Mark scrape as successfully completed"""
        self.completed_at = datetime.now(timezone.utc)
        self.status = "success"
        self.leads_new = leads_new
        self.leads_duplicate = leads_duplicate
        self.leads_skipped = leads_skipped
        self.leads_found = leads_new + leads_duplicate + leads_skipped
    
    def mark_failed(self, error_message: str, errors: list = None):
        """Mark scrape as failed"""
        self.completed_at = datetime.now(timezone.utc)
        self.status = "failed"
        self.error_message = error_message
        self.errors = errors
    
    def mark_partial(self, error_message: str, leads_new: int = 0):
        """Mark scrape as partially completed (some errors but got some leads)"""
        self.completed_at = datetime.now(timezone.utc)
        self.status = "partial"
        self.error_message = error_message
        self.leads_new = leads_new
        self.leads_found = leads_new