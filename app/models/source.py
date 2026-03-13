"""
Source model - stores websites we scrape for hotel leads
"""

from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Numeric, ARRAY
from sqlalchemy.dialects.postgresql import JSONB

from app.database import Base
from app.services.utils import local_now


class Source(Base):
    """Websites we scrape for hotel openings"""

    __tablename__ = "sources"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Source Information
    name = Column(String(100), nullable=False)
    base_url = Column(String(500), nullable=False, unique=True)
    source_type = Column(
        String(50), default="aggregator"
    )  # chain_newsroom, luxury_independent, aggregator, caribbean, florida, industry, travel_pub, pr_wire
    priority = Column(Integer, default=5)  # 1-10 (10 = highest priority)
    entry_urls = Column(ARRAY(Text))  # Multiple URLs for fallback/self-healing

    # Scraping Settings
    scrape_frequency = Column(String(20), default="daily")  # daily, weekly, monthly
    max_depth = Column(Integer, default=2)  # How deep to crawl links
    use_playwright = Column(Boolean, default=False)  # true = JS-heavy site
    is_active = Column(Boolean, default=True)

    # Statistics & Tracking
    last_scraped_at = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))  # Last successful scrape
    leads_found = Column(Integer, default=0)
    success_rate = Column(Numeric(5, 2), default=0.00)
    consecutive_failures = Column(Integer, default=0)  # For health monitoring

    # Gold URL Tracking
    gold_urls = Column(
        JSONB, default=dict
    )  # Audit Fix #7: callable, not mutable literal
    last_discovery_at = Column(DateTime(timezone=True))
    discovery_interval_days = Column(Integer, default=7)
    avg_lead_yield = Column(Numeric(5, 2), default=0.00)
    total_scrapes = Column(Integer, default=0)

    # Health Monitoring
    health_status = Column(
        String(20), default="new"
    )  # healthy, degraded, failing, dead, new

    # Notes
    notes = Column(Text)

    # Source Intelligence - adaptive learning data
    source_intelligence = Column(JSONB, default=dict)

    # Source Intelligence — adaptive learning data
    source_intelligence = Column(JSONB, default=dict)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: local_now())
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: local_now(),
        onupdate=lambda: local_now(),
    )

    def __repr__(self):
        return f"<Source(name='{self.name}', priority={self.priority}, health='{self.health_status}')>"

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
            "url": self.base_url,
            "base_url": self.base_url,
            "source_type": self.source_type,
            "priority": self.priority,
            "entry_urls": self.entry_urls,
            "scrape_frequency": self.scrape_frequency,
            "max_depth": self.max_depth,
            "use_playwright": self.use_playwright,
            "is_active": self.is_active,
            "last_scraped_at": self.last_scraped_at.isoformat()
            if self.last_scraped_at
            else None,
            "last_success_at": self.last_success_at.isoformat()
            if self.last_success_at
            else None,
            "leads_found": self.leads_found,
            "success_rate": float(self.success_rate) if self.success_rate else 0.0,
            "consecutive_failures": self.consecutive_failures,
            "health_status": self.health_status,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "gold_urls": self.gold_urls or {},
            "last_discovery_at": self.last_discovery_at.isoformat()
            if self.last_discovery_at
            else None,
            "discovery_interval_days": self.discovery_interval_days,
            "avg_lead_yield": float(self.avg_lead_yield)
            if self.avg_lead_yield
            else 0.0,
            "total_scrapes": self.total_scrapes or 0,
        }

    def record_success(self, leads_count: int = 0):
        """Record a successful scrape"""
        self.last_scraped_at = local_now()
        self.last_success_at = local_now()
        self.leads_found = (self.leads_found or 0) + leads_count
        self.consecutive_failures = 0
        self.health_status = "healthy"
        self._update_success_rate(True)

    def record_failure(self):
        """Record a failed scrape"""
        self.last_scraped_at = local_now()
        self.consecutive_failures = (self.consecutive_failures or 0) + 1
        self._update_success_rate(False)

        # Update health status based on consecutive failures
        if self.consecutive_failures >= 10:
            self.health_status = "dead"
        elif self.consecutive_failures >= 5:
            self.health_status = "failing"
        elif self.consecutive_failures >= 3:
            self.health_status = "degraded"

    def _update_success_rate(self, success: bool):
        """Update rolling success rate (simple approximation)"""
        current_rate = float(self.success_rate or 50.0)
        # Weighted moving average: 90% old + 10% new
        new_value = 100.0 if success else 0.0
        self.success_rate = (current_rate * 0.9) + (new_value * 0.1)
