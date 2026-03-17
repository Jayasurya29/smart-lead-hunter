"""
Smart Lead Hunter — Pydantic Request/Response Schemas
"""

from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, ConfigDict


class LeadBase(BaseModel):
    """Base lead schema"""

    hotel_name: str
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_name: Optional[str] = None
    contact_title: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = "USA"
    opening_date: Optional[str] = None
    room_count: Optional[int] = None
    hotel_type: Optional[str] = None
    brand: Optional[str] = None
    brand_tier: Optional[str] = None
    location_type: Optional[str] = None
    hotel_website: Optional[str] = None
    description: Optional[str] = None
    notes: Optional[str] = None


class LeadCreate(LeadBase):
    """Schema for creating a lead"""

    lead_score: Optional[int] = None
    source_url: Optional[str] = None
    source_site: Optional[str] = None


class LeadUpdate(BaseModel):
    """Schema for updating a lead"""

    status: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_name: Optional[str] = None
    contact_title: Optional[str] = None
    notes: Optional[str] = None
    lead_score: Optional[int] = None
    rejection_reason: Optional[str] = None


class LeadResponse(LeadBase):
    """Schema for lead response"""

    id: int
    lead_score: Optional[int] = None
    score_breakdown: Optional[dict] = None
    status: str
    source_url: Optional[str] = None
    source_site: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class LeadListResponse(BaseModel):
    """Paginated lead list response"""

    leads: List[LeadResponse]
    total: int
    page: int
    per_page: int
    pages: int


class SourceBase(BaseModel):
    """Base source schema"""

    name: str
    base_url: str
    source_type: Optional[str] = "aggregator"
    priority: Optional[int] = 5
    scrape_frequency: Optional[str] = "daily"
    use_playwright: Optional[bool] = False
    is_active: Optional[bool] = True
    notes: Optional[str] = None


class SourceCreate(SourceBase):
    """Schema for creating a source"""

    entry_urls: Optional[List[str]] = None


class SourceResponse(BaseModel):
    """Schema for source response"""

    id: int
    name: str
    base_url: str
    source_type: Optional[str] = None
    priority: Optional[int] = None
    entry_urls: Optional[List[str]] = None
    scrape_frequency: Optional[str] = None
    use_playwright: Optional[bool] = False
    is_active: bool
    last_scraped_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    leads_found: Optional[int] = 0
    success_rate: Optional[float] = None
    consecutive_failures: Optional[int] = 0
    health_status: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ScrapeLogResponse(BaseModel):
    """Schema for scrape log response"""

    id: int
    source_id: Optional[int] = None
    source_name: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    urls_scraped: int = 0
    leads_found: int = 0
    leads_new: int = 0
    leads_duplicate: int = 0
    leads_skipped: int = 0

    model_config = ConfigDict(from_attributes=True)


class StatsResponse(BaseModel):
    """Schema for dashboard stats"""

    total_leads: int
    new_leads: int
    approved_leads: int
    pending_leads: int
    rejected_leads: int
    hot_leads: int
    urgent_leads: int
    warm_leads: int
    cool_leads: int
    total_sources: int
    active_sources: int
    healthy_sources: int
    leads_today: int
    leads_this_week: int
