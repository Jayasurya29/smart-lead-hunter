"""
Smart Lead Hunter — Pydantic Request/Response Schemas
======================================================
All input validation happens here. Every field that enters the system
through an API endpoint is validated, stripped, and constrained.

Validation rules:
  - hotel_name: required, non-empty after strip, max 255 chars
  - emails: RFC-compliant format when provided
  - URLs: must start with http(s):// when provided
  - lead_score: 0-100
  - room_count: positive integer
  - status: constrained to valid workflow states
  - brand_tier: constrained to known tiers
  - source_type: constrained to known categories
  - priority: 1-10
  - All strings: stripped of leading/trailing whitespace, length-capped
"""

import re
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, ConfigDict, field_validator


# ── Reusable constants ─────────────────────────────────────────────────

VALID_STATUSES = {
    "new",
    "approved",
    "rejected",
    "deleted",
    "claimed",
    "pending",
    "pushed",
}

VALID_BRAND_TIERS = {
    "tier1_ultra_luxury",
    "tier2_luxury",
    "tier3_upper_upscale",
    "tier4_upscale",
    "tier5_skip",
    "unknown",
    "",  # Allow empty for clearing
}

VALID_SOURCE_TYPES = {
    "chain_newsroom",
    "luxury_independent",
    "aggregator",
    "industry",
    "florida",
    "caribbean",
    "travel_pub",
    "pr_wire",
}

VALID_SCRAPE_FREQUENCIES = {
    "daily",
    "every_3_days",
    "twice_weekly",
    "weekly",
    "monthly",
}

VALID_LOCATION_TYPES = {"florida", "caribbean", "usa", "international", ""}

# Simple email regex — catches obvious garbage without being overly strict
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# URL must start with http:// or https://
_URL_RE = re.compile(r"^https?://\S+$")


# ── Shared validators ──────────────────────────────────────────────────


def _strip_or_none(v: Optional[str], max_len: int = 500) -> Optional[str]:
    """Strip whitespace, return None if empty, enforce max length."""
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    return v[:max_len]


def _validate_email(v: Optional[str]) -> Optional[str]:
    """Validate email format if provided."""
    if v is None:
        return None
    v = v.strip().lower()
    if not v:
        return None
    if not _EMAIL_RE.match(v):
        raise ValueError(f"Invalid email format: {v}")
    return v


def _validate_url(v: Optional[str]) -> Optional[str]:
    """Validate URL format if provided."""
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    if not _URL_RE.match(v):
        raise ValueError(f"Invalid URL (must start with http:// or https://): {v}")
    return v[:2000]  # Cap URL length


# ═══════════════════════════════════════════════════════════════════════
# LEAD SCHEMAS
# ═══════════════════════════════════════════════════════════════════════


class LeadBase(BaseModel):
    """Base lead schema with full validation."""

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

    @field_validator("hotel_name")
    @classmethod
    def hotel_name_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Hotel name cannot be empty")
        if len(v) > 255:
            raise ValueError("Hotel name must be 255 characters or fewer")
        return v

    @field_validator("contact_email")
    @classmethod
    def validate_contact_email(cls, v):
        return _validate_email(v)

    @field_validator("hotel_website")
    @classmethod
    def validate_hotel_website(cls, v):
        return _validate_url(v)

    @field_validator("room_count")
    @classmethod
    def room_count_positive(cls, v):
        if v is not None and v < 0:
            raise ValueError("Room count cannot be negative")
        return v

    @field_validator("brand_tier")
    @classmethod
    def validate_brand_tier(cls, v):
        if v is not None and v.strip() and v.strip() not in VALID_BRAND_TIERS:
            raise ValueError(
                f"Invalid brand tier: {v}. "
                f"Must be one of: {', '.join(t for t in sorted(VALID_BRAND_TIERS) if t)}"
            )
        return _strip_or_none(v)

    @field_validator("location_type")
    @classmethod
    def validate_location_type(cls, v):
        if v is not None and v.strip() and v.strip() not in VALID_LOCATION_TYPES:
            raise ValueError(f"Invalid location type: {v}")
        return _strip_or_none(v)

    @field_validator(
        "contact_name",
        "contact_title",
        "contact_phone",
        "city",
        "state",
        "country",
        "brand",
        "opening_date",
    )
    @classmethod
    def strip_string_fields(cls, v):
        return _strip_or_none(v, max_len=255)

    @field_validator("description", "notes")
    @classmethod
    def strip_long_fields(cls, v):
        return _strip_or_none(v, max_len=5000)


class LeadCreate(LeadBase):
    """Schema for creating a lead."""

    lead_score: Optional[int] = None
    source_url: Optional[str] = None
    source_site: Optional[str] = None

    @field_validator("lead_score")
    @classmethod
    def score_in_range(cls, v):
        if v is not None and (v < 0 or v > 100):
            raise ValueError("Lead score must be between 0 and 100")
        return v

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, v):
        return _validate_url(v)

    @field_validator("source_site")
    @classmethod
    def strip_source_site(cls, v):
        return _strip_or_none(v, max_len=200)


class LeadUpdate(BaseModel):
    """Schema for updating a lead — all fields optional."""

    status: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_name: Optional[str] = None
    contact_title: Optional[str] = None
    notes: Optional[str] = None
    lead_score: Optional[int] = None
    rejection_reason: Optional[str] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        if v is not None and v not in VALID_STATUSES:
            raise ValueError(
                f"Invalid status: {v}. Must be one of: {', '.join(sorted(VALID_STATUSES))}"
            )
        return v

    @field_validator("lead_score")
    @classmethod
    def score_in_range(cls, v):
        if v is not None and (v < 0 or v > 100):
            raise ValueError("Lead score must be between 0 and 100")
        return v

    @field_validator("contact_email")
    @classmethod
    def validate_contact_email(cls, v):
        return _validate_email(v)

    @field_validator("contact_name", "contact_title", "contact_phone")
    @classmethod
    def strip_string_fields(cls, v):
        return _strip_or_none(v, max_len=255)

    @field_validator("notes")
    @classmethod
    def strip_notes(cls, v):
        return _strip_or_none(v, max_len=5000)

    @field_validator("rejection_reason")
    @classmethod
    def strip_rejection_reason(cls, v):
        return _strip_or_none(v, max_len=200)


class LeadResponse(LeadBase):
    """Schema for lead response."""

    id: int
    lead_score: Optional[int] = None
    score_breakdown: Optional[dict] = None
    status: str
    source_url: Optional[str] = None
    source_site: Optional[str] = None
    source_urls: Optional[list] = None
    source_extractions: Optional[dict] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

    # Disable input validators on response — data comes from DB, already validated
    @field_validator("hotel_name", mode="before")
    @classmethod
    def pass_hotel_name(cls, v):
        return v if v else ""

    @field_validator("contact_email", "hotel_website", mode="before")
    @classmethod
    def pass_existing(cls, v):
        return v

    @field_validator("brand_tier", "location_type", mode="before")
    @classmethod
    def pass_enum_fields(cls, v):
        return v


class LeadListResponse(BaseModel):
    """Paginated lead list response."""

    leads: List[LeadResponse]
    total: int
    page: int
    per_page: int
    pages: int


# ═══════════════════════════════════════════════════════════════════════
# SOURCE SCHEMAS
# ═══════════════════════════════════════════════════════════════════════


class SourceBase(BaseModel):
    """Base source schema with validation."""

    name: str
    base_url: str
    source_type: Optional[str] = "aggregator"
    priority: Optional[int] = 5
    scrape_frequency: Optional[str] = "daily"
    use_playwright: Optional[bool] = False
    is_active: Optional[bool] = True
    notes: Optional[str] = None

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Source name cannot be empty")
        if len(v) > 100:
            raise ValueError("Source name must be 100 characters or fewer")
        return v

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Base URL cannot be empty")
        if not _URL_RE.match(v):
            raise ValueError("Base URL must start with http:// or https://")
        return v[:500]

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v):
        if v is not None and v not in VALID_SOURCE_TYPES:
            raise ValueError(
                f"Invalid source type: {v}. "
                f"Must be one of: {', '.join(sorted(VALID_SOURCE_TYPES))}"
            )
        return v

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v):
        if v is not None and (v < 1 or v > 10):
            raise ValueError("Priority must be between 1 and 10")
        return v

    @field_validator("scrape_frequency")
    @classmethod
    def validate_frequency(cls, v):
        if v is not None and v not in VALID_SCRAPE_FREQUENCIES:
            raise ValueError(
                f"Invalid frequency: {v}. "
                f"Must be one of: {', '.join(sorted(VALID_SCRAPE_FREQUENCIES))}"
            )
        return v

    @field_validator("notes")
    @classmethod
    def strip_notes(cls, v):
        return _strip_or_none(v, max_len=2000)


class SourceCreate(SourceBase):
    """Schema for creating a source."""

    entry_urls: Optional[List[str]] = None

    @field_validator("entry_urls")
    @classmethod
    def validate_entry_urls(cls, v):
        if v is None:
            return None
        validated = []
        for url in v:
            url = url.strip()
            if url and _URL_RE.match(url):
                validated.append(url[:500])
        return validated or None


class SourceResponse(BaseModel):
    """Schema for source response."""

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


# ═══════════════════════════════════════════════════════════════════════
# SCRAPE LOG & STATS
# ═══════════════════════════════════════════════════════════════════════


class ScrapeLogResponse(BaseModel):
    """Schema for scrape log response."""

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
    """Schema for dashboard stats."""

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
