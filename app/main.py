"""
Smart Lead Hunter - Main Application
------------------------------------
FastAPI entry point with REST API endpoints

Run with:
    uvicorn app.main:app --reload --port 8000
"""

import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text

import asyncio
import json
import uuid

from app.config import settings
from app.database import get_db, init_db, async_session
from app.models import PotentialLead, Source, ScrapeLog
from app.services.utils import normalize_hotel_name

# Global dict to track active scrape jobs and their progress
# Protected by _scrape_lock for async-safe mutation within a single worker.
# NOTE: For multi-worker uvicorn (--workers >1), migrate to Redis hash/pub-sub.
active_scrapes: dict = {}
scrape_cancellations: set = set()
_scrape_lock = asyncio.Lock()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def escape_like(value: str) -> str:
    """Escape LIKE-special characters (%, _) so user input is treated literally.
    
    SQLAlchemy parameterizes the value (no SQL injection), but without
    escaping, a user-supplied '%' or '_' acts as a wildcard inside LIKE/ILIKE,
    allowing unintended pattern matching.
    
    The backslash is used as the escape character, which is the default for
    PostgreSQL and SQLite.  If you ever switch to a database that doesn't
    support backslash-escape in LIKE, add  .ilike(..., escape='\\')  to the
    query calls.
    """
    return (
        value
        .replace("\\", "\\\\")   # escape the escape char first
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


# -----------------------------------------------------------------------------
# Pydantic Models (Request/Response Schemas)
# -----------------------------------------------------------------------------

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
    
    class Config:
        from_attributes = True


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
    
    class Config:
        from_attributes = True


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
    
    class Config:
        from_attributes = True


class StatsResponse(BaseModel):
    """Schema for dashboard stats"""
    total_leads: int
    new_leads: int
    approved_leads: int
    pending_leads: int
    rejected_leads: int
    hot_leads: int
    warm_leads: int
    total_sources: int
    active_sources: int
    healthy_sources: int
    leads_today: int
    leads_this_week: int


# -----------------------------------------------------------------------------
# Application Lifecycle
# -----------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    logger.info("=" * 50)
    logger.info("Starting Smart Lead Hunter...")
    logger.info("=" * 50)
    
    await init_db()
    logger.info("Database tables verified")
    
    logger.info("Smart Lead Hunter is ready!")
    logger.info("API Docs: http://localhost:8000/docs")
    
    yield
    
    logger.info("Shutting down Smart Lead Hunter...")


# -----------------------------------------------------------------------------
# FastAPI App
# -----------------------------------------------------------------------------
from app.logging_config import setup_logging
setup_logging()

app = FastAPI(
    title="Smart Lead Hunter",
    description="Automated hotel lead generation system for J.A. Uniforms",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    # P-02 FIX: Environment-aware CORS (was allow_origins=["*"])
    # In production, restrict to your actual domain(s).
    # In development, allow localhost variants.
    allow_origins=(
        [
            "https://leads.jauniforms.com",  # TODO: Update to your production domain
        ]
        if getattr(settings, "environment", "development") == "production"
        else [
            "http://localhost:8000",
            "http://localhost:3000",
            "http://127.0.0.1:8000",
        ]
    ),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)


# P-03: Simple in-memory rate limiter for API endpoints
# Limits each client IP to a configurable number of requests per window.
# Skips static assets, dashboard HTML, and health checks.
_rate_limit_store: dict = defaultdict(lambda: {"count": 0, "reset": 0.0})
_RATE_LIMIT_MAX = 60        # requests per window
_RATE_LIMIT_WINDOW = 60.0   # seconds
_RATE_LIMIT_MAX_ENTRIES = 10000  # max tracked IPs before forced eviction
_rate_limit_last_cleanup = 0.0


def _cleanup_rate_limit_store(now: float) -> None:
    """Evict expired rate limit entries to prevent unbounded memory growth.
    
    Runs at most once per window period. Removes entries whose reset time
    has passed (plus a small buffer).
    """
    global _rate_limit_last_cleanup
    # Only clean up once per window
    if now - _rate_limit_last_cleanup < _RATE_LIMIT_WINDOW:
        return
    _rate_limit_last_cleanup = now
    
    expired = [ip for ip, bucket in _rate_limit_store.items()
               if now > bucket["reset"] + _RATE_LIMIT_WINDOW]
    for ip in expired:
        del _rate_limit_store[ip]
    
    # Emergency eviction if still too large (e.g., botnet with rotating IPs)
    if len(_rate_limit_store) > _RATE_LIMIT_MAX_ENTRIES:
        # Remove oldest entries first
        sorted_ips = sorted(_rate_limit_store.keys(),
                           key=lambda ip: _rate_limit_store[ip]["reset"])
        for ip in sorted_ips[:len(_rate_limit_store) - _RATE_LIMIT_MAX_ENTRIES // 2]:
            del _rate_limit_store[ip]


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """P-03: Rate limit API requests per client IP.
    
    Only applies to /leads, /sources, /scrape, and /api paths.
    Dashboard HTML pages, health checks, and static files are exempt.
    """
    path = request.url.path

    # Skip rate limiting for non-API paths
    if not any(path.startswith(p) for p in ["/leads", "/sources", "/scrape", "/api"]):
        return await call_next(request)

    # Get real client IP (handles reverse proxy like nginx/Cloudflare)
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    else:
        client_ip = request.client.host if request.client else "unknown"
    now = time.monotonic()
    
    # Periodic eviction of expired entries (prevents memory leak)
    _cleanup_rate_limit_store(now)
    
    bucket = _rate_limit_store[client_ip]

    # Reset window if expired
    if now > bucket["reset"]:
        bucket["count"] = 0
        bucket["reset"] = now + _RATE_LIMIT_WINDOW

    bucket["count"] += 1

    if bucket["count"] > _RATE_LIMIT_MAX:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please slow down."},
            headers={"Retry-After": str(int(bucket["reset"] - now))}
        )

    response = await call_next(request)
    return response


# -----------------------------------------------------------------------------
# Jinja2 Templates
# -----------------------------------------------------------------------------

templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))


# -----------------------------------------------------------------------------
# Shared Helpers
# -----------------------------------------------------------------------------

async def _get_dashboard_stats(db: AsyncSession) -> dict:
    """Fetch all dashboard stats in 2 queries instead of 12.
    
    Uses SQLAlchemy conditional aggregation:  count() + filter()
    so the database scans the leads table once and the sources table once.
    """
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())

    # -- Single query for ALL lead counts --
    lead_result = await db.execute(
        select(
            func.count(PotentialLead.id).label("total"),
            func.count(PotentialLead.id).filter(
                PotentialLead.status == "new"
            ).label("new"),
            func.count(PotentialLead.id).filter(
                PotentialLead.status == "approved"
            ).label("approved"),
            func.count(PotentialLead.id).filter(
                PotentialLead.status == "pending"
            ).label("pending"),
            func.count(PotentialLead.id).filter(
                PotentialLead.status.in_(["rejected", "bad"])
            ).label("rejected"),
            func.count(PotentialLead.id).filter(
                PotentialLead.lead_score >= settings.hot_lead_threshold
            ).label("hot"),
            func.count(PotentialLead.id).filter(
                PotentialLead.lead_score >= settings.warm_lead_threshold,
                PotentialLead.lead_score < settings.hot_lead_threshold
            ).label("warm"),
            func.count(PotentialLead.id).filter(
                PotentialLead.created_at >= today_start
            ).label("today"),
            func.count(PotentialLead.id).filter(
                PotentialLead.created_at >= week_start
            ).label("this_week"),
        )
    )
    lr = lead_result.one()

    # -- Single query for ALL source counts --
    source_result = await db.execute(
        select(
            func.count(Source.id).label("total"),
            func.count(Source.id).filter(Source.is_active == True).label("active"),
            func.count(Source.id).filter(Source.health_status == "healthy").label("healthy"),
        )
    )
    sr = source_result.one()

    return {
        "total_leads": lr.total or 0,
        "new_leads": lr.new or 0,
        "approved_leads": lr.approved or 0,
        "pending_leads": lr.pending or 0,
        "rejected_leads": lr.rejected or 0,
        "hot_leads": lr.hot or 0,
        "warm_leads": lr.warm or 0,
        "leads_today": lr.today or 0,
        "leads_this_week": lr.this_week or 0,
        "total_sources": sr.total or 0,
        "active_sources": sr.active or 0,
        "healthy_sources": sr.healthy or 0,
    }


# -----------------------------------------------------------------------------
# Health & Status Endpoints
# -----------------------------------------------------------------------------

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API info"""
    return {
        "name": "Smart Lead Hunter",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health", tags=["Health"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """Health check endpoint"""
    try:
        await db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {e}"
    
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "database": db_status
        }
    }


@app.get("/stats", response_model=StatsResponse, tags=["Dashboard"])
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Get dashboard statistics"""
    stats = await _get_dashboard_stats(db)
    return StatsResponse(**stats)


# -----------------------------------------------------------------------------
# Lead Endpoints
# -----------------------------------------------------------------------------

@app.get("/leads", response_model=LeadListResponse, tags=["Leads"])
async def list_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    state: Optional[str] = None,
    location_type: Optional[str] = None,
    brand_tier: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """List leads with filtering and pagination"""
    query = select(PotentialLead)
    count_query = select(func.count(PotentialLead.id))
    
    # Apply filters
    if status:
        query = query.where(PotentialLead.status == status)
        count_query = count_query.where(PotentialLead.status == status)
    if min_score:
        query = query.where(PotentialLead.lead_score >= min_score)
        count_query = count_query.where(PotentialLead.lead_score >= min_score)
    if state:
        safe_state = escape_like(state)
        query = query.where(PotentialLead.state.ilike(f"%{safe_state}%"))
        count_query = count_query.where(PotentialLead.state.ilike(f"%{safe_state}%"))
    if location_type:
        query = query.where(PotentialLead.location_type == location_type)
        count_query = count_query.where(PotentialLead.location_type == location_type)
    if brand_tier:
        query = query.where(PotentialLead.brand_tier == brand_tier)
        count_query = count_query.where(PotentialLead.brand_tier == brand_tier)
    if search:
        safe_search = escape_like(search)
        search_filter = (
            PotentialLead.hotel_name.ilike(f"%{safe_search}%") |
            PotentialLead.city.ilike(f"%{safe_search}%") |
            PotentialLead.brand.ilike(f"%{safe_search}%")
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)
    
    # Get total count
    result = await db.execute(count_query)
    total = result.scalar() or 0
    
    # Apply pagination and ordering - HIGH SCORES FIRST
    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc().nullslast(),
        PotentialLead.created_at.desc()
    ).offset(offset).limit(per_page)
    
    result = await db.execute(query)
    leads = result.scalars().all()
    
    pages = (total + per_page - 1) // per_page if total > 0 else 1
    
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages
    )


@app.get("/leads/hot", response_model=LeadListResponse, tags=["Leads"])
async def get_hot_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Get hot leads (score >= config threshold) - ready for outreach"""
    query = select(PotentialLead).where(
        PotentialLead.lead_score >= settings.hot_lead_threshold,
        PotentialLead.status == "new"
    )
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.lead_score >= settings.hot_lead_threshold,
        PotentialLead.status == "new"
    )
    
    result = await db.execute(count_query)
    total = result.scalar() or 0
    
    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc()
    ).offset(offset).limit(per_page)
    
    result = await db.execute(query)
    leads = result.scalars().all()
    
    pages = (total + per_page - 1) // per_page if total > 0 else 1
    
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages
    )


@app.get("/leads/florida", response_model=LeadListResponse, tags=["Leads"])
async def get_florida_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Get Florida leads - your primary market"""
    query = select(PotentialLead).where(
        PotentialLead.location_type == "florida"
    )
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.location_type == "florida"
    )
    
    result = await db.execute(count_query)
    total = result.scalar() or 0
    
    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc().nullslast()
    ).offset(offset).limit(per_page)
    
    result = await db.execute(query)
    leads = result.scalars().all()
    
    pages = (total + per_page - 1) // per_page if total > 0 else 1
    
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages
    )


@app.get("/leads/caribbean", response_model=LeadListResponse, tags=["Leads"])
async def get_caribbean_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Get Caribbean leads"""
    query = select(PotentialLead).where(
        PotentialLead.location_type == "caribbean"
    )
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.location_type == "caribbean"
    )
    
    result = await db.execute(count_query)
    total = result.scalar() or 0
    
    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc().nullslast()
    ).offset(offset).limit(per_page)
    
    result = await db.execute(query)
    leads = result.scalars().all()
    
    pages = (total + per_page - 1) // per_page if total > 0 else 1
    
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages
    )


@app.get("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def get_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single lead by ID"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    return LeadResponse.model_validate(lead)


@app.post("/leads", response_model=LeadResponse, tags=["Leads"])
async def create_lead(lead_data: LeadCreate, db: AsyncSession = Depends(get_db)):
    """Create a new lead manually"""
    # Use shared normalization (consistent with orchestrator + Celery paths)
    normalized_name = normalize_hotel_name(lead_data.hotel_name)
    
    # Check for duplicate before inserting
    existing = await db.execute(
        select(PotentialLead).where(
            PotentialLead.hotel_name_normalized == normalized_name
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"A lead with a similar name already exists: '{lead_data.hotel_name}'"
        )
    
    lead = PotentialLead(
        hotel_name=lead_data.hotel_name,
        hotel_name_normalized=normalized_name,
        contact_email=lead_data.contact_email,
        contact_phone=lead_data.contact_phone,
        contact_name=lead_data.contact_name,
        contact_title=lead_data.contact_title,
        city=lead_data.city,
        state=lead_data.state,
        country=lead_data.country,
        opening_date=lead_data.opening_date,
        room_count=lead_data.room_count,
        hotel_type=lead_data.hotel_type,
        brand=lead_data.brand,
        brand_tier=lead_data.brand_tier,
        location_type=lead_data.location_type,
        hotel_website=lead_data.hotel_website,
        description=lead_data.description,
        notes=lead_data.notes,
        source_url=lead_data.source_url,
        source_site=lead_data.source_site or "manual",
        status="new",
        lead_score=lead_data.lead_score
    )
    
    db.add(lead)
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Created lead: {lead.hotel_name} (ID: {lead.id}, Score: {lead.lead_score})")
    
    return LeadResponse.model_validate(lead)


@app.patch("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def update_lead(lead_id: int, updates: LeadUpdate, db: AsyncSession = Depends(get_db)):
    """Update a lead"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    update_data = updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(lead, field, value)
    
    lead.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Updated lead: {lead.hotel_name} (ID: {lead.id})")
    
    return LeadResponse.model_validate(lead)


@app.post("/leads/{lead_id}/approve", response_model=LeadResponse, tags=["Leads"])
async def approve_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Approve a lead - ready for CRM push"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    lead.status = "approved"
    lead.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Approved lead: {lead.hotel_name} (ID: {lead.id})")
    
    return LeadResponse.model_validate(lead)


@app.post("/leads/{lead_id}/reject", response_model=LeadResponse, tags=["Leads"])
async def reject_lead(
    lead_id: int, 
    reason: Optional[str] = Query(None, description="Rejection reason: duplicate, budget_brand, international, old_opening, bad_data"),
    db: AsyncSession = Depends(get_db)
):
    """Reject a lead"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Rejected lead: {lead.hotel_name} (ID: {lead.id}, Reason: {reason})")
    
    return LeadResponse.model_validate(lead)


@app.delete("/leads/{lead_id}", tags=["Leads"])
async def delete_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a lead"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    hotel_name = lead.hotel_name
    await db.delete(lead)
    await db.commit()
    
    logger.info(f"Deleted lead: {hotel_name} (ID: {lead_id})")
    
    return {"message": "Lead deleted", "id": lead_id}


# -----------------------------------------------------------------------------
# Source Endpoints
# -----------------------------------------------------------------------------

@app.get("/sources", response_model=List[SourceResponse], tags=["Sources"])
async def list_sources(
    active_only: bool = False,
    source_type: Optional[str] = None,
    min_priority: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """List all scraping sources"""
    query = select(Source)
    
    if active_only:
        query = query.where(Source.is_active == True)
    if source_type:
        query = query.where(Source.source_type == source_type)
    if min_priority:
        query = query.where(Source.priority >= min_priority)
    
    query = query.order_by(Source.priority.desc(), Source.name)
    
    result = await db.execute(query)
    sources = result.scalars().all()
    
    return [SourceResponse.model_validate(source) for source in sources]


@app.get("/sources/healthy", response_model=List[SourceResponse], tags=["Sources"])
async def list_healthy_sources(db: AsyncSession = Depends(get_db)):
    """List healthy sources ready for scraping"""
    query = select(Source).where(
        Source.is_active == True,
        Source.health_status.in_(["healthy", "new", "degraded"])
    ).order_by(Source.priority.desc())
    
    result = await db.execute(query)
    sources = result.scalars().all()
    
    return [SourceResponse.model_validate(source) for source in sources]


@app.get("/sources/problems", response_model=List[SourceResponse], tags=["Sources"])
async def list_problem_sources(db: AsyncSession = Depends(get_db)):
    """List sources with issues (failing/dead)"""
    query = select(Source).where(
        Source.health_status.in_(["failing", "dead"])
    ).order_by(Source.consecutive_failures.desc())
    
    result = await db.execute(query)
    sources = result.scalars().all()
    
    return [SourceResponse.model_validate(source) for source in sources]


@app.post("/sources", response_model=SourceResponse, tags=["Sources"])
async def create_source(source_data: SourceCreate, db: AsyncSession = Depends(get_db)):
    """Create a new scraping source"""
    # Check for duplicate URL
    result = await db.execute(
        select(Source).where(Source.base_url == source_data.base_url)
    )
    existing = result.scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Source with this URL already exists")
    
    source = Source(
        name=source_data.name,
        base_url=source_data.base_url,
        source_type=source_data.source_type,
        priority=source_data.priority,
        entry_urls=source_data.entry_urls or [source_data.base_url],
        scrape_frequency=source_data.scrape_frequency,
        use_playwright=source_data.use_playwright,
        is_active=source_data.is_active,
        notes=source_data.notes,
        health_status="new"
    )
    
    db.add(source)
    await db.commit()
    await db.refresh(source)
    
    logger.info(f"Created source: {source.name} (ID: {source.id})")
    
    return SourceResponse.model_validate(source)


@app.post("/sources/{source_id}/toggle", response_model=SourceResponse, tags=["Sources"])
async def toggle_source(source_id: int, db: AsyncSession = Depends(get_db)):
    """Toggle source active/inactive"""
    result = await db.execute(
        select(Source).where(Source.id == source_id)
    )
    source = result.scalar_one_or_none()
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    source.is_active = not source.is_active
    source.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(source)
    
    status = "activated" if source.is_active else "deactivated"
    logger.info(f"Source {status}: {source.name} (ID: {source.id})")
    
    return SourceResponse.model_validate(source)


@app.post("/sources/{source_id}/reset-health", response_model=SourceResponse, tags=["Sources"])
async def reset_source_health(source_id: int, db: AsyncSession = Depends(get_db)):
    """Reset a source's health status (after fixing issues)"""
    result = await db.execute(
        select(Source).where(Source.id == source_id)
    )
    source = result.scalar_one_or_none()
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    source.health_status = "new"
    source.consecutive_failures = 0
    source.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(source)
    
    logger.info(f"Reset health for source: {source.name} (ID: {source.id})")
    
    return SourceResponse.model_validate(source)


@app.delete("/sources/{source_id}", tags=["Sources"])
async def delete_source(source_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a source"""
    result = await db.execute(
        select(Source).where(Source.id == source_id)
    )
    source = result.scalar_one_or_none()
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    name = source.name
    await db.delete(source)
    await db.commit()
    
    logger.info(f"Deleted source: {name} (ID: {source_id})")
    
    return {"message": "Source deleted", "id": source_id}


# -----------------------------------------------------------------------------
# Scrape Logs Endpoint
# -----------------------------------------------------------------------------

@app.get("/scrape/logs", response_model=List[ScrapeLogResponse], tags=["Scraping"])
async def get_scrape_logs(
    limit: int = Query(20, ge=1, le=100),
    source_id: Optional[int] = None,
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get recent scrape logs"""
    # Single JOIN query instead of N+1 (was: 1 query per log to fetch source name)
    query = select(ScrapeLog, Source.name.label("source_name")).outerjoin(
        Source, ScrapeLog.source_id == Source.id
    )
    
    if source_id:
        query = query.where(ScrapeLog.source_id == source_id)
    if status:
        query = query.where(ScrapeLog.status == status)
    
    query = query.order_by(ScrapeLog.started_at.desc()).limit(limit)
    
    result = await db.execute(query)
    rows = result.all()
    
    response_list = []
    for log, src_name in rows:
        log_response = ScrapeLogResponse.model_validate(log)
        log_response.source_name = src_name
        response_list.append(log_response)
    
    return response_list


# -----------------------------------------------------------------------------
# Dashboard Endpoints (HTMX + Jinja2)
# -----------------------------------------------------------------------------
@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_page(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    view: Optional[str] = None,
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    location_type: Optional[str] = Query(None),
    brand_tier: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """Main dashboard page"""
    # Get stats (2 queries instead of 12)
    stats = await _get_dashboard_stats(db)

    # Apply view presets
    if view == "hot":
        min_score = settings.hot_lead_threshold
        status = "new"

    # Build query
    query = select(PotentialLead)
    count_query = select(func.count(PotentialLead.id))

    if status:
        query = query.where(PotentialLead.status == status)
        count_query = count_query.where(PotentialLead.status == status)
    if min_score:
        query = query.where(PotentialLead.lead_score >= min_score)
        count_query = count_query.where(PotentialLead.lead_score >= min_score)
    if location_type:
        query = query.where(PotentialLead.location_type == location_type)
        count_query = count_query.where(PotentialLead.location_type == location_type)
    if brand_tier:
        query = query.where(PotentialLead.brand_tier == brand_tier)
        count_query = count_query.where(PotentialLead.brand_tier == brand_tier)
    if search:
        safe_search = escape_like(search)
        search_filter = (
            PotentialLead.hotel_name.ilike(f"%{safe_search}%") |
            PotentialLead.city.ilike(f"%{safe_search}%") |
            PotentialLead.brand.ilike(f"%{safe_search}%")
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    result = await db.execute(count_query)
    total = result.scalar() or 0

    # Apply pagination
    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc().nullslast(),
        PotentialLead.created_at.desc()
    ).offset(offset).limit(per_page)

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "stats": stats,
            "leads": leads,
            "pagination": {"total": total, "page": page, "per_page": per_page, "pages": pages},
            "filters": {
                "search": search,
                "status": status,
                "min_score": min_score,
                "location_type": location_type,
                "brand_tier": brand_tier
            }
        }
    )


@app.get("/api/dashboard/stats", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_stats_partial(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX partial: Stats cards"""
    stats = await _get_dashboard_stats(db)

    return templates.TemplateResponse(
        "partials/stats.html",
        {"request": request, "stats": stats}
    )


@app.get("/api/dashboard/leads", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_leads_partial(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    location_type: Optional[str] = None,
    brand_tier: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """HTMX partial: Lead list with filtering and pagination"""
    query = select(PotentialLead)
    count_query = select(func.count(PotentialLead.id))

    if status:
        query = query.where(PotentialLead.status == status)
        count_query = count_query.where(PotentialLead.status == status)
    if min_score:
        query = query.where(PotentialLead.lead_score >= min_score)
        count_query = count_query.where(PotentialLead.lead_score >= min_score)
    if location_type:
        query = query.where(PotentialLead.location_type == location_type)
        count_query = count_query.where(PotentialLead.location_type == location_type)
    if brand_tier:
        query = query.where(PotentialLead.brand_tier == brand_tier)
        count_query = count_query.where(PotentialLead.brand_tier == brand_tier)
    if search:
        safe_search = escape_like(search)
        search_filter = (
            PotentialLead.hotel_name.ilike(f"%{safe_search}%") |
            PotentialLead.city.ilike(f"%{safe_search}%") |
            PotentialLead.brand.ilike(f"%{safe_search}%")
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    result = await db.execute(count_query)
    total = result.scalar() or 0

    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc().nullslast(),
        PotentialLead.created_at.desc()
    ).offset(offset).limit(per_page)

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return templates.TemplateResponse(
        "partials/lead_list.html",
        {
            "request": request,
            "leads": leads,
            "pagination": {"total": total, "page": page, "per_page": per_page, "pages": pages}
        }
    )


@app.get("/api/dashboard/leads/{lead_id}", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_lead_detail_partial(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db)
):
    """HTMX partial: Lead detail panel"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(
            content='<div class="p-6 text-center text-red-500">Lead not found</div>',
            status_code=404
        )

    return templates.TemplateResponse(
        "partials/lead_detail.html",
        {"request": request, "lead": lead}
    )


@app.post("/api/dashboard/leads/{lead_id}/approve", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_approve_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db)
):
    """HTMX: Approve lead and return updated row"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    lead.status = "approved"
    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    logger.info(f"Dashboard: Approved lead {lead.hotel_name} (ID: {lead.id})")

    return templates.TemplateResponse(
        "partials/lead_row.html",
        {"request": request, "lead": lead}
    )


@app.post("/api/dashboard/leads/{lead_id}/reject", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_reject_lead(
    request: Request,
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """HTMX: Reject lead and return updated row"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    logger.info(f"Dashboard: Rejected lead {lead.hotel_name} (ID: {lead.id}, Reason: {reason})")

    return templates.TemplateResponse(
        "partials/lead_row.html",
        {"request": request, "lead": lead}
    )


@app.post("/api/dashboard/scrape", tags=["Dashboard"])
async def dashboard_trigger_scrape(request: Request):
    """Trigger a scrape job via Celery (legacy endpoint)"""
    try:
        from app.tasks.scraping_tasks import run_full_scrape

        # Queue the task with Celery
        task = run_full_scrape.delay()

        logger.info(f"Dashboard: Scrape triggered by user (task_id: {task.id})")

        return {
            "status": "started",
            "message": "Scrape job queued successfully",
            "task_id": task.id
        }
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger scrape: {e}")
        return {
            "status": "error",
            "message": f"Failed to queue scrape: {str(e)}. Is Celery worker running?"
        }


# -----------------------------------------------------------------------------
# SSE Scrape Endpoint - Uses the REAL Orchestrator Pipeline
# -----------------------------------------------------------------------------
# This is the UNIFIED scrape path. Both the dashboard "Run Scrape" button
# and any future triggers use the same orchestrator that the CLI uses.
# No duplicate scraping/extraction/scoring/dedup logic.
# -----------------------------------------------------------------------------

@app.get("/api/dashboard/scrape/stream", tags=["Dashboard"])
async def scrape_with_progress(request: Request):
    """SSE endpoint for real-time scrape progress using the orchestrator pipeline"""

    scrape_id = str(uuid.uuid4())
    async with _scrape_lock:
        active_scrapes[scrape_id] = {"status": "starting"}

    async def event_generator():
        orchestrator = None
        try:
            import os

            # Send initial event with scrape ID
            yield f"data: {json.dumps({'type': 'started', 'scrape_id': scrape_id})}\n\n"

            # --- Initialize orchestrator (same one the CLI uses) ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Initializing pipeline...'})}\n\n"

            from app.services.orchestrator import LeadHunterOrchestrator
            orchestrator = LeadHunterOrchestrator(
                gemini_api_key=os.getenv("GEMINI_API_KEY"),
                save_to_database=True,
            )
            await orchestrator.initialize()

            yield f"data: {json.dumps({'type': 'info', 'message': 'Pipeline ready. Loading sources...'})}\n\n"

            # --- Get active sources from DB ---
            async with async_session() as session:
                result = await session.execute(
                    select(Source).where(Source.is_active == True).order_by(Source.priority.desc())
                )
                sources = result.scalars().all()

            total_sources = len(sources)
            source_names = [s.name for s in sources]

            yield f"data: {json.dumps({'type': 'info', 'message': f'Found {total_sources} active sources to scrape'})}\n\n"

            start_time = datetime.now(timezone.utc)

            # --- PHASE 1: SCRAPE all sources via the engine ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 1: Scraping sources...'})}\n\n"

            # Scrape one source at a time so we can send progress events
            all_pages = []
            sources_successful = 0

            for idx, source in enumerate(sources, 1):
                # Check for cancellation (async-safe)
                async with _scrape_lock:
                    cancelled = scrape_id in scrape_cancellations
                    if cancelled:
                        scrape_cancellations.discard(scrape_id)
                if cancelled:
                    yield f"data: {json.dumps({'type': 'cancelled', 'message': 'Scrape cancelled by user'})}\n\n"
                    break

                # Check if client disconnected (stop wasting resources)
                if await request.is_disconnected():
                    logger.info(f"Client disconnected during scrape {scrape_id}, stopping pipeline")
                    break

                source_name = source.name
                yield f"data: {json.dumps({'type': 'source_start', 'source': source_name, 'current': idx, 'total': total_sources})}\n\n"

                try:
                    # Use the orchestrator's scraping engine
                    scrape_results = await orchestrator.scraping_engine.scrape_sources(
                        [source_name], deep=True, max_concurrent=3
                    )

                    source_pages = 0
                    for sname, results in scrape_results.items():
                        successful = [r for r in results if r.success]
                        source_pages += len(successful)
                        for r in successful:
                            all_pages.append({
                                "source_name": sname,
                                "url": r.url,
                                "content": r.text or r.html or "",
                            })

                    if source_pages > 0:
                        sources_successful += 1
                        yield f"data: {json.dumps({'type': 'source_complete', 'source': source_name, 'current': idx, 'total': total_sources, 'pages': source_pages})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'url_error', 'url': source.base_url[:60], 'error': 'No content returned'})}\n\n"

                    # Update source last_scraped_at
                    async with async_session() as session:
                        source_obj = (await session.execute(
                            select(Source).where(Source.id == source.id)
                        )).scalar_one_or_none()
                        if source_obj:
                            if source_pages > 0:
                                source_obj.record_success(0)  # lead count updated after extraction
                            else:
                                source_obj.record_failure()
                            await session.commit()

                except Exception as e:
                    logger.error(f"Source {source_name} failed: {e}")
                    yield f"data: {json.dumps({'type': 'url_error', 'url': source.base_url[:60], 'error': str(e)[:100]})}\n\n"

                # Rate limiting between sources
                await asyncio.sleep(1)

            yield f"data: {json.dumps({'type': 'info', 'message': f'Scraping complete: {len(all_pages)} pages from {sources_successful} sources'})}\n\n"

            if not all_pages:
                yield f"data: {json.dumps({'type': 'complete', 'stats': {'sources_scraped': 0, 'leads_found': 0, 'leads_saved': 0}, 'duration_seconds': 0})}\n\n"
                return

            # --- PHASE 2: EXTRACTION via intelligent pipeline ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 2: AI extraction (Gemini)...'})}\n\n"

            pages_for_pipeline = [
                {'url': p['url'], 'content': p['content'], 'source': p['source_name']}
                for p in all_pages
            ]

            pipeline_result = await orchestrator.pipeline.process_pages(
                pages_for_pipeline,
                source_name="Dashboard Scrape"
            )

            leads_extracted = pipeline_result.leads_extracted
            yield f"data: {json.dumps({'type': 'info', 'message': f'Extracted {leads_extracted} leads from {pipeline_result.pages_classified} pages'})}\n\n"

            # --- PHASE 3: DEDUPLICATION via smart deduplicator ---
            if orchestrator.deduplicator and pipeline_result.final_leads:
                yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 3: Deduplication...'})}\n\n"

                leads_for_dedup = [lead.to_dict() for lead in pipeline_result.final_leads]
                merged_leads = orchestrator.deduplicator.deduplicate(leads_for_dedup)
                dedup_stats = orchestrator.deduplicator.get_stats()

                dupes_found = dedup_stats.get("duplicates_found", 0)
                unique_count = len(merged_leads)
                yield f"data: {json.dumps({'type': 'info', 'message': f'Dedup: {dupes_found} duplicates merged, {unique_count} unique leads'})}\n\n"

                # Convert MergedLead objects to dicts for save_leads_to_database
                lead_dicts = []
                for ml in merged_leads:
                    d = {
                        'hotel_name': ml.hotel_name,
                        'brand': ml.brand,
                        'property_type': ml.property_type,
                        'city': ml.city,
                        'state': ml.state,
                        'country': ml.country,
                        'opening_date': ml.opening_date,
                        'room_count': ml.room_count,
                        'contact_name': ml.contact_name,
                        'contact_title': ml.contact_title,
                        'contact_email': ml.contact_email,
                        'contact_phone': ml.contact_phone,
                        'source_url': ml.source_urls[0] if ml.source_urls else '',
                        'source_name': ml.source_names[0] if ml.source_names else '',
                        'key_insights': getattr(ml, 'key_insights', ''),
                        'confidence_score': ml.confidence_score,
                        'qualification_score': getattr(ml, 'qualification_score', 0),
                    }
                    if ml.merged_from_count > 1:
                        d['key_insights'] = (d.get('key_insights') or '') + f"\n\n Merged from {ml.merged_from_count} sources"
                    lead_dicts.append(d)
            else:
                # No deduplicator or no leads
                lead_dicts = [lead.to_dict() for lead in (pipeline_result.final_leads or [])]

            # --- PHASE 4: SAVE TO DATABASE via orchestrator ---
            if lead_dicts:
                yield f"data: {json.dumps({'type': 'info', 'message': f'Phase 4: Saving {len(lead_dicts)} leads to database...'})}\n\n"

                db_result = await orchestrator.save_leads_to_database(lead_dicts)
                leads_saved = db_result['saved']
                leads_dupes = db_result['duplicates']

                if leads_saved > 0:
                    yield f"data: {json.dumps({'type': 'leads_found', 'url': 'pipeline', 'found': len(lead_dicts), 'saved': leads_saved, 'total_saved': leads_saved})}\n\n"

                yield f"data: {json.dumps({'type': 'info', 'message': f'Saved {leads_saved} new leads, {leads_dupes} already existed'})}\n\n"
            else:
                leads_saved = 0
                leads_dupes = 0

            # --- COMPLETE ---
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            final_stats = {
                "sources_scraped": sources_successful,
                "urls_scraped": len(all_pages),
                "leads_found": leads_extracted,
                "leads_saved": leads_saved,
                "leads_skipped": leads_dupes,
                "errors": []
            }

            yield f"data: {json.dumps({'type': 'complete', 'stats': final_stats, 'duration_seconds': duration})}\n\n"

        except Exception as e:
            logger.error(f"Scrape stream error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            async with _scrape_lock:
                active_scrapes.pop(scrape_id, None)
                scrape_cancellations.discard(scrape_id)
            # Clean up orchestrator
            if orchestrator:
                try:
                    await orchestrator.close()
                except Exception:
                    pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/dashboard/scrape/cancel/{scrape_id}", tags=["Dashboard"])
async def cancel_scrape(scrape_id: str):
    """Cancel an active scrape job"""
    async with _scrape_lock:
        if scrape_id in active_scrapes:
            scrape_cancellations.add(scrape_id)
            return {"status": "cancelling", "message": "Cancellation requested"}
    return {"status": "not_found", "message": "Scrape job not found"}