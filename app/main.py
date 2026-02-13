"""
Smart Lead Hunter - Main Application
------------------------------------
FastAPI entry point with REST API endpoints

Run with:
    uvicorn app.main:app --reload --port 8000
"""

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text, or_

import asyncio
import json
import uuid

from app.config import settings
from app.logging_config import setup_logging
from app.database import get_db, init_db, async_session
from app.models import PotentialLead, Source, ScrapeLog
from app.services.utils import normalize_hotel_name

# Global dict to track active scrape jobs and their progress
# Protected by _scrape_lock for async-safe mutation within a single worker.
# NOTE: For multi-worker uvicorn (--workers >1), migrate to Redis hash/pub-sub.
active_scrapes: dict = {}
_pending_scrape_config = {}
_pending_extract_url = ""
scrape_cancellations: set = set()
_scrape_lock = asyncio.Lock()
_pending_scrape_config: dict = {}

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        value.replace("\\", "\\\\")  # escape the escape char first
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
setup_logging()

app = FastAPI(
    title="Smart Lead Hunter",
    description="Automated hotel lead generation system for J.A. Uniforms",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

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
_RATE_LIMIT_MAX = 60  # requests per window
_RATE_LIMIT_WINDOW = 60.0  # seconds
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

    expired = [
        ip
        for ip, bucket in _rate_limit_store.items()
        if now > bucket["reset"] + _RATE_LIMIT_WINDOW
    ]
    for ip in expired:
        del _rate_limit_store[ip]

    # Emergency eviction if still too large (e.g., botnet with rotating IPs)
    if len(_rate_limit_store) > _RATE_LIMIT_MAX_ENTRIES:
        # Remove oldest entries first
        sorted_ips = sorted(
            _rate_limit_store.keys(), key=lambda ip: _rate_limit_store[ip]["reset"]
        )
        for ip in sorted_ips[: len(_rate_limit_store) - _RATE_LIMIT_MAX_ENTRIES // 2]:
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
            headers={"Retry-After": str(int(bucket["reset"] - now))},
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
            func.count(PotentialLead.id)
            .filter(PotentialLead.status == "new")
            .label("new"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.status == "approved")
            .label("approved"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.status == "pending")
            .label("pending"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.status.in_(["rejected", "bad"]))
            .label("rejected"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.lead_score >= settings.hot_lead_threshold)
            .label("hot"),
            func.count(PotentialLead.id)
            .filter(
                PotentialLead.lead_score >= settings.warm_lead_threshold,
                PotentialLead.lead_score < settings.hot_lead_threshold,
            )
            .label("warm"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.created_at >= today_start)
            .label("today"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.created_at >= week_start)
            .label("this_week"),
            func.count(PotentialLead.id)
            .filter(PotentialLead.status == "deleted")
            .label("deleted"),
        )
    )
    lr = lead_result.one()

    # -- Single query for ALL source counts --
    source_result = await db.execute(
        select(
            func.count(Source.id).label("total"),
            func.count(Source.id).filter(Source.is_active.is_(True)).label("active"),
            func.count(Source.id)
            .filter(Source.health_status == "healthy")
            .label("healthy"),
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
        "deleted_leads": lr.deleted or 0,
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
        "docs": "/docs",
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
        "components": {"database": db_status},
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
    db: AsyncSession = Depends(get_db),
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
            PotentialLead.hotel_name.ilike(f"%{safe_search}%")
            | PotentialLead.city.ilike(f"%{safe_search}%")
            | PotentialLead.brand.ilike(f"%{safe_search}%")
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    # Get total count
    result = await db.execute(count_query)
    total = result.scalar() or 0

    # Apply pagination and ordering - HIGH SCORES FIRST
    offset = (page - 1) * per_page
    query = (
        query.order_by(
            PotentialLead.lead_score.desc().nullslast(), PotentialLead.created_at.desc()
        )
        .offset(offset)
        .limit(per_page)
    )

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get("/leads/hot", response_model=LeadListResponse, tags=["Leads"])
async def get_hot_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get hot leads (score >= config threshold) - ready for outreach"""
    query = select(PotentialLead).where(
        PotentialLead.lead_score >= settings.hot_lead_threshold,
        PotentialLead.status == "new",
    )
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.lead_score >= settings.hot_lead_threshold,
        PotentialLead.status == "new",
    )

    result = await db.execute(count_query)
    total = result.scalar() or 0

    offset = (page - 1) * per_page
    query = (
        query.order_by(PotentialLead.lead_score.desc()).offset(offset).limit(per_page)
    )

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get("/leads/florida", response_model=LeadListResponse, tags=["Leads"])
async def get_florida_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get Florida leads - your primary market"""
    query = select(PotentialLead).where(PotentialLead.location_type == "florida")
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.location_type == "florida"
    )

    result = await db.execute(count_query)
    total = result.scalar() or 0

    offset = (page - 1) * per_page
    query = (
        query.order_by(PotentialLead.lead_score.desc().nullslast())
        .offset(offset)
        .limit(per_page)
    )

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get("/leads/caribbean", response_model=LeadListResponse, tags=["Leads"])
async def get_caribbean_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get Caribbean leads"""
    query = select(PotentialLead).where(PotentialLead.location_type == "caribbean")
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.location_type == "caribbean"
    )

    result = await db.execute(count_query)
    total = result.scalar() or 0

    offset = (page - 1) * per_page
    query = (
        query.order_by(PotentialLead.lead_score.desc().nullslast())
        .offset(offset)
        .limit(per_page)
    )

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def get_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single lead by ID"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
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
            detail=f"A lead with a similar name already exists: '{lead_data.hotel_name}'",
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
        lead_score=lead_data.lead_score,
    )

    db.add(lead)
    await db.commit()
    await db.refresh(lead)

    logger.info(
        f"Created lead: {lead.hotel_name} (ID: {lead.id}, Score: {lead.lead_score})"
    )

    return LeadResponse.model_validate(lead)


@app.patch("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def update_lead(
    lead_id: int, updates: LeadUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a lead"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
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
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
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
    reason: Optional[str] = Query(
        None,
        description="Rejection reason: duplicate, budget_brand, international, old_opening, bad_data",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Reject a lead"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
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
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
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
    db: AsyncSession = Depends(get_db),
):
    """List all scraping sources"""
    query = select(Source)

    if active_only:
        query = query.where(Source.is_active.is_(True))
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
    query = (
        select(Source)
        .where(
            Source.is_active.is_(True),
            Source.health_status.in_(["healthy", "new", "degraded"]),
        )
        .order_by(Source.priority.desc())
    )

    result = await db.execute(query)
    sources = result.scalars().all()

    return [SourceResponse.model_validate(source) for source in sources]


@app.get("/sources/problems", response_model=List[SourceResponse], tags=["Sources"])
async def list_problem_sources(db: AsyncSession = Depends(get_db)):
    """List sources with issues (failing/dead)"""
    query = (
        select(Source)
        .where(Source.health_status.in_(["failing", "dead"]))
        .order_by(Source.consecutive_failures.desc())
    )

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
        raise HTTPException(
            status_code=409, detail="Source with this URL already exists"
        )

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
        health_status="new",
    )

    db.add(source)
    await db.commit()
    await db.refresh(source)

    logger.info(f"Created source: {source.name} (ID: {source.id})")

    return SourceResponse.model_validate(source)


@app.post(
    "/sources/{source_id}/toggle", response_model=SourceResponse, tags=["Sources"]
)
async def toggle_source(source_id: int, db: AsyncSession = Depends(get_db)):
    """Toggle source active/inactive"""
    result = await db.execute(select(Source).where(Source.id == source_id))
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


@app.post(
    "/sources/{source_id}/reset-health", response_model=SourceResponse, tags=["Sources"]
)
async def reset_source_health(source_id: int, db: AsyncSession = Depends(get_db)):
    """Reset a source's health status (after fixing issues)"""
    result = await db.execute(select(Source).where(Source.id == source_id))
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
    result = await db.execute(select(Source).where(Source.id == source_id))
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
    db: AsyncSession = Depends(get_db),
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
    tab: str = "pipeline",
    page: int = 1,
    search: str = "",
    score: str = "",
    location: str = "",
    tier: str = "",
    sort: str = "score_desc",
    db: AsyncSession = Depends(get_db),
):
    """Dashboard page with Pipeline/Approved/Rejected tabs"""
    from sqlalchemy import select, func

    # Map tab to status
    tab_status_map = {
        "pipeline": "new",
        "approved": "approved",
        "deleted": "deleted",
        "rejected": "rejected",
    }
    status = tab_status_map.get(tab, "new")

    # Base query
    query = select(PotentialLead).where(PotentialLead.status == status)

    # Filters
    if search:
        search_term = f"%{search}%"
        query = query.where(
            or_(
                PotentialLead.hotel_name.ilike(search_term),
                PotentialLead.city.ilike(search_term),
                PotentialLead.brand.ilike(search_term),
                PotentialLead.state.ilike(search_term),
            )
        )
    if score == "hot":
        query = query.where(PotentialLead.lead_score >= 70)
    elif score == "warm":
        query = query.where(PotentialLead.lead_score.between(50, 69))
    elif score == "cold":
        query = query.where(PotentialLead.lead_score < 50)
    if location:
        query = query.where(PotentialLead.location_type == location)
    if tier:
        query = query.where(PotentialLead.brand_tier == tier)

    # Order — support sort parameter
    sort = request.query_params.get("sort", "score_desc")
    if sort == "newest":
        query = query.order_by(PotentialLead.created_at.desc().nullslast())
    elif sort == "oldest":
        query = query.order_by(PotentialLead.created_at.asc().nullslast())
    elif sort == "score_asc":
        query = query.order_by(PotentialLead.lead_score.asc().nullslast())
    elif sort == "name_asc":
        query = query.order_by(PotentialLead.hotel_name.asc())
    elif sort == "opening":
        query = query.order_by(PotentialLead.opening_date.asc().nullslast())
    else:
        query = query.order_by(PotentialLead.lead_score.desc().nullslast())

    # Pagination
    per_page = 25
    offset = (page - 1) * per_page

    # Get total count for pagination
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total_count = total_result.scalar() or 0
    total_pages = max(1, (total_count + per_page - 1) // per_page)

    # Get leads
    result = await db.execute(query.offset(offset).limit(per_page))
    leads = result.scalars().all()

    pipeline_count_r = await db.execute(
        select(func.count()).where(PotentialLead.status == "new")
    )
    approved_count_r = await db.execute(
        select(func.count()).where(PotentialLead.status == "approved")
    )
    rejected_count_r = await db.execute(
        select(func.count()).where(PotentialLead.status == "rejected")
    )
    deleted_count_r = await db.execute(
        select(func.count()).where(PotentialLead.status == "deleted")
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "leads": leads,
            "active_tab": tab,
            "current_page": page,
            "total_pages": total_pages,
            "pipeline_count": pipeline_count_r.scalar() or 0,
            "approved_count": approved_count_r.scalar() or 0,
            "rejected_count": rejected_count_r.scalar() or 0,
            "deleted_count": deleted_count_r.scalar() or 0,
            "total_count": total_count,
        },
    )


@app.get("/api/dashboard/stats", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_stats_partial(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX partial: Stats cards"""
    stats = await _get_dashboard_stats(db)

    return templates.TemplateResponse(
        "partials/stats.html", {"request": request, "stats": stats}
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
    db: AsyncSession = Depends(get_db),
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
            PotentialLead.hotel_name.ilike(f"%{safe_search}%")
            | PotentialLead.city.ilike(f"%{safe_search}%")
            | PotentialLead.brand.ilike(f"%{safe_search}%")
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    result = await db.execute(count_query)
    total = result.scalar() or 0

    offset = (page - 1) * per_page
    query = (
        query.order_by(
            PotentialLead.lead_score.desc().nullslast(), PotentialLead.created_at.desc()
        )
        .offset(offset)
        .limit(per_page)
    )

    result = await db.execute(query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return templates.TemplateResponse(
        "partials/lead_list.html",
        {
            "request": request,
            "leads": leads,
            "pagination": {
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": pages,
            },
        },
    )


@app.get(
    "/api/dashboard/leads/{lead_id}", response_class=HTMLResponse, tags=["Dashboard"]
)
async def dashboard_lead_detail_partial(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """HTMX partial: Lead detail panel"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(
            content='<div class="p-6 text-center text-red-500">Lead not found</div>',
            status_code=404,
        )

    return templates.TemplateResponse(
        "partials/lead_detail.html", {"request": request, "lead": lead}
    )


@app.get(
    "/api/dashboard/leads/{lead_id}/row",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_lead_row_partial(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """HTMX partial: Return single lead row (for refresh after edit)"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="", status_code=404)

    return templates.TemplateResponse(
        "partials/lead_row.html", {"request": request, "lead": lead}
    )


@app.patch("/api/dashboard/leads/{lead_id}/edit", tags=["Dashboard"])
async def dashboard_edit_lead(
    lead_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    """Edit lead fields from the detail panel"""
    data = await request.json()

    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        from fastapi.responses import JSONResponse

        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    # Editable fields whitelist
    editable_fields = [
        "hotel_name",
        "brand",
        "brand_tier",
        "hotel_type",
        "city",
        "state",
        "country",
        "opening_date",
        "room_count",
        "management_company",
        "developer",
        "owner",
        "contact_name",
        "contact_title",
        "contact_email",
        "contact_phone",
        "description",
        "notes",
    ]

    for field in editable_fields:
        if field in data:
            value = data[field]
            # Convert empty strings to None
            if value == "" or value is None:
                setattr(lead, field, None)
            elif field == "room_count":
                setattr(lead, field, int(value) if value else None)
            else:
                setattr(lead, field, str(value))

    from datetime import datetime, timezone

    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    from fastapi.responses import JSONResponse

    return JSONResponse(content={"status": "ok", "id": lead.id})


@app.post(
    "/api/dashboard/leads/{lead_id}/approve",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_approve_lead(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """HTMX: Approve lead and return updated row"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    lead.status = "approved"
    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    logger.info(f"Dashboard: Approved lead {lead.hotel_name} (ID: {lead.id})")

    return templates.TemplateResponse(
        "partials/lead_row.html", {"request": request, "lead": lead}
    )


@app.post(
    "/api/dashboard/leads/{lead_id}/reject",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_reject_lead(
    request: Request,
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """HTMX: Reject lead and return updated row"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    logger.info(
        f"Dashboard: Rejected lead {lead.hotel_name} (ID: {lead.id}, Reason: {reason})"
    )

    return templates.TemplateResponse(
        "partials/lead_row.html", {"request": request, "lead": lead}
    )


@app.post(
    "/api/dashboard/leads/{lead_id}/restore",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_restore_lead(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """Restore a lead back to 'new' (pipeline) status"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(
            content="<div class='text-red-500 p-2'>Lead not found</div>",
            status_code=404,
        )

    lead.status = "new"
    lead.rejection_reason = None

    from datetime import datetime, timezone

    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    return templates.TemplateResponse(
        "partials/lead_row.html", {"request": request, "lead": lead}
    )


@app.post(
    "/api/dashboard/leads/{lead_id}/delete",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_delete_lead(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """Soft-delete a lead (can be restored from Deleted tab)"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(
            content="<div class='text-red-500 p-2'>Lead not found</div>",
            status_code=404,
        )

    lead.status = "deleted"

    from datetime import datetime, timezone

    lead.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(lead)

    return templates.TemplateResponse(
        "partials/lead_row.html", {"request": request, "lead": lead}
    )


@app.get("/api/dashboard/sources/list", tags=["Dashboard"])
async def dashboard_sources_list(db: AsyncSession = Depends(get_db)):
    """Return all sources with metadata for scrape modal source selection."""
    from datetime import timedelta

    result = await db.execute(
        select(Source)
        .where(Source.is_active.is_(True))
        .order_by(Source.priority.desc(), Source.name)
    )
    sources = result.scalars().all()

    now = datetime.now(timezone.utc)

    # Build category counts
    cat_counts = {}
    cat_labels = {
        "chain_newsroom": "🏨 Chain Newsrooms",
        "luxury_independent": "💎 Luxury & Independent",
        "aggregator": "📰 Aggregators",
        "industry": "🏗️ Industry",
        "florida": "🌴 Florida",
        "caribbean": "🏝️ Caribbean",
        "travel_pub": "✈️ Travel Pubs",
        "pr_wire": "📡 PR Wire",
    }

    all_sources = []
    due_sources = []

    # Frequency → hours threshold
    freq_hours = {
        "daily": 20,
        "every_3_days": 68,
        "twice_weekly": 96,
        "weekly": 160,
        "monthly": 720,
    }

    for src in sources:
        # Count categories
        cat_counts[src.source_type] = cat_counts.get(src.source_type, 0) + 1

        # Gold URL count
        gold_urls = src.gold_urls or {} if hasattr(src, "gold_urls") else {}
        active_gold = sum(1 for m in gold_urls.values() if m.get("miss_streak", 0) < 3)

        source_data = {
            "id": src.id,
            "name": src.name,
            "type": src.source_type,
            "priority": src.priority,
            "frequency": src.scrape_frequency or "daily",
            "health": src.health_status or "new",
            "leads": src.leads_found or 0,
            "gold_count": active_gold,
            "last_scraped": src.last_scraped_at.isoformat()
            if src.last_scraped_at
            else None,
        }
        all_sources.append(source_data)

        # Check if due for scraping
        freq = src.scrape_frequency or "daily"
        threshold = freq_hours.get(freq, 160)  # default weekly

        is_due = False
        reason = ""

        if not src.last_scraped_at:
            is_due = True
            reason = "Never scraped"
        else:
            hours_since = (now - src.last_scraped_at).total_seconds() / 3600
            if hours_since >= threshold:
                is_due = True
                reason = f"{freq} (last: {hours_since:.0f}h ago)"

        if is_due:
            # Determine scrape mode for this source
            scrape_mode = "discover" if active_gold == 0 else "gold"
            needs_discovery = True
            if hasattr(src, "last_discovery_at") and src.last_discovery_at:
                interval = getattr(src, "discovery_interval_days", 7) or 7
                needs_discovery = (now - src.last_discovery_at) > timedelta(
                    days=interval
                )

            if needs_discovery:
                scrape_mode = "discover"

            due_sources.append(
                {
                    **source_data,
                    "reason": reason,
                    "mode": scrape_mode,
                }
            )

    categories = [
        {"type": t, "label": cat_labels.get(t, t), "count": c}
        for t, c in sorted(cat_counts.items())
    ]

    return {
        "sources": all_sources,
        "due_sources": due_sources,
        "categories": categories,
        "total": len(all_sources),
        "total_due": len(due_sources),
    }


@app.post("/api/dashboard/scrape", tags=["Dashboard"])
async def dashboard_trigger_scrape(request: Request):
    try:
        # Parse request body (may be empty for backwards compat)
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass

        mode = body.get("mode", "full")
        source_ids = body.get("source_ids", [])

        # Store the scrape config in a global so the SSE stream can pick it up
        import uuid

        scrape_id = str(uuid.uuid4())

        # Store config for the SSE endpoint to use
        global _pending_scrape_config
        _pending_scrape_config = {
            "mode": mode,
            "source_ids": source_ids,
            "scrape_id": scrape_id,
        }

        logger.info(
            f"Dashboard: Scrape triggered (mode={mode}, sources={len(source_ids) if source_ids else 'all'})"
        )

        return {
            "status": "started",
            "message": f"Scrape job started ({mode} mode)",
            "scrape_id": scrape_id,
            "mode": mode,
            "source_count": len(source_ids) if source_ids else "all",
        }
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger scrape: {e}")
        return {"status": "error", "message": f"Failed to start scrape: {str(e)}"}


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

    # Get scrape config from the POST trigger
    global _pending_scrape_config
    scrape_config = dict(_pending_scrape_config)
    _pending_scrape_config = {}
    config_source_ids = scrape_config.get("source_ids", [])

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
                    select(Source)
                    .where(Source.is_active.is_(True))
                    .order_by(Source.priority.desc())
                )
                sources = result.scalars().all()

            # Filter if specific sources requested
            if config_source_ids:
                sources = [s for s in sources if s.id in config_source_ids]

            total_sources = len(sources)
            # source_names = [s.name for s in sources]

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
                    logger.info(
                        f"Client disconnected during scrape {scrape_id}, stopping pipeline"
                    )
                    break

                source_name = source.name

                # Check for gold URLs (fast scrape mode)
                gold_urls_dict = source.gold_urls or {}
                active_gold = [
                    url
                    for url, meta in gold_urls_dict.items()
                    if meta.get("miss_streak", 0) < 3
                ]

                # Decide: gold mode vs rediscovery
                use_gold = len(active_gold) > 0
                needs_rediscovery = False

                if use_gold and source.last_discovery_at:
                    discovery_interval = source.discovery_interval_days or 7
                    days_since_discovery = (
                        datetime.now(timezone.utc) - source.last_discovery_at
                    ).total_seconds() / 86400
                    if days_since_discovery >= discovery_interval:
                        needs_rediscovery = True
                        use_gold = False  # Force deep crawl to find new gold
                elif use_gold and not source.last_discovery_at:
                    # Has gold URLs but never formally discovered — do a full crawl
                    needs_rediscovery = True
                    use_gold = False

                if needs_rediscovery:
                    mode_label = (
                        f"🔄 Rediscovery (overdue, {len(active_gold)} gold exist)"
                    )
                elif use_gold:
                    mode_label = f"⚡ GOLD ({len(active_gold)} URLs)"
                else:
                    mode_label = "🔍 First Discovery"
                yield f"data: {json.dumps({'type': 'source_start', 'source': source_name, 'current': idx, 'total': total_sources, 'mode': 'gold' if use_gold else 'discover'})}\n\n"
                yield f"data: {json.dumps({'type': 'info', 'message': f'{source_name}: {mode_label}'})}\n\n"

                try:
                    if use_gold:
                        # FAST MODE: Hit gold URLs + follow their links (depth 1)
                        scrape_results = {source_name: []}
                        visited = set()
                        for gold_url in active_gold:
                            try:
                                # 1. Fetch the listing/hub page
                                await orchestrator.scraping_engine.rate_limiter.acquire(
                                    gold_url
                                )
                                result = await orchestrator.scraping_engine.http_scraper.scrape(
                                    gold_url
                                )
                                if result.success:
                                    scrape_results[source_name].append(result)
                                    visited.add(gold_url)

                                    # 2. Extract links and follow depth-1 (new articles)
                                    from bs4 import BeautifulSoup
                                    from urllib.parse import urljoin

                                    soup = BeautifulSoup(result.html or "", "lxml")
                                    links = set()
                                    for a in soup.find_all("a", href=True):
                                        full_url = urljoin(gold_url, a["href"])
                                        if (
                                            full_url not in visited
                                            and source.base_url in full_url
                                        ):
                                            links.add(full_url)

                                    # Fetch up to 15 linked pages
                                    for link_url in list(links)[:15]:
                                        try:
                                            await orchestrator.scraping_engine.rate_limiter.acquire(
                                                link_url
                                            )
                                            link_result = await orchestrator.scraping_engine.http_scraper.scrape(
                                                link_url
                                            )
                                            if link_result.success:
                                                scrape_results[source_name].append(
                                                    link_result
                                                )
                                                visited.add(link_url)
                                        except Exception:
                                            pass
                            except Exception as e:
                                logger.warning(f"Gold URL failed {gold_url[:50]}: {e}")
                        logger.info(
                            f"⚡ Gold mode: {source_name} → {len(scrape_results[source_name])} pages from {len(active_gold)} gold URLs"
                        )
                    else:
                        # DISCOVERY MODE: Deep crawl to find new gold URLs
                        scrape_results = (
                            await orchestrator.scraping_engine.scrape_sources(
                                [source_name], deep=True, max_concurrent=3
                            )
                        )
                    source_pages = 0
                    for sname, results in scrape_results.items():
                        successful = [r for r in results if r.success]
                        source_pages += len(successful)
                        for r in successful:
                            all_pages.append(
                                {
                                    "source_name": sname,
                                    "url": r.url,
                                    "content": r.text or r.html or "",
                                }
                            )

                    if source_pages > 0:
                        sources_successful += 1
                        yield f"data: {json.dumps({'type': 'source_complete', 'source': source_name, 'current': idx, 'total': total_sources, 'pages': source_pages})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'url_error', 'url': source.base_url[:60], 'error': 'No content returned'})}\n\n"

                    # Update source last_scraped_at
                    async with async_session() as session:
                        source_obj = (
                            await session.execute(
                                select(Source).where(Source.id == source.id)
                            )
                        ).scalar_one_or_none()
                        if source_obj:
                            if source_pages > 0:
                                source_obj.record_success(
                                    0
                                )  # lead count updated after extraction
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
                {"url": p["url"], "content": p["content"], "source": p["source_name"]}
                for p in all_pages
            ]

            pipeline_result = await orchestrator.pipeline.process_pages(
                pages_for_pipeline, source_name="Dashboard Scrape"
            )

            leads_extracted = pipeline_result.leads_extracted
            yield f"data: {json.dumps({'type': 'info', 'message': f'Extracted {leads_extracted} leads from {pipeline_result.pages_classified} pages'})}\n\n"

            # --- PHASE 3: DEDUPLICATION via smart deduplicator ---
            if orchestrator.deduplicator and pipeline_result.final_leads:
                yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 3: Deduplication...'})}\n\n"

                leads_for_dedup = [
                    lead.to_dict() for lead in pipeline_result.final_leads
                ]
                merged_leads = orchestrator.deduplicator.deduplicate(leads_for_dedup)
                dedup_stats = orchestrator.deduplicator.get_stats()

                dupes_found = dedup_stats.get("duplicates_found", 0)
                unique_count = len(merged_leads)
                yield f"data: {json.dumps({'type': 'info', 'message': f'Dedup: {dupes_found} duplicates merged, {unique_count} unique leads'})}\n\n"

                # Convert MergedLead objects to dicts for save_leads_to_database
                lead_dicts = []
                for ml in merged_leads:
                    d = {
                        "hotel_name": ml.hotel_name,
                        "brand": ml.brand,
                        "property_type": ml.property_type,
                        "city": ml.city,
                        "state": ml.state,
                        "country": ml.country,
                        "opening_date": ml.opening_date,
                        "room_count": ml.room_count,
                        "contact_name": ml.contact_name,
                        "contact_title": ml.contact_title,
                        "contact_email": ml.contact_email,
                        "contact_phone": ml.contact_phone,
                        "source_url": ml.source_urls[0] if ml.source_urls else "",
                        "source_name": ml.source_names[0] if ml.source_names else "",
                        "key_insights": getattr(ml, "key_insights", ""),
                        "confidence_score": ml.confidence_score,
                        "qualification_score": getattr(ml, "qualification_score", 0),
                    }
                    if ml.merged_from_count > 1:
                        d["key_insights"] = (
                            d.get("key_insights") or ""
                        ) + f"\n\n Merged from {ml.merged_from_count} sources"
                    lead_dicts.append(d)
            else:
                # No deduplicator or no leads
                lead_dicts = [
                    lead.to_dict() for lead in (pipeline_result.final_leads or [])
                ]

            # --- PHASE 4: SAVE TO DATABASE via orchestrator ---
            if lead_dicts:
                yield f"data: {json.dumps({'type': 'info', 'message': f'Phase 4: Saving {len(lead_dicts)} leads to database...'})}\n\n"

                db_result = await orchestrator.save_leads_to_database(lead_dicts)
                leads_saved = db_result["saved"]
                leads_dupes = db_result["duplicates"]

                if leads_saved > 0:
                    yield f"data: {json.dumps({'type': 'leads_found', 'url': 'pipeline', 'found': len(lead_dicts), 'saved': leads_saved, 'total_saved': leads_saved})}\n\n"

                yield f"data: {json.dumps({'type': 'info', 'message': f'Saved {leads_saved} new leads, {leads_dupes} already existed'})}\n\n"
            else:
                leads_saved = 0
                leads_dupes = 0

            # --- PHASE 5: GOLD URL TRACKING & SOURCE STATS ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Updating source intelligence...'})}\n\n"

            try:
                async with async_session() as stats_session:
                    source_id_map = {src.name: src.id for src in sources}

                    # Build map: source_id -> {url: lead_count}
                    url_lead_map = {}

                    if lead_dicts:
                        for lead in lead_dicts:
                            src_url = lead.get("source_url", "")
                            src_name = lead.get("source_name", "") or lead.get(
                                "source", ""
                            )

                            source_id = None
                            for sname, sid in source_id_map.items():
                                if (
                                    sname.lower() in (src_name or "").lower()
                                    or (src_name or "").lower() in sname.lower()
                                ):
                                    source_id = sid
                                    break

                            if source_id and src_url:
                                if source_id not in url_lead_map:
                                    url_lead_map[source_id] = {}
                                url_lead_map[source_id][src_url] = (
                                    url_lead_map[source_id].get(src_url, 0) + 1
                                )

                    # Update each source
                    for src in sources:
                        source_obj = (
                            await stats_session.execute(
                                select(Source).where(Source.id == src.id)
                            )
                        ).scalar_one_or_none()

                        if not source_obj:
                            continue

                        source_obj.total_scrapes = (source_obj.total_scrapes or 0) + 1
                        source_obj.last_scraped_at = datetime.now(timezone.utc)

                        source_leads = (
                            sum(url_lead_map.get(src.id, {}).values())
                            if src.id in url_lead_map
                            else 0
                        )

                        if source_leads > 0:
                            source_obj.leads_found = (
                                source_obj.leads_found or 0
                            ) + source_leads
                            source_obj.last_success_at = datetime.now(timezone.utc)
                            source_obj.consecutive_failures = 0
                            source_obj.health_status = "healthy"

                        scrapes = source_obj.total_scrapes or 1
                        old_avg = float(source_obj.avg_lead_yield or 0)
                        source_obj.avg_lead_yield = (
                            (old_avg * (scrapes - 1)) + source_leads
                        ) / scrapes

                        # Update gold URLs
                        gold = dict(source_obj.gold_urls or {})
                        now_str = datetime.now(timezone.utc).isoformat()

                        if src.id in url_lead_map:
                            for url, count in url_lead_map[src.id].items():
                                # Only record as gold if 2+ leads from same page
                                # (listing/hub pages have multiple leads, individual articles don't)
                                if count < 2 and url not in gold:
                                    continue  # Skip individual article pages

                                if url in gold:
                                    gold[url]["leads_found"] = (
                                        gold[url].get("leads_found", 0) + count
                                    )
                                    gold[url]["last_hit"] = now_str
                                    gold[url]["miss_streak"] = 0
                                    gold[url]["total_checks"] = (
                                        gold[url].get("total_checks", 0) + 1
                                    )
                                else:
                                    gold[url] = {
                                        "leads_found": count,
                                        "last_hit": now_str,
                                        "first_found": now_str,
                                        "miss_streak": 0,
                                        "total_checks": 1,
                                    }

                        # Track misses on existing gold URLs
                        scraped_urls_for_source = [
                            p["url"]
                            for p in all_pages
                            if p.get("source_name") == src.name
                        ]
                        for url in scraped_urls_for_source:
                            if url in gold and (
                                src.id not in url_lead_map
                                or url not in url_lead_map.get(src.id, {})
                            ):
                                gold[url]["miss_streak"] = (
                                    gold[url].get("miss_streak", 0) + 1
                                )
                                gold[url]["total_checks"] = (
                                    gold[url].get("total_checks", 0) + 1
                                )
                                if gold[url]["miss_streak"] >= 3:
                                    logger.info(
                                        f"Gold URL demoted (3 misses): {url[:60]}"
                                    )

                        source_obj.gold_urls = gold
                        source_obj.last_discovery_at = datetime.now(timezone.utc)

                    await stats_session.commit()

                total_gold = sum(len(urls) for urls in url_lead_map.values())
                if total_gold > 0:
                    yield f"data: {json.dumps({'type': 'info', 'message': f'Recorded {total_gold} gold URLs across {len(url_lead_map)} sources'})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'info', 'message': 'Source stats updated (no new gold URLs this run)'})}\n\n"

            except Exception as gold_err:
                logger.error(f"Gold URL tracking error: {gold_err}")
                yield f"data: {json.dumps({'type': 'info', 'message': f'Warning: Source stats update failed: {str(gold_err)[:50]}'})}\n\n"

            # --- COMPLETE ---
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            final_stats = {
                "sources_scraped": sources_successful,
                "urls_scraped": len(all_pages),
                "leads_found": leads_extracted,
                "leads_saved": leads_saved,
                "leads_skipped": leads_dupes,
                "errors": [],
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
            "X-Accel-Buffering": "no",
        },
    )


# =============================================================================
# URL EXTRACT FEATURE - Backend Endpoints
# =============================================================================
# ADD these to app/main.py
#
# STEP 1: Add this global variable near the other globals (around line 50-60,
#          near where _pending_scrape_config is defined):
#
#     _pending_extract_url = ""
#
# STEP 2: Add both endpoint functions BEFORE the cancel endpoint
#          (around line 1920, before @app.post("/api/dashboard/scrape/cancel/..."))
# =============================================================================


# --- ENDPOINT 1: Trigger URL extraction ---
# Paste this after the scrape/stream endpoint block and before scrape/cancel


@app.post("/api/dashboard/extract-url", tags=["Dashboard"])
async def dashboard_extract_url(request: Request):
    """Accept a URL for direct lead extraction"""
    try:
        body = await request.json()
        url = (body.get("url") or "").strip()

        if not url:
            return {"status": "error", "message": "No URL provided"}

        if not url.startswith("http"):
            url = "https://" + url

        # Store for SSE stream to pick up
        global _pending_extract_url
        _pending_extract_url = url

        logger.info(f"Dashboard: URL extract triggered for {url}")

        return {
            "status": "started",
            "message": "Extracting leads from URL",
            "url": url,
        }
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger URL extract: {e}")
        return {"status": "error", "message": f"Failed: {str(e)}"}


# --- ENDPOINT 2: SSE stream for URL extraction progress ---


@app.get("/api/dashboard/extract-url/stream", tags=["Dashboard"])
async def extract_url_stream(request: Request):
    """SSE endpoint for real-time URL extraction progress"""

    global _pending_extract_url
    target_url = _pending_extract_url
    _pending_extract_url = ""

    if not target_url:

        async def empty():
            yield f"data: {json.dumps({'type': 'error', 'message': 'No URL pending. Please click Extract again.'})}\n\n"

        return StreamingResponse(empty(), media_type="text/event-stream")

    async def event_generator():
        orchestrator = None
        try:
            import os

            yield f"data: {json.dumps({'type': 'started', 'scrape_id': 'url-extract'})}\n\n"
            yield f"data: {json.dumps({'type': 'info', 'message': f'Target: {target_url}'})}\n\n"

            # --- Initialize pipeline ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Initializing pipeline...'})}\n\n"

            from app.services.orchestrator import LeadHunterOrchestrator

            orchestrator = LeadHunterOrchestrator(
                gemini_api_key=os.getenv("GEMINI_API_KEY"),
                save_to_database=True,
            )
            await orchestrator.initialize()

            yield f"data: {json.dumps({'type': 'info', 'message': 'Pipeline ready.'})}\n\n"

            start_time = datetime.now(timezone.utc)

            # --- PHASE 1: SCRAPE the URL ---
            yield f"data: {json.dumps({'type': 'source_start', 'source': 'URL Extract', 'current': 1, 'total': 1, 'mode': 'direct'})}\n\n"
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 1: Fetching page content...'})}\n\n"

            # Try scraping with the engine's HTTP scraper
            scrape_result = None
            page_content = ""

            try:
                scrape_result = await orchestrator.scraping_engine.http_scraper.scrape(
                    target_url
                )
                if scrape_result and scrape_result.success:
                    page_content = scrape_result.text or scrape_result.html or ""
                    # Safety: if we got raw HTML instead of clean text, strip it
                    if (
                        page_content.strip().startswith("<")
                        and len(page_content) > 30000
                    ):
                        from bs4 import BeautifulSoup

                        soup = BeautifulSoup(page_content, "lxml")
                        for s in soup(["script", "style", "nav", "footer", "header"]):
                            s.decompose()
                        page_content = soup.get_text(separator="\n", strip=True)
                    yield f"data: {json.dumps({'type': 'source_complete', 'source': 'URL Extract', 'current': 1, 'total': 1, 'pages': 1})}\n\n"
                    yield f"data: {json.dumps({'type': 'info', 'message': f'Page fetched: {len(page_content):,} chars'})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'info', 'message': 'HTTP scraper failed, trying fallback...'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'info', 'message': f'HTTP scraper error: {str(e)[:60]}, trying fallback...'})}\n\n"

            # Fallback: try with httpx directly
            if not page_content:
                try:
                    import httpx

                    async with httpx.AsyncClient(
                        timeout=30,
                        follow_redirects=True,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                        },
                    ) as client:
                        resp = await client.get(target_url)
                        if resp.status_code == 200:
                            # Strip HTML to clean text (same as scraping engine)
                            from bs4 import BeautifulSoup

                            soup = BeautifulSoup(resp.text, "lxml")
                            for s in soup(
                                ["script", "style", "nav", "footer", "header"]
                            ):
                                s.decompose()
                            page_content = soup.get_text(separator="\n", strip=True)
                            yield f"data: {json.dumps({'type': 'source_complete', 'source': 'URL Extract', 'current': 1, 'total': 1, 'pages': 1})}\n\n"
                            yield f"data: {json.dumps({'type': 'info', 'message': f'Page fetched (fallback): {len(page_content):,} chars'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'type': 'error', 'message': f'Failed to fetch URL: HTTP {resp.status_code}'})}\n\n"
                            return
                except Exception as e2:
                    yield f"data: {json.dumps({'type': 'error', 'message': f'All fetch methods failed: {str(e2)[:80]}'})}\n\n"
                    return

            if not page_content:
                yield f"data: {json.dumps({'type': 'error', 'message': 'No content retrieved from URL'})}\n\n"
                return

            # Check if client disconnected
            if await request.is_disconnected():
                return

            # --- PHASE 2: AI EXTRACTION ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 2: AI extraction (Gemini)...'})}\n\n"

            # Extract domain for source name
            from urllib.parse import urlparse

            domain = urlparse(target_url).netloc.replace("www.", "")
            source_label = f"URL Extract ({domain})"

            pages_for_pipeline = [
                {
                    "url": target_url,
                    "content": page_content,
                    "source": source_label,
                }
            ]

            pipeline_result = await orchestrator.pipeline.process_pages(
                pages_for_pipeline,
                source_name=source_label,
            )

            leads_extracted = pipeline_result.leads_extracted
            yield f"data: {json.dumps({'type': 'info', 'message': f'Extracted {leads_extracted} leads from page'})}\n\n"

            if leads_extracted == 0:
                yield f"data: {json.dumps({'type': 'info', 'message': 'No hotel leads found on this page. Try a different URL with hotel opening announcements.'})}\n\n"
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                yield f"data: {json.dumps({'type': 'complete', 'stats': {'sources_scraped': 1, 'urls_scraped': 1, 'leads_found': 0, 'leads_saved': 0, 'leads_skipped': 0}, 'duration_seconds': duration})}\n\n"
                return

            # --- PHASE 3: DEDUPLICATION ---
            if orchestrator.deduplicator and pipeline_result.final_leads:
                yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 3: Deduplication...'})}\n\n"

                leads_for_dedup = [
                    lead.to_dict() for lead in pipeline_result.final_leads
                ]
                merged_leads = orchestrator.deduplicator.deduplicate(leads_for_dedup)
                dedup_stats = orchestrator.deduplicator.get_stats()

                dupes_found = dedup_stats.get("duplicates_found", 0)
                unique_count = len(merged_leads)
                yield f"data: {json.dumps({'type': 'info', 'message': f'Dedup: {dupes_found} duplicates merged, {unique_count} unique leads'})}\n\n"

                # Convert to dicts
                lead_dicts = []
                for ml in merged_leads:
                    d = {
                        "hotel_name": ml.hotel_name,
                        "brand": ml.brand,
                        "property_type": ml.property_type,
                        "city": ml.city,
                        "state": ml.state,
                        "country": ml.country,
                        "opening_date": ml.opening_date,
                        "room_count": ml.room_count,
                        "contact_name": ml.contact_name,
                        "contact_title": ml.contact_title,
                        "contact_email": ml.contact_email,
                        "contact_phone": ml.contact_phone,
                        "source_url": ml.source_urls[0]
                        if ml.source_urls
                        else target_url,
                        "source_name": source_label,
                        "key_insights": getattr(ml, "key_insights", ""),
                        "confidence_score": ml.confidence_score,
                        "qualification_score": getattr(ml, "qualification_score", 0),
                    }
                    lead_dicts.append(d)
            else:
                lead_dicts = [
                    lead.to_dict() for lead in (pipeline_result.final_leads or [])
                ]
                # Ensure source_url is set
                for d in lead_dicts:
                    if not d.get("source_url"):
                        d["source_url"] = target_url
                    if not d.get("source_name"):
                        d["source_name"] = source_label

            # --- PHASE 4: SAVE ---
            leads_saved = 0
            leads_dupes = 0
            if lead_dicts:
                yield f"data: {json.dumps({'type': 'info', 'message': f'Phase 4: Saving {len(lead_dicts)} leads to database...'})}\n\n"

                db_result = await orchestrator.save_leads_to_database(lead_dicts)
                leads_saved = db_result["saved"]
                leads_dupes = db_result["duplicates"]

                if leads_saved > 0:
                    yield f"data: {json.dumps({'type': 'leads_found', 'url': target_url, 'found': len(lead_dicts), 'saved': leads_saved, 'total_saved': leads_saved})}\n\n"

                yield f"data: {json.dumps({'type': 'info', 'message': f'Saved {leads_saved} new leads, {leads_dupes} already existed'})}\n\n"

            # --- COMPLETE ---
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            final_stats = {
                "sources_scraped": 1,
                "urls_scraped": 1,
                "leads_found": leads_extracted,
                "leads_saved": leads_saved,
                "leads_skipped": leads_dupes,
            }

            yield f"data: {json.dumps({'type': 'complete', 'stats': final_stats, 'duration_seconds': duration})}\n\n"

        except Exception as e:
            logger.error(f"URL extract stream error: {e}")
            import traceback

            traceback.print_exc()
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            if orchestrator:
                try:
                    await orchestrator.close()
                except Exception:
                    pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/dashboard/scrape/cancel/{scrape_id}", tags=["Dashboard"])
async def cancel_scrape(scrape_id: str):
    """Cancel an active scrape job"""
    async with _scrape_lock:
        if scrape_id in active_scrapes:
            scrape_cancellations.add(scrape_id)
            return {"status": "cancelling", "message": "Cancellation requested"}
    return {"status": "not_found", "message": "Scrape job not found"}
