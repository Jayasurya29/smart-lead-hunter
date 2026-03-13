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
from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ConfigDict
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text, or_, case, update, literal

import asyncio
import json
import os
import re
import uuid

from app.config import settings
from app.logging_config import setup_logging
from app.database import get_db, init_db, async_session
from app.models import PotentialLead, Source, ScrapeLog
from app.services.utils import normalize_hotel_name, local_now
from app.middleware.auth import APIKeyMiddleware
from app.models.lead_contact import LeadContact
from app.services.lead_factory import save_lead_to_db
from app.services.rescore import rescore_lead
from app.config.intelligence_config import SKIP_URL_PATTERNS

# Global dict to track active scrape jobs and their progress
# Protected by _scrape_lock for async-safe mutation within a single worker.
# NOTE: For multi-worker uvicorn (--workers >1), migrate to Redis hash/pub-sub.
active_scrapes: dict = {}
scrape_cancellations: set = set()
_scrape_lock = asyncio.Lock()
_SCRAPE_TTL = 1800  # 30 minutes — auto-evict stale scrape entries


async def _cleanup_stale_scrapes():
    """Remove scrape entries older than _SCRAPE_TTL (M-05 fix)."""
    async with _scrape_lock:
        now = time.monotonic()
        stale = [
            k
            for k, v in active_scrapes.items()
            if isinstance(v, dict) and now - v.get("_started", now) > _SCRAPE_TTL
        ]
        for k in stale:
            del active_scrapes[k]
            scrape_cancellations.discard(k)


# Keyed by unique ID to prevent race conditions between concurrent users (Audit Fix #3)
_pending_configs: dict = {}  # scrape_id -> {mode, source_ids, ...}
_pending_extract_urls: dict = {}  # extract_id -> url
_pending_discovery_configs: dict = {}  # discovery_id -> {mode, extract_leads, dry_run}
_PENDING_TTL = 300  # 5 minutes — evict stale entries


def _store_pending(store: dict, key: str, value):
    """Store a pending config with timestamp, evicting expired entries."""
    now = time.monotonic()
    store[key] = {"_v": value, "_t": now}
    # Evict expired entries on each write (cheap — typically <10 entries)
    cutoff = now - _PENDING_TTL
    expired = [
        k for k, v in store.items() if isinstance(v, dict) and v.get("_t", 0) < cutoff
    ]
    for k in expired:
        del store[k]


def _pop_pending(store: dict, key: str, default=None):
    """Pop a pending config by key, returning the original value."""
    entry = store.pop(key, None)
    if entry is None:
        return default
    if isinstance(entry, dict) and "_v" in entry:
        return entry["_v"]
    return entry  # Backward compat if raw value stored


# Logging configured by setup_logging() in lifespan
logger = logging.getLogger(__name__)

# M-04: Cached Redis connection for health checks
_health_redis = None


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


def _merged_lead_to_dict(ml, fallback_url: str = "", fallback_source: str = "") -> dict:
    """Convert a MergedLead object to a dict for save_leads_to_database.

    Used by both scrape_with_progress and extract_url_stream SSE endpoints
    to avoid duplicating the 20-field conversion logic.
    """
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
        "source_url": ml.source_urls[0] if ml.source_urls else fallback_url,
        "source_name": ml.source_names[0] if ml.source_names else fallback_source,
        "key_insights": getattr(ml, "key_insights", ""),
        "confidence_score": ml.confidence_score,
        "qualification_score": getattr(ml, "qualification_score", 0),
    }
    if ml.merged_from_count > 1:
        d["key_insights"] = (
            d.get("key_insights") or ""
        ) + f"\n\n Merged from {ml.merged_from_count} sources"
    return d


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
app.add_middleware(APIKeyMiddleware)


# P-03: Simple in-memory rate limiter for API endpoints
# Limits each client IP to a configurable number of requests per window.
# Skips static assets, dashboard HTML, and health checks.
_rate_limit_store: dict = defaultdict(lambda: {"count": 0, "reset": 0.0})
_RATE_LIMIT_MAX = 60  # requests per window
_RATE_LIMIT_WINDOW = 60.0  # seconds
_RATE_LIMIT_MAX_ENTRIES = 10000  # max tracked IPs before forced eviction
_rate_limit_last_cleanup = 0.0


def _safe_error(e: Exception, fallback: str = "Operation failed") -> str:
    """Sanitize error message for frontend (Audit Fix C-03).
    Strips URLs, API keys, and long tracebacks."""
    msg = str(e)
    # Remove anything that looks like a URL with params (could contain keys)
    msg = re.sub(r"https?://[^\s]+", "[URL removed]", msg)
    # Remove anything that looks like an API key
    msg = re.sub(r"[A-Za-z0-9_-]{20,}", "[REDACTED]", msg)
    # Truncate
    if len(msg) > 120:
        msg = msg[:120] + "..."
    return msg or fallback


# Audit Fix M-05: Request body size limit (1 MB)
MAX_BODY_SIZE = 1_048_576  # 1 MB


# Audit Fix H-05: CSRF protection for state-mutating endpoints.
# HTMX sends X-Requested-With by default. Requiring this header
# prevents cross-origin form submissions (CORS preflight blocks it).
def _require_ajax(request: Request):
    """Dependency that rejects non-AJAX requests to prevent CSRF."""
    requested_with = request.headers.get("x-requested-with", "")
    content_type = request.headers.get("content-type", "")
    # Allow: HTMX requests, JSON API calls, or explicit XMLHttpRequest
    if (
        "xmlhttprequest" in requested_with.lower()
        or "hx-request" in request.headers
        or "application/json" in content_type
    ):
        return True
    raise HTTPException(
        status_code=403, detail="CSRF check failed: missing required header"
    )


async def _checked_json(request: Request, max_size: int = MAX_BODY_SIZE) -> dict:
    """Parse JSON body with size limit to prevent DoS."""
    body = await request.body()
    if len(body) > max_size:
        raise HTTPException(status_code=413, detail="Request body too large")
    return json.loads(body)


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


async def _paginate_leads(
    db: AsyncSession,
    base_query,
    count_query,
    page: int,
    per_page: int,
    order_by=None,
):
    """Shared pagination logic for lead list endpoints (M-01).

    Returns: (leads, total, pages)
    """
    result = await db.execute(count_query)
    total = result.scalar() or 0

    offset = (page - 1) * per_page
    if order_by is not None:
        base_query = base_query.order_by(order_by)
    else:
        base_query = base_query.order_by(
            PotentialLead.lead_score.desc().nullslast(), PotentialLead.created_at.desc()
        )
    base_query = base_query.offset(offset).limit(per_page)

    result = await db.execute(base_query)
    leads = result.scalars().all()

    pages = (total + per_page - 1) // per_page if total > 0 else 1
    return leads, total, pages


def _apply_lead_filters(
    query,
    count_query,
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    state: Optional[str] = None,
    location_type: Optional[str] = None,
    brand_tier: Optional[str] = None,
    search: Optional[str] = None,
):
    """Apply common lead filters to both query and count_query (M-01)."""
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
    return query, count_query


def _lead_list_response(leads, total, page, per_page, pages) -> LeadListResponse:
    """Build standard LeadListResponse (M-01)."""
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


async def _get_dashboard_stats(db: AsyncSession) -> dict:
    """Fetch all dashboard stats in 2 queries instead of 12.

    Uses SQLAlchemy conditional aggregation:  count() + filter()
    so the database scans the leads table once and the sources table once.
    """
    now = local_now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())

    # -- Single query for ALL lead counts (Audit Fix P-04: portable case() syntax) --
    lead_result = await db.execute(
        select(
            func.count(PotentialLead.id).label("total"),
            func.sum(case((PotentialLead.status == "new", 1), else_=0)).label("new"),
            func.sum(case((PotentialLead.status == "approved", 1), else_=0)).label(
                "approved"
            ),
            func.sum(case((PotentialLead.status == "pending", 1), else_=0)).label(
                "pending"
            ),
            func.sum(
                case((PotentialLead.status.in_(["rejected", "bad"]), 1), else_=0)
            ).label("rejected"),
            # Hot/Warm now calculated in Python (timeline-based, not score-based)
            literal(0).label("hot"),
            literal(0).label("warm"),
            func.sum(case((PotentialLead.created_at >= today_start, 1), else_=0)).label(
                "today"
            ),
            func.sum(case((PotentialLead.created_at >= week_start, 1), else_=0)).label(
                "this_week"
            ),
            func.sum(case((PotentialLead.status == "deleted", 1), else_=0)).label(
                "deleted"
            ),
        )
    )
    lr = lead_result.one()

    # -- Single query for ALL source counts (Audit Fix P-04) --
    source_result = await db.execute(
        select(
            func.count(Source.id).label("total"),
            func.sum(case((Source.is_active.is_(True), 1), else_=0)).label("active"),
            func.sum(case((Source.health_status == "healthy", 1), else_=0)).label(
                "healthy"
            ),
        )
    )
    sr = source_result.one()

    # Calculate Hot/Warm/Urgent from opening timeline
    from app.services.utils import get_timeline_label

    timeline_result = await db.execute(
        select(PotentialLead.opening_date).where(
            PotentialLead.status.in_(["new", "approved"])
        )
    )
    timeline_counts = {"HOT": 0, "URGENT": 0, "WARM": 0, "COOL": 0}
    for row in timeline_result.scalars().all():
        label = get_timeline_label(row or "")
        if label in timeline_counts:
            timeline_counts[label] += 1

    return {
        "total_leads": lr.total or 0,
        "new_leads": lr.new or 0,
        "approved_leads": lr.approved or 0,
        "pending_leads": lr.pending or 0,
        "rejected_leads": lr.rejected or 0,
        "hot_leads": timeline_counts["HOT"],
        "urgent_leads": timeline_counts["URGENT"],
        "warm_leads": timeline_counts["WARM"],
        "cool_leads": timeline_counts["COOL"],
        "leads_today": lr.today or 0,
        "leads_this_week": lr.this_week or 0,
        "deleted_leads": lr.deleted or 0,
        "total_sources": sr.total or 0,
        "active_sources": sr.active or 0,
        "healthy_sources": sr.healthy or 0,
    }


# -----------------------------------------------------------------------------
# Health & Status Endpoints--------------------------------------------------------------------------


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
    """Health check endpoint — verifies DB, Gemini API, and Redis."""
    components = {}

    # 1. Database
    try:
        await db.execute(text("SELECT 1"))
        components["database"] = "healthy"
    except Exception as e:
        components["database"] = f"unhealthy: {_safe_error(e)}"

    # 2. Gemini API
    try:
        import httpx

        gemini_key = (
            settings.gemini_api_key if hasattr(settings, "gemini_api_key") else None
        )
        if not gemini_key:
            components["gemini"] = "not configured"
        else:
            gemini_model = getattr(settings, "gemini_model", "gemini-2.5-flash")
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}?key={gemini_key}"
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                components["gemini"] = "healthy"
            else:
                components["gemini"] = f"error: HTTP {resp.status_code}"
    except Exception as e:
        components["gemini"] = f"unhealthy: {_safe_error(e)}"

    # 3. Redis
    try:
        redis_url = getattr(settings, "redis_url", None)
        if not redis_url:
            components["redis"] = "not configured"
        else:
            import redis.asyncio as aioredis

            # Reuse module-level connection (M-04: avoid creating per-check)
            global _health_redis
            if "_health_redis" not in globals() or _health_redis is None:
                _health_redis = aioredis.from_url(redis_url, socket_connect_timeout=3)
            try:
                await _health_redis.ping()
                components["redis"] = "healthy"
            except Exception:
                # Connection stale — recreate
                try:
                    await _health_redis.aclose()
                except Exception:
                    pass
                _health_redis = aioredis.from_url(redis_url, socket_connect_timeout=3)
                await _health_redis.ping()
                components["redis"] = "healthy"
    except Exception as e:
        components["redis"] = f"unhealthy: {_safe_error(e)}"

    # Overall status
    healthy_count = sum(1 for v in components.values() if v == "healthy")
    if healthy_count == len(components):
        overall = "healthy"
    elif components.get("database") == "healthy":
        overall = "degraded"
    else:
        overall = "unhealthy"

    return {
        "status": overall,
        "timestamp": local_now().isoformat(),
        "components": components,
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

    query, count_query = _apply_lead_filters(
        query,
        count_query,
        status=status,
        min_score=min_score,
        state=state,
        location_type=location_type,
        brand_tier=brand_tier,
        search=search,
    )

    leads, total, pages = await _paginate_leads(db, query, count_query, page, per_page)
    return _lead_list_response(leads, total, page, per_page, pages)


@app.get("/leads/hot", response_model=LeadListResponse, tags=["Leads"])
async def get_hot_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get hot leads (score >= config threshold) - ready for outreach"""
    where = [
        PotentialLead.lead_score >= settings.hot_lead_threshold,
        PotentialLead.status == "new",
    ]
    query = select(PotentialLead).where(*where)
    count_query = select(func.count(PotentialLead.id)).where(*where)

    leads, total, pages = await _paginate_leads(
        db,
        query,
        count_query,
        page,
        per_page,
        order_by=PotentialLead.lead_score.desc(),
    )
    return _lead_list_response(leads, total, page, per_page, pages)


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

    leads, total, pages = await _paginate_leads(
        db,
        query,
        count_query,
        page,
        per_page,
        order_by=PotentialLead.lead_score.desc().nullslast(),
    )
    return _lead_list_response(leads, total, page, per_page, pages)


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

    leads, total, pages = await _paginate_leads(
        db,
        query,
        count_query,
        page,
        per_page,
        order_by=PotentialLead.lead_score.desc().nullslast(),
    )
    return _lead_list_response(leads, total, page, per_page, pages)


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
    """Create a new lead manually — routed through shared lead factory."""
    lead_dict = lead_data.model_dump()
    lead_dict["source_site"] = lead_dict.get("source_site") or "manual"

    result = await save_lead_to_db(lead_dict, db, commit=True)

    if result["status"] == "skipped":
        raise HTTPException(status_code=422, detail=result["reason"])
    if result["status"] in ("duplicate", "enriched"):
        raise HTTPException(
            status_code=409,
            detail=f"A lead with a similar name already exists (ID: {result['id']})",
        )

    # Fetch the saved lead for response
    lead = (
        await db.execute(select(PotentialLead).where(PotentialLead.id == result["id"]))
    ).scalar_one()

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

    lead.updated_at = local_now()

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
    lead.updated_at = local_now()

    # Push to Insightly CRM
    from app.services.insightly import get_insightly_client

    crm = get_insightly_client()
    if crm.enabled and not lead.insightly_id:
        lead_data = {
            "hotel_name": lead.hotel_name,
            "brand": lead.brand,
            "brand_tier": lead.brand_tier,
            "city": lead.city,
            "state": lead.state,
            "country": lead.country or "USA",
            "opening_date": lead.opening_date,
            "room_count": lead.room_count or 0,
            "lead_score": lead.lead_score or 0,
            "description": lead.description,
            "source_url": lead.source_url,
            "management_company": lead.management_company,
            "developer": lead.developer,
            "owner": lead.owner,
            "status": "approved",
            "id": lead.id,
        }
        result = await crm.push_lead(lead_data)
        if result:
            lead.insightly_id = result.get("RECORD_ID")
            logger.info(f"Insightly: synced {lead.hotel_name} → ID {lead.insightly_id}")
        else:
            logger.warning(f"Insightly: failed to sync {lead.hotel_name}")

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
    lead.updated_at = local_now()

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
    source.updated_at = local_now()

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
    source.updated_at = local_now()

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
    added: str = "",
    db: AsyncSession = Depends(get_db),
):
    """Dashboard page with Pipeline/Approved/Rejected tabs"""
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
    now = local_now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Filters (Audit Fix #6: escape LIKE wildcards in search input)
    if search:
        safe_search = escape_like(search)
        search_term = f"%{safe_search}%"
        query = query.where(
            or_(
                PotentialLead.hotel_name.ilike(search_term),
                PotentialLead.city.ilike(search_term),
                PotentialLead.brand.ilike(search_term),
                PotentialLead.state.ilike(search_term),
            )
        )
    if score in ("hot", "urgent", "warm", "cool", "late", "expired", "tbd"):
        from app.services.utils import get_timeline_label

        timeline_q = await db.execute(
            select(PotentialLead.id, PotentialLead.opening_date).where(
                PotentialLead.status.in_(["new", "approved"])
            )
        )
        label_map = {
            "hot": "HOT",
            "urgent": "URGENT",
            "warm": "WARM",
            "cool": "COOL",
            "late": "LATE",
            "expired": "EXPIRED",
            "tbd": "TBD",
        }
        matching_ids = [
            row[0]
            for row in timeline_q.all()
            if get_timeline_label(row[1] or "") == label_map[score]
        ]
        if matching_ids:
            query = query.where(PotentialLead.id.in_(matching_ids))
        else:
            query = query.where(PotentialLead.id == -1)

    if location:
        south_fl_cities = [
            "miami",
            "miami beach",
            "fort lauderdale",
            "hallandale beach",
            "west palm beach",
            "palm beach",
            "boca raton",
            "hollywood",
            "deerfield beach",
            "delray beach",
            "aventura",
            "coral gables",
            "key west",
            "key biscayne",
            "sweetwater",
            "doral",
            "hialeah",
            "homestead",
            "sunny isles beach",
            "surfside",
            "bal harbour",
            "north miami",
            "north miami beach",
            "miami gardens",
            "miami lakes",
            "coconut grove",
            "pompano beach",
            "lauderdale by the sea",
            "plantation",
            "weston",
            "davie",
            "sunrise",
            "pembroke pines",
            "miramar",
            "cooper city",
            "boynton beach",
            "jupiter",
            "riviera beach",
            "lake worth",
            "naples",
            "bonita springs",
            "marco island",
            "fort myers",
            "cape coral",
            "sarasota",
            "clearwater",
            "st. petersburg",
            "st petersburg",
        ]
        caribbean_countries = [
            "dominican republic",
            "bahamas",
            "jamaica",
            "cayman islands",
            "barbados",
            "aruba",
            "turks & caicos islands",
            "turks and caicos",
            "saint lucia",
            "st. lucia",
            "curacao",
            "u.s. virgin islands",
            "antigua and barbuda",
            "trinidad and tobago",
            "puerto rico",
        ]
        southeast_states = [
            "georgia",
            "tennessee",
            "south carolina",
            "north carolina",
            "alabama",
            "mississippi",
            "arkansas",
            "virginia",
        ]
        mountain_states = [
            "utah",
            "wyoming",
            "idaho",
            "colorado",
            "montana",
            "arizona",
            "new mexico",
        ]

        if location == "south_florida":
            query = query.where(func.lower(PotentialLead.city).in_(south_fl_cities))
        elif location == "rest_florida":
            query = query.where(
                func.lower(PotentialLead.state) == "florida",
                ~func.lower(PotentialLead.city).in_(south_fl_cities),
            )
        elif location == "caribbean":
            query = query.where(
                func.lower(PotentialLead.country).in_(caribbean_countries)
            )
        elif location == "california":
            query = query.where(func.lower(PotentialLead.state) == "california")
        elif location == "new_york":
            query = query.where(func.lower(PotentialLead.state) == "new york")
        elif location == "texas":
            query = query.where(func.lower(PotentialLead.state) == "texas")
        elif location == "southeast":
            query = query.where(func.lower(PotentialLead.state).in_(southeast_states))
        elif location == "mountain":
            query = query.where(func.lower(PotentialLead.state).in_(mountain_states))
    if added:
        if added == "this_week":
            week_start = today_start - timedelta(days=now.weekday())
            query = query.where(PotentialLead.created_at >= week_start)
        elif added == "last_7":
            query = query.where(
                PotentialLead.created_at >= today_start - timedelta(days=7)
            )
        elif added == "last_30":
            query = query.where(
                PotentialLead.created_at >= today_start - timedelta(days=30)
            )
    if tier:
        query = query.where(PotentialLead.brand_tier == tier)

    # Order — support sort parameter
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

    # Audit Fix M-01 + P-04: Single query, portable case() syntax
    _tab_counts_r = await db.execute(
        select(
            func.sum(case((PotentialLead.status == "new", 1), else_=0)).label("new"),
            func.sum(case((PotentialLead.status == "approved", 1), else_=0)).label(
                "approved"
            ),
            func.sum(case((PotentialLead.status == "rejected", 1), else_=0)).label(
                "rejected"
            ),
            func.sum(case((PotentialLead.status == "deleted", 1), else_=0)).label(
                "deleted"
            ),
        )
    )
    _tc = _tab_counts_r.one()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "leads": leads,
            "active_tab": tab,
            "current_page": page,
            "total_pages": total_pages,
            "pipeline_count": _tc.new or 0,
            "approved_count": _tc.approved or 0,
            "rejected_count": _tc.rejected or 0,
            "deleted_count": _tc.deleted or 0,
            "total_count": total_count,
            "api_auth_key": os.getenv("API_AUTH_KEY", ""),
        },
    )


@app.get("/api/dashboard/stats", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_stats_partial(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX partial: Stats cards"""
    stats = await _get_dashboard_stats(db)

    return templates.TemplateResponse(request, "partials/stats.html", {"stats": stats})


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

    query, count_query = _apply_lead_filters(
        query,
        count_query,
        status=status,
        min_score=min_score,
        location_type=location_type,
        brand_tier=brand_tier,
        search=search,
    )

    leads, total, pages = await _paginate_leads(db, query, count_query, page, per_page)

    return templates.TemplateResponse(
        request,
        "partials/lead_list.html",
        {
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
        request, "partials/lead_detail.html", {"lead": lead}
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

    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@app.patch("/api/dashboard/leads/{lead_id}/edit", tags=["Dashboard"])
async def dashboard_edit_lead(
    lead_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(_require_ajax),
):
    """Edit lead fields from the detail panel"""
    data = await _checked_json(request)

    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
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
                try:
                    setattr(lead, field, int(value) if value else None)
                except (ValueError, TypeError):
                    pass  # Skip invalid room_count
            else:
                setattr(lead, field, str(value))

    # Audit Fix: Keep normalized name in sync when hotel_name changes
    if "hotel_name" in data and data["hotel_name"]:
        lead.hotel_name_normalized = normalize_hotel_name(data["hotel_name"])

    # Audit Fix 5b: Wrap room_count safely
    # Rescore lead after edits
    tier_points_map = {
        "tier1_ultra_luxury": 25,
        "tier2_luxury": 20,
        "tier3_upper_upscale": 15,
        "tier4_upscale": 10,
        "tier5_skip": 0,
        "unknown": 0,
    }
    scoring_fields = {
        "hotel_name",
        "brand",
        "city",
        "state",
        "country",
        "opening_date",
        "room_count",
        "description",
    }
    scoring_changed = any(f in data for f in scoring_fields)

    if scoring_changed:
        # Full rescore with enriched contacts
        await db.flush()
        await rescore_lead(lead.id, db)
        if "brand_tier" in data and data["brand_tier"]:
            auto_points = (lead.score_breakdown or {}).get("brand", {}).get("points", 0)
            manual_points = tier_points_map.get(data["brand_tier"], 0)
            lead.lead_score = lead.lead_score - auto_points + manual_points
            lead.brand_tier = data["brand_tier"]
    elif "brand_tier" in data and data["brand_tier"]:
        old_points = tier_points_map.get(lead.brand_tier or "unknown", 0)
        new_points = tier_points_map.get(data["brand_tier"], 0)
        lead.lead_score = (lead.lead_score or 0) - old_points + new_points
        lead.brand_tier = data["brand_tier"]

    lead.updated_at = local_now()
    await db.commit()
    await db.refresh(lead)
    return JSONResponse(
        content={
            "status": "ok",
            "id": lead.id,
            "new_score": lead.lead_score,
            "new_tier": lead.brand_tier,
        }
    )


@app.post(
    "/api/dashboard/leads/{lead_id}/approve",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_approve_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(_require_ajax),
):
    """HTMX: Approve lead and return updated row"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    # Block approve if no contacts — must enrich first
    from app.models.lead_contact import LeadContact

    contacts_result = await db.execute(
        select(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .order_by(LeadContact.score.desc())
    )
    contacts = [c.to_dict() for c in contacts_result.scalars().all()]
    if not contacts:
        return HTMLResponse(
            content='<div class="text-red-600 text-sm font-medium p-2">Enrich first — no contacts to push to CRM</div>',
            status_code=200,
        )

    lead.status = "approved"
    lead.updated_at = local_now()

    # Push contacts as Insightly Leads
    from app.services.insightly import get_insightly_client

    crm = get_insightly_client()
    if crm.enabled and not lead.insightly_id:
        pushed = await crm.push_contacts_as_leads(
            contacts=contacts,
            hotel_name=lead.hotel_name,
            brand=lead.brand or "",
            brand_tier=lead.brand_tier or "",
            city=lead.city or "",
            state=lead.state or "",
            country=lead.country or "USA",
            opening_date=lead.opening_date or "",
            room_count=lead.room_count or 0,
            lead_score=lead.lead_score or 0,
            description=lead.description or "",
            source_url=lead.source_url or "",
            management_company=lead.management_company or "",
            developer=lead.developer or "",
            owner=lead.owner or "",
            slh_lead_id=lead.id,
        )
        successful = [p for p in pushed if p[1]]
        if successful:
            lead.insightly_id = successful[0][1]  # Store first Lead ID as reference
            logger.info(
                f"Insightly: pushed {len(successful)} contacts for "
                f"{lead.hotel_name} → Lead IDs: {[p[1] for p in successful]}"
            )
        else:
            logger.warning(f"Insightly: failed to push contacts for {lead.hotel_name}")

    await db.commit()
    await db.refresh(lead)

    logger.info(f"Dashboard: Approved lead {lead.hotel_name} (ID: {lead.id})")

    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


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
    _csrf=Depends(_require_ajax),
):
    """HTMX: Reject lead and return updated row"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = local_now()

    # Remove from Insightly if previously pushed
    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            deleted = await crm.delete_leads_by_slh_id(lead.id)
            logger.info(f"Insightly: deleted {deleted} leads for {lead.hotel_name}")
        lead.insightly_id = None

    await db.commit()
    await db.refresh(lead)

    logger.info(
        f"Dashboard: Rejected lead {lead.hotel_name} (ID: {lead.id}, Reason: {reason})"
    )

    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@app.post(
    "/api/dashboard/leads/{lead_id}/restore",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_restore_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(_require_ajax),
):
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        return HTMLResponse(
            content="<div class='text-red-500 p-2'>Lead not found</div>",
            status_code=404,
        )
    lead.status = "new"
    lead.rejection_reason = None
    lead.updated_at = local_now()

    # Remove from Insightly if previously pushed
    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            deleted = await crm.delete_leads_by_slh_id(lead.id)
            logger.info(f"Insightly: deleted {deleted} leads for {lead.hotel_name}")
        lead.insightly_id = None

    await db.commit()
    await db.refresh(lead)
    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@app.post(
    "/api/dashboard/leads/{lead_id}/delete",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_delete_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(_require_ajax),
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

    lead.updated_at = local_now()

    await db.commit()

    # Return empty response so HTMX removes the row from the current tab
    return HTMLResponse(content="", status_code=200)


@app.get("/api/dashboard/sources/list", tags=["Dashboard"])
async def dashboard_sources_list(db: AsyncSession = Depends(get_db)):
    """Return all sources with metadata for scrape modal source selection."""

    result = await db.execute(
        select(Source)
        .where(Source.is_active.is_(True))
        .order_by(Source.priority.desc(), Source.name)
    )
    sources = result.scalars().all()

    now = local_now()

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
async def dashboard_trigger_scrape(request: Request, _csrf=Depends(_require_ajax)):
    try:
        # Parse request body (may be empty for backwards compat)
        body = {}
        try:
            body = await _checked_json(request)
        except Exception:
            pass

        mode = body.get("mode", "full")
        source_ids = body.get("source_ids", [])

        scrape_id = str(uuid.uuid4())
        _store_pending(
            _pending_configs,
            scrape_id,
            {
                "mode": mode,
                "source_ids": source_ids,
            },
        )

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
        return {
            "status": "error",
            "message": f"Failed to start scrape: {_safe_error(e)}",
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

    # Get scrape config by ID from query param (Audit Fix #3 — race-safe)
    scrape_id = request.query_params.get("scrape_id", "")
    if not scrape_id or scrape_id not in _pending_configs:

        async def no_config():
            err = {
                "type": "error",
                "message": "No scrape config found. Please trigger scrape again.",
            }
            yield "data: " + json.dumps(err) + "\n\n"

        return StreamingResponse(no_config(), media_type="text/event-stream")

    scrape_config = _pop_pending(_pending_configs, scrape_id, {})
    config_source_ids = scrape_config.get("source_ids", [])

    async with _scrape_lock:
        active_scrapes[scrape_id] = {"status": "starting", "_started": time.monotonic()}
    # Periodic cleanup of stale entries (M-05)
    await _cleanup_stale_scrapes()

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

            start_time = local_now()

            # Load Source Intelligence for adaptive scraping
            from app.services.source_intelligence import SourceIntelligence

            source_intel_map = {}  # source_id -> SourceIntelligence

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

                # Load Source Intelligence (adaptive settings)
                scrape_settings = None
                try:
                    intel = SourceIntelligence(source)
                    source_intel_map[source.id] = intel
                    scrape_settings = intel.get_scrape_settings()
                    if scrape_settings.should_skip:
                        skip_msg = (
                            f"Skipping {source_name}: {scrape_settings.skip_reason}"
                        )
                        yield f"data: {json.dumps({'type': 'info', 'message': skip_msg})}\n\n"
                        continue
                except Exception as intel_err:
                    logger.warning(
                        f"Intelligence load failed for {source_name}: {intel_err}"
                    )

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
                        local_now() - source.last_discovery_at
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
                                # 1. Fetch the listing/hub page (adaptive delay)
                                if (
                                    scrape_settings
                                    and scrape_settings.delay_seconds > 1.0
                                ):
                                    import asyncio as _aio

                                    await _aio.sleep(scrape_settings.delay_seconds)
                                await orchestrator.scraping_engine.rate_limiter.acquire(
                                    gold_url
                                )
                                # Disconnect check before scrape call (Audit Fix C-05)
                                if await request.is_disconnected():
                                    return

                                result = await orchestrator.scraping_engine.http_scraper.scrape(
                                    gold_url
                                )
                                # Record response to intelligence
                                if scrape_settings and source.id in source_intel_map:
                                    src_intel = source_intel_map[source.id]
                                    if result.status_code in (429, 403):
                                        src_intel.record_rate_limit(result.status_code)
                                        logger.warning(
                                            f"Rate limit {result.status_code} from {source_name}"
                                        )
                                    if result.crawl_time_ms:
                                        src_intel.record_url_result(
                                            url=gold_url,
                                            produced_lead=False,
                                            response_time_ms=result.crawl_time_ms,
                                        )

                                if result.success:
                                    scrape_results[source_name].append(result)
                                    visited.add(gold_url)

                                    # 2. Extract links and follow depth-1 (new articles)
                                    from bs4 import BeautifulSoup
                                    from urllib.parse import urljoin

                                    soup = BeautifulSoup(result.html or "", "lxml")
                                    links = set()
                                    # M-10: Filter out junk URLs before following
                                    _skip_patterns = SKIP_URL_PATTERNS
                                    from urllib.parse import urlparse

                                    gold_domain = urlparse(gold_url).netloc
                                    for a in soup.find_all("a", href=True):
                                        full_url = urljoin(gold_url, a["href"])
                                        if (
                                            full_url not in visited
                                            and urlparse(full_url).netloc == gold_domain
                                            and not any(
                                                skip in full_url.lower()
                                                for skip in _skip_patterns
                                            )
                                        ):
                                            # Intelligence junk filter
                                            import re as _re

                                            is_junk = False
                                            if (
                                                scrape_settings
                                                and scrape_settings.junk_patterns
                                            ):
                                                for jp in scrape_settings.junk_patterns:
                                                    try:
                                                        if _re.search(jp, full_url):
                                                            is_junk = True
                                                            break
                                                    except _re.error:
                                                        pass
                                            if not is_junk:
                                                links.add(full_url)

                                    # Fetch linked pages (capped by intelligence)
                                    max_follow = (
                                        scrape_settings.max_pages
                                        if scrape_settings
                                        else 15
                                    )
                                    for link_url in list(links)[:max_follow]:
                                        try:
                                            # Adaptive delay from intelligence
                                            if (
                                                scrape_settings
                                                and scrape_settings.delay_seconds > 1.0
                                            ):
                                                import asyncio as _aio

                                                await _aio.sleep(
                                                    scrape_settings.delay_seconds
                                                )
                                            await orchestrator.scraping_engine.rate_limiter.acquire(
                                                link_url
                                            )
                                            link_result = await orchestrator.scraping_engine.http_scraper.scrape(
                                                link_url
                                            )
                                            # Track rate limits on followed links
                                            if (
                                                link_result.status_code in (429, 403)
                                                and source.id in source_intel_map
                                            ):
                                                source_intel_map[
                                                    source.id
                                                ].record_rate_limit(
                                                    link_result.status_code
                                                )
                                                break  # Stop following links if rate limited

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
                    # Log intelligence summary
                    if source.id in source_intel_map:
                        _si = source_intel_map[source.id]
                        _junk_count = len(_si.patterns.get("junk", []))
                        _gold_count = len(_si.patterns.get("gold", []))
                        logger.info(
                            f"Intelligence: {source_name} | "
                            f"score={_si.efficiency_score} | "
                            f"delay={scrape_settings.delay_seconds if scrape_settings else 1.0}s | "
                            f"{_gold_count} gold, {_junk_count} junk patterns"
                        )

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
                    yield f"data: {json.dumps({'type': 'url_error', 'url': source.base_url[:60], 'error': _safe_error(e)})}\n\n"

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

            # Disconnect check before Gemini processing (Audit Fix C-05)
            if await request.is_disconnected():
                return

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
                lead_dicts = [_merged_lead_to_dict(ml) for ml in merged_leads]
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
                        source_obj.last_scraped_at = local_now()

                        source_leads = (
                            sum(url_lead_map.get(src.id, {}).values())
                            if src.id in url_lead_map
                            else 0
                        )

                        if source_leads > 0:
                            source_obj.leads_found = (
                                source_obj.leads_found or 0
                            ) + source_leads
                            source_obj.last_success_at = local_now()
                            source_obj.consecutive_failures = 0
                            source_obj.health_status = "healthy"

                        scrapes = source_obj.total_scrapes or 1
                        old_avg = float(source_obj.avg_lead_yield or 0)
                        source_obj.avg_lead_yield = (
                            (old_avg * (scrapes - 1)) + source_leads
                        ) / scrapes

                        # Update gold URLs
                        gold = dict(source_obj.gold_urls or {})
                        now_str = local_now().isoformat()

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
                        source_obj.last_discovery_at = local_now()

                        # --- SOURCE INTELLIGENCE: Record & Learn ---
                        if source_obj.id in source_intel_map:
                            try:
                                src_intel = source_intel_map[source_obj.id]
                                src_pages = [
                                    p
                                    for p in all_pages
                                    if p.get("source_name") == src.name
                                ]
                                for pg in src_pages:
                                    pg_url = pg.get("url", "")
                                    pg_leads = url_lead_map.get(src.id, {}).get(
                                        pg_url, 0
                                    )
                                    src_intel.record_url_result(
                                        url=pg_url,
                                        produced_lead=pg_leads > 0,
                                        lead_count=pg_leads,
                                    )
                                src_intel.record_scrape_run(
                                    pages_scraped=len(src_pages),
                                    leads_found=source_leads,
                                    leads_saved=source_leads,
                                    duration_seconds=0,
                                    mode="gold"
                                    if source_obj.gold_urls
                                    else "discovery",
                                )
                                src_intel.save()
                                source_obj.source_intelligence = dict(src_intel._data)
                                logger.info(
                                    f"Brain updated: {src.name} (score={src_intel.efficiency_score})"
                                )
                            except Exception as intel_err:
                                logger.warning(
                                    f"Intelligence record failed for {src.name}: {intel_err}"
                                )

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
            end_time = local_now()
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
            yield f"data: {json.dumps({'type': 'error', 'message': _safe_error(e)})}\n\n"
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
#          near where _pending_configs is defined):
#
#     _pending_extract_url = ""
#
# STEP 2: Add both endpoint functions BEFORE the cancel endpoint
#          (around line 1920, before @app.post("/api/dashboard/scrape/cancel/..."))
# =============================================================================


# --- ENDPOINT 1: Trigger URL extraction ---
# Paste this after the scrape/stream endpoint block and before scrape/cancel


@app.post("/api/dashboard/extract-url", tags=["Dashboard"])
async def dashboard_extract_url(request: Request, _csrf=Depends(_require_ajax)):
    """Accept a URL for direct lead extraction"""
    try:
        body = await _checked_json(request)
        url = (body.get("url") or "").strip()

        if not url:
            return {"status": "error", "message": "No URL provided"}

        if not url.startswith("http"):
            url = "https://" + url

        # Store keyed by unique ID (Audit Fix #3 — race-safe)
        extract_id = str(uuid.uuid4())
        _store_pending(_pending_extract_urls, extract_id, url)

        logger.info(f"Dashboard: URL extract triggered for {url}")

        return {
            "status": "started",
            "message": "Extracting leads from URL",
            "url": url,
            "extract_id": extract_id,
        }
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger URL extract: {e}")
        return {"status": "error", "message": f"Failed: {_safe_error(e)}"}


# --- ENDPOINT 2: SSE stream for URL extraction progress ---


@app.get("/api/dashboard/extract-url/stream", tags=["Dashboard"])
async def extract_url_stream(request: Request):
    """SSE endpoint for real-time URL extraction progress"""

    extract_id = request.query_params.get("extract_id", "")
    target_url = (
        _pop_pending(_pending_extract_urls, extract_id, "") if extract_id else ""
    )

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

            start_time = local_now()

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
                        from app.services.utils import clean_html_to_text

                        page_content = clean_html_to_text(page_content)
                    yield f"data: {json.dumps({'type': 'source_complete', 'source': 'URL Extract', 'current': 1, 'total': 1, 'pages': 1})}\n\n"
                    yield f"data: {json.dumps({'type': 'info', 'message': f'Page fetched: {len(page_content):,} chars'})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'info', 'message': 'HTTP scraper failed, trying fallback...'})}\n\n"
            except Exception as e:
                _err_msg = f"HTTP scraper error: {_safe_error(e)}, trying fallback..."
                yield f"data: {json.dumps({'type': 'info', 'message': _err_msg})}\n\n"

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

                            from app.services.utils import clean_html_to_text

                            page_content = clean_html_to_text(resp.text)
                            yield f"data: {json.dumps({'type': 'source_complete', 'source': 'URL Extract', 'current': 1, 'total': 1, 'pages': 1})}\n\n"
                            yield f"data: {json.dumps({'type': 'info', 'message': f'Page fetched (fallback): {len(page_content):,} chars'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'type': 'error', 'message': f'Failed to fetch URL: HTTP {resp.status_code}'})}\n\n"
                            return
                except Exception as e2:
                    _err = f"All fetch methods failed: {_safe_error(e2)}"
                    yield f"data: {json.dumps({'type': 'error', 'message': _err})}\n\n"
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

            # Disconnect check before Gemini processing (Audit Fix C-05)
            if await request.is_disconnected():
                return

            pipeline_result = await orchestrator.pipeline.process_pages(
                pages_for_pipeline,
                source_name=source_label,
            )

            leads_extracted = pipeline_result.leads_extracted
            yield f"data: {json.dumps({'type': 'info', 'message': f'Extracted {leads_extracted} leads from page'})}\n\n"

            if leads_extracted == 0:
                yield f"data: {json.dumps({'type': 'info', 'message': 'No hotel leads found on this page. Try a different URL with hotel opening announcements.'})}\n\n"
                end_time = local_now()
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
                lead_dicts = [
                    _merged_lead_to_dict(
                        ml, fallback_url=target_url, fallback_source=source_label
                    )
                    for ml in merged_leads
                ]
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
            end_time = local_now()
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
            logger.error(f"URL extract stream error: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': _safe_error(e)})}\n\n"
        finally:
            if orchestrator:
                try:
                    await orchestrator.close()
                except Exception:
                    pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/dashboard/scrape/cancel/{scrape_id}", tags=["Dashboard"])
async def cancel_scrape(scrape_id: str, _csrf=Depends(_require_ajax)):
    """Cancel an active scrape job"""
    async with _scrape_lock:
        if scrape_id in active_scrapes:
            scrape_cancellations.add(scrape_id)
            return {"status": "cancelling", "message": "Cancellation requested"}
    return {"status": "not_found", "message": "Scrape job not found"}


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Start Discovery (stores config, returns immediately)
# ─────────────────────────────────────────────────────────────────────────────


@app.post("/api/dashboard/discovery/start", tags=["Dashboard"])
async def discovery_start(request: Request, _csrf=Depends(_require_ajax)):
    """Trigger a web discovery run from the dashboard"""
    try:
        body = await _checked_json(request)
        mode = body.get("mode", "full")
        extract_leads = body.get("extract_leads", True)
        dry_run = body.get("dry_run", False)

        # Store keyed by unique ID (Audit Fix #3 — race-safe)
        discovery_id = str(uuid.uuid4())
        _store_pending(
            _pending_discovery_configs,
            discovery_id,
            {
                "mode": mode,
                "extract_leads": extract_leads,
                "dry_run": dry_run,
            },
        )

        logger.info(
            f"Dashboard: Discovery triggered (mode={mode}, leads={extract_leads}, dry_run={dry_run})"
        )

        return {
            "status": "started",
            "message": f"Discovery started ({mode} mode)",
            "mode": mode,
            "discovery_id": discovery_id,
        }
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger discovery: {e}")
        return {"status": "error", "message": f"Failed: {_safe_error(e)}"}


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Discovery SSE Stream (asyncio-based, matches v5 engine)
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/api/dashboard/discovery/stream", tags=["Dashboard"])
async def discovery_stream(request: Request):
    """SSE endpoint for real-time web discovery progress."""

    # Get config by discovery_id query param (Audit Fix #3 — race-safe)
    discovery_id = request.query_params.get("discovery_id", "")
    config = (
        _pop_pending(_pending_discovery_configs, discovery_id, {})
        if discovery_id
        else {}
    )

    mode = config.get("mode", "full")
    extract_leads = config.get("extract_leads", True)
    dry_run = config.get("dry_run", False)

    async def event_generator():
        try:
            import sys
            import os

            sys.path.insert(0, os.getcwd())

            yield f"data: {json.dumps({'type': 'phase', 'message': '🌐 Initializing Web Discovery Engine v5...'})}\n\n"

            # v5 constructor: no use_ai param, uses IntelligentPipeline automatically
            max_queries = 5 if mode == "quick" else None
            start_time = local_now()

            progress_queue = asyncio.Queue()

            async def run_discovery():
                try:
                    from discover_sources import WebDiscoveryEngine

                    eng = WebDiscoveryEngine(
                        dry_run=dry_run,
                        min_quality=35,
                        sources_only=not extract_leads,
                    )
                    await eng.initialize()

                    # Audit Fix M-09: Use contextlib.redirect_stdout instead of
                    # monkey-patching builtins.print (global mutation).
                    import io
                    import contextlib

                    def _classify_msg_type(msg):
                        if any(
                            s in msg
                            for s in ["\u2705", "\u2728", "Found", "Added", "QUALIFIED"]
                        ):
                            return "success"
                        if any(s in msg for s in ["\u274c", "Error", "Failed"]):
                            return "error"
                        if any(
                            s in msg
                            for s in ["\u26a0\ufe0f", "Warning", "Skip", "\u26aa"]
                        ):
                            return "warning"
                        if any(
                            s in msg
                            for s in [
                                "\U0001f4e1",
                                "\U0001f50d",
                                "\U0001f9ea",
                                "\U0001f916",
                                "\U0001f4be",
                                "Phase",
                                "\u2550\u2550\u2550",
                            ]
                        ):
                            return "phase"
                        return "info"

                    class _ProgressWriter(io.TextIOBase):
                        """Captures print() output and routes to SSE queue."""

                        def write(self, text):
                            msg = text.strip()
                            if not msg:
                                return len(text)
                            progress_queue.put_nowait(
                                {
                                    "type": _classify_msg_type(msg),
                                    "message": msg,
                                }
                            )
                            try:
                                progress_queue.put_nowait(
                                    {
                                        "type": "stats",
                                        "queries": eng.stats.get("search_results", 0),
                                        "domains": (
                                            eng.stats.get("search_results", 0)
                                            - eng.stats.get("already_known", 0)
                                            - eng.stats.get("blacklisted", 0)
                                        ),
                                        "sources": len(eng.discovered),
                                        "leads": len(eng.extracted_leads),
                                    }
                                )
                            except Exception:
                                pass
                            return len(text)

                    try:
                        with contextlib.redirect_stdout(_ProgressWriter()):
                            await eng.run(max_queries=max_queries)
                    finally:
                        await eng.close()

                    # Final completion event with stats
                    elapsed = (local_now() - start_time).total_seconds()
                    progress_queue.put_nowait(
                        {
                            "type": "complete",
                            "message": f"✅ Discovery complete in {elapsed:.0f}s",
                            "stats": {
                                "queries": eng.stats.get("search_results", 0),
                                "domains": (
                                    eng.stats.get("search_results", 0)
                                    - eng.stats.get("already_known", 0)
                                    - eng.stats.get("blacklisted", 0)
                                ),
                                "sources": len(eng.discovered),
                                "leads": len(eng.extracted_leads),
                            },
                        }
                    )

                except Exception as e:
                    logger.error(f"Discovery error: {e}", exc_info=True)
                    progress_queue.put_nowait(
                        {
                            "type": "complete",
                            "message": f"❌ Discovery failed: {_safe_error(e)}",
                            "stats": {},
                        }
                    )

            # Run on the MAIN event loop (no threading — avoids AsyncEngine conflicts)
            task = asyncio.create_task(run_discovery())

            # Stream progress messages to frontend
            while True:
                if await request.is_disconnected():
                    task.cancel()
                    break

                try:
                    msg = progress_queue.get_nowait()
                    yield f"data: {json.dumps(msg)}\n\n"

                    if msg.get("type") == "complete":
                        break
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.3)

        except Exception as e:
            logger.error(f"Discovery stream error: {e}")
            yield f"data: {json.dumps({'type': 'complete', 'message': f'❌ Stream error: {_safe_error(e)}', 'stats': {}})}\n\n"

    from starlette.responses import StreamingResponse

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Contact Enrichment (Enrich button on lead detail panel)
# ─────────────────────────────────────────────────────────────────────────────


# Prevent duplicate enrichment runs for the same lead (Audit Fix M-09)
_enrichment_locks: dict[int, asyncio.Lock] = {}


@app.post("/api/dashboard/leads/{lead_id}/enrich", tags=["Dashboard"])
async def enrich_lead(lead_id: int, _csrf=Depends(_require_ajax)):
    # Check if enrichment already running for this lead
    if lead_id not in _enrichment_locks:
        _enrichment_locks[lead_id] = asyncio.Lock()
    if _enrichment_locks[lead_id].locked():
        return {
            "status": "already_running",
            "message": "Enrichment already in progress for this lead",
        }

    async with _enrichment_locks[lead_id]:
        return await _do_enrich_lead(lead_id)


async def _do_enrich_lead(lead_id: int):
    """Actual enrichment logic (extracted for lock wrapper)."""

    from sqlalchemy import and_, delete, select

    from app.database import async_session
    from app.models.potential_lead import PotentialLead
    from app.services.contact_enrichment import (
        enrich_lead_contacts,
        save_enrichment_to_lead,
    )

    # Load lead data
    async with async_session() as session:
        result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = result.scalar_one_or_none()

    if not lead:
        return {"status": "error", "message": "Lead not found"}

    if not lead.hotel_name:
        return {"status": "error", "message": "Lead has no hotel name"}

    logger.info(f"Enrichment requested for lead {lead_id}: {lead.hotel_name}")

    try:
        # Run enrichment
        enrichment_result = await enrich_lead_contacts(
            lead_id=lead.id,
            hotel_name=lead.hotel_name,
            brand=lead.brand,
            city=lead.city,
            state=lead.state,
            country=lead.country,
            management_company=lead.management_company,
            opening_date=lead.opening_date,
        )

        # Save to notes + lead fields
        save_result = await save_enrichment_to_lead(lead.id, enrichment_result)

        # Save to LeadContact table for the Contact tab
        if enrichment_result.contacts:
            async with async_session() as session:
                # Remove unsaved contacts from previous enrichment (keep saved/primary)
                await session.execute(
                    delete(LeadContact).where(
                        and_(
                            LeadContact.lead_id == lead_id,
                            LeadContact.is_saved == False,  # noqa: E712
                            LeadContact.is_primary == False,  # noqa: E712
                        )
                    )
                )

                # Check existing contacts to avoid duplicates
                existing = await session.execute(
                    select(LeadContact.name).where(LeadContact.lead_id == lead_id)
                )
                existing_names = {row[0].lower() for row in existing}

                for i, c in enumerate(enrichment_result.contacts):
                    if c["name"].lower() in existing_names:
                        # Fill blanks on existing contact — never overwrite
                        existing_contact = await session.execute(
                            select(LeadContact).where(
                                and_(
                                    LeadContact.lead_id == lead_id,
                                    LeadContact.name.ilike(c["name"]),
                                )
                            )
                        )
                        ec = existing_contact.scalar_one_or_none()
                        if ec:
                            filled = []
                            if not ec.email and c.get("email"):
                                ec.email = c["email"]
                                filled.append("email")
                            if not ec.phone and c.get("phone"):
                                ec.phone = c["phone"]
                                filled.append("phone")
                            if not ec.linkedin and c.get("linkedin"):
                                ec.linkedin = c["linkedin"]
                                filled.append("linkedin")
                            if not ec.title and c.get("title"):
                                ec.title = c["title"]
                                filled.append("title")
                            if not ec.organization and c.get("organization"):
                                ec.organization = c["organization"]
                                filled.append("organization")
                            if not ec.evidence_url and c.get("source"):
                                ec.evidence_url = c["source"]
                                filled.append("evidence_url")
                            if filled:
                                logger.info(
                                    f"Updated {ec.name}: filled {', '.join(filled)}"
                                )
                        continue
                    contact = LeadContact(
                        lead_id=lead_id,
                        name=c["name"],
                        title=c.get("title"),
                        email=c.get("email"),
                        phone=c.get("phone"),
                        linkedin=c.get("linkedin"),
                        organization=c.get("organization"),
                        scope=c.get("scope", "unknown"),
                        confidence=c.get(
                            "_validation_confidence", c.get("confidence", "medium")
                        ),
                        tier=c.get("_buyer_tier"),
                        score=c.get("_validation_score", 0),
                        is_primary=(i == 0),
                        found_via=", ".join(enrichment_result.layers_tried)
                        if enrichment_result.layers_tried
                        else "web_search",
                        source_detail=c.get(
                            "confidence_note", c.get("_validation_reason", "")
                        ),
                        evidence_url=c.get("source"),
                        last_enriched_at=local_now(),
                    )
                    session.add(contact)

                await session.commit()

        return {
            "status": save_result["status"],
            "lead_id": lead_id,
            "hotel_name": lead.hotel_name,
            "contacts_found": len(enrichment_result.contacts),
            "best_contact": enrichment_result.best_contact,
            "management_company": enrichment_result.management_company,
            "developer": enrichment_result.developer,
            "layers_tried": enrichment_result.layers_tried,
            "sources_used": enrichment_result.sources_used,
            "updated_fields": save_result.get("updated_fields", []),
            "errors": enrichment_result.errors,
        }

    except Exception as e:
        logger.error(f"Enrichment failed for lead {lead_id}: {e}", exc_info=True)
        return {"status": "error", "message": f"Enrichment failed: {str(e)}"}


# ═══════════════════════════════════════════════════════════════
# CONTACT MANAGEMENT
# ═══════════════════════════════════════════════════════════════


@app.get("/api/dashboard/leads/{lead_id}/contacts")
async def list_contacts(lead_id: int):
    async with async_session() as session:
        result = await session.execute(
            select(LeadContact)
            .where(LeadContact.lead_id == lead_id)
            .order_by(
                LeadContact.is_saved.desc(),
                LeadContact.is_primary.desc(),
                LeadContact.score.desc(),
            )
        )
        return [c.to_dict() for c in result.scalars().all()]


@app.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/save")
async def save_contact(lead_id: int, contact_id: int, _csrf=Depends(_require_ajax)):
    async with async_session() as session:
        result = await session.execute(
            select(LeadContact).where(
                LeadContact.id == contact_id, LeadContact.lead_id == lead_id
            )
        )
        contact = result.scalar_one_or_none()
        if not contact:
            raise HTTPException(status_code=404, detail="Contact not found")
        contact.is_saved = True
        contact.updated_at = local_now()
        await session.commit()
        return {"status": "saved", "contact_id": contact_id}


@app.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/unsave")
async def unsave_contact(lead_id: int, contact_id: int, _csrf=Depends(_require_ajax)):
    async with async_session() as session:
        result = await session.execute(
            select(LeadContact).where(
                LeadContact.id == contact_id, LeadContact.lead_id == lead_id
            )
        )
        contact = result.scalar_one_or_none()
        if not contact:
            raise HTTPException(status_code=404, detail="Contact not found")
        contact.is_saved = False
        contact.updated_at = local_now()
        await session.commit()
        return {"status": "unsaved", "contact_id": contact_id}


@app.delete("/api/dashboard/leads/{lead_id}/contacts/{contact_id}")
async def delete_contact(lead_id: int, contact_id: int, _csrf=Depends(_require_ajax)):
    async with async_session() as session:
        result = await session.execute(
            select(LeadContact).where(
                LeadContact.id == contact_id, LeadContact.lead_id == lead_id
            )
        )
        contact = result.scalar_one_or_none()
        if not contact:
            raise HTTPException(status_code=404, detail="Contact not found")
        await session.delete(contact)
        await session.commit()
        # Auto-rescore after contact removal
        try:
            await rescore_lead(lead_id, session)
            await session.commit()
        except Exception:
            pass
        return {"status": "deleted", "contact_id": contact_id}


@app.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/set-primary")
async def set_primary_contact(
    lead_id: int, contact_id: int, _csrf=Depends(_require_ajax)
):
    async with async_session() as session:
        await session.execute(
            update(LeadContact)
            .where(LeadContact.lead_id == lead_id)
            .values(is_primary=False)
        )
        result = await session.execute(
            select(LeadContact).where(
                LeadContact.id == contact_id, LeadContact.lead_id == lead_id
            )
        )
        contact = result.scalar_one_or_none()
        if not contact:
            raise HTTPException(status_code=404, detail="Contact not found")
        contact.is_primary = True
        contact.is_saved = True
        contact.updated_at = local_now()
        lead_result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = lead_result.scalar_one_or_none()
        if lead:
            lead.contact_name = contact.name
            lead.contact_title = contact.title
            lead.contact_email = contact.email
            lead.contact_phone = contact.phone
            if hasattr(lead, "contact_linkedin"):
                lead.contact_linkedin = contact.linkedin
            lead.updated_at = local_now()
        await session.commit()
        return {"status": "primary_set", "contact_id": contact_id}
