"""
Smart Lead Hunter — Shared State & Helpers
============================================
Globals, locks, pending stores, and shared query utilities
used across multiple route modules.
"""

import asyncio
import json
import logging
import re
import time
from datetime import timedelta
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy import select, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import PotentialLead, Source
from app.services.utils import local_now
from app.schemas import LeadResponse, LeadListResponse

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Scrape Job Tracking
# -----------------------------------------------------------------------------
# Protected by _scrape_lock for async-safe mutation within a single worker.
# NOTE: For multi-worker uvicorn (--workers >1), migrate to Redis hash/pub-sub.
active_scrapes: dict = {}
scrape_cancellations: set = set()
_scrape_lock = asyncio.Lock()
_SCRAPE_TTL = 1800  # 30 minutes

# Keyed by unique ID to prevent race conditions (Audit Fix #3)
_pending_configs: dict = {}  # scrape_id -> {mode, source_ids, ...}
_pending_extract_urls: dict = {}  # extract_id -> url
_pending_discovery_configs: dict = {}  # discovery_id -> config
_PENDING_TTL = 300  # 5 minutes

# Cached Redis connection for health checks
_health_redis = None

# Request body size limit (1 MB)
MAX_BODY_SIZE = 1_048_576


# -----------------------------------------------------------------------------
# Scrape Lifecycle Helpers
# -----------------------------------------------------------------------------


async def cleanup_stale_scrapes():
    """Remove scrape entries older than _SCRAPE_TTL."""
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


def store_pending(store: dict, key: str, value):
    """Store a pending config with timestamp, evicting expired entries."""
    now = time.monotonic()
    store[key] = {"_v": value, "_t": now}
    cutoff = now - _PENDING_TTL
    expired = [
        k for k, v in store.items() if isinstance(v, dict) and v.get("_t", 0) < cutoff
    ]
    for k in expired:
        del store[k]


def pop_pending(store: dict, key: str, default=None):
    """Pop a pending config by key, returning the original value."""
    entry = store.pop(key, None)
    if entry is None:
        return default
    if isinstance(entry, dict) and "_v" in entry:
        return entry["_v"]
    return entry


# -----------------------------------------------------------------------------
# Security / Sanitization Helpers
# -----------------------------------------------------------------------------


def escape_like(value: str) -> str:
    """Escape LIKE-special characters (%, _) so user input is treated literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def safe_error(e: Exception, fallback: str = "Operation failed") -> str:
    """Sanitize error message for frontend.
    Strips URLs, API keys, and long tracebacks."""
    msg = str(e)
    msg = re.sub(r"https?://[^\s]+", "[URL removed]", msg)
    msg = re.sub(r"[A-Za-z0-9_-]{20,}", "[REDACTED]", msg)
    if len(msg) > 120:
        msg = msg[:120] + "..."
    return msg or fallback


def require_ajax(request: Request):
    """Dependency that rejects non-AJAX requests to prevent CSRF.

    FIX C-05: Added Origin/Referer validation on top of the header check.
    The X-Requested-With header alone is insufficient because fetch() can
    set custom headers on same-origin requests. We now also verify that
    the Origin (or Referer) matches our known origins.

    Defense layers:
    1. CORS middleware blocks cross-origin preflight (first line of defense)
    2. SameSite=Lax cookie prevents cross-site cookie sending on POST
    3. This check: Origin/Referer must match allowed origins (belt + suspenders)
    4. X-Requested-With or Content-Type header must be present
    """
    # Layer 1: Check AJAX header (proves this isn't a plain <form> submit)
    requested_with = request.headers.get("x-requested-with", "")
    content_type = request.headers.get("content-type", "")
    has_ajax_header = (
        "xmlhttprequest" in requested_with.lower() or "application/json" in content_type
    )
    if not has_ajax_header:
        raise HTTPException(
            status_code=403, detail="CSRF check failed: missing required header"
        )

    # Layer 2: Validate Origin or Referer against allowed origins
    import os

    env = os.getenv("ENVIRONMENT", "development")
    if env == "production":
        allowed_origins = {"https://leads.jauniforms.com"}
    else:
        allowed_origins = {
            "http://localhost:8000",
            "http://localhost:3000",
            "http://127.0.0.1:8000",
            "http://192.168.30.59:8000",
            "http://192.168.30.59:3000",
        }

    origin = request.headers.get("origin", "")
    referer = request.headers.get("referer", "")

    # Extract origin from referer (https://host:port/path → https://host:port)
    if not origin and referer:
        from urllib.parse import urlparse

        parsed = urlparse(referer)
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else ""

    # If we have an origin, it must match. If no origin at all (same-origin
    # requests in some browsers omit it), allow through — the cookie SameSite
    # policy is the backstop.
    if origin and origin not in allowed_origins:
        logger.warning(f"CSRF: rejected request from origin {origin}")
        raise HTTPException(
            status_code=403, detail="CSRF check failed: origin not allowed"
        )

    return True


async def checked_json(request: Request, max_size: int = MAX_BODY_SIZE) -> dict:
    """Parse JSON body with size limit to prevent DoS."""
    body = await request.body()
    if len(body) > max_size:
        raise HTTPException(status_code=413, detail="Request body too large")
    return json.loads(body)


# -----------------------------------------------------------------------------
# MergedLead Conversion
# -----------------------------------------------------------------------------


def merged_lead_to_dict(ml, fallback_url: str = "", fallback_source: str = "") -> dict:
    """Convert a MergedLead object to a dict for save_leads_to_database."""
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
# Shared Query Helpers
# -----------------------------------------------------------------------------


async def paginate_leads(
    db: AsyncSession,
    base_query,
    count_query,
    page: int,
    per_page: int,
    order_by=None,
):
    """Shared pagination logic for lead list endpoints."""
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


def apply_lead_filters(
    query,
    count_query,
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    state: Optional[str] = None,
    location_type: Optional[str] = None,
    brand_tier: Optional[str] = None,
    search: Optional[str] = None,
):
    """Apply common lead filters to both query and count_query."""
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


def lead_list_response(leads, total, page, per_page, pages) -> LeadListResponse:
    """Build standard LeadListResponse."""
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


# ── Persistent Redis connection pool (singleton) ──
_stats_redis = None
_stats_redis_broken_until: float = (
    0.0  # FIX M-14: timestamp-based backoff (was fire-and-forget task)
)


async def _get_redis():
    """Get or create a persistent Redis connection. Returns None if unavailable."""
    global _stats_redis, _stats_redis_broken_until

    # FIX M-14: Simple timestamp check instead of asyncio.create_task that could
    # get silently cancelled on shutdown, leaving the flag stuck forever.
    import time

    now = time.monotonic()
    if _stats_redis_broken_until > now:
        return None

    if _stats_redis is not None:
        try:
            await _stats_redis.ping()
            return _stats_redis
        except Exception:
            # Connection went stale — recreate
            try:
                await _stats_redis.aclose()
            except Exception:
                pass
            _stats_redis = None

    try:
        from app.config import settings
        import redis.asyncio as aioredis

        redis_url = getattr(settings, "redis_url", None)
        if not redis_url:
            return None

        _stats_redis = aioredis.from_url(
            redis_url,
            socket_connect_timeout=1,
            socket_timeout=1,
            decode_responses=True,
        )
        await _stats_redis.ping()
        return _stats_redis
    except Exception:
        _stats_redis = None
        # Backoff 60s before retrying
        _stats_redis_broken_until = now + 60.0
        return None


async def get_dashboard_stats(db: AsyncSession) -> dict:
    """Fetch all dashboard stats in 2 queries using SQL aggregation.

    Results are cached in Redis for 30 seconds to reduce DB load
    when multiple browser tabs poll /stats simultaneously.
    Uses a persistent connection pool — no new TCP handshake per request.
    """
    # Try Redis cache first (persistent connection, sub-ms)
    r = await _get_redis()
    if r:
        try:
            cached = await r.get("slh:dashboard_stats")
            if cached:
                return json.loads(cached)
        except Exception:
            pass  # Cache miss or error — fall through to DB

    now = local_now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())

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
            func.sum(
                case(
                    (
                        (PotentialLead.timeline_label == "HOT")
                        & PotentialLead.status.in_(["new", "approved"]),
                        1,
                    ),
                    else_=0,
                )
            ).label("hot"),
            func.sum(
                case(
                    (
                        (PotentialLead.timeline_label == "URGENT")
                        & PotentialLead.status.in_(["new", "approved"]),
                        1,
                    ),
                    else_=0,
                )
            ).label("urgent"),
            func.sum(
                case(
                    (
                        (PotentialLead.timeline_label == "WARM")
                        & PotentialLead.status.in_(["new", "approved"]),
                        1,
                    ),
                    else_=0,
                )
            ).label("warm"),
            func.sum(
                case(
                    (
                        (PotentialLead.timeline_label == "COOL")
                        & PotentialLead.status.in_(["new", "approved"]),
                        1,
                    ),
                    else_=0,
                )
            ).label("cool"),
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

    stats = {
        "total_leads": lr.total or 0,
        "new_leads": lr.new or 0,
        "approved_leads": lr.approved or 0,
        "pending_leads": lr.pending or 0,
        "rejected_leads": lr.rejected or 0,
        "hot_leads": lr.hot or 0,
        "urgent_leads": lr.urgent or 0,
        "warm_leads": lr.warm or 0,
        "cool_leads": lr.cool or 0,
        "leads_today": lr.today or 0,
        "leads_this_week": lr.this_week or 0,
        "deleted_leads": lr.deleted or 0,
        "total_sources": sr.total or 0,
        "active_sources": sr.active or 0,
        "healthy_sources": sr.healthy or 0,
    }

    # Cache in Redis (30s TTL) — reuses persistent connection
    if r:
        try:
            await r.setex("slh:dashboard_stats", 30, json.dumps(stats))
        except Exception:
            pass

    return stats
