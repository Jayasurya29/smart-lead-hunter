"""Source management and scrape log endpoints."""

import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, or_, case, cast, String
from sqlalchemy.orm import defer
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Source, ScrapeLog
from app.schemas import (
    SourceCreate,
    SourceResponse,
    SourceListResponse,
    SourceStatsResponse,
    ScrapeLogResponse,
)
from app.services.utils import local_now

logger = logging.getLogger(__name__)

router = APIRouter()


# Columns that contain large JSONB blobs and are NOT exposed in the
# SourceResponse schema. We defer loading them at query time so list
# endpoints don't pay the deserialization cost for data nobody sees.
# Performance: cuts /sources list-page load from ~8s to <1s on 250+ rows.
# Detail endpoints that need this data should fetch it explicitly.
_HEAVY_DEFERRED_COLUMNS = (
    defer(Source.source_intelligence),
    defer(Source.gold_urls),
)


# Map the UI filter-tab value -> a SQLAlchemy WHERE clause. Kept in one place
# so the list endpoint and any future caller stay consistent with the tabs
# shown on the Sources page (All / Healthy / Problems / Active / Inactive).
def _status_filter(status: Optional[str]):
    if status == "healthy":
        return Source.health_status == "healthy"
    if status == "problems":
        return Source.health_status.in_(["failing", "dead", "degraded"])
    if status == "active":
        return Source.is_active.is_(True)
    if status == "inactive":
        return Source.is_active.is_(False)
    return None  # "" / "all" / None -> no filter


def _apply_source_filters(
    query,
    count_query,
    *,
    search: Optional[str],
    status: Optional[str],
    active_only: bool,
    source_type: Optional[str],
    min_priority: Optional[int],
):
    """Apply the same filters to both the row query and the count query."""
    if search:
        s = f"%{search}%"
        f = or_(Source.name.ilike(s), Source.base_url.ilike(s))
        query, count_query = query.where(f), count_query.where(f)

    status_f = _status_filter(status)
    if status_f is not None:
        query, count_query = query.where(status_f), count_query.where(status_f)

    if active_only:
        query = query.where(Source.is_active.is_(True))
        count_query = count_query.where(Source.is_active.is_(True))
    if source_type:
        query = query.where(Source.source_type == source_type)
        count_query = count_query.where(Source.source_type == source_type)
    if min_priority:
        query = query.where(Source.priority >= min_priority)
        count_query = count_query.where(Source.priority >= min_priority)

    return query, count_query


@router.get("/sources", response_model=SourceListResponse, tags=["Sources"])
async def list_sources(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    search: Optional[str] = None,
    status: Optional[str] = None,  # healthy | problems | active | inactive
    active_only: bool = False,
    source_type: Optional[str] = None,
    min_priority: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
):
    """Paginated list of scraping sources.

    Server-side pagination + search + status filtering so the Sources page
    no longer pulls all rows on every load. Ordered leads-desc to match the
    page's previous client-side sort (highest-yield sources first).
    """
    query = select(Source).options(*_HEAVY_DEFERRED_COLUMNS)
    count_query = select(func.count(Source.id))
    query, count_query = _apply_source_filters(
        query,
        count_query,
        search=search,
        status=status,
        active_only=active_only,
        source_type=source_type,
        min_priority=min_priority,
    )

    total = (await db.execute(count_query)).scalar() or 0

    query = (
        query.order_by(
            Source.leads_found.desc().nullslast(),
            Source.priority.desc(),
            Source.name,
        )
        .offset((page - 1) * per_page)
        .limit(per_page)
    )

    sources = (await db.execute(query)).scalars().all()
    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return SourceListResponse(
        sources=[SourceResponse.model_validate(s) for s in sources],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@router.get("/sources/stats", response_model=SourceStatsResponse, tags=["Sources"])
async def source_stats(db: AsyncSession = Depends(get_db)):
    """Aggregate counts for the Sources page stat cards.

    Computed over the FULL table in one query so the cards stay correct
    regardless of which page/filter the list is showing. 'no_patterns'
    counts rows whose gold_urls JSONB is NULL or an empty object — this is
    the real count (the old client-side card always read total because
    gold_urls isn't carried on the list payload)."""
    row = (
        await db.execute(
            select(
                func.count(Source.id).label("total"),
                func.count(case((Source.is_active.is_(True), 1))).label("active"),
                func.count(case((Source.health_status == "healthy", 1))).label(
                    "healthy"
                ),
                func.count(
                    case((Source.health_status.in_(["failing", "dead"]), 1))
                ).label("blocked"),
                func.coalesce(func.sum(Source.leads_found), 0).label("total_leads"),
                func.count(
                    case(
                        (
                            or_(
                                Source.gold_urls.is_(None),
                                cast(Source.gold_urls, String) == "{}",
                            ),
                            1,
                        )
                    )
                ).label("no_patterns"),
            )
        )
    ).one()

    return SourceStatsResponse(
        total=row.total or 0,
        active=row.active or 0,
        healthy=row.healthy or 0,
        blocked=row.blocked or 0,
        total_leads=int(row.total_leads or 0),
        no_patterns=row.no_patterns or 0,
    )


@router.get("/sources/healthy", response_model=List[SourceResponse], tags=["Sources"])
async def list_healthy_sources(db: AsyncSession = Depends(get_db)):
    """List healthy sources ready for scraping"""
    query = (
        select(Source)
        .options(*_HEAVY_DEFERRED_COLUMNS)
        .where(
            Source.is_active.is_(True),
            Source.health_status.in_(["healthy", "new", "degraded"]),
        )
        .order_by(Source.priority.desc())
    )
    result = await db.execute(query)
    sources = result.scalars().all()
    return [SourceResponse.model_validate(source) for source in sources]


@router.get("/sources/problems", response_model=List[SourceResponse], tags=["Sources"])
async def list_problem_sources(db: AsyncSession = Depends(get_db)):
    """List sources with health problems"""
    query = (
        select(Source)
        .options(*_HEAVY_DEFERRED_COLUMNS)
        .where(Source.health_status.in_(["failing", "dead"]))
        .order_by(Source.consecutive_failures.desc())
    )
    result = await db.execute(query)
    sources = result.scalars().all()
    return [SourceResponse.model_validate(source) for source in sources]


@router.post("/sources", response_model=SourceResponse, tags=["Sources"])
async def create_source(source_data: SourceCreate, db: AsyncSession = Depends(get_db)):
    """Create a new scraping source"""
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


@router.post(
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


@router.post(
    "/sources/{source_id}/reset-health", response_model=SourceResponse, tags=["Sources"]
)
async def reset_source_health(source_id: int, db: AsyncSession = Depends(get_db)):
    """Reset a source's health status"""
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


@router.delete("/sources/{source_id}", tags=["Sources"])
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
# Scrape Logs
# -----------------------------------------------------------------------------


@router.get("/scrape/logs", response_model=List[ScrapeLogResponse], tags=["Scraping"])
async def get_scrape_logs(
    limit: int = Query(20, ge=1, le=100),
    source_id: Optional[int] = None,
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Get recent scrape logs"""
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
# Discovery Query Intelligence
# -----------------------------------------------------------------------------


@router.get("/discovery/queries", tags=["Discovery"])
async def list_discovery_queries(
    status_filter: Optional[str] = Query(
        None, description="Filter by status: gold | maybe | junk | paused"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Return all tracked discovery queries with their learning state.

    Used by the Sources page 'Queries' tab to show which search queries are
    earning their keep vs. wasting Serper credits.
    """
    from app.models.discovery_query_stat import DiscoveryQueryStat
    from sqlalchemy import desc

    query = select(DiscoveryQueryStat)
    if status_filter:
        query = query.where(DiscoveryQueryStat.status == status_filter)
    query = query.order_by(
        desc(DiscoveryQueryStat.total_new_leads),
        desc(DiscoveryQueryStat.total_new_sources),
        desc(DiscoveryQueryStat.total_runs),
    )

    result = await db.execute(query)
    rows = result.scalars().all()

    return [
        {
            "query_text": r.query_text,
            "status": r.status,
            "total_runs": r.total_runs,
            "total_new_sources": r.total_new_sources,
            "total_new_leads": r.total_new_leads,
            "total_duplicates": r.total_duplicates,
            "consecutive_zero_runs": r.consecutive_zero_runs,
            "first_run_at": r.first_run_at.isoformat() if r.first_run_at else None,
            "last_run_at": r.last_run_at.isoformat() if r.last_run_at else None,
            "last_success_at": r.last_success_at.isoformat()
            if r.last_success_at
            else None,
            "paused_until": r.paused_until.isoformat() if r.paused_until else None,
            "last_run_detail": r.last_run_detail,
        }
        for r in rows
    ]


@router.get("/discovery/queries/stats", tags=["Discovery"])
async def discovery_query_summary(db: AsyncSession = Depends(get_db)):
    """
    Aggregate summary stats for the Sources > Queries tab header cards.
    """
    from app.models.discovery_query_stat import DiscoveryQueryStat
    from sqlalchemy import func

    result = await db.execute(
        select(
            DiscoveryQueryStat.status,
            func.count().label("count"),
            func.sum(DiscoveryQueryStat.total_new_sources).label("total_sources"),
            func.sum(DiscoveryQueryStat.total_new_leads).label("total_leads"),
        ).group_by(DiscoveryQueryStat.status)
    )
    rows = result.all()

    by_status = {
        r.status: {
            "count": r.count,
            "sources": r.total_sources or 0,
            "leads": r.total_leads or 0,
        }
        for r in rows
    }
    total_queries = sum(r["count"] for r in by_status.values())
    total_sources = sum(r["sources"] for r in by_status.values())
    total_leads = sum(r["leads"] for r in by_status.values())

    return {
        "total_queries": total_queries,
        "gold": by_status.get("gold", {"count": 0, "sources": 0, "leads": 0}),
        "maybe": by_status.get("maybe", {"count": 0, "sources": 0, "leads": 0}),
        "junk": by_status.get("junk", {"count": 0, "sources": 0, "leads": 0}),
        "paused": by_status.get("paused", {"count": 0, "sources": 0, "leads": 0}),
        "total_new_sources_ever": total_sources,
        "total_new_leads_ever": total_leads,
    }
