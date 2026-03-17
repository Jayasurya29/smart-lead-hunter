"""Source management and scrape log endpoints."""

import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Source, ScrapeLog
from app.schemas import SourceCreate, SourceResponse, ScrapeLogResponse
from app.services.utils import local_now

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/sources", response_model=List[SourceResponse], tags=["Sources"])
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


@router.get("/sources/healthy", response_model=List[SourceResponse], tags=["Sources"])
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


@router.get("/sources/problems", response_model=List[SourceResponse], tags=["Sources"])
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
