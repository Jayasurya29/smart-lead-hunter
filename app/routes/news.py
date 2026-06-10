"""Hotel intelligence news feed API."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db

router = APIRouter()


@router.get("/api/news")
async def list_news(
    category: str | None = None,
    region: str | None = None,
    vertical: str | None = None,
    days: int = 14,
    only_relationships: bool = False,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    """Recent hospitality news, newest first. only_relationships=true
    returns just the gold: items whose person matched our contacts."""
    where = ["created_at > NOW() - make_interval(days => :days)"]
    params: dict = {"days": days, "lim": min(max(limit, 1), 300)}
    if category:
        where.append("category = :cat")
        params["cat"] = category
    if region:
        where.append("region = :reg")
        params["reg"] = region
    if vertical:
        where.append("vertical = :vert")
        params["vert"] = vertical
    if only_relationships:
        where.append("relationship_hits IS NOT NULL")
    rows = (
        (
            await db.execute(
                text(
                    "SELECT id, url, title, snippet, source, published_hint, "
                    "category, vertical, region, hotel_name, brand, person_name, "
                    "person_title, luxury, in_pipeline, pipeline_ref, "
                    "relationship_hits, created_at FROM hotel_news "
                    "WHERE " + " AND ".join(where) + " "
                    "ORDER BY created_at DESC LIMIT :lim"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return [dict(r) for r in rows]


@router.get("/api/news/source-stats")
async def news_source_stats(db: AsyncSession = Depends(get_db)):
    """Productivity analytics for the Sources page News tab.

    Per query and per source/outlet, derived from hotel_news:
      - stories, fresh_30d, active_days (distinct days it produced something),
        last_seen, and how many led to a known-contact or pipeline hit.
    active_days > 1 = a continuous producer; active_days = 1 = one-time/static.
    """
    queries = (
        (
            await db.execute(
                text(
                    "SELECT query, "
                    "COUNT(*) AS stories, "
                    "COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '30 days') AS fresh_30d, "
                    "COUNT(DISTINCT date_trunc('day', created_at)) AS active_days, "
                    "MAX(created_at) AS last_seen, "
                    "COUNT(*) FILTER (WHERE relationship_hits IS NOT NULL) AS rel_hits, "
                    "COUNT(*) FILTER (WHERE in_pipeline) AS pipeline_hits "
                    "FROM hotel_news WHERE query IS NOT NULL AND query <> '' "
                    "GROUP BY query ORDER BY stories DESC"
                )
            )
        )
        .mappings()
        .all()
    )
    sources = (
        (
            await db.execute(
                text(
                    "SELECT source, "
                    "COUNT(*) AS stories, "
                    "COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '30 days') AS fresh_30d, "
                    "COUNT(DISTINCT date_trunc('day', created_at)) AS active_days, "
                    "MIN(created_at) AS first_seen, "
                    "MAX(created_at) AS last_seen, "
                    "COUNT(*) FILTER (WHERE relationship_hits IS NOT NULL) AS rel_hits, "
                    "COUNT(*) FILTER (WHERE in_pipeline) AS pipeline_hits "
                    "FROM hotel_news WHERE source IS NOT NULL AND source <> '' "
                    "GROUP BY source ORDER BY stories DESC"
                )
            )
        )
        .mappings()
        .all()
    )
    return {
        "queries": [dict(r) for r in queries],
        "sources": [dict(r) for r in sources],
    }
