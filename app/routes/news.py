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
    if only_relationships:
        where.append("relationship_hits IS NOT NULL")
    rows = (
        (
            await db.execute(
                text(
                    "SELECT id, url, title, snippet, source, published_hint, "
                    "category, region, hotel_name, brand, person_name, "
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
