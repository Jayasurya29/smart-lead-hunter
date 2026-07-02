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
    returns just the gold: items whose person matched our contacts.
    Each story also carries its action-queue status (lead_queue_* /
    person_review_*) so the feed can show badges + approve/action buttons."""
    where = ["hn.created_at > NOW() - make_interval(days => :days)"]
    params: dict = {"days": days, "lim": min(max(limit, 1), 300)}
    if category:
        where.append("hn.category = :cat")
        params["cat"] = category
    if region:
        where.append("hn.region = :reg")
        params["reg"] = region
    if vertical:
        where.append("hn.vertical = :vert")
        params["vert"] = vertical
    if only_relationships:
        where.append("hn.relationship_hits IS NOT NULL")
    rows = (
        (
            await db.execute(
                text(
                    "SELECT hn.id, hn.url, hn.title, hn.snippet, hn.source, "
                    "hn.published_hint, hn.category, hn.vertical, hn.region, "
                    "hn.hotel_name, hn.brand, hn.person_name, hn.person_title, "
                    "hn.luxury, hn.in_pipeline, hn.pipeline_ref, "
                    "hn.relationship_hits, hn.created_at, "
                    "lq.id AS lead_queue_id, lq.status AS lead_queue_status, "
                    "lq.created_lead_id AS lead_queue_lead_id, "
                    "pr.id AS person_review_id, pr.status AS person_review_status, "
                    "cr.id AS contact_review_id, cr.status AS contact_review_status, "
                    "cr.hotel_name AS contact_review_hotel "
                    "FROM hotel_news hn "
                    "LEFT JOIN news_lead_queue lq ON lq.news_id = hn.id "
                    "LEFT JOIN news_person_review pr ON pr.news_id = hn.id "
                    "LEFT JOIN news_contact_review cr ON cr.news_id = hn.id "
                    "WHERE " + " AND ".join(where) + " "
                    "ORDER BY hn.created_at DESC LIMIT :lim"
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


# ── ACTION QUEUES: approve/reject hotels · action/dismiss person moves ──────
# Thin wrappers over app.services.news_actions (same logic the review CLI uses).


@router.post("/api/news/lead-queue/{queue_id}/approve")
async def approve_news_lead(queue_id: int, db: AsyncSession = Depends(get_db)):
    """Approve a queued hotel → save_lead_to_db (its own dedup runs)."""
    from app.services.news_actions import approve_lead

    return await approve_lead(db, queue_id)


@router.post("/api/news/lead-queue/{queue_id}/reject")
async def reject_news_lead(queue_id: int, db: AsyncSession = Depends(get_db)):
    from app.services.news_actions import reject_lead

    return {"ok": await reject_lead(db, queue_id)}


@router.post("/api/news/person-review/{review_id}/action")
async def action_news_person(review_id: int, db: AsyncSession = Depends(get_db)):
    """Apply a job-change: former edge + employed_by + warm lead_contact.
    Blocks (leaves pending) if the destination hotel isn't a lead yet."""
    from app.services.news_actions import apply_person_move

    return await apply_person_move(db, review_id)


@router.post("/api/news/person-review/{review_id}/dismiss")
async def dismiss_news_person(review_id: int, db: AsyncSession = Depends(get_db)):
    from app.services.news_actions import set_person_status

    return {"ok": await set_person_status(db, review_id, "dismissed")}


@router.post("/api/news/contact-review/{review_id}/add")
async def add_news_contact(review_id: int, db: AsyncSession = Depends(get_db)):
    """Attach a queued person to a hotel we already own as a new contact."""
    from app.services.news_actions import approve_contact

    return await approve_contact(db, review_id)


@router.post("/api/news/contact-review/{review_id}/skip")
async def skip_news_contact(review_id: int, db: AsyncSession = Depends(get_db)):
    from app.services.news_actions import set_contact_status

    return {"ok": await set_contact_status(db, review_id, "dismissed")}
