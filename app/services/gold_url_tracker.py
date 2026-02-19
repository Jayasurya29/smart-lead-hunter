"""
Gold URL Tracker
================
Tracks which specific URLs on each source actually produce leads.
Enables "smart scrape" mode: hit known gold URLs first (fast),
only do full deep crawl when discovering or when gold URLs go stale.

Gold URL lifecycle:
1. DISCOVERY: Deep crawl → classify → extract → record producing URLs as gold
2. FAST SCRAPE: Hit gold URLs directly (skips crawling) → extract → save
3. REDISCOVERY: If gold URLs fail OR discovery_interval_days elapsed → deep crawl again
4. DECAY: Gold URLs that stop producing get demoted after 3 misses

Gold URL format in DB (JSONB):
{
    "https://example.com/new-hotels": {
        "leads_found": 5,
        "last_hit": "2026-02-09T12:00:00Z",
        "first_found": "2026-02-01T12:00:00Z",
        "miss_streak": 0,
        "total_checks": 8
    }
}
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.source import Source
from app.services.utils import local_now

logger = logging.getLogger(__name__)


class GoldURLTracker:
    """Manages gold URL discovery, tracking, and smart scrape decisions."""

    # After this many consecutive misses, demote a gold URL
    MAX_MISS_STREAK = 3

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_scrape_plan(self, source_id: int) -> Dict:
        """
        Decide HOW to scrape a source:
        Returns {
            "mode": "gold" | "discover" | "full",
            "urls": [...],  # URLs to scrape (for gold mode)
            "reason": "..."
        }
        """
        source = await self._get_source(source_id)
        if not source:
            return {"mode": "full", "urls": [], "reason": "Source not found"}

        gold_urls = source.gold_urls or {}
        now = local_now()

        # Filter out dead gold URLs (too many misses)
        active_gold = {
            url: meta
            for url, meta in gold_urls.items()
            if meta.get("miss_streak", 0) < self.MAX_MISS_STREAK
        }

        # Decision logic
        needs_discovery = self._needs_discovery(source, now)
        has_gold = len(active_gold) > 0

        if not has_gold or needs_discovery:
            # No gold URLs known, or it's time to rediscover
            reason = (
                "No gold URLs found yet"
                if not has_gold
                else f"Discovery interval ({source.discovery_interval_days}d) elapsed"
            )
            return {"mode": "discover", "urls": [], "reason": reason}
        else:
            # We have gold URLs — use fast mode
            urls = sorted(
                active_gold.keys(),
                key=lambda u: active_gold[u].get("leads_found", 0),
                reverse=True,
            )
            return {
                "mode": "gold",
                "urls": urls,
                "reason": f"{len(urls)} gold URLs known",
            }

    def _needs_discovery(self, source: Source, now: datetime) -> bool:
        """Check if source needs a fresh discovery crawl."""
        if not source.last_discovery_at:
            return True

        interval = timedelta(days=source.discovery_interval_days or 7)
        return (now - source.last_discovery_at) > interval

    async def record_hits(
        self,
        source_id: int,
        url_lead_counts: Dict[str, int],
        is_discovery: bool = False,
    ):
        """
        After extraction, record which URLs produced leads.

        url_lead_counts: {url: number_of_leads_extracted}
        is_discovery: True if this was a deep crawl (updates last_discovery_at)
        """
        source = await self._get_source(source_id)
        if not source:
            return

        gold_urls = dict(source.gold_urls or {})
        now_str = local_now().isoformat()

        for url, lead_count in url_lead_counts.items():
            if lead_count > 0:
                # URL produced leads — it's gold!
                if url in gold_urls:
                    gold_urls[url]["leads_found"] = (
                        gold_urls[url].get("leads_found", 0) + lead_count
                    )
                    gold_urls[url]["last_hit"] = now_str
                    gold_urls[url]["miss_streak"] = 0
                    gold_urls[url]["total_checks"] = (
                        gold_urls[url].get("total_checks", 0) + 1
                    )
                else:
                    gold_urls[url] = {
                        "leads_found": lead_count,
                        "last_hit": now_str,
                        "first_found": now_str,
                        "miss_streak": 0,
                        "total_checks": 1,
                    }
                logger.info(f"⭐ Gold URL: {url[:60]} → {lead_count} leads")
            else:
                # URL checked but no leads
                if url in gold_urls:
                    gold_urls[url]["miss_streak"] = (
                        gold_urls[url].get("miss_streak", 0) + 1
                    )
                    gold_urls[url]["total_checks"] = (
                        gold_urls[url].get("total_checks", 0) + 1
                    )

                    if gold_urls[url]["miss_streak"] >= self.MAX_MISS_STREAK:
                        logger.info(f"💀 Demoting gold URL (3 misses): {url[:60]}")

        # Update source
        source.gold_urls = gold_urls

        # Update stats
        total_leads = sum(c for c in url_lead_counts.values() if c > 0)
        source.total_scrapes = (source.total_scrapes or 0) + 1
        old_avg = float(source.avg_lead_yield or 0)
        scrapes = source.total_scrapes
        source.avg_lead_yield = ((old_avg * (scrapes - 1)) + total_leads) / scrapes

        if is_discovery:
            source.last_discovery_at = local_now()

        await self.session.commit()

        active_count = sum(
            1
            for m in gold_urls.values()
            if m.get("miss_streak", 0) < self.MAX_MISS_STREAK
        )
        logger.info(
            f"📊 Source {source.name}: {active_count} active gold URLs, avg yield: {source.avg_lead_yield:.1f}"
        )

    async def get_source_stats(self, source_id: int) -> Optional[Dict]:
        """Get gold URL stats for a source."""
        source = await self._get_source(source_id)
        if not source:
            return None

        gold_urls = source.gold_urls or {}
        active = {
            u: m
            for u, m in gold_urls.items()
            if m.get("miss_streak", 0) < self.MAX_MISS_STREAK
        }
        dead = {
            u: m
            for u, m in gold_urls.items()
            if m.get("miss_streak", 0) >= self.MAX_MISS_STREAK
        }

        return {
            "source_name": source.name,
            "source_type": source.source_type,
            "total_gold_urls": len(gold_urls),
            "active_gold_urls": len(active),
            "dead_gold_urls": len(dead),
            "avg_lead_yield": float(source.avg_lead_yield or 0),
            "total_scrapes": source.total_scrapes or 0,
            "last_discovery_at": source.last_discovery_at.isoformat()
            if source.last_discovery_at
            else None,
            "needs_discovery": self._needs_discovery(source, local_now()),
            "gold_urls": {
                url: {
                    "leads_found": meta.get("leads_found", 0),
                    "miss_streak": meta.get("miss_streak", 0),
                    "status": "active"
                    if meta.get("miss_streak", 0) < self.MAX_MISS_STREAK
                    else "dead",
                }
                for url, meta in gold_urls.items()
            },
        }

    async def _get_source(self, source_id: int) -> Optional[Source]:
        result = await self.session.execute(
            select(Source).where(Source.id == source_id)
        )
        return result.scalar_one_or_none()


async def get_smart_scrape_queue(session: AsyncSession) -> List[Dict]:
    """
    Get the list of sources that need scraping NOW based on their frequency.
    Returns sources sorted by priority, filtered by schedule.

    Frequency schedule:
    - daily: every day
    - every_3_days: if last_scraped > 3 days ago
    - twice_weekly: Mon + Thu (or if last_scraped > 4 days ago)
    - weekly: if last_scraped > 7 days ago
    - monthly: if last_scraped > 30 days ago
    """
    result = await session.execute(
        select(Source).where(Source.is_active).order_by(Source.priority.desc())
    )
    sources = result.scalars().all()

    now = local_now()
    day_of_week = now.weekday()  # 0=Mon, 6=Sun

    due_sources = []

    for src in sources:
        freq = src.scrape_frequency or "daily"
        last = src.last_scraped_at

        # Never scraped = always due
        if not last:
            due_sources.append(
                {"source": src, "reason": "Never scraped", "overdue_hours": 999}
            )
            continue

        hours_since = (now - last).total_seconds() / 3600

        is_due = False
        reason = ""

        if freq == "daily" and hours_since >= 20:  # 20h buffer
            is_due = True
            reason = f"Daily (last: {hours_since:.0f}h ago)"
        elif freq == "every_3_days" and hours_since >= 68:  # ~3 days
            is_due = True
            reason = f"Every 3 days (last: {hours_since:.0f}h ago)"
        elif freq == "twice_weekly":
            # Mon (0) and Thu (3), or if >4 days since last
            if day_of_week in (0, 3) and hours_since >= 20:
                is_due = True
                reason = "Twice weekly - scheduled day"
            elif hours_since >= 96:  # 4 days safety net
                is_due = True
                reason = f"Twice weekly - overdue ({hours_since:.0f}h)"
        elif freq == "weekly" and hours_since >= 160:  # ~7 days
            is_due = True
            reason = f"Weekly (last: {hours_since:.0f}h ago)"
        elif freq == "monthly" and hours_since >= 720:  # ~30 days
            is_due = True
            reason = f"Monthly (last: {hours_since:.0f}h ago)"

        if is_due:
            due_sources.append(
                {"source": src, "reason": reason, "overdue_hours": hours_since}
            )

    return due_sources
