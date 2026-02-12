"""
SMART LEAD HUNTER - SOURCE SEED SCRIPT
======================================
ONE-TIME script to populate the database with sources.

Run this ONCE to set up your sources:
    python -m app.services.source_seed

After running, sources live in the DATABASE (sources table).
This file is just for initial setup - the database is the truth.

To add new sources later, use the dashboard or add directly to DB.
"""

import asyncio
import logging
from typing import List, Dict, Any

from sqlalchemy import select
from app.database import async_session
from app.models import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# SOURCE DEFINITIONS - All your sources in one place
# =============================================================================

SOURCES: List[Dict[str, Any]] = [
    # =========================================================================
    # TIER 1: CHAIN NEWSROOMS (Priority 10) - Direct from source
    # =========================================================================
    {
        "name": "Marriott News",
        "base_url": "https://news.marriott.com/news/",
        "source_type": "chain_newsroom",
        "priority": 10,
        "use_playwright": False,
        "notes": "30+ brands - Ritz-Carlton, St. Regis, W, EDITION, JW Marriott",
    },
    {
        "name": "Hilton Newsroom",
        "base_url": "https://stories.hilton.com/releases",
        "source_type": "chain_newsroom",
        "priority": 10,
        "use_playwright": False,
        "notes": "22+ brands - Waldorf, Conrad, LXR, Curio",
    },
    {
        "name": "Hyatt Newsroom",
        "base_url": "https://newsroom.hyatt.com/news-releases",
        "source_type": "chain_newsroom",
        "priority": 10,
        "use_playwright": False,
        "notes": "25+ brands - Park Hyatt, Andaz, Thompson, Alila",
    },
    {
        "name": "IHG News",
        "base_url": "https://www.ihgplc.com/en/news-and-media/news-releases",
        "source_type": "chain_newsroom",
        "priority": 9,
        "use_playwright": False,
        "notes": "17+ brands - InterContinental, Kimpton, Six Senses, Regent",
    },
    {
        "name": "Four Seasons Press",
        "base_url": "https://press.fourseasons.com/news-releases/",
        "source_type": "chain_newsroom",
        "priority": 10,
        "use_playwright": False,
        "notes": "Ultra-luxury - Puerto Rico, Naples Beach Club",
    },
    {
        "name": "Four Seasons New Openings",
        "base_url": "https://www.fourseasons.com/newopenings/",
        "source_type": "chain_newsroom",
        "priority": 10,
        "use_playwright": True,  # JS-heavy carousel
        "notes": "New openings carousel page",
    },
    # =========================================================================
    # TIER 2: INDUSTRY PUBLICATIONS (Priority 9-10)
    # =========================================================================
    {
        "name": "Hotel Dive",
        "base_url": "https://www.hoteldive.com/topic/development/",
        "source_type": "industry",
        "priority": 10,
        "use_playwright": False,
        "notes": "BEST US SOURCE - excellent development coverage",
    },
    {
        "name": "LODGING Magazine",
        "base_url": "https://lodgingmagazine.com/category/industrynews/",
        "source_type": "industry",
        "priority": 10,
        "use_playwright": False,
        "notes": "AHLA official publication",
    },
    {
        "name": "Hospitality Net",
        "base_url": "https://www.hospitalitynet.org/news/global.html",
        "source_type": "industry",
        "priority": 8,
        "use_playwright": False,
        "notes": "International focus - filter carefully for USA/Caribbean",
    },
    {
        "name": "Bisnow Hotels",
        "base_url": "https://www.bisnow.com/tags/hotels",
        "source_type": "industry",
        "priority": 9,
        "use_playwright": False,
        "notes": "Commercial real estate focus - hotel deals and development",
    },
    {
        "name": "CoStar Hotels",
        "base_url": "https://www.costar.com/article/topic/hotels",
        "source_type": "industry",
        "priority": 9,
        "use_playwright": True,
        "notes": "Premium data - some free articles",
    },
    # =========================================================================
    # TIER 3: AGGREGATORS (Priority 10) - Multiple hotels per page
    # =========================================================================
    {
        "name": "Orange Studio",
        "base_url": "https://www.theorangestudio.com/hotel-openings",
        "source_type": "aggregator",
        "priority": 10,
        "use_playwright": True,  # JS-rendered
        "notes": "BEST AGGREGATOR - single page with 200+ hotels",
    },
    {
        "name": "New Hotels 2026",
        "base_url": "https://www.newhotelsopening.com/hotel-openings-2026",
        "source_type": "aggregator",
        "priority": 10,
        "use_playwright": False,
        "notes": "2026 openings - current year focus",
    },
    {
        "name": "New Hotels 2027",
        "base_url": "https://www.newhotelsopening.com/hotel-openings-2027",
        "source_type": "aggregator",
        "priority": 8,
        "use_playwright": False,
        "notes": "2027 openings - future pipeline",
    },
    # =========================================================================
    # TIER 4: FLORIDA SOURCES (Priority 10) - YOUR CORE MARKET
    # =========================================================================
    {
        "name": "South Florida Business Journal",
        "base_url": "https://www.bizjournals.com/southflorida/news/industry/hotels",
        "source_type": "florida",
        "priority": 10,
        "use_playwright": True,  # BizJournals blocks httpx
        "notes": "Miami/Fort Lauderdale/Palm Beach - CORE MARKET",
    },
    {
        "name": "Orlando Business Journal",
        "base_url": "https://www.bizjournals.com/orlando/news/industry/hotels",
        "source_type": "florida",
        "priority": 10,
        "use_playwright": True,
        "notes": "Orlando/Central Florida - CORE MARKET",
    },
    {
        "name": "Tampa Bay Business Journal",
        "base_url": "https://www.bizjournals.com/tampabay/news/industry/hotels",
        "source_type": "florida",
        "priority": 9,
        "use_playwright": True,
        "notes": "Tampa Bay - CORE MARKET",
    },
    # =========================================================================
    # TIER 5: CARIBBEAN SOURCES (Priority 9-10)
    # =========================================================================
    {
        "name": "Caribbean Journal",
        "base_url": "https://www.caribjournal.com/category/hotels/",
        "source_type": "caribbean",
        "priority": 10,
        "use_playwright": False,
        "notes": "THE source for Caribbean hotel news",
    },
    {
        "name": "Caribbean Hotel & Tourism Association",
        "base_url": "https://caribbeanhotelandtourism.com/category/news/",
        "source_type": "caribbean",
        "priority": 9,
        "use_playwright": False,
        "notes": "CHTA - 1,000+ member properties",
    },
    {
        "name": "Sandals Press",
        "base_url": "https://www.sandals.com/press-releases/",
        "source_type": "caribbean",
        "priority": 8,
        "use_playwright": False,
        "notes": "Sandals & Beaches all-inclusive resorts",
    },
    # =========================================================================
    # TIER 6: TRAVEL PUBLICATIONS (Priority 8-9)
    # =========================================================================
    {
        "name": "Travel + Leisure",
        "base_url": "https://www.travelandleisure.com/hotels-resorts/hotel-openings",
        "source_type": "travel_pub",
        "priority": 9,
        "use_playwright": False,
        "notes": "Luxury focus - annual It List",
    },
]


# =============================================================================
# SEED FUNCTION
# =============================================================================


async def seed_sources(force: bool = False):
    """
    Populate database with sources.

    Args:
        force: If True, update existing sources. If False, skip existing.
    """
    logger.info("=" * 60)
    logger.info("SMART LEAD HUNTER - SOURCE SEEDING")
    logger.info("=" * 60)

    added = 0
    updated = 0
    skipped = 0

    async with async_session() as db:
        for source_data in SOURCES:
            name = source_data["name"]
            base_url = source_data["base_url"]

            # Check if source exists
            result = await db.execute(select(Source).where(Source.base_url == base_url))
            existing = result.scalars().first()

            if existing:
                if force:
                    # Update existing
                    existing.name = name
                    existing.source_type = source_data.get("source_type", "aggregator")
                    existing.priority = source_data.get("priority", 5)
                    existing.use_playwright = source_data.get("use_playwright", False)
                    existing.notes = source_data.get("notes", "")
                    updated += 1
                    logger.info(f"   🔄 Updated: {name}")
                else:
                    skipped += 1
                    logger.info(f"   ⏭️  Skipped (exists): {name}")
            else:
                # Create new
                source = Source(
                    name=name,
                    base_url=base_url,
                    source_type=source_data.get("source_type", "aggregator"),
                    priority=source_data.get("priority", 5),
                    use_playwright=source_data.get("use_playwright", False),
                    notes=source_data.get("notes", ""),
                    is_active=True,
                    health_status="new",
                )
                db.add(source)
                added += 1
                logger.info(f"   ✅ Added: {name}")

        await db.commit()

    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ SEEDING COMPLETE")
    logger.info(f"   Added: {added}")
    logger.info(f"   Updated: {updated}")
    logger.info(f"   Skipped: {skipped}")
    logger.info(f"   Total in DB: {added + updated + skipped}")
    logger.info("=" * 60)


async def list_sources():
    """List all sources in database"""
    async with async_session() as db:
        result = await db.execute(
            select(Source).order_by(Source.priority.desc(), Source.name)
        )
        sources = result.scalars().all()

        print("\n" + "=" * 70)
        print("SOURCES IN DATABASE")
        print("=" * 70)
        print(f"\n{'Name':<35} {'Type':<15} {'Pri':<4} {'Health':<10}")
        print("-" * 70)

        for s in sources:
            status_icon = {
                "healthy": "🟢",
                "degraded": "🟡",
                "failing": "🟠",
                "dead": "🔴",
                "new": "⚪",
            }.get(s.health_status, "⚪")

            print(
                f"{s.name:<35} {s.source_type:<15} {s.priority:<4} {status_icon} {s.health_status}"
            )

        print("-" * 70)
        print(f"Total: {len(sources)} sources")
        print("=" * 70)


async def clear_sources():
    """Clear all sources (use with caution!)"""
    async with async_session() as db:
        result = await db.execute(select(Source))
        sources = result.scalars().all()

        for source in sources:
            await db.delete(source)

        await db.commit()
        logger.info(f"🗑️ Cleared {len(sources)} sources from database")


# =============================================================================
# CLI
# =============================================================================


async def main():
    import sys

    if len(sys.argv) < 2:
        print("""
SMART LEAD HUNTER - SOURCE SEED SCRIPT
======================================

Commands:
    python -m app.services.source_seed seed          # Add sources (skip existing)
    python -m app.services.source_seed seed --force  # Add/update all sources
    python -m app.services.source_seed list          # List all sources in DB
    python -m app.services.source_seed clear         # Clear all sources (caution!)

This is a ONE-TIME setup script. After running, manage sources via dashboard.
""")
        return

    command = sys.argv[1]

    if command == "seed":
        force = "--force" in sys.argv
        await seed_sources(force=force)

    elif command == "list":
        await list_sources()

    elif command == "clear":
        confirm = input("⚠️  This will DELETE all sources. Type 'yes' to confirm: ")
        if confirm.lower() == "yes":
            await clear_sources()
        else:
            print("Cancelled.")

    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())
