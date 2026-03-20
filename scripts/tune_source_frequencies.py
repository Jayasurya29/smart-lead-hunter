"""
Source Frequency Tuner
=====================
Sets optimal scrape frequencies based on source type and priority.

Logic:
- Chain newsrooms: They post 1-3x/week → scrape 2x/week
- Luxury independent: Slow posters → weekly
- Aggregators (Hotel Dive, etc): Active daily → daily
- Industry pubs: 2-3x/week content → every 3 days
- Florida/Caribbean regional: Weekly content → weekly
- PR wires: Daily releases → daily
- Travel pubs: Weekly features → weekly

Also sets discovery_interval_days for gold URL refresh.
"""
import asyncio
from sqlalchemy import select, update
from app.database import async_session
from app.models.source import Source


# Frequency rules by source_type
FREQUENCY_RULES = {
    "chain_newsroom": {
        "scrape_frequency": "twice_weekly",  # Mon + Thu
        "discovery_interval_days": 14,
        "max_depth": 2,
        "notes_suffix": "Chain newsrooms post 1-3x/week"
    },
    "luxury_independent": {
        "scrape_frequency": "weekly",
        "discovery_interval_days": 14,
        "max_depth": 2,
        "notes_suffix": "Luxury brands post infrequently"
    },
    "aggregator": {
        "scrape_frequency": "daily",
        "discovery_interval_days": 7,
        "max_depth": 3,
        "notes_suffix": "Aggregators update daily, highest yield"
    },
    "industry": {
        "scrape_frequency": "every_3_days",
        "discovery_interval_days": 10,
        "max_depth": 2,
        "notes_suffix": "Industry pubs post 2-3x/week"
    },
    "florida": {
        "scrape_frequency": "weekly",
        "discovery_interval_days": 14,
        "max_depth": 2,
        "notes_suffix": "Regional Florida sources"
    },
    "caribbean": {
        "scrape_frequency": "weekly",
        "discovery_interval_days": 14,
        "max_depth": 2,
        "notes_suffix": "Regional Caribbean sources"
    },
    "travel_pub": {
        "scrape_frequency": "weekly",
        "discovery_interval_days": 14,
        "max_depth": 2,
        "notes_suffix": "Travel publications, feature-style content"
    },
    "pr_wire": {
        "scrape_frequency": "daily",
        "discovery_interval_days": 7,
        "max_depth": 1,
        "notes_suffix": "PR wires have daily releases"
    },
}

# Priority overrides: P10 sources always get daily regardless
PRIORITY_OVERRIDE_THRESHOLD = 9  # P9-P10 get bumped to at minimum every_3_days


async def tune_sources():
    async with async_session() as session:
        result = await session.execute(
            select(Source).order_by(Source.source_type, Source.priority.desc())
        )
        sources = result.scalars().all()

        print(f"📊 Tuning {len(sources)} sources\n")
        print(f"{'Source':<45} {'Type':<20} {'P':<3} {'Old Freq':<12} {'New Freq':<15} {'Discovery'}")
        print("─" * 130)

        updated = 0
        for src in sources:
            rules = FREQUENCY_RULES.get(src.source_type, {
                "scrape_frequency": "weekly",
                "discovery_interval_days": 14,
                "max_depth": 2,
                "notes_suffix": "Unknown type"
            })

            new_freq = rules["scrape_frequency"]
            discovery_days = rules["discovery_interval_days"]

            # Priority override: high-priority sources get more frequent scraping
            if src.priority >= PRIORITY_OVERRIDE_THRESHOLD:
                if new_freq in ("weekly", "twice_weekly"):
                    new_freq = "every_3_days"
                    discovery_days = min(discovery_days, 7)

            # High-yield override: if source has historically found lots of leads
            if (src.leads_found or 0) > 10:
                if new_freq == "weekly":
                    new_freq = "every_3_days"

            old_freq = src.scrape_frequency
            changed = old_freq != new_freq

            src.scrape_frequency = new_freq
            src.discovery_interval_days = discovery_days
            src.max_depth = rules.get("max_depth", src.max_depth)

            marker = "🔄" if changed else "  "
            print(f"{marker} {src.name[:43]:<43} {src.source_type:<20} {src.priority:<3} {old_freq:<12} {new_freq:<15} {discovery_days}d")

            if changed:
                updated += 1

        await session.commit()
        print(f"\n✅ Updated {updated} sources")

        # Summary
        freq_counts = {}
        for src in sources:
            freq_counts[src.scrape_frequency] = freq_counts.get(src.scrape_frequency, 0) + 1

        print(f"\n📋 Frequency Distribution:")
        for freq, count in sorted(freq_counts.items()):
            print(f"   {freq:<15} {count} sources")

        # Estimate daily scrape load
        daily_map = {
            "daily": 1.0,
            "every_3_days": 0.33,
            "twice_weekly": 0.29,
            "weekly": 0.14,
            "monthly": 0.03,
        }
        daily_load = sum(daily_map.get(src.scrape_frequency, 0.14) for src in sources)
        print(f"\n📈 Estimated daily scrape load: ~{daily_load:.0f} sources/day (down from 79)")


if __name__ == "__main__":
    asyncio.run(tune_sources())
