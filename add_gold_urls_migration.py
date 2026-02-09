"""
Migration: Add gold_urls tracking to sources table
Gold URLs = specific URLs on a source that actually produced leads
"""
import asyncio
from sqlalchemy import text
from app.database import engine as async_engine


async def migrate():
    async with async_engine.begin() as conn:
        # 1. gold_urls: JSONB dict of {url: {leads_found, last_hit, first_found}}
        try:
            await conn.execute(text("""
                ALTER TABLE sources ADD COLUMN gold_urls JSONB DEFAULT '{}'::jsonb
            """))
            print("✅ Added gold_urls column")
        except Exception as e:
            if "already exists" in str(e):
                print("⏭️  gold_urls already exists")
            else:
                raise

        # 2. last_discovery_at: When we last did a deep crawl to find new gold URLs
        try:
            await conn.execute(text("""
                ALTER TABLE sources ADD COLUMN last_discovery_at TIMESTAMPTZ
            """))
            print("✅ Added last_discovery_at column")
        except Exception as e:
            if "already exists" in str(e):
                print("⏭️  last_discovery_at already exists")
            else:
                raise

        # 3. discovery_interval_days: How often to rediscover (default 7 = weekly)
        try:
            await conn.execute(text("""
                ALTER TABLE sources ADD COLUMN discovery_interval_days INTEGER DEFAULT 7
            """))
            print("✅ Added discovery_interval_days column")
        except Exception as e:
            if "already exists" in str(e):
                print("⏭️  discovery_interval_days already exists")
            else:
                raise

        # 4. avg_lead_yield: Average leads per scrape (for prioritization)
        try:
            await conn.execute(text("""
                ALTER TABLE sources ADD COLUMN avg_lead_yield NUMERIC(5,2) DEFAULT 0.00
            """))
            print("✅ Added avg_lead_yield column")
        except Exception as e:
            if "already exists" in str(e):
                print("⏭️  avg_lead_yield already exists")
            else:
                raise

        # 5. total_scrapes: Count of scrapes for averaging
        try:
            await conn.execute(text("""
                ALTER TABLE sources ADD COLUMN total_scrapes INTEGER DEFAULT 0
            """))
            print("✅ Added total_scrapes column")
        except Exception as e:
            if "already exists" in str(e):
                print("⏭️  total_scrapes already exists")
            else:
                raise

    print("🎯 Gold URLs migration complete!")


if __name__ == "__main__":
    asyncio.run(migrate())