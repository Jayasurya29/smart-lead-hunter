"""
Migration: Add source_urls and source_extractions columns to potential_leads.

Run this ONCE:
    python add_source_tracking_migration.py
"""
import asyncio
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from app.database import engine as async_engine


async def migrate():
    async with async_engine.begin() as conn:
        # Check if columns already exist
        result = await conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'potential_leads' AND column_name IN ('source_urls', 'source_extractions')
        """))
        existing = {row[0] for row in result.fetchall()}

        if 'source_urls' not in existing:
            await conn.execute(text("""
                ALTER TABLE potential_leads
                ADD COLUMN source_urls JSONB DEFAULT '[]'::jsonb
            """))
            print("✅ Added source_urls column")
        else:
            print("⏭️ source_urls already exists")

        if 'source_extractions' not in existing:
            await conn.execute(text("""
                ALTER TABLE potential_leads
                ADD COLUMN source_extractions JSONB DEFAULT '{}'::jsonb
            """))
            print("✅ Added source_extractions column")
        else:
            print("⏭️ source_extractions already exists")

        # Backfill existing leads: copy source_url into source_urls array
        await conn.execute(text("""
            UPDATE potential_leads
            SET source_urls = jsonb_build_array(source_url)
            WHERE source_url IS NOT NULL
              AND (source_urls IS NULL OR source_urls = '[]'::jsonb)
        """))
        print("✅ Backfilled source_urls from existing source_url")

    print("\n🎯 Migration complete!")


if __name__ == "__main__":
    asyncio.run(migrate())