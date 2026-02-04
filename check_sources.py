"""Check all configured sources and their performance"""
import asyncio
from sqlalchemy import select
from app.database import async_session
from app.models import Source


async def check_sources():
    async with async_session() as db:
        result = await db.execute(
            select(Source).order_by(Source.leads_found.desc().nullslast())
        )
        sources = result.scalars().all()

        print(f"\nTotal sources: {len(sources)}")
        print(f"Active: {len([s for s in sources if s.is_active])}")
        print()
        print("=" * 100)
        print(f"{'NAME':<35} {'TYPE':<12} {'STATUS':<8} {'HEALTH':<10} {'LEADS':<6} {'URL'}")
        print("=" * 100)

        for s in sources:
            status = "Active" if s.is_active else "Off"
            health = s.health_status or "new"
            leads = s.leads_found or 0
            src_type = (s.source_type or "")[:11]
            url = (s.base_url or "")[:40]
            print(f"{s.name[:34]:<35} {src_type:<12} {status:<8} {health:<10} {leads:<6} {url}")
        
        print("=" * 100)
        print(f"\nTop performers (with leads):")
        for s in sources[:10]:
            if s.leads_found and s.leads_found > 0:
                print(f"  - {s.name}: {s.leads_found} leads")


if __name__ == "__main__":
    asyncio.run(check_sources())