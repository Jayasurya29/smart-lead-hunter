import asyncio, os, sys
sys.path.insert(0, os.getcwd())
from dotenv import load_dotenv
load_dotenv()

async def report():
    from app.database import async_session
    from sqlalchemy import text

    async with async_session() as s:
        # Enriched vs unenriched by timeline
        rows = (await s.execute(text("""
            SELECT pl.timeline_label,
                   COUNT(DISTINCT pl.id) as total,
                   COUNT(DISTINCT CASE WHEN lc.id IS NOT NULL THEN pl.id END) as enriched
            FROM potential_leads pl
            LEFT JOIN lead_contacts lc ON lc.lead_id = pl.id
            WHERE pl.status = 'new'
            GROUP BY pl.timeline_label
            ORDER BY CASE pl.timeline_label
                WHEN 'URGENT' THEN 1 WHEN 'HOT' THEN 2
                WHEN 'WARM' THEN 3 WHEN 'COOL' THEN 4 ELSE 5 END
        """))).fetchall()

        print("ENRICHMENT STATUS:")
        print(f"  {'Timeline':<10} {'Total':>6} {'Enriched':>10} {'Missing':>9}")
        print(f"  {'-'*38}")
        for r in rows:
            missing = r[1] - r[2]
            print(f"  {r[0] or 'TBD':<10} {r[1]:>6} {r[2]:>10} {missing:>9}")

asyncio.run(report())
