import asyncio
import asyncpg
from app.config import settings


async def go():
    url = settings.database_url.replace("+asyncpg", "")
    c = await asyncpg.connect(url)
    rows = await c.fetch(
        """
        SELECT name, title, scope, tier, score,
               strategist_priority, strategist_reasoning,
               created_at, updated_at
        FROM lead_contacts
        WHERE lead_id = 541
        ORDER BY created_at
        """
    )
    print(f"\n{len(rows)} contacts for lead 541:\n")
    for r in rows:
        sp = r["strategist_priority"] or "NULL"
        created = r["created_at"].strftime("%m-%d %H:%M") if r["created_at"] else "?"
        updated = r["updated_at"].strftime("%m-%d %H:%M") if r["updated_at"] else "?"
        name = (r["name"] or "")[:25]
        title = (r["title"] or "")[:45]
        scope = r["scope"] or "?"
        score = r["score"] or 0
        print(f"[{sp:4}] {name:25} | {title:45} | {scope:18} | score={score}")
        print(f"       created={created} updated={updated}")
        reason = r["strategist_reasoning"] or ""
        if reason:
            print(f"       reason: {reason[:100]}")
        print()
    await c.close()


asyncio.run(go())
