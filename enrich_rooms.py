import asyncio
import sys
import re
import httpx
from app.database import async_session
from app.config import settings
from sqlalchemy import select, update
from app.models.existing_hotel import ExistingHotel
from app.services.revenue_calculator import (
    calculate_new_opening, calculate_annual_recurring, detect_tier_from_brand,
)

TIER_MAP = {
    'tier1_ultra_luxury': 'ultra_luxury',
    'tier2_luxury': 'luxury',
    'tier3_upper_upscale': 'upper_upscale',
    'tier4_upscale': 'upscale',
}

GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={settings.gemini_api_key}"


async def get_room_count(client, hotel_name, city, state):
    prompt = f"How many guest rooms does {hotel_name} in {city}, {state} have? Reply ONLY a number, nothing else."
    try:
        resp = await client.post(GEMINI_URL, json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 500,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }, timeout=30)
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        numbers = re.findall(r'\d+', text)
        if numbers:
            rooms = int(numbers[0])
            if 5 <= rooms <= 5000:
                return rooms
        return None
    except Exception as e:
        print(f"    Error: {e}")
        return None


async def run(zone_name):
    async with async_session() as session:
        result = await session.execute(
            select(ExistingHotel)
            .where(ExistingHotel.zone == zone_name)
            .where(ExistingHotel.status != 'rejected')
            .where(
                (ExistingHotel.room_count.is_(None)) | (ExistingHotel.room_count == 0)
            )
            .order_by(ExistingHotel.name)
        )
        hotels = result.scalars().all()

    print(f"Zone: {zone_name}")
    print(f"Hotels to enrich: {len(hotels)}")
    if not hotels:
        print("Nothing to enrich!")
        return

    enriched = 0
    failed = 0

    async with httpx.AsyncClient() as client:
        for i, h in enumerate(hotels, 1):
            city = h.city or ""
            state = h.state or "Florida"
            rooms = await get_room_count(client, h.name, city, state)

            if rooms:
                tier_key = TIER_MAP.get(h.brand_tier)
                if not tier_key and h.brand:
                    tier_key = detect_tier_from_brand(h.brand)

                rev_opening = None
                rev_annual = None
                if tier_key:
                    loc = f"{city}, {state}" if city else state
                    prop_type = h.property_type or "resort"
                    try:
                        opening = calculate_new_opening(rooms, tier_key, prop_type, loc)
                        annual = calculate_annual_recurring(rooms, tier_key, prop_type, loc)
                        rev_opening = round(opening.ja_addressable)
                        rev_annual = round(annual.ja_addressable)
                    except Exception:
                        pass

                async with async_session() as session:
                    await session.execute(
                        update(ExistingHotel)
                        .where(ExistingHotel.id == h.id)
                        .values(
                            room_count=rooms,
                            revenue_opening=rev_opening,
                            revenue_annual=rev_annual,
                        )
                    )
                    await session.commit()

                rev_text = ""
                if rev_annual:
                    rev_text = " | Annual: ${:,}".format(rev_annual)
                print(f"  [{i}/{len(hotels)}] {h.name:<55} {rooms:>5} rooms{rev_text}")
                enriched += 1
            else:
                print(f"  [{i}/{len(hotels)}] {h.name:<55}   ??? (no data)")
                failed += 1

            await asyncio.sleep(0.5)

    print(f"\nDone: {enriched} enriched, {failed} no data, {len(hotels)} total")


if __name__ == "__main__":
    zone = sys.argv[1] if len(sys.argv) > 1 else "South Florida"
    asyncio.run(run(zone))
