# -*- coding: utf-8 -*-
"""
SMART LEAD HUNTER - Lead Data Enrichment
==========================================
Fills missing lead fields (opening_date, brand_tier, room_count, brand)
using web search (Serper) + AI extraction (Gemini).

Different from contact_enrichment.py which finds PEOPLE.
This finds HOTEL DATA.

Two modes:
- "smart": Only search for missing fields (fast, targeted)
- "full":  Search everything for latest updates (comprehensive refresh)

Used by:
- POST /api/leads/{id}/smart-fill  (manual button)
- POST /api/leads/batch-smart-fill (batch endpoint)
- auto_enrich task (scheduled batch, future)
"""

import json
import logging
import os
import re
from typing import Dict, Optional

import httpx

from app.services.utils import get_timeline_label

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")


async def _search_web(query: str, max_results: int = 5) -> list[dict]:
    """Search with Serper API."""
    if not SERPER_API_KEY:
        return []

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY},
                json={"q": query, "num": max_results},
            )
            if resp.status_code == 200:
                data = resp.json()
                results = []
                for r in data.get("organic", [])[:max_results]:
                    results.append(
                        {
                            "title": r.get("title", ""),
                            "snippet": r.get("snippet", ""),
                            "url": r.get("link", ""),
                        }
                    )
                return results
    except Exception as e:
        logger.warning(f"Serper search failed: {e}")
    return []


async def _call_gemini(prompt: str, temperature: float = 0.1) -> Optional[str]:
    """Call Gemini API for extraction."""
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY not set")
        return None

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": temperature},
                },
            )
            if resp.status_code != 200:
                logger.warning(f"Gemini error {resp.status_code}: {resp.text[:200]}")
                return None

            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        logger.warning(f"Gemini call failed: {e}")
        return None


async def enrich_lead_data(
    hotel_name: str,
    city: str = "",
    state: str = "",
    brand: str = "",
    current_opening_date: str = "",
    current_brand_tier: str = "",
    current_room_count: int = 0,
    mode: str = "smart",
) -> Dict:
    """
    Search the web for hotel info and extract missing fields.

    Modes:
    - "smart": Only search for and return missing fields (fast, targeted)
    - "full":  Search everything, return latest data even if values exist

    Returns dict with any fields that were found:
    {
        "opening_date": "Q3 2026",
        "brand_tier": "tier2_luxury",
        "brand": "Auberge Collection",
        "room_count": 150,
        "description": "...",
        "source_url": "https://...",
        "confidence": "high|medium|low",
        "changes": ["opening_date", "brand_tier"],
    }
    """
    result = {
        "changes": [],
        "confidence": "low",
        "source_url": None,
    }

    # Determine what to search for
    location = ", ".join(filter(None, [city, state]))

    if mode == "full":
        # Refresh everything
        missing = ["opening_date", "brand_tier", "room_count", "brand"]
        queries = [
            f'"{hotel_name}" {location} hotel opening 2025 2026 2027',
            f'"{hotel_name}" {location} hotel rooms brand development',
        ]
    else:
        # Smart mode: only search for missing fields
        missing = []
        queries = []

        if not current_opening_date:
            missing.append("opening_date")
            queries.append(f'"{hotel_name}" {location} opening date 2025 2026 2027')
        if not current_room_count:
            missing.append("room_count")
            queries.append(f'"{hotel_name}" {location} hotel rooms keys guest rooms')
        if not current_brand_tier or current_brand_tier in ("unknown", ""):
            missing.append("brand_tier")
            queries.append(f'"{hotel_name}" hotel luxury boutique upscale brand')
        if not brand:
            missing.append("brand")

        if not missing:
            return result

        # Always include a general query for context
        queries.insert(0, f'"{hotel_name}" {location} hotel')

    # Limit to 3 queries max
    queries = queries[:3]

    # Search
    all_snippets = []
    all_urls = []
    for q in queries:
        results = await _search_web(q, max_results=5)
        for r in results:
            snippet = f"{r['title']}. {r['snippet']}"
            all_snippets.append(snippet)
            if r["url"]:
                all_urls.append(r["url"])

    if not all_snippets:
        logger.info(f"No search results for: {hotel_name}")
        return result

    # Build Gemini prompt
    snippets_text = "\n".join(f"- {s}" for s in all_snippets[:10])

    fields_instruction = (
        "Extract ALL available information, even if current values exist. Report the LATEST data."
        if mode == "full"
        else f"Extract ONLY these missing fields: {', '.join(missing)}"
    )

    prompt = f"""You are a hotel industry research assistant. {fields_instruction}

HOTEL: {hotel_name}
LOCATION: {location}
BRAND: {brand or "Unknown"}
CURRENT OPENING DATE: {current_opening_date or "Unknown"}
CURRENT TIER: {current_brand_tier or "Unknown"}
CURRENT ROOMS: {current_room_count or "Unknown"}

FIELDS TO FIND: {", ".join(missing)}
MODE: {"Full refresh - report latest data even if values exist" if mode == "full" else "Smart fill - only find missing fields"}

WEB SEARCH RESULTS:
{snippets_text}

TIER DEFINITIONS:
- tier1_ultra_luxury: Ritz-Carlton, Four Seasons, Aman, Rosewood, Faena level ($500+/night)
- tier2_luxury: JW Marriott, Conrad, Kimpton, Thompson, W Hotel, boutique luxury ($300-500/night)
- tier3_upper_upscale: Marriott, Hilton, Westin, Sheraton, upscale independent ($150-300/night)
- tier4_upscale: Smaller independents, business hotels ($100-200/night)

CRITICAL CHECK: If the search results indicate this hotel has ALREADY OPENED (is currently operating, accepting guests, has reviews), include "already_opened": true and "opened_date": "Month Year" in your response. This is the most important check.
Respond ONLY with a JSON object. Include ONLY fields you found evidence for. Do NOT guess.
Example: {{"opening_date": "Q3 2026", "brand_tier": "tier2_luxury", "room_count": 150, "brand": "Auberge Collection", "description": "Brief 1-sentence summary", "confidence": "high"}}

If you cannot find reliable information for a field, DO NOT include it.
Confidence levels: "high" (multiple sources agree), "medium" (one source), "low" (inferred)
"""

    gemini_response = await _call_gemini(prompt)
    if not gemini_response:
        return result

    # Parse response
    try:
        text = re.sub(r"```json\s*", "", gemini_response)
        text = re.sub(r"```\s*", "", text)
        parsed = json.loads(text.strip())
    except (json.JSONDecodeError, ValueError):
        logger.warning(f"Failed to parse Gemini response for {hotel_name}")
        return result

    # Map results
    if "opening_date" in parsed:
        new_date = str(parsed["opening_date"]).strip()
        # Reject vague dates
        vague = [
            "later this year",
            "coming soon",
            "tbd",
            "tba",
            "unknown",
            "announced",
            "not announced",
            "n/a",
            "to be announced",
            "to be determined",
            "this year",
            "next year",
        ]
        is_vague = new_date.lower() in vague or len(new_date) < 4
        if not is_vague:
            # Only overwrite in full mode if new date is MORE specific
            if mode == "full" and current_opening_date:
                if len(new_date) > len(current_opening_date):
                    result["opening_date"] = new_date
                    result["timeline_label"] = get_timeline_label(new_date)
                    result["changes"].append("opening_date")
            elif not current_opening_date:
                result["opening_date"] = new_date
                result["timeline_label"] = get_timeline_label(new_date)
                result["changes"].append("opening_date")

    if "brand_tier" in parsed:
        if mode == "full" or not current_brand_tier or current_brand_tier == "unknown":
            tier = parsed["brand_tier"]
            if tier in (
                "tier1_ultra_luxury",
                "tier2_luxury",
                "tier3_upper_upscale",
                "tier4_upscale",
            ):
                result["brand_tier"] = tier
                result["changes"].append("brand_tier")

    if "room_count" in parsed:
        if mode == "full" or not current_room_count:
            try:
                rc = int(parsed["room_count"])
                if 5 < rc < 5000:
                    result["room_count"] = rc
                    result["changes"].append("room_count")
            except (ValueError, TypeError):
                pass

    if "brand" in parsed:
        if mode == "full" or not brand:
            result["brand"] = str(parsed["brand"])
            result["changes"].append("brand")

    if "description" in parsed:
        result["description"] = str(parsed["description"])[:500]

    # Check if hotel already opened
    if parsed.get("already_opened"):
        result["already_opened"] = True
        result["opened_date"] = parsed.get("opened_date", "")
        result["changes"].append("already_opened")

    result["confidence"] = parsed.get("confidence", "medium")
    result["source_url"] = all_urls[0] if all_urls else None

    if result["changes"]:
        logger.info(
            f"Smart Fill ({mode}): {hotel_name} -> {', '.join(result['changes'])} "
            f"(confidence: {result['confidence']})"
        )

    return result


async def batch_smart_fill(limit: int = 10, mode: str = "smart") -> Dict:
    """
    Find leads with missing data and enrich them.
    Called by scheduled task or manual trigger.

    Returns: {"checked": N, "enriched": N, "details": [...]}
    """
    from sqlalchemy import select, or_
    from app.database import async_session
    from app.models.potential_lead import PotentialLead

    stats = {"checked": 0, "enriched": 0, "details": []}

    async with async_session() as session:
        # Find leads missing key data, prioritize by score
        query = (
            select(PotentialLead)
            .where(
                PotentialLead.status == "new",
                or_(
                    PotentialLead.opening_date.is_(None),
                    PotentialLead.opening_date == "",
                    PotentialLead.brand_tier.is_(None),
                    PotentialLead.brand_tier == "unknown",
                    PotentialLead.brand_tier == "",
                    PotentialLead.room_count.is_(None),
                    PotentialLead.room_count == 0,
                ),
            )
            .order_by(PotentialLead.lead_score.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        leads = result.scalars().all()

        if not leads:
            logger.info("Smart Fill: No leads need data enrichment")
            return stats

        logger.info(f"Smart Fill: {len(leads)} leads need data enrichment")

        for lead in leads:
            stats["checked"] += 1

            enriched = await enrich_lead_data(
                hotel_name=lead.hotel_name,
                city=lead.city or "",
                state=lead.state or "",
                brand=lead.brand or "",
                current_opening_date=lead.opening_date or "",
                current_brand_tier=lead.brand_tier or "",
                current_room_count=lead.room_count or 0,
                mode=mode,
            )

            if not enriched.get("changes"):
                stats["details"].append(
                    {
                        "name": lead.hotel_name,
                        "status": "no_data_found",
                    }
                )
                continue

            # Apply changes
            changes = []
            if "opening_date" in enriched:
                lead.opening_date = enriched["opening_date"]
                lead.timeline_label = enriched.get(
                    "timeline_label",
                    get_timeline_label(enriched["opening_date"]),
                )
                changes.append(f"opening_date={enriched['opening_date']}")

            if "brand_tier" in enriched:
                lead.brand_tier = enriched["brand_tier"]
                changes.append(f"tier={enriched['brand_tier']}")

            if "room_count" in enriched:
                lead.room_count = enriched["room_count"]
                changes.append(f"rooms={enriched['room_count']}")

            if "brand" in enriched:
                lead.brand = enriched["brand"]
                changes.append(f"brand={enriched['brand']}")

            if "description" in enriched:
                if mode == "full" or not lead.description:
                    lead.description = enriched["description"]

            # Recalculate score with new data
            from app.services.scorer import calculate_lead_score

            score_result = calculate_lead_score(
                hotel_name=lead.hotel_name,
                city=lead.city,
                state=lead.state,
                country=lead.country,
                opening_date=lead.opening_date,
                room_count=lead.room_count,
                contact_name=lead.contact_name,
                contact_email=lead.contact_email,
                contact_phone=lead.contact_phone,
                brand=lead.brand,
            )
            if score_result.get("should_save", True):
                lead.lead_score = score_result["total_score"]

            stats["enriched"] += 1
            stats["details"].append(
                {
                    "name": lead.hotel_name,
                    "status": "enriched",
                    "changes": changes,
                    "confidence": enriched.get("confidence", "unknown"),
                }
            )

        await session.commit()

    logger.info(
        f"Smart Fill complete: {stats['checked']} checked, "
        f"{stats['enriched']} enriched"
    )
    return stats
