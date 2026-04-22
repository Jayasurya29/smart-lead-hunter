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

GEMINI_API_KEY = (
    "vertex-ai"  # Auth handled by gemini_client.py (Vertex AI $300 credits)
)
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

                # Extract answer box — often has direct answers like room counts
                if data.get("answerBox"):
                    ab = data["answerBox"]
                    answer_text = (
                        ab.get("answer") or ab.get("snippet") or ab.get("title") or ""
                    )
                    if answer_text:
                        results.append(
                            {
                                "title": "Direct Answer",
                                "snippet": answer_text,
                                "url": ab.get("link", ""),
                            }
                        )

                # Extract knowledge graph — hotel info cards
                if data.get("knowledgeGraph"):
                    kg = data["knowledgeGraph"]
                    kg_desc = kg.get("description") or kg.get("title") or ""
                    kg_attrs = " ".join(
                        f"{k}: {v}" for k, v in kg.get("attributes", {}).items()
                    )
                    if kg_desc or kg_attrs:
                        results.append(
                            {
                                "title": kg.get("title", "Knowledge Graph"),
                                "snippet": f"{kg_desc} {kg_attrs}".strip(),
                                "url": kg.get("website", ""),
                            }
                        )

                # Organic results
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
    """Call Gemini API via Vertex AI ($300 credits)."""
    from app.services.gemini_client import get_gemini_url, get_gemini_headers

    url = get_gemini_url("gemini-2.5-flash")
    headers = get_gemini_headers()

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                headers=headers,
                json={
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": temperature,
                        "maxOutputTokens": 8192,
                        "thinkingConfig": {"thinkingBudget": 0},
                    },
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
        missing = [
            "opening_date",
            "brand_tier",
            "room_count",
            "brand",
            "city",
            "state",
            "country",
        ]
        queries = [
            f'"{hotel_name}" {location} hotel opening 2025 2026 2027',
            f'"{hotel_name}" {location} hotel rooms brand development',
            # Name discovery — find the official name + operator
            f'"{hotel_name}" {location} hotel "managed by" OR "operated by" OR "officially" OR "known as"',
        ]
    else:
        # Smart mode: only search for missing fields
        missing = []
        queries = []

        # Always add a name-discovery query — correcting the name is always valuable
        queries.append(
            f'"{hotel_name}" {location} hotel announcement OR opening OR management'
        )

        if not city:
            missing.append("city")
        if not state:
            missing.append("state")
        if not current_opening_date:
            missing.append("opening_date")
            queries.append(f'"{hotel_name}" {location} opening date 2025 2026 2027')
        if not current_room_count:
            missing.append("room_count")
            queries.append(
                f'"{hotel_name}" {location} "rooms" OR "keys" OR "suites" total number'
            )
            queries.append(
                f'"{hotel_name}" site:sandals.com OR site:marriott.com OR site:hilton.com'
            )
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
    queries = queries[:4]

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
        else f"Extract ONLY these missing fields: {', '.join(missing)}. For room_count: look for patterns like 'X rooms', 'X keys', 'X guest rooms', 'X-room', 'X suites and rooms' — extract the NUMBER only."
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

TIER INFERENCE FOR INDEPENDENT/BOUTIQUE HOTELS:
When the brand is "Independent" or unknown, infer tier from these clues:
- Room rates mentioned in articles: "$995/night peak" = tier2_luxury, "$200-400" = tier3_upper_upscale
- Positioning words: "luxury" = tier2_luxury, "high-design" or "boutique" = tier3_upper_upscale
- Comparable properties mentioned: "joins Nantucket's luxury scene" = tier2_luxury
- Room count: under 50 rooms + design-forward = usually tier2_luxury or tier3_upper_upscale
- Developer reputation: Blue Flag Capital, Auberge, etc. = tier2_luxury
Do NOT leave brand_tier as "unknown" if ANY pricing or positioning clue exists.

ROOM COUNT EXTRACTION: For room_count, scan ALL snippets for patterns like:
"227 rooms", "300 keys", "150-room hotel", "120 guest rooms", "34 suites and 266 rooms"
Extract the TOTAL room/key count as an integer. This is usually stated clearly in hotel descriptions.

LOCATION EXTRACTION: If city or state is missing, extract from snippets or the hotel name itself.
- "Hard Rock Hotel & Casino San Juan" → city: "San Juan", state: "Puerto Rico", country: "Puerto Rico"
- "Kali Hotel and Rooftop, Autograph Collection" in Inglewood → city: "Inglewood", state: "California", country: "USA"
- "Royalton CHIC Jamaica Paradise Cove" in Runaway Bay → city: "Runaway Bay", state: "St. Ann", country: "Jamaica"
- For Caribbean islands, country = island name (e.g. "Jamaica", "Turks and Caicos", "Barbados")
- For US territories, country = "USA" but state = territory name (e.g. "Puerto Rico", "US Virgin Islands")

═══════════════════════════════════════════════════════════════
NAME INTELLIGENCE — ALWAYS EXTRACT THESE (regardless of mode):
═══════════════════════════════════════════════════════════════

1. official_name: The CORRECT, full official hotel name as used in press releases,
   the hotel's own website, or Marriott/Hilton/Hyatt listings. The input name may be
   a project code, developer shorthand, or abbreviated version.
   Examples:
   - "KPC Hollywood Park Hotel" → "Kali Hotel and Rooftop, Autograph Collection"
   - "Treasure Beach Village, Beaches Turks & Caicos" → same (already correct)
   - "Dreams Rose Hall Resort & Spa" → same (already correct)

2. search_name: A SHORT version of the hotel name for Google searches — strip
   "Resort & Spa", "Hotel", "An Autograph Collection Resort", "by Marriott" etc.
   Examples:
   - "Kali Hotel and Rooftop, Autograph Collection" → "Kali Hotel"
   - "Dreams Rose Hall Resort & Spa" → "Dreams Rose Hall"
   - "Secrets Macao Beach Punta Cana" → "Secrets Macao Beach"
   - "Royalton CHIC Jamaica Paradise Cove" → "Royalton CHIC Jamaica"

3. management_company: The company that OPERATES/MANAGES the hotel day-to-day.
   This is NOT always the brand owner. For soft brands like Autograph Collection,
   Tribute Portfolio, Curio Collection — the operator is often a third-party company.
   Examples:
   - Kali Hotel → Crescent Hotels & Resorts (NOT Marriott)
   - Dreams Rose Hall → Hyatt Inclusive Collection
   - Royalton CHIC Jamaica → Royalton Hotels & Resorts (formerly Blue Diamond)

4. owner: The company/person that OWNS the physical property.
   Examples:
   - Kali Hotel → KPC Development Company
   - Dreams Rose Hall → Hyatt (owned and operated)
   - Secrets Macao Beach → GSM Investissements Dominicana S.R.L.

5. former_names: JSON array of any PREVIOUS names this property had, if it was
   rebranded or converted. Empty array [] if it's a new build.
   Examples:
   - St. Regis Kapalua Bay → ["Montage Kapalua Bay", "The Residences at Kapalua Bay"]
   - Dreams Rose Hall → ["Hilton Rose Hall Resort & Spa"]
   - Kali Hotel → [] (new build)

═══════════════════════════════════════════════════════════════

CRITICAL CHECKS — READ CAREFULLY:

1. ALREADY OPEN: If the hotel is currently operating (has reviews, accepting guests, open for business), include "already_opened": true and "opened_date": "Month Year".

2. HURRICANE / DISASTER CLOSURE: If a date appears in the context of "closed for repairs", "closed due to hurricane", "closed until [date] for renovation/damage", "reopening after storm" — this is NOT a new opening date. Include "already_opened": true, "project_type": "renovation", and "reopening_date": "[date]". Do NOT put this date in opening_date.

3. RENOVATION REOPENING: If the hotel closed temporarily for refurbishment and is reopening, include "already_opened": true, "project_type": "renovation". The date is a reopening, not a new build opening.

4. NEW BUILD ONLY: Only use opening_date for hotels that are genuinely under construction for the first time and have never operated before.

Keywords that mean NOT a new opening: "closed for", "repair", "hurricane", "storm damage", "renovation", "refurbishment", "reopening", "temporarily closed", "damage".
Respond ONLY with a JSON object. Include ONLY fields you found evidence for. Do NOT guess.
Example: {{"opening_date": "Q3 2026", "brand_tier": "tier2_luxury", "room_count": 150, "brand": "Auberge Collection", "official_name": "Auberge Beach Residences & Spa", "search_name": "Auberge Beach", "management_company": "Auberge Resorts Collection", "owner": "The Related Group", "former_names": [], "description": "Brief 1-sentence summary", "confidence": "high"}}

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

    if "city" in parsed:
        c = str(parsed["city"]).strip()
        if c and c.lower() not in ("unknown", "n/a", ""):
            result["city"] = c
            result["changes"].append("city")
    if "state" in parsed:
        s = str(parsed["state"]).strip()
        if s and s.lower() not in ("unknown", "n/a", ""):
            result["state"] = s
            result["changes"].append("state")
    if "country" in parsed:
        co = str(parsed["country"]).strip()
        if co and co.lower() not in ("unknown", "n/a", ""):
            result["country"] = co
            result["changes"].append("country")

    if "description" in parsed:
        result["description"] = str(parsed["description"])[:500]

    # Check if hotel already opened or is a renovation reopening
    if parsed.get("already_opened"):
        result["already_opened"] = True
        result["opened_date"] = parsed.get("opened_date", "")
        result["changes"].append("already_opened")

        # If it's a renovation/hurricane closure, capture the reopening date
        # and project type — do NOT use the date as an opening_date
        if parsed.get("project_type") == "renovation":
            result["project_type"] = "renovation"
            result["changes"].append("project_type")
            # Use reopening_date as the opening_date so the lead shows
            # the correct date but is classified as renovation
            reopening = parsed.get("reopening_date", "")
            if reopening and "opening_date" not in result:
                result["opening_date"] = reopening
                result["changes"].append("opening_date")
            logger.info(
                f"Renovation/closure detected for {hotel_name} — "
                f"reopening: {reopening}. Classified as renovation, not new build."
            )

    # ── NAME INTELLIGENCE — always extracted ──
    if "official_name" in parsed:
        official = str(parsed["official_name"]).strip()
        if official and official.lower() != hotel_name.lower():
            result["official_name"] = official
            result["changes"].append("official_name")
            logger.info(f"Name correction: '{hotel_name}' → '{official}'")

    if "search_name" in parsed:
        result["search_name"] = str(parsed["search_name"]).strip()
        result["changes"].append("search_name")

    if "management_company" in parsed:
        mc = str(parsed["management_company"]).strip()
        if mc and mc.lower() not in ("unknown", "n/a", "none", ""):
            result["management_company"] = mc
            result["changes"].append("management_company")

    if "owner" in parsed:
        ow = str(parsed["owner"]).strip()
        if ow and ow.lower() not in ("unknown", "n/a", "none", ""):
            result["owner"] = ow
            result["changes"].append("owner")

    if "former_names" in parsed:
        fn = parsed["former_names"]
        if isinstance(fn, list) and fn:
            result["former_names"] = fn
            result["changes"].append("former_names")

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

            # Recalculate score + breakdown via rescore_lead so both stay
            # in sync. Previously this only updated lead_score, leaving
            # score_breakdown stale — the UI "Why this score?" would show
            # pre-SmartFill component points against the new total.
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
                try:
                    from app.services.rescore import rescore_lead

                    await rescore_lead(lead.id, session)
                except Exception as rescore_err:
                    logger.warning(
                        f"rescore after lead_data_enrichment failed for "
                        f"{lead.id}: {rescore_err} — falling back to direct "
                        f"score write"
                    )
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
        f"Smart Fill complete: {stats['checked']} checked, {stats['enriched']} enriched"
    )
    return stats
