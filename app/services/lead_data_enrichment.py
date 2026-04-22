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


async def _call_gemini(
    prompt: str,
    temperature: float = 0.1,
    response_schema: Optional[dict] = None,
) -> Optional[str]:
    """
    Call Gemini API via Vertex AI.

    Args:
        prompt: The prompt to send.
        temperature: Sampling temperature (0.1 = mostly deterministic).
        response_schema: Optional JSON schema to force structured output.
            When provided, Gemini returns ONLY valid JSON matching the schema
            — can't hallucinate fields, can't return invalid enum values.
            Critical for brand_tier which must be one of a fixed set of values.
    """
    from app.services.gemini_client import get_gemini_url, get_gemini_headers

    url = get_gemini_url("gemini-2.5-flash")
    headers = get_gemini_headers()

    generation_config = {
        "temperature": temperature,
        "maxOutputTokens": 8192,
        "thinkingConfig": {"thinkingBudget": 0},
    }
    # Structured output — forces valid JSON shape + enum constraints.
    # Without this, Gemini occasionally returns "brand_tier": "unknown"
    # or "luxury" (wrong format) which breaks downstream parsing. With
    # responseSchema, Gemini is FORCED to emit only the allowed values
    # (or omit the field entirely if uncertain).
    if response_schema:
        generation_config["responseMimeType"] = "application/json"
        generation_config["responseSchema"] = response_schema

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                headers=headers,
                json={
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": generation_config,
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
            "management_company",
            "owner",
        ]
        # Better, more targeted queries for Full Refresh.
        # The old queries were generic ("hotel opening 2025 2026 2027")
        # which often pulled low-quality aggregator results. These target
        # authoritative press wires + operator/owner specifics.
        queries = [
            # 1. Authoritative announcement (press wire targeting)
            f'"{hotel_name}" {location} opening (site:prnewswire.com OR site:businesswire.com OR site:hospitalitynet.org)',
            # 2. Room count + tier positioning
            f'"{hotel_name}" {location} "rooms" OR "keys" OR "suites" luxury OR boutique OR upscale',
            # 3. Operator / management company (often missed)
            f'"{hotel_name}" {location} "managed by" OR "operated by" OR "operator" OR "management company"',
            # 4. Owner / developer (critical for pre-opening leads)
            f'"{hotel_name}" {location} "developer" OR "owner" OR "ownership group" OR "investment group" OR "CEO"',
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

    # Cap queries — 5 is the sweet spot. More than this and we get
    # rate-limited on Serper for minimal quality gain.
    queries = queries[:5]

    # Search — gather richer snippet pool (6 per query × 5 queries = 30 snippets max)
    all_snippets = []
    all_urls = []
    for q in queries:
        results = await _search_web(q, max_results=6)
        for r in results:
            snippet = f"{r['title']}. {r['snippet']}"
            all_snippets.append(snippet)
            if r["url"]:
                all_urls.append(r["url"])

    if not all_snippets:
        logger.info(f"No search results for: {hotel_name}")
        return result

    # Build Gemini prompt. Use up to 25 snippets now (was 10) — more context
    # means better tier inference for unusual brands like Nickelodeon.
    snippets_text = "\n".join(f"- {s}" for s in all_snippets[:25])

    # Diagnostic: log snippet count + sample so we can see what context Gemini got.
    # If Gemini later returns None for operator/owner despite the hotel having clear
    # press coverage, this tells us whether the problem is search (no snippets)
    # or prompt (snippets present but Gemini ignored them).
    logger.info(
        f"Smart Fill [{hotel_name}] search fed Gemini {len(all_snippets)} snippets "
        f"(using top 25). First snippet: {all_snippets[0][:200] if all_snippets else 'NONE'!r}"
    )

    # ── USER-EDIT AWARENESS ──
    # If the user manually set brand_tier / brand / room_count, tell Gemini
    # to RESPECT those values unless it has strong contradicting evidence.
    # Without this, Full Refresh overwrites carefully-curated human edits
    # with automated guesses.
    _PROTECTED_SENTINELS = {"", "unknown", "none", "n/a", "tbd"}
    user_verified_notes = []
    if (
        current_brand_tier
        and current_brand_tier.strip().lower() not in _PROTECTED_SENTINELS
    ):
        user_verified_notes.append(
            f"- brand_tier = {current_brand_tier} (USER-VERIFIED — keep unless "
            f"authoritative press explicitly contradicts it)"
        )
    if brand and brand.strip().lower() not in _PROTECTED_SENTINELS:
        user_verified_notes.append(
            f"- brand = {brand} (USER-VERIFIED — keep unless authoritative press "
            f"shows a different official brand)"
        )
    if current_room_count and current_room_count > 0:
        user_verified_notes.append(
            f"- room_count = {current_room_count} (USER-VERIFIED — keep unless "
            f"press gives a different specific number)"
        )
    user_edits_block = ""
    if user_verified_notes:
        user_edits_block = (
            "\nUSER-VERIFIED FIELDS (PRESERVE UNLESS STRONG EVIDENCE):\n"
            + "\n".join(user_verified_notes)
            + "\nWhen a value is USER-VERIFIED, omit it from your JSON response "
            "unless you have direct quotes from a press release or official site "
            "that contradict it. DO NOT overwrite a user-verified value with a guess.\n"
        )

    fields_instruction = (
        "Extract ALL available information from the search results below. Preserve user-verified values."
        if mode == "full"
        else f"Extract ONLY these missing fields: {', '.join(missing)}. For room_count: look for patterns like 'X rooms', 'X keys', 'X guest rooms', 'X-room', 'X suites and rooms' — extract the NUMBER only."
    )

    # ── AGGRESSIVE ENTITY EXTRACTION INSTRUCTION ──
    # Empirically discovered that Gemini returns `null` for management_company/
    # owner/developer even when snippets clearly contain the info (because the
    # structured schema lets optional fields be null and Gemini plays safe).
    # This block FORCES Gemini to actively scan the snippets and classify
    # every entity mentioned into the four roles.
    entity_extraction_directive = """

ENTITY EXTRACTION — MANDATORY:

The search result snippets will mention multiple company names.
You MUST scan every snippet and classify EACH company name into ONE of these roles:

  [A] BRAND — the hotel brand name (e.g. "Nickelodeon Hotels & Resorts", "Autograph Collection")
  [B] OPERATOR — the day-to-day hotel management company (e.g. "Lion Star Hospitality", "Commonwealth Hotels")
  [C] OWNER — the property-owning real estate entity (e.g. "Teramir Group", "Birkla Investment Group")
  [D] DEVELOPER — the entity building/developing the property (e.g. "Everest Place", often same as OWNER)
  [E] IP LICENSOR — the media/brand IP owner (e.g. "Paramount" for Nickelodeon brand)
  [F] OTHER — not relevant (PR agencies, analysts, unrelated companies)

If snippets mention "operated by X" or "X Hospitality" → X is the OPERATOR, populate management_company.
If snippets mention "owned by Y" or "Y Group" with real estate context → Y is OWNER.
If snippets mention "developed by Z" or "Z development complex" → Z is DEVELOPER.
If snippets mention a media brand (Paramount, Disney) that licenses the name → IP LICENSOR (do NOT put in owner or mgmt_company).

DO NOT return null for management_company/owner/developer when snippets clearly name relevant entities.
Null means "no snippet mentioned this role at all" — it should be rare for well-documented hotels.
When in doubt, make your best inference from the snippet evidence. An inferred value is better than null.
"""

    prompt = f"""You are a hotel industry research assistant. {fields_instruction}
{entity_extraction_directive}
HOTEL: {hotel_name}
LOCATION: {location}
BRAND: {brand or "Unknown"}
CURRENT OPENING DATE: {current_opening_date or "Unknown"}
CURRENT TIER: {current_brand_tier or "Unknown"}
CURRENT ROOMS: {current_room_count or "Unknown"}

FIELDS TO FIND: {", ".join(missing)}
MODE: {"Full refresh - report latest data, preserve user-verified values" if mode == "full" else "Smart fill - only find missing fields"}
{user_edits_block}
WEB SEARCH RESULTS:
{snippets_text}

TIER DEFINITIONS (MUST use one of these exact values or OMIT the field entirely):
- tier1_ultra_luxury: Ritz-Carlton, Four Seasons, Aman, Rosewood, Faena, St. Regis, Pendry, Auberge level ($500+/night)
- tier2_luxury: JW Marriott, Conrad, Kimpton, Thompson, W Hotel, boutique luxury, themed experiential resorts ($300-500/night)
- tier3_upper_upscale: Marriott, Hilton, Westin, Sheraton, Hyatt Regency, Hyatt Centric, Kimpton, upscale independent ($150-300/night)
- tier4_upscale: Courtyard, Hilton Garden Inn, Hyatt Place, AC Hotels, Cambria, smaller independents ($100-200/night)
- tier5_upper_midscale: Hampton Inn, Holiday Inn Express, Comfort Inn ($80-150/night)
- tier6_midscale: La Quinta, Wingate, Country Inn ($60-120/night)
- tier7_economy: Motel 6, Days Inn, Econo Lodge ($40-80/night)

TIER INFERENCE RULES (CRITICAL — apply these in priority order):

1. THEMED / EXPERIENTIAL / FAMILY RESORTS (Nickelodeon, Margaritaville, Disney, Universal, Legoland, Great Wolf, Hard Rock, Dollywood):
   → tier2_luxury — these charge premium rates for the branded experience

2. ALL-INCLUSIVE BRANDS (Sandals, Beaches, Royalton, Secrets, Dreams, Hyatt Inclusive, Majestic, Karisma):
   → tier2_luxury (Sandals, Royalton Luxury, Secrets Maroma) or tier3_upper_upscale depending on sub-brand

3. ULTRA-LUXURY / RESIDENTIAL-HOTEL BRANDS (Rosewood, Aman, Six Senses, Bulgari, Mandarin Oriental, Peninsula, Cheval Blanc):
   → tier1_ultra_luxury

4. SOFT-BRAND COLLECTIONS (Autograph Collection, Tribute Portfolio, Curio Collection, Luxury Collection, The Unbound Collection):
   → Infer from property positioning. Boutique luxury = tier2_luxury. Mid-tier = tier3_upper_upscale.

5. INDEPENDENT / BOUTIQUE HOTELS:
   Infer from the strongest signal:
   - Room rates mentioned: "$995/night peak" = tier2_luxury, "$200-400" = tier3_upper_upscale, "$100-200" = tier4_upscale
   - Positioning words: "luxury"/"five-star" = tier2_luxury, "high-design"/"boutique" = tier3_upper_upscale, "select service"/"focused service" = tier4_upscale
   - Comparable properties: "joins Manhattan's luxury scene" = tier2_luxury
   - Developer reputation: Auberge, Related, Turnberry, Blue Flag = tier2_luxury

6. CASINO / INTEGRATED RESORTS (Wynn, Venetian, Bellagio level): tier1_ultra_luxury. Regional casinos: tier3_upper_upscale.

7. IF NO EVIDENCE: OMIT brand_tier from response. Do NOT guess "tier4_upscale" as a safe default — leaving it blank is better than a wrong guess.

BRAND EXTRACTION:
- Look for the ACTUAL brand name in press releases (e.g. "Autograph Collection", "Curio Collection", "Tapestry Collection", "Hyatt Centric")
- Soft brands are often attached: "Hotel X, Autograph Collection" → brand = "Autograph Collection"
- If the hotel has no recognizable brand, use "Independent"
- Do NOT use "Unknown" as a brand value — prefer omitting the field

ROOM COUNT EXTRACTION: scan ALL snippets for patterns like
"227 rooms", "300 keys", "150-room hotel", "120 guest rooms", "34 suites and 266 rooms",
"400 hotel rooms and condo units". Extract the TOTAL room/key count as an integer.

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

3. management_company (the HOTEL OPERATOR):
   The company that OPERATES/MANAGES the hotel day-to-day.
   This is NOT the brand name. This is NOT the IP licensor. This is NOT the developer.
   It IS the company with boots on the ground running hotel operations.

   CRITICAL — DO NOT CONFUSE with these:
   ❌ Brand name (e.g. "Nickelodeon Hotels & Resorts" is a BRAND, not an operator)
   ❌ IP licensor (e.g. "Paramount" licenses Nickelodeon IP — NOT the operator)
   ❌ Property developer (e.g. "Everest Place" develops the property — NOT the operator)
   ❌ Brand parent (e.g. "Marriott" owns the Autograph brand — NOT the operator of every Autograph hotel)

   ✅ Correct examples:
   - Kali Hotel (Autograph Collection) → management_company = "Crescent Hotels & Resorts" (NOT "Marriott" and NOT "Autograph Collection")
   - Dreams Rose Hall → management_company = "Hyatt Inclusive Collection"
   - Royalton CHIC Jamaica → management_company = "Royalton Hotels & Resorts"
   - Nickelodeon Hotels & Resorts Orlando → management_company = "Lion Star Hospitality Inc." (licensee of Karisma, NOT "Nickelodeon" and NOT "Paramount")
   - Hyatt Centric Cincinnati → management_company = "Commonwealth Hotels" (NOT "Hyatt")

   Look for exact phrases like "operated by", "managed by", "hotel management company",
   "exclusive licensee for", "under management of". The operator name follows these phrases.

4. owner (the PROPERTY OWNER — the entity holding the real estate):
   The company/individual that OWNS THE PHYSICAL PROPERTY. This is the check-writer
   for construction, FF&E, and uniforms during pre-opening.

   CRITICAL — DO NOT CONFUSE with these:
   ❌ IP licensor (e.g. "Paramount" owns the Nickelodeon brand — NOT the hotel property)
   ❌ Hotel operator (e.g. "Lion Star" operates it — NOT the real estate owner)
   ❌ Brand parent (e.g. "Hyatt" owns the Centric brand — NOT the Cincinnati property)

   For joint ventures or partnerships, list ONLY the property-owning entity.
   Do NOT concatenate multiple companies with commas.

   ✅ Correct examples:
   - Kali Hotel → owner = "KPC Development Company" (NOT "Marriott" and NOT "Autograph")
   - Hyatt Centric Cincinnati → owner = "Birkla Investment Group" (NOT "Hyatt")
   - Nickelodeon Hotels & Resorts Orlando → owner = "Teramir Group" (parent of Everest Place).
     DO NOT include "Paramount" (IP licensor) or "Lion Star" (operator).
   - Secrets Macao Beach → owner = "GSM Investissements Dominicana S.R.L."

4a. developer (the entity BUILDING the property):
    The development company/project responsible for constructing the hotel.
    Often SAME as owner for new builds, but sometimes separate (e.g. hired developer).
    Use the DEVELOPMENT entity name, not the operator or IP licensor.

    ✅ Correct examples:
    - Nickelodeon Hotels & Resorts Orlando → developer = "Everest Place" (Teramir's development brand)
    - Hyatt Centric Cincinnati → developer = "Birkla Investment Group" (same as owner)
    - Kali Hotel → developer = "KPC Development Company"

    DO NOT include the operator (Lion Star, Commonwealth Hotels) in developer field.

5. former_names: JSON array of any PREVIOUS names this property had, if it was
   rebranded or converted. Empty array [] if it's a new build.
   Examples:
   - St. Regis Kapalua Bay → ["Montage Kapalua Bay", "The Residences at Kapalua Bay"]
   - Dreams Rose Hall → ["Hilton Rose Hall Resort & Spa"]
   - Kali Hotel → [] (new build)

FOUR-ROLE SEPARATION CHECKLIST (before you finalize):
For each entity mentioned in the press releases, classify it into EXACTLY ONE role:
  - BRAND (goes in 'brand' field) → e.g. "Nickelodeon Hotels & Resorts", "Autograph Collection"
  - OPERATOR (goes in 'management_company') → the company running operations
  - OWNER (goes in 'owner') → the property-holding entity
  - DEVELOPER (goes in 'developer') → the construction/development entity
  - IP LICENSOR (do NOT assign anywhere) → e.g. "Paramount" licenses Nickelodeon but owns no hotels

When a single entity plays multiple roles (e.g. owner-operated Hyatt properties),
list it in each applicable field. When entities are distinct, keep them in their
own fields. NEVER comma-separate multiple entities into a single field.

═══════════════════════════════════════════════════════════════

CRITICAL CHECKS — READ CAREFULLY:

1. ALREADY OPEN: If the hotel is currently operating (has reviews, accepting guests, open for business), include "already_opened": true and "opened_date": "Month Year".

2. HURRICANE / DISASTER CLOSURE: If a date appears in the context of "closed for repairs", "closed due to hurricane", "closed until [date] for renovation/damage", "reopening after storm" — this is NOT a new opening date. Include "already_opened": true, "project_type": "renovation", and "reopening_date": "[date]". Do NOT put this date in opening_date.

3. RENOVATION REOPENING: If the hotel closed temporarily for refurbishment and is reopening, include "already_opened": true, "project_type": "renovation". The date is a reopening, not a new build opening.

4. NEW BUILD ONLY: Only use opening_date for hotels that are genuinely under construction for the first time and have never operated before.

Keywords that mean NOT a new opening: "closed for", "repair", "hurricane", "storm damage", "renovation", "refurbishment", "reopening", "temporarily closed", "damage".

═══════════════════════════════════════════════════════════════
OUTPUT INSTRUCTIONS — READ CAREFULLY:
═══════════════════════════════════════════════════════════════

Respond ONLY with a JSON object.

FILL FIELDS GENEROUSLY when the search results mention the information, even if only
once. The search results below ARE your evidence. If a press release says
"operated by Lion Star Hospitality Inc." → populate management_company = "Lion Star
Hospitality Inc.". If it says "developed by Teramir Group" → populate owner or
developer accordingly. Do not leave fields blank when the answer is in the snippets.

Only OMIT a field if:
- The search results genuinely don't mention anything relevant to that field, AND
- You would have to make a pure guess with zero evidence

For USER-VERIFIED fields (listed above): if the search results CONFIRM the user's
value, omit the field (no change needed). If the search results CONTRADICT the user's
value with clear evidence (specific press release quote), include the corrected value.
Do not overwrite a user-verified value with a field you have no evidence for.

EXAMPLES of when to FILL a field:
- Snippet mentions "Commonwealth Hotels will manage the property" → management_company = "Commonwealth Hotels"
- Snippet mentions "owned by Birkla Investment Group" → owner = "Birkla Investment Group"
- Snippet mentions "the 170-room hotel" → room_count = 170
- Snippet mentions "luxury branded experience" + themed resort → brand_tier = "tier2_luxury"

EXAMPLES of when to OMIT a field:
- No snippet mentions the operator/manager at all → omit management_company
- Only generic positioning words with no room count → omit room_count

Example response shape:
{{"opening_date": "Q3 2026", "brand_tier": "tier2_luxury", "room_count": 170, "brand": "Hyatt Centric", "official_name": "Hyatt Centric Cincinnati", "search_name": "Hyatt Centric Cincinnati", "management_company": "Commonwealth Hotels", "owner": "Birkla Investment Group", "developer": "Birkla Investment Group", "former_names": [], "description": "Brief 1-sentence summary", "confidence": "high"}}

Confidence levels: "high" (multiple sources agree), "medium" (one source), "low" (inferred from weak signal)
"""

    # Structured output schema — forces Gemini to return ONLY valid enum values
    # for brand_tier (can't return "unknown" or misspelled values). All fields
    # are optional — Gemini omits fields it doesn't have confident answers for.
    smart_fill_schema = {
        "type": "object",
        "properties": {
            "opening_date": {"type": "string"},
            "brand_tier": {
                "type": "string",
                "enum": [
                    "tier1_ultra_luxury",
                    "tier2_luxury",
                    "tier3_upper_upscale",
                    "tier4_upscale",
                    "tier5_upper_midscale",
                    "tier6_midscale",
                    "tier7_economy",
                ],
            },
            "room_count": {"type": "integer"},
            "brand": {"type": "string"},
            "city": {"type": "string"},
            "state": {"type": "string"},
            "country": {"type": "string"},
            "description": {"type": "string"},
            "official_name": {"type": "string"},
            "search_name": {"type": "string"},
            "management_company": {"type": "string"},
            "owner": {"type": "string"},
            "developer": {"type": "string"},
            "former_names": {
                "type": "array",
                "items": {"type": "string"},
            },
            "already_opened": {"type": "boolean"},
            "opened_date": {"type": "string"},
            "reopening_date": {"type": "string"},
            "project_type": {
                "type": "string",
                "enum": [
                    "new_opening",
                    "renovation",
                    "rebrand",
                    "reopening",
                    "conversion",
                    "ownership_change",
                    "residences_only",
                ],
            },
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
            },
        },
    }
    gemini_response = await _call_gemini(prompt, response_schema=smart_fill_schema)
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

    # Diagnostic: log exactly what Gemini returned for the critical entity fields.
    # Without this, when mgmt/owner/developer fail to update we can't tell if:
    #   (a) Gemini returned nothing → prompt/search issue
    #   (b) Gemini returned garbage → cleanup filter issue
    #   (c) Gemini returned valid value → persist/writeback issue
    _entity_fields_raw = {
        k: parsed.get(k)
        for k in (
            "management_company",
            "owner",
            "developer",
            "brand",
            "brand_tier",
            "room_count",
        )
    }
    logger.info(f"Smart Fill [{hotel_name}] raw Gemini response: {_entity_fields_raw}")

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

    # Known IP licensors / brand companies that should NEVER appear as
    # management_company or owner values. Gemini occasionally conflates
    # the IP licensor (e.g. Paramount for Nickelodeon brand) with the
    # actual property owner. This blocklist catches that.
    _IP_LICENSOR_NAMES = {
        "paramount",
        "paramount global",
        "paramount studios",
        "disney",
        "walt disney",
        "warner bros",
        "warner media",
        "nbcuniversal",
        "universal",
        "nickelodeon",
        "viacom",
        "sony pictures",
        "columbia pictures",
        "fox",
        "marriott international",
        "marriott",  # brand parent — not hotel owner
        "hilton worldwide",
        "hilton",
        "hyatt hotels corporation",
        "hyatt",
        "ihg hotels & resorts",
        "ihg",
        "accor",
        "accor hotels",
    }

    def _clean_entity_field(raw: str, field_name: str, hotel_brand: str = "") -> str:
        """
        Clean up entity field (management_company, owner, developer).
        - Strips comma-concatenated lists (Gemini sometimes returns
          "Lion Star, Everest Place, Paramount" — nonsense)
        - Removes IP licensor names (Paramount, Disney, etc.)
        - Rejects the brand name itself (if Gemini confused brand with operator)
        """
        if not raw:
            return ""
        # Split on commas and take only the first non-IP-licensor entity
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        brand_lower = (hotel_brand or "").strip().lower()
        for part in parts:
            pl = part.lower().strip()
            # Reject IP licensors and brand matches
            if pl in _IP_LICENSOR_NAMES:
                continue
            if brand_lower and (pl == brand_lower or brand_lower in pl):
                # e.g. rejecting "Nickelodeon Hotels & Resorts" as mgmt_company
                # when brand IS "Nickelodeon Hotels & Resorts"
                continue
            # Accept the first clean entity we find
            return part
        return ""

    if "management_company" in parsed:
        mc_raw = str(parsed["management_company"]).strip()
        if mc_raw and mc_raw.lower() not in ("unknown", "n/a", "none", ""):
            # Use fresh brand value if updated this run, else fall back to param
            current_brand = result.get("brand") or brand or ""
            mc = _clean_entity_field(mc_raw, "management_company", current_brand)
            if mc:
                result["management_company"] = mc
                result["changes"].append("management_company")
                if mc != mc_raw:
                    logger.info(
                        f"Smart Fill cleaned management_company: "
                        f"{mc_raw!r} -> {mc!r} (stripped licensor/brand names)"
                    )
            else:
                # Cleanup rejected everything — log so we know why DB didn't update
                logger.warning(
                    f"Smart Fill REJECTED management_company value from Gemini: "
                    f"{mc_raw!r} — all entities matched IP licensor blocklist or "
                    f"brand name {current_brand!r}. DB value unchanged."
                )

    if "owner" in parsed:
        ow_raw = str(parsed["owner"]).strip()
        if ow_raw and ow_raw.lower() not in ("unknown", "n/a", "none", ""):
            current_brand = result.get("brand") or brand or ""
            ow = _clean_entity_field(ow_raw, "owner", current_brand)
            if ow:
                result["owner"] = ow
                result["changes"].append("owner")
                if ow != ow_raw:
                    logger.info(
                        f"Smart Fill cleaned owner: "
                        f"{ow_raw!r} -> {ow!r} (stripped licensor/brand names)"
                    )
            else:
                logger.warning(
                    f"Smart Fill REJECTED owner value from Gemini: "
                    f"{ow_raw!r} — all entities matched IP licensor blocklist or "
                    f"brand name {current_brand!r}. DB value unchanged."
                )

    # Developer — who's building the property (separate from operator/owner).
    # For pre-opening leads, the developer is often the best first contact
    # (Tony Birkla for Hyatt Centric Cincinnati, Zafir Rashid for Nickelodeon).
    if "developer" in parsed:
        dv_raw = str(parsed["developer"]).strip()
        if dv_raw and dv_raw.lower() not in ("unknown", "n/a", "none", ""):
            current_brand = result.get("brand") or brand or ""
            dv = _clean_entity_field(dv_raw, "developer", current_brand)
            if dv:
                result["developer"] = dv
                result["changes"].append("developer")
                if dv != dv_raw:
                    logger.info(
                        f"Smart Fill cleaned developer: "
                        f"{dv_raw!r} -> {dv!r} (stripped licensor/brand names)"
                    )
            else:
                logger.warning(
                    f"Smart Fill REJECTED developer value from Gemini: "
                    f"{dv_raw!r} — all entities matched IP licensor blocklist or "
                    f"brand name {current_brand!r}. DB value unchanged."
                )

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
