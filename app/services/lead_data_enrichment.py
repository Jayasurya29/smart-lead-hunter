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
import asyncio as _asyncio

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
        async with httpx.AsyncClient(timeout=75) as client:
            resp = await client.post(
                url,
                headers=headers,
                json={
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": generation_config,
                },
            )
            if resp.status_code == 429:
                logger.warning("Gemini 429 rate limit — retrying in 8s...")
                await _asyncio.sleep(8)
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
        logger.warning(f"Gemini call failed [{type(e).__name__}]: {e}", exc_info=True)
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
    search_name: str = "",
) -> Dict:
    """
    REDESIGNED 2026-04-23 — two-stage enrichment:
      1. CLASSIFY: one small focused call determines project_type
         (new_opening, renovation, rebrand, reopening, conversion,
         ownership_change) from the hotel name + a quick search.
      2. EXTRACT: branched queries + branched prompt tailored to the
         project type, pulling the RIGHT dates and entities.

    Why this works better than one-shot:
      - Old approach: one big prompt tried to handle all cases, got confused
        between "original opening date" vs "reopening date" vs "rebrand date"
      - New approach: classifier decides the type FIRST, then extraction
        runs with context-specific instructions

    Returns dict with any fields found:
    {
        "project_type": "renovation",
        "opening_date": "December 18, 2026",   # reopening date for renovations
        "reopening_date": "December 18, 2026", # same, flagged for route handler
        "brand_tier": "tier2_luxury",
        "management_company": "Sandals Resorts International",
        "owner": "...",
        "developer": "...",
        "room_count": 272,
        "brand": "Sandals",
        "already_opened": True,  # hotel was open pre-closure
        "confidence": "high",
        "changes": [...],
        "source_url": "..."
    }
    """
    result: Dict = {
        "changes": [],
        "confidence": "low",
        "source_url": None,
    }
    location = ", ".join(filter(None, [city, state]))

    # ── STAGE 1: PROJECT TYPE CLASSIFICATION ──
    # One fast Gemini call using 2-3 targeted snippets to decide:
    #   new_opening | renovation | rebrand | reopening | conversion | ownership_change
    # This drives the subsequent query strategy and extraction prompt.
    project_type = await _classify_project_type(
        hotel_name=hotel_name,
        location=location,
        current_opening_date=current_opening_date,
    )
    if project_type:
        result["project_type"] = project_type
        result["changes"].append("project_type")
        logger.info(
            f"Smart Fill [{hotel_name}] classified as project_type={project_type!r}"
        )
    else:
        # If classifier fails, default to new_opening — most leads in SLH
        # are genuine greenfield builds, so this is the conservative guess.
        project_type = "new_opening"
        logger.info(
            f"Smart Fill [{hotel_name}] project_type unclassified — "
            f"defaulting to new_opening"
        )

    # ── STAGE 2: BRANCHED QUERY BUILDING ──
    # Different project types need different searches. A renovation
    # reopening needs "$200 million renovation reopen 2026" queries, not
    # "hotel opening 2025 2026 2027" queries that return stale 1981 info.
    queries, missing = _build_queries_for_project_type(
        hotel_name=hotel_name,
        location=location,
        project_type=project_type,
        mode=mode,
        current_city=city,
        current_state=state,
        current_opening_date=current_opening_date,
        current_brand_tier=current_brand_tier,
        current_room_count=current_room_count,
        current_brand=brand,
        search_name=search_name,
    )
    if mode == "smart" and not missing:
        # Nothing to fetch in smart mode — all fields already populated
        return result

    # ── STAGE 3: SEARCH ──
    all_snippets: list[str] = []
    all_urls: list[str] = []
    for q in queries[:5]:  # cap at 5 queries to manage Serper quota
        results = await _search_web(q, max_results=6)
        for r in results:
            snippet = f"{r['title']}. {r['snippet']}"
            all_snippets.append(snippet)
            if r["url"]:
                all_urls.append(r["url"])

    if not all_snippets:
        logger.info(f"Smart Fill [{hotel_name}]: no search results")
        return result

    logger.info(
        f"Smart Fill [{hotel_name}] fed Gemini {len(all_snippets)} snippets "
        f"(project_type={project_type}, queries={len(queries)})"
    )

    # ── STAGE 4: SPLIT EXTRACTION (data fields + entities sequential) ──
    import asyncio as _asyncio

    data_extraction = await _extract_fields_for_project_type(
        hotel_name=hotel_name,
        location=location,
        project_type=project_type,
        snippets=all_snippets[:15],
        missing=[
            f for f in missing if f not in ("management_company", "owner", "developer")
        ],
        mode=mode,
        current_brand=brand,
        current_opening_date=current_opening_date,
        current_brand_tier=current_brand_tier,
        current_room_count=current_room_count,
    )
    await _asyncio.sleep(2)
    entity_extraction = await _extract_entities(
        hotel_name=hotel_name,
        location=location,
        snippets=all_snippets[:15],
    )
    # Merge entity results into data extraction
    extraction = data_extraction or {}
    if entity_extraction:
        _ENTITY_BLOCKLIST_KEYWORDS = {
            "water and sewerage",
            "wasa",
            "government",
            "ministry",
            "municipality",
            "sewage",
            "utility",
            "utilities",
            "alg vacations corp",
            "apple leisure group",
            "paramount",
            "disney",
            "nbcuniversal",
            "viacom",
            "warner bros",
        }
        for field in ("management_company", "owner", "developer"):
            val = entity_extraction.get(field)
            if not val:
                continue
            val_lower = val.lower().strip()
            blocked = any(kw in val_lower for kw in _ENTITY_BLOCKLIST_KEYWORDS)
            if not blocked:
                extraction[field] = val
    if not extraction:
        logger.warning(f"Smart Fill [{hotel_name}]: extraction returned nothing")
        return result

    logger.info(f"Smart Fill [{hotel_name}] raw Gemini: {extraction}")

    # ── STAGE 5: MAP extraction to result dict ──
    _map_extraction_to_result(
        extraction=extraction,
        result=result,
        project_type=project_type,
        mode=mode,
        current_brand=brand,
        current_opening_date=current_opening_date,
        current_brand_tier=current_brand_tier,
        current_room_count=current_room_count,
    )

    result["confidence"] = extraction.get("confidence", "medium")
    result["source_url"] = all_urls[0] if all_urls else None

    if result["changes"]:
        logger.info(
            f"Smart Fill ({mode}): {hotel_name} -> {', '.join(result['changes'])} "
            f"(confidence: {result['confidence']}, project_type={project_type})"
        )

    return result


async def _classify_project_type(
    hotel_name: str,
    location: str,
    current_opening_date: str = "",
) -> Optional[str]:
    """
    Stage 1: Quick classification call.

    Runs a single focused Gemini call on 2-3 generic search results to decide
    which project type we're dealing with. Cheap (small prompt, small response)
    and high-value (drives every downstream decision).

    Returns one of: new_opening, renovation, rebrand, reopening, conversion,
    ownership_change, or None if classification fails.
    """
    # One general query for initial context
    query = f'"{hotel_name}" {location} hotel news 2025 2026'
    results = await _search_web(query, max_results=4)
    if not results:
        return None

    # Compact snippet block — we don't need 25 snippets to classify
    snippet_lines = [f"- {r['title']}. {r['snippet'][:200]}" for r in results[:4]]
    snippets_text = "\n".join(snippet_lines)

    prompt = f"""Classify this hotel project by reading the search snippets.

HOTEL: {hotel_name}
LOCATION: {location}
KNOWN OPENING/REOPENING DATE: {current_opening_date or "(unknown)"}

SNIPPETS:
{snippets_text}

Classify the project as ONE of:
  - "new_opening"       = greenfield; property never existed before. Look for: "slated to open", "will open", "construction", "new-build"
  - "renovation"        = existing hotel, closed for refurbishment, reopening. Look for: "closed for renovation", "$X million renovation", "reopen after"
  - "rebrand"           = existing hotel, changing brand affiliation. Look for: "formerly Hilton", "converts to", "rebranded as", "now operating as"
  - "reopening"         = reopening after hurricane / seasonal closure / other. Look for: "reopens after hurricane", "post-closure", "resumes operations"
  - "conversion"        = changing hotel type (e.g. residences → hotel). Look for: "converted from", "transformed into"
  - "ownership_change"  = sold to new owner, same brand/operator. Look for: "acquired by", "new owner", "sold to"

Return JSON only:
{{"project_type": "<one of above>", "confidence": "high|medium|low", "evidence": "one-sentence quote from snippet"}}

If snippets don't clearly indicate, use your best inference. Default to "new_opening" if truly ambiguous.
"""

    classify_schema = {
        "type": "object",
        "properties": {
            "project_type": {
                "type": "string",
                "enum": [
                    "new_opening",
                    "renovation",
                    "rebrand",
                    "reopening",
                    "conversion",
                    "ownership_change",
                ],
            },
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
            },
            "evidence": {"type": "string"},
        },
        "required": ["project_type"],
    }
    try:
        resp = await _call_gemini(
            prompt, temperature=0.1, response_schema=classify_schema
        )
        if not resp:
            return None
        parsed = json.loads(resp)
        pt = parsed.get("project_type")
        if pt:
            logger.debug(
                f"Classifier for {hotel_name}: {pt} "
                f"(conf={parsed.get('confidence')}, ev={parsed.get('evidence','')[:80]!r})"
            )
        return pt
    except Exception as ex:
        logger.debug(f"Classifier failed for {hotel_name}: {ex}")
        return None


def _build_queries_for_project_type(
    hotel_name: str,
    location: str,
    project_type: str,
    mode: str,
    current_city: str,
    current_state: str,
    current_opening_date: str,
    current_brand_tier: str,
    current_room_count: int,
    current_brand: str,
    search_name: str = "",
) -> tuple[list[str], list[str]]:
    """
    Stage 2: Branched query building.

    Returns (queries, missing_fields). Each project type gets queries
    tuned to the kind of evidence that actually exists for it.

    For renovations: we want press about the renovation (re-open dates,
    $$$ scope, operator continuation). For new openings: we want
    developer/owner press + room count + opening announcements.
    For rebrands: we want flag-change press + old/new brand names.
    """
    hn = hotel_name
    hn = search_name.strip() if search_name and search_name.strip() else hotel_name
    loc = location

    # Determine what's missing (smart mode only hits missing fields)
    missing: list[str] = []
    if mode == "full":
        missing = [
            "opening_date",
            "brand_tier",
            "room_count",
            "brand",
            "management_company",
            "owner",
            "developer",
            "description",
        ]
    else:
        if not current_city:
            missing.append("city")
        if not current_state:
            missing.append("state")
        if not current_opening_date:
            missing.append("opening_date")
        if not current_room_count:
            missing.append("room_count")
        if not current_brand_tier or current_brand_tier in ("unknown", ""):
            missing.append("brand_tier")
        if not current_brand:
            missing.append("brand")
        # These are almost always worth re-fetching in both modes since the
        # pipeline downstream depends on them heavily.
        missing.append("management_company")
        missing.append("owner")
        missing.append("developer")

    # ── Branch on project type ──
    if project_type == "renovation":
        queries = [
            # Authoritative renovation announcement
            f'"{hn}" {loc} renovation reopen 2026 (site:prnewswire.com OR site:businesswire.com OR site:hospitalitynet.org)',
            # Scope and dollar figures
            f'"{hn}" "$" million renovation refurbishment',
            # Operator continuation during reno
            f'"{hn}" {loc} "operated by" OR "managed by" OR "management"',
            # Owner/investment entity behind the refresh
            f'"{hn}" owner OR "ownership group" OR "investment"',
            # Reopening-specific date search
            f'"{hn}" reopening date November December 2026',
        ]
    elif project_type == "rebrand":
        queries = [
            f'"{hn}" {loc} rebrand OR converted OR "formerly" OR "now operating as"',
            f'"{hn}" "will become" OR "will convert" OR "changes brand"',
            f'"{hn}" {loc} "operated by" OR "managed by"',
            f'"{hn}" owner OR "investment group"',
            f'"{hn}" {loc} announcement 2025 2026',
        ]
    elif project_type == "reopening":
        queries = [
            f'"{hn}" {loc} reopen hurricane damage recovery',
            f'"{hn}" reopening date 2025 2026',
            f'"{hn}" {loc} "operated by" OR "managed by"',
            f'"{hn}" {loc}',
        ]
    elif project_type == "conversion":
        queries = [
            f'"{hn}" {loc} conversion converted transformation',
            f'"{hn}" "becomes" OR "transformed into"',
            f'"{hn}" {loc} "operated by" OR "managed by"',
            f'"{hn}" owner OR developer',
        ]
    elif project_type == "ownership_change":
        queries = [
            f'"{hn}" {loc} acquired sold new owner',
            f'"{hn}" "acquired by" OR "purchased by" OR "new ownership"',
            f'"{hn}" {loc} "operated by"',
        ]
    else:  # new_opening (default)
        queries = [
            # Opening date — official source first
            f'"{hn}" opening date',
            # Developer / owner hunt — no location restriction
            f'"{hn}" owner OR developer OR "owned by" OR "developed by"',
            # Operator (may differ from brand for soft brands)
            f'"{hn}" {loc} "managed by" OR "operated by" OR "management company"',
            # Authoritative announcement
            f'"{hn}" {loc} opening (site:prnewswire.com OR site:businesswire.com OR site:hospitalitynet.org)',
            # Tier positioning + rooms
            f'"{hn}" {loc} "rooms" OR "keys" OR "suites" luxury boutique upscale',
        ]

    return queries, missing


async def _extract_fields_for_project_type(
    hotel_name: str,
    location: str,
    project_type: str,
    snippets: list[str],
    missing: list[str],
    mode: str,
    current_brand: str,
    current_opening_date: str,
    current_brand_tier: str,
    current_room_count: int,
) -> Optional[dict]:
    """
    Stage 4: Branched extraction.

    Sends Gemini a project-type-specific prompt. Each branch explains
    WHICH dates and entities matter for that project type — so Gemini
    extracts the right things.

    Uses structured output (enum-locked brand_tier) + response_schema
    so the return is clean JSON.
    """
    snippets_text = "\n".join(f"- {s}" for s in snippets)

    # ── Per-type guidance block ──
    # Tells Gemini EXACTLY what to look for based on classification.
    if project_type == "renovation":
        type_guidance = """
PROJECT TYPE: RENOVATION / REFURBISHMENT
This hotel EXISTS and is CLOSED for renovation. You need:
  - opening_date: the FUTURE REOPENING date (e.g. "December 18, 2026"), NOT the original opening year
  - reopening_date: same as opening_date — explicit flag for renovation cases
  - already_opened: true  (hotel was open before closure; reopening is the relevant date)
  - management_company: the CURRENT operator (usually same as before closure)
  - owner: the CURRENT property owner
  - developer: typically empty for pure renovations (unless it's a major expansion)

IMPORTANT: Do NOT return the original 1970s/1980s/1990s opening year as opening_date.
The search snippets will likely mention the old date — IGNORE it for opening_date.
The RELEVANT date is the reopening after the current renovation."""
    elif project_type == "rebrand":
        type_guidance = """
PROJECT TYPE: REBRAND
This existing hotel is switching brand affiliation. You need:
  - opening_date: the date of the brand change / reflag
  - brand: the NEW brand (e.g. "Autograph Collection" not the old brand)
  - former_names: the OLD name(s) before rebrand
  - management_company: the CURRENT or INCOMING operator
  - owner: the property owner (usually unchanged during a rebrand)
  - already_opened: true  (property was open under old brand)
"""
    elif project_type == "reopening":
        type_guidance = """
PROJECT TYPE: REOPENING (post-closure, hurricane, seasonal)
This hotel closed due to an event and is reopening. You need:
  - opening_date: the reopening date (future)
  - already_opened: true
  - management_company: the operator (usually same as pre-closure)
  - owner: the owner
"""
    elif project_type == "conversion":
        type_guidance = """
PROJECT TYPE: CONVERSION (changing hotel type, e.g. residences→hotel)
  - opening_date: when it opens AS A HOTEL
  - brand: new brand after conversion
  - management_company: incoming operator
  - owner: property owner
"""
    elif project_type == "ownership_change":
        type_guidance = """
PROJECT TYPE: OWNERSHIP CHANGE (sold to new owner, brand/operator may stay)
  - opening_date: typically not relevant (hotel stayed open)
  - already_opened: true
  - management_company: usually unchanged unless sale includes operator change
  - owner: the NEW owner (the acquiring entity)
"""
    else:  # new_opening
        type_guidance = """
PROJECT TYPE: NEW OPENING (greenfield, first-ever opening)
This is a brand-new hotel being built. You need:
  - opening_date: the FIRST EVER opening date (e.g. "Q3 2026", "December 2026")
  - already_opened: false  (hotel doesn't exist yet)
  - management_company: the operator signed to run the hotel
  - owner: the REAL ESTATE owner (often the developer, may be separate)
  - developer: the entity building the property
  - room_count: the total planned room count

CRITICAL for pre-opening: the developer/owner is often the single most
valuable contact (signs vendor contracts before operator takes over).
Look for "developed by", "owner", "investment group", "ownership group".
"""

    # ── User-verified fields (SMART MODE ONLY) ──
    # In smart mode: user manually set values that we should preserve unless
    # snippets clearly contradict. In full mode: user explicitly asked to
    # re-verify everything, so we pass NO preservation hints — let Gemini
    # return its best understanding of all fields.
    #
    # Without this mode guard, passing "brand_tier = tier4_upscale" to Gemini
    # causes it to conservatively omit every field adjacent to the user-set
    # one (brand, brand_tier, mgmt_company, owner) even when snippets have
    # clear evidence. Observed on Sandals Montego Bay where full refresh
    # returned only 4/9 fields instead of 9/9 when user_edits_block was shown.
    user_verified_block = ""
    user_verified: list[str] = []
    _SENTINELS = {"", "unknown", "none", "n/a", "tbd"}
    if current_brand_tier and current_brand_tier.strip().lower() not in _SENTINELS:
        user_verified.append(f"brand_tier = {current_brand_tier}")
    if current_brand and current_brand.strip().lower() not in _SENTINELS:
        user_verified.append(f"brand = {current_brand}")
    if current_room_count and current_room_count > 0:
        user_verified.append(f"room_count = {current_room_count}")
    if user_verified:
        user_verified_block = (
            "\nCURRENT VALUES IN DB (use as context; update if snippets have better evidence):\n  "
            + "; ".join(user_verified)
            + "\n"
        )
    # ── Compact role definitions ──
    role_defs = """
ROLE DEFINITIONS:
  brand            = flag/name displayed (e.g. "Sandals", "Hyatt Centric", "Nickelodeon Hotels & Resorts")
  management_company = day-to-day operator (e.g. "Sandals Resorts International", "Commonwealth Hotels", "Lion Star Hospitality Inc.")
  owner            = real-estate holder (e.g. "Birkla Investment Group", "Teramir Group"). For owner-operated chains (Sandals, many Hyatts), same as operator.
  developer        = entity building/developing (often same as owner for new builds)
  IGNORE: IP licensors like Paramount/Disney/Viacom that license brand names but don't run hotels
  IGNORE: Corporate parents/holding companies (ALG Vacations Corp, Apple Leisure Group, Marriott International HQ)
          Use the OPERATING brand instead (Royalton Luxury Resorts, not ALG Vacations Corp)
"""

    # ── Tier rules (compact) ──
    tier_rules = """
TIER RULES:
  tier1_ultra_luxury: Ritz-Carlton, Four Seasons, Aman, Rosewood, Faena, St. Regis, Pendry, Auberge
  tier2_luxury: Sandals, Royalton, Nickelodeon, Margaritaville, themed/experiential resorts, JW Marriott, Conrad, Kimpton
  tier3_upper_upscale: Marriott, Hilton, Westin, Sheraton, Hyatt Regency, Hyatt Centric
  tier4_upscale: Courtyard, Hilton Garden Inn, Hyatt Place, AC Hotels
  tier5_upper_midscale: Hampton Inn, Holiday Inn Express
  tier6_midscale: La Quinta, Wingate
  tier7_economy: Motel 6, Days Inn
"""

    # Location rules — Caribbean leads often confuse Gemini (city vs parish vs country)
    location_rules = """
LOCATION RULES (critical for Caribbean):
  - state = the administrative division (Florida, California, St. Ann Parish, Westmoreland)
    NOT the city. "Montego Bay" is a CITY in Jamaica, not a state.
  - For Caribbean islands: country = island name (Jamaica, Turks and Caicos, Barbados, etc.)
    state = parish or administrative region (St. Ann, St. James, Westmoreland, etc.)
  - For Puerto Rico / US Virgin Islands: country = "USA", state = territory name
  - For mainland USA: country = "USA", state = full state name (Florida, not FL)
  - City = the specific municipality (Montego Bay, Kissimmee, San Juan)

Examples:
  - Sandals Montego Bay → city: "Montego Bay", state: "St. James", country: "Jamaica"
  - Hard Rock San Juan → city: "San Juan", state: "Puerto Rico", country: "USA"
  - Hyatt Centric Cincinnati → city: "Cincinnati", state: "Ohio", country: "USA"
  - Royalton CHIC Montego Bay → city: "Montego Bay", state: "St. James", country: "Jamaica"
  - Royalton Vessence Barbados → city: "Holetown", state: "Saint James", country: "Barbados"
  - Sandals Royal Barbados → city: "Hastings", state: "Christ Church", country: "Barbados"
"""

    data_fields = [
        f for f in missing if f not in ("management_company", "owner", "developer")
    ]

    prompt = f"""You are extracting facts about a specific hotel from search snippets.

HOTEL: {hotel_name}
LOCATION: {location}
{type_guidance}
{user_verified_block}{role_defs}
{tier_rules}
{location_rules}

STRUCTURED FIELDS TO EXTRACT: {', '.join(data_fields) if data_fields else 'none'}

ENTITY FIELDS (scan every snippet for company names — these are mandatory):
  management_company: who operates/manages this hotel day-to-day
  owner: who owns the real estate
  developer: who is building/developing the property
SEARCH SNIPPETS ({len(snippets)} results):
{snippets_text}

INSTRUCTIONS:
- Read all snippets carefully
- Fill fields when snippets provide evidence (even from one snippet)
- For enums (brand_tier, project_type), use ONE of the valid values or omit
- For comma-concatenated entities (e.g. "Lion Star, Paramount, Teramir"), split
  them and put EACH in its correct role field — do not return the whole list
- For owner-operated chains (Sandals, many Hyatts), same company can be both
  management_company AND owner — that's fine, list it in both
- If snippets describe a joint venture, list ONLY the property-owning entity
  in owner (ignore IP licensors like Paramount for Nickelodeon)
- Omit fields you truly cannot infer from snippets

Return JSON:
"""

    schema = {
        "type": "object",
        "properties": {
            "opening_date": {"type": "string"},
            "reopening_date": {"type": "string"},
            "opened_date": {"type": "string"},
            "already_opened": {"type": "boolean"},
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
            "official_name": {"type": "string"},
            "search_name": {"type": "string"},
            "management_company": {"type": "string"},
            "owner": {"type": "string"},
            "developer": {"type": "string"},
            "city": {"type": "string"},
            "state": {"type": "string"},
            "country": {"type": "string"},
            "description": {"type": "string"},
            "former_names": {"type": "array", "items": {"type": "string"}},
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
            },
        },
    }

    try:
        resp = await _call_gemini(prompt, temperature=0.1, response_schema=schema)
        if not resp:
            return None
        parsed = json.loads(resp)
        return parsed if isinstance(parsed, dict) else None
    except Exception as ex:
        logger.warning(f"Extraction failed for {hotel_name}: {ex}")
        return None


async def _extract_entities(
    hotel_name: str,
    location: str,
    snippets: list[str],
) -> Optional[dict]:
    """
    Focused entity extraction — management_company, owner, developer only.
    Runs in parallel with _extract_fields_for_project_type so entities
    never compete with date/tier/rooms for Gemini's attention.
    """
    snippets_text = "\n".join(f"- {s}" for s in snippets)

    prompt = f"""You are identifying the operator, owner, and developer of a hotel from search snippets.

HOTEL: {hotel_name}
LOCATION: {location}

ROLE DEFINITIONS:
  management_company = day-to-day hotel operator (e.g. "Lion Star Hospitality Inc.", "Commonwealth Hotels")
  owner              = real estate holder (e.g. "Teramir Group", "Birkla Investment Group")
  developer          = entity building the property (e.g. "Everest Place", often same as owner)

RULES:
  - IGNORE IP licensors: Paramount, Disney, Viacom, NBCUniversal — they license brand names, NOT hotel owners
  - IGNORE corporate holding parents: ALG Vacations Corp, Apple Leisure Group — use the operating brand instead
  - IGNORE government utilities, water authorities, sewage authorities, municipal services
  - IGNORE entities only mentioned in legal disputes, complaints, or infrastructure context
  - For comma-separated lists — assign each to the correct role
  - Owner-operated chains (Sandals, Blue Diamond, Hyatt): same entity can be both management_company AND owner
  - owner = the hotel real estate owner/investor, NOT a utility or government body

SEARCH SNIPPETS:
{snippets_text}

Return JSON with only the fields you can find evidence for:
"""

    schema = {
        "type": "object",
        "properties": {
            "management_company": {"type": "string"},
            "owner": {"type": "string"},
            "developer": {"type": "string"},
        },
    }

    try:
        resp = await _call_gemini(prompt, temperature=0.1, response_schema=schema)
        if not resp:
            return None
        parsed = json.loads(resp)
        return parsed if isinstance(parsed, dict) else None
    except Exception as ex:
        logger.warning(f"Entity extraction failed for {hotel_name}: {ex}")
        return None


def _map_extraction_to_result(
    extraction: dict,
    result: dict,
    project_type: str,
    mode: str,
    current_brand: str,
    current_opening_date: str,
    current_brand_tier: str,
    current_room_count: int,
) -> None:
    """
    Stage 5: Copy extracted fields into the result dict.

    Handles:
      - Entity cleanup (strip comma-concat, remove IP licensor noise)
      - Project-type-aware date handling (reopening vs opening)
      - User-edit protection (don't overwrite user's valid manual value
        with a guess)
      - Brand/operator deduplication (exact-match only, so
        "Sandals Resorts International" passes for brand="Sandals")

    Mutates `result` in place. Appends field names to result['changes'].
    """
    # IP licensors that should never appear as mgmt/owner/developer
    IP_LICENSORS = {
        "paramount",
        "paramount global",
        "paramount studios",
        "disney",
        "walt disney",
        "warner bros",
        "warner media",
        "nbcuniversal",
        "comcast",
        "sony pictures",
        "viacom",
        "viacomcbs",
        "nickelodeon",
        "alg vacations corp",
        "apple leisure group",
        # Utilities, government bodies, municipal services
        "water and sewerage authority",
        "wasa",
        "government",
        "ministry",
        "municipality",
        "authority",
        "sewage",
        "utility",
        "utilities",
    }

    def _clean_entity(raw, hotel_brand):
        """Clean up entity: split commas, drop licensors, reject exact brand."""
        if not raw or not isinstance(raw, str):
            return ""
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        brand_l = (hotel_brand or "").strip().lower()
        for part in parts:
            pl = part.lower().strip()
            if pl in IP_LICENSORS:
                continue
            if (
                brand_l
                and pl == brand_l
                and not any(
                    chain in brand_l
                    for chain in (
                        "six senses",
                        "sandals",
                        "beaches",
                        "aman",
                        "four seasons",
                        "rosewood",
                        "auberge",
                        "hyatt",
                        "marriott",
                        "hilton",
                    )
                )
            ):
                continue
            return part
        return ""

    # ── Post-extraction reclassification ──
    # The classifier runs on 2-3 generic snippets and can fail (rate limits,
    # ambiguous early signal). The extraction runs on 15 focused snippets
    # and often has clearer evidence. If extraction reveals this is a
    # reopening (already_opened=True + future reopening_date), override the
    # classifier's guess REGARDLESS of what it originally said.
    #
    # Real case caught: Sandals Montego Bay — classifier 429-errored,
    # defaulted to new_opening. Extraction returned already_opened=True,
    # reopening_date='December 2026'. Without this override, the code would
    # expire the lead using opened_date=1981 (the 1981 first-opening year).
    REOPENING_TYPES = {
        "renovation",
        "rebrand",
        "reopening",
        "conversion",
        "ownership_change",
    }
    reopening_signal = (extraction.get("reopening_date") or "").strip()
    already_opened_signal = bool(extraction.get("already_opened"))

    # If classifier said new_opening BUT extraction shows reopening evidence,
    # upgrade to "renovation" (the safest default reopening type).
    if (
        project_type not in REOPENING_TYPES
        and reopening_signal
        and already_opened_signal
    ):
        logger.info(
            f"Smart Fill: extraction evidence overrides classifier. "
            f"Was project_type={project_type!r} but extraction returned "
            f"already_opened=True + reopening_date={reopening_signal!r} → "
            f"reclassifying as 'renovation'"
        )
        project_type = "renovation"
        result["project_type"] = "renovation"
        if "project_type" not in result["changes"]:
            result["changes"].append("project_type")

    # ── opening_date / reopening_date handling (project-type aware) ──
    # For reopening-type projects, prefer reopening_date if available
    if project_type in REOPENING_TYPES:
        reopening = (extraction.get("reopening_date") or "").strip()
        opening = (extraction.get("opening_date") or "").strip()
        effective_date = reopening or opening
        if effective_date:
            result["opening_date"] = effective_date
            result["reopening_date"] = effective_date
            result["timeline_label"] = get_timeline_label(effective_date)
            if "opening_date" not in result["changes"]:
                result["changes"].append("opening_date")
            if "reopening_date" not in result["changes"]:
                result["changes"].append("reopening_date")
    else:
        # new_opening: straight opening_date
        opening = (extraction.get("opening_date") or "").strip()
        if opening and opening.lower() not in ("unknown", "tbd", "n/a"):
            if mode == "full" or not current_opening_date:
                result["opening_date"] = opening
                result["timeline_label"] = get_timeline_label(opening)
                if "opening_date" not in result["changes"]:
                    result["changes"].append("opening_date")

    # ── already_opened flag ──
    # For reopening types, this flag is not actionable for the UI route
    # handler — we've already routed via is_live_reopening above. Only set
    # the flag when it's a genuinely-opened lead (not a reopening).
    if project_type not in REOPENING_TYPES:
        if extraction.get("already_opened") is not None:
            result["already_opened"] = bool(extraction["already_opened"])
    # else: skip setting already_opened so scraping.py doesn't hit
    # the expire-lead branch. reopening_date above handles routing.
    opened_date = (extraction.get("opened_date") or "").strip()
    if opened_date:
        result["opened_date"] = opened_date

    # ── brand_tier (enum-forced) ──
    # Full mode: Gemini's answer wins (this is an intentional refresh)
    # Smart mode: only fill if current is empty/unknown (don't touch user values)
    VALID_TIERS = {
        "tier1_ultra_luxury",
        "tier2_luxury",
        "tier3_upper_upscale",
        "tier4_upscale",
        "tier5_upper_midscale",
        "tier6_midscale",
        "tier7_economy",
    }
    INVALID_TIER_SENTINELS = {"", "unknown", "none", "n/a", "tbd"}
    new_tier = (extraction.get("brand_tier") or "").strip().lower()
    current_tier_l = (current_brand_tier or "").strip().lower()
    if new_tier in VALID_TIERS:
        if mode == "full":
            # Full Refresh = trust Gemini's latest classification
            result["brand_tier"] = extraction["brand_tier"]
            if "brand_tier" not in result["changes"]:
                result["changes"].append("brand_tier")
        elif current_tier_l in INVALID_TIER_SENTINELS:
            # Smart mode: only fill if current is empty/unknown
            result["brand_tier"] = extraction["brand_tier"]
            if "brand_tier" not in result["changes"]:
                result["changes"].append("brand_tier")
        # else: smart mode + current is valid → preserve existing

    # ── room_count ──
    try:
        new_rc = int(extraction.get("room_count") or 0)
    except (TypeError, ValueError):
        new_rc = 0
    if new_rc > 0:
        if mode == "full" or not current_room_count or current_room_count <= 0:
            result["room_count"] = new_rc
            if "room_count" not in result["changes"]:
                result["changes"].append("room_count")

    # ── brand ──
    new_brand = (extraction.get("brand") or "").strip()
    if new_brand and new_brand.lower() not in ("unknown", "n/a", "none"):
        if mode == "full" or not current_brand:
            result["brand"] = new_brand
            if "brand" not in result["changes"]:
                result["changes"].append("brand")

    # ── city / state / country ──
    for loc_field in ("city", "state", "country"):
        val = (extraction.get(loc_field) or "").strip()
        if val and val.lower() not in ("unknown", "n/a", ""):
            result[loc_field] = val
            if loc_field not in result["changes"]:
                result["changes"].append(loc_field)

    # ── management_company / owner / developer (cleaned) ──
    # These use _clean_entity to handle comma-concat and IP licensors.
    # The brand value we pass in is whatever Gemini returned (or user had).
    cleaning_brand = result.get("brand") or current_brand or ""
    for entity_field in ("management_company", "owner", "developer"):
        raw = extraction.get(entity_field)
        if not raw:
            continue
        cleaned = _clean_entity(raw, cleaning_brand)
        if cleaned:
            result[entity_field] = cleaned
            if entity_field not in result["changes"]:
                result["changes"].append(entity_field)
        else:
            logger.warning(
                f"Smart Fill cleaned {entity_field} to empty: "
                f"raw={raw!r} brand={cleaning_brand!r} (all parts were "
                f"licensors or exact brand match)"
            )

    # ── description ──
    desc = (extraction.get("description") or "").strip()
    if desc:
        result["description"] = desc[:500]
        if "description" not in result["changes"]:
            result["changes"].append("description")

    # ── official_name / search_name / former_names ──
    for name_field in ("official_name", "search_name"):
        val = (extraction.get(name_field) or "").strip()
        if val:
            result[name_field] = val
            if name_field not in result["changes"]:
                result["changes"].append(name_field)
    fn = extraction.get("former_names")
    if isinstance(fn, list) and fn:
        result["former_names"] = fn
        result["changes"].append("former_names")


# ═══════════════════════════════════════════════════════════════
# LEGACY / DEPRECATED: _enrich_lead_data_legacy  (kept for reference)
# The new enrich_lead_data above is the active implementation.
# ═══════════════════════════════════════════════════════════════


async def _enrich_lead_data_legacy(
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

    # Build Gemini prompt. Use up to 15 snippets — reduced from 25 to keep
    # total prompt size small enough that Gemini responds within 30-60s.
    # 15 snippets × ~300 chars = ~4,500 chars of search context, plenty
    # for tier inference and entity extraction without choking the request.
    snippets_text = "\n".join(f"- {s}" for s in all_snippets[:15])

    # Diagnostic: log snippet count + sample so we can see what context Gemini got.
    # If Gemini later returns None for operator/owner despite the hotel having clear
    # press coverage, this tells us whether the problem is search (no snippets)
    # or prompt (snippets present but Gemini ignored them).
    logger.info(
        f"Smart Fill [{hotel_name}] search fed Gemini {len(all_snippets)} snippets "
        f"(using top 15). First snippet: {all_snippets[0][:200] if all_snippets else 'NONE'!r}"
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

3. management_company = HOTEL OPERATOR (runs day-to-day operations)
   Look for: "operated by X", "managed by X", "X Hospitality"
   ❌ NOT the brand ("Nickelodeon Hotels & Resorts" is brand, not operator)
   ❌ NOT the IP licensor (Paramount licenses Nickelodeon — not operator)
   ❌ NOT the brand parent (Marriott ≠ operator of every Autograph hotel)
   ✅ Examples:
   - Kali Hotel (Autograph) → "Crescent Hotels & Resorts"
   - Nickelodeon Hotels & Resorts Orlando → "Lion Star Hospitality Inc."
   - Hyatt Centric Cincinnati → "Commonwealth Hotels"

4. owner = PROPERTY OWNER (holds the real estate, check-writer for FF&E/uniforms)
   For JVs, list ONLY the property-holding entity. Do NOT comma-separate.
   ❌ NOT the IP licensor (Paramount owns Nickelodeon brand, NOT the hotel)
   ❌ NOT the operator
   ✅ Examples:
   - Hyatt Centric Cincinnati → "Birkla Investment Group"
   - Nickelodeon Orlando → "Teramir Group" (NOT Paramount, NOT Lion Star)
   - Kali Hotel → "KPC Development Company"

4a. developer = the BUILDING entity (often same as owner for new builds)
    ✅ Examples:
    - Nickelodeon Orlando → "Everest Place" (or Teramir Group)
    - Hyatt Centric Cincinnati → "Birkla Investment Group"
    DO NOT include operator (Lion Star, Commonwealth) in developer field.

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

        # If it's a renovation / rebrand / reopening, capture the reopening
        # date and project type. Surfaces both `opening_date` (for timeline
        # calc) AND `reopening_date` (for scraping.py's is_live_reopening
        # check that decides whether to expire or keep the lead live).
        _REOPENING_PROJECT_TYPES = {
            "renovation",
            "rebrand",
            "reopening",
            "conversion",
            "ownership_change",
        }
        parsed_proj_type = (parsed.get("project_type") or "").strip().lower()
        if parsed_proj_type in _REOPENING_PROJECT_TYPES:
            result["project_type"] = parsed_proj_type
            if "project_type" not in result["changes"]:
                result["changes"].append("project_type")
            # Use reopening_date as the opening_date so the lead shows
            # the correct date but is classified as a reopening.
            reopening = parsed.get("reopening_date", "").strip()
            if reopening:
                # CRITICAL: expose reopening_date on result so the route
                # handler (scraping.py) can detect this is a live reopening
                # and avoid the expire-the-lead branch.
                result["reopening_date"] = reopening
                if "reopening_date" not in result["changes"]:
                    result["changes"].append("reopening_date")
                # Also write opening_date if nothing else claimed it
                if "opening_date" not in result:
                    result["opening_date"] = reopening
                    result["changes"].append("opening_date")
            logger.info(
                f"Reopening-type project ({parsed_proj_type}) detected for "
                f"{hotel_name} — reopening_date={reopening!r}. "
                f"Classified as reopening, not new build."
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

    # Known IP licensors — media/entertainment companies that license their
    # brand/characters to hotel operators but do NOT own or operate hotels.
    # These should NEVER appear as management_company, owner, or developer.
    #
    # Real case: Nickelodeon Hotels & Resorts is branded with Paramount IP,
    # operated by Lion Star Hospitality, owned by Teramir Group. Gemini
    # occasionally lists Paramount as owner — that's wrong because Paramount
    # licenses the Nickelodeon brand but has no hotel real estate.
    #
    # NOTE: We deliberately DO NOT include hotel brand parents like Marriott,
    # Hilton, Hyatt, IHG here. Those brands legitimately own and operate many
    # of their properties (e.g. Hyatt Regency Waikiki IS owned+operated by
    # Hyatt). The earlier version of this list was over-aggressive and
    # filtered out correct answers like "Sandals Resorts International" for
    # Sandals-owned properties. Let Gemini's classification stand for those.
    _IP_LICENSOR_NAMES = {
        "paramount",
        "paramount global",
        "paramount studios",
        "paramount pictures",
        "disney",
        "walt disney",
        "walt disney company",
        "warner bros",
        "warner media",
        "warner bros. discovery",
        "nbcuniversal",
        "nbc universal",
        "comcast",
        "sony pictures",
        "columbia pictures",
        "fox",
        "viacom",
        "viacomcbs",
        # Nickelodeon is the brand itself — included for cases where Gemini
        # returns it as mgmt_company instead of brand.
        "nickelodeon",
    }

    def _clean_entity_field(raw: str, field_name: str, hotel_brand: str = "") -> str:
        """
        Clean up entity field (management_company, owner, developer).
        - Strips comma-concatenated lists (Gemini sometimes returns
          "Lion Star, Everest Place, Paramount" — nonsense)
        - Removes IP licensor names (Paramount, Disney, etc.)
        - Rejects the brand name itself (if Gemini literally returned the
          brand where operator should be)

        IMPORTANT: The brand-check uses EXACT equality, not substring match.
        "Sandals Resorts International" is a legit operator even though it
        contains "Sandals" as substring. Same for "Royalton Hotels & Resorts",
        "Marriott Vacations Worldwide" (different from just "Marriott"), etc.
        Only reject when Gemini returned literally the brand name unchanged.
        """
        if not raw:
            return ""
        # Split on commas and take only the first non-IP-licensor entity
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        brand_lower = (hotel_brand or "").strip().lower()
        for part in parts:
            pl = part.lower().strip()
            # Reject IP licensors (Paramount, Disney, media companies)
            if pl in _IP_LICENSOR_NAMES:
                continue
            # Reject ONLY exact brand match — "Sandals Resorts International"
            # is a VALID operator that happens to contain "Sandals" (brand).
            # Same for "Royalton Hotels & Resorts", "Playa Hotels & Resorts",
            # "Marriott International", etc. Substring match was over-eager.
            if brand_lower and pl == brand_lower:
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
                search_name=lead.search_name or "",
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
