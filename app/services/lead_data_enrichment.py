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
    """Call Gemini API via Vertex AI with 429 retry."""
    from app.services.gemini_client import get_gemini_url, get_gemini_headers

    url = get_gemini_url("gemini-2.5-flash")
    headers = get_gemini_headers()

    generation_config = {
        "temperature": temperature,
        "maxOutputTokens": 8192,
        "thinkingConfig": {"thinkingBudget": 0},
    }
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
    Three-stage enrichment:
      1. CLASSIFY project_type
      2. EXTRACT data fields (date, tier, rooms, brand, location)
      3. EXTRACT entities (mgmt co, owner, developer) + address — separate call
    """
    result: Dict = {
        "changes": [],
        "confidence": "low",
        "source_url": None,
    }
    location = ", ".join(filter(None, [city, state]))

    # ── STAGE 1: PROJECT TYPE CLASSIFICATION ──
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
        project_type = "new_opening"
        logger.info(
            f"Smart Fill [{hotel_name}] project_type unclassified — defaulting to new_opening"
        )

    # ── STAGE 2: BRANCHED QUERY BUILDING ──
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
        return result

    # ── STAGE 3: SEARCH ──
    all_snippets: list[str] = []
    all_urls: list[str] = []
    for q in queries[:5]:
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

    # ── STAGE 4a: DATA FIELD EXTRACTION ──
    data_extraction = await _extract_fields_for_project_type(
        hotel_name=hotel_name,
        location=location,
        project_type=project_type,
        snippets=all_snippets[:15],
        missing=[
            f
            for f in missing
            if f
            not in ("management_company", "owner", "developer", "address", "zip_code")
        ],
        mode=mode,
        current_brand=brand,
        current_opening_date=current_opening_date,
        current_brand_tier=current_brand_tier,
        current_room_count=current_room_count,
    )

    # ── STAGE 4b: ENTITY EXTRACTION (sequential, 2s gap) ──
    await _asyncio.sleep(2)
    entity_extraction = await _extract_entities(
        hotel_name=hotel_name,
        location=location,
        snippets=all_snippets[:15],
    )

    # ── STAGE 4c: ADDRESS EXTRACTION (sequential, 2s gap) ──
    await _asyncio.sleep(2)
    address_extraction = await _extract_address(
        hotel_name=hotel_name,
        location=location,
        snippets=all_snippets[:15],
    )

    logger.info(f"Smart Fill [{hotel_name}] data extraction raw: {data_extraction}")
    logger.info(f"Smart Fill [{hotel_name}] entity extraction raw: {entity_extraction}")
    logger.info(
        f"Smart Fill [{hotel_name}] address extraction raw: {address_extraction}"
    )

    # ── MERGE all extractions ──
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

    if address_extraction:
        for field in ("address", "zip_code"):
            val = address_extraction.get(field)
            if val and val.strip():
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
    """Stage 1: Quick classification call."""
    query = f'"{hotel_name}" {location} hotel news 2025 2026'
    results = await _search_web(query, max_results=4)
    if not results:
        return None

    snippet_lines = [f"- {r['title']}. {r['snippet'][:200]}" for r in results[:4]]
    snippets_text = "\n".join(snippet_lines)

    prompt = f"""Classify this hotel project by reading the search snippets.

HOTEL: {hotel_name}
LOCATION: {location}
KNOWN OPENING/REOPENING DATE: {current_opening_date or "(unknown)"}

SNIPPETS:
{snippets_text}

Classify the project as ONE of:
  - "new_opening"       = greenfield; property never existed before
  - "renovation"        = existing hotel, closed for refurbishment, reopening
  - "rebrand"           = existing hotel, changing brand affiliation
  - "reopening"         = reopening after hurricane / seasonal closure
  - "conversion"        = changing hotel type (e.g. residences → hotel)
  - "ownership_change"  = sold to new owner, same brand/operator

Return JSON only:
{{"project_type": "<one of above>", "confidence": "high|medium|low", "evidence": "one-sentence quote from snippet"}}

Default to "new_opening" if truly ambiguous.
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
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
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
    """Stage 2: Branched query building. Uses search_name for queries when available."""
    hn = search_name.strip() if search_name and search_name.strip() else hotel_name
    loc = location

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
            "address",
            "zip_code",
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
        missing.append("management_company")
        missing.append("owner")
        missing.append("developer")
        missing.append("address")
        missing.append("zip_code")

    if project_type == "renovation":
        queries = [
            f'"{hn}" {loc} renovation reopen 2026 (site:prnewswire.com OR site:businesswire.com OR site:hospitalitynet.org)',
            f'"{hn}" "$" million renovation refurbishment',
            f'"{hn}" {loc} "operated by" OR "managed by" OR "management"',
            f'"{hn}" owner OR "ownership group" OR "investment"',
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
            # Owner/developer
            f'"{hn}" owner OR developer OR "owned by" OR "developed by"',
            # Operator
            f'"{hn}" {loc} "managed by" OR "operated by" OR "management company"',
            # Address — use full hotel_name for better address results
            f'"{hotel_name}" {loc} address OR "located at" OR street',
            # Tier + rooms
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
    """Stage 4a: Data field extraction (date, tier, rooms, brand, location)."""
    snippets_text = "\n".join(f"- {s}" for s in snippets)

    if project_type == "renovation":
        type_guidance = """
PROJECT TYPE: RENOVATION / REFURBISHMENT
  - opening_date: the FUTURE REOPENING date, NOT the original opening year
  - reopening_date: same as opening_date
  - already_opened: true"""
    elif project_type == "rebrand":
        type_guidance = """
PROJECT TYPE: REBRAND
  - opening_date: the date of the brand change
  - brand: the NEW brand
  - former_names: the OLD name(s)
  - already_opened: true"""
    elif project_type == "reopening":
        type_guidance = """
PROJECT TYPE: REOPENING
  - opening_date: the reopening date (future)
  - already_opened: true"""
    elif project_type == "conversion":
        type_guidance = """
PROJECT TYPE: CONVERSION
  - opening_date: when it opens AS A HOTEL
  - brand: new brand after conversion"""
    elif project_type == "ownership_change":
        type_guidance = """
PROJECT TYPE: OWNERSHIP CHANGE
  - already_opened: true"""
    else:
        type_guidance = """
PROJECT TYPE: NEW OPENING (greenfield, first-ever opening)
  - opening_date: the FIRST EVER opening date (e.g. "Q3 2026", "December 2026")
  - already_opened: false
  - room_count: the total planned room count"""

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

    tier_rules = """
TIER RULES:
  tier1_ultra_luxury: Ritz-Carlton, Four Seasons, Aman, Rosewood, Faena, St. Regis, Pendry, Auberge
  tier2_luxury: Sandals, Royalton, Nickelodeon, Margaritaville, themed/experiential resorts, JW Marriott, Conrad, Kimpton, all-inclusive luxury
  tier3_upper_upscale: Marriott, Hilton, Westin, Sheraton, Hyatt Regency, Hyatt Centric
  tier4_upscale: Courtyard, Hilton Garden Inn, Hyatt Place, AC Hotels
  tier5_upper_midscale: Hampton Inn, Holiday Inn Express
  tier6_midscale: La Quinta, Wingate
  tier7_economy: Motel 6, Days Inn
"""

    location_rules = """
LOCATION RULES:
  - state = administrative division (Florida, Ohio, St. James Parish) NOT city or country
  - Caribbean: country = island name, state = parish/region
  - Puerto Rico / USVI: country = "USA", state = territory name
  - USA mainland: country = "USA", state = full state name
Examples:
  - Royalton Vessence Barbados → city: "Holetown", state: "Saint James", country: "Barbados"
  - Hard Rock San Juan → city: "San Juan", state: "Puerto Rico", country: "USA"
  - Hyatt Centric Cincinnati → city: "Cincinnati", state: "Ohio", country: "USA"
"""

    data_fields = [
        f
        for f in missing
        if f not in ("management_company", "owner", "developer", "address", "zip_code")
    ]

    prompt = f"""You are extracting facts about a specific hotel from search snippets.

HOTEL: {hotel_name}
LOCATION: {location}
{type_guidance}
{user_verified_block}
{tier_rules}
{location_rules}

FIELDS TO EXTRACT: {', '.join(data_fields) if data_fields else 'none'}

SEARCH SNIPPETS ({len(snippets)} results):
{snippets_text}

INSTRUCTIONS:
- Fill fields when snippets provide evidence
- For brand_tier enums, use ONE of the valid values or omit
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
            "city": {"type": "string"},
            "state": {"type": "string"},
            "country": {"type": "string"},
            "description": {"type": "string"},
            "former_names": {"type": "array", "items": {"type": "string"}},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
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
    """Stage 4b: Focused entity extraction — mgmt co, owner, developer."""
    snippets_text = "\n".join(f"- {s}" for s in snippets)

    prompt = f"""You are identifying the operator, owner, and developer of a hotel from search snippets.

HOTEL: {hotel_name}
LOCATION: {location}

ROLE DEFINITIONS:
  management_company = day-to-day hotel operator (e.g. "Lion Star Hospitality Inc.", "Blue Diamond Resorts", "Commonwealth Hotels")
  owner              = real estate holder (e.g. "Teramir Group", "Birkla Investment Group", "Shad Khan")
  developer          = entity building the property (e.g. "Everest Place", "PVH Group Inc.", often same as owner)

RULES:
  - IGNORE IP licensors: Paramount, Disney, Viacom, NBCUniversal
  - IGNORE corporate holding parents: ALG Vacations Corp, Apple Leisure Group
  - IGNORE government utilities, water authorities, sewage authorities
  - Owner-operated chains (Sandals, Blue Diamond): same entity can be both management_company AND owner

SEARCH SNIPPETS:
{snippets_text}

Return JSON with only the fields you can find evidence for. Omit fields with no evidence.
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


async def _extract_address(
    hotel_name: str,
    location: str,
    snippets: list[str],
) -> Optional[dict]:
    """
    Stage 4c: Dedicated address extraction — address and zip_code only.
    Does its own targeted search using full hotel name for best results.
    """
    # Run a dedicated address search using the full hotel name
    addr_query = f'"{hotel_name}" {location} address OR "located at" OR street'
    addr_results = await _search_web(addr_query, max_results=6)
    addr_snippets = [f"{r['title']}. {r['snippet']}" for r in addr_results]
    # Combine with passed snippets, deduplicated
    all_addr_snippets = addr_snippets + [s for s in snippets if s not in addr_snippets]
    snippets_text = "\n".join(f"- {s}" for s in all_addr_snippets[:15])

    prompt = f"""Extract the street address of this hotel from the snippets below.

HOTEL: {hotel_name}
LOCATION: {location}

SNIPPETS:
{snippets_text}

Look for street addresses like:
  - "1406 E Bay St, Jacksonville, FL 32202" → address="1406 E Bay St", zip_code="32202"
  - "set for 1406 E Bay St" → address="1406 E Bay St"
  - "3001 Turtle Creek Blvd, Dallas, TX 75219" → address="3001 Turtle Creek Blvd", zip_code="75219"
  - "located at 500 Brickell Ave" → address="500 Brickell Ave"

Rules:
  - address = street number + street name ONLY (no city, state, zip)
  - zip_code = 5-digit zip code only
  - If no street address with a number found, return empty object {{}}

Return JSON:
"""

    schema = {
        "type": "object",
        "properties": {
            "address": {"type": "string"},
            "zip_code": {"type": "string"},
        },
    }

    try:
        resp = await _call_gemini(prompt, temperature=0.1, response_schema=schema)
        if not resp:
            return None
        parsed = json.loads(resp)
        return parsed if isinstance(parsed, dict) else None
    except Exception as ex:
        logger.warning(f"Address extraction failed for {hotel_name}: {ex}")
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
    """Stage 5: Copy extracted fields into the result dict."""
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

    REOPENING_TYPES = {
        "renovation",
        "rebrand",
        "reopening",
        "conversion",
        "ownership_change",
    }
    reopening_signal = (extraction.get("reopening_date") or "").strip()
    already_opened_signal = bool(extraction.get("already_opened"))

    if (
        project_type not in REOPENING_TYPES
        and reopening_signal
        and already_opened_signal
    ):
        logger.info(
            f"Smart Fill: extraction evidence overrides classifier. "
            f"Was project_type={project_type!r} → reclassifying as 'renovation'"
        )
        project_type = "renovation"
        result["project_type"] = "renovation"
        if "project_type" not in result["changes"]:
            result["changes"].append("project_type")

    # ── opening_date ──
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
        opening = (extraction.get("opening_date") or "").strip()
        if opening and opening.lower() not in ("unknown", "tbd", "n/a"):
            if mode == "full" or not current_opening_date:
                result["opening_date"] = opening
                result["timeline_label"] = get_timeline_label(opening)
                if "opening_date" not in result["changes"]:
                    result["changes"].append("opening_date")

    # ── already_opened ──
    if project_type not in REOPENING_TYPES:
        if extraction.get("already_opened") is not None:
            result["already_opened"] = bool(extraction["already_opened"])
    opened_date = (extraction.get("opened_date") or "").strip()
    if opened_date:
        result["opened_date"] = opened_date

    # ── brand_tier ──
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
            result["brand_tier"] = extraction["brand_tier"]
            if "brand_tier" not in result["changes"]:
                result["changes"].append("brand_tier")
        elif current_tier_l in INVALID_TIER_SENTINELS:
            result["brand_tier"] = extraction["brand_tier"]
            if "brand_tier" not in result["changes"]:
                result["changes"].append("brand_tier")

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

    # ── management_company / owner / developer ──
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
                f"raw={raw!r} brand={cleaning_brand!r}"
            )

    # ── address / zip_code ──
    for addr_field in ("address", "zip_code"):
        val = (extraction.get(addr_field) or "").strip()
        if val and val.lower() not in ("unknown", "n/a", ""):
            result[addr_field] = val
            if addr_field not in result["changes"]:
                result["changes"].append(addr_field)

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
    """DEPRECATED — kept for reference only. Use enrich_lead_data() instead."""
    result = {"changes": [], "confidence": "low", "source_url": None}
    return result


async def batch_smart_fill(limit: int = 10, mode: str = "smart") -> Dict:
    """
    Find leads with missing data and enrich them.
    Called by scheduled task or manual trigger.
    """
    from sqlalchemy import select, or_
    from app.database import async_session
    from app.models.potential_lead import PotentialLead

    stats = {"checked": 0, "enriched": 0, "details": []}

    async with async_session() as session:
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
                    {"name": lead.hotel_name, "status": "no_data_found"}
                )
                continue

            changes = []
            if "opening_date" in enriched:
                lead.opening_date = enriched["opening_date"]
                lead.timeline_label = enriched.get(
                    "timeline_label", get_timeline_label(enriched["opening_date"])
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
                        f"{lead.id}: {rescore_err} — falling back to direct score write"
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
