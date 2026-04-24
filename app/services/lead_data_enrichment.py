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


# ═══════════════════════════════════════════════════════════════════════
# DATE SPECIFICITY — used by the Smart Fill date-preservation guard
# ═══════════════════════════════════════════════════════════════════════
# Prevents Full Refresh from regressing a specific date like "2026-12-18"
# to a vague one like "2026" when Gemini happens to be less confident on
# a given run. Ordered highest-to-lowest specificity:
#   4 = full date            2026-12-18, 12/18/2026, Dec 18 2026
#   3 = month + year         December 2026, Dec 2026
#   2 = quarter or season    Q4 2026, Spring 2026, Late 2026
#   1 = bare year            2026
#   0 = empty / unparseable  "", "unknown", "TBD"

_MONTH_NAMES_LOWER = {
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "jan",
    "feb",
    "mar",
    "apr",
    "jun",
    "jul",
    "aug",
    "sep",
    "sept",
    "oct",
    "nov",
    "dec",
}

_QUARTER_SEASON_KEYWORDS = {
    "q1",
    "q2",
    "q3",
    "q4",
    "first quarter",
    "second quarter",
    "third quarter",
    "fourth quarter",
    "spring",
    "summer",
    "fall",
    "autumn",
    "winter",
    "early",
    "mid",
    "late",
    "end",
}


def _date_specificity(date_str: str) -> int:
    """
    Return a specificity score for a date string. Higher = more specific.

    Used by _map_extraction_to_result to prevent Smart Fill from regressing
    a precise existing date (e.g. "2026-12-18") to a vague Gemini re-extract
    (e.g. "2026") across runs.
    """
    if not date_str or not str(date_str).strip():
        return 0
    s = str(date_str).strip().lower()

    # Full date: ISO (2026-12-18) or US (12/18/2026, 12-18-2026)
    if re.match(r"^\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}$", s):
        return 4
    if re.match(r"^\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4}$", s):
        return 4

    # Month-name + day + year ("December 18, 2026", "Dec 18 2026")
    if any(m in s for m in _MONTH_NAMES_LOWER) and re.search(r"\b\d{1,2}\b", s):
        return 4

    # Month-name + year only ("December 2026", "Dec 2026")
    if any(m in s for m in _MONTH_NAMES_LOWER):
        return 3

    # Quarter / season + year ("Q4 2026", "Spring 2026", "Late 2026")
    if any(kw in s for kw in _QUARTER_SEASON_KEYWORDS):
        return 2

    # Bare year ("2026")
    if re.fullmatch(r"20\d{2}", s):
        return 1

    # Unknown / junk
    if s in ("unknown", "tbd", "n/a", "na", "none"):
        return 0

    # Default: something we don't recognize — treat as low-specificity
    return 1


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
    max_attempts: int = 3,
) -> Optional[str]:
    """
    Call Gemini API via Vertex AI with full transient-error retry.

    Retries on:
      - httpx.ReadTimeout  (Vertex AI slow response / cold-boot latency)
      - httpx.ConnectError / ConnectTimeout
      - HTTP 429 (rate limit — existing behavior)
      - HTTP 5xx (transient server errors)

    Silent fallback on persistent failure (returns None). The caller
    handles the fallback path (e.g. classifier falls back to
    'new_opening' default; extraction-evidence-override can still
    correct the project_type downstream).
    """
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

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": generation_config,
    }

    # Exponential backoff: 2s, 4s, 8s
    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=75) as client:
                resp = await client.post(url, headers=headers, json=payload)

            if resp.status_code == 200:
                data = resp.json()
                return data["candidates"][0]["content"]["parts"][0]["text"]

            if resp.status_code == 429:
                wait = min(2**attempt, 16)
                logger.warning(
                    f"Gemini 429 rate limit (attempt {attempt}/{max_attempts}), "
                    f"retrying in {wait}s..."
                )
                await _asyncio.sleep(wait)
                continue

            if 500 <= resp.status_code < 600:
                wait = min(2**attempt, 16)
                logger.warning(
                    f"Gemini {resp.status_code} server error "
                    f"(attempt {attempt}/{max_attempts}), retrying in {wait}s..."
                )
                await _asyncio.sleep(wait)
                continue

            # Non-retryable (4xx other than 429) — bail immediately
            logger.warning(
                f"Gemini error {resp.status_code} (non-retryable): "
                f"{resp.text[:200]}"
            )
            return None

        except (httpx.ReadTimeout, httpx.ConnectError, httpx.ConnectTimeout) as e:
            wait = min(2**attempt, 16)
            logger.warning(
                f"Gemini {type(e).__name__} "
                f"(attempt {attempt}/{max_attempts}), retrying in {wait}s..."
            )
            if attempt < max_attempts:
                await _asyncio.sleep(wait)
                continue
            logger.warning(
                f"Gemini call failed after {max_attempts} attempts: {type(e).__name__}"
            )
            return None

        except Exception as e:
            # Non-transient errors (JSON decode, auth, etc.) — no retry
            logger.warning(
                f"Gemini call failed [{type(e).__name__}]: {e}", exc_info=True
            )
            return None

    # Exhausted retries
    logger.warning(f"Gemini call exhausted {max_attempts} attempts")
    return None


async def enrich_lead_data(
    hotel_name: str,
    city: str = "",
    state: str = "",
    country: str = "",
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

    `country` is the lead's KNOWN country from the DB. It flows into the
    location context for every stage, and critically into the address
    extractor's country detection so Caribbean / international properties
    get the right country-specific rules even when Gemini doesn't re-return
    `country` in its extraction.
    """
    result: Dict = {
        "changes": [],
        "confidence": "low",
        "source_url": None,
    }
    # Include country in location so the address extractor and the
    # project-type classifier both see it. Previously this was just
    # "{city}, {state}" which made Caribbean country detection fail
    # whenever Gemini omitted `country` from its response.
    location = ", ".join(filter(None, [city, state, country]))

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
    # Country detection precedence for the address extractor:
    #   1. Gemini's fresh extraction (data_extraction['country']) — most up-to-date
    #   2. The DB-passed `country` arg — falls back when Gemini omits it
    #   3. Parse from location string (inside _detect_country)
    # Prior to the `country` param being threaded through, this relied
    # solely on Gemini's extraction and degraded to "Generic / Unknown
    # Caribbean" whenever Gemini happened not to return country.
    detected_country = (
        (data_extraction.get("country") if data_extraction else None) or country or ""
    )
    detected_city = (
        (data_extraction.get("city") if data_extraction else None) or city or ""
    )
    address_extraction = await _extract_address(
        hotel_name=hotel_name,
        location=location,
        snippets=all_snippets[:15],
        country=detected_country,
        city=detected_city,
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

    # TIER RULES generated from canonical_tiers.py (STR 2024 + CBRE-derived).
    # Previously this was hardcoded and conflicted with scorer.py + brand_registry.py.
    # All three sources now share the same tier truth via canonical_tiers.CANONICAL_TIERS.
    from app.config.canonical_tiers import build_tier_rules_prompt_block

    tier_rules = "\n" + build_tier_rules_prompt_block()

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


# ═══════════════════════════════════════════════════════════════════════
# ADDRESS EXTRACTION — Fully trained for USA + Caribbean
# ═══════════════════════════════════════════════════════════════════════
#
# The extractor is country-aware. Each country has its own:
#   - postcode format description + regex validator
#   - typical street-naming conventions
#   - 3-6 real-world address examples Gemini can pattern-match
#   - explicit rules (what to include, what to reject)
#
# Countries covered (all JA markets):
#   USA · Puerto Rico · USVI · Jamaica · Bahamas · Barbados · Cayman Islands
#   Anguilla · Turks and Caicos · Dominican Republic · Aruba · Curaçao
#   St. Lucia · St. Kitts and Nevis · Antigua and Barbuda · Grenada
#   British Virgin Islands · Bermuda · Trinidad and Tobago · Dominica
#   St. Vincent & Grenadines · St. Martin / Sint Maarten
#
# Unknown countries fall back to a "Generic Caribbean" ruleset.
# ═══════════════════════════════════════════════════════════════════════


_ADDRESS_RULES: Dict[str, Dict] = {
    # ── USA ─────────────────────────────────────────────────────────
    "USA": {
        "postcode_desc": "5-digit ZIP code (ZIP+4 like 90210-1234 → use first 5)",
        "postcode_validate": re.compile(r"^\d{5}$"),
        "postcode_extract": re.compile(r"\b(\d{5})(?:-\d{4})?\b"),
        "examples": [
            '"1435 Brickell Ave, Miami, FL 33131"          → address="1435 Brickell Ave",    zip_code="33131"',
            '"3001 Turtle Creek Blvd, Dallas, TX 75219"     → address="3001 Turtle Creek Blvd", zip_code="75219"',
            '"1 S County Rd, Palm Beach, FL 33480"          → address="1 S County Rd",        zip_code="33480"',
            '"4441 Collins Ave, Miami Beach, FL 33140"      → address="4441 Collins Ave",     zip_code="33140"',
            '"540 N Michigan Ave, Chicago, IL 60611"        → address="540 N Michigan Ave",   zip_code="60611"',
            '"180 Rutherford Hill Rd, Rutherford, CA 94573" → address="180 Rutherford Hill Rd", zip_code="94573"',
        ],
        "rules": [
            "address = street number + street name only (e.g., '1435 Brickell Ave')",
            "zip_code = 5-digit US ZIP only",
            "Do NOT include city, state, or 'USA' in the address field",
            "Strongly prefer entries that include a street number",
        ],
    },
    # ── Puerto Rico (US territory, ZIP 006xx-009xx) ─────────────────
    "Puerto Rico": {
        "postcode_desc": "USA-style 5-digit ZIP in the 006xx–009xx range",
        "postcode_validate": re.compile(r"^00[6-9]\d{2}$"),
        "postcode_extract": re.compile(r"\b(00[6-9]\d{2})(?:-\d{4})?\b"),
        "examples": [
            '"100 Brumbaugh St, Old San Juan, PR 00901"              → address="100 Brumbaugh St",         zip_code="00901"',
            '"6961 Ave of the Governors, Rio Grande, PR 00745"       → address="6961 Ave of the Governors", zip_code="00745"',
            '"1055 Ashford Ave, Condado, San Juan, PR 00907"         → address="1055 Ashford Ave",         zip_code="00907"',
            '"Carr 987 KM 6.3, Fajardo, PR 00738"                     → address="Carr 987 KM 6.3",          zip_code="00738"',
        ],
        "rules": [
            "Puerto Rico uses USA ZIPs in the 006xx–009xx range",
            "Treat like USA — prefer street number + street name",
        ],
    },
    # ── US Virgin Islands (US territory, ZIP 008xx) ─────────────────
    "U.S. Virgin Islands": {
        "postcode_desc": "USA-style 5-digit ZIP in the 008xx range",
        "postcode_validate": re.compile(r"^008\d{2}$"),
        "postcode_extract": re.compile(r"\b(008\d{2})(?:-\d{4})?\b"),
        "examples": [
            '"6900 Great Bay, St. Thomas, USVI 00802"   → address="6900 Great Bay",   zip_code="00802"',
            '"Smith Bay Road, St. Thomas, USVI 00802"   → address="Smith Bay Road",   zip_code="00802"',
            '"Estate Peter Bay, St. John, USVI 00830"   → address="Estate Peter Bay", zip_code="00830"',
        ],
        "rules": [
            "USVI uses USA ZIPs in the 008xx range",
        ],
    },
    # ── Jamaica (no postcodes) ──────────────────────────────────────
    "Jamaica": {
        "postcode_desc": "none — Jamaica does not use postcodes. zip_code MUST be empty.",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Kent Avenue, Mahoe Bay, Montego Bay, St. James, Jamaica" → address="Kent Avenue, Mahoe Bay", zip_code=""',
            '"P.O. Box 167, Mahoe Bay, Montego Bay, Jamaica"            → address="Mahoe Bay",              zip_code=""',
            '"Norman Manley Boulevard, Negril, Westmoreland, Jamaica"   → address="Norman Manley Boulevard", zip_code=""',
            '"Main Road, Ocho Rios, St. Ann, Jamaica"                   → address="Main Road, Ocho Rios",    zip_code=""',
            '"White River, Oracabessa, St. Mary, Jamaica"               → address="White River, Oracabessa", zip_code=""',
            '"Rose Hall Main Road, Montego Bay, Jamaica"                → address="Rose Hall Main Road",     zip_code=""',
        ],
        "rules": [
            "Jamaica addresses rarely have street numbers — that's normal, not a reason to reject",
            "Extract street/road/avenue/bay + district (e.g., 'Kent Avenue, Mahoe Bay')",
            "If only a P.O. Box + district is given, use the district name (e.g., 'Mahoe Bay')",
            "DO NOT use 'P.O. Box 167' as the address",
            "DO NOT include parish names (St. James, St. Ann) in the address field — those are state",
            "zip_code MUST be empty for Jamaica",
        ],
    },
    # ── Bahamas (no postcodes) ──────────────────────────────────────
    "Bahamas": {
        "postcode_desc": "none — Bahamas does not use postcodes. zip_code MUST be empty.",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Cable Beach, Nassau, Bahamas"                       → address="Cable Beach",            zip_code=""',
            '"One Casino Drive, Paradise Island, Bahamas"         → address="One Casino Drive",       zip_code=""',
            '"Queens Highway, Great Exuma, Bahamas"               → address="Queens Highway",         zip_code=""',
            '"Old Fort Bay Road, Nassau, Bahamas"                 → address="Old Fort Bay Road",      zip_code=""',
            '"Stocking Island, George Town, Exuma, Bahamas"       → address="Stocking Island, George Town", zip_code=""',
            '"Harbour Island, Eleuthera, Bahamas"                 → address="Harbour Island",          zip_code=""',
        ],
        "rules": [
            "Extract road/highway/drive/beach + island-area name",
            "Some modern resort addresses include street numbers (e.g., 'One Casino Drive') — include them",
            "zip_code MUST be empty for Bahamas",
        ],
    },
    # ── Barbados (BB#####) ──────────────────────────────────────────
    "Barbados": {
        "postcode_desc": "'BB' followed by exactly 5 digits (e.g., 'BB24013', 'BB17038')",
        "postcode_validate": re.compile(r"^BB\d{5}$"),
        "postcode_extract": re.compile(r"\bBB\d{5}\b"),
        "examples": [
            '"Dorsett Road, Holetown, St. James, Barbados BB24013" → address="Dorsett Road, Holetown", zip_code="BB24013"',
            '"Paynes Bay, St. James, Barbados BB24024"              → address="Paynes Bay",             zip_code="BB24024"',
            '"The Crane, St. Philip, Barbados BB17038"              → address="The Crane, St. Philip",  zip_code="BB17038"',
            '"Highway 1, Sandy Lane, Holetown, Barbados BB24024"    → address="Highway 1, Sandy Lane",  zip_code="BB24024"',
            '"Maxwell Coast Road, Christ Church, Barbados BB15094"  → address="Maxwell Coast Road",     zip_code="BB15094"',
        ],
        "rules": [
            "Barbados postcodes are strictly 'BB' + 5 digits — validate format",
            "Parishes (St. James, Christ Church) are state, not address",
        ],
    },
    # ── Cayman Islands (KY#-####) ───────────────────────────────────
    "Cayman Islands": {
        "postcode_desc": "'KY' + single digit + hyphen + 4 digits (e.g., 'KY1-1209')",
        "postcode_validate": re.compile(r"^KY\d-\d{4}$"),
        "postcode_extract": re.compile(r"\bKY\d-\d{4}\b"),
        "examples": [
            '"West Bay Road, Seven Mile Beach, Grand Cayman KY1-1209" → address="West Bay Road, Seven Mile Beach", zip_code="KY1-1209"',
            '"Seven Mile Beach, Grand Cayman KY1-1205"                 → address="Seven Mile Beach",               zip_code="KY1-1205"',
            '"South Church Street, George Town, Grand Cayman KY1-1103" → address="South Church Street, George Town", zip_code="KY1-1103"',
        ],
        "rules": [
            "Cayman postcodes are strictly 'KY' + digit + '-' + 4 digits",
        ],
    },
    # ── Anguilla (AI-2640) ──────────────────────────────────────────
    "Anguilla": {
        "postcode_desc": "'AI-2640' is the single territory-wide postcode",
        "postcode_validate": re.compile(r"^AI-2640$"),
        "postcode_extract": re.compile(r"\bAI-2640\b"),
        "examples": [
            '"Maundays Bay, West End, Anguilla AI-2640" → address="Maundays Bay, West End", zip_code="AI-2640"',
            '"Meads Bay, Anguilla AI-2640"              → address="Meads Bay",              zip_code="AI-2640"',
            '"Shoal Bay East, Anguilla AI-2640"         → address="Shoal Bay East",         zip_code="AI-2640"',
            '"Rendezvous Bay, Anguilla AI-2640"         → address="Rendezvous Bay",         zip_code="AI-2640"',
        ],
        "rules": [
            "All Anguilla properties use the same postcode: AI-2640",
        ],
    },
    # ── Turks and Caicos (TKCA 1ZZ, rarely seen) ────────────────────
    "Turks and Caicos": {
        "postcode_desc": "'TKCA 1ZZ' is the single territory postcode (rarely published — empty is OK)",
        "postcode_validate": re.compile(r"^TKCA 1ZZ$"),
        "postcode_extract": re.compile(r"\bTKCA 1ZZ\b"),
        "examples": [
            '"Grace Bay Road, Providenciales, Turks and Caicos"   → address="Grace Bay Road, Providenciales",   zip_code=""',
            '"Leeward Highway, Providenciales, Turks and Caicos"  → address="Leeward Highway, Providenciales",  zip_code=""',
            '"Parrot Cay, Turks and Caicos"                        → address="Parrot Cay",                        zip_code=""',
        ],
        "rules": [
            "Turks and Caicos postcodes are rarely in listings — leave zip_code empty unless explicitly present",
        ],
    },
    # ── Dominican Republic ──────────────────────────────────────────
    "Dominican Republic": {
        "postcode_desc": "5-digit numeric (rarely published in resort listings — empty OK)",
        "postcode_validate": re.compile(r"^\d{5}$"),
        "postcode_extract": re.compile(r"\b(\d{5})\b"),
        "examples": [
            '"Playa Bavaro, Punta Cana, La Altagracia, Dominican Republic" → address="Playa Bavaro, Punta Cana", zip_code=""',
            '"Avenida Francia, Puerto Plata, Dominican Republic"            → address="Avenida Francia, Puerto Plata", zip_code=""',
            '"Carretera Macao, Punta Cana 23000, Dominican Republic"        → address="Carretera Macao, Punta Cana",  zip_code="23000"',
            '"Playa Cabeza de Toro, Punta Cana, Dominican Republic"         → address="Playa Cabeza de Toro, Punta Cana", zip_code=""',
        ],
        "rules": [
            "Street names are in Spanish (Avenida, Calle, Carretera, Playa)",
            "Postcodes (5 digits) are rarely published — leave zip_code empty unless clearly shown",
        ],
    },
    # ── Aruba (no postcodes) ────────────────────────────────────────
    "Aruba": {
        "postcode_desc": "none — Aruba does not use postcodes. zip_code MUST be empty.",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"J.E. Irausquin Boulevard 77, Palm Beach, Aruba" → address="J.E. Irausquin Boulevard 77, Palm Beach", zip_code=""',
            '"L.G. Smith Boulevard 101, Oranjestad, Aruba"     → address="L.G. Smith Boulevard 101, Oranjestad",    zip_code=""',
            '"Eagle Beach, Aruba"                                → address="Eagle Beach",                              zip_code=""',
        ],
        "rules": [
            "Aruba uses Dutch-style addressing — street numbers are common",
            "zip_code MUST be empty for Aruba",
        ],
    },
    # ── Curaçao (no postcodes) ──────────────────────────────────────
    "Curaçao": {
        "postcode_desc": "none — Curaçao does not use postcodes. zip_code MUST be empty.",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Piscadera Bay, Willemstad, Curaçao"                 → address="Piscadera Bay, Willemstad",            zip_code=""',
            '"John F. Kennedy Boulevard, Willemstad, Curaçao"     → address="John F. Kennedy Boulevard, Willemstad", zip_code=""',
            '"Penstraat 130, Willemstad, Curaçao"                  → address="Penstraat 130, Willemstad",             zip_code=""',
        ],
        "rules": [
            "zip_code MUST be empty for Curaçao",
        ],
    },
    # ── St. Lucia (LC## ###, rarely published) ──────────────────────
    "St. Lucia": {
        "postcode_desc": "'LC' + 2 digits + space + 3 digits (rarely published — empty OK)",
        "postcode_validate": re.compile(r"^LC\d{2} \d{3}$"),
        "postcode_extract": re.compile(r"\bLC\d{2} \d{3}\b"),
        "examples": [
            '"Pigeon Island Causeway, Gros Islet, St. Lucia" → address="Pigeon Island Causeway, Gros Islet", zip_code=""',
            '"Rodney Bay, Gros Islet, St. Lucia"             → address="Rodney Bay, Gros Islet",             zip_code=""',
            '"Anse Chastanet, Soufriere, St. Lucia"          → address="Anse Chastanet, Soufriere",          zip_code=""',
            '"Jalousie, Soufriere, St. Lucia"                → address="Jalousie, Soufriere",                zip_code=""',
        ],
        "rules": [
            "St. Lucia postcodes rarely appear in listings — leave zip_code empty unless explicit",
        ],
    },
    # ── Antigua and Barbuda (no postcodes) ──────────────────────────
    "Antigua and Barbuda": {
        "postcode_desc": "none",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Dickenson Bay, St. John\'s, Antigua" → address="Dickenson Bay, St. John\'s", zip_code=""',
            '"Jolly Harbour, Antigua"                → address="Jolly Harbour",               zip_code=""',
            '"Hodges Bay, St. John\'s, Antigua"    → address="Hodges Bay, St. John\'s",    zip_code=""',
            '"Mamora Bay, Antigua"                   → address="Mamora Bay",                  zip_code=""',
        ],
        "rules": [
            "zip_code empty",
        ],
    },
    # ── St. Kitts and Nevis (no postcodes) ──────────────────────────
    "St. Kitts and Nevis": {
        "postcode_desc": "none",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Pinney\'s Beach, Charlestown, Nevis" → address="Pinney\'s Beach, Charlestown", zip_code=""',
            '"Frigate Bay, Basseterre, St. Kitts"    → address="Frigate Bay, Basseterre",       zip_code=""',
            '"Christophe Harbour, St. Kitts"          → address="Christophe Harbour",             zip_code=""',
        ],
        "rules": [
            "zip_code empty",
        ],
    },
    # ── Grenada (no postcodes) ──────────────────────────────────────
    "Grenada": {
        "postcode_desc": "none",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Grand Anse Beach, St. George\'s, Grenada" → address="Grand Anse Beach, St. George\'s", zip_code=""',
            '"Morne Rouge Bay, St. George\'s, Grenada"  → address="Morne Rouge Bay, St. George\'s",  zip_code=""',
            '"Pink Gin Beach, St. George\'s, Grenada"   → address="Pink Gin Beach, St. George\'s",   zip_code=""',
        ],
        "rules": [
            "zip_code empty",
        ],
    },
    # ── British Virgin Islands (VG####, rarely published) ───────────
    "British Virgin Islands": {
        "postcode_desc": "'VG' + 4 digits (rarely published)",
        "postcode_validate": re.compile(r"^VG\d{4}$"),
        "postcode_extract": re.compile(r"\bVG\d{4}\b"),
        "examples": [
            '"The Settlement, Virgin Gorda, BVI"        → address="The Settlement, Virgin Gorda", zip_code=""',
            '"Prospect Reef, Road Town, Tortola, BVI"   → address="Prospect Reef, Road Town",     zip_code=""',
            '"Little Dix Bay, Virgin Gorda, BVI VG1150" → address="Little Dix Bay, Virgin Gorda", zip_code="VG1150"',
        ],
        "rules": [
            "zip_code usually empty unless explicitly given",
        ],
    },
    # ── Bermuda (AA ##) ─────────────────────────────────────────────
    "Bermuda": {
        "postcode_desc": "'XX ##' (two letters, space, two digits — e.g., 'HM 08', 'SN 02')",
        "postcode_validate": re.compile(r"^[A-Z]{2} \d{2}$"),
        "postcode_extract": re.compile(r"\b([A-Z]{2} \d{2})\b"),
        "examples": [
            '"60 South Shore Road, Southampton, Bermuda SN 02" → address="60 South Shore Road, Southampton", zip_code="SN 02"',
            '"1 King Street, Hamilton, Bermuda HM 12"           → address="1 King Street, Hamilton",          zip_code="HM 12"',
            '"Tucker\'s Point, Hamilton Parish, Bermuda HS 02"  → address="Tucker\'s Point, Hamilton Parish", zip_code="HS 02"',
        ],
        "rules": [
            "Bermuda postcodes are common — capture them",
        ],
    },
    # ── Trinidad and Tobago (no postcodes in tourism) ───────────────
    "Trinidad and Tobago": {
        "postcode_desc": "6-digit (rarely used in hotel listings — empty OK)",
        "postcode_validate": re.compile(r"^\d{6}$"),
        "postcode_extract": re.compile(r"\b(\d{6})\b"),
        "examples": [
            '"Store Bay Local Road, Crown Point, Tobago" → address="Store Bay Local Road, Crown Point", zip_code=""',
            '"Pigeon Point, Scarborough, Tobago"          → address="Pigeon Point, Scarborough",         zip_code=""',
            '"Wrightson Road, Port of Spain, Trinidad"    → address="Wrightson Road, Port of Spain",     zip_code=""',
        ],
        "rules": [
            "zip_code usually empty",
        ],
    },
    # ── Dominica (no postcodes) ─────────────────────────────────────
    "Dominica": {
        "postcode_desc": "none",
        "postcode_validate": None,
        "postcode_extract": None,
        "examples": [
            '"Morne Daniel, Roseau, Dominica" → address="Morne Daniel, Roseau", zip_code=""',
            '"Castle Comfort, Roseau, Dominica" → address="Castle Comfort, Roseau", zip_code=""',
        ],
        "rules": ["zip_code empty"],
    },
    # ── St. Vincent & Grenadines ────────────────────────────────────
    "St. Vincent and the Grenadines": {
        "postcode_desc": "'VC####' (rarely used)",
        "postcode_validate": re.compile(r"^VC\d{4}$"),
        "postcode_extract": re.compile(r"\bVC\d{4}\b"),
        "examples": [
            '"Mustique Island, St. Vincent and the Grenadines" → address="Mustique Island", zip_code=""',
            '"Young Island, Kingstown, St. Vincent"             → address="Young Island, Kingstown", zip_code=""',
        ],
        "rules": ["zip_code usually empty"],
    },
    # ── St. Martin / Sint Maarten ───────────────────────────────────
    "St. Martin": {
        "postcode_desc": "French side: 97150; Dutch side: none",
        "postcode_validate": re.compile(r"^\d{5}$"),
        "postcode_extract": re.compile(r"\b(\d{5})\b"),
        "examples": [
            '"Anse Marcel, 97150 Saint-Martin"          → address="Anse Marcel",            zip_code="97150"',
            '"Simpson Bay, Sint Maarten"                  → address="Simpson Bay",            zip_code=""',
            '"Maho Beach, Sint Maarten"                   → address="Maho Beach",             zip_code=""',
        ],
        "rules": [
            "French side (Saint-Martin) uses 5-digit postcode 97150",
            "Dutch side (Sint Maarten) has no postcodes",
        ],
    },
}

# Fallback for any Caribbean / international country not explicitly listed.
_ADDRESS_RULES_DEFAULT_CARIBBEAN = {
    "postcode_desc": "unknown — leave empty unless clearly published",
    "postcode_validate": None,
    "postcode_extract": None,
    "examples": [
        '"Beach Road, Main District, Country"   → address="Beach Road, Main District", zip_code=""',
        '"Bay Area, Town, Country"              → address="Bay Area, Town",            zip_code=""',
    ],
    "rules": [
        "Extract the best street/bay/road + district you can find",
        "Caribbean addresses rarely have street numbers",
        "zip_code empty unless explicitly given",
    ],
}


# Country-name aliases → canonical key in _ADDRESS_RULES.
# Strings in `location` are compared case-insensitively; multi-word aliases
# checked first so "st. lucia" wins over "lucia".
_COUNTRY_ALIASES = {
    # USA
    "usa": "USA",
    "united states": "USA",
    "u.s.a.": "USA",
    "u.s.": "USA",
    "america": "USA",
    # US territories
    "puerto rico": "Puerto Rico",
    "pr": "Puerto Rico",
    "u.s. virgin islands": "U.S. Virgin Islands",
    "us virgin islands": "U.S. Virgin Islands",
    "usvi": "U.S. Virgin Islands",
    "saint thomas": "U.S. Virgin Islands",  # contextual
    "st. thomas": "U.S. Virgin Islands",
    "saint croix": "U.S. Virgin Islands",
    "st. croix": "U.S. Virgin Islands",
    # Caribbean
    "jamaica": "Jamaica",
    "bahamas": "Bahamas",
    "the bahamas": "Bahamas",
    "barbados": "Barbados",
    "cayman islands": "Cayman Islands",
    "grand cayman": "Cayman Islands",
    "cayman": "Cayman Islands",
    "anguilla": "Anguilla",
    "turks and caicos": "Turks and Caicos",
    "turks & caicos": "Turks and Caicos",
    "dominican republic": "Dominican Republic",
    "aruba": "Aruba",
    "curaçao": "Curaçao",
    "curacao": "Curaçao",
    "st. lucia": "St. Lucia",
    "saint lucia": "St. Lucia",
    "st lucia": "St. Lucia",
    "st. kitts and nevis": "St. Kitts and Nevis",
    "saint kitts and nevis": "St. Kitts and Nevis",
    "st. kitts": "St. Kitts and Nevis",
    "st kitts": "St. Kitts and Nevis",
    "nevis": "St. Kitts and Nevis",
    "antigua and barbuda": "Antigua and Barbuda",
    "antigua": "Antigua and Barbuda",
    "grenada": "Grenada",
    "british virgin islands": "British Virgin Islands",
    "bvi": "British Virgin Islands",
    "tortola": "British Virgin Islands",
    "virgin gorda": "British Virgin Islands",
    "bermuda": "Bermuda",
    "trinidad and tobago": "Trinidad and Tobago",
    "trinidad": "Trinidad and Tobago",
    "tobago": "Trinidad and Tobago",
    "dominica": "Dominica",
    "st. vincent and the grenadines": "St. Vincent and the Grenadines",
    "saint vincent and the grenadines": "St. Vincent and the Grenadines",
    "st. vincent": "St. Vincent and the Grenadines",
    "st vincent": "St. Vincent and the Grenadines",
    "st. martin": "St. Martin",
    "saint-martin": "St. Martin",
    "saint martin": "St. Martin",
    "sint maarten": "St. Martin",
}


# Directory / OTA / map sources that usually carry the canonical address.
# Snippets from these domains get sorted to the TOP of the prompt so
# Gemini anchors its extraction on high-trust data.
_ADDRESS_TRUSTED_DOMAINS = {
    "visitjamaica.com",
    "cvent.com",
    "tripadvisor.com",
    "travelweekly.com",
    "travelagewest.com",
    "booking.com",
    "expedia.com",
    "hotels.com",
    "agoda.com",
    "frommers.com",
    "fodors.com",
    "waze.com",
    "google.com/maps",
    "maps.google.com",
    "marriott.com",
    "hilton.com",
    "hyatt.com",
    "ihg.com",
    "sandals.com",
    "hotel-online.com",
    "wikipedia.org",
}


# Things Gemini sometimes returns that are NOT addresses — we reject these.
_ADDRESS_REJECTS = {
    "",
    "n/a",
    "na",
    "unknown",
    "tbd",
    "none",
    "not available",
    "not found",
    "not specified",
    "address not available",
    "to be announced",
}


def _detect_country(location: str, passed_country: str = "") -> str:
    """
    Detect canonical country key from a location string.

    Priority:
      1. Explicit `passed_country` arg if it matches an alias
      2. Last comma-separated segment of `location` (typical format)
      3. Any alias found anywhere in the location string
      4. 2-letter US state code → USA
      5. Fallback: "Generic Caribbean" (returns empty string, caller uses default rules)
    """
    if passed_country:
        pc = passed_country.strip().lower()
        if pc in _COUNTRY_ALIASES:
            return _COUNTRY_ALIASES[pc]

    loc = (location or "").strip().lower()
    if not loc:
        return ""

    # Try last comma-separated segment first (e.g. "..., Jamaica")
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if parts:
        last = parts[-1]
        if last in _COUNTRY_ALIASES:
            return _COUNTRY_ALIASES[last]

    # Try any alias in the full string (longer aliases first so
    # "dominican republic" wins over "dominica")
    for alias in sorted(_COUNTRY_ALIASES.keys(), key=len, reverse=True):
        if alias in loc:
            return _COUNTRY_ALIASES[alias]

    # US state code heuristic — if the location has ", XX " or ends in ", XX"
    # where XX is a valid US 2-letter state code, treat as USA.
    _US_STATES = {
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "DC",
    }
    orig = (location or "").strip()
    # Look at each comma-separated segment for a standalone 2-letter code
    for seg in (s.strip() for s in orig.split(",")):
        # seg might be "FL" or "FL 33101" or "Florida"
        tokens = seg.split()
        if tokens and tokens[0].upper() in _US_STATES:
            return "USA"

    return ""  # caller will use default rules


def _rank_snippets_by_trust(snippets: list[tuple[str, str]]) -> list[str]:
    """
    Given [(snippet_text, source_url), ...], return snippet strings sorted
    so trusted-domain snippets come FIRST. Keeps original order within each
    tier.
    """
    trusted: list[str] = []
    other: list[str] = []
    for text, url in snippets:
        url_lower = (url or "").lower()
        is_trusted = any(d in url_lower for d in _ADDRESS_TRUSTED_DOMAINS)
        if is_trusted:
            trusted.append(text)
        else:
            other.append(text)
    return trusted + other


def _validate_extraction(
    extracted: dict,
    country: str,
    city: str,
    hotel_name: str,
) -> dict:
    """
    Post-Gemini validation pass.

    - Rejects addresses that are just the city name, country name, or a
      stop-word (e.g. 'unknown')
    - Validates postcodes against the country's regex; drops malformed
    - Normalizes casing and trims whitespace
    """
    out = {}
    addr = (extracted.get("address") or "").strip().strip(",").strip()
    zipc = (extracted.get("zip_code") or "").strip()

    # Reject obvious junk addresses
    if addr:
        addr_l = addr.lower()
        if addr_l in _ADDRESS_REJECTS:
            addr = ""
        elif city and addr_l == city.strip().lower():
            # "Montego Bay" returned as address when city is Montego Bay
            logger.info(f"Address extraction: rejecting '{addr}' — identical to city")
            addr = ""
        elif country and addr_l == country.strip().lower():
            logger.info(
                f"Address extraction: rejecting '{addr}' — identical to country"
            )
            addr = ""
        elif len(addr) <= 3:
            # Too short to be useful (e.g. "St.")
            logger.info(f"Address extraction: rejecting too-short '{addr}'")
            addr = ""

    if addr:
        out["address"] = addr

    # Validate postcode against country format
    if zipc:
        rules = _ADDRESS_RULES.get(country, _ADDRESS_RULES_DEFAULT_CARIBBEAN)
        validator = rules.get("postcode_validate")
        if validator is None:
            # Country doesn't use postcodes → drop whatever Gemini returned
            logger.info(
                f"Address extraction: dropping zip '{zipc}' — "
                f"{country or 'unknown'} doesn't use postcodes"
            )
        elif validator.match(zipc):
            out["zip_code"] = zipc
        else:
            logger.info(
                f"Address extraction: dropping invalid zip '{zipc}' for {country} "
                f"(expected: {rules['postcode_desc']})"
            )

    return out


def _regex_fallback_address(
    snippets: list[str],
    country: str,
) -> dict:
    """
    Best-effort regex extraction when Gemini returns nothing usable.
    Looks for common patterns:
      - "123 Street Name, City" (US-style — only for US-family countries)
      - "Street/Road/Avenue/Bay, District, City" (Caribbean-style)
      - Country-specific postcodes
    """
    joined = "\n".join(snippets)
    result: dict = {}

    # Strip out "P.O. Box NNN" / "PO Box NNN" before regex matching —
    # otherwise the US pattern greedily matches "167 Mahoe Bay St" from
    # "P.O. Box 167 Mahoe Bay St James", treating the Box number as a
    # street number. Replace with a sentinel that breaks that match.
    cleaned = re.sub(
        r"P\.?\s*O\.?\s*Box\s+\d+",
        "|POBOX|",
        joined,
        flags=re.IGNORECASE,
    )

    # Try postcode extraction — if found, address often precedes it
    rules = _ADDRESS_RULES.get(country, _ADDRESS_RULES_DEFAULT_CARIBBEAN)
    pc_extract = rules.get("postcode_extract")
    if pc_extract:
        m = pc_extract.search(cleaned)
        if m:
            result["zip_code"] = m.group(0) if m.lastindex is None else m.group(1)

    # Caribbean pattern — "Named Road/Bay/Beach/etc., District"
    # Runs FIRST for non-US countries so US pattern doesn't grab a P.O.
    # Box number masquerading as a street number.
    is_us_family = country in ("USA", "Puerto Rico", "U.S. Virgin Islands")
    carib_pat = re.compile(
        r"\b([A-Z][a-zA-Z\.\']*(?:\s+[A-Z][a-zA-Z\.\']*){0,3}\s+"
        r"(?:Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Bay|Beach|"
        r"Highway|Hwy|Gap|Harbour|Harbor))(?:,\s*"
        r"([A-Z][a-zA-Z\.\']*(?:\s+[A-Z][a-zA-Z\.\']*){0,2}))?"
    )

    if not is_us_family:
        m = carib_pat.search(cleaned)
        if m:
            pieces = [m.group(1)]
            if m.group(2) and m.group(2).lower() not in ("p.o", "po", "pobox"):
                pieces.append(m.group(2))
            result["address"] = ", ".join(pieces)
            return result

    # US pattern — "Number Word Word St/Ave/Rd/Blvd/Dr/etc."
    us_street_pat = re.compile(
        r"\b(\d{1,6}\s+[A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*){0,4}\s+"
        r"(?:Ave(?:nue)?|St(?:reet)?|Blvd|Boulevard|Rd|Road|Dr(?:ive)?|"
        r"Ln|Lane|Way|Pkwy|Parkway|Hwy|Highway|Ct|Court|Pl|Place|"
        r"Ter|Terrace))\b"
    )
    m = us_street_pat.search(cleaned)
    if m:
        result["address"] = m.group(1)
        return result

    # Caribbean pattern as final fallback even for US-family if US failed
    if is_us_family:
        m = carib_pat.search(cleaned)
        if m:
            pieces = [m.group(1)]
            if m.group(2) and m.group(2).lower() not in ("p.o", "po", "pobox"):
                pieces.append(m.group(2))
            result["address"] = ", ".join(pieces)

    return result


async def _extract_address(
    hotel_name: str,
    location: str,
    snippets: list[str],
    country: str = "",
    city: str = "",
) -> Optional[dict]:
    """
    Stage 4c: Address extraction — country-aware, US + Caribbean trained.

    Pipeline:
      1. Detect country (from `country` arg or parsing `location`)
      2. Run 3 parallel targeted searches (general, "located at", contact/directions)
      3. Merge + dedupe snippets, rank by source trust
      4. Build country-specific prompt with real examples + rules
      5. Call Gemini with structured JSON schema
      6. Post-validate: reject city/country-name echoes, drop malformed postcodes
      7. Regex fallback if Gemini returns nothing usable

    Args:
        hotel_name:  e.g. "Sandals Royal Caribbean"
        location:    e.g. "Montego Bay, Saint James, Jamaica"
        snippets:    existing snippets from Stage 2 search (general enrichment)
        country:     optional explicit country (from lead.country field)
        city:        optional city (for validation — reject addr == city)

    Returns:
        dict with optional 'address' and 'zip_code' keys, or None on total failure.
    """
    # ── STAGE 1: Country detection ───────────────────────────────────
    canonical_country = _detect_country(location, country)
    rules = _ADDRESS_RULES.get(canonical_country, _ADDRESS_RULES_DEFAULT_CARIBBEAN)
    country_label = canonical_country or "Generic / Unknown Caribbean"

    # ── STAGE 2: Three parallel searches ─────────────────────────────
    # 2a. General address query
    # 2b. "Located at" — catches resort websites, directory pages
    # 2c. Contact/directions — usually the official footer address page
    search_queries = [
        f'"{hotel_name}" {location} address',
        f'"{hotel_name}" "located at" OR "located on" OR "situated on"',
        f'"{hotel_name}" {location} contact OR directions OR "P.O. Box"',
    ]

    async def _safe_search(q: str) -> list[dict]:
        try:
            return await _search_web(q, max_results=5)
        except Exception as ex:
            logger.debug(f"Address search failed for query {q!r}: {ex}")
            return []

    search_batches = await _asyncio.gather(*[_safe_search(q) for q in search_queries])

    # ── STAGE 3: Merge + rank snippets ───────────────────────────────
    seen_urls = set()
    ranked_input: list[tuple[str, str]] = []
    for batch in search_batches:
        for r in batch:
            url = r.get("link") or ""
            if url in seen_urls:
                continue
            seen_urls.add(url)
            txt = f"{r.get('title', '')}. {r.get('snippet', '')}".strip()
            if txt and txt != ".":
                ranked_input.append((txt, url))

    # Also include snippets from the caller (Stage 2 enrichment) —
    # they have no URL so we treat them as "other"
    for s in snippets:
        if s and s.strip():
            ranked_input.append((s.strip(), ""))

    addr_snippets = _rank_snippets_by_trust(ranked_input)[:18]
    snippets_text = "\n".join(f"- {s}" for s in addr_snippets)

    if not addr_snippets:
        logger.warning(
            f"Address extraction [{hotel_name}]: no snippets to extract from"
        )
        return None

    # ── STAGE 4: Build country-specific prompt ───────────────────────
    examples_block = "\n".join(f"  {ex}" for ex in rules["examples"])
    rules_block = "\n".join(f"  - {r}" for r in rules["rules"])

    prompt = f"""Extract the street address of this specific hotel from the search snippets.

HOTEL:    {hotel_name}
LOCATION: {location}
COUNTRY:  {country_label}

POSTCODE FORMAT FOR THIS COUNTRY:
  {rules["postcode_desc"]}

EXAMPLES OF GOOD EXTRACTIONS FOR {country_label.upper()}:
{examples_block}

RULES FOR {country_label.upper()}:
{rules_block}

UNIVERSAL RULES:
  - Extract the PROPERTY's street address, NOT a head office / sales office address
  - If multiple candidate addresses appear, prefer the one on the hotel's own domain
    or on a trusted directory (Visit Jamaica, Cvent, TripAdvisor, Frommers)
  - DO NOT return the city name, the country name, or a parish/region as the address
  - DO NOT return a P.O. Box number alone — use the street/district associated with it
  - An incomplete-but-correct address (e.g. "Kent Avenue, Mahoe Bay") is BETTER than
    an empty response. Always return the most specific street/road/bay/beach you can find.
  - If the snippets truly contain no usable address, return {{"address": "", "zip_code": ""}}

SNIPPETS ({len(addr_snippets)} sources, trusted-domain snippets first):
{snippets_text}

Return JSON with keys: address, zip_code
"""

    schema = {
        "type": "object",
        "properties": {
            "address": {"type": "string"},
            "zip_code": {"type": "string"},
        },
    }

    # ── STAGE 5: Call Gemini ─────────────────────────────────────────
    raw_response = None
    try:
        resp = await _call_gemini(prompt, temperature=0.1, response_schema=schema)
        if resp:
            raw_response = resp
            parsed = json.loads(resp)
            if not isinstance(parsed, dict):
                parsed = {}
        else:
            parsed = {}
    except Exception as ex:
        logger.warning(f"Address extraction Gemini call failed for {hotel_name}: {ex}")
        parsed = {}

    # ── STAGE 6: Validate + normalize ────────────────────────────────
    validated = _validate_extraction(
        parsed,
        canonical_country,
        city or (location.split(",")[0].strip() if location else ""),
        hotel_name,
    )

    if validated.get("address"):
        logger.info(
            f"Address extraction [{hotel_name}] ({country_label}): "
            f"address={validated.get('address')!r}, zip={validated.get('zip_code', '')!r}"
        )
        return validated

    # ── STAGE 7: Regex fallback ──────────────────────────────────────
    logger.info(
        f"Address extraction [{hotel_name}]: Gemini returned no usable "
        f"address (raw={raw_response!r}) — trying regex fallback"
    )
    fallback = _regex_fallback_address(addr_snippets, canonical_country)
    if fallback.get("address"):
        # Validate fallback too
        fallback_validated = _validate_extraction(
            fallback,
            canonical_country,
            city or (location.split(",")[0].strip() if location else ""),
            hotel_name,
        )
        if fallback_validated.get("address"):
            logger.info(
                f"Address extraction [{hotel_name}] (regex fallback): "
                f"address={fallback_validated.get('address')!r}"
            )
            return fallback_validated

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
    # SPECIFICITY GUARD: don't overwrite a more-specific existing date with
    # a vaguer new extraction. Gemini has non-zero variance — one run can
    # return "2026-12-18" and the next "2026" for the same hotel + same web
    # data. Without this guard, Full Refresh silently degrades good data.
    #
    # Specificity ladder (higher = more specific):
    #   4 = full date   ("2026-12-18", "12/18/2026")
    #   3 = month+year  ("December 2026", "Dec 2026")
    #   2 = quarter     ("Q4 2026", "Spring 2026")
    #   1 = year only   ("2026")
    #   0 = empty/invalid
    #
    # Rule: if new_spec < current_spec, preserve current. Manual edits via
    # the UI always win because the user can Edit tab → save to force-set.
    if project_type in REOPENING_TYPES:
        reopening = (extraction.get("reopening_date") or "").strip()
        opening = (extraction.get("opening_date") or "").strip()
        effective_date = reopening or opening
        if effective_date and current_opening_date:
            new_spec = _date_specificity(effective_date)
            cur_spec = _date_specificity(current_opening_date)
            if new_spec < cur_spec:
                logger.info(
                    f"Smart Fill: preserving more-specific current "
                    f"opening_date {current_opening_date!r} (spec={cur_spec}) "
                    f"over new extraction {effective_date!r} (spec={new_spec})"
                )
                effective_date = current_opening_date
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
            # Same specificity guard for new-build / greenfield leads
            if current_opening_date:
                new_spec = _date_specificity(opening)
                cur_spec = _date_specificity(current_opening_date)
                if new_spec < cur_spec:
                    logger.info(
                        f"Smart Fill: preserving more-specific current "
                        f"opening_date {current_opening_date!r} (spec={cur_spec}) "
                        f"over new extraction {opening!r} (spec={new_spec})"
                    )
                    opening = current_opening_date
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
                country=lead.country or "",
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
