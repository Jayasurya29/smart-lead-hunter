"""
Gemini-based hotel tier classifier.
====================================
Uses Vertex AI Gemini 2.5 Flash via the modern google-genai SDK
(vertexai=True flag) to route through Vertex AI endpoint and consume
your $300 GCP free credits — NOT through AI Studio billing.

USAGE:
    from app.services.gemini_classifier import classify_unknowns
    results = classify_unknowns(unknown_hotels)
    # results: {source_id: (tier_db_name, confidence, reasoning)}

BILLING:
    With vertexai=True, calls hit the Vertex AI endpoint and bill
    against your GCP project credits. Verify in Cloud Console →
    Billing → Reports → filter by "Vertex AI API".

COST:
    Gemini 2.5 Flash: ~$0.30/1M input + $2.50/1M output tokens.
    A batch of 30 hotels costs ~$0.002. Full state of unknowns: <$0.20.
    All US + Caribbean unknowns: <$10.
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════
BATCH_SIZE = 20
MAX_RETRIES = 4
RATE_LIMIT_SLEEP = 8.0  # Vertex quotas are tight; ~15 req/min
DEFAULT_CONFIDENCE_THRESHOLD = 0.75

KEEP_TIERS = {
    "tier1_ultra_luxury",
    "tier2_luxury",
    "tier3_upper_upscale",
    "tier4_upscale",
}
SKIP_TIERS = {"tier5_skip", "tier5_budget", "unknown"}
VALID_TIERS = KEEP_TIERS | {"tier5_budget", "unknown"}

TIER_DB_TO_LABEL = {
    "tier1_ultra_luxury": "Ultra Luxury (Gemini)",
    "tier2_luxury": "Luxury (Gemini)",
    "tier3_upper_upscale": "Upper Upscale (Gemini)",
    "tier4_upscale": "Upscale (Gemini)",
}

TIER_DB_TO_NUM = {
    "tier1_ultra_luxury": 1,
    "tier2_luxury": 2,
    "tier3_upper_upscale": 3,
    "tier4_upscale": 4,
    "tier5_budget": 5,
    "unknown": 0,
}


PROMPT_TEMPLATE = """You are a hotel industry expert classifying properties for JA Uniforms, a uniform supplier targeting luxury and upscale hotels in the US and Caribbean.

For each hotel below, classify into ONE of these tiers based on STR (Smith Travel Research) brand-tier conventions and your knowledge of the property:

- tier1_ultra_luxury: Aman, Four Seasons, Belmond, Mandarin Oriental, Ritz-Carlton Reserve, The Setai, The Carlyle (NYC ONLY). ADR $600+
- tier2_luxury: Ritz-Carlton, St. Regis, Waldorf Astoria, Conrad, Park Hyatt, Fairmont, Edition, Auberge, W Hotels, Bulgari. ADR $350-600
- tier3_upper_upscale: JW Marriott, Westin, Sheraton, Hyatt Regency, InterContinental, Renaissance, Kimpton, Hotel Indigo, Andaz, Hard Rock Hotel. ADR $200-400
- tier4_upscale: Hilton, Marriott, DoubleTree, Embassy Suites, Crowne Plaza, Sonesta, Wyndham Grand, Margaritaville. ADR $130-250
- tier5_budget: Hampton Inn, Holiday Inn Express, Motel 6, Super 8, Days Inn, Best Western, Comfort Inn, Quality Inn, Howard Johnson, Days Hotel, TownePlace Suites, Chase Suite, Larkspur Landing, Heritage Inn. ADR under $130
- unknown: cannot determine — genuinely unfamiliar or ambiguous

CRITICAL ANTI-FALSE-POSITIVE RULES — read these before every classification:

1. DO NOT default to upscale just because a property is in San Francisco, NYC, or another major city. Most "Hotel ___" names in expensive cities are actually budget SROs, residential hotels, or economy properties.

2. The following property types are NEVER tier3 or tier4 — classify as tier5_budget or unknown:
   - Extended-stay properties (Larkspur Landing, TownePlace Suites, Chase Suite, Studios Inn, Residence Inn — except Marriott Residence Inn which is tier4)
   - Old SF residential hotels and SROs ("Bay Hotel", "Coast Hotel", "Post Hotel", "Hotel Embassy", "Carriage Inn", "Bijou", "Adante", "Andrews", "Cornell Hotel de France", "Hayes Valley Inn", "Taylor Hotel", "COVA")
   - Highway motels ("Marin Lodge", "Pacific Inn", "Corporate Inn", "Heritage Inn", "Marina Inn", "Park Pointe")
   - Howard Johnson, Days Hotel/Inn, Travelodge — ALWAYS tier5_budget regardless of city
   - "Hotel" + generic geographic word ("Bay Hotel", "Coast Hotel", "Marina Hotel") in a major city = almost always SRO/budget

3. The following are NOT hotels at all — classify as unknown:
   - Timeshares (anything with "Vacation Club", "WorldMark", "Wyndham Canterbury", "Marriott Vacation Club", "Nob Hill Inn" which is a timeshare)
   - Military or government lodging ("Navy Lodge", "NASA", "Exchange Lodge", "VOQ", "BOQ", "Air Force Inn")
   - Hostels, B&Bs with under 10 rooms, vacation rentals
   - Wedding venues, convention centers, restaurants with "Hotel" in the name historically

4. For unbranded independent properties, judge by name + location + your industry knowledge:
   - "Casa del Mar" in Santa Monica → tier2_luxury (known luxury beachfront)
   - "Shutters on the Beach" in Santa Monica → tier2_luxury
   - "Terranea Resort" in Rancho Palos Verdes → tier3_upper_upscale
   - "Petit Ermitage" in West Hollywood → tier3_upper_upscale (boutique)
   - "The Inn at Spanish Bay" Pebble Beach → tier2_luxury
   - "Sunset Motel" → tier5_budget
   - "Joe's Roadside Inn" rural → tier5_budget
   - Anything you genuinely don't recognize → unknown (NOT a guess at upper_upscale)

5. Same name in different cities is NOT the same property. "Carlyle Hotel" in Campbell CA is NOT The Carlyle in NYC. Always weight the city heavily.

CONFIDENCE CALIBRATION (strict):
- 0.90+: You actually know this exact property by name AND location
- 0.75-0.89: Strong name+location signal pointing to a known boutique or chain
- 0.65-0.74: Educated guess from name pattern only — DO NOT use this range for tier1/tier2
- Below 0.65 or "unknown": You're guessing. Just say unknown.

DO NOT GUESS UPWARD. False positives waste sales-team time. When in doubt, choose unknown or tier5_budget — never tier3.

Return ONLY a JSON array — no markdown fences, no preamble, no commentary:
[
  {"id": "abc123", "tier": "tier2_luxury", "confidence": 0.92, "reasoning": "known luxury beachfront"},
  {"id": "def456", "tier": "tier5_budget", "confidence": 0.95, "reasoning": "motel"},
  {"id": "xyz789", "tier": "unknown", "confidence": 0.4, "reasoning": "no information"}
]

Hotels to classify:
{hotels_block}
"""

# ════════════════════════════════════════════════════════════════
# VERTEX AI CLIENT (modern google-genai SDK)
# ════════════════════════════════════════════════════════════════
_client = None


def _get_client():
    """
    Returns a google-genai Client configured for Vertex AI.
    The vertexai=True flag routes calls through the Vertex AI endpoint
    so they bill against your GCP project credits ($300 free), NOT
    against AI Studio billing.
    """
    global _client
    if _client is not None:
        return _client

    try:
        from app.config import settings
    except ImportError as ex:
        raise RuntimeError("Cannot import app.config.settings") from ex

    # Set credentials env var if key file exists and isn't already set
    if settings.vertex_key_path and os.path.exists(settings.vertex_key_path):
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(
                settings.vertex_key_path
            )

    try:
        from google import genai
    except ImportError as ex:
        raise RuntimeError(
            "google-genai SDK not installed. Run: pip install google-genai"
        ) from ex

    _client = genai.Client(
        vertexai=True,  # ← Vertex AI endpoint, not AI Studio
        project=settings.vertex_project_id,
        location=settings.vertex_location,
    )
    logger.info(
        "Initialized google-genai (Vertex): model=%s project=%s location=%s",
        settings.gemini_model,
        settings.vertex_project_id,
        settings.vertex_location,
    )
    return _client


# ════════════════════════════════════════════════════════════════
# PROMPT BUILDER
# ════════════════════════════════════════════════════════════════
def _build_prompt(batch: List[dict]) -> str:
    lines = []
    for h in batch:
        sid = h.get("source_id", "")
        name = (h.get("name") or "").replace('"', "'")
        city = (h.get("city") or "").replace('"', "'")
        state = (h.get("state") or "").replace('"', "'")
        brand = (h.get("brand") or "").replace('"', "'")

        line = f'id={sid} | name="{name}" | city="{city}" | state="{state}"'
        if brand:
            line += f' | brand="{brand}"'
        lines.append(line)
    return PROMPT_TEMPLATE.replace("{hotels_block}", "\n".join(lines))


# ════════════════════════════════════════════════════════════════
# RESPONSE PARSER
# ════════════════════════════════════════════════════════════════
def _parse_response(text: str) -> List[dict]:
    """Strip markdown fences if present and parse JSON array."""
    text = text.strip()
    if text.startswith("```"):
        text = text.lstrip("`")
        if text.lower().startswith("json"):
            text = text[4:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    return json.loads(text)


# ════════════════════════════════════════════════════════════════
# BATCH CLASSIFICATION
# ════════════════════════════════════════════════════════════════
def _classify_batch(batch: List[dict]) -> Dict[str, Tuple[str, float, str]]:
    """
    Send one batch to Gemini via Vertex.
    Returns: {source_id: (tier_db, confidence, reasoning)}
    """
    if not batch:
        return {}

    client = _get_client()
    prompt = _build_prompt(batch)

    # Lazy import types to avoid module-level failure if SDK missing
    from google.genai import types
    from app.config import settings

    config = types.GenerateContentConfig(
        temperature=0.1,
        response_mime_type="application/json",
        max_output_tokens=8192,
    )

    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=settings.gemini_model_lite,  # gemini-2.5-flash-lite (higher quota, perfect for classification)
                contents=prompt,
                config=config,
            )
            text = response.text or ""

            # Diagnostic: when text is empty, dump candidate metadata
            if not text.strip():
                finish_reason = "unknown"
                safety = "n/a"
                try:
                    if response.candidates:
                        cand = response.candidates[0]
                        finish_reason = str(getattr(cand, "finish_reason", "?"))
                        safety = str(getattr(cand, "safety_ratings", "?"))[:200]
                except Exception:
                    pass
                logger.warning(
                    "Empty response from Gemini. finish_reason=%s safety=%s",
                    finish_reason,
                    safety,
                )
                raise ValueError(f"Empty response (finish={finish_reason})")

            results = _parse_response(text)

            classifications: Dict[str, Tuple[str, float, str]] = {}
            for entry in results:
                if not isinstance(entry, dict):
                    continue
                sid = str(entry.get("id", "")).strip()
                tier = str(entry.get("tier", "")).strip()
                try:
                    conf = float(entry.get("confidence", 0))
                except (TypeError, ValueError):
                    conf = 0.0
                reasoning = str(entry.get("reasoning", "")).strip()[:200]

                if sid and tier in VALID_TIERS:
                    classifications[sid] = (tier, conf, reasoning)

            return classifications

        except json.JSONDecodeError as ex:
            last_error = ex
            logger.warning(
                "Gemini batch %d/%d: JSON parse error (%s), retrying",
                attempt,
                MAX_RETRIES,
                ex,
            )
        except Exception as ex:
            last_error = ex
            err_str = str(ex)
            # Detect rate-limit errors and back off much longer
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                wait = 60  # Vertex quotas refill on minute boundaries
                logger.warning(
                    "Gemini batch %d/%d hit rate limit, waiting %ds...",
                    attempt,
                    MAX_RETRIES,
                    wait,
                )
                time.sleep(wait)
                continue
            logger.warning(
                "Gemini batch %d/%d failed: %s: %s",
                attempt,
                MAX_RETRIES,
                type(ex).__name__,
                ex,
            )

        if attempt < MAX_RETRIES:
            time.sleep(2**attempt)

    logger.error(
        "Gemini batch failed after %d attempts: %s",
        MAX_RETRIES,
        last_error,
    )
    return {}


# ════════════════════════════════════════════════════════════════
# PUBLIC API
# ════════════════════════════════════════════════════════════════
def classify_unknowns(
    hotels: List[dict],
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
) -> Dict[str, Tuple[str, float, str]]:
    """
    ...
    """
    if not hotels:
        return {}

    # Dedupe by normalized name — classify each unique name once, fan out results.
    # "Holiday Inn Express Foster City" and "Holiday Inn Express San Mateo" are
    # the same classification problem; no point spending credits twice.
    def _norm(h: dict) -> str:
        import re

        name = (h.get("name") or "").lower()
        # Strip city/location suffixes after dash, comma, "at", "by"
        name = re.split(r"\s+[-–—]\s+|,| at | by ", name)[0]
        # Collapse whitespace and strip non-alphanumerics
        name = re.sub(r"[^a-z0-9 ]", "", name)
        return re.sub(r"\s+", " ", name).strip()

    groups: Dict[str, List[dict]] = {}
    for h in hotels:
        groups.setdefault(_norm(h), []).append(h)

    representatives = [grp[0] for grp in groups.values()]
    print(
        f"      → Deduped {len(hotels)} unknowns to {len(representatives)} unique names"
    )
    print(f"      → Classifying {len(representatives)} unknowns with Gemini Flash...")

    # Run classification on representatives only, then fan out to all members
    rep_hotels = representatives
    num_batches = (len(rep_hotels) + BATCH_SIZE - 1) // BATCH_SIZE

    raw_results: Dict[str, Tuple[str, float, str]] = {}

    for i in range(0, len(rep_hotels), BATCH_SIZE):
        batch = rep_hotels[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(
            f"        batch {batch_num}/{num_batches} ({len(batch)} hotels)... ",
            end="",
            flush=True,
        )

        results = _classify_batch(batch)
        raw_results.update(results)

        kept_in_batch = sum(
            1
            for tier, conf, _ in results.values()
            if tier in KEEP_TIERS and conf >= confidence_threshold
        )
        print(f"got {len(results)} responses, {kept_in_batch} kept as 4★+")

        if batch_num < num_batches:
            time.sleep(RATE_LIMIT_SLEEP)

    # Filter to keepers only, then fan results from each representative
    # back to every duplicate that shared its normalized name.
    filtered: Dict[str, Tuple[str, float, str]] = {}
    counts = {"kept": 0, "low_conf": 0, "budget": 0, "unknown": 0}

    for rep in representatives:
        rep_sid = rep.get("source_id", "")
        if rep_sid not in raw_results:
            continue
        tier, conf, reasoning = raw_results[rep_sid]

        # All hotels (including the rep itself) that share this normalized name
        group_members = groups[_norm(rep)]

        if tier in KEEP_TIERS:
            if conf >= confidence_threshold:
                for member in group_members:
                    filtered[member["source_id"]] = (tier, conf, reasoning)
                    counts["kept"] += 1
            else:
                counts["low_conf"] += len(group_members)
        elif tier in ("tier5_budget", "tier5_skip"):
            counts["budget"] += len(group_members)
        else:
            counts["unknown"] += len(group_members)

    print("      Gemini results:")
    print(f"        kept (≥{confidence_threshold}):       {counts['kept']}")
    print(f"        low confidence:        {counts['low_conf']}")
    print(f"        classified as budget:  {counts['budget']}")
    print(f"        still unknown:         {counts['unknown']}")

    return filtered


# ════════════════════════════════════════════════════════════════
# CLI smoke test
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    test_hotels = [
        {
            "source_id": "1",
            "name": "Casa del Mar",
            "city": "Santa Monica",
            "state": "CA",
            "brand": "",
        },
        {
            "source_id": "2",
            "name": "Sunset Motel",
            "city": "Bakersfield",
            "state": "CA",
            "brand": "",
        },
        {
            "source_id": "3",
            "name": "Terranea Resort",
            "city": "Rancho Palos Verdes",
            "state": "CA",
            "brand": "",
        },
        {
            "source_id": "4",
            "name": "The Inn at Spanish Bay",
            "city": "Pebble Beach",
            "state": "CA",
            "brand": "",
        },
        {
            "source_id": "5",
            "name": "Joe's Highway Lodge",
            "city": "Bakersfield",
            "state": "CA",
            "brand": "",
        },
        {
            "source_id": "6",
            "name": "Petit Ermitage",
            "city": "West Hollywood",
            "state": "CA",
            "brand": "",
        },
    ]

    print("Smoke test with 6 known hotels:\n")
    results = classify_unknowns(test_hotels)

    print("\nFinal kept results:")
    for sid, (tier, conf, reasoning) in results.items():
        h = next(h for h in test_hotels if h["source_id"] == sid)
        print(f"  {h['name']:35} → {tier:20} conf={conf:.2f}  ({reasoning})")
