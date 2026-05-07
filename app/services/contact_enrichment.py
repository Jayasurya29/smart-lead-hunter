"""
SMART LEAD HUNTER — Contact Enrichment Service v4.2
=====================================================
Multi-layer contact discovery with SAP-trained intelligence.

Layer 0: Google Search via Serper.dev (finds LinkedIn posts, press releases)
Layer 1: Web Scrape + Gemini AI Extract (scrape articles from search results)
Layer 2: LinkedIn Snippet Extraction (names from search snippets)
Layer 3: Gemini Verification (validates and scores all contacts)
Fallback: DuckDuckGo (free, unlimited) when Serper unavailable

KEY v4.2 CHANGES:
- TIGHTENED _title_proves_hotel: now requires CONTIGUOUS hotel-name match
  (literal phrase or distinctive bigram) instead of scattered word-overlap.
  Word-bag overlap was generating false positives on short hotel names
  (e.g. "Pan Am Hotel" was matching any text containing "pan" + "am" + "hotel"
  scattered anywhere — promoted Iqbal Mallik (Magnuson Grand) as the
  Pan Am Hotel GM and made him immune to Gemini's correct rejection).
- Protection now ALSO requires the contact's name to look like a real person
  (≥2 words, not all-caps, no org words). Catches "Travel Turtle Magazine"
  type publications that were protected by the old rule.

KEY v4.1 CHANGES:
- Google search via Serper.dev (finds Kara DePool that DDG misses)
- DDG as free fallback when SERPER_API_KEY not set
- SAP-trained title classifier (780 titles → 7 buyer tiers)
- Contact validator with name-collision detection (Nora Hotel fix)
- Smart query builder with parent company fallback
- Auto-retry when all contacts are false positives
"""

import asyncio
import json
import logging
import os
import random
import re
from datetime import date
from typing import Optional
from app.services.gemini_client import get_gemini_url, get_gemini_headers
from app.services.ai_client import is_vertex_ai

import httpx
from dotenv import load_dotenv

from app.config.enrichment_config import (
    BRAND_TO_PARENT,
    CONTACT_SEARCH_PRIORITIES,
    ENRICHMENT_SETTINGS,
    HOSPITALITY_NEWS_DOMAINS,
    get_enrichment_gemini_model,
)
from app.config.sap_title_classifier import title_classifier, BuyerTier
from app.config.brand_registry import BrandRegistry
from app.config.project_type_intelligence import classify_project_type
from app.services.contact_validator import (
    contact_validator,
    query_builder,
    is_corporate_title,
    is_irrelevant_org,
)
from app.services.utils import local_now

load_dotenv()

logger = logging.getLogger(__name__)

MAX_CONTACTS_TO_SAVE = 8

# ── GROUNDED CONTACT FAST-PATH ──
# Single Gemini googleSearch call asks "who makes uniform decisions here?"
# instead of running 6-iteration Serper+Gemini pipeline. Uses us-central1
# regional endpoint (same as lead_data_enrichment.py grounding path).
_CONTACT_GROUNDING_TIMEOUT_S = 45.0
_CONTACT_GROUNDING_MIN_CONTACTS = 2  # below this, fall back to iterative pipeline

# Smart-distribution targets: desired mix of contacts in the final cap.
# Fills in priority order — any unfilled slots get backfilled with the
# best remaining contacts regardless of category. This ensures the
# sales team always sees an owner/check-writer at the top if available,
# plus a balanced mix of operator execs.
# Total = 8 slots (= MAX_CONTACTS_TO_SAVE):
#   2 owner slots so we can keep both the owner principal AND the
#     developer principal (frequently distinct people, e.g. Vista
#     Development partners + Alpha Tech CEO for Andaz T&C)
#   3 P1 operator slots so we can keep on-property GM + Dir Ops + the
#     brand regional decision-maker (e.g. Hyatt SVP Development LatAm)
#   2 P2 operator slots for property dept heads / regional VPs
#   1 backfill for whatever's left (sales/F&B/etc. — the Trevor pattern)
_SMART_CAP_TARGETS = {
    "owner": 2,  # Owner principal + developer principal
    "p1_operator": 3,  # Top 3 P1 at management_corporate / hotel_specific
    "p2_operator": 2,  # Top 2 P2 at management_corporate / hotel_specific
    "backfill": 1,  # Best remaining (any scope/priority)
}


# ═══════════════════════════════════════════════════════════════
# LINKEDIN URL CANONICALIZATION
# ═══════════════════════════════════════════════════════════════
# LinkedIn serves the same profile under country-specific subdomains
# based on where the profile owner has set their location:
#   https://tc.linkedin.com/in/adelphine-pitter-...   (Turks & Caicos)
#   https://za.linkedin.com/in/stephen-meredith-...   (South Africa)
#   https://uk.linkedin.com/in/...                    (UK), de., fr., etc.
# These all redirect to the canonical https://www.linkedin.com/in/...
# but stored as-is they look unprofessional in the dashboard and may
# trip up downstream tools (Wiza expects www.). Canonicalize at every
# write site so the DB only ever holds www.linkedin.com URLs.
_LINKEDIN_SUBDOMAIN_RE = re.compile(
    r"^https?://([a-z]{2,3})\.linkedin\.com/", re.IGNORECASE
)


def _canonicalize_linkedin_url(url: Optional[str]) -> Optional[str]:
    """Normalize country subdomains (tc., za., uk., de., fr., etc.) to www.
    Returns the URL unchanged if already canonical or not a LinkedIn URL.
    Returns None / empty for None / empty input."""
    if not url:
        return url
    s = url.strip()
    if not s:
        return s
    m = _LINKEDIN_SUBDOMAIN_RE.match(s)
    if not m:
        return s
    sub = m.group(1).lower()
    if sub == "www":
        return s
    # Replace the matched [a-z]{2,3} subdomain with www
    return _LINKEDIN_SUBDOMAIN_RE.sub("https://www.linkedin.com/", s, count=1)


# ═══════════════════════════════════════════════════════════════
# SHARED HTTP CLIENT — connection pooling for Serper/Gemini
# Reuses TCP connections across calls (30-50% faster enrichment)
# ═══════════════════════════════════════════════════════════════

_shared_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    """Get or create the shared httpx client with connection pooling."""
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(
            timeout=30,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=True,
        )
    return _shared_client


# ═══════════════════════════════════════════════════════════════
# GEMINI API CALLER WITH RETRY (H-08)
# Retries on 429/503/500 with exponential backoff
# ═══════════════════════════════════════════════════════════════

_GEMINI_RETRY_ATTEMPTS = (
    6  # Increased from 3. Vertex AI Gemini uses Dynamic Shared Quota
    # (per Google's official doc: "If you receive a resource exhausted
    # (429) error, it means the shared pool is temporarily experiencing
    # high demand from many users at once. You should implement retry
    # mechanisms in your application, as availability can change
    # quickly."). DSQ bursts typically clear in 30-60 seconds, so
    # patient retry is the correct response.
)
_GEMINI_RETRY_JITTER = (
    0.5  # ±50% randomness on each backoff to de-sync parallel callers
)
_GEMINI_RETRY_MAX_DELAY = (
    30  # seconds — cap on any single retry delay. Bumped from 8s to 30s
    # because DSQ throttles can take ~30 sec to clear during demand
    # spikes. With 6 attempts capped at 30s and exponential backoff,
    # total wait reaches ~90 sec. Worth the wait — alternative is
    # falling back to title-only priority which produces worse output.
)


def _try_recover_json(text: str):
    """Attempt to recover usable JSON from a slightly malformed Gemini response.

    Tries (in order):
      1. Strip trailing garbage after the last balanced brace/bracket
      2. Use json5 if available (more lenient — accepts trailing commas,
         single-quoted strings, unquoted keys)
      3. Extract the largest JSON array via regex and parse just that
      4. Per-item parsing for arrays — keep the items that parse cleanly
      5. TRUNCATION RECOVERY — for responses where Gemini was cut off
         mid-object, walk object-by-object and keep all complete ones
    """
    import json as _json
    import re as _re

    if not text:
        return None
    text = text.strip()

    # ── Strategy 1: trim trailing junk past the last balanced close ──
    for end_char in ("]", "}"):
        last = text.rfind(end_char)
        if last > 0:
            try:
                return _json.loads(text[: last + 1])
            except _json.JSONDecodeError:
                pass

    # ── Strategy 2: try json5 (handles trailing commas, comments, etc) ──
    try:
        import json5  # type: ignore

        try:
            return json5.loads(text)
        except Exception:
            pass
    except ImportError:
        pass  # json5 not installed — skip

    # ── Strategy 3: extract largest JSON array via regex ──
    array_match = _re.search(r"\[\s*\{.*\}\s*\]", text, flags=_re.DOTALL)
    if array_match:
        try:
            return _json.loads(array_match.group(0))
        except _json.JSONDecodeError:
            # ── Strategy 4: split into individual { ... } blocks, parse each ──
            arr_text = array_match.group(0)
            object_pattern = _re.compile(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", _re.DOTALL)
            recovered = []
            for obj_match in object_pattern.finditer(arr_text):
                try:
                    recovered.append(_json.loads(obj_match.group(0)))
                except _json.JSONDecodeError:
                    continue
            if recovered:
                return recovered

    # ── Strategy 5: TRUNCATION RECOVERY ──
    # Gemini response was cut off mid-object (common when it hits the
    # token limit while generating an array of contacts). Walk the text
    # and extract every complete balanced {...} block we can find.
    recovered_objects = []
    # Find start of what looks like a JSON array of objects
    start = text.find("[")
    if start < 0:
        start = text.find('{"')
    if start < 0:
        return None

    depth = 0
    in_string = False
    escape_next = False
    obj_start = -1
    working = text[start:]

    for i, ch in enumerate(working):
        if escape_next:
            escape_next = False
            continue
        if ch == "\\":
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue

        if ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and obj_start >= 0:
                candidate = working[obj_start : i + 1]
                try:
                    parsed = _json.loads(candidate)
                    if isinstance(parsed, dict):
                        recovered_objects.append(parsed)
                except _json.JSONDecodeError:
                    pass
                obj_start = -1

    if recovered_objects:
        return recovered_objects  # caller wraps lists into {"contacts": [...]}

    return None


def _count_items(data) -> int:
    """How many items are in a recovered JSON payload (for logging)."""
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        for k in ("contacts", "items", "results"):
            if k in data and isinstance(data[k], list):
                return len(data[k])
        return 1
    return 0


_GEMINI_RETRY_BASE_DELAY = 2  # seconds

# Global concurrency cap for in-flight Gemini calls across the whole app.
# Without this, two parallel enrichments (different leads, different sales
# employees) can stack 6+ concurrent Gemini calls, immediately hammer
# Vertex AI's per-project RPM limit, get 429'd, and BOTH enrichments slow
# to a crawl on retries. The semaphore caps in-flight calls at 5 — beyond
# that, callers wait their turn. Trade: slightly slower individual
# enrichment, dramatically fewer 429s under multi-user load.
#
# 5 was chosen because:
# - Vertex Tier 1 is ~1,000 RPM = ~16 requests/sec sustained
# - 5 in-flight calls × 2-3s avg latency = ~2 calls/sec per slot = 10 calls/sec total
# - That leaves headroom for Smart Fill bursts + Discovery on top
# Bump up if you get a quota increase; bump down if 429s persist.
_GEMINI_CONCURRENCY = 5
_gemini_semaphore: Optional[asyncio.Semaphore] = None


def _get_gemini_semaphore() -> asyncio.Semaphore:
    """Lazy-init the semaphore so it binds to the running event loop.

    Module-level Semaphore() construction binds to whatever loop is
    current at import time, which can differ from the loop that handles
    the actual request. Lazy init avoids the cross-loop bug.
    """
    global _gemini_semaphore
    if _gemini_semaphore is None:
        _gemini_semaphore = asyncio.Semaphore(_GEMINI_CONCURRENCY)
    return _gemini_semaphore


def _retry_delay_with_jitter(attempt: int) -> float:
    """Exponential backoff with ±50% randomness to de-sync parallel retries.

    Without jitter, two enrichments hitting 429 at the same instant both
    sleep 2s, both retry at the same instant, both 429 again. With jitter,
    one might sleep 1.4s and the other 2.6s — they spread out across
    Vertex's RPM window and one succeeds.

    Capped at _GEMINI_RETRY_MAX_DELAY so a string of failures doesn't
    waste a minute+ of clock time per call.
    """
    base = _GEMINI_RETRY_BASE_DELAY * (2**attempt)
    jitter = base * _GEMINI_RETRY_JITTER * (2 * random.random() - 1)  # [-0.5x, +0.5x]
    return max(0.5, min(_GEMINI_RETRY_MAX_DELAY, base + jitter))


async def _call_gemini_single(
    prompt: str,
    model: Optional[str] = None,
    timeout: int = 90,
    response_schema: Optional[dict] = None,
    max_output_tokens: int = 16384,
    endpoint: Optional[str] = None,
) -> Optional[dict]:
    """Call Gemini API with retry and exponential backoff (SINGLE model only).

    Wrapped by `_call_gemini` which adds multi-model + multi-endpoint fallback.
    Most callers should use that — only use this directly if you specifically
    want to bypass the fallback chain.

    Returns parsed JSON response or None on failure.
    Retries on 429 (rate limit), 500, 503 (server errors).

    Args:
        prompt: The user prompt.
        model: Model name; defaults to enrichment model.
        timeout: Request timeout in seconds.
        response_schema: Optional OpenAPI-3.0 subset JSON schema.
        max_output_tokens: Output token budget (default 16384).
        endpoint: Vertex AI endpoint location override. When set, builds the
            URL directly for that region (e.g. "us-central1", "us-east4",
            "us-west1") instead of using the configured VERTEX_LOCATION.
            "global" uses the global endpoint (aiplatform.googleapis.com).
            None = use configured default (VERTEX_LOCATION env var).
    """
    if not model:
        model = get_enrichment_gemini_model()

    # Build URL — either for a specific endpoint or the configured default
    if endpoint is not None:
        from app.services.ai_client import _get_config, _ensure_init

        _ensure_init()
        config = _get_config()
        project = config["vertex_project_id"]
        if endpoint == "global":
            host = "aiplatform.googleapis.com"
        else:
            host = f"{endpoint}-aiplatform.googleapis.com"
        url = (
            f"https://{host}/v1/"
            f"projects/{project}/locations/{endpoint}/"
            f"publishers/google/models/{model}:generateContent"
        )
    else:
        url = get_gemini_url(model)
    generation_config: dict = {
        "temperature": 0.1,
        "maxOutputTokens": max_output_tokens,
    }

    # ── THINKING CONFIG ──
    # Gemini 2.5+ models include "thinking" — internal reasoning before
    # the actual response. Default thinking budget can take 30-60 seconds
    # for complex prompts, which causes our 30s timeout to fire and kill
    # the call mid-flight (the empty-error ReadTimeouts we kept seeing).
    #
    # For enrichment we don't need deep reasoning — most calls are simple
    # extraction ("find names in this text") or scoring. Setting minimum
    # thinking gives us same quality with ~5x faster response time.
    #
    # Gemini 3 uses `thinking_level` (minimal/low/medium/high)
    # Gemini 2.5 uses `thinking_budget` (integer token count, 0 disables)
    if "gemini-3" in model.lower():
        generation_config["thinking_config"] = {"thinking_level": "minimal"}
    elif "gemini-2.5" in model.lower():
        generation_config["thinking_config"] = {"thinking_budget": 0}
    # Older models (2.0, 1.5) have no thinking config — leave it off

    # ── STRUCTURED OUTPUTS (Vertex AI / Gemini 2.x) ──
    # When a schema is supplied and we're on Vertex, force Gemini to return
    # well-formed JSON matching the schema. This eliminates the malformed-JSON
    # recovery path that was silently dropping contacts (Bug #1, 2026-04-22).
    schema_active = False
    if response_schema is not None:
        try:
            if is_vertex_ai():
                generation_config["responseMimeType"] = "application/json"
                generation_config["responseSchema"] = response_schema
                schema_active = True
        except Exception as ex:
            # Never let schema attachment block the call — fall back to prompt-only
            logger.debug(f"Could not attach responseSchema (continuing without): {ex}")

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": generation_config,
    }

    client = _get_client()
    last_error = None

    # Global concurrency cap — only N Gemini calls in-flight across the app
    sem = _get_gemini_semaphore()

    for attempt in range(_GEMINI_RETRY_ATTEMPTS):
        try:
            async with sem:
                resp = await client.post(
                    url, json=payload, headers=get_gemini_headers(), timeout=timeout
                )

            if resp.status_code == 200:
                data = resp.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    feedback = data.get("promptFeedback", {})
                    logger.warning(
                        f"Gemini returned no candidates. Feedback: {feedback}"
                    )
                    return None
                try:
                    text = candidates[0]["content"]["parts"][0]["text"]
                except (KeyError, IndexError) as e:
                    logger.error(f"Unexpected Gemini response structure: {e}")
                    return None
                # finishReason surfaces truncation — log it if it's not STOP
                finish_reason = candidates[0].get("finishReason", "")
                text = re.sub(r"```json\s*", "", text)
                text = re.sub(r"```\s*", "", text)
                text = text.strip()
                try:
                    parsed = json.loads(text)
                    if finish_reason and finish_reason not in (
                        "STOP",
                        "FINISH_REASON_UNSPECIFIED",
                    ):
                        logger.warning(
                            f"Gemini finished with {finish_reason!r} "
                            f"(schema_active={schema_active}, "
                            f"max_output_tokens={max_output_tokens}, "
                            f"text_len={len(text)}) — output may be truncated"
                        )
                    return parsed
                except json.JSONDecodeError as e:
                    # Gemini occasionally returns slightly malformed JSON
                    # (missing comma, unescaped quote, trailing comma, control
                    # char in string, or a response truncated mid-object because
                    # "thinking" tokens ate the output budget). Try recovery
                    # strategies before giving up — losing a batch of contacts
                    # because of one stray comma is too costly.
                    #
                    # NOTE: when response_schema is supplied on Vertex, this
                    # path should almost never fire. If it does, something
                    # deeper is wrong (schema mismatch, provider regression).
                    logger.warning(
                        f"Gemini JSON parse error: {e} "
                        f"(schema_active={schema_active}, "
                        f"finish_reason={finish_reason!r}, "
                        f"text_len={len(text)}). Attempting recovery..."
                    )
                    recovered = _try_recover_json(text)
                    if recovered is not None:
                        # If recovery returned a bare list, wrap it so callers
                        # that expect {"contacts": [...]} don't crash with
                        # "'list' object has no attribute 'get'".
                        if isinstance(recovered, list):
                            recovered = {"contacts": recovered}
                        recovered_count = _count_items(recovered)
                        # Flag suspicious low counts — 3 from a >5K-char
                        # response usually means truncation or cascading
                        # per-item parse failures. This is the signal
                        # Bug #1 was silently missing.
                        if recovered_count < 3 and len(text) > 5000:
                            logger.warning(
                                f"JSON recovery yielded only {recovered_count} "
                                f"items from {len(text)}-char response — likely "
                                f"truncation. Consider raising max_output_tokens "
                                f"or enabling response_schema."
                            )
                        else:
                            logger.info(
                                f"JSON recovery succeeded "
                                f"(extracted {recovered_count} items from "
                                f"{len(text)}-char response)"
                            )
                        return recovered
                    logger.error(
                        f"Gemini response unrecoverable "
                        f"(text_len={len(text)}). First 300 chars: {text[:300]!r}"
                    )
                    return None

            elif resp.status_code in (429, 500, 503):
                # Google's Vertex AI returns a Retry-After header on 429
                # indicating exactly when the throttle clears. Use it
                # when present — beats our exponential backoff guess.
                retry_after = resp.headers.get("Retry-After") or resp.headers.get(
                    "retry-after"
                )
                if retry_after:
                    try:
                        # Header can be seconds (int) or HTTP date.
                        # Vertex AI sends seconds.
                        delay = float(retry_after)
                        # Cap to MAX_DELAY anyway, in case Google asks
                        # for a 5-minute wait — we'd rather just give up
                        # and fall back than block enrichment for that long.
                        delay = min(delay, _GEMINI_RETRY_MAX_DELAY)
                        logger.warning(
                            f"Gemini {resp.status_code} (attempt {attempt + 1}/{_GEMINI_RETRY_ATTEMPTS}), "
                            f"server requested retry in {delay:.1f}s (Retry-After header)..."
                        )
                    except (ValueError, TypeError):
                        delay = _retry_delay_with_jitter(attempt)
                        logger.warning(
                            f"Gemini {resp.status_code} (attempt {attempt + 1}/{_GEMINI_RETRY_ATTEMPTS}), "
                            f"retrying in {delay:.1f}s..."
                        )
                else:
                    delay = _retry_delay_with_jitter(attempt)
                    logger.warning(
                        f"Gemini {resp.status_code} (attempt {attempt + 1}/{_GEMINI_RETRY_ATTEMPTS}), "
                        f"retrying in {delay:.1f}s..."
                    )
                await asyncio.sleep(delay)
                last_error = f"HTTP {resp.status_code}"
            else:
                logger.error(f"Gemini API error: {resp.status_code}")
                return None

        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Gemini response parse error: {e}")
            return None
        except Exception as e:
            err_type = type(e).__name__
            err_repr = repr(e) or f"{err_type}(no details)"

            # ── EARLY BAILOUT FOR TIMEOUT ERRORS ──
            # ReadTimeout / ConnectTimeout mean Gemini received the request
            # but didn't respond in time — the model's DSQ pool is overloaded.
            # Retrying the SAME model 6 times wastes 7+ minutes (90s × 6).
            # After 2 consecutive timeouts, bail out immediately so the
            # multi-model fallback chain in _call_gemini() can try a
            # different model with its own fresh DSQ pool.
            is_timeout = err_type in (
                "ReadTimeout",
                "ConnectTimeout",
                "PoolTimeout",
                "TimeoutException",
            )
            if is_timeout and attempt >= 1:
                logger.warning(
                    f"Gemini [{err_type}] on attempt {attempt + 1} — "
                    f"bailing out to try fallback model (same model keeps timing out)"
                )
                last_error = err_repr
                break  # exit retry loop → _call_gemini() tries next model

            delay = _retry_delay_with_jitter(attempt)
            logger.warning(
                f"Gemini call failed (attempt {attempt + 1}/{_GEMINI_RETRY_ATTEMPTS}): "
                f"[{err_type}] {err_repr}, retrying in {delay:.1f}s..."
            )
            await asyncio.sleep(delay)
            last_error = err_repr

    logger.error(
        f"Gemini [{model}] failed after {attempt + 1} attempt(s) "
        f"(max={_GEMINI_RETRY_ATTEMPTS}): {last_error}"
    )
    return None


# ═══════════════════════════════════════════════════════════════
# MULTI-MODEL FALLBACK CHAIN
# ═══════════════════════════════════════════════════════════════
#
# When the primary model's DSQ pool is saturated, we exhaust 6 retries
# (~90s wasted) before falling back to title-only priority — at the cost
# of brief quality. Instead, when retries fail with 429, we automatically
# try a different model's pool. Per Google's DSQ docs:
#
#   "The throughput limit shown for a model family applies independently
#   to each model within that family. Usage against one of these limits
#   doesn't impact the throughput for other models."
#
# So if gemini-2.5-flash is throttled, gemini-2.5-flash-lite (separate
# pool) likely succeeds immediately. Quality drop is minor; reliability
# gain is huge.
#
# Order matters — best model first, lighter fallbacks after.

# ── FALLBACK CHAIN: (model, endpoint) tuples ──
# Verified 2026-05-06 via scripts/test_endpoints.py + scripts/test_endpoints2.py
#
# Working combinations:
#   gemini-2.5-flash      global ✅  us-central1 ✅  us-east4 ✅  us-west1 ✅
#   gemini-3-flash-preview global ✅  us-central1 ❌  us-east4 ❌  us-west1 ❌
#   gemini-2.5-flash-lite  global ✅  us-central1 ✅  us-east4 ✅  us-west1 ✅
#
# Strategy: exhaust all ENDPOINTS for the primary model before downgrading
# quality. Each region is separate physical infrastructure + separate DSQ pool.
# None in model position = resolved at call time to env-configured primary.
_GEMINI_FALLBACK_CHAIN: list[tuple] = [
    # ── Primary model (gemini-2.5-flash) — 4 endpoints ──
    (None, "global"),  # 1. primary default
    (None, "us-central1"),  # 2. regional — proven reliable
    (None, "us-east4"),  # 3. east coast region
    (None, "us-west1"),  # 4. west coast region
    # ── gemini-3-flash-preview — global only ──
    ("gemini-3-flash-preview", "global"),  # 5. different model family/pool
    # ── gemini-2.5-flash-lite — 4 endpoints, last resort ──
    ("gemini-2.5-flash-lite", "global"),  # 6.
    ("gemini-2.5-flash-lite", "us-central1"),  # 7.
    ("gemini-2.5-flash-lite", "us-east4"),  # 8.
    ("gemini-2.5-flash-lite", "us-west1"),  # 9. absolute last resort
]


async def _call_gemini(
    prompt: str,
    model: Optional[str] = None,
    timeout: int = 90,
    response_schema: Optional[dict] = None,
    max_output_tokens: int = 16384,
) -> Optional[dict]:
    """Multi-model + multi-endpoint fallback wrapper.

    Iterates through _GEMINI_FALLBACK_CHAIN — a list of (model, endpoint)
    tuples verified 2026-05-06. Each tuple is a genuinely separate DSQ pool:
    - Different models → different model family quota
    - Different endpoints → different regional infrastructure

    Strategy: exhaust all endpoints for primary model first (same quality),
    then try fallback models. This keeps output quality high for as long as
    possible before degrading to the lite model.

    If caller specifies model= explicitly, honor that — single model, no chain.
    """
    # Caller pinned a specific model — single attempt, no fallback
    if model is not None:
        return await _call_gemini_single(
            prompt,
            model=model,
            timeout=timeout,
            response_schema=response_schema,
            max_output_tokens=max_output_tokens,
        )

    primary_model = get_enrichment_gemini_model()

    # Resolve None placeholders to primary model, skip exact duplicates
    chain: list[tuple[str, str]] = []
    seen = set()
    for m, ep in _GEMINI_FALLBACK_CHAIN:
        resolved_model = m if m else primary_model
        key = (resolved_model, ep)
        if key not in seen:
            seen.add(key)
            chain.append((resolved_model, ep))

    last_label = None
    for idx, (attempt_model, attempt_endpoint) in enumerate(chain):
        is_fallback = idx > 0
        label = f"{attempt_model} @ {attempt_endpoint}"

        if is_fallback:
            logger.warning(
                f"[FALLBACK #{idx}] Trying {label} " f"(prev failed: {last_label})"
            )

        result = await _call_gemini_single(
            prompt,
            model=attempt_model,
            timeout=timeout,
            response_schema=response_schema,
            max_output_tokens=max_output_tokens,
            endpoint=attempt_endpoint,
        )

        if result is not None:
            if is_fallback:
                logger.info(f"[FALLBACK] ✓ Recovered via {label}")
            return result

        last_label = label

    logger.error(
        f"All {len(chain)} fallback combinations exhausted "
        f"(last: {last_label}). Returning None."
    )
    return None


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT RESULT
# ═══════════════════════════════════════════════════════════════


class EnrichmentResult:
    """Holds all data found during enrichment."""

    def __init__(self):
        self.contacts: list[dict] = []
        self.fallback_contacts: list[
            dict
        ] = []  # rescued rejects for zero-contact cases
        self.management_company: Optional[str] = None
        self.developer: Optional[str] = None
        self.opening_update: Optional[str] = None
        self.additional_details: Optional[str] = None
        self.sources_used: list[str] = []
        self.layers_tried: list[str] = []
        self.errors: list[str] = []
        self.metadata: dict = {}
        # Phase B: project-type rejection flags (surfaced from ResearchState)
        self.should_reject: bool = False
        self.rejection_reason: Optional[str] = None

    @property
    def best_contact(self) -> Optional[dict]:
        """Return highest-priority contact: hotel_specific first, then by confidence."""
        if not self.contacts:
            return None

        scope_rank = {
            "hotel_specific": 0,
            "chain_area": 1,
            "management_corporate": 2,
            "chain_corporate": 3,
            "owner": 2,
            "unknown": 4,
        }
        confidence_rank = {"high": 0, "medium": 1, "low": 2}

        def sort_key(c):
            return (
                scope_rank.get(c.get("scope", "unknown"), 3),
                confidence_rank.get(c.get("confidence", "low"), 2),
            )

        sorted_contacts = sorted(self.contacts, key=sort_key)
        return sorted_contacts[0]

    def to_dict(self) -> dict:
        return {
            "contacts": self.contacts,
            "management_company": self.management_company,
            "developer": self.developer,
            "opening_update": self.opening_update,
            "additional_details": self.additional_details,
            "sources_used": self.sources_used,
            "layers_tried": self.layers_tried,
            "errors": self.errors,
        }


# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════


def _get_search_mode(opening_date: Optional[str]) -> str:
    """Determine if hotel is 'pre_opening' or 'opening_soon' based on opening date."""
    if not opening_date:
        return "pre_opening"

    today = date.today()
    odate = opening_date.lower().strip()

    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
        "q1": 2,
        "q2": 5,
        "q3": 8,
        "q4": 11,
        "early": 3,
        "mid": 6,
        "late": 10,
        "spring": 4,
        "summer": 7,
        "fall": 10,
        "winter": 1,
    }

    year_match = re.search(r"20(\d{2})", odate)
    if not year_match:
        return "pre_opening"

    year = 2000 + int(year_match.group(1))
    month = 6

    for keyword, m in month_map.items():
        if keyword in odate:
            month = m
            break

    try:
        opening = date(year, month, 15)
        months_until = (opening.year - today.year) * 12 + (opening.month - today.month)
        return "opening_soon" if months_until <= 6 else "pre_opening"
    except ValueError:
        return "pre_opening"


def _get_priority_titles(mode: str, brand: Optional[str] = None) -> list[str]:
    """
    Get flat list of titles in priority order for the given mode.
    If brand is provided, prepends brand-specific pre-opening titles
    from BrandRegistry so we search for the right decision makers first.
    """
    priorities = CONTACT_SEARCH_PRIORITIES.get(
        mode, CONTACT_SEARCH_PRIORITIES["pre_opening"]
    )
    titles = []
    for group in priorities:
        titles.extend(group["titles"])

    # Prepend brand-specific titles at the front so they get searched first
    if brand and mode == "pre_opening":
        brand_info = BrandRegistry.lookup(brand)
        brand_titles = brand_info.pre_opening_contact_titles
        # Add brand-specific titles that aren't already in the list
        prepend = [t for t in brand_titles if t not in titles]
        titles = prepend + titles

    return titles


def _resolve_parent_brand(
    brand: Optional[str], hotel_name: Optional[str], mgmt_company: Optional[str]
) -> tuple[str, str]:
    """Resolve brand for web search. Returns (specific_brand, parent_company)."""
    specific = brand or ""
    parent = ""

    if brand:
        key = brand.lower().strip()
        if key in BRAND_TO_PARENT:
            parent = BRAND_TO_PARENT[key]
            specific = brand
        else:
            for k, v in BRAND_TO_PARENT.items():
                if k in key or key in k:
                    parent = v
                    specific = brand
                    break

    if not specific and hotel_name:
        name_lower = hotel_name.lower()
        for k, v in BRAND_TO_PARENT.items():
            if k in name_lower:
                parent = v
                specific = k.title()
                break

    if not specific and mgmt_company:
        specific = mgmt_company
        parent = mgmt_company

    return (specific or parent, parent or specific)


def _build_location_string(
    city: Optional[str], state: Optional[str], country: Optional[str]
) -> str:
    """Build location string for web search."""
    parts = []
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    if country and country.upper() not in ("USA", "US", "UNITED STATES"):
        parts.append(country)
    elif not state:
        parts.append("United States")
    return ", ".join(parts) if parts else "United States"


def _build_region_string(state: Optional[str], country: Optional[str]) -> str:
    """Build broader region string for web search fallback."""
    if state:
        return f"{state}, United States"
    if country:
        return country
    return "United States"


def _clean_title(raw_title: str) -> str:
    """Clean up a messy title extracted from LinkedIn snippets."""
    if not raw_title:
        return ""

    raw_title = re.sub(r"\s*\|?\s*LinkedIn.*$", "", raw_title, flags=re.IGNORECASE)
    raw_title = re.sub(r"\s*#\w+.*$", "", raw_title)
    raw_title = re.sub(
        r"\s*\|\s*\d+\s*comments?.*$", "", raw_title, flags=re.IGNORECASE
    )
    raw_title = re.sub(r"\s*-\s*LinkedIn.*$", "", raw_title, flags=re.IGNORECASE)

    if len(raw_title) > 60:
        for sep in [" | ", " - ", " at ", " ... "]:
            idx = raw_title.find(sep)
            if 5 < idx < 60:
                raw_title = raw_title[:idx]
                break

    role_words = [
        "director",
        "manager",
        "chef",
        "coordinator",
        "supervisor",
        "housekeeper",
        "housekeeping",
        "purchasing",
        "procurement",
        "operations",
        "sales",
        "f&b",
        "food",
        "beverage",
        "spa",
        "general manager",
        "assistant",
        "executive",
        "buyer",
        "uniform",
        "wardrobe",
        "laundry",
        "steward",
        "rooms",
        "front office",
        "resort",
        "property",
    ]
    title_lower = raw_title.lower()
    has_role = any(w in title_lower for w in role_words)
    if not has_role and len(raw_title) > 20:
        return ""

    if len(raw_title) > 80:
        raw_title = raw_title[:80].rsplit(" ", 1)[0]

    return raw_title.strip()


# ═══════════════════════════════════════════════════════════════
# FUZZY NAME DEDUP — collapse Amanda/Mandy, Michael/Mike, etc.
# ═══════════════════════════════════════════════════════════════


# Canonical name → common nicknames. Keys are "true" names, values are
# sets of nicknames that should collapse into the key when comparing
# contacts for dedup. Used in _normalize_first_name() below.
#
# Two rules apply:
#   1. If contact A's first name is a key and B's first name is in the
#      value set (or vice-versa), AND last names match → likely same person.
#   2. Typo/near-miss last names (Vogelsang vs Foglesong) handled by
#      Levenshtein similarity check — see _likely_same_person().
_NICKNAME_TO_CANONICAL = {
    "mandy": "amanda",
    "mandi": "amanda",
    "mandie": "amanda",
    "bob": "robert",
    "rob": "robert",
    "bobby": "robert",
    "robbie": "robert",
    "mike": "michael",
    "mick": "michael",
    "mikey": "michael",
    "bill": "william",
    "billy": "william",
    "will": "william",
    "willie": "william",
    "jim": "james",
    "jimmy": "james",
    "jamie": "james",
    "tom": "thomas",
    "tommy": "thomas",
    "dave": "david",
    "davy": "david",
    "nick": "nicholas",
    "nickolas": "nicholas",
    "dick": "richard",
    "rich": "richard",
    "richie": "richard",
    "rick": "richard",
    "ricky": "richard",
    "joe": "joseph",
    "joey": "joseph",
    "jo": "joseph",
    "tony": "anthony",
    "ant": "anthony",
    "chris": "christopher",
    "kit": "christopher",
    "dan": "daniel",
    "danny": "daniel",
    "ed": "edward",
    "eddie": "edward",
    "ted": "edward",
    "teddy": "edward",
    "jack": "john",
    "johnny": "john",
    "kate": "katherine",
    "katie": "katherine",
    "kathy": "katherine",
    "cathy": "katherine",
    "kathleen": "katherine",
    "liz": "elizabeth",
    "beth": "elizabeth",
    "betty": "elizabeth",
    "betsy": "elizabeth",
    "lizzy": "elizabeth",
    "eliza": "elizabeth",
    "peg": "margaret",
    "peggy": "margaret",
    "meg": "margaret",
    "maggie": "margaret",
    "megan": "margaret",
    "amy": "amelia",
    "sam": "samuel",
    "sammy": "samuel",
    "tim": "timothy",
    "timmy": "timothy",
    "steve": "steven",
    "stevie": "steven",
    "matt": "matthew",
    "matty": "matthew",
    "andy": "andrew",
    "drew": "andrew",
    "alex": "alexander",
    "lex": "alexander",
    "ben": "benjamin",
    "benny": "benjamin",
    "jen": "jennifer",
    "jenny": "jennifer",
    "jenn": "jennifer",
    "cindy": "cynthia",
    "patty": "patricia",
    "pat": "patricia",
    "trish": "patricia",
    "debbie": "deborah",
    "deb": "deborah",
    "sue": "susan",
    "susie": "susan",
    "suzy": "susan",
    "dorothy": "dorothea",
    "dotty": "dorothy",
    "dot": "dorothy",
    "don": "donald",
    "donny": "donald",
    "ron": "ronald",
    "ronnie": "ronald",
    "greg": "gregory",
    "russ": "russell",
    "doug": "douglas",
    "phil": "philip",
    "tracey": "tracy",
}

# Common title prefixes to strip when comparing names
_NAME_TITLE_PREFIXES = {
    "dr",
    "dr.",
    "mr",
    "mr.",
    "mrs",
    "mrs.",
    "ms",
    "ms.",
    "miss",
    "prof",
    "prof.",
    "sir",
    "madam",
    "rev",
    "rev.",
}


def _normalize_first_name(raw: str) -> str:
    """Convert a first name to its canonical form (nickname → canonical)."""
    if not raw:
        return ""
    token = raw.lower().strip().rstrip(".,;:'\"")
    return _NICKNAME_TO_CANONICAL.get(token, token)


def _strip_name_titles(name: str) -> list[str]:
    """Return the name tokens with titles and initials stripped."""
    if not name:
        return []
    raw_parts = name.split()
    clean = []
    for p in raw_parts:
        low = p.lower().strip().rstrip(".,;:'\"")
        if low in _NAME_TITLE_PREFIXES:
            continue
        # Drop single-letter initials like "P." (but keep real names)
        cleaned_p = p.strip(".,;:'\"")
        if len(cleaned_p) <= 1:
            continue
        clean.append(cleaned_p)
    return clean


def _levenshtein_ratio(a: str, b: str) -> float:
    """
    Simple string-similarity ratio 0..1. Pure-Python, no external deps.
    Used to catch near-miss last names like Vogelsang vs Foglesong.
    """
    if not a or not b:
        return 0.0
    a, b = a.lower(), b.lower()
    if a == b:
        return 1.0
    m, n = len(a), len(b)
    if m < n:
        a, b, m, n = b, a, n, m
    # DP edit distance
    prev = list(range(n + 1))
    for i in range(1, m + 1):
        curr = [i] + [0] * n
        for j in range(1, n + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            curr[j] = min(
                curr[j - 1] + 1,  # insertion
                prev[j] + 1,  # deletion
                prev[j - 1] + cost,  # substitution
            )
        prev = curr
    edits = prev[n]
    return 1.0 - (edits / max(m, n))


def _likely_same_person(name_a: str, name_b: str) -> bool:
    """
    Returns True if name_a and name_b probably refer to the same person.

    Rules:
    1. Exact match (after lowercasing + title stripping).
    2. One name is a substring of the other after title stripping
       (e.g. "Kali Chaudhuri" within "Dr. Kali P. Chaudhuri").
    3. Same first name (via nickname canonicalization) AND (any of):
       a. Same last name.
       b. One last name is a prefix of the other with min 4 chars shared
          (Vogel↔Vogelsang, Smith↔Smithson).
       c. Levenshtein similarity >= 0.70 on last names (catches typos +
          phonetic variants like Vogelsang↔Foglesong).
    4. Different first name (even after nickname normalization) → not same.
    """
    if not name_a or not name_b:
        return False

    a_lower = name_a.lower().strip()
    b_lower = name_b.lower().strip()
    if a_lower == b_lower:
        return True

    parts_a = _strip_name_titles(name_a)
    parts_b = _strip_name_titles(name_b)
    if not parts_a or not parts_b:
        return False

    # Rule 2: substring match (one is "Dr. Kali P. Chaudhuri", other "Kali Chaudhuri")
    joined_a = " ".join(p.lower() for p in parts_a)
    joined_b = " ".join(p.lower() for p in parts_b)
    if joined_a in joined_b or joined_b in joined_a:
        return True

    # Rules 3 & 4: require usable first + last name for both
    if len(parts_a) < 2 or len(parts_b) < 2:
        return False

    first_a = _normalize_first_name(parts_a[0])
    first_b = _normalize_first_name(parts_b[0])
    last_a = parts_a[-1].lower().rstrip(".,;:'\"")
    last_b = parts_b[-1].lower().rstrip(".,;:'\"")

    # Different first name (even after nickname normalization) → not same
    if first_a != first_b:
        return False

    # Same first + same last → same person
    if last_a == last_b:
        return True

    # Rule 3b: prefix match with min 4-char overlap catches short variants
    # like "Vogel" (nickname/shortened) vs "Vogelsang" (full surname)
    min_shared = 4
    shorter, longer = (
        (last_a, last_b) if len(last_a) <= len(last_b) else (last_b, last_a)
    )
    if len(shorter) >= min_shared and longer.startswith(shorter):
        return True

    # Rule 3c: overall Levenshtein similarity ≥ 0.70 catches typos
    # (Vogelsang↔Foglesong, Johnson↔Johnsen, etc.)
    if _levenshtein_ratio(last_a, last_b) >= 0.70:
        return True

    return False


def _merge_contacts(primary: dict, secondary: dict) -> dict:
    """
    Merge two contact dicts representing the same person. Primary wins
    on scalar fields where both are populated; secondary fills gaps.
    Evidence arrays are concatenated and de-duped by URL.
    """
    merged = dict(primary)
    # Scalar fields — fill if primary missing
    for field in (
        "title",
        "email",
        "phone",
        "linkedin",
        "organization",
        "source_detail",
        "_current_employer",
        "_current_title",
    ):
        if not merged.get(field) and secondary.get(field):
            merged[field] = secondary[field]

    # Evidence — concatenate + dedupe by URL
    primary_ev = merged.get("_evidence_items") or []
    secondary_ev = secondary.get("_evidence_items") or []
    if secondary_ev:
        seen_urls = {e.get("source_url") for e in primary_ev if isinstance(e, dict)}
        for ev in secondary_ev:
            if ev.get("source_url") not in seen_urls:
                primary_ev.append(ev)
                seen_urls.add(ev.get("source_url"))
        merged["_evidence_items"] = primary_ev

    # Prefer the higher priority (P1 > P2 > P3 > P4)
    _PRI_RANK = {"P1": 0, "P2": 1, "P3": 2, "P4": 3, None: 4}
    p1 = _PRI_RANK.get(primary.get("_final_priority"), 4)
    p2 = _PRI_RANK.get(secondary.get("_final_priority"), 4)
    if p2 < p1:
        merged["_final_priority"] = secondary.get("_final_priority")
        merged["_final_reasoning"] = secondary.get("_final_reasoning")

    return merged


def _apply_smart_cap(contacts: list[dict], max_total: int = 6) -> list[dict]:
    """
    Cap the contact list to `max_total` using smart distribution instead
    of pure top-N-by-score. Prevents the failure mode where 6 near-dup
    management_corporate execs push the property owner off the list.

    Filling order (each slot is a separate pass):
      1. 1 slot for best OWNER (scope=owner) — check-writer priority
      2. 2 slots for top P1 at operator/property (management_corporate,
         hotel_specific, chain_area)
      3. 2 slots for top P2 at operator/property
      4. 1 slot for best remaining contact (any category)

    If a slot's category has no candidates, it's left for backfill.
    Result: always get owner if present, never all P1 dupes, balanced mix.

    Assumes `contacts` is already sorted by priority/scope/score.
    """
    if not contacts or len(contacts) <= max_total:
        return contacts

    def _category(c: dict) -> str:
        scope = (c.get("scope") or "").lower()
        priority = c.get("_final_priority") or ""
        if scope == "owner":
            return "owner"
        if priority == "P1" and scope in (
            "hotel_specific",
            "management_corporate",
            "chain_area",
        ):
            return "p1_operator"
        if priority == "P2" and scope in (
            "hotel_specific",
            "management_corporate",
            "chain_area",
        ):
            return "p2_operator"
        return "other"

    buckets: dict[str, list[dict]] = {
        "owner": [],
        "p1_operator": [],
        "p2_operator": [],
        "other": [],
    }
    for c in contacts:
        buckets[_category(c)].append(c)

    kept: list[dict] = []
    seen_names: set[str] = set()

    def _add(contact: dict) -> bool:
        nm = (contact.get("name") or "").lower().strip()
        if not nm or nm in seen_names or len(kept) >= max_total:
            return False
        kept.append(contact)
        seen_names.add(nm)
        return True

    targets = _SMART_CAP_TARGETS
    for cat, count in [
        ("owner", targets["owner"]),
        ("p1_operator", targets["p1_operator"]),
        ("p2_operator", targets["p2_operator"]),
    ]:
        taken = 0
        for c in buckets[cat]:
            if taken >= count or len(kept) >= max_total:
                break
            if _add(c):
                taken += 1

    # Backfill remaining slots with best-remaining in sort order
    for c in contacts:
        if len(kept) >= max_total:
            break
        _add(c)

    return kept


def _fuzzy_dedupe_contacts(contacts: list[dict]) -> list[dict]:
    """
    Collapse near-duplicate contacts (Amanda/Mandy variants, typo
    last names, with-or-without-title-prefixes).

    Algorithm: iterate contacts in score order (highest first); for each,
    check if it's likely-same-as any already-kept contact. If yes, merge
    into the kept one. Otherwise, keep as a new entry.
    """
    if not contacts:
        return []

    # Sort by score desc so the highest-quality copy wins when merging
    sorted_contacts = sorted(
        contacts,
        key=lambda c: -(c.get("_validation_score") or c.get("score") or 0),
    )

    kept: list[dict] = []
    merged_count = 0
    for c in sorted_contacts:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        match_idx = None
        for i, k in enumerate(kept):
            if _likely_same_person(name, k.get("name") or ""):
                match_idx = i
                break
        if match_idx is not None:
            merged = _merge_contacts(kept[match_idx], c)
            logger.info(
                f"[DEDUP] Collapsed '{c.get('name')}' into "
                f"'{kept[match_idx].get('name')}' (likely same person)"
            )
            kept[match_idx] = merged
            merged_count += 1
        else:
            kept.append(c)

    if merged_count:
        logger.info(
            f"[DEDUP] Collapsed {merged_count} duplicate contacts "
            f"({len(contacts)} → {len(kept)})"
        )
    return kept


def _is_hotel_relevant_title(title: str) -> bool:
    """Check if a title is relevant to hotel uniform sales using SAP classifier."""
    if not title:
        return False

    classification = title_classifier.classify(title)
    # Anything Tier 1-5 is relevant; Tier 6 (Finance) and Tier 7 (Irrelevant) are not
    return classification.tier.value <= BuyerTier.TIER5_HR.value


def _is_irrelevant_org(org: str) -> bool:
    """Filter out contacts from non-hotel organizations.
    L-04: Delegates to shared is_irrelevant_org() in contact_validator.
    """
    return is_irrelevant_org(org)


def _is_corporate_title(title: str) -> bool:
    """Filter out corporate/executive/investor titles — not property-level contacts.
    L-04: Delegates to shared is_corporate_title() in contact_validator.
    """
    return is_corporate_title(title)


# ═══════════════════════════════════════════════════════════════
# PROTECTION HELPERS — used by _title_proves_hotel
# ═══════════════════════════════════════════════════════════════

# Words that disqualify a "name" from being a real person.
# Used to reject "Travel Turtle Magazine", "UPDATE GROUP", etc. from
# the protection rule that makes contacts immune to Gemini rejection.
_NON_PERSON_NAME_TOKENS = frozenset(
    {
        "magazine",
        "news",
        "media",
        "press",
        "post",
        "update",
        "group",
        "company",
        "corp",
        "corporation",
        "inc",
        "llc",
        "ltd",
        "hotel",
        "hotels",
        "resort",
        "resorts",
        "the",
        "linkedin",
        "publication",
        "publishing",
        "journal",
        "review",
        "times",
        "weekly",
        "daily",
        "today",
        "report",
        "channel",
        "network",
        "tv",
        "radio",
        "podcast",
        "blog",
        "official",
    }
)


def _looks_like_real_person(name: str) -> bool:
    """Heuristic check that a name belongs to an individual, not an entity.

    Returns False for things like "Travel Turtle Magazine", "UPDATE GROUP",
    "LinkedIn News", or single-word handles. Returns True for plausible
    "First Last" style person names.
    """
    if not name:
        return False
    name = name.strip()
    if len(name) < 4:
        return False
    if name.isupper():
        return False
    parts = name.split()
    if len(parts) < 2:
        return False
    # Reject if any token is a known non-person noun (publication / org word)
    for token in parts:
        clean = token.strip(".,;:'\"()[]").lower()
        if clean in _NON_PERSON_NAME_TOKENS:
            return False
    # First and last token should both start with a capital letter
    if not (parts[0][0].isupper() and parts[-1][0].isupper()):
        return False
    return True


def _hotel_phrase_appears(hotel_name: str, haystacks: list[str]) -> bool:
    """Check if the hotel name appears as a CONTIGUOUS phrase in any haystack.

    This is the tightened replacement for the old word-bag overlap that
    falsely matched 'Pan Am Hotel' against any text containing the words
    'pan', 'am', and 'hotel' scattered separately.

    Match rules (any one is sufficient):
      1. The full normalized hotel name appears as a contiguous substring
         in any haystack (e.g. 'pan am hotel' must literally appear).
      2. For multi-word hotel names with ≥2 distinctive (non-filler) words,
         the contiguous bigram of the FIRST TWO distinctive words appears
         (e.g. 'pan am' for 'Pan Am Hotel'; 'four seasons' for 'Four
         Seasons Resort Maui'). This catches cases where the snippet
         abbreviates but preserves the brand-distinguishing portion.

    Filler words (the, hotel, resort, etc.) are stripped before the
    bigram check so they don't act as the "first distinctive word".
    """
    if not hotel_name:
        return False

    _filler = {
        "the",
        "and",
        "at",
        "by",
        "of",
        "in",
        "on",
        "for",
        "a",
        "an",
        "&",
        "east",
        "west",
        "north",
        "south",
        "village",
        "downtown",
        "uptown",
        "hotel",
        "hotels",
        "resort",
        "resorts",
        "spa",
        "inn",
        "lodge",
        "suites",
        "suite",
        "club",
        "house",
        "tower",
        "towers",
        "collection",
        "residences",
        "residence",
    }

    # Normalize hotel name: lowercase, strip punctuation, collapse spaces
    hotel_norm = re.sub(r"[^a-z0-9\s]", " ", hotel_name.lower())
    hotel_norm = " ".join(hotel_norm.split())
    if not hotel_norm:
        return False

    # Build distinctive-word bigram (first two non-filler tokens)
    distinctive_tokens = [t for t in hotel_norm.split() if t not in _filler]
    distinctive_bigram = ""
    if len(distinctive_tokens) >= 2:
        distinctive_bigram = f"{distinctive_tokens[0]} {distinctive_tokens[1]}"
    elif len(distinctive_tokens) == 1 and len(distinctive_tokens[0]) >= 5:
        # Single distinctive word — only use it if long enough to be unambiguous
        # (avoids "dean" matching "dean's italian steakhouse")
        distinctive_bigram = distinctive_tokens[0]

    for h in haystacks:
        if not h:
            continue
        h_norm = re.sub(r"[^a-z0-9\s]", " ", h.lower())
        h_norm = " ".join(h_norm.split())
        if not h_norm:
            continue
        # Match #1: full hotel name as contiguous substring
        if hotel_norm in h_norm:
            return True
        # Match #2: distinctive bigram as contiguous substring,
        # using word boundaries so 'pan am' does NOT match inside 'panama'
        if distinctive_bigram:
            if re.search(r"\b" + re.escape(distinctive_bigram) + r"\b", h_norm):
                return True

    return False


# ═══════════════════════════════════════════════════════════════
# GEMINI AI EXTRACTION PROMPT v3 — Stricter hotel verification
# ═══════════════════════════════════════════════════════════════

# ── STRUCTURED OUTPUT SCHEMA (matches CONTACT_EXTRACTION_PROMPT_V3) ──
# Passed to _call_gemini(response_schema=...) when using Vertex AI.
# Forces Gemini to return well-formed JSON by construction, bypassing the
# _try_recover_json path that was silently dropping contacts (Bug #1).
#
# Schema grammar = OpenAPI 3.0 subset supported by Vertex AI:
#   https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/control-generated-output
# Use `nullable: true` instead of JSON Schema's null union type. Keep enums
# aligned with the prompt's SCOPE / CONFIDENCE rules.
CONTACT_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "contacts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "title": {"type": "string"},
                    "email": {"type": "string", "nullable": True},
                    "phone": {"type": "string", "nullable": True},
                    "linkedin": {"type": "string", "nullable": True},
                    "organization": {"type": "string", "nullable": True},
                    "scope": {
                        "type": "string",
                        "enum": [
                            "hotel_specific",
                            "chain_area",
                            "chain_corporate",
                            "wrong_hotel",
                            "irrelevant",
                        ],
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                    },
                    "confidence_note": {"type": "string"},
                },
                "required": ["name", "title", "scope", "confidence"],
            },
        },
        "management_company": {"type": "string", "nullable": True},
        "developer": {"type": "string", "nullable": True},
        "opening_update": {"type": "string", "nullable": True},
        "additional_details": {"type": "string", "nullable": True},
    },
    "required": ["contacts"],
}


CONTACT_EXTRACTION_PROMPT_V3 = """You are extracting hotel staff contact information from a news article.

TARGET HOTEL: {hotel_name}
LOCATION: {location}

CRITICAL RULES:
1. ONLY extract people who work at hotel/hospitality operations roles (GM, Directors, Managers)
2. DO NOT extract: journalists, government officials, real estate brokers, architects, investors, developers, lawyers
3. If this article is about a DIFFERENT hotel than "{hotel_name}", mark those contacts scope as "wrong_hotel"
4. Maximum 5 contacts — only the most relevant ones

For each contact, determine SCOPE:
- "hotel_specific" = CONFIRMED to work at {hotel_name} specifically (article names them WITH this hotel)
- "chain_area" = Works for parent brand in same area, NOT confirmed at this specific property
- "chain_corporate" = Corporate/HQ level role at parent company
- "wrong_hotel" = Works at a DIFFERENT hotel mentioned in the same article
- "irrelevant" = Not a hotel operations person (broker, journalist, politician, developer)

CONFIDENCE:
- "high" = Article explicitly says "[Name] is the [Title] of/at {hotel_name}"
- "medium" = Strong indication (LinkedIn title matches hotel name)
- "low" = Weak connection, might be different property or role

Return JSON with max 5 contacts:
- name: Full name
- title: Job title only (short, no company name)
- email: Email or null
- phone: Phone or null
- linkedin: LinkedIn URL or null
- organization: Hotel or company name
- scope: hotel_specific | chain_area | chain_corporate | wrong_hotel | irrelevant
- confidence: high | medium | low
- confidence_note: One sentence why

Also extract:
- management_company: or null
- developer: or null
- opening_update: or null
- additional_details: or null

Return ONLY valid JSON:
{{
    "contacts": [],
    "management_company": null,
    "developer": null,
    "opening_update": null,
    "additional_details": null
}}

Article text:
{article_text}
"""


# ═══════════════════════════════════════════════════════════════
# LAYER 1: WEB SEARCH + SCRAPE + AI EXTRACT
# ═══════════════════════════════════════════════════════════════


async def _search_serper(query: str, max_results: int = 5) -> list[dict]:
    """Search Google via Serper.dev API. Returns same format as DDG for compatibility."""
    api_key = os.getenv("SERPER_API_KEY")
    if not api_key:
        logger.debug("SERPER_API_KEY not set, skipping Google search")
        return []

    try:
        client = _get_client()
        resp = await client.post(
            "https://google.serper.dev/search",
            json={"q": query, "num": max_results},
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
        )
        if resp.status_code != 200:
            logger.warning(f"Serper API error: {resp.status_code}")
            return []

        data = resp.json()
        results = []

        # Organic results
        for r in data.get("organic", [])[:max_results]:
            results.append(
                {
                    "title": r.get("title", ""),
                    "url": r.get("link", ""),
                    "snippet": r.get("snippet", ""),
                }
            )

        # Knowledge graph — often contains the GM name directly
        kg = data.get("knowledgeGraph", {})
        if kg and kg.get("description"):
            logger.info(
                f"Serper Knowledge Graph: {kg.get('title', '')} — {kg.get('description', '')[:100]}"
            )

        return results
    except Exception as e:
        logger.warning(f"Serper search failed: {e}")
        return []


async def _search_duckduckgo(query: str, max_results: int = 3) -> list[dict]:
    """Search DuckDuckGo using the ddgs package. Free fallback when Serper unavailable."""
    try:
        from duckduckgo_search import DDGS

        def _sync_search():
            return list(DDGS().text(query, max_results=max_results))

        results = await asyncio.to_thread(_sync_search)
        return [
            {
                "title": r.get("title", ""),
                "url": r.get("href", ""),
                "snippet": r.get("body", ""),
            }
            for r in results
        ]
    except Exception as e:
        logger.warning(f"DuckDuckGo search failed: {e}")
        return []


async def _search_web(query: str, max_results: int = 5) -> list[dict]:
    """Unified search: Serper (Google) only. DDG removed — endpoints are dead and were
    blocking every query for ~20s waiting on timeouts."""
    all_results = []
    seen_urls = set()

    try:
        serper_results = await _search_serper(query, max_results=max_results)
    except Exception as e:
        logger.warning(f"Serper failed: {e}")
        serper_results = []

    if serper_results:
        logger.info(f"Google (Serper): {len(serper_results)} results for: {query[:60]}")
        for r in serper_results:
            url_key = r["url"].rstrip("/").lower()
            if url_key not in seen_urls:
                seen_urls.add(url_key)
                all_results.append(r)
    else:
        logger.info(f"No search results for: {query[:60]}")

    return all_results


async def _scrape_url(url: str) -> Optional[str]:
    """Scrape article text - tries httpx first, falls back to Crawl4AI for blocked sites."""
    timeout = ENRICHMENT_SETTINGS["crawl_timeout_seconds"]
    skip_domains = [
        "linkedin.com",
        "indeed.com",
        "ziprecruiter.com",
        "careers.",
        "jobs.",
        "wikipedia.org",
        # Social/auth-gated sites — always return HTTP 4xx to unauth
        # scrapers. No point burning time + Crawl4AI retries on them.
        # Empirically observed returning 400/403 on every attempt.
        "facebook.com",
        "fb.com",
        "instagram.com",
        "twitter.com",
        "x.com",
        "tiktok.com",
        "youtube.com",
        "youtu.be",
        "threads.net",
        "pinterest.com",
        "reddit.com",
    ]
    url_lower = url.lower()
    for skip in skip_domains:
        if skip in url_lower:
            logger.info(f"Skipping: {url} (non-article site)")
            return None

    text = ""
    httpx_failed = False

    # Try httpx first (fast)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                },
            )
            if resp.status_code == 200:
                text = re.sub(
                    r"<script[^>]*>.*?</script>", "", resp.text, flags=re.DOTALL
                )
                text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 200:
                    return text[: ENRICHMENT_SETTINGS["max_article_chars"]]
            # 403 = retry with full browser headers before Crawl4AI
            if resp.status_code == 403:
                logger.info(f"httpx blocked (403), retrying with full headers: {url}")
                try:
                    resp2 = await client.get(
                        url,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Accept-Encoding": "gzip, deflate, br",
                            "Referer": "https://www.google.com/",
                            "DNT": "1",
                            "Connection": "keep-alive",
                            "Upgrade-Insecure-Requests": "1",
                            "Sec-Fetch-Dest": "document",
                            "Sec-Fetch-Mode": "navigate",
                            "Sec-Fetch-Site": "cross-site",
                            "Cache-Control": "max-age=0",
                        },
                    )
                    if resp2.status_code == 200:
                        text = re.sub(
                            r"<script[^>]*>.*?</script>",
                            "",
                            resp2.text,
                            flags=re.DOTALL,
                        )
                        text = re.sub(
                            r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL
                        )
                        text = re.sub(r"<[^>]+>", " ", text)
                        text = re.sub(r"\s+", " ", text).strip()
                        if len(text) > 200:
                            logger.info(
                                f"httpx retry succeeded: {url} ({len(text)} chars)"
                            )
                            return text[: ENRICHMENT_SETTINGS["max_article_chars"]]
                except Exception:
                    pass
                httpx_failed = True
            elif resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
    except Exception as e:
        logger.info(f"httpx failed ({e}), trying Crawl4AI: {url}")
        httpx_failed = True

    if not httpx_failed:
        return None

    # Fallback: Crawl4AI with browser rendering
    # Run in separate thread to avoid Windows event loop subprocess issues under uvicorn
    try:
        import os

        os.environ["PYTHONIOENCODING"] = "utf-8"

        def _crawl_sync(target_url: str) -> Optional[str]:
            """Run Crawl4AI in a fresh event loop on a separate thread."""
            import asyncio as _asyncio
            import sys

            # Fix Windows encoding for Crawl4AI unicode output
            if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
                try:
                    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
                    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
                except Exception:
                    pass

            from crawl4ai import AsyncWebCrawler

            async def _do_crawl():
                async with AsyncWebCrawler(verbose=False) as crawler:
                    return await crawler.arun(url=target_url)

            loop = _asyncio.new_event_loop()
            try:
                return loop.run_until_complete(_do_crawl())
            finally:
                loop.close()

        result = await asyncio.to_thread(_crawl_sync, url)
        if result and result.markdown:
            crawled = result.markdown.strip()
            if len(crawled) > 200:
                # Strip markdown formatting for cleaner Gemini extraction
                crawled = re.sub(r"!\[.*?\]\(.*?\)", "", crawled)
                crawled = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", crawled)
                crawled = re.sub(r"#{1,6}\s*", "", crawled)
                crawled = re.sub(r"\*{1,3}([^*]+)\*{1,3}", r"\1", crawled)
                crawled = re.sub(r"\n{3,}", "\n\n", crawled)
                crawled = crawled.strip()
                logger.info(f"Crawl4AI succeeded: {url} ({len(crawled)} chars)")
                return crawled[: ENRICHMENT_SETTINGS["max_article_chars"]]
        logger.warning(f"Crawl4AI returned no content for {url}")
        return None
    except Exception as e:
        logger.warning(f"Crawl4AI failed for {url}: {e}")
        return None


async def _extract_contacts_with_gemini(
    article_text: str, hotel_name: str, location: str
) -> Optional[dict]:
    """Use Gemini to extract contacts with scope tagging.

    Uses Vertex AI structured outputs (responseSchema) to guarantee
    well-formed JSON, and raises max_output_tokens to 32768 to leave
    headroom for Gemini 2.5 Flash "thinking" tokens on large pages.
    (Bug #1 fix — 2026-04-22.)
    """
    model = get_enrichment_gemini_model()
    prompt = CONTACT_EXTRACTION_PROMPT_V3.format(
        hotel_name=hotel_name,
        location=location,
        article_text=article_text[: ENRICHMENT_SETTINGS["max_article_chars"]],
    )

    try:
        return await _call_gemini(
            prompt,
            model=model,
            response_schema=CONTACT_EXTRACTION_SCHEMA,
            max_output_tokens=32768,
        )
    except Exception as e:
        logger.error(f"Gemini extraction failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# GROUNDED CONTACT FAST-PATH — one-shot Gemini googleSearch
# ═══════════════════════════════════════════════════════════════
# Instead of searching role-by-role (GM, Director of Housekeeping, etc.),
# ask Gemini the *business question* directly: "Who at this hotel or its
# management company is responsible for uniform purchasing decisions?"
#
# Advantages over the iterative pipeline:
#   - Intent-based search — Gemini understands WHAT we need, not just titles
#   - Surfaces centralized procurement at the management company (P1 priority)
#     without us having to know which titles to look for
#   - Finds press releases + LinkedIn + trade publications in one shot
#   - ~15-20s vs 2-3 minutes for the 6-iteration pipeline
#   - Falls back cleanly if grounding returns < 2 named contacts


def _build_contact_grounding_prompt(
    hotel_name: str,
    management_company: Optional[str],
    developer: Optional[str],
    owner: Optional[str],
    brand: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    opening_date: Optional[str],
) -> str:
    """Build a context-aware grounding prompt using full SLH intelligence stack.

    Uses BrandRegistry, procurement_intelligence, and MANAGEMENT_COMPANY_INTEL
    to tell Gemini EXACTLY who to search for based on:
    - Operating model (managed / franchised / collection / all_inclusive / independent)
    - Procurement model (avendra_gpo / brand_managed / owner_managed / fully_open)
    - Management company known intel (procurement titles, portfolio size)
    - Developer/owner involvement (pre-opening budget control)
    - Pre-opening vs open status
    """
    from app.config.brand_registry import BrandRegistry
    from app.config.procurement_intelligence import (
        build_prospecting_strategy,
        get_management_company_intel,
    )

    location = ", ".join(filter(None, [city, state, country]))
    mode = _get_search_mode(opening_date)
    is_pre_opening = mode == "pre_opening"

    # ── Pull brand intelligence ──
    brand_info = None
    operating_model = "unknown"
    procurement_model = "unknown"
    uniform_freedom = "medium"
    gpo = None
    pre_opening_titles = []
    try:
        if brand:
            brand_info = BrandRegistry.lookup(brand)
            operating_model = brand_info.operating_model or "unknown"
            procurement_model = brand_info.procurement_model or "unknown"
            uniform_freedom = brand_info.uniform_freedom or "medium"
            gpo = brand_info.gpo
            pre_opening_titles = brand_info.pre_opening_contact_titles or []
    except Exception:
        pass

    # ── Pull management company intelligence ──
    mgmt_intel = get_management_company_intel(management_company or "")
    mgmt_procurement_titles = []
    mgmt_ops_titles = []
    mgmt_known_contacts = []
    mgmt_portfolio = ""
    mgmt_note = ""
    if mgmt_intel:
        mgmt_procurement_titles = mgmt_intel.get("procurement_titles", [])
        mgmt_ops_titles = mgmt_intel.get("ops_titles", [])
        mgmt_known_contacts = mgmt_intel.get("known_contacts", [])
        mgmt_portfolio = mgmt_intel.get("portfolio_size", "")
        mgmt_note = mgmt_intel.get("note", "")

    # ── Build prospecting strategy ──
    try:
        strategy = build_prospecting_strategy(
            hotel_name=hotel_name,
            brand=brand,
            management_company=management_company,
            operating_model=operating_model,
            procurement_model=procurement_model,
            gpo=gpo,
            months_until_opening=None,
        )
        primary_titles = strategy.primary_titles[:6]
        approach_note = strategy.approach_note
    except Exception:
        primary_titles = [
            "Director of Procurement",
            "VP Procurement",
            "Director of Purchasing",
            "Director of Operations",
            "General Manager",
            "SVP Operations",
        ]
        approach_note = ""

    # Merge brand-specific titles at the front
    all_primary = []
    seen = set()
    for t in pre_opening_titles[:3] + primary_titles:
        if t.lower() not in seen:
            seen.add(t.lower())
            all_primary.append(t)
    all_primary = all_primary[:7]

    # ── Build context blocks ──

    # GPO warning
    gpo_block = ""
    if gpo and uniform_freedom == "low":
        gpo_block = f"""
⚠️ GPO ALERT: {brand or hotel_name} uses {gpo} for procurement.
JA Uniforms must be {gpo}-approved to easily sell to this brand.
Focus on PROPERTY-LEVEL contacts (GM, Dir of Housekeeping) who have
more flexibility than corporate procurement on non-mandated items like uniforms."""

    # Developer block (pre-opening budget control)
    developer_block = ""
    if developer and is_pre_opening:
        developer_block = f"""
DEVELOPER: {developer}
The developer controls the ENTIRE FF&E and OS&E budget during construction.
For pre-opening hotels, the developer signs vendor contracts BEFORE the operator
takes over — including uniforms. They are P1 contacts RIGHT NOW.
Search for: CEO, President, CFO, Managing Director at {developer}"""

    # Owner block
    owner_block = ""
    if owner and owner != developer:
        owner_block = f"""
PROPERTY OWNER: {owner}
The property owner approves all major vendor spend.
{"For this independent/boutique property, the owner IS the procurement decision-maker." if operating_model in ("independent", "unknown") else ""}
Search for: Principal, President, Managing Partner, CEO at {owner}"""

    # Management company block
    mgmt_block = ""
    if management_company:
        titles_to_show = (mgmt_procurement_titles + mgmt_ops_titles)[:5]
        if not titles_to_show:
            titles_to_show = all_primary[:4]
        portfolio_str = f" ({mgmt_portfolio})" if mgmt_portfolio else ""
        known_str = ""
        if mgmt_known_contacts:
            known_str = "\n  Known contacts: " + ", ".join(mgmt_known_contacts)
        mgmt_block = f"""
MANAGEMENT COMPANY: {management_company}{portfolio_str}
{mgmt_note}
ONE relationship at {management_company} = access to ALL their properties.
Search for these titles at {management_company}:{known_str}
{chr(10).join(f"  - {t}" for t in titles_to_show)}"""

    # Operating model context
    if operating_model == "all_inclusive":
        model_context = f"""
OPERATING MODEL: ALL-INCLUSIVE (fully centralized procurement)
ALL procurement decisions are made at {management_company or brand} corporate.
Property GMs have ZERO purchasing authority.
ONLY search corporate-level procurement and operations executives."""

    elif operating_model == "managed":
        model_context = """
OPERATING MODEL: BRAND-MANAGED (e.g. Four Seasons, Aman, EDITION)
The brand's regional operations team controls vendor standards.
For pre-opening: search brand corporate VP Operations / Pre-Opening Director.
The incoming GM (announced 6-12 months before opening) is also P1."""

    elif operating_model in ("franchised", "collection"):
        model_context = f"""
OPERATING MODEL: {operating_model.upper()} (management company controls day-to-day)
The management company ({management_company or "unknown"}) is the PRIMARY target.
Their corporate procurement team buys uniforms for ALL their properties.
Brand corporate (e.g. Marriott HQ) does NOT control procurement here."""

    elif uniform_freedom == "high" or operating_model == "independent":
        model_context = """
OPERATING MODEL: INDEPENDENT / BOUTIQUE (owner controls everything)
No corporate procurement layer — the owner/operator makes all vendor decisions.
The owner, managing director, or GM is the uniform buyer.
Include owner/founder-level contacts — they ARE the decision-makers here."""

    else:
        model_context = ""

    # ──────────────────────────────────────────────────────────────────
    # STATUS CONTEXT — both modes lead with property-level staff.
    # The "property-first" cascade is the PRIMARY directive; status only
    # changes whether the property GM is "incoming" (announced but not
    # started) vs already on site, and whether mgmt-company corporate is
    # available as a graceful fallback for not-yet-hired property roles.
    # ──────────────────────────────────────────────────────────────────
    if is_pre_opening:
        status_context = f"""
STATUS: PRE-OPENING — uniforms must be ordered 6+ months before opening.
For pre-opening, an INCOMING GM (announced but not yet started) IS the
property-level buyer right now. That announcement IS the buying moment.
Search property-level FIRST:
  - Incoming GM / Pre-Opening GM (announced via press release / LinkedIn)
  - Pre-Opening Director of Operations or Director of Housekeeping
  - Pre-Opening Project Manager (when assigned to THIS property)
Only fall back to {management_company or brand or "operator"} corporate if
no on-property pre-opening team has been named yet."""
    else:
        status_context = """
STATUS: OPEN AND OPERATING
The on-property team IS hired. Property-level staff are the day-to-day
uniform buyers — they are ALWAYS the primary targets:
  - Director of Housekeeping / Executive Housekeeper (specs uniforms,
    largest uniform category, repeat buyer for life)
  - General Manager / Hotel Manager (approves vendor + signs PO)
  - Director of Human Resources / People & Culture (owns the uniform
    program, runs onboarding, sets dress-code policy)
  - Director of Purchasing / Purchasing Manager (issues PO)
  - F&B Director / Executive Chef (specs F&B uniforms)
For an OPEN hotel, mgmt-company corporate is a LAST-RESORT fallback,
not a substitute. Always exhaust property-level searches first."""

    # ──────────────────────────────────────────────────────────────────
    # LINKEDIN SEARCH GUIDANCE — property-first.
    # Lead Gemini's grounding searches at THIS hotel's roster before
    # anything else. Only fall back to operator-org searches afterward.
    # ──────────────────────────────────────────────────────────────────
    li_searches = []
    # Property-level searches FIRST — anchored to the hotel name itself.
    li_searches.append(
        f'  site:linkedin.com/in "{hotel_name}" "Director of Housekeeping"'
    )
    li_searches.append(
        f'  site:linkedin.com/in "{hotel_name}" "General Manager" OR "Hotel Manager"'
    )
    li_searches.append(
        f'  site:linkedin.com/in "{hotel_name}" '
        f'"Director of Human Resources" OR "Director of People" OR "HR Director"'
    )
    li_searches.append(
        f'  site:linkedin.com/in "{hotel_name}" "Director of Purchasing" OR "Purchasing Manager"'
    )
    li_searches.append(
        f'  "{hotel_name}" "appointed" OR "joins" OR "named" site:linkedin.com'
    )
    # Mgmt-company fallback searches — only run AFTER property-level.
    primary_org = management_company or brand or hotel_name
    if management_company:
        for title in all_primary[:2]:
            li_searches.append(
                f'  (FALLBACK) site:linkedin.com/in "{primary_org}" "{title}"'
            )
    if developer and is_pre_opening:
        li_searches.append(
            f'  (FALLBACK) site:linkedin.com/in "{developer}" CEO OR President OR CFO'
        )

    li_block = "LinkedIn searches in CASCADE ORDER:\n" + "\n".join(li_searches)

    # ──────────────────────────────────────────────────────────────────
    # The actual prompt — property-first cascade as the PRIMARY directive.
    # ──────────────────────────────────────────────────────────────────
    return f"""You are a B2B sales researcher for JA Uniforms — a hotel uniform supplier
in Miami, FL. JA Uniforms sells staff uniforms EXCLUSIVELY to 4-star and above
hotels (luxury, upper-upscale, upscale). NOT for budget or economy hotels.

═══════════════════════════════════════════════════════
TARGET PROPERTY
═══════════════════════════════════════════════════════
HOTEL: {hotel_name}
LOCATION: {location}
{"BRAND: " + brand if brand else ""}
{"OPENING DATE: " + opening_date if opening_date else ""}
{gpo_block}
{model_context}
{status_context}
{developer_block}
{owner_block}
{mgmt_block}

═══════════════════════════════════════════════════════
STEP 1 — IDENTIFY HOW THIS HOTEL OPERATES (do this FIRST)
═══════════════════════════════════════════════════════

Different hotels have completely different buyers. Before searching for
contacts, decide which OPERATING MODEL fits {hotel_name}, then target
the role that owns uniform decisions for THAT model.

┌─────────────────────────────────────────────────────────────────┐
│ MODEL A — INDEPENDENT / BOUTIQUE / FAMILY-OWNED                 │
│ Signals: no chain affiliation, single property, "boutique" or   │
│   "lifestyle" in marketing, founder named in press              │
│ Buyer: OWNER / FOUNDER personally                               │
│ Find: Owner, Founder, Principal, Managing Director              │
│ Note: A property GM may exist but defers to owner on vendors    │
├─────────────────────────────────────────────────────────────────┤
│ MODEL B — BRANDED FULL-SERVICE (chain-direct flag)              │
│ Examples: Marriott, Hilton, Sheraton, Westin, Hyatt Regency,    │
│   Conrad, Doubletree, Crowne Plaza, InterContinental            │
│ Buyer: PROPERTY-LEVEL within brand's approved-vendor list       │
│ Find: Director of Housekeeping (specs), GM (approves vendor),   │
│   Director of Purchasing (issues PO)                            │
│ Note: Brand HQ sets the approved-vendor list but does NOT pick  │
│   the winner — property picks within the list                   │
├─────────────────────────────────────────────────────────────────┤
│ MODEL C — SOFT BRAND / COLLECTION (operator runs day-to-day)    │
│ Examples: Autograph Collection, Curio Collection, Tribute       │
│   Portfolio, Tapestry Collection, Unbound Collection, JdV,      │
│   MGallery, The Unbound Collection                              │
│ Buyer: PROPERTY GM + OPERATOR'S CORPORATE PROCUREMENT (combo)   │
│ Find: Property Dir of Housekeeping FIRST (runs day-to-day),     │
│   then operator's VP/Director Procurement (signs master         │
│   contract for the operator's whole portfolio)                  │
│ Note: Brand parent (Marriott / Hilton / Hyatt HQ) is NOT the    │
│   buyer here — IGNORE brand corporate. The mgmt company is the  │
│   actual operator and they own procurement.                     │
├─────────────────────────────────────────────────────────────────┤
│ MODEL D — LUXURY BRAND-MANAGED (brand controls standards)       │
│ Examples: Ritz-Carlton, Four Seasons, Aman, EDITION, St. Regis, │
│   Park Hyatt, Andaz, Mandarin Oriental, Rosewood, Bulgari,      │
│   Peninsula, Auberge Resorts, Conrad, Waldorf Astoria, LXR      │
│ Buyer: PROPERTY GM + brand regional ops team (joint decision)   │
│ Find — IN ORDER:                                                │
│   1. Property GM at this hotel (incl. INCOMING GM if announced) │
│   2. Property Director of Housekeeping at this hotel            │
│   3. Brand regional VP/SVP Operations covering this property's  │
│      region (e.g. "VP Operations, Ritz-Carlton Americas",       │
│      "SVP, The Ritz-Carlton & EDITION", "VP Hotel Operations    │
│      Four Seasons North America"). For Marriott luxury (Ritz,   │
│      St. Regis, EDITION), search Marriott Luxury Group.         │
│   4. Chief Development Officer / VP Development covering this   │
│      property's region (e.g. "Chief Development Officer,        │
│      Caribbean & Latin America" at Marriott — they signed the   │
│      management agreement and oversee brand integration into    │
│      pre-opening; key contact for pre-opening uniform decisions)│
│   5. Pre-Opening Project Director at the brand (pre-opening)    │
│ Note: Brand sets STRICT standards (specific fabric weights,     │
│   colors, suppliers). Property executes within them but picks   │
│   the actual vendor relationship. The brand parent here IS the  │
│   operator — their regional team is management_corporate scope, │
│   NOT chain_corporate.                                          │
│ ⚠️ DO NOT default to owners/developers for Model D. Even if the │
│   property is pre-opening and only owners are findable in press, │
│   the brand has a regional ops team that exists right now and is │
│   reachable. Search Marriott / Four Seasons / Aman / etc.       │
│   regional ops on LinkedIn before falling back to owners.       │
├─────────────────────────────────────────────────────────────────┤
│ MODEL E — FRANCHISE (owner-franchisee runs the property)        │
│ Examples: most Holiday Inn / Express, many Best Western, many   │
│   Hampton Inn, La Quinta, Choice brands                         │
│ Buyer: FRANCHISEE OWNER + property GM                           │
│ Find: Franchisee owner (often a small hotel-investment group),  │
│   property GM, Dir Housekeeping                                 │
│ Note: Many of these are below 4-star — may be SKIP for JA       │
│   Uniforms. Confirm property is actually 4-star+ before chasing │
├─────────────────────────────────────────────────────────────────┤
│ MODEL F — ALL-INCLUSIVE (FULLY CENTRALIZED procurement)         │
│ Examples: Sandals, Beaches, Karisma, Palace Resorts, Bahia      │
│   Principe, Iberostar, RIU, Hard Rock all-inclusive, Royalton,  │
│   Excellence, Atelier, Couples Resorts, Club Med, Posadas       │
│   (Fiesta Americana / Live Aqua / Fiesta Inn)                   │
│ Buyer: CENTRALIZED PROCUREMENT at chain HQ                      │
│ Find — IN ORDER (call all of these for max coverage):           │
│   1. Property General Manager (specs needs, local input —       │
│      INFLUENCER not buyer; still keep as primary)               │
│   2. Director / VP / SVP of Procurement at chain HQ             │
│      (THE actual vendor relationship — owns the PO)             │
│   3. Chief Operating Officer / Director General at chain HQ     │
│      (oversees brand standards; uniform spec changes cross      │
│      their desk for operational sign-off)                       │
│   4. VP / Director of Development at chain HQ                   │
│      (for pre-opening properties, sets up vendor contracts      │
│      during launch phase — same pattern as Mauricio Elizondo    │
│      at Posadas, equivalent role in every Model F chain)        │
│ Note: Property GMs have ZERO buying authority — DO NOT contact  │
│   them as the SOLE primary. They influence specs but the chain  │
│   HQ procurement team awards the contract.                      │
│ Specific brand carve-outs:                                      │
│   • SANDALS / BEACHES → HPI (Hospitality Purveyors Inc., Miami) │
│     is the EXCLUSIVE procurement arm — one HPI relationship =   │
│     access to all ~20 Sandals/Beaches properties globally.      │
│   • POSADAS (Fiesta Americana / Live Aqua) → Mexico City HQ;    │
│     target Director Compras / Director de Procurement plus      │
│     Director General (COO) for brand-standards sign-off.        │
│     Family owners (Azcárraga family) approve but do not pick    │
│     vendors directly — operational layer is the actual buyer.   │
├─────────────────────────────────────────────────────────────────┤
│ MODEL G — CASINO RESORT                                         │
│ Examples: MGM, Wynn, Caesars, Hard Rock casino-attached, Sands, │
│   Las Vegas Sands, Boyd Gaming, Penn Entertainment              │
│ Buyer: CASINO GROUP'S CORPORATE PROCUREMENT                     │
│ Find: Director of Procurement at the casino group + property    │
│   Director of Housekeeping for spec input                       │
├─────────────────────────────────────────────────────────────────┤
│ MODEL H — PRE-OPENING, NO NAMED OPERATOR YET                    │
│ Signals: Construction phase, brand announced but no mgmt        │
│   company named, opening 12+ months out                         │
│ Buyer: DEVELOPER / OWNER (controls FF&E + OS&E budget)          │
│ Find: CEO / President / Managing Director at the developing or  │
│   owning entity                                                 │
│ Note: Once an operator is named, decision shifts to the         │
│   operator's pre-opening team (then becomes Model B/C/D)        │
└─────────────────────────────────────────────────────────────────┘

How to figure out the model from the lead context:
  - Brand name "Autograph Collection / Curio / Tribute / Tapestry" + named
    independent operator like "Crescent / Aimbridge / Highgate" → MODEL C
  - Brand "Sandals" or "Beaches" anywhere → MODEL F (target HPI)
  - Brand "Ritz-Carlton / Four Seasons / Aman / EDITION / St. Regis /
    Rosewood / Mandarin Oriental / Auberge / Park Hyatt" → MODEL D
  - Brand "Marriott / Hilton / Hyatt Regency / Sheraton / Westin /
    Conrad / InterContinental" (chain-direct, no soft-brand suffix) → MODEL B
  - Brand "RIU / Iberostar / Karisma / Palace / Bahia Principe / Royalton /
    Hard Rock all-inclusive / Excellence / Atelier / Fiesta Americana /
    Live Aqua / Fiesta Inn" → MODEL F
  - Brand "MGM / Wynn / Caesars / Sands / Boyd / Penn / Hard Rock casino" → MODEL G
  - No chain brand at all, founder named → MODEL A
  - Pre-opening + no operator named → MODEL H
  - Default if uncertain → MODEL B (branded full-service, property-level)

Mention which model you picked in the search_summary so we can audit.

═══════════════════════════════════════════════════════
STEP 2 — RUN THE SEARCH CASCADE FOR THAT MODEL
═══════════════════════════════════════════════════════

Once you've picked the model, search in CASCADE ORDER. The cascade
default is property-first (Tier 1 → Tier 2 → Tier 3), but Models F
and G start at Tier 2 because property has zero authority for those.
Models A, B, C, D, E, H all start at Tier 1.

JA Uniforms sells uniforms TO HOTELS. For most operating models, the
people who actually buy uniforms work AT THE HOTEL — Director of
Housekeeping orders 30-50% of staff uniforms, the GM signs the PO,
the Executive Chef specs the F&B uniforms. Talk to THEM first unless
the operating model says otherwise.

Run searches in CASCADE ORDER. Only drop to the next tier when the
previous tier genuinely returns no qualified contacts at this property.
Do NOT skip a tier because the next one is easier to find.

╔══════════════════════════════════════════════════════════════════╗
║ TIER 1 — PROPERTY-LEVEL  (PRIMARY for Models A, B, C, D, E, H)   ║
╠══════════════════════════════════════════════════════════════════╣
║ Operational staff working AT {hotel_name}:
║
║   • Director of Housekeeping / Executive Housekeeper
║       └─ THE primary uniform buyer (housekeeping = 30-50% of
║          headcount, biggest uniform line item)
║   • General Manager / Hotel Manager / Resort Manager
║       └─ Approves vendor + signs PO
║   • Director of Human Resources / Director of People & Culture /
║     HR Director / People & Culture Director
║       └─ Owns the uniform PROGRAM — every new hire gets uniforms
║          through HR onboarding; HR sets dress-code policy and
║          owns staff appearance standards. CRITICAL CONTACT for
║          uniform vendor decisions, especially at pre-opening
║          where HR sets up the entire onboarding pipeline.
║   • Director of Purchasing / Purchasing Manager
║       └─ Issues POs day-to-day
║   • F&B Director / Director of Food & Beverage
║       └─ Specs F&B staff uniforms
║   • Executive Chef / Director of Culinary
║       └─ Specs kitchen uniforms
║   • Director of Operations / Director of Rooms
║       └─ Backup decision-maker / approver
║
║ For PRE-OPENING properties:
║   An "incoming" or "pre-opening" version of any role above IS
║   property-level. The announcement IS the buying moment.
║
║ How to confirm someone is property-level:
║   - Their LinkedIn / press release / bio names "{hotel_name}"
║     specifically (not just the brand or chain)
║   - Their job title contains the hotel name or distinctive city
║   - Their employer is a mgmt-company BUT their headline names
║     this specific hotel (textbook GM-via-operator pattern)
╚══════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════╗
║ TIER 2 — MANAGEMENT COMPANY CORPORATE  (BACKUP only)             ║
╠══════════════════════════════════════════════════════════════════╣
║ Run these searches ONLY when Tier 1 returns nobody at this
║ property. Corporate procurement does not pick fabric or fit;
║ they rubber-stamp the property's pick. Use them only as a route
║ in when no property-level contact exists yet.
║
║ At {management_company or "the operator (find from press/web)"}:
║   • SVP / VP / Director of Procurement
║   • SVP / VP / Director of Operations
║   • Pre-Opening Director (pre-opening properties only)
╚══════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════╗
║ TIER 3 — OWNER / DEVELOPER  (LAST RESORT — pre-opening only)     ║
╠══════════════════════════════════════════════════════════════════╣
║ Owner/developer entities are check-writers, not operators. Use
║ them only when Tiers 1 + 2 produce nothing AND the property is
║ pre-opening (developer controls FF&E + uniform spend before the
║ operator's pre-opening team is hired).
║
║   • Principal / Managing Partner at {owner or "the owning entity"}
║   • CEO / President at {developer or "the developing entity"}
╚══════════════════════════════════════════════════════════════════╝

EXCEPTION — when Tier 1 is structurally absent:
  - All-inclusive resorts: procurement is fully centralized at corporate;
    property GMs have ZERO buying authority. Tier 2 IS the primary.
  - GPO-locked brands (Avendra etc.): corporate procurement is locked
    in to mandated suppliers; pivot to property-level Director of
    Housekeeping who has flexibility on non-mandated items like uniforms.
  - Independent / boutique / founder-led: founders ARE the buyers (no
    corporate layer exists). Treat the founder as Tier 1.

⚠️ DO NOT include generic CEO / Founder / Board Member / Investor
unless they are explicitly the property owner or developer with budget
control AND Tier 1 returned nobody.

⚠️ HOWEVER — owner-side HOTEL-OPERATIONS execs are NOT "generic" and
ARE always valid contacts. When the lead has a known owner or developer
(see OWNER / DEVELOPER context above) and the owner entity has hotel-
specific titles, INCLUDE THEM. Examples that ARE valid (do NOT skip):
  • VP / SVP / EVP of Hotels at the owner REIT
  • VP / Director of Hotel Operations at the owner / developer
  • Director of Hotel Finance at the owner
  • Director of Construction / MD of Construction at the owner
These are owner-side staff who specifically run the hotel portfolio
and have FF&E/OS&E budget authority. They are TIER 3 contacts (owner
side) but they ARE the buyers when no operator is in place yet.

⚠️ NEVER return zero contacts if the lead has ANY of: a known owner,
developer, management_company, or brand. There is ALWAYS someone to
contact. At minimum, return one of:
  - The property GM (especially for OPEN or REOPENING properties — the
    GM exists publicly and is announced via the operator's PR or trade
    press, ALWAYS findable)
  - The owner-side hotel-portfolio execs (VP Hotel Operations etc.)
  - The owner / developer principals (Chairman, Founder, Managing Partner)
  - The management company's regional VP/SVP of Operations or President
  - The brand parent's regional Development VP or CDO who signed the deal

Returning empty when ANY of these entities is known on the lead means
the search wasn't thorough enough. RETRY by searching:
  - "{hotel_name}" "General Manager" appointment OR named OR joins
  - site:linkedin.com/in "{hotel_name}" "General Manager"
  - the operator's website /leadership or /executive-team page
  - "{management_company}" "President" OR "SVP Operations"

⚠️ FOR REOPENING PROPERTIES SPECIFICALLY: the property already operates
or has operated, the entire staff exists, the GM is named publicly. If
your search returns 0 contacts on a reopening property, you missed
something — search for the property's pre-closure GM, who very likely
returns post-reopening, and search for the SVP of Operations at the
chain who oversees the recovery / reopening.

{approach_note}

═══════════════════════════════════════════════════════
SEARCH PLAN
═══════════════════════════════════════════════════════
{li_block}

Also search: press releases, appointment announcements, trade publications
(Hotel Management, Hospitality Net, HotelNewsNow, Skift), contact directories
(RocketReach, ZoomInfo, SignalHire), hotel/operator websites — but ALWAYS
filter by Tier 1 (property-level) before falling back to Tier 2.

═══════════════════════════════════════════════════════
RETURN FORMAT
═══════════════════════════════════════════════════════
Return up to 8 contacts as JSON, in CASCADE ORDER (Tier 1 first, then
Tier 2 only if Tier 1 didn't fill, then Tier 3 only if Tiers 1+2 didn't):

{{
  "operating_model_inferred": "A|B|C|D|E|F|G|H — which model fits this hotel from STEP 1",
  "model_reasoning": "1 sentence — why you chose that model based on the brand/operator/context",
  "contacts": [
    {{
      "name": "Full Name",
      "title": "Current Job Title",
      "organization": "Their employer (hotel OR management company OR owner entity)",
      "linkedin_url": "https://linkedin.com/in/slug — include if found, else null",
      "email": "work@email.com — only if publicly available, else null",
      "tier": "property|mgmt_corporate|owner_developer|brand_corporate",
      "decision_role": "specifier|buyer|approver",
      "why_relevant": "1 sentence — name THIS hotel and explain why this person buys uniforms for it given the operating model (e.g. 'Director of Housekeeping at {hotel_name} — Model C soft-brand operator, day-to-day uniform spec lives at property')",
      "confidence": "high|medium|low"
    }}
  ],
  "search_summary": "Which model you picked + which tier each contact came from + what you searched"
}}

CONFIDENCE:
- high = Named in press release, official website, or trade article with title + this hotel
- medium = Found on LinkedIn with clear current role at this hotel
- low = Inferred from context

RANKING WITHIN THE RESPONSE (most important first — DO NOT REORDER):
1. Property-level Director of Housekeeping / Executive Housekeeper at {hotel_name}
   (Models A, B, C, D — primary spec authority for uniforms)
2. Property-level General Manager / Hotel Manager at {hotel_name} (incl. incoming GM)
   (Models A, B, C, D — vendor approval authority)
3. Property-level Director of Purchasing / F&B Director / Exec Chef at {hotel_name}
   (Models A, B, C, D — purchasing execution + F&B uniform specs)
4. Mgmt-company / chain corporate procurement / operations
   (Models C, F, G — primary for centralized models; Tier 2 backup elsewhere)
5. Owner / Developer principal
   (Models A, H — primary for independent / pre-opening with no operator)
6. Brand corporate
   (extreme last resort — only when no other tier produced a name)

For MODELS F and G (centralized), the order flips: corporate procurement
goes first because property has zero authority.

If Tier 1 only returns 2 property contacts, fill remaining 6 slots from Tier 2.
If Tier 1 returns 8+ property contacts, return all 8 from Tier 1 — do NOT pad
with mgmt corporate just to hit a quota.

Return ONLY valid JSON — no preamble, no markdown fences.
If no contacts found at all: {{"operating_model_inferred": "X", "model_reasoning": "...", "contacts": [], "search_summary": "reason"}}"""


async def _enrich_contacts_grounded(
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    developer: Optional[str],
    owner: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    opening_date: Optional[str],
) -> Optional[list[dict]]:
    """One-shot grounded contact enrichment via Gemini googleSearch.

    Returns list of contact dicts (2+ found) or None to trigger pipeline fallback.

    Uses the us-central1 regional endpoint — global endpoint produces phantom
    grounding (zero citations, unverified data) for the same reason as in
    lead_data_enrichment.py.
    """
    import time as _time

    try:
        from app.services.gemini_client import get_gemini_headers
        from app.services.ai_client import _get_config, _ensure_init

        _ensure_init()
        config = _get_config()
        project = config["vertex_project_id"]
        model = config["model"]
        grounding_location = "us-central1"
        url = (
            f"https://{grounding_location}-aiplatform.googleapis.com/v1/"
            f"projects/{project}/locations/{grounding_location}/"
            f"publishers/google/models/{model}:generateContent"
        )
        headers = get_gemini_headers()
    except Exception as e:
        logger.warning(f"Contact grounding: cannot build URL/headers: {e}")
        return None

    prompt = _build_contact_grounding_prompt(
        hotel_name=hotel_name,
        management_company=management_company,
        developer=developer,
        owner=owner,
        brand=brand,
        city=city,
        state=state,
        country=country,
        opening_date=opening_date,
    )

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,  # required for grounding per Vertex docs
            "maxOutputTokens": 8192,  # bigger prompt (model map + cascade) needs room
            # Skip extended thinking for this focused JSON-output task. With the
            # operating-model map + cascade in the prompt, thinking was burning
            # 1-2K output tokens on reasoning that's already specified in the
            # prompt, leaving no room for the actual JSON. Result was empty
            # `text` field with finishReason=MAX_TOKENS → JSON parse failed.
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    _get_client()  # ensure client is initialized
    start = _time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
            resp = await gc.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        logger.warning(
            f"Contact grounding TIMEOUT after {_time.monotonic()-start:.1f}s "
            f"for '{hotel_name}' — falling back to pipeline"
        )
        return None
    except Exception as e:
        logger.warning(
            f"Contact grounding ERROR for '{hotel_name}': {e} — falling back"
        )
        return None

    elapsed = _time.monotonic() - start

    # Defensive parsing — Gemini can return:
    #   1) Normal: candidates[0].content.parts[0].text = JSON string
    #   2) MAX_TOKENS / SAFETY blocked: candidates[0].finishReason set, parts empty
    #   3) Bad shape: missing candidates entirely
    # We capture finishReason in every error path so token-cap / safety blocks
    # are diagnosable from logs without needing to repro the call.
    try:
        candidate = data["candidates"][0]
        finish_reason = candidate.get("finishReason", "UNKNOWN")
        parts = (candidate.get("content") or {}).get("parts") or []
        content_text = (parts[0].get("text") if parts else "") or ""
        content_text = content_text.strip()
    except (KeyError, IndexError) as e:
        logger.warning(
            f"Contact grounding: bad response shape for '{hotel_name}': {e}. "
            f"data keys={list(data.keys())}"
        )
        return None

    if not content_text:
        logger.warning(
            f"Contact grounding: EMPTY text for '{hotel_name}' "
            f"(finishReason={finish_reason}). "
            f"If MAX_TOKENS, bump maxOutputTokens; if SAFETY, check prompt content."
        )
        return None

    # Strip markdown fences if Gemini wrapped the JSON
    if content_text.startswith("```"):
        parts_split = content_text.split("```")
        if len(parts_split) >= 2:
            inner = parts_split[1]
            if inner.startswith("json"):
                inner = inner[4:].strip()
            content_text = inner.strip().rstrip("`").strip()

    try:
        parsed = json.loads(content_text)
    except json.JSONDecodeError as e:
        preview = content_text[:200].replace("\n", " ")
        logger.warning(
            f"Contact grounding: JSON parse failed for '{hotel_name}': {e}. "
            f"finishReason={finish_reason}, text[:200]={preview!r}"
        )
        return None

    if not isinstance(parsed, dict):
        return None

    contacts_raw = parsed.get("contacts", [])
    if not isinstance(contacts_raw, list):
        return None

    # Capture source citations — both title AND uri from groundingChunks
    grounding_meta = candidate.get("groundingMetadata", {}) or {}
    sources = []  # titles (for logging/display)
    source_urls = []  # actual URLs (for evidence links)
    for chunk in (grounding_meta.get("groundingChunks", []) or [])[:5]:
        web = chunk.get("web", {}) or {}
        title = web.get("title")
        uri = web.get("uri") or ""
        # Vertex AI grounding URIs are vertexaisearch redirects — strip to base URL
        # e.g. "https://vertexaisearch.cloud.google.com/grounding-api-redirect/..."
        # These ARE clickable and open the real source page, so we keep them as-is.
        if title:
            sources.append(title)
            source_urls.append(uri)  # may be empty string if not provided

    logger.info(
        f"Contact grounding raw for '{hotel_name}': {len(contacts_raw)} contacts, "
        f"{len(sources)} citations. "
        f"Model={parsed.get('operating_model_inferred', '?')} "
        f"({(parsed.get('model_reasoning') or '')[:80]}). "
        f"Summary: {parsed.get('search_summary', '')[:100]}"
    )

    # ── PHANTOM GROUNDING CHECK ──
    # If groundingChunks is empty, Gemini answered from training data instead
    # of actually searching Google. Contact data is especially risky as phantom —
    # people change roles, training data is stale, hallucinated names are common.
    # Unlike lead_data_enrichment.py (where we trust high-confidence phantoms for
    # well-documented properties), contact grounding ALWAYS requires real citations.
    # People change jobs; a 2023 GM may be at a different hotel by 2026.
    if len(sources) == 0:
        # Log the phantom contacts so we can inspect them in the logs
        for i, pc in enumerate(contacts_raw[:5], 1):
            if isinstance(pc, dict):
                logger.info(
                    f"  PHANTOM contact {i}: {pc.get('name','?')} | "
                    f"{pc.get('title','?')} | {pc.get('organization','?')} | "
                    f"role={pc.get('decision_role','?')} conf={pc.get('confidence','?')} | "
                    f"{(pc.get('why_relevant') or '')[:80]}"
                )

        # ── TIERED PHANTOM POLICY ──
        # Zero citations = Gemini answered from training data.
        # For MANAGEMENT COMPANY contacts this is often reliable — a named
        # SVP of Procurement at Hotel Equities or Crescent Hotels is a real
        # person documented in press releases that Gemini learned. Trust if:
        #   - All contacts are from the known management_company OR developer OR owner
        #   - All contacts have confidence=high
        #   - At least 2 contacts found
        # Otherwise fall back to iterative pipeline.
        known_orgs = set(
            filter(
                None,
                [
                    (management_company or "").lower().strip(),
                    (developer or "").lower().strip(),
                    (owner or "").lower().strip(),
                ],
            )
        )
        high_conf_contacts = [
            c
            for c in contacts_raw
            if isinstance(c, dict) and (c.get("confidence") or "").lower() == "high"
        ]
        all_from_known_org = (
            all(
                any(
                    org in (c.get("organization") or "").lower()
                    for org in known_orgs
                    if org
                )
                for c in high_conf_contacts
            )
            if (known_orgs and high_conf_contacts)
            else False
        )

        is_trustworthy = len(high_conf_contacts) >= 2 and all_from_known_org

        if is_trustworthy:
            logger.warning(
                f"Contact grounding PHANTOM-TRUSTED for '{hotel_name}' — "
                f"{len(high_conf_contacts)} high-confidence contacts all from known "
                f"org(s) {known_orgs}. Management company contacts are reliably "
                f"documented in training data. Proceeding without citations."
            )
            # fall through — use the contacts
        else:
            logger.warning(
                f"Contact grounding PHANTOM for '{hotel_name}' — Gemini returned "
                f"{len(contacts_raw)} contacts but ZERO source citations. "
                f"Conditions for trust not met (known_orgs={known_orgs}, "
                f"high_conf={len(high_conf_contacts)}, all_from_known={all_from_known_org}). "
                f"Falling back to iterative pipeline."
            )
            return None

    # Filter: must have a real name
    valid_contacts = [
        c
        for c in contacts_raw
        if isinstance(c, dict)
        and (c.get("name") or "").strip()
        and len((c.get("name") or "").strip()) > 3
        and _looks_like_real_person((c.get("name") or "").strip())
    ]

    if len(valid_contacts) < _CONTACT_GROUNDING_MIN_CONTACTS:
        logger.info(
            f"Contact grounding: only {len(valid_contacts)} valid contacts for "
            f"'{hotel_name}' (min {_CONTACT_GROUNDING_MIN_CONTACTS}) — falling back to pipeline"
        )
        return None

    logger.info(
        f"Contact grounding SUCCESS for '{hotel_name}' in {elapsed:.1f}s: "
        f"{len(valid_contacts)} contacts, {len(sources)} sources"
    )

    # Convert to internal contact dict format
    mgmt_lower = (management_company or "").lower()
    hotel_lower = hotel_name.lower()
    result_contacts = []
    for c in valid_contacts:
        org = (c.get("organization") or hotel_name).strip()
        org_lower = org.lower()
        confidence = (c.get("confidence") or "medium").lower()
        if confidence not in ("high", "medium", "low"):
            confidence = "medium"

        # Assign scope based on organization
        if mgmt_lower and (mgmt_lower in org_lower or org_lower in mgmt_lower):
            scope = "management_corporate"
        elif any(w in org_lower for w in hotel_lower.split() if len(w) > 3):
            scope = "hotel_specific"
        else:
            scope = "chain_area"

        # Map decision_role → scope hint for scorer
        # specifier (housekeeping) and buyer (purchasing) are hotel_specific ops
        # approver (GM/owner) may be management_corporate or owner
        decision_role = (c.get("decision_role") or "").lower().strip()
        if decision_role == "approver" and scope == "management_corporate":
            scope = "management_corporate"
        elif decision_role == "approver" and "owner" in (c.get("title") or "").lower():
            scope = "owner"

        # ── LinkedIn URL sanity check ──
        # Grounding sometimes returns crossed or hallucinated LinkedIn URLs.
        # Validate: the URL slug must contain at least one token from the
        # person's name. If not, null it out — the LinkedIn lookup pass
        # below will find the correct URL via Serper.
        raw_li = (c.get("linkedin_url") or c.get("linkedin") or "").strip()
        validated_li = None
        if raw_li and "linkedin.com/in/" in raw_li:
            slug = raw_li.lower().split("linkedin.com/in/")[-1].rstrip("/")
            name_tokens = [
                t for t in (c.get("name") or "").lower().split() if len(t) >= 3
            ]
            if name_tokens and any(t in slug for t in name_tokens):
                validated_li = raw_li
            else:
                logger.info(
                    f"Grounding LinkedIn URL rejected for {c.get('name')!r}: "
                    f"slug {slug!r} contains none of {name_tokens} — "
                    f"will re-lookup via Serper"
                )

        contact = {
            "name": (c.get("name") or "").strip(),
            "title": _clean_title(c.get("title") or ""),
            "organization": org,
            "linkedin": validated_li,
            "email": c.get("email"),
            "phone": None,
            "scope": scope,
            "confidence": confidence,
            "confidence_note": c.get("why_relevant") or "Found via grounding search",
            "source": sources[0] if sources else "[grounding]",
            "source_type": "grounding",
            "_decision_role": decision_role,  # specifier | buyer | approver
            "_raw_snippet": c.get("why_relevant") or "",
            "_raw_title": c.get("title") or "",
            "_evidence_items": [
                {
                    "quote": c.get("why_relevant") or "",
                    "source_url": source_urls[0] if source_urls else "",
                    "source_title": sources[0] if sources else "Grounding search",
                    "source_domain": sources[0].split(" ")[0]
                    if sources
                    else "grounding",
                    "trust_tier": "official" if confidence == "high" else "trade",
                    "source_year": None,
                }
            ]
            if sources
            else [],
        }
        result_contacts.append(contact)

    return result_contacts


async def _layer_web_search(
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    opening_date: Optional[str],
    result: EnrichmentResult,
    retry_attempt: int = 0,
    phase: int = 0,
    project_type: str = "unknown",
) -> bool:
    """Layer 1: Search web using smart queries, scrape articles, extract contacts.

    Args:
        phase: Starting phase from project_type_intelligence (1, 2, or 3).
               Pass 0 for default (Phase 1 / Phase 3 based on opening proximity).
        project_type: Result of classify_project_type — "new_opening",
               "renovation", "rebrand", "ownership_change", or "unknown".
               Drives phase-specific query templates.
    """
    result.layers_tried.append(f"web_search_attempt_{retry_attempt}")

    location = _build_location_string(city, state, country)

    # ── SMART QUERY BUILDER — replaces hardcoded queries ──
    queries = query_builder.build_queries(
        hotel_name=hotel_name,
        brand=brand,
        management_company=management_company or result.management_company,
        city=city,
        state=state,
        country=country,
        mode=_get_search_mode(opening_date),
        retry_attempt=retry_attempt,
        phase=phase,
        project_type=project_type,
    )

    all_urls = []
    for query in queries:
        logger.info(f"Web search (attempt {retry_attempt}): {query}")
        search_results = await _search_web(query, max_results=5)
        for sr in search_results:
            if sr["url"] not in [u["url"] for u in all_urls]:
                all_urls.append(sr)
        has_serper = bool(os.getenv("SERPER_API_KEY"))
        delay = (
            ENRICHMENT_SETTINGS["serper_delay_seconds"]
            if has_serper
            else ENRICHMENT_SETTINGS["ddg_delay_seconds"]
        )
        await asyncio.sleep(delay)

    if not all_urls:
        logger.info(f"No web results for {hotel_name} (attempt {retry_attempt})")
        return False

    # Prioritize hospitality news sources
    def _source_priority(item):
        url_lower = item["url"].lower()
        for i, domain in enumerate(HOSPITALITY_NEWS_DOMAINS):
            if domain in url_lower:
                return i
        return 100

    all_urls.sort(key=_source_priority)

    # ── BRAND-AWARE CORPORATE FILTERING ──
    # Default behavior: filter out corporate/founder/C-suite titles because
    # for chain-managed brands they're locked behind GPOs.
    # For independent / founder-led brands, corporate IS the buyer — so we
    # keep them. This decision flag is checked when applying the filter below.
    skip_corporate_filter = False
    try:
        layer1_brand_info = BrandRegistry.lookup(brand) if brand else None
        if layer1_brand_info:
            uniform_freedom = (layer1_brand_info.uniform_freedom or "").lower()
            procurement_model = (layer1_brand_info.procurement_model or "").lower()
            opportunity = (layer1_brand_info.opportunity_level or "").lower()
            is_independent = (
                uniform_freedom in ("high", "full")
                or procurement_model
                in ("fully_open", "independent", "owner_decides", "open")
                or opportunity == "high"
            )
        else:
            # No brand registry entry — check if this is an independent/boutique hotel.
            # "Independent", blank brand, or unrecognized brand = founder-led.
            # At these hotels, the CEO/founder/principal IS the uniform buyer.
            brand_lower = (brand or "").lower().strip()
            is_independent = (
                not brand_lower
                or brand_lower == "independent"
                or brand_lower == "boutique"
                or brand_lower == "lifestyle"
            )
            uniform_freedom = "high" if is_independent else ""
            procurement_model = "independent" if is_independent else ""
            opportunity = ""

            if is_independent:
                skip_corporate_filter = True
                logger.info(
                    f"Brand {brand!r} is independent/boutique "
                    f"(no registry entry) — keeping corporate/regional contacts"
                )
    except Exception as ex:
        logger.debug(f"Could not resolve brand for corporate filter: {ex}")

    found_contacts = False
    for item in all_urls[: ENRICHMENT_SETTINGS["max_articles_to_scrape"]]:
        url = item["url"]
        logger.info(f"Scraping: {url}")

        article_text = await _scrape_url(url)
        if not article_text or len(article_text) < 100:
            continue

        extracted = await _extract_contacts_with_gemini(
            article_text, hotel_name, location
        )
        if not extracted:
            continue

        result.sources_used.append(url)

        for contact in extracted.get("contacts", []):
            name = contact.get("name", "")
            scope = contact.get("scope", "unknown")

            if scope in ("wrong_hotel", "irrelevant"):
                logger.info(f"Filtered out: {name} [{scope}]")
                continue

            if not name or len(name) < 3:
                continue

            if _is_corporate_title(contact.get("title", "")):
                if skip_corporate_filter:
                    # Independent / founder-led brand — corporate IS the buyer.
                    # Don't reject. Tag scope so downstream knows this is a
                    # corporate-level contact, not a property-specific one.
                    logger.info(
                        f"KEEPING corporate (independent brand): {name} "
                        f"({contact.get('title')})"
                    )
                    if contact.get("scope") == "unknown" or not contact.get("scope"):
                        contact["scope"] = "chain_corporate"
                else:
                    logger.info(
                        f"Filtered out: {name} (corporate title: {contact.get('title')})"
                    )
                    # Stash as fallback — if enrichment ends with zero contacts,
                    # a corporate/founder contact at a small brand is better than nothing.
                    contact["_fallback_reason"] = (
                        f"corporate_title: {contact.get('title')}"
                    )
                    contact["source"] = url
                    contact["source_type"] = "press_release"
                    result.fallback_contacts.append(contact)
                    continue

            if _is_irrelevant_org(contact.get("organization", "")):
                logger.info(
                    f"Filtered out: {name} (irrelevant org: {contact.get('organization')})"
                )
                continue

            contact["source"] = url
            contact["source_type"] = "press_release"
            contact["title"] = _clean_title(contact.get("title", ""))

            if "scope" not in contact:
                contact["scope"] = "unknown"
            if "confidence" not in contact:
                contact["confidence"] = "medium"
            if "confidence_note" not in contact:
                contact["confidence_note"] = "Extracted from web article"

            result.contacts.append(contact)
            found_contacts = True

        if extracted.get("management_company") and not result.management_company:
            result.management_company = extracted["management_company"]
        if extracted.get("developer") and not result.developer:
            result.developer = extracted["developer"]
        if extracted.get("opening_update"):
            result.opening_update = extracted["opening_update"]
        if extracted.get("additional_details"):
            result.additional_details = extracted["additional_details"]

    return found_contacts


# ═══════════════════════════════════════════════════════════════
# LAYER 2: LINKEDIN SNIPPET EXTRACTION
# ═══════════════════════════════════════════════════════════════


async def _layer_linkedin_snippets(
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    result: EnrichmentResult,
) -> bool:
    """Layer 2: Extract contact names/titles from LinkedIn search snippets."""
    result.layers_tried.append("linkedin_snippets")

    # ── SMART QUERIES for LinkedIn ──
    location_str = ", ".join(
        filter(
            None,
            [
                city,
                state,
                country if country and country.upper() not in ("US", "USA") else None,
            ],
        )
    )
    # ── Broad LinkedIn queries only — Layer 1 already searched all 13 titles ──
    queries = [
        f"{hotel_name} General Manager OR Director site:linkedin.com",
        f"{hotel_name} Purchasing OR Housekeeping OR Operations site:linkedin.com",
        f"{hotel_name} Chef OR Laundry OR Rooms site:linkedin.com",
    ]

    # ── OPERATOR + REGION queries for corporate executives ──
    # FIX: Corporate execs (e.g. "VP Commercial Services Latin America Caribbean")
    # don't list individual hotels in their LinkedIn. They DO list the
    # operator parent ("Hyatt Inclusive Collection") and the regional
    # bucket ("Caribbean", "Latin America"). These queries target them.
    #
    # Why Layer 2 instead of Layer 1: Layer 1 scrapes full pages and skips
    # LinkedIn URLs entirely. Layer 2 already has rich snippet-extraction
    # logic for LinkedIn profiles — we just need to feed it the right
    # queries.
    try:
        from app.config.region_map import regional_terms_for_country

        # Resolve the best operator name to search by
        brand_info = BrandRegistry.lookup(brand) if brand else None
        operator_candidates = []
        if brand_info and brand_info.parent_company:
            # Strip parenthetical context like "(formerly AMR Collection)"
            parent_clean = brand_info.parent_company.split("(")[0].strip()
            operator_candidates.append(parent_clean)
        # Only add management_company if it's NOT shorter/more generic than parent
        if management_company:
            mgmt_clean = management_company.strip()
            already_have = any(
                mgmt_clean.lower() in cand.lower() or cand.lower() in mgmt_clean.lower()
                for cand in operator_candidates
            )
            if not already_have:
                operator_candidates.append(mgmt_clean)

        # Get up to 2 regional buckets (most-specific first)
        region_terms = regional_terms_for_country(country)[:2]

        # Build operator + region queries for corporate roles
        if operator_candidates and region_terms:
            corporate_titles = []
            if brand_info and brand_info.pre_opening_contact_titles:
                corporate_titles = brand_info.pre_opening_contact_titles[:4]

            # Prepend operator+region queries (most valuable — runs first)
            #
            # NO site:linkedin.com here. Corporate execs are named in trade
            # press (Travel Market Report, Hospitality Net, Hotelier
            # Magazine, operator newsroom press releases), NOT in property-
            # specific LinkedIn results. Property-anchored LinkedIn searches
            # in Layer 1 already cover the easy targets — Layer 2's job is
            # to surface the corporate names that LinkedIn searches miss.
            operator_queries = []
            for operator in operator_candidates[:1]:  # primary operator only
                for region in region_terms:
                    # Operator + region appointment / leadership sweep.
                    # Trade press articles literally name new corporate
                    # appointments by full name + title.
                    operator_queries.append(
                        f'{operator} {region} appointment OR leadership OR "vice president"'
                    )
                # Per-title operator searches (no region — title is
                # specific enough). Trade press uses these exact phrases.
                for title in corporate_titles[:3]:
                    operator_queries.append(f'"{operator}" "{title}"')
                # Title + region combo for highest-priority Phase 1 titles
                if corporate_titles and region_terms:
                    operator_queries.append(
                        f'"{operator}" "General Manager" {region_terms[0]}'
                    )

            queries = operator_queries + queries  # prepend → runs first
            logger.info(
                f"Layer 2: added {len(operator_queries)} operator+region queries "
                f"(operator={operator_candidates[0]!r}, "
                f"region={region_terms[0] if region_terms else 'none'!r})"
            )
    except Exception as ex:
        logger.warning(f"Failed to build operator+region queries: {ex}")

    # Hotel name variants to catch what Layer 1 missed
    short_hotel_name = re.sub(
        r"\s+(?:Resort|Hotel|Spa|Suites?|Residences?|Inn|Lodge|&)+(?:\s+(?:Resort|Hotel|Spa|Suites?|Residences?|Inn|Lodge|&))*\s*$",
        "",
        hotel_name,
        flags=re.IGNORECASE,
    ).strip()
    if short_hotel_name and short_hotel_name.lower() != hotel_name.lower():
        queries.append(
            f"{short_hotel_name} {location_str} hotel staff OR manager site:linkedin.com"
        )
    if brand and brand.lower() not in hotel_name.lower():
        queries.append(
            f"{brand} {location_str} hotel manager OR director site:linkedin.com"
        )
    # Parent/management company query
    parent = management_company or brand
    if parent:
        queries.append(f"{parent} {hotel_name} site:linkedin.com")

    found = False
    for query in queries:
        logger.info(f"LinkedIn snippet search: {query}")
        search_results = await _search_web(query, max_results=5)
        has_serper = bool(os.getenv("SERPER_API_KEY"))
        delay = (
            ENRICHMENT_SETTINGS["serper_delay_seconds"]
            if has_serper
            else ENRICHMENT_SETTINGS["ddg_delay_seconds"]
        )
        await asyncio.sleep(delay)

        for sr in search_results:
            url = sr.get("url", "")
            title = sr.get("title", "")
            snippet = sr.get("snippet", "")

            is_profile = "linkedin.com/in/" in url
            is_post = "linkedin.com/posts/" in url

            # ── CONTACT DIRECTORY SITES: RocketReach, SignalHire, ZoomInfo, Lusha ──
            is_contact_directory = any(
                domain in url
                for domain in [
                    "rocketreach.co",
                    "signalhire.com",
                    "zoominfo.com",
                    "lusha.com",
                    "rockreach.com",
                    "contactout.com",
                ]
            )

            if not is_profile and not is_post and not is_contact_directory:
                continue

            if is_contact_directory:
                cd_name = None
                cd_title = None
                cd_org = None

                # Multiple snippet patterns these sites use:
                snippet_patterns = [
                    # "Name, based in Location, is currently a Title at Org. Name brings..."
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3}),?\s+(?:based in .+?,\s+)?is (?:currently |a |the )?(.*?)\s+at\s+(.+?)(?:\.\s|\s+\w+ brings)",
                    # "Name is a Title at Org."
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s*(?:is|,)\s+(?:currently\s+)?(?:a\s+|the\s+)?(.*?)\s+at\s+(.+?)(?:\.|$)",
                    # "Name, Title at Org, has been..."
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3}),\s+(.*?)\s+at\s+(.+?)(?:,\s+has|\.\s|$)",
                    # "Name · Title · Org" (ZoomInfo style)
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s+·\s+(.*?)\s+·\s+(.+?)(?:\s+·|\.|$)",
                    # "Name - Title - Org" (some directory styles)
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s+-\s+(.*?)\s+-\s+(.+?)(?:\.|$)",
                ]

                for pattern in snippet_patterns:
                    cd_match = re.search(pattern, snippet)
                    if cd_match:
                        cd_name = cd_match.group(1).strip()
                        cd_title = cd_match.group(2).strip()
                        cd_org = cd_match.group(3).strip().rstrip(".")
                        break

                # Fallback: extract org from search result title
                # "Carlos Noboa | The Ritz-Carlton, Grand Cayman - RocketReach"
                if not cd_name and title:
                    title_match = re.match(
                        r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s*(?:\||[-–—])\s*(.+?)(?:\s*[-–—|]\s*(?:RocketReach|SignalHire|ZoomInfo|Lusha|ContactOut))",
                        title,
                    )
                    if title_match:
                        cd_name = title_match.group(1).strip()
                        rest = title_match.group(2).strip()
                        # "Email & Phone Number | Org" or just "Org"
                        rest_clean = re.sub(
                            r"(?:Email|Phone|Contact).*?(?:\||[-–—])\s*",
                            "",
                            rest,
                        ).strip()
                        if rest_clean:
                            cd_org = rest_clean

                if cd_name:
                    # Clean title — strip articles and "currently"
                    if cd_title:
                        # Strip "based in Location, is currently a" prefix
                        # Anchor on known title keywords to avoid eating good content
                        cd_title = re.sub(
                            r"^.*?(?:is\s+)?(?:currently\s+)?(?:a\s+|an\s+|the\s+)?(?=(?:Director|Manager|Head|Chief|Executive|Assistant|General|Resort|Hotel|Front|Purchasing|Operations|Housekeeping|Coordinator|Supervisor)\b)",
                            "",
                            cd_title,
                            flags=re.IGNORECASE,
                        ).strip()
                        # Strip trailing org after dash: "Director Of Housekeeping - Kimpton Seafire"
                        cd_title = re.sub(
                            r"\s*[-–—]\s+.*$",
                            "",
                            cd_title,
                        ).strip()
                        # Final cleanup of remaining articles
                        cd_title = re.sub(
                            r"^(?:a|an|the)\s+",
                            "",
                            cd_title,
                            flags=re.IGNORECASE,
                        ).strip()

                    source_site = url.split("/")[2].replace("www.", "")

                    # Check if this person already exists — update title if missing
                    existing_names = [
                        c.get("name", "").lower() for c in result.contacts
                    ]
                    if cd_name.lower() in existing_names:
                        if cd_title:
                            for existing_c in result.contacts:
                                if (
                                    existing_c.get("name", "").lower()
                                    == cd_name.lower()
                                ):
                                    if not existing_c.get("title"):
                                        existing_c["title"] = cd_title
                                        if cd_org and not existing_c.get(
                                            "organization"
                                        ):
                                            existing_c["organization"] = cd_org
                                        logger.info(
                                            f"{source_site} title update: {cd_name} -> {cd_title}"
                                        )
                                    break
                        continue

                    # New contact
                    hotel_lower = hotel_name.lower()
                    cd_org_lower = (cd_org or "").lower()
                    hotel_words = [w for w in hotel_lower.split() if len(w) > 3]
                    org_matches = sum(1 for w in hotel_words if w in cd_org_lower)
                    match_ratio = org_matches / len(hotel_words) if hotel_words else 0

                    if match_ratio >= 0.5:
                        scope = "hotel_specific"
                        confidence = "medium"
                        confidence_note = (
                            f"{source_site} confirms {cd_title} at {cd_org}"
                        )
                    else:
                        scope = "chain_area"
                        confidence = "low"
                        confidence_note = (
                            f"{source_site}: {cd_org} (hotel match unclear)"
                        )

                    contact = {
                        "name": cd_name,
                        "title": cd_title or "",
                        "email": None,
                        "phone": None,
                        "linkedin": None,
                        "organization": cd_org or "",
                        "scope": scope,
                        "confidence": confidence,
                        "confidence_note": confidence_note,
                        "source": url,
                        "source_type": f"{source_site}_snippet",
                        "_raw_snippet": snippet,
                        "_raw_title": title,
                    }
                    result.contacts.append(contact)
                    result.sources_used.append(f"{source_site}: {cd_name}")
                    found = True
                    logger.info(
                        f"{source_site}: {cd_name} - {cd_title} at {cd_org} [{scope}]"
                    )
                continue

            name = None
            extracted_title = None
            org = ""
            linkedin_url = url  # For posts, we'll try to build profile URL

            if is_profile:
                # ── PROFILE URL: "Kara dePool - General Manager at The Nora Hotel | LinkedIn" ──
                # Also handles: "Steven Andre - Grand Hyatt Grand Cayman Resort & Spa - LinkedIn"
                m = re.match(
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s*[-\u2013\u2014]\s*(.+)",
                    title,
                )
                if m:
                    name = m.group(1).strip()
                    raw_rest = m.group(2).strip()

                    for sep in [" at ", " | ", " - "]:
                        if sep in raw_rest:
                            parts = raw_rest.split(sep, 1)
                            extracted_title = parts[0].strip()
                            remainder = parts[1].strip() if len(parts) > 1 else ""
                            org = re.sub(
                                r"\s*\|?\s*LinkedIn.*$",
                                "",
                                remainder,
                                flags=re.IGNORECASE,
                            ).strip()
                            break
                    else:
                        extracted_title = raw_rest

                    # ── FIX: Detect when "title" is actually an org/hotel name ──
                    # e.g. "Grand Hyatt Grand Cayman Resort & Spa" is NOT a job title
                    if extracted_title:
                        title_lower = extracted_title.lower()
                        org_indicators = [
                            "hotel",
                            "resort",
                            "hyatt",
                            "hilton",
                            "marriott",
                            "ihg",
                            "accor",
                            "four seasons",
                            "fairmont",
                            "westin",
                            "sheraton",
                            "waldorf",
                            "conrad",
                            "intercontinental",
                            "kimpton",
                            "rosewood",
                            "mandarin",
                            "peninsula",
                            "hospitality",
                            "group",
                            "collection",
                            "nora",
                        ]
                        is_org_not_title = any(
                            kw in title_lower for kw in org_indicators
                        )

                        # Also check: real titles have role words
                        role_words = [
                            "director",
                            "manager",
                            "chef",
                            "coordinator",
                            "supervisor",
                            "housekeeper",
                            "purchasing",
                            "operations",
                            "general manager",
                            "assistant",
                            "executive",
                            "buyer",
                            "vp",
                            "head of",
                            "ceo",
                            "coo",
                            "cfo",
                            "president",
                            "chairman",
                            "investor",
                            "founder",
                            "partner",
                            "board member",
                        ]
                        has_role_word = any(rw in title_lower for rw in role_words)

                        if is_org_not_title and not has_role_word:
                            # It's an org name, not a title — swap
                            org = extracted_title
                            extracted_title = ""

                            # Try to find actual title from snippet
                            snippet_lower = snippet.lower()
                            for rw in [
                                "chief executive officer",
                                "chief operating officer",
                                "chief financial officer",
                                "investor",
                                "board member",
                                "chairman",
                                "president",
                                "senior vice president",
                                "executive vice president",
                                "vice president",
                                "regional director",
                                "area director",
                                "svp",
                                "evp",
                                "ceo",
                                "coo",
                                "cfo",
                                "director of operations",
                                "director of food and beverage",
                                "director of food & beverage",
                                "director of housekeeping",
                                "director of procurement",
                                "director of purchasing",
                                "director of rooms",
                                "director of front office",
                                "director of banquets",
                                "director of catering",
                                "director of f&b",
                                "director of sales",
                                "assistant general manager",
                                "rooms division manager",
                                "general manager",
                                "resort manager",
                                "operations manager",
                                "executive housekeeper",
                                "purchasing manager",
                                "housekeeping manager",
                                "f&b director",
                                "front office manager",
                                "hotel manager",
                                "property manager",
                                "restaurant general manager",
                                "restaurants general manager",
                            ]:
                                if rw in snippet_lower:
                                    extracted_title = rw.title()
                                    break
                            if extracted_title:
                                logger.info(
                                    f"Title recovered from snippet: {name} -> {extracted_title}"
                                )

            elif is_post:
                # ── POST URL: "Kara dePool's Post - LinkedIn" ──
                # Name from title: "Kara dePool's Post"
                m = re.match(
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})(?:'s)?\s+Post",
                    title,
                )
                if m:
                    name = m.group(1).strip()

                # Title from snippet: look for role keywords
                snippet_lower = snippet.lower()
                if is_post:
                    # For POSTS: titles in text refer to OTHER people, not the poster
                    # e.g. "recently-named General Manager Brett Orlando" -> Brett = GM
                    mention_patterns = [
                        r"(?:recently[- ]?named|appointed|named|hired|announcing)\s+(?:our\s+)?(?:new\s+)?((?:general manager|director of \w+|executive housekeeper|resort manager|hotel manager|purchasing manager|operations manager|front office manager))\s+([A-Z][a-z]+\s+[A-Z][a-zA-Z]+)",
                        r"(?:our\s+)?(?:new\s+)?(general manager|director of \w+|executive housekeeper|resort manager|hotel manager|purchasing manager|operations manager)\s+([A-Z][a-z]+\s+[A-Z][a-zA-Z]+)",
                    ]
                    for mp in mention_patterns:
                        mm = re.search(mp, sr.get("snippet", ""), re.IGNORECASE)
                        if mm:
                            mentioned_title = mm.group(1).strip().title()
                            mentioned_name = mm.group(2).strip()
                            # Validate: real name must be 2+ capitalized words (not "for the", "at our", etc.)
                            name_words = mentioned_name.split()
                            is_real_name = (
                                len(mentioned_name) > 4
                                and len(name_words) >= 2
                                and all(w[0].isupper() for w in name_words)
                                and not any(
                                    w.lower()
                                    in (
                                        "the",
                                        "our",
                                        "for",
                                        "at",
                                        "as",
                                        "and",
                                        "or",
                                        "in",
                                        "of",
                                        "a",
                                        "an",
                                    )
                                    for w in name_words
                                )
                            )
                            if is_real_name:
                                existing = [
                                    c.get("name", "").lower() for c in result.contacts
                                ]
                                if mentioned_name.lower() not in existing:
                                    mentioned_contact = {
                                        "name": mentioned_name,
                                        "title": mentioned_title,
                                        "email": None,
                                        "phone": None,
                                        "linkedin": None,
                                        "organization": hotel_name,
                                        "scope": "hotel_specific",
                                        "confidence": "medium",
                                        "confidence_note": f"Mentioned in LinkedIn post by {name}",
                                        "source": url,
                                        "source_type": "linkedin_post_mention",
                                        "_raw_snippet": sr.get("snippet", ""),
                                        "_raw_title": title,
                                    }
                                    result.contacts.append(mentioned_contact)
                                    logger.info(
                                        f"Post mention extracted: {mentioned_name} - {mentioned_title}"
                                    )
                    # Poster gets NO title from post body
                    extracted_title = None
                else:
                    role_patterns = [
                        r"role of\s+([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                        r"(?:appointed|named|hired|joined)\s+(?:as\s+)?(?:the\s+)?([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                        r"(?:i am|i\'m|i\'ve)\s+(?:the\s+)?(?:new\s+)?([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                        r"stepped into the role of\s+([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                    ]
                    for pattern in role_patterns:
                        role_match = re.search(pattern, snippet_lower)
                        if role_match:
                            extracted_title = role_match.group(1).strip().title()
                            break

                # If no role found in snippet, try to extract from post URL slug
                # e.g. linkedin.com/posts/kara-depool-a69a85151_...
                if not extracted_title:
                    # Check snippet for common title keywords
                    for kw in [
                        "chief executive officer",
                        "investor",
                        "board member",
                        "chairman",
                        "president",
                        "senior vice president",
                        "vice president",
                        "ceo",
                        "coo",
                        "svp",
                        "general manager",
                        "director of",
                        "executive housekeeper",
                        "purchasing manager",
                        "operations manager",
                        "resort manager",
                    ]:
                        if kw in snippet_lower:
                            extracted_title = kw.title()
                            break

                # Try to build profile URL from post URL slug
                post_slug = re.search(r"linkedin\.com/posts/([a-z0-9-]+?)_", url)
                if post_slug:
                    linkedin_url = f"https://www.linkedin.com/in/{post_slug.group(1)}"

                logger.info(
                    f"LinkedIn post parsed: {name} - {extracted_title} (from post snippet)"
                )

            if not name or len(name) < 4:
                continue

            # ── Filter out non-person names (e.g. "UPDATE GROUP", "LinkedIn News") ──
            name_words = name.split()
            if len(name_words) < 2:
                continue  # Need at least first + last name
            if name.isupper():
                continue  # All caps = not a person name
            if any(
                w.lower()
                in {
                    "group",
                    "hotel",
                    "hotels",
                    "news",
                    "update",
                    "the",
                    "resort",
                    "company",
                    "district",
                    "post",
                    "linkedin",
                }
                for w in name_words
            ):
                continue  # Contains non-person words

            extracted_title = _clean_title(extracted_title or "")

            # Determine scope based on hotel name match
            hotel_lower = hotel_name.lower()
            combined_text = f"{title} {sr.get('snippet', '')}".lower()

            hotel_words = [w for w in hotel_lower.split() if len(w) > 3]

            # ── Use whole-word matching to avoid substring false positives ──
            # e.g. "dean" must NOT match "dean's italian steakhouse"
            # The negative lookahead (?!['\w]) excludes possessive/compound forms
            def _whole_word_match(word: str, text: str) -> bool:
                return bool(re.search(r"\b" + re.escape(word) + r"(?!['\w])", text))

            matches = sum(1 for w in hotel_words if _whole_word_match(w, combined_text))
            match_ratio = matches / len(hotel_words) if hotel_words else 0

            # ── Check for name collision BEFORE assigning hotel_specific ──
            name_lower = name.lower()
            name_parts = set(name_lower.split())
            hotel_word_set = set(w.lower() for w in hotel_words)
            has_name_collision = bool(name_parts & hotel_word_set)

            # ── Also check: is the person's NAME the reason for the match? ──
            # e.g. "Nora Cunningham" matches "Nora Hotel" because of "nora" in her name
            # Remove name words from the match count to get true hotel relevance
            true_matches = 0
            for hw in hotel_words:
                if _whole_word_match(hw, combined_text):
                    # Check if match is ONLY because of the person's name
                    text_without_name = combined_text.replace(name_lower, "")
                    if _whole_word_match(hw, text_without_name):
                        true_matches += 1
            true_match_ratio = true_matches / len(hotel_words) if hotel_words else 0

            if true_match_ratio >= 0.6 and not has_name_collision:
                scope = "hotel_specific"
                confidence = "medium"
                confidence_note = f"LinkedIn profile mentions {hotel_name}"
                if not org:
                    org = hotel_name
            elif match_ratio >= 0.6:
                # Match exists but might be from name collision — lower confidence
                scope = "hotel_specific" if not has_name_collision else "unknown"
                confidence = "medium" if not has_name_collision else "low"
                confidence_note = f"LinkedIn profile mentions {hotel_name}"
            else:
                scope = "chain_area"
                confidence = "low"
                confidence_note = (
                    "Found in LinkedIn search but hotel name not confirmed"
                )

            existing_names = [c.get("name", "").lower() for c in result.contacts]
            if name.lower() in existing_names:
                # If existing contact has no title but this one does, UPDATE it
                if extracted_title:
                    for existing_c in result.contacts:
                        if existing_c.get("name", "").lower() == name.lower():
                            if not existing_c.get("title"):
                                existing_c["title"] = extracted_title
                                existing_c["_raw_snippet"] = sr.get("snippet", "")
                                existing_c["_raw_title"] = title
                                if scope == "hotel_specific":
                                    existing_c["scope"] = scope
                                logger.info(
                                    f"Title updated for {name}: {extracted_title}"
                                )
                            break
                continue

            contact = {
                "name": name,
                "title": extracted_title or "",
                "email": None,
                "phone": None,
                "linkedin": linkedin_url,
                "organization": org,
                "scope": scope,
                "confidence": confidence,
                "confidence_note": confidence_note,
                "source": url,
                "source_type": "linkedin_snippet",
                "_raw_snippet": sr.get("snippet", ""),
                "_raw_title": title,
            }

            result.contacts.append(contact)
            result.sources_used.append(f"LinkedIn: {name}")
            found = True
            logger.info(f"LinkedIn: {name} - {extracted_title} [{scope}]")

    # ── TITLE RECOVERY PASS: search by name for contacts missing titles ──
    untitled = [c for c in result.contacts if not c.get("title") and c.get("name")]
    for contact in untitled[:3]:
        recovery_name = contact["name"]
        recovery_query = f'"{recovery_name}" "{hotel_name}"'
        logger.info(f"Title recovery search: {recovery_query}")
        recovery_results = await _search_web(recovery_query, max_results=5)
        has_serper = bool(os.getenv("SERPER_API_KEY"))
        delay = (
            ENRICHMENT_SETTINGS["serper_delay_seconds"]
            if has_serper
            else ENRICHMENT_SETTINGS["ddg_delay_seconds"]
        )
        await asyncio.sleep(delay)

        title_keywords = [
            "resort manager",
            "hotel manager",
            "general manager",
            "assistant general manager",
            "director of operations",
            "director of food and beverage",
            "director of food & beverage",
            "director of housekeeping",
            "executive housekeeper",
            "director of rooms",
            "rooms division manager",
            "purchasing manager",
            "operations manager",
            "front office manager",
            "director of procurement",
            "director of purchasing",
            "director of banquets",
            "director of catering",
            "director of f&b",
            "director of sales",
            "director of engineering",
            "spa director",
            "director of spa",
            "restaurant general manager",
            "restaurants general manager",
        ]

        for sr in recovery_results:
            snippet_lower = sr.get("snippet", "").lower()
            title_text = sr.get("title", "").lower()
            combined = f"{snippet_lower} {title_text}"
            # Name MUST appear in same snippet - prevents stealing another person's title
            name_parts = recovery_name.lower().split()
            name_in_snippet = any(
                part in combined for part in name_parts if len(part) > 2
            )
            if not name_in_snippet:
                continue
            for kw in title_keywords:
                if kw in combined:
                    contact["title"] = kw.title()
                    logger.info(f"Title recovered for {recovery_name}: {kw.title()}")
                    break
            if contact.get("title"):
                break

    return found


# ═══════════════════════════════════════════════════════════════
# GEMINI CONTACT VERIFICATION — AI reads context to fix false positives
# ═══════════════════════════════════════════════════════════════


CONTACT_VERIFICATION_PROMPT = """You are a hotel staffing verification expert for JA Uniforms, a hotel uniform supplier.

TARGET HOTEL: {hotel_name}
LOCATION: {location}
BRAND: {brand}
MANAGEMENT COMPANY: {management_company}
{hotel_status}

{procurement_guidance}

Below are contacts discovered during lead research. For EACH contact, determine:
1) Their ACTUAL job title (not someone else mentioned in a post)
2) Their ACTUAL employer/organization
3) Whether they are OPERATIONAL HOTEL STAFF at the target hotel

OPERATIONAL HOTEL STAFF includes: General Manager, Director of Housekeeping, Executive Housekeeper,
Purchasing Manager, Director of Operations, Director of Rooms, Front Office Manager, F&B Director,
Assistant GM, Resort Manager, Property Manager, Hotel Manager, Operations Manager, Housekeeping Manager,
Uniform Manager, Wardrobe Manager, Laundry Manager, Supply Chain Manager, Procurement Manager, Executive Chef,
Director of Purchasing, Director of Finance, Controller, Procurement Manager, Housekeeping Director.

GATEWAY CONTACTS (DO NOT REJECT — classify normally for scope):
- Director of People & Culture, Director of Talent & Culture, HR Director, HR Manager
- These may be kept or dropped later based on how many operational contacts we find.
- Classify their scope normally (hotel_specific vs chain_area) based on evidence.

NOT operational hotel staff (REJECT these):
- C-suite executives at the chain parent: CEO, COO, CFO, Chairman, Board
  Member, Investor, Founder, President of the ENTIRE company
  (UNLESS the contact is at the lead's owner entity)
- Pure regional roles at the chain that aren't Development or Operations:
  Regional Director (sales/area), Area Manager, etc.
- Construction contractors, architects, project managers AT BUILDING FIRMS
  (working at architecture/contracting/engineering firms, NOT at the
  brand parent — see Development exception below)
- Pure marketing / branding / revenue / sales roles regardless of seniority:
  Director of Marketing, AVP Marketing, VP Marketing, Chief Marketing
  Officer, Director of Branding, Director of PR, Director of Revenue,
  Director of Key Accounts, National Sales Manager
- People at other hotels, not the target hotel
- People mentioned in a LinkedIn post who are NOT the post author

CONCRETE REJECT EXAMPLES (make sure these get rejected):
  • Mark Hoplamazian (CEO Hyatt Hotels Corporation) → REJECT
    Reason: CEO of the entire chain, too senior for property uniform buying
  • Joan Bottarini (CFO Hyatt Hotels Corporation) → REJECT
    Reason: CFO at brand parent, not owner-side, not procurement
  • Anthony Capuano (CEO Marriott International) → REJECT
  • Melanie Benozich (AVP Marketing & Global Branding, Hyatt Inclusive
    Collection) → REJECT
    Reason: Pure marketing role, doesn't manage uniformed staff

EXCEPTION — VP/Director of Development at the BRAND PARENT or MGMT COMPANY → KEEP:
"VP of Development", "SVP of Development", "Chief Development Officer",
"Regional VP Development" at the management company / brand parent
(Hyatt, Marriott, Hilton, IHG, Accor, Posadas, etc.) are NOT construction
contractors — they are the brand executives who sign management
agreements and own pre-opening vendor relationships. KEEP these as
corrected_scope="management_corporate".
  • Cristiano Goncalves (VP Development South America, Hyatt) → management_corporate
  • Camilo Bolaños (SVP Development LatAm/Caribbean, Hyatt) → management_corporate
  • Laurent de Kousemaeker (CDO Caribbean & LatAm, Marriott) → management_corporate
  • Mauricio Elizondo (VP Development, Posadas) → management_corporate
  • Maria Zarraluqui (SVP Hyatt Inclusive Collection Growth) → management_corporate

⚠️ The Development exception is NARROW — it only applies to titles
containing "Development" or "CDO". It does NOT extend to:
  - CFO / CEO / COO of the chain (still REJECT)
  - VP/Director of Marketing or Branding (still REJECT)
  - VP/Director of Sales (still REJECT, except Sales & Events / Catering /
    Banquets which manage uniformed event staff)

ONLY reject "Development" titles when the organization is clearly a
construction/architecture/contracting firm (e.g., HBA, Wimberly Interiors,
Gensler, Turner Construction) — those are vendors, not buyers.

CRITICAL RULE FOR LINKEDIN POSTS: If source_url contains /posts/, the contact name is the POST AUTHOR.
Titles mentioned in the snippet may refer to SOMEONE ELSE discussed in the post, NOT the author.
Read the snippet carefully to determine the post author actual role vs who they are writing about.

IMPORTANT RULES FOR KEEPING vs REJECTING:
ALWAYS REJECT these even if they appear connected to the hotel:
- C-suite/corporate at the BRAND PARENT level: CEO, COO, CFO, Chairman,
  President of the entire chain (Mark Hoplamazian at Hyatt, Anthony
  Capuano at Marriott — too senior for property-level uniform decisions)
- C-suite at OWNER entities → KEEP as corrected_scope="owner"
- Regional/area roles at NON-hotel-operator entities: Regional Director,
  SVP, Area Manager at construction firms or unrelated companies
- Construction: contractors, architects, project managers AT BUILDING
  FIRMS (NOT at the brand parent — see exception below)
- Pure marketing/revenue roles (Revenue Manager, National Accounts, PR Director)
  BUT KEEP: Director of Sales & Events, Director of Catering,
  Director of Banquets — they manage staff who NEED uniforms
- People confirmed to work at a DIFFERENT hotel than the target
- People with NO title whose snippet context mentions construction, building site, onsite progress, or groundbreaking
  (these are typically contractors visiting the construction site, NOT hotel operational staff)
- People whose ORGANIZATION name contains: Construction, Capital, Holdings,
  Investment, Architecture, Contracting, Consulting, Engineering
  (these are vendors/developers, NOT hotel operational staff)
  Exception 1: keep them if they hold a clear operational hotel title like
    General Manager or Director of Housekeeping
  Exception 2: keep them as scope="owner" if their org is the lead's owner
    or developer entity (e.g. Daniel Zuleta at Cotton Bay Holdings)

EXCEPTION — Brand-side Development execs are KEPT:
"VP/SVP/Director of Development" or "Chief Development Officer" at the
hotel chain (Hyatt, Marriott, Hilton, IHG, Posadas etc.) are KEPT as
management_corporate — they are brand executives who sign management
agreements and own pre-opening relationships, NOT construction
contractors.

CRITICAL: A contact MUST be confirmed at the EXACT target hotel to be kept as hotel_specific.
Same city is NOT enough. Same brand/chain is NOT enough.
"Director of Housekeeping in Miami" does NOT mean they work at the target hotel.
"Director of Operations at Auberge Resorts Collection" does NOT mean they work at the target Auberge property.

SAME-BRAND DIFFERENT-PROPERTY: If a contact works for the same brand/collection but at a DIFFERENT
property, they should be REJECTED. For example:
- Target: Shell Bay Club, Auberge → GM at Goldener Hirsch, Auberge = REJECT (different property)
- Target: Grand Hyatt Grand Cayman → Resort Manager at Hyatt Regency Chicago = REJECT (different property)
- Target: Westin Cocoa Beach → Housekeeping Manager at Westin Savannah = REJECT (different property)

If the snippet/organization ONLY mentions the parent brand (e.g. "Auberge Resorts Collection", "Hyatt",
"Marriott", "The Ritz-Carlton", "Marriott International") without naming the SPECIFIC target property,
set corrected_scope to "chain_area" not "hotel_specific".
The target property name "{hotel_name}" or its distinguishing location (e.g. "San Juan", "Cancun")
MUST appear in the snippet, organization, or profile URL for a contact to qualify as "hotel_specific".

SAME-CITY DIFFERENT-HOTEL: A contact working in the same city but at a DIFFERENT hotel is NOT at the target.
For example if the target is "The Ritz-Carlton, San Juan":
- "Director of Housekeeping at Marriott International" with no mention of San Juan = chain_area
- "Executive Housekeeper at Marriott Hotels" in Puerto Rico but no mention of Ritz-Carlton San Juan = REJECT
- "Director of Housekeeping at The Ritz-Carlton" with a Washington DC area code or DC location = chain_area (different property)
- A contact whose LinkedIn says "The Ritz-Carlton Spa, San Juan" but actually works at a different Marriott property = REJECT

BRAND-ONLY ORG RULE: If a contact's organization is ONLY a brand name or parent company
(e.g. "The Ritz-Carlton", "Marriott International", "Hilton", "Hyatt") without the specific
property name or city, they MUST be set to "chain_area" — NEVER "hotel_specific".
This applies even if they hold an operational title like Director of Housekeeping.

MANAGEMENT COMPANY RULE (CRITICAL — READ CAREFULLY):
Hotel operational staff are FREQUENTLY employed by a third-party management company rather than
by the hotel brand directly. The management company is the LEGAL EMPLOYER, but the person still
works at the target hotel. Common management companies include: Aimbridge Hospitality, Highgate,
Crescent Hotels, Davidson Hospitality, HEI Hotels, Concord Hospitality, Pyramid Hotel Group,
Interstate Hotels, Extell Hospitality Services, Palladium Hotel Group, Hyatt Hotels Corporation
(when managing non-Hyatt brands), and many others. These names usually contain words like
"Hospitality", "Hotels", "Hotel Group", "Hotel Management", or "Hospitality Services".

The MANAGEMENT COMPANY / OPERATOR(s) for this lead is: {management_company}
(This may list multiple companies separated by " / " — contacts from ANY of them are valid.)

RULES:
- If a contact's organization matches ANY of the management companies/operators listed above
  (or is clearly a hotel management company by name), and their TITLE or LinkedIn headline
  mentions the target hotel or its specific location → KEEP as hotel_specific with high confidence.
- When multiple operators are listed (e.g. "Crescent Hotels & Resorts / Marriott"), the first
  is typically the day-to-day management company and the second is the brand flag. Contacts
  from BOTH are valid — do NOT reject one as "a different company."
- A management company employer is NOT grounds for rejection. Do NOT reject Andrew Carey types
  who say "General Manager - Canopy by Hilton at Deer Valley" employed by "Extell Hospitality
  Services" — that is the textbook case of a real GM at the target hotel.
- Only reject management-company employees if their title is corporate/regional (VP, Regional
  Director, Area Manager, Corporate ___) OR if they work at a clearly different property.

TITLE-CONTAINS-HOTEL RULE (OVERRIDES OTHER CHECKS):
If a contact's job title, LinkedIn headline, or raw_snippet explicitly contains the target hotel
name "{hotel_name}" (or its distinctive location keywords), they are CONFIRMED at the target
hotel — KEEP as hotel_specific with high confidence, regardless of what the organization field
says. Examples that MUST be kept:
- title: "General Manager - Canopy by Hilton at Deer Valley", org: "Extell Hospitality Services"
  → KEEP (title names target hotel)
- title: "Director of Housekeeping", raw_snippet: "...joins The Nora Hotel as Director..."
  → KEEP (snippet names target hotel)

VENUE NAME FALSE POSITIVE WARNING (CRITICAL):
The target hotel name may appear as part of a COMPLETELY DIFFERENT venue name. This is a FALSE POSITIVE.
You MUST reject these — the word overlap does NOT confirm the contact works at the target hotel.
Examples:
- Target hotel: "The Dean" → contact title: "General Manager, Dean's Italian Steakhouse" = REJECT
  ("Dean's" is a restaurant inside a different hotel, not The Dean Hotel)
- Target hotel: "The Bristol" → contact title: "Bristol Bar Manager" = REJECT (different venue)
- Target hotel: "The Henry" → contact org: "Henry's Pub" = REJECT (different business)
RULE: The hotel name must appear as a STANDALONE reference to the target hotel, not embedded
inside a different restaurant/bar/venue name. If the contact's title or org contains the hotel
name word as part of a longer venue name that is clearly a restaurant, bar, steakhouse, pub,
or other F&B outlet at a DIFFERENT property, it is a FALSE POSITIVE — REJECT it.

Check raw_snippet and organization carefully - the target hotel name or its specific city/location
must appear in their actual profile/snippet (not just in the search query that found them).
If you cannot confirm the SPECIFIC hotel, set corrected_scope to "chain_area" not "hotel_specific".

ALWAYS KEEP these:
- Contacts whose ACTUAL ROLE (not a role mentioned in someone else's post) is operational hotel staff
  AND whose snippet/org confirms they work at the TARGET hotel specifically
- Resort Manager, Director of Operations, Director of Rooms, Director of F&B, Executive Housekeeper,
  Purchasing Manager, Housekeeping Manager, Front Office Manager, Hotel Manager, Property Manager

DIFFERENT HOTEL = ALWAYS REJECT:
If the snippet or organization mentions a SPECIFIC hotel name that is NOT the target hotel, REJECT immediately.
Examples:
- Target: "Fairmont New Orleans" → snippet mentions "Archer Hotel" or "The Roosevelt" = REJECT
- Target: "Westin Cocoa Beach" → snippet mentions "Hilton Garden Inn" = REJECT
- This applies even if the contact has an operational title and is in the same city.
A different hotel name is definitive proof they do NOT work at the target.

WHEN IN DOUBT about hotel connection:
- If snippet/org mentions a DIFFERENT specific hotel → REJECT (not chain_area, REJECT)
- If snippet/org mentions only the parent brand → chain_area (NEVER reject these)
- If snippet/org has NO hotel mentioned at all → chain_area (NOT hotel_specific)
- Only assign hotel_specific when the TARGET hotel name or its unique location appears in the snippet
- ALWAYS REJECT pure Sales (Director of Key Accounts, National Sales, etc.)
  EXCEPT Director of Sales & Events / Director of Catering / Banquets
- REJECT chain-parent CEO/COO/CFO (Mark Hoplamazian, Anthony Capuano)
  unless the lead's owner field IS the chain — then they're the owner
- REJECT pure investor / board-member roles
- REJECT construction/architecture firm roles UNLESS the contact is at
  the lead's owner / developer entity (Daniel Zuleta at Cotton Bay)
- KEEP Brand-side VP/SVP/Director of Development at the chain
  (Cristiano Goncalves at Hyatt, Camilo Bolaños at Hyatt, Laurent de
  Kousemaeker at Marriott — they sign management agreements)

PRE-OPENING HOTEL RULE: If the hotel has NOT yet opened (see HOTEL STATUS above), contacts at the
parent brand in the target city are EXPECTED — the property has no LinkedIn presence yet.
- Parent brand + target city + operational title → chain_area (NEVER reject)
- Parent brand + target city + no title but posts about the property → chain_area (NEVER reject)
- A LinkedIn post about hiring/recruiting for a property in the target city is evidence of involvement
- Do NOT reject contacts just because they lack a title — if they're at the parent brand in the
  target city, keep them as chain_area and let the scoring system handle prioritization

REMINDER: For LinkedIn POSTS (source_url contains /posts/), the poster OWN title is NOT in the post text.
The post text describes OTHER people. The poster is typically a corporate executive sharing company news.

EXAMPLE OF A FALSE POSITIVE YOU MUST CATCH:
- name: "Alinio Azevedo"
- source_url: linkedin.com/posts/alinioazevedo_exciting-day-onsite...
- raw_snippet: "Exciting day onsite at our Westin Cocoa Beach Resort. Construction is progressing well and our recently-named General Manager Brett Orlando..."
- extracted_title: "General Manager"
WRONG: Keeping Alinio as General Manager. "General Manager" refers to Brett Orlando, not Alinio.
RIGHT: Alinio is the poster (CEO/Investor). He should be REJECTED as corporate. Brett should be a separate contact.

Apply this same logic to ALL posts: the poster is sharing news about someone else getting a role.
If raw_search_title says "Person Name Post - LinkedIn", that person is the POSTER not the role holder.
Cross-reference the snippet text carefully - who is ACTUALLY being named/appointed/hired?

IMPORTANT: If a contact has NO title but their LinkedIn URL or org explicitly contains the TARGET hotel name,
keep them as hotel_specific. But if their profile shows a DIFFERENT hotel or no hotel at all, REJECT or set chain_area.
Do NOT default to keeping contacts just because you are uncertain — uncertainty without evidence means chain_area at best.

LOW-SCORE CONTACTS: If a contact has no title AND no organization matching the target hotel, they should be REJECTED.
A contact with zero evidence of working at the target hotel should never be classified as hotel_specific.

OWNER / PRINCIPAL RULE (CRITICAL — do not reject as "Investor"):
If a contact arrives with scope="owner" already set, OR if their title is
Chairman, Founder, Principal, Managing Partner, Owner, Managing Member and
their organization is a development/holdings/capital/REIT/family-office entity,
they are the property's CHECK-WRITER. Keep them and return corrected_scope="owner".
- They will NOT have hotel-operational titles (that's the whole point).
- They WILL have non-hotel organizations (development companies, REITs, family
  offices, LLCs). That's also the whole point.
- Owner principals are legitimate P2 sales contacts (budget authority) — do
  NOT reject them as "Investor" or "non-hotel-ops" just because they don't fit
  the operational-staff mold.
- Example: "Dr. Kali P. Chaudhuri — Founder & Chairman, KPC Development Company"
  → is_hotel_ops=false, is_at_target_hotel=false, corrected_scope="owner",
  rejection_reason=null.

CRITICAL DISTINCTION — FAMILY-OWNED CHAINS vs FUNCTIONAL CORPORATE EXECS:

⚠️ DO NOT do this:
   "This contact's organization is Grupo Posadas, and Posadas owns this
   property, so this contact must be scope=owner."
   → WRONG. Working at a chain that owns hotels does NOT make someone
   an owner. Family-owned chains have BOTH owner-principals (family) AND
   professional functional execs. They get DIFFERENT scopes.

⚠️ DO NOT change scope from management_corporate → owner just because the
contact's organization happens to be the family-owned chain operator.
That's the WHOLE POINT — most chain executives are NOT family. Only flip
to owner if the contact has POSITIVE evidence of family-membership.

Some hotel chains are family-owned (Posadas — Azcárraga family; Mardin —
Hashimoto family; Riu — Riu family; Iberostar — Fluxà family; etc.). At
these chains, the C-suite includes BOTH family-owner principals AND
professional executives running operations. They get DIFFERENT scopes:

- Family-owner C-suite at the holdco / chain parent → corrected_scope="owner"
  (Chairman, Vice-Chairman, Founder, Family Member-CEO of holdco,
  family-named Board members)
  • Pablo Azcárraga Andrade (Chairman of Grupo Posadas, family member) → owner
  • José Carlos Azcárraga Andrade (CEO Grupo Posadas, family member) → owner

- Professional / functional execs at the SAME family-owned chain →
  corrected_scope="management_corporate" (NOT owner)
  (VP Development, COO, Chief Financial Officer, Director General who is
  not a family member, Director of Procurement, Director of Operations,
  Director of Investor Relations, VP HR, Director of IT, etc.)
  • Mauricio Elizondo (VP Development at Posadas) → management_corporate,
    NOT owner — Elizondo is NOT an Azcárraga, his title is functional.
  • Gerardo de Prevoisin (Director Investor Relations at Posadas) →
    management_corporate, NOT owner — Prevoisin is NOT an Azcárraga.
  • Enrique Calderón (Director General/COO Posadas) → management_corporate,
    NOT owner — Calderón is NOT an Azcárraga.
  • Antonio García at Posadas → management_corporate UNLESS title is
    Chairman/Founder/family-Owner → García is not the founder family.

DECISION RULE — apply STRICTLY when scope arrives as management_corporate:
1. Does the contact's SURNAME match the founder family of the chain?
   (Azcárraga at Posadas, Riu at RIU, Fluxà at Iberostar, Sarmiento at
   Cotton Bay, Stewart at Sandals, etc.)
2. AND does their title indicate ownership (Chairman, Vice-Chairman,
   Founder, Owner, family-named C-suite role)?

If BOTH yes → flip to corrected_scope="owner".
If EITHER no → KEEP corrected_scope="management_corporate".

Owner scope is reserved for the actual owners. A functional title at a
family-owned chain ≠ owner.

MANAGEMENT COMPANY vs BRAND PARENT SCOPE DISTINCTION (CRITICAL):
The same parent company (Marriott / Hilton / Hyatt) plays TWO different
roles depending on the brand. Read carefully — this determines whether
their execs are P1 or P4 for this lead.

ROLE 1 — BRAND PARENT for SOFT-BRAND PROPERTIES (P3/P4):
Soft-brand properties (Autograph Collection, Curio Collection, Tribute
Portfolio, MGallery, Tapestry Collection, JdV) are BRANDED by Marriott/
Hilton/Hyatt but OPERATED by an independent management company (Crescent
Hotels, Aimbridge, Highgate, Pyramid, etc.).
- Operator's corporate execs → corrected_scope="management_corporate" (P1/P2)
- Brand parent execs (Marriott VP of Autograph Brand, Hilton Curio team)
  → corrected_scope="chain_corporate" (P3/P4 — they don't control
  procurement for soft-brand properties).
Detect: management_company is something LIKE Crescent / Aimbridge /
Highgate, NOT Marriott/Hilton/Hyatt itself.

ROLE 2 — BRAND PARENT IS THE OPERATOR for BRAND-MANAGED PROPERTIES (P1/P2):
Direct flags Ritz-Carlton, St. Regis, EDITION, W Hotels (Marriott direct);
Conrad, Waldorf Astoria, LXR (Hilton direct); Park Hyatt, Andaz, Grand
Hyatt (Hyatt direct); Four Seasons, Aman, Mandarin Oriental, Rosewood,
Bulgari, Peninsula, Auberge are operated by the brand itself or its own
hospitality company. There is NO separate independent management company.
- Brand parent's regional execs → corrected_scope="management_corporate" (P1/P2)
  because the brand parent IS the operator. Examples that should be
  management_corporate, NOT chain_corporate:
  • "President, Caribbean & Latin America" at Marriott (for a Ritz-Carlton)
  • "VP Operations, Ritz-Carlton Americas" / "SVP, The Ritz-Carlton & EDITION"
  • "VP Hotel Operations, Four Seasons North America"
  • "Regional VP, Hyatt Luxury" (for a Park Hyatt)
Detect: management_company equals or contains the brand parent (e.g.
management_company = "Marriott International" AND brand = "Ritz-Carlton"
or "Ritz-Carlton Reserve" or "St. Regis" or "EDITION" — direct Marriott
luxury). For these, Marriott execs covering this property's region are
the actual operator's ops team and should be management_corporate.

DECISION CHECKLIST — apply in this order:
1. Look at the management_company string for this lead: {management_company}
2. Look at the contact's organization (Marriott / Hilton / etc.).
3. If management_company contains the same parent name as the contact's
   organization → this is ROLE 2 (brand-managed). Mark management_corporate.
4. If management_company is a separate independent operator (Crescent,
   Aimbridge, Highgate, Pyramid, etc.) → this is ROLE 1 (soft brand).
   Mark Marriott/Hilton/Hyatt execs as chain_corporate.
5. When unsure, look at the brand:
   - Direct luxury brands (Ritz-Carlton, St. Regis, EDITION, Four Seasons,
     Aman, Conrad, Waldorf, Park Hyatt, Mandarin Oriental, Rosewood,
     Auberge, Bulgari, Peninsula) → ROLE 2 → management_corporate
   - Soft-brand collections (Autograph, Curio, Tribute, MGallery,
     Tapestry, JdV, Unbound, Hilton Curio etc.) → ROLE 1 → chain_corporate

CONTACTS TO VERIFY:
{contacts_json}

Respond with ONLY a JSON array. For each contact:
{{"name": "original name", "verified_title": "actual title or empty", "verified_org": "actual employer", "is_hotel_ops": true/false, "is_at_target_hotel": true/false, "rejection_reason": "why rejected or null", "corrected_scope": "hotel_specific|chain_area|chain_corporate|management_corporate|owner|rejected"}}
"""


async def _verify_contacts_with_gemini(
    contacts: list[dict],
    hotel_name: str,
    brand: Optional[str] = None,
    management_company: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    opening_date: Optional[str] = None,
    project_type: Optional[str] = None,
    developer: Optional[str] = None,
    owner: Optional[str] = None,
) -> list[dict]:
    """
    AI verification layer: Gemini reads raw snippets to determine each contact
    real title, org, and relevance. Fixes false positives from regex parsing.

    `developer` and `owner` are the entities for THIS lead (e.g. "Cotton Bay
    Holdings Limited"). When supplied, they anchor an owner-entity rule that
    prevents Gemini from rejecting owner-side execs (Construction MD, General
    Counsel, etc.) as generic contractors or unrelated C-suite.
    """
    # Build verification payload with raw snippet context
    contacts_for_verification = []
    for i, c in enumerate(contacts):
        entry = {
            "index": i,
            "name": c.get("name", ""),
            "extracted_title": c.get("title", ""),
            "organization": c.get("organization", ""),
            "source_url": c.get("source", ""),
            "source_type": c.get("source_type", ""),
            "current_scope": c.get("scope", ""),
            "raw_snippet": c.get("_raw_snippet", ""),
            "raw_search_title": c.get("_raw_title", ""),
        }
        contacts_for_verification.append(entry)

    if not contacts_for_verification:
        return contacts

    location = _build_location_string(city, state, country)
    mode = _get_search_mode(opening_date)
    if mode == "pre_opening":
        hotel_status = (
            "HOTEL STATUS: PRE-OPENING — This hotel has NOT yet opened. "
            "Staff are being hired under the parent brand. "
            "Parent brand + target city contacts should be chain_area, NOT rejected."
        )
    elif mode == "opening_soon":
        hotel_status = (
            "HOTEL STATUS: OPENING SOON — This hotel is opening within 6 months. "
            "Some staff may still be listed under the parent brand."
        )
    else:
        hotel_status = "HOTEL STATUS: OPEN — Standard verification rules apply."

    # ── BRAND-AWARE PROCUREMENT GUIDANCE ──
    # Independent / boutique / founder-led brands have a fundamentally
    # different procurement structure than chain-managed brands. For a
    # brand like Appellation (independent, fully open procurement), the
    # FOUNDER and CORPORATE EXECUTIVES are the actual uniform buyers —
    # rejecting them is rejecting our customer.
    #
    # For chain-managed brands (Marriott, Hilton, IHG with Avendra GPO),
    # corporate is locked in to mandated suppliers and unreachable. For
    # those, keep rejecting C-suite and target only property-level staff.
    procurement_guidance = ""
    try:
        brand_info = BrandRegistry.lookup(brand) if brand else None
        if brand_info:
            uniform_freedom = (brand_info.uniform_freedom or "").lower()
            procurement_model = (brand_info.procurement_model or "").lower()
            opportunity = (brand_info.opportunity_level or "").lower()

            # Independent / founder-led / boutique signals
            is_independent = (
                uniform_freedom in ("high", "full")
                or procurement_model
                in ("fully_open", "independent", "owner_decides", "open")
                or opportunity == "high"
            )

            # NEW: Regional cluster brands (HIC, certain Hyatt clusters, Auberge,
            # Belmond etc) have brand-managed procurement BUT through regional
            # corporate execs who ARE reachable. Detect this by looking at the
            # brand registry's own pre_opening_contact_titles list — if it
            # explicitly names corporate roles (VP, SVP, Cluster, Above
            # Property, Corporate, Regional), then the brand registry is
            # telling us "for this brand, corporate IS the buyer."
            _CLUSTER_TITLE_KEYWORDS = (
                "vp ",
                "vice president",
                "svp",
                "senior vice president",
                "cluster",
                "above property",
                "corporate",
                "regional",
                "head of",
                "chief commercial",
                "chief operating",
            )
            is_cluster_brand = False
            if brand_info.pre_opening_contact_titles:
                for tt in brand_info.pre_opening_contact_titles:
                    tt_lower = (tt or "").lower()
                    if any(kw in tt_lower for kw in _CLUSTER_TITLE_KEYWORDS):
                        is_cluster_brand = True
                        break

            if is_independent:
                procurement_guidance = (
                    "═══════════════════════════════════════════════════════════════\n"
                    "PROCUREMENT MODEL: INDEPENDENT / BOUTIQUE / FOUNDER-LED\n"
                    "═══════════════════════════════════════════════════════════════\n"
                    f"This brand has procurement model: {brand_info.procurement_model!r}, "
                    f"uniform freedom: {brand_info.uniform_freedom!r}, "
                    f"opportunity level: {brand_info.opportunity_level!r}.\n"
                    "\n"
                    "CRITICAL OVERRIDE: For THIS brand, the buyers we want ARE corporate.\n"
                    "Founders, co-founders, presidents, COOs, VPs of Operations, and\n"
                    "regional VPs at independent / boutique brands are the ACTUAL\n"
                    "decision-makers for uniform contracts. They are NOT to be rejected\n"
                    "as 'C-suite' or 'corporate roles' for this brand.\n"
                    "\n"
                    "KEEP these for this brand (mark scope as chain_corporate or chain_area):\n"
                    "- Founder, Co-Founder, Owner, Principal\n"
                    "- President, COO, CEO (if directly involved in operations)\n"
                    "- VP Operations, VP Hotel Operations, SVP Operations\n"
                    "- Regional VP, Regional Director of Operations\n"
                    "- Head of Procurement, Director of Procurement, VP Procurement\n"
                    "- Chief Operating Officer, Chief Brand Officer\n"
                    "\n"
                    "STILL REJECT for this brand:\n"
                    "- Pure investors / board members with no operational role\n"
                    "- Pure marketing/revenue roles (Revenue Manager, National Accounts, PR)\n"
                    "  BUT KEEP: Director of Sales & Events, Director of Catering,\n"
                    "  Director of Banquets — they manage staff who NEED uniforms\n"
                    "- Construction / development contractors\n"
                    "- People at clearly different hotels/brands\n"
                    "═══════════════════════════════════════════════════════════════\n"
                )
            elif is_cluster_brand:
                # Build the "preferred titles" list directly from the brand
                # registry so Gemini sees exactly what we want for THIS brand
                titles_str = "\n".join(
                    f"- {t}" for t in (brand_info.pre_opening_contact_titles or [])
                )
                procurement_guidance = (
                    "═══════════════════════════════════════════════════════════════\n"
                    "PROCUREMENT MODEL: REGIONAL CLUSTER BRAND (corporate IS reachable)\n"
                    "═══════════════════════════════════════════════════════════════\n"
                    f"This brand ({brand!r}) has procurement model: {brand_info.procurement_model!r}, "
                    f"but is structured around REGIONAL corporate teams.\n"
                    "\n"
                    "CRITICAL OVERRIDE: For THIS brand, the actual uniform buyers are\n"
                    "REGIONAL CORPORATE executives — VPs of Commercial Services, Cluster\n"
                    "GMs, Above-Property Procurement Directors, Senior Corporate F&B\n"
                    "Directors, etc. These people are NOT 'corporate noise to filter out';\n"
                    "they ARE the decision-makers JA Uniforms needs to reach.\n"
                    "\n"
                    f"PREFERRED TITLES per the brand registry for {brand!r}:\n"
                    f"{titles_str}\n"
                    "\n"
                    "KEEP for this brand (mark scope as chain_corporate or chain_area):\n"
                    "- Anyone with the preferred titles above\n"
                    "- Regional VPs / SVPs covering the property's region\n"
                    "- Cluster General Managers (one GM covers multiple properties)\n"
                    "- Above-Property / Corporate Procurement Directors\n"
                    "- Senior Corporate Directors of F&B, Operations, Housekeeping\n"
                    "- President/SVP of the brand's regional sub-organization\n"
                    "  (e.g. 'President, Latin America & Caribbean')\n"
                    "- VP/SVP Commercial Services for the property's region\n"
                    "\n"
                    "REGIONAL FIT MATTERS: A 'VP Operations EMEA' is NOT a fit for a\n"
                    "Caribbean property. Match region to property location. If a contact\n"
                    "covers a clearly different region, mark scope as chain_area instead\n"
                    "of rejecting outright.\n"
                    "\n"
                    "STILL REJECT for this brand:\n"
                    "- Pure investors / board members with no operational role\n"
                    "- Pure marketing/revenue roles (Revenue Manager, National Accounts, PR)\n"
                    "  BUT KEEP: Director of Sales & Events, Director of Catering,\n"
                    "  Director of Banquets — they manage staff who NEED uniforms\n"
                    "- Construction / development contractors\n"
                    "- People at clearly different brands\n"
                    "- People whose region is clearly mismatched (e.g. EMEA contact for Caribbean property)\n"
                    "═══════════════════════════════════════════════════════════════\n"
                )
            else:
                procurement_guidance = (
                    "═══════════════════════════════════════════════════════════════\n"
                    "PROCUREMENT MODEL: CHAIN-MANAGED (corporate locked / GPO-controlled)\n"
                    "═══════════════════════════════════════════════════════════════\n"
                    f"This brand has procurement model: {brand_info.procurement_model!r}, "
                    f"uniform freedom: {brand_info.uniform_freedom!r}.\n"
                    "Apply standard rules — reject C-suite/corporate, target property-\n"
                    "level operational staff only. Corporate procurement at this brand\n"
                    "is locked in to mandated suppliers / GPOs and not reachable.\n"
                    "═══════════════════════════════════════════════════════════════\n"
                )
    except Exception as ex:
        logger.debug(f"Failed to build procurement guidance: {ex}")

    # ── CONVERSION/REBRAND OVERRIDE ──
    # For conversion leads (Montage→St. Regis, Hilton→Dreams), the development
    # executive who signed the deal IS relevant — they're managing the transition
    # and making vendor decisions during the conversion period.
    conversion_override = ""
    if project_type in ("conversion", "rebrand", "ownership_change"):
        conversion_override = (
            "\n═══════════════════════════════════════════════════════════════\n"
            "CONVERSION/REBRAND OVERRIDE\n"
            "═══════════════════════════════════════════════════════════════\n"
            "This property is undergoing a CONVERSION or REBRAND. During this\n"
            "transition period, development executives (Chief Development Officer,\n"
            "VP Development, SVP Development) who signed the management agreement\n"
            "ARE relevant — they are managing the transition and making vendor\n"
            "decisions. Do NOT reject them as 'C-suite/development'.\n"
            "KEEP: CDO, VP Development, SVP Development for conversion leads.\n"
            "═══════════════════════════════════════════════════════════════\n"
        )

    # ── OWNER ENTITY ANCHOR ──
    # Lead-specific override: tell verification WHO the owner/developer is
    # for THIS hotel. Without this, Gemini sees "Daniel Zuleta - Managing
    # Director of Construction at Cotton Bay Holdings" and rejects him as a
    # construction contractor — even though Cotton Bay IS the owner/developer
    # for this lead and Daniel is its top construction-side exec, who literally
    # picks vendors. Same failure mode for Jonnell Rigby-Knowles (General
    # Counsel at Cotton Bay) — owner-side legal handles vendor agreements.
    owner_entity_anchor = ""
    owner_entities: list[str] = []
    if developer:
        owner_entities.append(developer.strip())
    if owner and (not developer or owner.strip().lower() != developer.strip().lower()):
        owner_entities.append(owner.strip())
    if owner_entities:
        entities_str = " / ".join(f'"{e}"' for e in owner_entities)
        owner_entity_anchor = (
            "\n═══════════════════════════════════════════════════════════════\n"
            "OWNER ENTITY ANCHOR — lead-specific (READ THIS FIRST)\n"
            "═══════════════════════════════════════════════════════════════\n"
            f"For THIS lead, the owner / developer entity is: {entities_str}\n\n"
            "ABSOLUTE RULE: If a contact's organization matches this entity\n"
            "(or contains it as a substring), they are OWNER-SIDE for this\n"
            "property. KEEP them and assign corrected_scope='owner', regardless\n"
            "of what their title says. Specifically:\n"
            "- 'Managing Director of Construction' AT the owner entity is the\n"
            "  owner's top construction exec who personally picks vendors.\n"
            "  KEEP — this is NOT a third-party contractor.\n"
            "- 'General Counsel' / 'Chief Legal Officer' / 'VP Legal' AT the\n"
            "  owner entity drafts and signs vendor agreements (incl. uniforms).\n"
            "  KEEP — this is NOT generic C-suite.\n"
            "- 'CEO' / 'President' / 'Managing Partner' / 'Principal' /\n"
            "  'Chairman' / 'Founder' AT the owner entity is the check-writer.\n"
            "  KEEP — this is the textbook pre-opening procurement decider.\n"
            "- 'CFO' / 'CIO' AT the owner entity controls budgets. KEEP.\n"
            "- 'Development Director' / 'VP Development' / 'Project Director'\n"
            "  AT the owner entity manages the build-out. KEEP.\n"
            "- 'Director of Procurement' / 'VP Procurement' AT the owner\n"
            "  entity does literal vendor selection. KEEP — assign scope='owner'\n"
            "  and priority=P1.\n\n"
            "The 'reject construction contractors' and 'reject C-suite' rules\n"
            "below DO NOT apply to anyone at the owner entity. Those rules are\n"
            "for THIRD-PARTY general contractors, sub-contractors, architects,\n"
            "and unrelated C-suite at OTHER companies.\n"
            "═══════════════════════════════════════════════════════════════\n"
        )

    # ── PRE-OPENING OWNER / DEVELOPER OVERRIDE ──
    # CRITICAL for new-build leads before the operator has taken over.
    # The property owner / developer SIGNS the management agreement and FUNDS
    # construction, FF&E (furniture/fixtures/equipment) AND uniforms. They
    # pre-commit to vendors BEFORE the hotel operator arrives. Gemini's
    # default rejection logic treats "development company" as irrelevant,
    # but for pre-opening leads these are P1 contacts.
    #
    # Real examples where this override matters:
    #   Tony Birkla (Birkla Investment Group) → owner of Hyatt Centric Cincinnati
    #   Zafir Rashid (Teramir Group / Everest Place) → owner of Nickelodeon Orlando
    #   Khalid Muneer (Everest Place) → Managing Director, same project
    #   Daniel Zuleta (Cotton Bay Holdings) → owner-side Managing Director of
    #     Construction for Ritz-Carlton Reserve Eleuthera (committed to 60%
    #     Bahamian vendors — actively choosing suppliers)
    #   Jonnell Rigby-Knowles (Cotton Bay Holdings) → General Counsel,
    #     drafts vendor agreements for the same project
    #
    # These are NOT construction contractors. They're the check-writers and
    # signers.
    pre_opening_override = ""
    # Trigger on new_opening project types. Also trigger when project_type is
    # None/unknown (conservative default — better to include owner contacts
    # for ambiguous leads than to wrongly reject them).
    _PRE_OPENING_TYPES = {"new_opening", "greenfield", "", None}
    if (
        project_type or ""
    ).strip().lower() in _PRE_OPENING_TYPES or project_type is None:
        pre_opening_override = (
            "\n═══════════════════════════════════════════════════════════════\n"
            "PRE-OPENING OWNER / DEVELOPER OVERRIDE (new-build property)\n"
            "═══════════════════════════════════════════════════════════════\n"
            "This is a PRE-OPENING / new-build property. The owner/developer\n"
            "entity is the PRIMARY uniform buyer during pre-opening phase\n"
            "(signs management agreement, funds construction + FF&E + uniforms).\n\n"
            "KEEP — do NOT reject these roles for pre-opening properties:\n"
            "- Owner, Principal, Managing Partner at the ownership entity\n"
            "- CEO, Managing Director, President at the OWNING / DEVELOPING company\n"
            "  (Birkla Investment Group, Teramir Group, Everest Place,\n"
            "  Cotton Bay Holdings, TMGOC Ventures, etc.)\n"
            "- CFO / CIO at the ownership entity (controls budgets + FF&E spend)\n"
            "- Development Director, VP Development AT THE OWNERSHIP ENTITY\n"
            "- Managing Director of Construction / Director of Construction\n"
            "  Management AT THE OWNERSHIP ENTITY (owner-side construction\n"
            "  lead — picks vendors; NOT a third-party contractor)\n"
            "- General Counsel / Chief Legal Officer / VP Legal AT THE\n"
            "  OWNERSHIP ENTITY (signs vendor agreements)\n"
            "- Project Director / VP Project Management AT THE OWNERSHIP\n"
            "  ENTITY (manages the build-out, NOT a GC firm)\n\n"
            "Key distinction: these roles at a DEVELOPMENT / INVESTMENT / OWNERSHIP\n"
            "company are the CHECK-WRITERS and CONTRACT-SIGNERS for pre-opening\n"
            "procurement. Do NOT confuse them with construction contractors\n"
            "(GC, subcontractors) at SEPARATE general-contracting firms, or\n"
            "unrelated real-estate professionals (realtors, brokers).\n\n"
            "TEST: is the contact's organization the SAME as the owner/developer\n"
            "for this lead? Yes → KEEP, scope='owner'. No, they're at a\n"
            "general-contracting / architecture / engineering firm → reject.\n\n"
            "For these contacts: assign scope='owner' and priority=P1 (or P2 if\n"
            "the role is finance/CFO rather than CEO/principal).\n"
            "═══════════════════════════════════════════════════════════════\n"
        )

    # ── INDEPENDENT / BOUTIQUE OVERRIDE ──
    # For independent hotels (no chain brand), the CEO/founder/principal
    # IS the uniform buyer. There's no corporate procurement layer.
    # A 29-room boutique hotel's CEO picks every vendor personally.
    independent_override = ""
    brand_lower = (brand or "").lower().strip()
    if (
        not brand_lower
        or brand_lower in ("independent", "boutique", "lifestyle")
        or (brand_lower and not BrandRegistry.lookup(brand))
    ):
        independent_override = (
            "\n═══════════════════════════════════════════════════════════════\n"
            "INDEPENDENT / BOUTIQUE HOTEL OVERRIDE\n"
            "═══════════════════════════════════════════════════════════════\n"
            "This is an INDEPENDENT or boutique hotel — NOT part of a major chain.\n"
            "For independent hotels, the normal C-suite rejection rules DO NOT APPLY.\n"
            "Founders, CEOs, Managing Directors, Principals, and Co-Founders ARE\n"
            "the uniform buyers at these properties. There is no corporate procurement\n"
            "layer or GPO. The owner/operator makes every vendor decision directly.\n\n"
            "KEEP (do NOT reject) these roles for independent hotels:\n"
            "- CEO, Co-Founder, Founder, Principal, Managing Partner\n"
            "- Managing Director, President, COO\n"
            "- CDO (Chief Development Officer) — handles procurement for new openings\n"
            "- VP Operations, Director of Operations\n"
            "- General Manager, Hotel Manager\n"
            "═══════════════════════════════════════════════════════════════\n"
        )

    prompt = CONTACT_VERIFICATION_PROMPT.format(
        hotel_name=hotel_name,
        location=location,
        brand=brand or "Independent",
        management_company=management_company or "Unknown",
        hotel_status=hotel_status,
        procurement_guidance=owner_entity_anchor
        + procurement_guidance
        + conversion_override
        + pre_opening_override
        + independent_override,
        contacts_json=json.dumps(contacts_for_verification, indent=2),
    )

    model = get_enrichment_gemini_model()

    try:
        verifications = await _call_gemini(prompt, model=model, timeout=120)
        if verifications is None:
            logger.warning(
                "Gemini verification returned no data, keeping contacts as-is"
            )
            return contacts
        # _call_gemini returns parsed JSON — for verification, it's a list
        if not isinstance(verifications, list):
            logger.error(
                f"Gemini verification returned non-list: {type(verifications)}"
            )
            return contacts

    except Exception as e:
        import traceback

        logger.error(f"Gemini contact verification failed: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        return contacts

    # Apply verification results
    verified_contacts = []
    for v in verifications:
        vname = v.get("name", "").lower().strip()
        # Find matching original contact
        match = None
        for c in contacts:
            if c.get("name", "").lower().strip() == vname:
                match = c
                break
        if not match:
            continue

        corrected_scope = v.get("corrected_scope", "")
        rejection_reason = v.get("rejection_reason")

        if corrected_scope == "rejected":
            logger.info(f"Gemini REJECTED: {v.get('name')} -- {rejection_reason}")
            match["_gemini_rejected"] = True
            match["_gemini_rejection_reason"] = rejection_reason
            continue

        # Update with verified info
        verified_title = v.get("verified_title", "")
        verified_org = v.get("verified_org", "")

        if verified_title:
            old_title = match.get("title", "")
            old_lower = old_title.lower().strip()

            # Only accept Gemini's title if original was missing or incomplete
            # e.g. "Director Of" → "Director of Food & Beverage" (good)
            # but NOT "Director of Operations" → "Managing Director" (bad)
            title_is_missing = not old_lower
            title_is_incomplete = (
                old_lower.endswith(" of")
                or old_lower.endswith(" for")
                or old_lower.endswith(" and")
                or old_lower.endswith(" -")
                or len(old_lower.split()) <= 1
            )
            # Gemini expands the original (contains old words)
            old_words = set(old_lower.split()) - {
                "of",
                "the",
                "and",
                "for",
                "at",
                "in",
                "&",
            }
            new_words = set(verified_title.lower().split()) - {
                "of",
                "the",
                "and",
                "for",
                "at",
                "in",
                "&",
            }
            title_is_expansion = old_words and old_words.issubset(new_words)

            # Allow Gemini correction if original came from non-LinkedIn sources
            # (Facebook, Instagram, press releases often use different titles than LinkedIn)
            source_type = match.get("source_type", "")
            source_url = match.get("source", "")
            is_linkedin_profile = (
                source_type == "linkedin_snippet"
                and "linkedin.com/in/" in (source_url or "")
            )

            if title_is_missing or title_is_incomplete or title_is_expansion:
                if old_lower != verified_title.lower().strip():
                    logger.info(
                        f"Gemini title fix: {v.get('name')}: "
                        f"'{old_title}' -> '{verified_title}'"
                    )
                match["title"] = verified_title
            elif (
                not is_linkedin_profile
                and not match.get("_title_source") == "web_resolution"
            ):
                # Non-LinkedIn source — trust Gemini's correction (likely from LinkedIn data)
                # BUT never override titles we resolved from press releases / official sources
                if old_lower != verified_title.lower().strip():
                    logger.info(
                        f"Gemini title correction (non-LinkedIn source): {v.get('name')}: "
                        f"'{old_title}' -> '{verified_title}'"
                    )
                match["title"] = verified_title
            elif match.get("_title_source") == "web_resolution":
                if old_lower != verified_title.lower().strip():
                    logger.info(
                        f"Gemini title change BLOCKED (web-resolved): {v.get('name')}: "
                        f"kept '{old_title}' (Gemini wanted '{verified_title}')"
                    )
            elif old_lower != verified_title.lower().strip():
                logger.info(
                    f"Gemini title change BLOCKED: {v.get('name')}: "
                    f"kept '{old_title}' (Gemini wanted '{verified_title}')"
                )

        if verified_org:
            match["organization"] = verified_org

        if corrected_scope in (
            "hotel_specific",
            "chain_area",
            "chain_corporate",
            "management_corporate",
            "owner",
        ):
            old_scope = match.get("scope", "")
            if old_scope != corrected_scope:
                logger.info(
                    f"Gemini scope fix: {v.get('name')}: "
                    f"'{old_scope}' -> '{corrected_scope}'"
                )
            match["scope"] = corrected_scope

        match["_gemini_verified"] = True
        match["_gemini_is_hotel_ops"] = v.get("is_hotel_ops", False)
        match["_gemini_is_at_target"] = v.get("is_at_target_hotel", False)
        verified_contacts.append(match)

    # Keep contacts Gemini did not mention (do not drop silently)
    verified_names = {v.get("name", "").lower().strip() for v in verifications}
    for c in contacts:
        if c.get("name", "").lower().strip() not in verified_names:
            logger.warning(f"Gemini skipped contact: {c.get('name')} -- keeping as-is")
            verified_contacts.append(c)

    # Deterministic backstop: reject contacts from non-hotel orgs with no operational title
    NON_HOTEL_ORG_KEYWORDS = [
        "construction",
        "capital",
        "development",
        "holdings",
        "investment",
        "architecture",
        "contracting",
        "consulting",
        "engineering",
        "ventures",
        "equity",
        "realty",
        "real estate",
        "contractors",
        "builders",
    ]
    OPERATIONAL_TITLES = [
        "general manager",
        "director of",
        "executive housekeeper",
        "purchasing manager",
        "housekeeping manager",
        "front office manager",
        "hotel manager",
        "resort manager",
        "property manager",
        "operations manager",
        "assistant general manager",
        "rooms division",
        "uniform manager",
        "wardrobe manager",
        "laundry manager",
        # ── Owner-side hotel-operations patterns ──
        # Owner / developer entities (REITs, real-estate firms, family
        # offices) often have their own internal hotel-operations team.
        # These titles indicate the contact runs hotel ops for the
        # owner — same buying authority as a property GM but on the
        # capital-side. Examples: Pam Ryan "VP Hotel Operations" at
        # Ensemble Real Estate Investments; Kristi Allen "EVP Hotels".
        "hotel operations",
        "hotels operations",
        "hotel finance",  # Samuel Grant pattern
        "vp hotel",
        "svp hotel",
        "evp hotel",
        "vice president, hotel",
        "vice president hotel",
        "vice president of hotel",
        "vice president, hotels",
        "vice president hotels",
        "vp, hotel",
        "vp, hotels",
        "head of hotel",
        "head of hotels",
        "managing director, hotel",
        "managing director hotel",
        # Owner-side construction/development titles (Daniel Zuleta
        # pattern at Cotton Bay Holdings)
        "managing director of construction",
        "director of construction",
    ]

    # ── Owner-entity tokens (build once per call) ──
    # If the contact's org matches the lead's owner or developer entity,
    # they are owner-side staff and should be KEPT regardless of whether
    # their title matches the operational keyword list. This catches
    # patterns like "VP, Finance" at the owner REIT — they're not
    # generic finance VPs, they're the owner's hotel-portfolio finance
    # lead with FF&E/OS&E budget signoff.
    def _owner_entity_tokens(*entities):
        toks = set()
        STOPWORDS = {
            "the",
            "and",
            "of",
            "for",
            "at",
            "a",
            "an",
            "in",
            "on",
            "to",
            "by",
            "or",
            "&",
            "llc",
            "ltd",
            "inc",
            "corp",
            "corporation",
            "company",
            "companies",
            "group",
            "holdings",
            "limited",
            "international",
            "global",
            "the",
            "investments",
            "investment",
            "partners",
            "ventures",
            "capital",
            "real",
            "estate",
            "realty",
            "development",
            "developments",
        }
        for e in entities:
            for w in re.split(r"[^a-z0-9]+", (e or "").lower()):
                if len(w) >= 4 and w not in STOPWORDS:
                    toks.add(w)
        return toks

    owner_dev_tokens = _owner_entity_tokens(owner, developer)

    final_contacts = []
    for c in verified_contacts:
        org = (c.get("organization") or "").lower()
        title = (c.get("title") or "").lower()
        scope = (c.get("scope") or "").lower()
        has_non_hotel_org = any(kw in org for kw in NON_HOTEL_ORG_KEYWORDS)
        has_operational_title = any(kw in title for kw in OPERATIONAL_TITLES)

        # ── Owner/principal whitelist (Bug #7 fix — 2026-04-22) ──
        # Contacts flagged scope=="owner" are the property's check-writers
        # (e.g. Dr. Kali P. Chaudhuri / KPC Development Company). By
        # definition they sit at an investment/development entity, NOT at
        # a hotel, and their title is ownership-class (Chairman, Founder,
        # Principal, Managing Partner) — NOT operational. Without this
        # whitelist the NON_HOTEL_ORG_KEYWORDS filter deletes every owner
        # principal we surface via Iter 1's explicit owner extraction,
        # defeating Bug #4's entire purpose. Owners are P2 per the Iter 6
        # strategist; they belong in the final contact list.
        is_owner_principal = scope == "owner"

        # ── Owner-entity match (2026-05-07 — The Waylen / Ensemble fix) ──
        # If the contact's org tokens overlap with the lead's owner /
        # developer entity, they are owner-side staff — KEEP them
        # regardless of operational title pattern. This catches the
        # owner's internal hotel-operations team (VP Hotel Ops, EVP
        # Hotels, Hotel Finance) at REITs / investment firms.
        org_tokens = set(re.split(r"[^a-z0-9]+", org)) - {"", "the"}
        is_at_owner_entity = bool(owner_dev_tokens and (org_tokens & owner_dev_tokens))

        if (
            has_non_hotel_org
            and not has_operational_title
            and not is_owner_principal
            and not is_at_owner_entity
        ):
            logger.info(
                f"Org-filter REJECTED: {c.get('name')} -- org='{c.get('organization')}' "
                f"title='{c.get('title', '')}' (non-hotel org, no operational title)"
            )
            continue
        if is_owner_principal and has_non_hotel_org:
            logger.info(
                f"Org-filter KEPT owner: {c.get('name')} -- "
                f"org='{c.get('organization')}' title='{c.get('title', '')}' "
                f"(scope=owner exempt from non-hotel-org reject)"
            )
        elif is_at_owner_entity and has_non_hotel_org:
            logger.info(
                f"Org-filter KEPT owner-side staff: {c.get('name')} -- "
                f"org='{c.get('organization')}' title='{c.get('title', '')}' "
                f"(org matches lead's owner/developer entity)"
            )
            # Also auto-correct scope to 'owner' if it isn't set — these
            # are owner-side staff with FF&E budget authority.
            if not scope or scope in ("unknown", "chain_area"):
                c["scope"] = "owner"
        final_contacts.append(c)

    rejected_count = len(contacts) - len(final_contacts)
    logger.info(
        f"Gemini verification: {len(contacts)} in -> "
        f"{len(final_contacts)} out ({rejected_count} rejected)"
    )
    return final_contacts


# ═══════════════════════════════════════════════════════════════
# MAIN ENRICHMENT ORCHESTRATOR — v4 with validation + auto-retry
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════
# LINKEDIN SERP VALIDATION — shared by grounding + iterative pipeline
# ═══════════════════════════════════════════════════════════════════════
#
# Five failure scenarios this covers (2026-05-06 audit):
#
# 1. ROLE TITLE MISMATCH — searched "Resort Manager", bio says "VP Operations"
#    Fix: role families group equivalent titles together
#
# 2. ORG NAME ALIAS — searched "Marriott Vacations Worldwide", bio says "MVW"
#    Fix: org alias table maps abbreviations to canonical names
#
# 3. NAME VARIATION — "Lindsay Meadows" vs "Lindsay A. Meadows-Johnson"
#    Fix: relaxed name check allows middle initials and hyphenated surnames
#
# 4. NON-ENGLISH TITLES — Caribbean GMs list titles in Spanish/French
#    Fix: role families include Spanish/French equivalents
#
# 5. HOTEL NAME IN TITLE — "General Manager - Canopy by Hilton at Deer Valley"
#    Fix: if hotel name tokens appear in SERP title → strong accept
#
# Both the grounding LinkedIn lookup AND the iterative pipeline LinkedIn
# lookup call _linkedin_serp_valid() so the same logic applies everywhere.

_ROLE_FAMILIES: dict[str, set[str]] = {
    "gm": {
        "general manager",
        "resort manager",
        "hotel manager",
        "property manager",
        "managing director",
        "vp operations",
        "vice president operations",
        "vp hotel operations",
        "vp, operations",
        "v.p. operations",
        "regional vp",
        "cluster gm",
        "area general manager",
        "managing partner",
        "chief operating officer",
        "coo",
        "director of operations",
        "director, operations",
        "interim gm",
        "acting general manager",
        # Spanish / French
        "gerente general",
        "directeur général",
        "directeur des operations",
        "director general",
    },
    "purchasing": {
        "purchasing",
        "procurement",
        "sourcing",
        "buyer",
        "supply chain",
        "vendor",
        "ff&e",
        "ose",
        "os&e",
        "purchasing manager",
        "director of purchasing",
        "vp procurement",
        "vp supply chain",
        "strategic sourcing",
        "category manager",
        "accounting & procurement",
        "accounting and procurement",
        "svp accounting",
        "vp accounting",
        # Spanish / French
        "compras",
        "abastecimiento",
        "approvisionnement",
        "achats",
        "responsable achats",
        "directeur des achats",
    },
    "housekeeping": {
        "housekeeping",
        "housekeeper",
        "rooms",
        "rooms division",
        "director of rooms",
        "laundry",
        "uniform",
        "wardrobe",
        "executive housekeeper",
        "head housekeeper",
        "rooms & housekeeping",
        # Spanish / French
        "ama de llaves",
        "gouvernante",
        "gouvernante générale",
        "directeur hébergement",
    },
    "fb": {
        "food and beverage",
        "f&b",
        "food & beverage",
        "culinary",
        "executive chef",
        "restaurant",
        "banquet",
        "catering",
        "food beverage",
        "director of food",
        "director of catering",
        "director of banquets",
        # Spanish / French
        "alimentos y bebidas",
        "ayb",
        "directeur restauration",
        "directeur de la restauration",
    },
    "hr": {
        "human resources",
        "people and culture",
        "talent",
        "hr director",
        "people & culture",
        "hr manager",
        "talent acquisition",
        "director of people",
        "vp human resources",
        # Spanish / French
        "recursos humanos",
        "ressources humaines",
        "directeur des ressources",
    },
    "finance": {
        "accounting",
        "finance",
        "controller",
        "cfo",
        "financial",
        "director of finance",
        "vp finance",
        "chief financial",
        "director of accounting",
        # Spanish / French
        "finanzas",
        "contabilidad",
        "comptabilité",
        "directeur financier",
    },
    "ops": {
        "operations",
        "operational",
        "chief operating",
        "vp operations",
        "director of operations",
        "svp operations",
        "senior vice president",
        "svp",
        "evp",
        "executive vice president",
        # Spanish / French
        "operaciones",
        "opérations",
    },
    "sales_events": {
        "director of sales",
        "sales manager",
        "catering sales",
        "director of catering",
        "director of events",
        "banquet sales",
        "director of banquets",
        "convention services",
        "group sales",
    },
    "owner_dev": {
        "owner",
        "principal",
        "founder",
        "co-founder",
        "chairman",
        "managing member",
        "managing partner",
        "president",
        "ceo",
        "chief executive",
        "developer",
        "development",
        "investment",
    },
}

# Flattened set for quick "is this a hospitality role?" check
_ALL_ROLE_KEYWORDS: set[str] = {
    kw for family in _ROLE_FAMILIES.values() for kw in family
}

# Org name aliases — LinkedIn profiles often abbreviate company names.
# Key = lowercase canonical form, values = aliases seen on LinkedIn.
_ORG_ALIASES: dict[str, set[str]] = {
    "marriott vacations worldwide": {
        "mvw",
        "marriott vacations",
        "marriott vacation club",
        "marriott vacations worldwide corporation",
    },
    "hotel equities": {
        "he hospitality",
        "hotel equities group",
        "hotel equities inc",
        "he hotel",
    },
    "hyatt inclusive collection": {
        "hic",
        "apple leisure group",
        "alg",
        "alg vacations",
    },
    "aimbridge hospitality": {
        "aimbridge",
        "interstate hotels",
        "interstate hotels & resorts",
    },
    "crescent hotels": {
        "crescent hotels & resorts",
        "crescent hotels and resorts",
        "crescent",
    },
    "hilton": {
        "hilton hotels",
        "hilton worldwide",
        "hilton hotels & resorts",
        "hilton international",
    },
    "marriott": {
        "marriott international",
        "marriott hotels",
        "marriott hotels & resorts",
    },
    "hyatt": {"hyatt hotels", "hyatt hotels corporation", "hyatt hotels & resorts"},
    "ihg": {
        "intercontinental hotels",
        "ihg hotels & resorts",
        "intercontinental hotels group",
    },
    "sandals": {
        "sandals resorts",
        "beaches resorts",
        "sandals & beaches",
        "sandals resorts international",
    },
    "blue diamond resorts": {"blue diamond", "royalton hotels", "royalton"},
    "davidson hospitality": {"davidson hotels", "davidson hotel company"},
    "highgate": {"highgate hotels", "highgate hotel"},
    "concord hospitality": {"concord hospitality enterprises"},
    "pyramid global hospitality": {"pyramid hotel group", "pyramid global"},
}


def _org_tokens_match(org_name: str, haystack: str) -> bool:
    """Check if org name (or any known alias) appears in haystack.

    Handles:
    - Direct token match (≥2 distinctive tokens from org name in haystack)
    - Alias match (e.g. "MVW" matches "Marriott Vacations Worldwide")
    - Partial match on any alias abbreviation
    """
    if not org_name or not haystack:
        return False

    org_lower = org_name.lower().strip()

    # Direct substring match — most common case
    if org_lower in haystack:
        return True

    # Check canonical → alias table
    for canonical, aliases in _ORG_ALIASES.items():
        if org_lower == canonical or org_lower in aliases:
            # Check if canonical OR any alias appears in haystack
            if canonical in haystack:
                return True
            if any(alias in haystack for alias in aliases):
                return True

    # Reverse: haystack might contain an alias that maps to our org
    for canonical, aliases in _ORG_ALIASES.items():
        for alias in aliases:
            if alias in haystack and (
                org_lower == canonical or org_lower in aliases or canonical in org_lower
            ):
                return True

    # Token overlap: 2+ distinctive org words (≥4 chars) appear in haystack
    _ORG_STOPWORDS = {
        "the",
        "and",
        "of",
        "for",
        "at",
        "a",
        "an",
        "&",
        "hotel",
        "hotels",
        "resort",
        "resorts",
        "hospitality",
        "group",
        "inc",
        "llc",
        "ltd",
        "corp",
        "company",
        "international",
        "global",
        "management",
        "services",
    }
    org_words = [
        w
        for w in re.split(r"[^a-z0-9]+", org_lower)
        if len(w) >= 4 and w not in _ORG_STOPWORDS
    ]
    if len(org_words) >= 2:
        matched = sum(1 for w in org_words if w in haystack)
        if matched >= 2:
            return True
    elif len(org_words) == 1 and len(org_words[0]) >= 5:
        # Single distinctive word ≥5 chars (e.g. "equities", "sandals")
        if org_words[0] in haystack:
            return True

    return False


def _name_in_serp(name: str, serp_title: str) -> bool:
    """Check if person name appears in SERP title.

    Handles middle initials, hyphenated surnames, name order, suffixes,
    AND diacritics (Bolaños matches bolanos, de Kousemaeker etc).
    """
    if not name or not serp_title:
        return False

    # Strip diacritics so accented names match across SERP responses
    # that may or may not preserve the accents (e.g. LinkedIn often
    # returns 'Camilo Bolanos' even when the profile is 'Bolaños').
    import unicodedata

    def _strip_accents(s: str) -> str:
        return (
            unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
        )

    name_lower = _strip_accents(name.strip())
    title_lower = _strip_accents(serp_title)

    if name_lower in title_lower:
        return True

    # Strip common suffixes
    clean_name = re.sub(r"\b(jr|sr|ii|iii|iv|phd|mba|cha)\b\.?", "", name_lower).strip()

    # Split into parts, drop titles and single chars
    _NAME_STOP = {"dr", "mr", "mrs", "ms", "miss", "prof", "sir"}
    strip_chars = ".,;:'\""
    parts = [
        p.strip(strip_chars)
        for p in clean_name.split()
        if len(p.strip(strip_chars)) >= 2
        and p.strip(strip_chars).lower() not in _NAME_STOP
    ]

    if not parts:
        return False

    first = parts[0]
    last = parts[-1].split("-")[0]  # "meadows" from "meadows-johnson"

    if first in title_lower and last in title_lower:
        return True

    # All hyphenated last name components
    if len(parts) >= 2:
        last_parts = parts[-1].split("-")
        if first in title_lower and all(lp in title_lower for lp in last_parts):
            return True

    return False


def _role_family_match(contact_title: str, haystack: str) -> bool:
    """Check if the contact's title role family appears in the SERP haystack.

    If the contact is a "Resort Manager" and the SERP says "VP Operations" —
    both are in the "gm" family → match.
    If the contact is a "Director of Procurement" and the SERP says
    "SVP Accounting & Procurement" → both in "purchasing" family → match.
    """
    if not contact_title or not haystack:
        return False

    title_lower = contact_title.lower()

    # Find which family the contact's title belongs to
    contact_families = set()
    for family_name, keywords in _ROLE_FAMILIES.items():
        if any(kw in title_lower for kw in keywords):
            contact_families.add(family_name)

    if not contact_families:
        # Contact title not in any family — check raw hospitality keywords
        return any(kw in haystack for kw in _ALL_ROLE_KEYWORDS)

    # Check if ANY keyword from the SAME family appears in the SERP
    for family_name in contact_families:
        if any(kw in haystack for kw in _ROLE_FAMILIES[family_name]):
            return True

    return False


# Country-specific LinkedIn subdomains — these indicate the VIEWER's country,
# not the profile owner's country. A US-based hotel executive will always
# appear at www.linkedin.com/in/ regardless of where the searcher is located.
# If Serper returns an au./jm./za./etc. URL, it's often the wrong person
# (a local namesake) — require stronger evidence before accepting.
_LINKEDIN_COUNTRY_SUBDOMAINS = re.compile(
    r"^https?://([a-z]{2})\.linkedin\.com/in/",
    re.IGNORECASE,
)

# Extract the slug portion of a LinkedIn /in/ URL, e.g.
#   https://bs.linkedin.com/in/daniel-zuleta/  → "daniel-zuleta"
#   https://www.linkedin.com/in/daniel-zuleta-074246b → "daniel-zuleta-074246b"
_LINKEDIN_SLUG_RE = re.compile(r"linkedin\.com/in/([^/?#]+)/?", re.IGNORECASE)


def _is_clean_name_slug(url: str, name: str) -> bool:
    """Detect whether a LinkedIn URL slug is a clean canonical match to the
    person's full name — i.e., the slug is exactly 'first-last', 'last-first',
    'firstlast' (concatenated), or hyphenated full-name with no random hash.

    LinkedIn assigns clean slugs (no hash) only to ONE profile per name claim,
    so a clean slug match is provably the right person. Hash-suffixed slugs
    (e.g. 'daniel-zuleta-074246b') could be one of many namesakes.

    Examples:
      url='bs.linkedin.com/in/daniel-zuleta', name='Daniel Zuleta' → True
      url='jm.linkedin.com/in/mariodavalos', name='Mario Davalos' → True (NEW)
      url='www.linkedin.com/in/daniel-zuleta-074246b', name='Daniel Zuleta' → False
      url='www.linkedin.com/in/dz', name='Daniel Zuleta' → False
    """
    if not url or not name:
        return False
    m = _LINKEDIN_SLUG_RE.search(url)
    if not m:
        return False
    slug = m.group(1).lower().rstrip("/")

    # Build the candidate clean slug from the name (diacritic-stripped)
    import unicodedata

    name_norm = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    )
    name_norm = re.sub(r"[^a-z\s-]", "", name_norm).strip()
    # Two parsings: (a) drop initials/short tokens for hyphenated forms,
    # (b) keep all tokens for concatenated forms (handles "Steven M Rubin")
    parts = [p for p in re.split(r"[\s-]+", name_norm) if p and len(p) >= 2]
    parts_with_initials = [p for p in re.split(r"[\s-]+", name_norm) if p]
    if len(parts) < 2:
        return False  # need at least first + last

    first, last = parts[0], parts[-1]
    candidates = {
        # Hyphenated forms (skip 1-letter middle initials)
        f"{first}-{last}",
        f"{last}-{first}",
        "-".join(parts),  # 'luis-carlos-sarmiento'
        # Concatenated (no-hyphen) forms — many LinkedIn users use these
        # (e.g. mariodavalos, juansanchez)
        f"{first}{last}",
        f"{last}{first}",
        "".join(parts),
        # Concatenated WITH middle initials — handles "Steven M Rubin"
        # → "stevenmrubin"
        "".join(parts_with_initials),
        # Initial + last name — early-LinkedIn-user pattern (gsindhi for
        # Gaurav Sindhi, jdoe for John Doe). Also reverse: first + last_initial
        # (gauravs for Gaurav S.)
        f"{first[0]}{last}",
        f"{first}{last[0]}",
        f"{first[0]}-{last}",  # hyphenated variant: "g-sindhi"
    }
    return slug in candidates


def _slug_obviously_wrong_person(url: str, name: str) -> bool:
    """Permissive sanity check — return True only when the LinkedIn slug
    OBVIOUSLY belongs to a different named person.

    Used as the gate for the sparse-SERP fallback. We accept LinkedIn URLs
    when Serper returns few results (specific query → high signal), but we
    don't want to accept URLs that are clearly someone else.

    Logic: extract name tokens from the slug (stripping hyphens/numbers).
    If NONE of the contact's name tokens (first/last) appear in the slug
    AND NONE appear in the URL path at all → wrong person.
    Otherwise → trust it.

    Examples (name='Mario Davalos'):
      url='linkedin.com/in/john-smith'          → True (obviously wrong)
      url='linkedin.com/in/mariodavalos'        → False (right person)
      url='linkedin.com/in/davalos-mario-1234'  → False (right person, hash)
      url='linkedin.com/in/m-davalos'           → False (initial+last)
      url='linkedin.com/in/marioleonardo'       → False (could be middle name)
      url='linkedin.com/in/sindhi'              → False (last-name only, ambiguous)
    """
    if not url or not name:
        return False
    m = _LINKEDIN_SLUG_RE.search(url)
    if not m:
        return False
    slug = m.group(1).lower().rstrip("/")

    import unicodedata

    name_norm = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    )
    name_norm = re.sub(r"[^a-z\s-]", "", name_norm).strip()
    name_tokens = [p for p in re.split(r"[\s-]+", name_norm) if p and len(p) >= 3]
    if not name_tokens:
        return False  # name too short to validate; trust the SERP

    # Check 1: any name token (3+ chars) appears as substring in slug?
    slug_clean = slug.replace("-", "").replace("_", "")
    for tok in name_tokens:
        if tok in slug_clean:
            return False  # token found → trust this URL

    # Check 2: first or last initial + 2+ chars of other name token in slug?
    # Handles "g-sindhi" / "gsindhi" patterns where first token isn't full
    if len(name_tokens) >= 2:
        first_init = name_tokens[0][0]
        last_init = name_tokens[-1][0]
        if name_tokens[-1] in slug_clean or name_tokens[0] in slug_clean:
            return False
        # initial+last fragment
        if (
            f"{first_init}{name_tokens[-1][:3]}" in slug_clean
            or f"{name_tokens[0][:3]}{last_init}" in slug_clean
        ):
            return False

    # Nothing matched → likely wrong person
    return True


def _linkedin_serp_valid(
    name: str,
    serp_result: dict,
    query_tokens: list[str],
    hotel_name: str = "",
    management_company: str = "",
    developer: str = "",
    owner: str = "",
    contact_title: str = "",
) -> tuple[bool, str]:
    """Validate a LinkedIn SERP result for a specific contact.

    Returns (is_valid, reason_string).

    Decision tree:
    1. Name must appear in SERP title (flexible — allows middle initials, hyphenated)
    2. If org token found in haystack → ACCEPT (strongest signal)
    3. If hotel name tokens found in SERP title → ACCEPT (e.g. "GM - Canopy by Hilton")
    4. If role family match found → ACCEPT (VP Ops matches Resort Manager)
    5. If query token found IN SERP TITLE (not snippet) → ACCEPT
       (snippet-only match too weak — query words bleed into unrelated profiles)
    6. If any hospitality keyword → ACCEPT only if NOT a country-subdomain URL
    7. Otherwise REJECT

    Country-subdomain URLs (au., jm., za., etc.) require org-level evidence
    (steps 2-4) — the weaker signals (5-6) are insufficient because country
    subdomains frequently surface local namesakes rather than the target person.
    """
    serp_title = (serp_result.get("title") or "").lower()
    serp_snippet = (serp_result.get("snippet") or "").lower()
    haystack = serp_title + " " + serp_snippet
    r_url = serp_result.get("url", "")

    # Detect country-subdomain URLs — require stronger evidence for these
    country_subdomain_match = _LINKEDIN_COUNTRY_SUBDOMAINS.match(r_url)
    is_country_subdomain = bool(country_subdomain_match)
    if is_country_subdomain:
        country_code = country_subdomain_match.group(1).upper()

    # ── Step 1: Name check (required) ──
    if not _name_in_serp(name, serp_title):
        return False, f"name '{name}' not found in SERP title"

    # ── Step 2: Org token match (strongest — accepted for all URLs) ──
    for org in [management_company, developer, owner]:
        if org and _org_tokens_match(org, haystack):
            return True, f"org match: '{org}' in SERP"

    # Also check raw query tokens as org-level signal
    # (but only count if they appear in the SERP TITLE, not just snippet —
    # snippet can contain query echoes from unrelated profile sections)
    if any(t in serp_title for t in query_tokens):
        return True, "query token in SERP title"

    # ── Step 3: Hotel name in SERP title ──
    if hotel_name:
        hotel_words = [
            w
            for w in re.split(r"[^a-z0-9]+", hotel_name.lower())
            if len(w) >= 4
            and w not in {"hotel", "resort", "the", "and", "collection", "suites"}
        ]
        if hotel_words:
            matched_hotel = sum(1 for w in hotel_words if w in serp_title)
            if matched_hotel >= min(2, len(hotel_words)):
                return True, "hotel name tokens in SERP title"

    # ── Step 4: Role family match ──
    if contact_title and _role_family_match(contact_title, haystack):
        return True, f"role family match for '{contact_title}'"

    # ── Step 4.5: Clean URL slug exact-name match (high confidence) ──
    # If the LinkedIn URL slug is exactly "first-last" with no random hash,
    # this is provably the right person — LinkedIn assigns clean slugs to
    # only ONE profile per name. Accept even on country subdomains.
    # This catches cases like bs.linkedin.com/in/daniel-zuleta where the
    # profile is hosted on the country subdomain because the person is
    # based there, but it IS the right person.
    if _is_clean_name_slug(r_url, name):
        return True, "clean URL slug exact-name match"

    # ── Step 4.6: Role-token from contact's title appears in SERP ──
    # If the contact is "Director of Procurement" and the SERP haystack
    # contains "procurement" — that's strong proof we're on the right
    # profile. A random "Adam Butts" elsewhere on LinkedIn (accountant,
    # plumber, doctor) wouldn't have "procurement" or "housekeeping" or
    # "development" in their headline. The contact's KNOWN title from
    # grounding is independent evidence.
    if contact_title:
        _ROLE_STOPWORDS = {
            "the",
            "and",
            "of",
            "for",
            "at",
            "a",
            "an",
            "&",
            "in",
            "on",
            "to",
            "by",
            "or",
            "senior",
            "junior",
            "assistant",
            "associate",
            "head",
            "chief",
            "executive",
            "manager",
            "director",
        }
        role_tokens = set()
        for w in re.split(r"[^a-z0-9]+", contact_title.lower()):
            if len(w) >= 4 and w not in _ROLE_STOPWORDS:
                role_tokens.add(w)
        # Distinctive tokens like: procurement, purchasing, housekeeping,
        # construction, development, hospitality, operations, culinary,
        # banquet, sales, design. These rarely appear in unrelated profiles.
        if role_tokens and any(t in haystack for t in role_tokens):
            matched = [t for t in role_tokens if t in haystack][:2]
            return True, f"role keyword match: {matched}"

    # ── Steps 5-6: Weaker signals — REJECT for country subdomains ──
    if is_country_subdomain:
        return False, (
            f"country subdomain URL ({country_code}.linkedin.com) with only weak "
            f"evidence — likely local namesake, not target person. "
            f"Requires org/title match to accept."
        )

    # Query token in snippet only (weaker than title match)
    if any(t in serp_snippet for t in query_tokens):
        return True, "query token in SERP snippet"

    # Any hospitality keyword (weakest — only for www. URLs)
    if any(kw in haystack for kw in _ALL_ROLE_KEYWORDS):
        return True, "hospitality keyword in SERP (weak)"

    return False, "no org/role/hotel match in SERP — likely wrong person"


async def enrich_lead_contacts(
    lead_id: int,
    hotel_name: str,
    brand: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    management_company: Optional[str] = None,
    developer: Optional[str] = None,
    owner: Optional[str] = None,
    opening_date: Optional[str] = None,
    timeline_label: Optional[str] = None,
    description: Optional[str] = None,
    project_type_str: Optional[str] = None,
    search_name: Optional[str] = None,
    former_names: Optional[list] = None,
    progress_callback=None,
    *,
    is_existing_hotel: bool = False,
) -> EnrichmentResult:
    """
    Main enrichment entry (v5 — iterative researcher).

    Replaces the v4 fixed-query pipeline with an incremental researcher
    that asks ~3 queries, learns about the lead, then asks smarter
    follow-up queries based on what it learned. See iterative_researcher.py
    for the full strategy.

    Output shape is identical to v4 (EnrichmentResult), so all callers
    (routes, Celery tasks) keep working unchanged.

    progress_callback (optional): async callable invoked at the start of
    each iteration as `await cb(stage_num, total_stages, label)`. Used by
    the SSE endpoint to push live progress to the UI. Passing None (the
    default) preserves the old fire-and-forget behavior for Celery and
    batch jobs.

    is_existing_hotel (kw-only, default False): True when enriching an
    already-operating hotel from existing_hotels (vs a pre-opening
    potential_lead). Triggers lean-mode optimizations:
      - Iter 1 (Discovery) skipped — Smart Fill already gave us
        operator + owner; no need to re-derive.
      - Iter 2 (GM Hunt) uses 2 queries instead of 5 — open hotels
        have established teams + property websites with named GMs;
        the broad "find ANY GM mention" angles aren't needed.
      - Iter 3 (Corporate Hunt) GATED on Iter 2.5 results —
        if 2+ property-level decision-makers were found
        (housekeeping/F&B/operations/sales/HR/GM), corporate is
        skipped entirely. We already have the buyers.
    Other iterations (4-6.5) unchanged — verification quality
    matters equally regardless of property status.

    If iterative researcher fails for any reason, falls back to v4 legacy
    pipeline so enrichment never returns nothing.
    """
    from app.services.iterative_researcher import (
        ResearchState,
        run_iterative_research,
    )

    result = EnrichmentResult()
    logger.info(
        f"Starting enrichment v5 (iterative) for lead {lead_id}: {hotel_name}"
        + (" [LEAN MODE — open hotel]" if is_existing_hotel else "")
    )

    # ── Grounded fast-path: try one-shot grounding before iterative pipeline ──
    # Single Gemini googleSearch call asks the business question directly:
    # "Who at [Hotel/Management Company] makes uniform purchasing decisions?"
    # If 2+ named contacts found → use them, skip the 2-3 minute pipeline.
    # Falls back to iterative researcher on failure or < 2 contacts found.
    if progress_callback is not None:
        try:
            await progress_callback(1, 11, "Researching contacts via Gemini Search")
        except Exception:
            pass

    grounded_contacts = await _enrich_contacts_grounded(
        hotel_name=hotel_name,
        brand=brand,
        management_company=management_company,
        developer=developer,
        owner=owner,
        city=city,
        state=state,
        country=country,
        opening_date=opening_date,
    )

    if grounded_contacts is not None:
        logger.info(
            f"Contact grounding fast-path SUCCESS for {hotel_name}: "
            f"{len(grounded_contacts)} contacts — ALSO running iterative pipeline "
            f"to find operational staff (Directors of Operations/Housekeeping/Purchasing)"
        )
        result.contacts = grounded_contacts
        result.layers_tried = ["grounding"]
        result.management_company = management_company

        # Run Gemini verification to assign proper scopes + reject false positives
        if progress_callback is not None:
            try:
                await progress_callback(10, 11, "Verifying contact scope (Gemini)")
            except Exception:
                pass
        if result.contacts:
            try:
                result.contacts = await _verify_contacts_with_gemini(
                    contacts=result.contacts,
                    hotel_name=hotel_name,
                    brand=brand,
                    management_company=management_company,
                    city=city,
                    state=state,
                    country=country,
                    opening_date=opening_date,
                    project_type=project_type_str,
                    developer=developer,
                    owner=owner,
                )
            except Exception as ex:
                logger.warning(
                    f"Grounding contact verification failed (keeping raw): {ex}"
                )

        # ── LinkedIn URL lookup for grounded contacts missing a URL ──
        # Grounding sometimes finds names but not their LinkedIn profile URL.
        # Run the same targeted lookup the iterative pipeline uses:
        # site:linkedin.com/in + name + hotel/operator tokens.
        # Capped at 3 contacts to stay fast (grounding already got us the names).
        _LI_STOPWORDS = {
            "the",
            "and",
            "of",
            "for",
            "at",
            "a",
            "an",
            "&",
            "in",
            "on",
            "to",
            "by",
            "or",
            "hotel",
            "hotels",
            "resort",
            "resorts",
            "spa",
            "inn",
            "suites",
            "suite",
            "lodge",
            "club",
            "property",
            "hospitality",
            "collection",
            "collections",
            "autograph",
            "curio",
            "tribute",
            "luxury",
            "group",
            "company",
            "corp",
            "inc",
            "llc",
            "ltd",
            "international",
            "global",
            "management",
            "services",
            "grand",
            "plaza",
            "palace",
        }

        def _li_tokens(sources_list):
            out = set()
            for s in sources_list:
                for word in re.split(r"[^a-z0-9]+", (s or "").lower()):
                    if len(word) >= 3 and word not in _LI_STOPWORDS:
                        out.add(word)
            return out

        li_lookup_count = 0
        for c in result.contacts:
            if li_lookup_count >= 5:
                continue
            # NOTE: always run Serper lookup even if grounding returned a LinkedIn URL.
            # Grounding URLs are frequently garbled (e.g. matt-fowler-91004a4 instead of
            # matt-fowler). Serper verifies the URL by checking the SERP title contains
            # the person's name — this replaces the bad grounding URL with the correct one.
            name = (c.get("name") or "").strip()
            if not name:
                continue
            try:
                # ── Scope-aware token selection ──
                # For hotel_specific contacts → hotel + brand tokens (their
                # LinkedIn names the property)
                # For owner / management_corporate / chain_* contacts → use
                # their OWN organization (Grupo Posadas, Cotton Bay Holdings,
                # Marriott International) since their LinkedIn profile is
                # about the parent company, not a specific property.
                scope = (c.get("scope") or "").lower()
                contact_org = (c.get("organization") or "").strip()
                is_corporate = scope in (
                    "management_corporate",
                    "chain_corporate",
                    "chain_area",
                    "owner",
                )

                if is_corporate:
                    # Try contact's own org first, then mgmt company / owner
                    # / developer as fallbacks
                    tokens = _li_tokens([contact_org])
                    if not tokens:
                        tokens = _li_tokens([management_company, owner, developer])
                    if not tokens:
                        tokens = _li_tokens([hotel_name])
                else:
                    # hotel_specific or unknown — hotel name first
                    tokens = _li_tokens([hotel_name, management_company])
                    if not tokens:
                        tokens = _li_tokens([contact_org])

                if not tokens:
                    continue
                tokens_clause = " OR ".join(f'"{t}"' for t in sorted(tokens)[:3])
                li_q = f'"{name}" ({tokens_clause}) site:linkedin.com/in'
                li_results = await _search_web(li_q, max_results=3)
                query_tokens = [t.strip('"') for t in tokens_clause.split(" OR ")]

                # ── Filter to actual /in/ profile URLs ──
                profile_results = [
                    r for r in li_results if "linkedin.com/in/" in (r.get("url") or "")
                ]

                # ── Strict-filter pass: try each result against full validation ──
                accepted = False
                strict_rejection_reasons = []
                for r in profile_results:
                    r_url = r["url"]
                    valid, reason = _linkedin_serp_valid(
                        name=name,
                        serp_result=r,
                        query_tokens=query_tokens,
                        hotel_name=hotel_name,
                        management_company=management_company or "",
                        developer=developer or "",
                        owner=owner or "",
                        contact_title=c.get("title") or "",
                    )
                    if valid:
                        c["linkedin"] = _canonicalize_linkedin_url(r_url)
                        logger.info(
                            f"Grounding LinkedIn URL found for {name}: "
                            f"{c['linkedin']} ({reason})"
                        )
                        li_lookup_count += 1
                        accepted = True
                        break
                    strict_rejection_reasons.append((r_url, reason))

                # ── Sparse-result fallback (Jay's principle, 2026-05-07) ──
                # If the strict filter rejected everything BUT Serper only
                # returned 1-2 results for our specific quoted-name + org
                # query, those results are likely correct — Serper's query
                # specificity makes few-result responses high-signal. Adding
                # more slug patterns to the strict filter is whack-a-mole;
                # trusting a sparse SERP is the correct architectural choice.
                # We only require: (a) it's a real LinkedIn /in/ URL, and
                # (b) the URL does NOT obviously belong to a different
                # named person (e.g. slug starts with a totally different
                # first-name token).
                if not accepted and 1 <= len(profile_results) <= 2:
                    for r_url, prev_reason in strict_rejection_reasons:
                        if not _slug_obviously_wrong_person(r_url, name):
                            c["linkedin"] = _canonicalize_linkedin_url(r_url)
                            logger.info(
                                f"Grounding LinkedIn URL found for {name}: "
                                f"{c['linkedin']} (sparse SERP fallback — "
                                f"{len(profile_results)} result(s), strict "
                                f"check said: {prev_reason})"
                            )
                            li_lookup_count += 1
                            accepted = True
                            break

                # ── Log final rejections only if we couldn't accept any ──
                if not accepted:
                    for r_url, reason in strict_rejection_reasons:
                        logger.info(f"LinkedIn REJECTED for {name}: {reason}")
            except Exception as li_e:
                logger.debug(f"Grounding LinkedIn lookup failed for {name}: {li_e}")

        # Score contacts using unified scoring module
        from app.services.contact_scoring import score_contact_dict

        for c in result.contacts:
            score_contact_dict(c)

        # ── Sort: property-first cascade, score is tie-breaker within scope ──
        # Pure score sorting let mgmt-corporate Tier-1 SVP Procurement (1.5×20=30)
        # tie with property-level Tier-3 GM (3.0×10=30) and corporate often won
        # on Gemini's confidence boost. The cascade is the business rule:
        # property staff > owner > mgmt corporate > chain corporate. Score is
        # only a tie-breaker WITHIN a scope bucket.
        _GROUNDING_SCOPE_RANK = {
            "hotel_specific": 0,  # at THIS hotel — primary uniform buyer
            "owner": 1,  # check-writer (pre-opening) — second-best
            "chain_area": 2,  # regional cluster covering this property
            "management_corporate": 3,  # operator HQ — backup only
            "chain_corporate": 4,  # brand parent — almost never the buyer
        }

        def _grounding_sort_key(c: dict) -> tuple:
            scope = (c.get("scope") or "unknown").lower()
            scope_rank = _GROUNDING_SCOPE_RANK.get(scope, 5)
            score = c.get("_validation_score") or 0
            return (scope_rank, -score)

        result.contacts.sort(key=_grounding_sort_key)
        result.contacts = _apply_smart_cap(result.contacts, MAX_CONTACTS_TO_SAVE)

        if progress_callback is not None:
            try:
                await progress_callback(11, 11, "Saving & scoring contacts")
            except Exception:
                pass

        # ── Grounding succeeded — check quality ──
        # Grounding is designed to find the BEST contacts at this moment:
        #   • Hotel-specific GM / Dir of Housekeeping / Dir of Purchasing → IDEAL
        #   • Management company operational staff (SVP Procurement, Dir of Ops) → GREAT
        #   • Management company leadership (COO, VP) → OK
        #   • CEO / Founder / Owner only → run iterative to supplement
        #
        # Two reasons to fall through to iterative:
        #   (1) all_cxo: only pure C-suite/investor contacts (don't buy uniforms)
        #   (2) no_operational: only owners and/or brand-parent corporate, with
        #       NO operational buyer (hotel_specific / management_corporate /
        #       chain_area). Owners are check-writers and worth keeping, but
        #       they don't run housekeeping or sign vendor POs. We need at least
        #       one operational contact before calling grounding "complete."
        #       This was the Ritz-Carlton Savannah failure: 2 owners at TMGOC
        #       Ventures came back, system said "complete" and never searched
        #       for the incoming GM or Marriott's Ritz-Carlton regional ops.
        _CXO_ONLY_KW = {
            "ceo",
            "chief executive officer",
            "founder",
            "co-founder",
            "chairman",
            "board member",
            "investor",
            "angel investor",
        }

        _OPERATIONAL_SCOPES = {
            "hotel_specific",
            "management_corporate",
            "chain_area",
        }

        def _is_cxo_only(contact):
            title = (contact.get("title") or "").lower()
            scope = (contact.get("scope") or "").lower()
            # Owner scope is intentional (check-writer) — keep it
            if scope == "owner":
                return False
            return any(kw in title for kw in _CXO_ONLY_KW) and not any(
                kw in title
                for kw in {
                    "operations",
                    "procurement",
                    "purchasing",
                    "housekeeping",
                    "rooms",
                    "general manager",
                    "hotel manager",
                    "resort manager",
                }
            )

        all_cxo = result.contacts and all(_is_cxo_only(c) for c in result.contacts)

        # has_operational = at least one contact with operational buying authority
        has_operational = any(
            (c.get("scope") or "").lower() in _OPERATIONAL_SCOPES
            for c in result.contacts
        )

        if not all_cxo and has_operational:
            # Grounding found useful contacts (operational staff, mgmt company,
            # or properly scoped owners) — trust it, skip iterative
            logger.info(
                f"Contact grounding complete for {hotel_name}: "
                f"{len(result.contacts)} contacts — skipping iterative pipeline ✅"
            )
            return result
        elif all_cxo:
            # Grounding only found pure C-suite (CEO/Founder) — these don't
            # buy uniforms. Run iterative to find operational staff.
            logger.info(
                f"Contact grounding found only C-suite contacts for {hotel_name} "
                f"— running iterative to find operational staff"
            )
            # Fall through — grounding contacts saved, merged after iterative
        else:
            # Grounding returned contacts but no operational buyer — only owners
            # and/or brand-parent corporate. Owners are check-writers worth
            # keeping, but the property GM, Director of Housekeeping, and
            # operator's procurement team are still missing. Run iterative
            # to fill those slots, then merge results.
            scope_summary = ", ".join(
                sorted({(c.get("scope") or "unknown") for c in result.contacts})
            )
            logger.info(
                f"Contact grounding for {hotel_name}: {len(result.contacts)} "
                f"contacts but NO operational buyer (scopes: {scope_summary}) — "
                f"running iterative to find property staff / mgmt-company ops, "
                f"will merge with grounding results"
            )
            # Fall through — grounding contacts saved, merged after iterative

    # Run iterative researcher — fallback (grounding failed/timed out/C-suite only)
    grounding_had_contacts = len(result.contacts) > 0
    if not grounding_had_contacts:
        logger.info(
            f"Contact grounding insufficient for {hotel_name} — "
            f"falling back to iterative pipeline"
        )

    # ── Build research state from lead facts ──
    research_state = ResearchState(
        hotel_name=hotel_name,
        brand=brand,
        management_company=management_company,
        city=city,
        state=state,
        country=country,
        opening_date=opening_date,
        timeline_label=timeline_label,
        project_type=project_type_str,
        search_name=search_name,
        former_names=former_names,
        description=description,  # ← Phase B: flow DB description into classifier
        is_existing_hotel=is_existing_hotel,  # ← Lean mode flag
    )

    # ── Run the iteration loop ──
    try:
        await run_iterative_research(
            research_state,
            progress_callback=progress_callback,
        )
        # Phase B: surface project-type rejection flags to caller
        result.should_reject = research_state.should_reject
        result.rejection_reason = research_state.rejection_reason
    except Exception as ex:
        logger.exception(f"Iterative researcher failed, falling back to v4: {ex}")
        return await _enrich_lead_contacts_v4_legacy(
            lead_id=lead_id,
            hotel_name=hotel_name,
            brand=brand,
            city=city,
            state=state,
            country=country,
            management_company=management_company,
            opening_date=opening_date,
            timeline_label=timeline_label,
            description=description,
            project_type_str=project_type_str,
        )

    # ── Convert ResearchState into EnrichmentResult ──
    # Merge grounding contacts (CEO/COO level) with iterative contacts
    # (operational Directors). Grounding contacts go first so they don't
    # get pushed out by the smart cap — they're still valid P2/P3 contacts.
    grounding_contacts = list(result.contacts)  # save grounding results
    result.contacts = []
    for n in research_state.discovered_names:
        result.contacts.append(
            {
                "name": n.get("name", ""),
                "title": n.get("title", ""),
                "organization": n.get("organization", ""),
                "scope": n.get("scope", "unknown"),
                "confidence": n.get("confidence", "medium"),
                "source": n.get("source", ""),
                "source_type": n.get("source_type", "trade_press"),
                "source_detail": n.get("source_detail"),  # Rich evidence from Iter 5/6
                "linkedin": n.get("linkedin"),
                # Evidence array captured during snippet extraction —
                # list of {quote, source_url, source_domain, trust_tier,
                # source_year, ...} items. Rendered as per-contact
                # evidence cards in the UI.
                "_evidence_items": n.get("_evidence_items", []),
                "_iteration_found": n.get("_iteration_found"),
                "_verification_result": n.get("_verification_result"),
                "_current_employer": n.get("_current_employer"),
                "_current_title": n.get("_current_title"),
                "_role_period": n.get("_role_period"),
                "_final_priority": n.get("_final_priority"),  # Iter 6: P1/P2/P3/P4
                "_final_reasoning": n.get(
                    "_final_reasoning"
                ),  # Iter 6: strategist reasoning
            }
        )
    # Merge grounding contacts in — dedupe by normalized name
    if grounding_contacts:
        existing_names = {
            (c.get("name") or "").lower().strip() for c in result.contacts
        }
        added_from_grounding = 0
        for gc in grounding_contacts:
            gc_name = (gc.get("name") or "").lower().strip()
            if gc_name and gc_name not in existing_names:
                result.contacts.append(gc)
                existing_names.add(gc_name)
                added_from_grounding += 1
        if added_from_grounding:
            logger.info(
                f"Merged {added_from_grounding} grounding contacts into "
                f"iterative results for {hotel_name}"
            )

    result.sources_used = list(set(research_state.urls_scraped))
    result.layers_tried = ["grounding"] + [
        f"iter_{i}" for i in range(1, research_state.iterations_done + 1)
    ]
    # Prefer SmartFill's management_company (actual operator like "Crescent")
    # over Shift A's operator_parent (brand flag like "Marriott")
    result.management_company = management_company or research_state.operator_parent

    # ── Apply Gemini verification on the discovered contacts ──
    # CRITICAL: _verify_contacts_with_gemini returns NEW contact dicts that
    # don't carry our Iter 5/6 metadata forward. Save the strategist verdicts
    # by name BEFORE calling it, then re-merge them onto the survivors AFTER.
    # Without this, strategist_priority is NULL in the DB and priority badges
    # fall back to algorithmic values.
    #
    # Progress: the iterations emitted events 1-9. This Gemini scope-check
    # is a real, user-visible stage (can take 30-60s under 429 backoff), so
    # we expose it as "stage 10" even though _TOTAL=9 in run_iterative_research.
    # The progress bar will cap at ~95% here — honest signal that work is
    # still happening.
    if progress_callback is not None:
        try:
            # Emit as a fractional stage — tells the UI "past 9, not yet done"
            await progress_callback(10, 11, "Verifying contact scope (Gemini)")
        except Exception as e:
            logger.debug(f"Post-iter progress callback failed (non-fatal): {e}")

    strategist_verdicts_by_name: dict = {}
    for c in result.contacts:
        nm = (c.get("name") or "").strip().lower()
        if not nm:
            continue
        strategist_verdicts_by_name[nm] = {
            "_final_priority": c.get("_final_priority"),
            "_final_reasoning": c.get("_final_reasoning"),
            "source_detail": c.get("source_detail"),
            "_verification_result": c.get("_verification_result"),
            "_current_employer": c.get("_current_employer"),
            "_current_title": c.get("_current_title"),
            "_role_period": c.get("_role_period"),
            "_iteration_found": c.get("_iteration_found"),
            # Preserve evidence — Gemini verification rebuilds contact dicts
            # and wipes our capture, so we re-merge it after verification.
            "_evidence_items": c.get("_evidence_items", []),
        }

    if result.contacts:
        # Build a combined list of ALL verified operators so the prompt
        # doesn't reject contacts from the management company.
        # "Marriott" = brand flag, "Crescent Hotels & Resorts" = actual operator.
        # BOTH are valid — contacts from either should NOT be rejected.
        all_operators = set()
        if research_state.operator_parent:
            all_operators.add(research_state.operator_parent)
        if management_company:
            all_operators.add(management_company)
        for vc in research_state.verified_current_companies:
            all_operators.add(vc)
        combined_mgmt = " / ".join(sorted(all_operators)) or "Unknown"

        try:
            result.contacts = await _verify_contacts_with_gemini(
                contacts=result.contacts,
                hotel_name=hotel_name,
                brand=brand,
                management_company=combined_mgmt,
                city=city,
                state=state,
                country=country,
                opening_date=opening_date,
                project_type=research_state.project_stage,
                developer=developer,
                owner=owner,
            )
        except Exception as ex:
            logger.warning(f"Gemini verification failed (keeping raw contacts): {ex}")

    # ── Re-merge strategist verdicts onto surviving contacts ──
    # Gemini verification removed some contacts (rejected) and rebuilt the
    # dicts for the ones it kept. Restore our Iter 5/6 fields so the strategist
    # priority actually reaches the DB.
    merged_count = 0
    for c in result.contacts:
        nm = (c.get("name") or "").strip().lower()
        saved = strategist_verdicts_by_name.get(nm)
        if not saved:
            continue
        for key, val in saved.items():
            if val is not None and not c.get(key):
                c[key] = val
        if saved.get("_final_priority"):
            merged_count += 1
    if merged_count:
        logger.info(
            f"[PERSIST] Re-merged strategist verdicts onto {merged_count} "
            f"surviving contacts after Gemini verification"
        )

    # ── Fuzzy name dedupe ──
    # Collapses Amanda/Mandy/Amy variants, "Dr. Kali Chaudhuri" vs
    # "Kali Chaudhuri", "Michael T. George" vs "Michael Thomas George",
    # etc. Keeps the contact with the highest score; merges evidence.
    result.contacts = _fuzzy_dedupe_contacts(result.contacts)

    # ── Classify tier + score every contact (unified scoring module) ──
    # Single source of truth: app.services.contact_scoring.score_contact_dict
    # Same formula used by: manual add, edit contact, toggle scope.
    # Changes to scoring logic live in ONE place now.
    #
    # Writes these keys onto each contact dict (consumed by
    # persist_enrichment_contacts and routes/contacts.py):
    #   _validation_score       → final int score
    #   _buyer_tier             → BuyerTier enum name
    #   _validation_confidence  → "high" | "medium" | "low"
    #   _score_breakdown        → JSONB breakdown for score_breakdown column
    from app.services.contact_scoring import score_contact_dict

    for c in result.contacts:
        score_contact_dict(c)

    # ── Sort contacts: by Iter 6 final priority first (P1→P4), then score ──
    _PRIORITY_RANK = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
    result.contacts.sort(
        key=lambda c: (
            _PRIORITY_RANK.get(c.get("_final_priority") or "", 4),  # P1 contacts first
            # Within same priority, prefer property-specific scope
            {
                "hotel_specific": 0,
                "chain_area": 1,
                "management_corporate": 2,
                "chain_corporate": 3,
                "owner": 2,
            }.get(c.get("scope") or "unknown", 4),
            -(c.get("_validation_score") or 0),
        )
    )

    # ── SMART CAP: top 6 with distribution ──
    # Without this, the cap would just take the top 6 by score — producing
    # 6 near-duplicate management_corporate execs and pushing the owner
    # (who has a lower base score but is THE most important pre-opening
    # contact) off the list. This preserves variety: owner + 2 P1 + 2 P2 +
    # 1 best-remaining = balanced outreach target list.
    pre_cap = len(result.contacts)
    result.contacts = _apply_smart_cap(result.contacts, MAX_CONTACTS_TO_SAVE)
    if pre_cap > len(result.contacts):
        logger.info(
            f"[SMART CAP] {pre_cap} → {len(result.contacts)} contacts kept "
            f"(dropped {pre_cap - len(result.contacts)} lower-value)"
        )

    # Final stage event before returning — tells the UI all work is done,
    # contacts are sorted + capped + ready to render.
    if progress_callback is not None:
        try:
            await progress_callback(11, 11, "Saving & scoring contacts")
        except Exception as e:
            logger.debug(f"Final progress callback failed (non-fatal): {e}")

    logger.info(
        f"Enrichment v5 complete for {hotel_name}: "
        f"{len(result.contacts)} contacts, "
        f"iters={research_state.iterations_done}, "
        f"queries_run={len(research_state.queries_run)}, "
        f"discovered_owner={research_state.owner_company!r}, "
        f"discovered_operator_parent={research_state.operator_parent!r}"
    )

    return result


async def _enrich_lead_contacts_v4_legacy(
    lead_id: int,
    hotel_name: str,
    brand: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    management_company: Optional[str] = None,
    opening_date: Optional[str] = None,
    timeline_label: Optional[str] = None,
    description: Optional[str] = None,
    project_type_str: Optional[str] = None,
) -> EnrichmentResult:
    """
    Main enrichment function v4. Runs multi-layer search with
    SAP-trained validation and auto-retry on false positives.

    Now phase-aware: uses timeline_label + project_type to determine
    the correct starting phase (1=corporate, 2=GM, 3=dept heads)
    and cascades automatically if a phase returns nothing.
    """
    result = EnrichmentResult()
    logger.info(f"Starting enrichment v4 for lead {lead_id}: {hotel_name}")

    # ── Classify project type + determine starting phase ──
    pt = classify_project_type(
        hotel_name=hotel_name,
        description=description or "",
        project_type=project_type_str or "",
        timeline_label=timeline_label or "",
        management_company=management_company or "",
    )
    logger.info(
        f"Project type: {pt.project_type} (confidence={pt.confidence}) | "
        f"Starting phase: {pt.starting_phase} | {pt.phase_reason[:80]}"
    )
    result.metadata["project_type"] = pt.project_type
    result.metadata["starting_phase"] = pt.starting_phase
    result.metadata["phase_reason"] = pt.phase_reason
    result.metadata["phase_history"] = []

    # ── Layer 1: Web search + scrape + AI extract ──
    try:
        found = await _layer_web_search(
            hotel_name,
            brand,
            management_company,
            city,
            state,
            country,
            opening_date,
            result,
            retry_attempt=0,
            phase=pt.starting_phase,
            project_type=pt.project_type,
        )
        if found:
            hotel_specific = [
                c for c in result.contacts if c.get("scope") == "hotel_specific"
            ]
            logger.info(
                f"Layer 1: {len(result.contacts)} contacts "
                f"({len(hotel_specific)} hotel-specific)"
            )
    except Exception as e:
        result.errors.append(f"Web search failed: {str(e)}")
        logger.error(f"Layer 1 error: {e}")

    # ── Layer 2: LinkedIn snippet extraction ──
    try:
        found = await _layer_linkedin_snippets(
            hotel_name,
            brand,
            management_company,
            city,
            state,
            country,
            result,
        )
        if found:
            logger.info("Layer 2: LinkedIn snippets found contacts")
    except Exception as e:
        result.errors.append(f"LinkedIn snippets failed: {str(e)}")
        logger.error(f"Layer 2 error: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TITLE RESOLUTION — search for contacts with missing titles
    # ═══════════════════════════════════════════════════════════════
    untitled = [c for c in result.contacts if not c.get("title", "").strip()]
    if untitled:
        logger.info(f"Title resolution: {len(untitled)} contacts missing titles")
        for contact in untitled:
            name = contact.get("name", "").strip()
            if not name:
                continue
            try:
                # Build a tiered set of queries — start strict, then loosen.
                # Strict full-name + full-hotel often returns ZERO results when
                # the DB hotel name has suffixes (e.g. "East Village") that don't
                # appear on the actual LinkedIn profile.
                queries = [f'"{name}" "{hotel_name}"']

                # Loose fallback 1: name + brand (no quotes on hotel)
                # e.g. "Andrew Carey" Canopy Deer Valley
                hotel_words = [w for w in hotel_name.split() if len(w) > 2]
                # Drop common suffix words that often differ between sources
                drop_words = {
                    "east",
                    "west",
                    "north",
                    "south",
                    "village",
                    "downtown",
                    "uptown",
                    "the",
                    "and",
                    "at",
                    "by",
                    "of",
                    "resort",
                    "hotel",
                    "hotels",
                }
                core_words = [w for w in hotel_words if w.lower() not in drop_words]
                if core_words:
                    loose = " ".join(core_words[:4])
                    queries.append(f'"{name}" {loose}')

                # Loose fallback 2: name + linkedin (catches profile pages directly)
                queries.append(
                    f'"{name}" linkedin {core_words[0] if core_words else ""}'.strip()
                )

                search_results = []
                for q in queries:
                    search_results = await _search_web(q, max_results=3)
                    if search_results:
                        break

                for sr in search_results:
                    snippet = (
                        sr.get("snippet", "") + " " + sr.get("title", "")
                    ).lower()
                    # Check for direct role mentions
                    role_keywords = [
                        "general manager",
                        "regional vice president",
                        "vp and general manager",
                        "regional vp",
                        "director of operations",
                        "director of housekeeping",
                        "executive housekeeper",
                        "director of rooms",
                        "director of purchasing",
                        "executive chef",
                        "hotel manager",
                        "resort manager",
                        "director of food and beverage",
                        "director of human resources",
                        "director of people and culture",
                        "purchasing manager",
                    ]
                    for role in role_keywords:
                        if role in snippet and name.split()[0].lower() in snippet:
                            # Capitalize properly
                            resolved_title = role.title()
                            contact["title"] = resolved_title
                            contact["_title_source"] = "web_resolution"
                            logger.info(
                                f"Title resolved: {name} → {resolved_title} (from web search)"
                            )
                            break
                    if contact.get("_title_source"):
                        break
                if not contact.get("_title_source"):
                    logger.debug(f"Title not resolved for {name}")
            except Exception as e:
                logger.debug(f"Title resolution failed for {name}: {e}")

    # ═══════════════════════════════════════════════════════════
    # GEMINI AI VERIFICATION — fix false positives before scoring
    # ═══════════════════════════════════════════════════════════

    # PRE-GEMINI PROTECTION: deterministically mark contacts whose title or
    # raw snippet PROVES they work at the target hotel (contiguous phrase
    # match + plausible person name). These survive even if Gemini votes
    # to reject them — Gemini sometimes loses hotel context after title
    # resolution overwrites the original headline.
    #
    # v4.2 TIGHTENED: protection now requires
    #   (a) the hotel name (or its distinctive bigram) appears as a
    #       contiguous phrase in title/org/snippet — NOT just word-bag
    #       overlap that produced false positives like Iqbal Mallik
    #       (Magnuson Grand) being protected as the Pan Am Hotel GM,
    #   AND
    #   (b) the contact's name passes the person-name heuristic — so
    #       publication names like "Travel Turtle Magazine" can never
    #       be protected.
    def _title_proves_hotel(contact: dict, hotel: str) -> bool:
        """Return True only if (a) hotel name appears as a contiguous
        phrase in title/snippet/org AND (b) the contact name looks like
        a real person."""
        if not _looks_like_real_person(contact.get("name", "")):
            return False
        haystacks = [
            contact.get("title") or "",
            contact.get("_raw_snippet") or "",
            contact.get("_raw_title") or "",
            contact.get("organization") or "",
        ]
        return _hotel_phrase_appears(hotel, haystacks)

    for c in result.contacts:
        if _title_proves_hotel(c, hotel_name):
            c["_protected_title_match"] = True
            logger.info(
                f"PROTECTED: {c.get('name')} — hotel name appears verbatim in "
                f"title/snippet, immune to Gemini rejection"
            )

    if result.contacts:
        try:
            # Keep a reference to the pre-verify list. _verify_contacts_with_gemini
            # mutates contacts in place (setting _gemini_rejected on rejected ones)
            # but RETURNS only the non-rejected subset. We need the original list
            # to find protected contacts that Gemini tried to reject.
            contacts_before_verify = result.contacts
            result.contacts = await _verify_contacts_with_gemini(
                contacts=result.contacts,
                hotel_name=hotel_name,
                brand=brand,
                management_company=management_company or result.management_company,
                city=city,
                state=state,
                country=country,
                opening_date=opening_date,
                project_type=project_type_str,
            )
            # Restore protected contacts that Gemini tried to reject.
            # These are in contacts_before_verify with _gemini_rejected=True but
            # are NOT in the returned list because _verify drops rejects.
            #
            # FIX: Even when "title proves hotel" (target hotel name appears
            # in snippet), we must NOT override Gemini if its rejection reason
            # explicitly identifies the contact as a hotel/brand name rather
            # than a person, OR as belonging to a different hotel, OR as
            # not having a confirmable operational role. Trust Gemini in
            # these cases — the heuristic is wrong.
            #
            # ADDITIONAL FIX: Even if the rejection reason isn't on the trust
            # list, never force-keep a contact with no title. A "high
            # confidence hotel_specific" tag on someone whose title is empty
            # is meaningless and contaminates the contact list.
            _GEMINI_TRUST_PHRASES = (
                # Identity-based — not a person at all
                "is a hotel name",
                "is likely a hotel",
                "is the hotel",
                "is the resort",
                "is a brand",
                "is a brand name",
                "is the name of",
                "is a property",
                "not a person",
                "is not a person",
                # Wrong target
                "different hotel",
                "different property",
                "different resort",
                "wrong hotel",
                "wrong property",
                "mentioned in a post by someone else",
                # No usable role
                "no current job title",
                "no specific job title",
                "no specific operational title",
                "no operational title",
                "no job title provided",
                "is not a current job title",
                "aspiring",
                "aspiring chef",
                "aspiring professional",
                # Wrong role for uniform sales
                "not operational hotel staff",
                "is a sales/marketing role",
                "sales role",
                "marketing role",
            )

            def _has_real_title(contact: dict) -> bool:
                """Contact must have a non-empty, non-generic title to be
                worth force-keeping despite Gemini rejection."""
                title = (contact.get("title") or "").strip().lower()
                if not title or len(title) < 4:
                    return False
                # Reject placeholder/generic titles
                generic_titles = {
                    "director of",
                    "manager",
                    "professional",
                    "staff",
                    "employee",
                    "aspiring",
                    "aspiring chef",
                    "student",
                    "intern",
                    "trainee",
                }
                return title not in generic_titles

            restored_names = {
                c.get("name", "").lower().strip() for c in result.contacts
            }
            for c in contacts_before_verify:
                if (
                    c.get("_protected_title_match")
                    and c.get("_gemini_rejected")
                    and c.get("name", "").lower().strip() not in restored_names
                ):
                    rejection_reason = (c.get("_gemini_rejection_reason") or "").lower()
                    # Trust Gemini when reason matches any trust phrase
                    if any(p in rejection_reason for p in _GEMINI_TRUST_PHRASES):
                        logger.info(
                            f"NO OVERRIDE: trusting Gemini reject for "
                            f"{c.get('name')} — reason matches trust phrase "
                            f"({(c.get('_gemini_rejection_reason') or '')[:80]})"
                        )
                        continue
                    # Also reject the override if the contact has no real title.
                    # A no-title contact should NEVER be force-kept as
                    # "high confidence hotel_specific".
                    if not _has_real_title(c):
                        logger.info(
                            f"NO OVERRIDE: refusing to force-keep "
                            f"{c.get('name')} — title is empty/generic "
                            f"({c.get('title')!r}), can't justify override"
                        )
                        continue
                    logger.info(
                        f"OVERRIDE: keeping {c.get('name')} despite Gemini reject "
                        f"({c.get('_gemini_rejection_reason')}) — title proves hotel"
                    )
                    c["_gemini_rejected"] = False
                    c["scope"] = "hotel_specific"
                    c["confidence"] = "high"
                    result.contacts.append(c)
                elif c.get("_gemini_rejected"):
                    # Stash corporate/C-suite rejects as fallback. At small
                    # independent brands these are often the only real contacts.
                    reason = (c.get("_gemini_rejection_reason") or "").lower()
                    is_corporate_reject = any(
                        kw in reason
                        for kw in (
                            "c-suite",
                            "corporate",
                            "regional",
                            "vp",
                            "vice president",
                            "ceo",
                            "coo",
                            "cfo",
                            "president",
                            "founder",
                            "chairman",
                        )
                    )
                    if is_corporate_reject and c.get("name"):
                        c["_fallback_reason"] = (
                            f"gemini_corporate: {c.get('_gemini_rejection_reason')}"
                        )
                        result.fallback_contacts.append(c)
        except Exception as e:
            result.errors.append(f"Gemini verification failed: {str(e)}")
            logger.error(f"Gemini verification error: {e}")

    # ══════════════════════════════════════════════════════════
    # CONTACT VALIDATION — SAP-trained scoring + false positive filter
    # ══════════════════════════════════════════════════════════

    if result.contacts:
        scored_contacts = contact_validator.validate_and_score(
            contacts=result.contacts,
            hotel_name=hotel_name,
            brand=brand,
            management_company=management_company or result.management_company,
            city=city,
            state=state,
            country=country,
        )

        # Check if we should retry (all name collisions / no decision makers)
        should_retry, retry_reason = contact_validator.should_retry_search(
            scored_contacts
        )
        if should_retry:
            logger.info(f"Validation says retry: {retry_reason}")

            # ── AUTO-RETRY with different queries ──
            retry_result_contacts_before = len(result.contacts)
            try:
                await _layer_web_search(
                    hotel_name,
                    brand,
                    management_company,
                    city,
                    state,
                    country,
                    opening_date,
                    result,
                    retry_attempt=1,
                    phase=pt.starting_phase,
                    project_type=pt.project_type,
                )
                # Re-validate with new contacts included
                if len(result.contacts) > retry_result_contacts_before:
                    scored_contacts = contact_validator.validate_and_score(
                        contacts=result.contacts,
                        hotel_name=hotel_name,
                        brand=brand,
                        management_company=management_company
                        or result.management_company,
                        city=city,
                        state=state,
                        country=country,
                    )
            except Exception as e:
                logger.warning(f"Retry search failed: {e}")

        # Apply brand-specific score multiplier from BrandRegistry
        # e.g. independent/collection brands score higher (more opportunity)
        #      Avendra-constrained brands score slightly lower
        brand_multiplier = BrandRegistry.get_contact_score_multiplier(brand or "")

        # FIX: For renovations/rebrands, property-level contacts gain expanded
        # vendor authority (emergency reorders, post-closure procurement).
        # The brand registry's static "brand_managed → 0.75x" penalty understates
        # opportunity in these scenarios. Bump multiplier toward 1.0 (or above)
        # for these project types so property-found contacts aren't suppressed.
        if pt.project_type in ("rebrand", "renovation") and brand_multiplier < 1.0:
            adjusted_multiplier = min(1.1, brand_multiplier + 0.25)
            logger.info(
                f"Project type {pt.project_type}: bumping brand multiplier "
                f"{brand_multiplier}x -> {adjusted_multiplier}x "
                f"(property GMs gain vendor authority during reopenings)"
            )
            brand_multiplier = adjusted_multiplier

        if brand_multiplier != 1.0:
            for sc in scored_contacts:
                sc.total_score = int(sc.total_score * brand_multiplier)
            logger.debug(f"Brand '{brand}' score multiplier: {brand_multiplier}x")

        # Log brand procurement intelligence
        if brand:
            brand_info = BrandRegistry.lookup(brand)
            logger.info(
                f"Brand intel: {brand} | model={brand_info.operating_model} | "
                f"procurement={brand_info.procurement_model} | "
                f"opportunity={brand_info.opportunity_level} | "
                f"uniform_freedom={brand_info.uniform_freedom}"
            )

        # Filter and rank — keep only good contacts
        good_contacts = contact_validator.filter_and_rank(
            scored_contacts,
            min_score=5,
            max_contacts=MAX_CONTACTS_TO_SAVE,
        )

        # HR contacts are kept — Director of HR handles uniform onboarding
        # Replace raw contacts with validated ones, preserving extra metadata
        validated_contacts = []
        for sc in good_contacts:
            c = sc.contact.copy()
            c["_validation_score"] = sc.total_score
            c["_buyer_tier"] = sc.title_tier.name if sc.title_tier else "UNKNOWN"
            c["_validation_confidence"] = sc.confidence
            c["_validation_scope"] = sc.scope_tag
            c["_validation_reason"] = sc.reason
            validated_contacts.append(c)

        result.contacts = validated_contacts
        logger.info(
            f"Validation: {len(validated_contacts)} contacts passed "
            f"(from {len(scored_contacts)} raw)"
        )

    # ── Final deduplicate by name ──
    seen = set()
    unique = []
    for c in result.contacts:
        key = c.get("name", "").lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(c)
    result.contacts = unique

    # ── Fill missing LinkedIn URLs via quick search ──
    # For each discovered contact, search Google to find their specific
    # LinkedIn profile URL. Uses the full name + operator context.
    #
    # FIX: use parent_company (e.g. "Hyatt Inclusive Collection") over bare
    # brand (e.g. "Dreams") because corporate execs list the PARENT on
    # LinkedIn, not the individual brand. Also quote the name to force an
    # exact-match lookup — unquoted names match unrelated people.
    try:
        brand_info = BrandRegistry.lookup(brand) if brand else None
    except Exception:
        brand_info = None

    # Preferred operator context for LinkedIn search, in priority order:
    #   1. parent_company from registry (e.g. "Hyatt Inclusive Collection")
    #   2. the lead's management_company
    #   3. fall back to the bare brand
    operator_context = ""
    if brand_info and brand_info.parent_company:
        operator_context = brand_info.parent_company.split("(")[0].strip()
    elif management_company:
        operator_context = management_company
    elif brand:
        operator_context = brand

    for c in result.contacts:
        if not c.get("linkedin") and c.get("name"):
            try:
                name = c["name"]

                # ── DISTINCTIVE TOKEN BUILDER (priority-based) ──
                # What token sources matter depends on WHO we're searching
                # for. A hotel GM lists the PROPERTY on LinkedIn. A VP at
                # the operator lists the OPERATOR (Crescent Hotels). Only
                # a Marriott HQ employee lists the CHAIN. Nobody on earth
                # lists "Autograph Collection" as an employer — that's a
                # marketing umbrella, not a company.
                _LINKEDIN_STOPWORDS = {
                    # Short words / articles
                    "the",
                    "and",
                    "of",
                    "for",
                    "at",
                    "a",
                    "an",
                    "&",
                    "in",
                    "on",
                    "to",
                    "by",
                    "or",
                    # Generic hospitality category words
                    "hotel",
                    "hotels",
                    "resort",
                    "resorts",
                    "spa",
                    "spas",
                    "inn",
                    "inns",
                    "suites",
                    "suite",
                    "lodge",
                    "club",
                    "property",
                    "properties",
                    "lodging",
                    "hospitality",
                    # Soft-brand / collection labels — marketing umbrellas,
                    # NOT employers. Must be filtered out or we end up
                    # searching for the wrong associations on LinkedIn.
                    "collection",
                    "collections",
                    "autograph",
                    "curio",
                    "tribute",
                    "unbound",
                    "luxury",
                    "tapestry",
                    "mgallery",
                    "vignette",
                    "destination",
                    "editions",
                    "edition",
                    # Generic corporate suffixes
                    "group",
                    "groups",
                    "company",
                    "companies",
                    "corp",
                    "corporation",
                    "inc",
                    "llc",
                    "ltd",
                    "plc",
                    "international",
                    "global",
                    "americas",
                    "worldwide",
                    "management",
                    "services",
                    # Generic hotel-name descriptors
                    "rooftop",
                    "tower",
                    "towers",
                    "plaza",
                    "palace",
                    "grand",
                    "downtown",
                    "boutique",
                    "premium",
                }

                def _extract_tokens(sources):
                    out = set()
                    for s in sources:
                        for word in re.split(r"[^a-z0-9]+", (s or "").lower()):
                            if len(word) >= 3 and word not in _LINKEDIN_STOPWORDS:
                                out.add(word)
                    return out

                # Detect contact scope — this drives which tokens we use.
                scope = (
                    c.get("scope", "") or c.get("_validation_scope", "") or ""
                ).lower()

                # TIER 1 (property / operator) — for hotel-specific or
                # chain-area contacts, i.e. people who actually work at
                # the property or for the operator of the property.
                tier1 = _extract_tokens([hotel_name, management_company])

                # TIER 2 (chain parent) — only used when the contact is
                # flagged as corporate/HQ-level (scope=chain_corporate),
                # because those folks list the parent chain on LinkedIn.
                # For everyone else, the chain name is NOISE: a Crescent
                # VP doesn't have "Marriott" in their Experience section.
                tier2 = set()
                if scope == "chain_corporate":
                    tier2 = _extract_tokens([operator_context])

                # Fallback: if tier 1 is empty (no hotel name, no mgmt co)
                # AND this isn't a corporate contact, lean on whatever we
                # have — org field and operator context. Still filtered
                # against the stopword list so "Autograph" etc. are out.
                fallback = set()
                if not tier1 and not tier2:
                    fallback = _extract_tokens(
                        [c.get("organization"), operator_context]
                    )

                distinctive = tier1 | tier2 | fallback

                # No distinctive tokens = no way to verify a match — skip.
                # Attaching a blind URL here is what produced the
                # Michael Metcalf -> CS student bug.
                if not distinctive:
                    logger.debug(
                        f"LinkedIn lookup skipped for {name}: "
                        f"no distinctive tokens (scope={scope!r})"
                    )
                    continue

                # ── Targeted query: site:linkedin.com/in + name + tokens.
                #    Google indexes the Experience section of public
                #    LinkedIn profiles. Putting tokens INSIDE the search
                #    query forces Google to return only profiles whose
                #    indexed content (headline, summary, OR Experience
                #    entries) contains at least one of our tokens.
                #    Catches legit execs with generic headlines; rejects
                #    namesakes with zero association to the property.
                tokens_for_query = sorted(distinctive)[:3]
                tokens_clause = " OR ".join(f'"{t}"' for t in tokens_for_query)
                li_query = f'"{name}" ({tokens_clause}) site:linkedin.com/in'
                li_results = await _search_web(li_query, max_results=3)

                attached = False
                iter_query_tokens = [t.strip('"') for t in tokens_clause.split(" OR ")]
                for r in li_results:
                    r_url = r.get("url", "")
                    if "linkedin.com/in/" not in r_url:
                        continue
                    valid, reason = _linkedin_serp_valid(
                        name=name,
                        serp_result=r,
                        query_tokens=iter_query_tokens,
                        hotel_name=hotel_name,
                        management_company=management_company or "",
                        developer="",
                        owner="",
                        contact_title=c.get("title") or "",
                    )
                    if not valid:
                        logger.debug(f"LinkedIn rejected for {name}: {reason}")
                        continue
                    c["linkedin"] = _canonicalize_linkedin_url(r_url)
                    logger.info(
                        f"LinkedIn URL found for {name}: "
                        f"{c['linkedin']} ({reason}, scope={scope})"
                    )
                    attached = True
                    break

                if not attached:
                    logger.info(
                        f"No verifiable LinkedIn URL for {name} "
                        f"(query tokens: {tokens_for_query}, scope={scope}) "
                        f"— leaving null rather than attach wrong profile"
                    )
            except Exception as e:
                logger.debug(f"LinkedIn URL lookup failed for {c.get('name')}: {e}")

    # ── Sort: hotel_specific > chain_area > unknown, then by validation score ──
    scope_rank = {
        "hotel_specific": 0,
        "chain_area": 1,
        "management_corporate": 2,
        "chain_corporate": 3,
        "owner": 2,
        "unknown": 4,
    }
    # ── LAST-RESORT FALLBACK ──
    # If all real contacts got filtered out but we have stashed corporate/C-suite
    # contacts from the press releases, rescue the best one. At small independent
    # brands (Trailborn, Mosaic-type collections, founder-led startups) the COO or
    # Co-CEO is often the actual operational decision-maker for uniform/supply buys.
    if not result.contacts and result.fallback_contacts:
        # Dedupe by name (same person can appear in multiple articles)
        seen = set()
        unique_fallbacks = []
        for c in result.fallback_contacts:
            key = (
                c.get("name", "").lower().strip(),
                c.get("title", "").lower().strip(),
            )
            if key not in seen and key[0]:
                seen.add(key)
                unique_fallbacks.append(c)

        if unique_fallbacks:
            # Pick the most-mentioned fallback (most article appearances = most
            # prominent person associated with the brand) — otherwise first.
            name_counts = {}
            for c in result.fallback_contacts:
                n = c.get("name", "").lower().strip()
                name_counts[n] = name_counts.get(n, 0) + 1
            unique_fallbacks.sort(
                key=lambda c: -name_counts.get(c.get("name", "").lower().strip(), 0)
            )
            best = unique_fallbacks[0]
            best["scope"] = "chain_corporate"
            best["confidence"] = "low"
            best["_is_fallback"] = True
            logger.info(
                f"FALLBACK PROMOTED: {best.get('name')} ({best.get('title')}) — "
                f"zero property-level contacts found, using most-mentioned "
                f"corporate contact. Reason: {best.get('_fallback_reason')}"
            )
            result.contacts.append(best)

    result.contacts.sort(
        key=lambda c: (
            scope_rank.get(c.get("scope", c.get("_validation_scope", "unknown")), 3),
            -(c.get("_validation_score", 0)),
        )
    )

    # ── TOP 5 ONLY ──
    result.contacts = result.contacts[:MAX_CONTACTS_TO_SAVE]

    logger.info(
        f"Enrichment v4 complete for {hotel_name}: "
        f"{len(result.contacts)} contacts, "
        f"layers: {result.layers_tried}"
    )
    return result


# ═══════════════════════════════════════════════════════════════
# SAVE TO DATABASE — v4 with tier scoring in notes
# ═══════════════════════════════════════════════════════════════


async def save_enrichment_to_lead(lead_id: int, result: EnrichmentResult) -> dict:
    """Save enrichment results. REPLACES old enrichment notes (no duplicates)."""
    from app.database import async_session
    from sqlalchemy import select
    from app.models.potential_lead import PotentialLead

    best = result.best_contact
    if not best and not result.management_company and not result.developer:
        return {"status": "no_data", "message": "No contacts or details found"}

    async with async_session() as session:
        db_result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = db_result.scalar_one_or_none()
        if not lead:
            return {"status": "error", "message": f"Lead {lead_id} not found"}

        updated_fields = []

        if best:
            lead.contact_name = best.get("name")
            updated_fields.append("contact_name")

            lead.contact_title = best.get("title")
            updated_fields.append("contact_title")

            if best.get("email"):
                lead.contact_email = best["email"]
                updated_fields.append("contact_email")

            # NOTE: contact_linkedin column does not exist on PotentialLead.
            # LinkedIn is stored on the LeadContact record only.

            if best.get("phone"):
                lead.contact_phone = best["phone"]
                updated_fields.append("contact_phone")

        if result.management_company:
            lead.management_company = result.management_company
            updated_fields.append("management_company")
        if result.developer:
            lead.developer = result.developer
            updated_fields.append("developer")

        # ── Build notes: REPLACE old enrichment section with tier-scored format ──
        if result.contacts:
            existing_notes = lead.notes or ""
            enrichment_marker = "--- Enrichment ("
            if enrichment_marker in existing_notes:
                idx = existing_notes.index(enrichment_marker)
                existing_notes = existing_notes[:idx].rstrip()

            lines = []
            if existing_notes:
                lines.append(existing_notes)

            lines.append(
                f"\n--- Enrichment ({local_now().strftime('%b %d, %Y')}) "
                f"— Top {len(result.contacts)} contacts ---"
            )

            for i, c in enumerate(result.contacts, 1):
                # Tier emoji from SAP classifier
                tier_name = c.get("_buyer_tier", "UNKNOWN")
                tier_emoji = {
                    "TIER1_UNIFORM_DIRECT": "\U0001f3c6",  # 🏆
                    "TIER2_PURCHASING": "\U0001f4b0",  # 💰
                    "TIER3_GM_OPS": "\U0001f3e8",  # 🏨
                    "TIER4_FB": "\U0001f37d\ufe0f",  # 🍽️
                    "TIER5_HR": "\U0001f465",  # 👥
                }.get(tier_name, "\u2753")

                v_conf = c.get("_validation_confidence", c.get("confidence", "low"))
                confidence_icon = {
                    "high": "\U0001f7e2",  # 🟢
                    "medium": "\U0001f7e1",  # 🟡
                    "low": "\U0001f534",  # 🔴
                }.get(v_conf, "\U0001f534")

                scope_label = c.get("scope", "unknown").replace("_", " ").title()
                conf_label = v_conf.title()
                v_score = c.get("_validation_score", "?")

                line = f"\n{i}. {tier_emoji} {confidence_icon} {c['name']}"
                line += f"\n   Title: {c.get('title', 'N/A')}"
                line += f"\n   Tier: {tier_name} | Score: {v_score}"
                if c.get("email"):
                    line += f"\n   Email: {c['email']}"
                if c.get("linkedin"):
                    line += f"\n   LinkedIn: {c['linkedin']}"
                if c.get("phone"):
                    line += f"\n   Phone: {c['phone']}"
                if c.get("organization"):
                    line += f"\n   Org: {c['organization']}"
                line += f"\n   [{scope_label} | {conf_label}] {c.get('confidence_note', c.get('_validation_reason', ''))}"

                lines.append(line)

            lead.notes = "\n".join(lines)
            updated_fields.append("notes")

        if result.additional_details and not lead.description:
            lead.description = result.additional_details
            updated_fields.append("description")

        lead.updated_at = local_now()
        await session.commit()

        return {
            "status": "success",
            "updated_fields": updated_fields,
            "contacts_found": len(result.contacts),
            "best_contact": best,
        }


# ═══════════════════════════════════════════════════════════════════════════
# PERSIST HELPER — saves enrichment to BOTH flat fields AND lead_contacts table
# ───────────────────────────────────────────────────────────────────────────
# Why this exists:
#   - save_enrichment_to_lead() (above) only updates potential_leads.contact_*
#     and the notes blob. It never populates the lead_contacts table.
#   - The dashboard's /enrich button (routes/contacts.py) had this logic
#     inlined. The auto_enrich Celery task did NOT — so auto-enriched leads
#     showed an empty contacts panel in the dashboard.
#   - This helper centralizes the lead_contacts persistence so any caller
#     (dashboard route, autonomous task, future CLI) gets identical behavior.
#
# Behavior:
#   - flat fields on potential_leads: fill-empty only (never overwrite)
#   - lead_contacts table: MERGE on normalized name
#       * existing contact found → fill empty fields only (respects user pins)
#       * new name → insert as LeadContact
#   - First contact in the result list is marked is_primary=True on insert
#   - Caller MUST commit the session (this helper does not commit)
#   - Caller may want to call rescore_lead() afterwards
# ═══════════════════════════════════════════════════════════════════════════


async def persist_enrichment_contacts(
    lead_id: int = None,
    enrichment_result: "EnrichmentResult" = None,
    session=None,
    *,
    existing_hotel_id: int = None,
) -> dict:
    """
    Persist enrichment results into the database.

    Supports BOTH parent kinds (migration 018, 2026-04-27):
      - lead_id: write to potential_leads + lead_contacts.lead_id
      - existing_hotel_id: write to existing_hotels + lead_contacts.existing_hotel_id

    Pass exactly one of (lead_id, existing_hotel_id), never both, never
    neither. Caller's responsibility — enforced by ValueError below.

    Updates:
      1. parent row: contact_* / management_company / developer / owner
         (fill-empty only — never overwrites populated fields)
      2. lead_contacts table (MERGE on normalized name) — sets the correct
         FK based on parent kind. Same CHECK constraint in DB ensures
         exactly one of (lead_id, existing_hotel_id) is set per row.

    Args:
        lead_id: PotentialLead.id to persist into. Mutually exclusive with
                 existing_hotel_id.
        enrichment_result: EnrichmentResult from enrich_lead_contacts()
        session: open AsyncSession (caller manages transaction + commit)
        existing_hotel_id: ExistingHotel.id to persist into (kw-only).
                           Mutually exclusive with lead_id.

    Returns:
        dict with: status, contacts_added, contacts_updated, flat_fields_updated, lead_not_found
    """
    from sqlalchemy import select
    from app.models.potential_lead import PotentialLead
    from app.models.existing_hotel import ExistingHotel
    from app.models.lead_contact import LeadContact
    from app.services.utils import normalize_hotel_name

    # ── Validate parent ──
    # Exactly one of lead_id / existing_hotel_id must be set. We validate
    # in code AND the DB has a CHECK constraint as last-line defense.
    if (lead_id is None) == (existing_hotel_id is None):
        raise ValueError(
            "persist_enrichment_contacts requires exactly one of lead_id or "
            f"existing_hotel_id. Got lead_id={lead_id}, "
            f"existing_hotel_id={existing_hotel_id}."
        )

    # Normalize parent identifier — `parent_id` and `parent_kind` drive
    # the rest of this function. parent_kind is what we pass to log lines
    # and what determines which FK gets set on new LeadContact rows.
    if lead_id is not None:
        parent_id = lead_id
        parent_kind = "lead"
    else:
        parent_id = existing_hotel_id
        parent_kind = "hotel"

    summary = {
        "status": "no_data",
        "contacts_added": 0,
        "contacts_updated": 0,
        "flat_fields_updated": [],
    }

    # ── Load parent (potential_lead OR existing_hotel) ──
    if parent_kind == "lead":
        parent_result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == parent_id)
        )
    else:
        parent_result = await session.execute(
            select(ExistingHotel).where(ExistingHotel.id == parent_id)
        )
    lead = parent_result.scalar_one_or_none()
    if not lead:
        summary["status"] = "lead_not_found"
        logger.warning(
            f"persist_enrichment_contacts: {parent_kind} {parent_id} not found"
        )
        return summary

    # ── Update flat lead fields (fill-empty only) ──
    if enrichment_result.management_company and not lead.management_company:
        lead.management_company = enrichment_result.management_company
        summary["flat_fields_updated"].append("management_company")
    if enrichment_result.developer and not lead.developer:
        lead.developer = enrichment_result.developer
        summary["flat_fields_updated"].append("developer")
    if getattr(enrichment_result, "owner", None) and not lead.owner:
        lead.owner = enrichment_result.owner
        summary["flat_fields_updated"].append("owner")

    if enrichment_result.best_contact:
        bc = enrichment_result.best_contact
        if bc.get("name") and not lead.contact_name:
            lead.contact_name = bc["name"]
            summary["flat_fields_updated"].append("contact_name")
        if bc.get("title") and not lead.contact_title:
            lead.contact_title = bc["title"]
            summary["flat_fields_updated"].append("contact_title")
        if bc.get("email") and not lead.contact_email:
            lead.contact_email = bc["email"]
            summary["flat_fields_updated"].append("contact_email")
        if bc.get("phone") and not lead.contact_phone:
            lead.contact_phone = bc["phone"]
            summary["flat_fields_updated"].append("contact_phone")

    lead.updated_at = local_now()

    # ── Persist contacts to lead_contacts table (MERGE) ──
    if enrichment_result.contacts:
        # Filter contacts by whichever parent FK is set (dual-FK schema —
        # see migration 018 + lead_contact.py CHECK constraint).
        if parent_kind == "lead":
            existing_filter = LeadContact.lead_id == parent_id
        else:
            existing_filter = LeadContact.existing_hotel_id == parent_id
        existing_result = await session.execute(
            select(LeadContact).where(existing_filter)
        )
        existing_by_norm: dict = {
            normalize_hotel_name(c.name): c for c in existing_result.scalars().all()
        }

        for i, c in enumerate(enrichment_result.contacts):
            name = (c.get("name") or "").strip()
            if not name:
                continue

            normalized = normalize_hotel_name(name)
            existing = existing_by_norm.get(normalized)

            if existing:
                # Fill empty fields only — respects user-pinned contacts
                filled = []
                if not existing.email and c.get("email"):
                    existing.email = c["email"]
                    filled.append("email")
                if not existing.phone and c.get("phone"):
                    existing.phone = c["phone"]
                    filled.append("phone")
                if not existing.linkedin and c.get("linkedin"):
                    existing.linkedin = _canonicalize_linkedin_url(c["linkedin"])
                    filled.append("linkedin")
                if not existing.title and c.get("title"):
                    existing.title = c["title"]
                    filled.append("title")
                if not existing.organization and c.get("organization"):
                    existing.organization = c["organization"]
                    filled.append("organization")
                if not existing.evidence_url and c.get("source"):
                    existing.evidence_url = c["source"]
                    filled.append("evidence_url")
                # Strategist verdict is always refreshed (not fill-empty) —
                # each enrichment should carry the latest strategic assessment
                if c.get("_final_priority"):
                    if existing.strategist_priority != c["_final_priority"]:
                        filled.append("strategist_priority")
                    existing.strategist_priority = c["_final_priority"]
                if c.get("_final_reasoning"):
                    existing.strategist_reasoning = c["_final_reasoning"]
                # ── Always refresh classification fields on re-enrichment
                #    (bug fix 2026-04-22). Previously these stayed frozen
                #    from the first insert — so a contact scored as P1/5
                #    on day 1 would keep score=5 forever, even after the
                #    pipeline ranked them P1/28 on day N. The routes path
                #    got this fix already; the Celery auto_enrich path
                #    (which calls persist_enrichment_contacts) kept the
                #    bug until now.
                new_score = c.get("_validation_score")
                if new_score is not None and new_score != existing.score:
                    filled.append(f"score({existing.score}->{new_score})")
                    existing.score = new_score
                new_tier = c.get("_buyer_tier")
                if new_tier and new_tier != existing.tier:
                    filled.append("tier")
                    existing.tier = new_tier
                new_confidence = c.get("_validation_confidence") or c.get("confidence")
                if new_confidence and new_confidence != existing.confidence:
                    filled.append("confidence")
                    existing.confidence = new_confidence
                # Scope may shift if Iter 6 or verifier reclassified
                # (e.g. chain_corporate -> management_corporate, or
                # chain_area -> owner)
                new_scope = c.get("scope")
                if new_scope and new_scope != existing.scope:
                    filled.append(f"scope({existing.scope}->{new_scope})")
                    existing.scope = new_scope
                # Always refresh score_breakdown so the "why this score?"
                # UI stays in sync with the current scoring logic
                new_breakdown = c.get("_score_breakdown")
                if new_breakdown:
                    existing.score_breakdown = new_breakdown
                    if "score_breakdown" not in filled:
                        filled.append("score_breakdown")
                # Merge evidence (new items) — don't blow away existing
                # evidence on re-enrichment, but DO add any new items we
                # captured this run. Dedupe by source_url.
                new_evidence = c.get("_evidence_items") or []
                if new_evidence:
                    existing_evidence = existing.evidence or []
                    existing_urls = {
                        e.get("source_url")
                        for e in existing_evidence
                        if isinstance(e, dict)
                    }
                    added = 0
                    for ev in new_evidence:
                        if ev.get("source_url") not in existing_urls:
                            existing_evidence.append(ev)
                            existing_urls.add(ev.get("source_url"))
                            added += 1
                    if added:
                        # Re-sort by trust tier (highest first), then year
                        try:
                            from app.services.source_tier import trust_score as _ts

                            existing_evidence.sort(
                                key=lambda e: (
                                    -_ts(e.get("trust_tier", "unknown")),
                                    -(e.get("source_year") or 0),
                                )
                            )
                        except Exception:
                            pass
                        # Cap to top 8 evidence items per contact
                        existing.evidence = existing_evidence[:8]
                        filled.append(f"evidence(+{added})")
                # source_detail gets refreshed too when new rich evidence arrives
                new_detail = c.get("source_detail")
                if new_detail and new_detail != existing.source_detail:
                    existing.source_detail = new_detail
                    filled.append("source_detail")
                if filled:
                    existing.last_enriched_at = local_now()
                    summary["contacts_updated"] += 1
                    logger.info(
                        f"persist_enrichment_contacts: {parent_kind} {parent_id}: "
                        f"updated '{existing.name}' (filled {', '.join(filled)})"
                    )
            else:
                # Insert new contact. Set ONE of (lead_id, existing_hotel_id)
                # based on parent kind — DB CHECK constraint enforces exactly
                # one is non-NULL.
                contact = LeadContact(
                    lead_id=parent_id if parent_kind == "lead" else None,
                    existing_hotel_id=parent_id if parent_kind == "hotel" else None,
                    name=name,
                    title=c.get("title"),
                    email=c.get("email"),
                    phone=c.get("phone"),
                    linkedin=_canonicalize_linkedin_url(c.get("linkedin")),
                    organization=c.get("organization"),
                    scope=c.get("scope", "unknown"),
                    confidence=c.get(
                        "_validation_confidence", c.get("confidence", "medium")
                    ),
                    tier=c.get("_buyer_tier"),
                    score=c.get("_validation_score", 0),
                    score_breakdown=c.get(
                        "_score_breakdown"
                    ),  # Unified scoring breakdown
                    evidence=c.get("_evidence_items")
                    or None,  # Evidence array from snippet extraction
                    # Iter 6 strategist verdict — authoritative priority + reasoning
                    strategist_priority=c.get("_final_priority"),
                    strategist_reasoning=c.get("_final_reasoning"),
                    is_primary=(i == 0),
                    found_via=", ".join(enrichment_result.layers_tried)
                    if enrichment_result.layers_tried
                    else "web_search",
                    source_detail=c.get(
                        "source_detail",  # Rich evidence from Iter 5 verification
                        c.get("confidence_note", c.get("_validation_reason", "")),
                    ),
                    evidence_url=c.get("source"),
                    last_enriched_at=local_now(),
                )
                session.add(contact)
                summary["contacts_added"] += 1
                logger.info(
                    f"persist_enrichment_contacts: {parent_kind} {parent_id}: "
                    f"added '{name}' [{c.get('scope', 'unknown')}]"
                )

    if (
        summary["contacts_added"]
        or summary["contacts_updated"]
        or summary["flat_fields_updated"]
    ):
        summary["status"] = "saved"

    return summary
