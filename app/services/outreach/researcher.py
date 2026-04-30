"""Agent 1 — Researcher (v2).

What changed from PitchIQ v1:
  - Uses SLH's already-known context (brand_tier, project_type,
    timeline_label, room_count, etc.) instead of asking Gemini to
    re-derive things SLH already knows
  - Tailors search queries by project_type — a Renovation lead gets
    queries about renovation completion + rebrand, while a New Build
    gets pre-opening hiring + opening date queries
  - JSON mode (response_mime_type) for guaranteed parseable output
  - Returns 3 conversation_hooks (alternates) instead of just 1, so
    the Writer / rep have variety
  - Handles list-of-blocks responses from langchain-google-vertexai 3.x

Cost per call: ~$0.10 (6 Serper + 1 website scrape + 1 Gemini synthesis).
"""

from __future__ import annotations

import concurrent.futures
import logging

import httpx
from bs4 import BeautifulSoup

from .state import PitchState
from .config import get_llm, SERPER_API_KEY
from ._helpers import (
    JA_BACKGROUND,
    fmt_known_context,
    invoke_json,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Project-type-aware query templates
# ─────────────────────────────────────────────────────────────────────────────


def _build_search_queries(state: dict) -> dict[str, str]:
    """6 Serper queries, tailored by project_type when known."""
    hotel = state.get("hotel_name", "")
    location = state.get("hotel_location", "")
    contact_name = state.get("contact_name", "")
    contact_title = state.get("contact_title", "")
    project_type = (state.get("project_type") or "").lower()

    # Always-on queries
    queries = {
        "contact": f"{contact_name} {contact_title} {hotel}",
        "news": f"{hotel} news 2026 {location}".strip(),
        "awards": f"{hotel} awards recognition 2025 2026",
    }

    # Tailor remaining 3 by project_type
    if project_type == "new_opening":
        queries["pre_opening"] = f"{hotel} opening date pre-opening hiring 2026"
        queries["expansion"] = f"{hotel} construction completion grand opening"
        queries["staffing"] = f"{hotel} hiring staff team 2026 jobs"
    elif project_type == "renovation":
        queries["pre_opening"] = f"{hotel} renovation completion reopening 2026"
        queries["expansion"] = f"{hotel} renovation update construction milestone"
        queries["staffing"] = f"{hotel} renovation rooms refresh"
    elif project_type == "rebrand":
        queries["pre_opening"] = f"{hotel} rebrand new flag conversion 2026"
        queries["expansion"] = f"{hotel} brand conversion management change"
        queries["staffing"] = f"{hotel} rebrand staff transition uniforms"
    elif project_type == "reopening":
        queries["pre_opening"] = f"{hotel} reopening date 2026 return"
        queries["expansion"] = f"{hotel} reopening renovation investment"
        queries["staffing"] = f"{hotel} hiring staff reopening"
    elif project_type == "ownership_change":
        queries["pre_opening"] = f"{hotel} new owner acquisition 2026"
        queries["expansion"] = f"{hotel} ownership change repositioning"
        queries["staffing"] = f"{hotel} new management team"
    else:
        # Generic / unknown → original PitchIQ defaults
        queries["pre_opening"] = f"{hotel} expansion renovation rebranding 2026"
        queries["expansion"] = f"{hotel} expansion development pipeline"
        queries["staffing"] = f"{hotel} hiring staff 2026 jobs"

    return queries


# ─────────────────────────────────────────────────────────────────────────────
# Search engines
# ─────────────────────────────────────────────────────────────────────────────


def serper_search(query: str) -> list[str]:
    if not SERPER_API_KEY:
        return []
    try:
        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
        payload = {"q": query, "num": 5}
        response = httpx.post(url, headers=headers, json=payload, timeout=10)
        data = response.json()
        results = []
        items = data.get("organic", []) or data.get("news", [])
        for item in items[:5]:
            snippet = item.get("snippet", "")
            title = item.get("title", "")
            link = item.get("link", "")
            if snippet:
                results.append(f"{title}: {snippet} ({link})")
        return results
    except Exception as e:
        logger.warning(f"Serper search failed for {query!r}: {e}")
        return []


def ddg_search(query: str) -> list[str]:
    try:
        from duckduckgo_search import DDGS

        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=5):
                results.append(f"{r['title']}: {r['body']} ({r['href']})")
        return results
    except Exception as e:
        logger.warning(f"DDG search failed for {query!r}: {e}")
        return []


def smart_search(query: str) -> list[str]:
    results = serper_search(query)
    if not results:
        results = ddg_search(query)
    return results


def run_all_searches(queries: dict[str, str]) -> dict[str, list[str]]:
    """Fire all queries in parallel."""
    results: dict[str, list[str]] = {}

    def fetch(key: str, q: str):
        results[key] = smart_search(q)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch, k, q) for k, q in queries.items()]
        concurrent.futures.wait(futures, timeout=20)

    return results


def extract_best_url(search_results: list[str], hotel_name: str) -> str:
    if not search_results or not hotel_name:
        return ""
    hotel_word = hotel_name.lower().split()[0]
    for result in search_results:
        url_start = result.rfind("(") + 1
        url_end = result.rfind(")")
        if url_start > 0 and url_end > url_start:
            url = result[url_start:url_end]
            if any(
                word in url.lower()
                for word in [
                    "hotel",
                    hotel_word,
                    "resort",
                    "marriott",
                    "hilton",
                    "hyatt",
                ]
            ):
                return url
    return ""


def scrape_hotel_website(url: str) -> str:
    if not url:
        return "No website found"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = httpx.get(url, headers=headers, timeout=10, verify=False)
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        text = " ".join(text.split())
        if len(text) > 300:
            return text[:4000]
    except Exception as e:
        logger.warning(f"httpx scrape failed for {url}: {e}")
    return "Could not scrape website"


# ─────────────────────────────────────────────────────────────────────────────
# Gemini synthesis
# ─────────────────────────────────────────────────────────────────────────────


_DEFAULT_SYNTHESIS = {
    "hotel_summary": "",
    "hotel_tier_inferred": "",
    "recent_news": [],
    "hiring_signals": [],
    "expansion_signals": [],
    "awards": [],
    "pain_points": [],
    "signals": [],
    "contact_summary": "",
    "outreach_angle": "",
    "personalization_hook": "",
    "conversation_hooks": [],
    "hotel_intel": {},
    "contact_intel": {},
}


def synthesize(state: dict, search_results: dict, website_text: str) -> dict:
    """Single Gemini call that synthesizes ALL research into structured JSON."""

    contact_name = state.get("contact_name", "")
    contact_title = state.get("contact_title", "")
    hotel_name = state.get("hotel_name", "")

    # Format search blocks
    blocks = []
    for key, items in search_results.items():
        if items:
            label = key.replace("_", " ").upper()
            block = f"=== {label} ===\n" + "\n".join(items)
            blocks.append(block)
    search_block = "\n\n".join(blocks) if blocks else "(no search results)"

    is_client = bool(state.get("is_client"))
    cold_or_warm = "WARM (existing customer)" if is_client else "COLD (prospect)"

    prompt = f"""You are a senior B2B sales intelligence analyst for J.A. Uniforms.

{JA_BACKGROUND}

You're researching a {cold_or_warm} prospect. Your job is to extract
ACTIONABLE outreach intelligence — specific facts a sales rep can
reference in an email opener that proves they did real homework.

═══ ALREADY-KNOWN CONTEXT (don't re-derive these) ═══
Hotel: {hotel_name}
Contact: {contact_name} — {contact_title}
{fmt_known_context(state)}

═══ RAW WEB RESEARCH ═══
{search_block}

═══ WEBSITE CONTENT ═══
{website_text[:2000] if website_text else '(none)'}

═══ YOUR TASK ═══
Return ONLY a JSON object with this exact shape (no markdown, no preamble):

{{
  "hotel_summary": "2-3 sentence summary covering brand, size, segment, and ONE notable recent development",
  "hotel_tier_inferred": "ultra_luxury|luxury|upper_upscale|upscale|midscale|economy",
  "recent_news": ["specific recent news item 1 with year/date", "..."],
  "hiring_signals": ["specific hiring move (role + when)", "..."],
  "expansion_signals": ["renovation/expansion fact with timing", "..."],
  "awards": ["specific award + year", "..."],
  "pain_points": [
    "uniform-specific pain (NOT generic — must tie to staff size, brand consistency, turnover, etc.)",
    "..."
  ],
  "signals": ["buying signal that says 'this hotel needs uniforms now'", "..."],
  "contact_summary": "2-3 sentences about {contact_name}: tenure, decision authority, recent moves, any notable LinkedIn activity",
  "outreach_angle": "the SINGLE best angle for the email opener — one clear hook, not multiple stacked angles",
  "personalization_hook": "one specific FACT (not generic) the email opener can reference verbatim",
  "conversation_hooks": [
    "alternative hook #1 — different angle from personalization_hook",
    "alternative hook #2 — different angle still",
    "alternative hook #3 — different angle still"
  ],
  "hotel_intel": {{
    "current_status": "operating | pre-opening | renovating | reopening",
    "estimated_staff": "rough headcount as integer or null",
    "uniform_relevant_departments": ["F&B", "Housekeeping", "Front Desk", "..."]
  }},
  "contact_intel": {{
    "tenure_estimate": "<1yr | 1-3yr | 3-5yr | 5+yr | unknown",
    "previous_role": "if found",
    "decision_authority": "high | medium | low",
    "linkedin_activity": "active | inactive | unknown",
    "notable_post": "single recent LinkedIn or press quote if found, else empty string"
  }}
}}

Hard rules:
- Every pain_point must be specific to UNIFORM/STAFF presentation/operations.
  Do not write "operational efficiency" — write "managing uniforms across 200+ staff during peak season".
- personalization_hook must be a verbatim-referenceable fact (e.g.,
  "Just announced 2 new properties in Curaçao + Saint Vincent in March 2026").
  Not "expanding their portfolio" — that's generic.
- conversation_hooks must each be a DIFFERENT angle than personalization_hook.
- If a field has no real data, return "" or [] — don't fabricate.
"""

    return invoke_json(get_llm(), prompt, _DEFAULT_SYNTHESIS)


# ─────────────────────────────────────────────────────────────────────────────
# Public agent entry
# ─────────────────────────────────────────────────────────────────────────────


def researcher_agent(state: PitchState) -> PitchState:
    contact_name = state.get("contact_name", "")
    hotel_name = state.get("hotel_name", "")

    logger.info(f"[Researcher] Starting research for {hotel_name} / {contact_name}")

    queries = _build_search_queries(dict(state))
    search_results = run_all_searches(queries)
    found_count = sum(len(v) for v in search_results.values())
    logger.info(
        f"[Researcher] Found {found_count} search results across {len(queries)} queries"
    )

    # Scrape the most relevant website (try news + pre-opening results)
    candidates = (search_results.get("news") or []) + (
        search_results.get("pre_opening") or []
    )
    website_url = extract_best_url(candidates, hotel_name)
    website_text = scrape_hotel_website(website_url) if website_url else ""

    synthesis = synthesize(dict(state), search_results, website_text)

    angle = synthesis.get("outreach_angle") or "(empty)"
    logger.info(f"[Researcher] Synthesis done — angle: {angle}")

    return {
        **state,
        "company_summary": synthesis.get("hotel_summary", ""),
        "contact_summary": synthesis.get("contact_summary", ""),
        "hotel_intel": synthesis.get("hotel_intel") or {},
        "contact_intel": synthesis.get("contact_intel") or {},
        "recent_news": synthesis.get("recent_news") or [],
        "hiring_signals": synthesis.get("hiring_signals") or [],
        "expansion_signals": synthesis.get("expansion_signals") or [],
        "awards": synthesis.get("awards") or [],
        "pain_points": synthesis.get("pain_points") or [],
        "signals": synthesis.get("signals") or [],
        "outreach_angle": synthesis.get("outreach_angle") or "",
        "personalization_hook": synthesis.get("personalization_hook") or "",
        "conversation_hooks": synthesis.get("conversation_hooks") or [],
        "hotel_tier_inferred": synthesis.get("hotel_tier_inferred") or "",
    }
