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
from .config import get_researcher_llm, SERPER_API_KEY
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
# Fallback / deep-research helpers — kick in when initial pass returns
# sparse data, so we don't end up with thin or hallucinated briefs.
# ─────────────────────────────────────────────────────────────────────────────


def _count_useful_chars(search_results: dict[str, list[str]]) -> int:
    """Total characters of meaningful text retrieved across all queries.
    Used to decide if research was 'rich' or 'sparse'."""
    return sum(len(s) for snippets in search_results.values() for s in snippets)


def _count_filled_buckets(search_results: dict[str, list[str]]) -> int:
    """How many of the search categories actually returned anything."""
    return sum(1 for snippets in search_results.values() if snippets)


def _build_fallback_queries(state: dict) -> dict[str, str]:
    """When initial 6 queries return little, try 4 different angles.
    These target different sources (LinkedIn, press, parent brand) to
    avoid just rerunning slight variations of what already failed."""
    contact_name = state.get("contact_name", "")
    hotel_name = state.get("hotel_name", "")
    brand = state.get("brand", "")
    location = state.get("hotel_location", "")
    city = location.split(",")[0].strip() if location else ""

    fallbacks = {}
    if contact_name:
        fallbacks["contact_linkedin"] = f'"{contact_name}" {hotel_name} linkedin'
    if hotel_name:
        fallbacks["press_release"] = (
            f'"{hotel_name}" press release OR announcement 2026'
        )
    if brand and city:
        fallbacks["brand_city"] = f"{brand} {city} new hotel"
    elif brand:
        fallbacks["brand"] = f"{brand} new property opening 2026"
    if contact_name and brand:
        fallbacks["contact_brand"] = f'"{contact_name}" {brand}'
    return fallbacks


def _scrape_multiple_urls(
    search_results: dict[str, list[str]],
    hotel_name: str,
    max_urls: int = 3,
) -> str:
    """Try up to N URLs across different result categories, return the
    longest usable text. Beats single-URL scraping when the first URL
    returns garbage or fails — we just move to the next candidate."""
    if not hotel_name:
        return ""

    # Collect candidate URLs from each category in priority order
    candidates = []
    priority_categories = ["news", "pre_opening", "expansion", "awards"]
    for cat in priority_categories:
        snippets = search_results.get(cat) or []
        for snippet in snippets:
            url_start = snippet.rfind("(") + 1
            url_end = snippet.rfind(")")
            if url_start > 0 and url_end > url_start:
                url = snippet[url_start:url_end]
                if url and url not in candidates:
                    candidates.append(url)
            if len(candidates) >= max_urls * 2:  # gather extras for filtering
                break
        if len(candidates) >= max_urls * 2:
            break

    if not candidates:
        return ""

    # Filter to URLs that look hotel-related
    hotel_word = hotel_name.lower().split()[0] if hotel_name else ""
    relevant = [
        u
        for u in candidates
        if any(
            word in u.lower()
            for word in [
                hotel_word,
                "hotel",
                "resort",
                "marriott",
                "hilton",
                "hyatt",
                "ihg",
                "wyndham",
            ]
        )
    ]
    # If no relevant ones, fall back to top candidates anyway
    to_try = (relevant or candidates)[:max_urls]

    # Scrape each, keep the longest usable result
    best_text = ""
    for url in to_try:
        text = scrape_hotel_website(url)
        if (
            text
            and not text.startswith("Could not")
            and not text.startswith("No website")
        ):
            if len(text) > len(best_text):
                best_text = text
                # 4000 chars is plenty — if we got that, stop trying more
                if len(best_text) >= 3500:
                    break
    return best_text


def _compute_confidence(
    useful_chars: int, filled_buckets: int, website_chars: int
) -> str:
    """Map quantitative signals to a confidence label the rep can see.

    Thresholds chosen empirically:
      - High: enough text to write a substantive brief (5000+ chars total)
        AND most queries returned data
      - Medium: enough to write SOMETHING real but on thin ice
      - Low: brief will likely be generic — rep should manually verify
    """
    total = useful_chars + website_chars
    if total >= 5000 and filled_buckets >= 4:
        return "high"
    if total >= 2500 and filled_buckets >= 3:
        return "medium"
    return "low"


def _extract_sources(search_results: dict[str, list[str]]) -> list[dict]:
    """Pull out every {title, url, category} from the 30+ search snippets.
    Each snippet is formatted by smart_search() as 'Title: snippet (url)'.

    Returns a deduped list of source records the UI shows as clickable
    citation cards. Keeping the original category lets us label sources
    e.g. "Press" vs "LinkedIn" so the rep can tell what kind of evidence
    backs each fact.
    """
    sources = []
    seen_urls = set()
    for category, snippets in search_results.items():
        for snippet in snippets:
            # Format: "Title: body (url)"
            url_start = snippet.rfind("(")
            url_end = snippet.rfind(")")
            if url_start == -1 or url_end <= url_start:
                continue
            url = snippet[url_start + 1 : url_end].strip()
            if not url or not url.startswith(("http://", "https://")):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Title is the part before the first ":"
            head = snippet[:url_start].strip()
            colon = head.find(":")
            title = head[:colon].strip() if colon > 0 else head
            body = head[colon + 1 :].strip() if colon > 0 else ""
            # Trim to readable lengths
            title = (title[:120] + "…") if len(title) > 120 else title
            body = (body[:200] + "…") if len(body) > 200 else body

            sources.append(
                {
                    "url": url,
                    "title": title or url,
                    "snippet": body,
                    "category": category,
                }
            )
    return sources


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
    "fact_citations": [],
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
    "uniform-specific pain referencing only facts found in research",
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
    "estimated_staff": "rough headcount as integer, ONLY if mentioned in research, else null",
    "estimated_staff_source": "the exact phrase from research that supports this number, else empty string",
    "uniform_relevant_departments": ["F&B", "Housekeeping", "Front Desk", "..."]
  }},
  "contact_intel": {{
    "tenure_estimate": "<1yr | 1-3yr | 3-5yr | 5+yr | unknown",
    "previous_role": "if found",
    "decision_authority": "high | medium | low",
    "linkedin_activity": "active | inactive | unknown",
    "notable_post": "single recent LinkedIn or press quote if found, else empty string"
  }},
  "fact_citations": [
    {{ "claim": "any specific number or date you mentioned above", "source_quote": "the exact phrase from the research findings that supports this claim" }},
    "..."
  ]
}}

Hard rules — anti-hallucination protocol:

1. NUMBERS YOU CITE MUST APPEAR IN THE RESEARCH.
   - If research mentions "300 staff", you can write "outfit 300 staff".
   - If research doesn't mention a staff number, write "outfit the team" or
     "across all departments" — never invent one.
   - Same for dates, room counts, budgets, departments — every concrete
     number must trace back to a research snippet you saw above.

2. EVERY CONCRETE NUMBER OR DATE GOES IN fact_citations.
   - For each number/date you mention anywhere (pain_points, value props,
     hooks, etc.), add an entry to fact_citations linking the claim to the
     exact source phrase. If you can't cite it, don't claim it.

3. PAIN POINTS MUST BE UNIFORM-SPECIFIC.
   - Write "managing brand-aligned uniforms across the F&B team" — good.
   - Write "operational efficiency" — bad (too generic).

4. PERSONALIZATION HOOK = VERBATIM FACT.
   - Pull a specific phrase straight from the research findings. The user's
     email will reference this in the opener so it must check out.
   - Bad: "expanding their portfolio"
   - Good: "Just announced 2 new properties in Curaçao + Saint Vincent in March 2026"

5. CONVERSATION HOOKS = DIFFERENT ANGLES.
   - Each one references a different angle from personalization_hook.

6. IF DATA IS MISSING, DO NOT FABRICATE.
   - "" or [] is always better than a guessed value.
   - Empty estimated_staff is better than a hallucinated one.
"""

    return invoke_json(get_researcher_llm(), prompt, _DEFAULT_SYNTHESIS)


# ─────────────────────────────────────────────────────────────────────────────
# Public agent entry
# ─────────────────────────────────────────────────────────────────────────────


def researcher_agent(state: PitchState) -> PitchState:
    contact_name = state.get("contact_name", "")
    hotel_name = state.get("hotel_name", "")

    logger.info(f"[Researcher] Starting research for {hotel_name} / {contact_name}")

    # ── Round 1: initial 6 project-type-aware queries ────────────────
    queries = _build_search_queries(dict(state))
    search_results = run_all_searches(queries)
    found_count = sum(len(v) for v in search_results.values())
    useful_chars = _count_useful_chars(search_results)
    filled_buckets = _count_filled_buckets(search_results)
    logger.info(
        f"[Researcher] Round 1: {found_count} results, "
        f"{useful_chars} chars, {filled_buckets}/{len(queries)} buckets filled"
    )

    # ── Round 2 (conditional): fallback queries when initial was sparse
    # Skip if Round 1 already gave us plenty — saves Serper credits.
    if useful_chars < 3000 or filled_buckets < 4:
        logger.info("[Researcher] Round 1 sparse — running fallback queries")
        fallback_queries = _build_fallback_queries(dict(state))
        if fallback_queries:
            fallback_results = run_all_searches(fallback_queries)
            # Merge into the main results dict so synthesizer sees them
            for k, v in fallback_results.items():
                search_results[k] = v
            new_useful_chars = _count_useful_chars(search_results)
            new_filled_buckets = _count_filled_buckets(search_results)
            recovered = new_useful_chars - useful_chars
            useful_chars = new_useful_chars
            filled_buckets = new_filled_buckets
            logger.info(
                f"[Researcher] Round 2 added {recovered} chars, "
                f"now {filled_buckets} buckets filled"
            )

    # ── Multi-URL scraping (instead of just 1 URL) ───────────────────
    # Try up to 3 different URLs across categories — beats single-URL
    # scraping when first URL fails or returns garbage
    website_text = _scrape_multiple_urls(search_results, hotel_name, max_urls=3)
    if website_text:
        logger.info(f"[Researcher] Scraped {len(website_text)} chars from web")
    else:
        logger.info("[Researcher] No usable website content")

    # ── Compute research confidence — surfaced in UI so rep knows ────
    # if they should fact-check the brief manually before sending
    confidence = _compute_confidence(
        useful_chars=useful_chars,
        filled_buckets=filled_buckets,
        website_chars=len(website_text or ""),
    )
    logger.info(f"[Researcher] Confidence: {confidence}")

    synthesis = synthesize(dict(state), search_results, website_text)

    angle = synthesis.get("outreach_angle") or "(empty)"
    # Stash raw research text + the model's own fact_citations so the
    # downstream Validator can sanity-check that every concrete number
    # cited in the brief actually appears in the source. Without this
    # the Analyst happily writes value props citing fabricated headcounts.
    raw_research_text = (
        "\n".join(line for snippets in search_results.values() for line in snippets)
        + "\n"
        + (website_text or "")
    )

    # Extract a structured source list from all 30 snippets so the UI
    # can show clickable citation cards. Reps can then click [Press],
    # [LinkedIn], [News] etc. links to verify exactly where each fact
    # came from — critical for trust-but-verify.
    sources = _extract_sources(search_results)
    logger.info(f"[Researcher] Captured {len(sources)} unique source URLs")

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
        "fact_citations": synthesis.get("fact_citations") or [],
        "raw_research_text": raw_research_text[:8000],  # cap to keep state size sane
        "research_confidence": confidence,
        "sources": sources,
    }
