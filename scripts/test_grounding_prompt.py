"""
Smart Lead Hunter — Grounding Prompt A/B Tester v2
=====================================================
Run from smart-lead-hunter root:

    python scripts/test_grounding_prompt.py "The Ritz-Carlton South Beach" "Miami Beach" "FL"
    python scripts/test_grounding_prompt.py "Fontainebleau Miami Beach" "Miami Beach" "FL"
    python scripts/test_grounding_prompt.py "Omni Orlando Resort at ChampionsGate" "Orlando" "FL"
    python scripts/test_grounding_prompt.py "Four Seasons Fort Lauderdale" "Fort Lauderdale" "FL"

Custom prompt:
    python scripts/test_grounding_prompt.py --prompt "Who is the current GM at The Ritz-Carlton South Beach?"

Modes:
    --contacts    Test CONTACT discovery prompts (default)
    --lead-data   Test LEAD DATA enrichment prompts
    --both        Test both contact and lead data prompts

Tests grounding directly — no pipeline, no DB. Shows:
  - The search queries Gemini actually generated
  - Citation count + source URLs
  - Freshness signals (year mentions in sources/text)
  - Stale contact red flags (former, previously, departed, etc.)
  - Side-by-side comparison: broad vs targeted prompts
"""

import asyncio
import json
import re
import sys
import os
import time
from collections import Counter
from typing import Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()


# ─── Freshness / Staleness Analysis ──────────────────────────────────────────

# Words in grounded text that suggest a contact is NOT current
_STALE_SIGNALS = [
    "former", "formerly", "previously", "ex-", "departed",
    "left the", "stepped down", "retired", "resigned",
    "moved to", "joined", "transitioned to", "no longer",
    "was appointed",  # past tense = might be stale
    "was named",      # past tense
]

# Words that suggest a contact IS current
_FRESH_SIGNALS = [
    "currently", "current", "as of 2025", "as of 2026",
    "is the", "serves as", "leads the", "oversees",
    "appointed in 2025", "appointed in 2026",
    "named in 2025", "named in 2026",
    "joined in 2025", "joined in 2026",
]

# Year extraction for citation freshness
_YEAR_RE = re.compile(r"\b(201\d|202[0-9])\b")


def _analyze_freshness(text: str, sources: list[dict]) -> dict:
    """Analyze grounded text and sources for freshness/staleness signals."""
    text_lower = text.lower()

    stale_hits = []
    for sig in _STALE_SIGNALS:
        if sig in text_lower:
            idx = text_lower.index(sig)
            start = max(0, idx - 40)
            end = min(len(text_lower), idx + len(sig) + 40)
            snippet = text[start:end].strip().replace("\n", " ")
            stale_hits.append(f"  '{sig}' → ...{snippet}...")

    fresh_hits = []
    for sig in _FRESH_SIGNALS:
        if sig in text_lower:
            idx = text_lower.index(sig)
            start = max(0, idx - 40)
            end = min(len(text_lower), idx + len(sig) + 40)
            snippet = text[start:end].strip().replace("\n", " ")
            fresh_hits.append(f"  '{sig}' → ...{snippet}...")

    years_in_text = _YEAR_RE.findall(text)
    year_counts = Counter(years_in_text)

    source_years = []
    for s in sources:
        title = s.get("title", "")
        years = _YEAR_RE.findall(title)
        source_years.extend(years)
    source_year_counts = Counter(source_years)

    return {
        "stale_signals": stale_hits,
        "fresh_signals": fresh_hits,
        "years_in_text": dict(year_counts.most_common()),
        "years_in_sources": dict(source_year_counts.most_common()),
        "freshness_score": len(fresh_hits) - len(stale_hits),
    }


# ─── Endpoint Builder ────────────────────────────────────────────────────────

def _build_grounding_url(project: str, model: str) -> tuple[str, str]:
    """Pick the right endpoint for the model.

    Gemini 3.x models only work on the global endpoint.
    Gemini 2.x models prefer us-central1 (global can phantom-ground).

    Returns (url, location_used).
    """
    # Gemini 3.x → global only
    if any(tag in model for tag in ("gemini-3", "gemini-4")):
        location = "global"
        host = "aiplatform.googleapis.com"
    else:
        # 2.x → us-central1 (proven reliable for grounding)
        location = "us-central1"
        host = f"{location}-aiplatform.googleapis.com"

    url = (
        f"https://{host}/v1/"
        f"projects/{project}/locations/{location}/"
        f"publishers/google/models/{model}:generateContent"
    )
    return url, location


# ─── Grounding Call ──────────────────────────────────────────────────────────

async def call_grounding(prompt: str, label: str = "") -> dict:
    """Call the grounding API and return structured results."""
    import httpx
    from app.services.ai_client import _ensure_init, _get_config
    from app.services.gemini_client import get_gemini_headers

    _ensure_init()
    config = _get_config()
    project = config["vertex_project_id"]
    model = config["model"]

    url, location = _build_grounding_url(project, model)

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,
            "maxOutputTokens": 8192,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(url, headers=get_gemini_headers(), json=payload)
        elapsed = time.monotonic() - start
        data = resp.json()
    except Exception as e:
        elapsed = time.monotonic() - start
        return {
            "label": label,
            "prompt": prompt,
            "error": f"{type(e).__name__}: {e}",
            "elapsed": elapsed,
            "model": model,
            "location": location,
        }

    # Check for API error
    if resp.status_code != 200:
        error = data.get("error", {}).get("message", f"HTTP {resp.status_code}")
        return {
            "label": label,
            "prompt": prompt,
            "error": f"{resp.status_code}: {error[:120]}",
            "elapsed": elapsed,
            "model": model,
            "location": location,
        }

    # Parse response
    try:
        candidate = data["candidates"][0]
        parts = (candidate.get("content") or {}).get("parts") or []
        text = (parts[0].get("text") if parts else "") or ""
    except (KeyError, IndexError):
        return {
            "label": label,
            "prompt": prompt,
            "error": f"Bad response shape: {list(data.keys())}",
            "elapsed": elapsed,
            "model": model,
            "location": location,
        }

    # Citations
    grounding_meta = candidate.get("groundingMetadata", {}) or {}
    sources = []
    for chunk in (grounding_meta.get("groundingChunks", []) or []):
        web = chunk.get("web", {}) or {}
        title = web.get("title", "")
        uri = web.get("uri", "")
        domain = web.get("domain", "")
        if title or uri:
            sources.append({"title": title, "uri": uri, "domain": domain})

    search_queries = grounding_meta.get("webSearchQueries", []) or []
    supports = grounding_meta.get("groundingSupports", []) or []
    finish_reason = candidate.get("finishReason", "?")
    freshness = _analyze_freshness(text, sources)

    return {
        "label": label,
        "prompt": prompt,
        "text": text,
        "text_len": len(text),
        "elapsed": elapsed,
        "finish_reason": finish_reason,
        "citations": len(sources),
        "sources": sources,
        "search_queries": search_queries,
        "supports_count": len(supports),
        "is_phantom": len(sources) == 0,
        "freshness": freshness,
        "model": model,
        "location": location,
    }


def _print_result(r: dict, verbose: bool = True):
    """Pretty-print a grounding result."""
    label = r.get("label", "")
    if label:
        print(f"\n{'━'*60}")
        print(f"  {label}")
        print(f"{'━'*60}")

    if r.get("error"):
        print(f"  ❌ ERROR: {r['error']}")
        print(f"  ⏱  {r['elapsed']:.1f}s  |  model={r.get('model','')}  |  endpoint={r.get('location','')}")
        return

    prompt = r["prompt"]
    if len(prompt) > 120:
        prompt = prompt[:117] + "..."
    print(f"  PROMPT: {prompt}")
    print(f"  ⏱  {r['elapsed']:.1f}s  |  "
          f"📝 {r['text_len']} chars  |  "
          f"📎 {r['citations']} citations  |  "
          f"🔍 {len(r['search_queries'])} queries  |  "
          f"{'🔴 PHANTOM' if r['is_phantom'] else '🟢 GROUNDED'}  |  "
          f"model={r.get('model','')}  |  endpoint={r.get('location','')}")

    if r.get("search_queries"):
        print(f"\n  QUERIES GEMINI RAN:")
        for i, q in enumerate(r["search_queries"], 1):
            print(f"    {i}. {q}")

    f = r.get("freshness", {})
    score = f.get("freshness_score", 0)
    emoji = "🟢" if score > 0 else "🔴" if score < 0 else "🟡"
    print(f"\n  FRESHNESS: {emoji} score={score} "
          f"({len(f.get('fresh_signals', []))} fresh, "
          f"{len(f.get('stale_signals', []))} stale)")

    if f.get("years_in_text"):
        print(f"  Years in text: {f['years_in_text']}")
    if f.get("stale_signals"):
        print(f"  ⚠ STALE SIGNALS:")
        for s in f["stale_signals"][:5]:
            print(f"    {s}")
    if f.get("fresh_signals"):
        print(f"  ✅ FRESH SIGNALS:")
        for s in f["fresh_signals"][:5]:
            print(f"    {s}")

    if verbose and r.get("text"):
        print(f"\n  TEXT:")
        print(f"  {'─'*50}")
        for line in r["text"].split("\n"):
            print(f"  {line}")
        print(f"  {'─'*50}")

    if r.get("sources"):
        print(f"\n  SOURCES:")
        for s in r["sources"][:8]:
            title = s.get("title", "(no title)")
            uri = s.get("uri", "")[:80]
            print(f"    • {title}")
            if uri:
                print(f"      {uri}")
    print()


# ─── Test Suites ─────────────────────────────────────────────────────────────

def _build_contact_prompts(hotel: str, city: str, state: str) -> list[dict]:
    """Build the A/B test suite for contact discovery."""
    loc = f"{city}, {state}" if city else state

    return [
        {
            "label": "A1 — BROAD: Leadership team (current SLH approach)",
            "prompt": (
                f"Who are the current leadership and management team at "
                f"{hotel} in {loc}? "
                f"List each person's full name, exact job title, and company. "
                f"Only include people currently working at this property in 2026."
            ),
        },
        {
            "label": "A2 — BROAD: Keywords style",
            "prompt": (
                f"{hotel} in {loc} current leadership team and department heads 2026"
            ),
        },
        {
            "label": "A3 — BROAD: Specific roles list",
            "prompt": (
                f"Who are the current General Manager, Director of Housekeeping, "
                f"Director of Operations, Director of HR, Director of Purchasing, "
                f"and Director of Food & Beverage at {hotel} in {loc}? "
                f"List each person's full name, exact job title, and company."
            ),
        },
        {
            "label": "B1 — TARGETED: Current GM only",
            "prompt": (
                f"Who is the current General Manager at {hotel} in {loc} "
                f"as of 2025 or 2026?"
            ),
        },
        {
            "label": "B2 — TARGETED: Current Dir Ops / Housekeeping",
            "prompt": (
                f"Who is the current Director of Operations or Director of "
                f"Housekeeping at {hotel} in {loc}?"
            ),
        },
        {
            "label": "B3 — TARGETED: Purchasing / procurement",
            "prompt": (
                f"Who handles purchasing or procurement at {hotel} in {loc}? "
                f"Name the person currently in that role."
            ),
        },
        {
            "label": "B4 — TARGETED: Simple natural question",
            "prompt": (
                f"Who runs {hotel} in {loc} right now?"
            ),
        },
    ]


def _build_lead_data_prompts(hotel: str, city: str, state: str) -> list[dict]:
    """Build the A/B test suite for lead data enrichment."""
    loc = f"{city}, {state}" if city else state

    return [
        {
            "label": "A — JSON prompt (current SLH approach — phantom risk)",
            "prompt": (
                f"You are researching a specific hotel for a B2B uniform sales pipeline.\n\n"
                f"HOTEL: {hotel}\n"
                f"LOCATION: {loc}\n\n"
                f"Use Google Search to find the MOST CURRENT information about this hotel.\n"
                f"Return a JSON object with: opening_date, room_count, brand, "
                f"management_company, owner, developer, address, hotel_website.\n"
                f"Return ONLY a single JSON object — no preamble."
            ),
        },
        {
            "label": "B — Natural question (better grounding, extract JSON in call 2)",
            "prompt": (
                f"What is the latest news about {hotel} in {loc}? "
                f"When does it open, who is building it, who will operate it, "
                f"and how many rooms will it have?"
            ),
        },
        {
            "label": "C — Ultra-short (let Gemini decide what to search)",
            "prompt": f"{hotel} {loc} opening date details",
        },
    ]


# ─── Comparison Summary ─────────────────────────────────────────────────────

def _print_comparison(results: list[dict]):
    """Print a side-by-side comparison table."""
    print(f"\n{'='*60}")
    print(f"  COMPARISON SUMMARY")
    print(f"{'='*60}\n")

    # Show model + endpoint
    model = results[0].get("model", "?") if results else "?"
    location = results[0].get("location", "?") if results else "?"
    print(f"  Model: {model}  |  Endpoint: {location}\n")

    print(f"  {'Label':<52} {'Time':>5} {'Cite':>4} {'Qry':>3} {'Fresh':>5} {'Phantom':>7}")
    print(f"  {'─'*52} {'─'*5} {'─'*4} {'─'*3} {'─'*5} {'─'*7}")

    for r in results:
        if r.get("error"):
            print(f"  {r['label']:<52} {'ERR':>5}")
            continue

        f = r.get("freshness", {})
        score = f.get("freshness_score", 0)
        phantom = "🔴 YES" if r["is_phantom"] else "  no"
        fresh_emoji = "🟢" if score > 0 else "🔴" if score < 0 else "🟡"

        label = r["label"][:52]
        print(
            f"  {label:<52} "
            f"{r['elapsed']:>4.1f}s "
            f"{r['citations']:>4} "
            f"{len(r['search_queries']):>3} "
            f"{fresh_emoji}{score:>+3} "
            f"{phantom}"
        )

    valid = [r for r in results if not r.get("error") and not r.get("is_phantom")]
    if valid:
        best = max(valid, key=lambda r: (
            r.get("freshness", {}).get("freshness_score", 0),
            r["citations"],
            -r["elapsed"],
        ))
        print(f"\n  🏆 BEST: {best['label']}")
        print(f"     (freshness={best['freshness']['freshness_score']:+d}, "
              f"{best['citations']} citations, {best['elapsed']:.1f}s)")
    print()


# ─── Main ────────────────────────────────────────────────────────────────────

async def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    mode = "contacts"
    verbose = True
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    for a in sys.argv[1:]:
        if a == "--lead-data":
            mode = "lead-data"
        elif a == "--both":
            mode = "both"
        elif a == "--contacts":
            mode = "contacts"
        elif a == "--quiet":
            verbose = False

    # Show which model + endpoint will be used
    from app.services.ai_client import _ensure_init, _get_config
    _ensure_init()
    config = _get_config()
    model = config["model"]
    _, location = _build_grounding_url(config["vertex_project_id"], model)
    print(f"\n  Using model={model}, endpoint={location}")

    # Custom prompt mode
    if args and args[0] == "--prompt":
        prompt = " ".join(args[1:])
        result = await call_grounding(prompt, label="Custom Prompt")
        _print_result(result, verbose=verbose)
        return

    if not args:
        print("Need a hotel name. Run with no args for help.")
        sys.exit(1)

    hotel = args[0]
    city = args[1] if len(args) > 1 else ""
    state = args[2] if len(args) > 2 else ""

    print(f"\n{'#'*60}")
    print(f"  GROUNDING A/B TEST: {hotel}")
    print(f"  Location: {city}, {state}")
    print(f"  Mode: {mode}")
    print(f"  Model: {model}  |  Endpoint: {location}")
    print(f"{'#'*60}")

    if mode in ("contacts", "both"):
        print(f"\n{'═'*60}")
        print(f"  CONTACT DISCOVERY PROMPTS")
        print(f"{'═'*60}")

        prompts = _build_contact_prompts(hotel, city, state)
        results = []

        for i, p in enumerate(prompts):
            result = await call_grounding(p["prompt"], label=p["label"])
            results.append(result)
            _print_result(result, verbose=verbose)
            if i < len(prompts) - 1:
                await asyncio.sleep(1.5)

        _print_comparison(results)

    if mode in ("lead-data", "both"):
        print(f"\n{'═'*60}")
        print(f"  LEAD DATA ENRICHMENT PROMPTS")
        print(f"{'═'*60}")

        prompts = _build_lead_data_prompts(hotel, city, state)
        results = []

        for i, p in enumerate(prompts):
            result = await call_grounding(p["prompt"], label=p["label"])
            results.append(result)
            _print_result(result, verbose=verbose)
            if i < len(prompts) - 1:
                await asyncio.sleep(1.5)

        _print_comparison(results)


if __name__ == "__main__":
    asyncio.run(main())
