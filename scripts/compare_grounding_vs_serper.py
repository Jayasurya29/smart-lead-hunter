"""
Compare Grounding vs Serper+Gemini head-to-head on real leads
==============================================================

Runs BOTH paths on the same lead and reports which one filled more
fields, how long each took, and what each one returned.

The point is to decide whether Gemini's built-in Google Search
("grounding") is actually better than the existing Serper→Gemini
extraction pipeline for hotel research — before wiring grounding into
production tasks.

What's compared:
----------------
Path A — Serper+Gemini (current production)
    6-stage pipeline in `enrich_lead_data`: classify → build queries
    → Serper×5 → Gemini extracts data → Gemini extracts entities →
    Gemini extracts address. Cheap (~$0.01/lead), deterministic
    (temp 0.2), proven.

Path B — Gemini with googleSearch tool (proposed)
    One-shot grounded prompt. Gemini chooses what to search and reads
    the results directly. Same as Google's "AI Overview" feature.
    More expensive (~$0.04/lead) but synthesizes in one call.

Usage (Windows PowerShell):
---------------------------

    # Test on top 5 highest-score leads with at least one empty core field
    python scripts\\compare_grounding_vs_serper.py --missing-only

    # Test on a specific lead
    python scripts\\compare_grounding_vs_serper.py --lead-id 1273

    # Test on top 10 highest-score leads regardless of completeness
    python scripts\\compare_grounding_vs_serper.py --limit 10

    # Save full results to JSON for analysis
    python scripts\\compare_grounding_vs_serper.py --limit 5 --save-json results.json

Cost guard:
-----------
Each lead costs roughly $0.05 across both paths. The script ALWAYS
prints the projected total before running and asks for confirmation.

Model pinning:
--------------
MODEL = "gemini-2.5-flash" is hardcoded. Switch to "gemini-3-flash"
later (60% cheaper grounding once it lands on Vertex). Both paths use
the SAME model so the comparison is apples-to-apples.

Created: 2026-05-01
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import httpx  # noqa: E402

from sqlalchemy import select, or_  # noqa: E402

from app.database import async_session  # noqa: E402
from app.models.potential_lead import PotentialLead  # noqa: E402
from app.services.lead_data_enrichment import enrich_lead_data  # noqa: E402
from app.services.gemini_client import (  # noqa: E402
    get_gemini_url,
    get_gemini_headers,
)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

# Pin model so both paths run on the same one — apples to apples.
# To switch to Gemini 3 (60% cheaper grounding) later, change this string.
MODEL = "gemini-2.5-flash"

# Fields we care about for hotel research. Both paths are scored on
# how many of these they successfully populate.
TARGET_FIELDS = [
    "opening_date",
    "project_type",
    "brand",
    "brand_tier",
    "room_count",
    "management_company",
    "owner",
    "developer",
    "address",
    "description",
]

# Cost estimates (per lead). Used in the run-size confirmation.
SERPER_COST_PER_LEAD = 0.01  # ~3-5 Serper + 2-3 Gemini Flash calls
GROUNDING_COST_PER_LEAD = 0.04  # 1 grounded call ($0.035 + ~$0.005 tokens)

logging.basicConfig(
    level=logging.WARNING,  # quiet — we want clean comparison output
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Path A — Serper+Gemini (current production)
# ─────────────────────────────────────────────────────────────────────────────


async def run_serper_path(lead: PotentialLead) -> Dict[str, Any]:
    start = time.monotonic()
    try:
        result = await enrich_lead_data(
            hotel_name=lead.hotel_name,
            city=lead.city or "",
            state=lead.state or "",
            country=lead.country or "",
            brand=lead.brand or "",
            current_opening_date=lead.opening_date or "",
            current_brand_tier=lead.brand_tier or "",
            current_room_count=lead.room_count or 0,
            mode="full",  # `full` so it looks at everything for fair comparison
            search_name=lead.search_name or "",
        )
    except Exception as e:
        return {
            "elapsed_s": time.monotonic() - start,
            "error": f"{type(e).__name__}: {e}",
            "fields": {},
        }

    elapsed = time.monotonic() - start
    return {
        "elapsed_s": elapsed,
        "fields": {f: result.get(f) for f in TARGET_FIELDS},
        "confidence": result.get("confidence"),
        "source_url": result.get("source_url"),
        "raw": result,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Path B — Gemini with googleSearch tool (grounding)
# ─────────────────────────────────────────────────────────────────────────────


def _build_grounding_prompt(lead: PotentialLead) -> str:
    location = ", ".join(
        filter(None, [lead.city, lead.state, lead.country])
    )
    brand_line = f"BRAND: {lead.brand}\n" if lead.brand else ""
    return f"""You are researching a specific hotel for a B2B uniform sales pipeline.

HOTEL: {lead.hotel_name}
LOCATION: {location}
{brand_line}
Use Google Search to find the MOST CURRENT information about this hotel.
Prefer sources from the last 12 months. Look for:
  - Most recent announced opening / reopening date (delays count!)
  - Full ownership and operator chain
  - Property details: room count, brand tier, project type
  - Street address (only if officially announced)

Return a JSON object with these fields. Use null for any field you
cannot confidently determine — DO NOT guess.

{{
  "opening_date": "Most recent announced date — e.g. 'December 2026', 'Q4 2027', 'Spring 2028'",
  "project_type": "new_opening | renovation | rebrand | reopening | conversion | ownership_change",
  "room_count": 250,
  "brand": "Hotel brand / flag (e.g. 'Marriott', 'Four Seasons', 'Independent')",
  "brand_tier": "tier1_ultra_luxury | tier2_luxury | tier3_upper_upscale | tier4_upscale | tier5_upper_midscale | tier6_midscale | tier7_economy",
  "management_company": "Day-to-day hotel OPERATOR — the company running the hotel (NOT brand licensor like Paramount, Disney, Nickelodeon)",
  "owner": "Property owner / real estate holding entity",
  "developer": "Entity BUILDING the property (often same as owner)",
  "address": "Street address if officially announced — null if not yet public",
  "description": "1-2 sentence summary of the property",
  "confidence": "high | medium | low"
}}

Return ONLY the JSON object — no preamble, no markdown fences."""


async def run_grounding_path(lead: PotentialLead) -> Dict[str, Any]:
    url = get_gemini_url(MODEL)
    headers = get_gemini_headers()

    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": _build_grounding_prompt(lead)}]}
        ],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,  # Grounding works best at 1.0 per Google docs
            "maxOutputTokens": 4096,
        },
    }

    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {
            "elapsed_s": time.monotonic() - start,
            "error": f"{type(e).__name__}: {e}",
            "fields": {},
        }

    elapsed = time.monotonic() - start

    try:
        candidate = data["candidates"][0]
        content = candidate["content"]["parts"][0]["text"].strip()
    except (KeyError, IndexError) as e:
        return {
            "elapsed_s": elapsed,
            "error": f"Couldn't parse Gemini response: {e}",
            "fields": {},
            "raw_response": data,
        }

    # Strip markdown fences if Gemini wrapped JSON in them
    clean = content
    if clean.startswith("```"):
        parts = clean.split("```")
        if len(parts) >= 2:
            inner = parts[1]
            if inner.startswith("json"):
                inner = inner[4:].strip()
            clean = inner.strip()

    try:
        parsed = json.loads(clean)
    except json.JSONDecodeError as e:
        return {
            "elapsed_s": elapsed,
            "error": f"JSON parse error: {e}",
            "fields": {},
            "raw_text": content,
        }

    grounding_meta = candidate.get("groundingMetadata", {}) or {}
    web_queries = grounding_meta.get("webSearchQueries", []) or []
    grounding_chunks = grounding_meta.get("groundingChunks", []) or []
    sources = []
    for chunk in grounding_chunks:
        web = chunk.get("web", {}) or {}
        title = web.get("title")
        if title:
            sources.append(title)

    return {
        "elapsed_s": elapsed,
        "fields": {f: parsed.get(f) for f in TARGET_FIELDS},
        "confidence": parsed.get("confidence"),
        "search_queries": web_queries,
        "source_titles": sources,
        "source_count": len(grounding_chunks),
        "raw": parsed,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Comparison + scoring
# ─────────────────────────────────────────────────────────────────────────────

_BLANK_VALUES = {"", "unknown", "none", "n/a", "null", "tbd", "—"}


def _is_meaningful(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip().lower() not in _BLANK_VALUES and bool(v.strip())
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, (list, dict)):
        return bool(v)
    return True


def _fields_filled(fields: Dict[str, Any]) -> int:
    return sum(1 for v in fields.values() if _is_meaningful(v))


def _fmt(v: Any, width: int = 32) -> str:
    if v is None:
        return "—"
    s = str(v).replace("\n", " ")
    if len(s) > width:
        return s[: width - 3] + "..."
    return s


def _print_field_table(
    serper_fields: Dict[str, Any], grounding_fields: Dict[str, Any]
) -> None:
    width = 33
    print(
        f"  {'FIELD':<22} | {'SERPER+GEMINI':<{width}} | {'GROUNDING':<{width}}"
    )
    print(f"  {'-'*22}-+-{'-'*width}-+-{'-'*width}")
    for field in TARGET_FIELDS:
        s_val = serper_fields.get(field)
        g_val = grounding_fields.get(field)
        s_meaningful = _is_meaningful(s_val)
        g_meaningful = _is_meaningful(g_val)
        marker = "  "
        if s_meaningful and not g_meaningful:
            marker = "<<"  # Serper found it, grounding didn't
        elif g_meaningful and not s_meaningful:
            marker = ">>"  # Grounding found it, serper didn't
        elif s_meaningful and g_meaningful and str(s_val).strip() != str(g_val).strip():
            marker = "!="  # both found something but they differ
        print(
            f"  {field:<22} | {_fmt(s_val, width):<{width}} | "
            f"{_fmt(g_val, width):<{width}} {marker}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Per-lead comparison
# ─────────────────────────────────────────────────────────────────────────────


async def compare_one_lead(lead: PotentialLead, verbose: bool) -> Dict[str, Any]:
    print()
    print("=" * 100)
    print(f"  LEAD #{lead.id}: {lead.hotel_name}")
    loc_parts = [lead.city, lead.state, lead.country]
    print(f"  Location: {', '.join(p for p in loc_parts if p)}")
    print(
        f"  Current:  opening_date={lead.opening_date!r}, "
        f"brand_tier={lead.brand_tier!r}, "
        f"rooms={lead.room_count}, "
        f"mgmt={lead.management_company!r}"
    )
    print("=" * 100)

    print()
    print("  Running Serper+Gemini path...", end=" ", flush=True)
    serper_result = await run_serper_path(lead)
    if "error" in serper_result:
        print(f"ERROR — {serper_result['error']}")
    else:
        print(f"done in {serper_result['elapsed_s']:.1f}s")

    print("  Running Grounding path......", end=" ", flush=True)
    grounding_result = await run_grounding_path(lead)
    if "error" in grounding_result:
        print(f"ERROR — {grounding_result['error']}")
    else:
        print(f"done in {grounding_result['elapsed_s']:.1f}s")

    s_fields = serper_result.get("fields", {})
    g_fields = grounding_result.get("fields", {})

    print()
    _print_field_table(s_fields, g_fields)

    s_score = _fields_filled(s_fields)
    g_score = _fields_filled(g_fields)
    n = len(TARGET_FIELDS)

    print()
    print(
        f"  Score    — Serper: {s_score}/{n}  |  Grounding: {g_score}/{n}"
    )
    print(
        f"  Latency  — Serper: {serper_result.get('elapsed_s', 0):.1f}s  |  "
        f"Grounding: {grounding_result.get('elapsed_s', 0):.1f}s"
    )

    queries = grounding_result.get("search_queries", [])
    if queries:
        head = ", ".join(queries[:3])
        suffix = f" (+{len(queries)-3} more)" if len(queries) > 3 else ""
        print(f"  Grounding searched: {head}{suffix}")
    sources = grounding_result.get("source_titles", [])
    if sources and verbose:
        print(f"  Grounding read {len(sources)} sources:")
        for src in sources[:8]:
            print(f"    · {src}")

    return {
        "lead_id": lead.id,
        "hotel_name": lead.hotel_name,
        "serper": serper_result,
        "grounding": grounding_result,
        "serper_score": s_score,
        "grounding_score": g_score,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Lead selection
# ─────────────────────────────────────────────────────────────────────────────


async def _load_leads(
    limit: int,
    lead_id: Optional[int],
    missing_only: bool,
) -> List[PotentialLead]:
    async with async_session() as s:
        if lead_id is not None:
            result = await s.execute(
                select(PotentialLead).where(PotentialLead.id == lead_id)
            )
            lead = result.scalar_one_or_none()
            return [lead] if lead else []

        q = select(PotentialLead).where(PotentialLead.status == "new")
        if missing_only:
            q = q.where(
                or_(
                    PotentialLead.opening_date.is_(None),
                    PotentialLead.opening_date == "",
                    PotentialLead.brand_tier.is_(None),
                    PotentialLead.brand_tier == "",
                    PotentialLead.brand_tier == "unknown",
                    PotentialLead.room_count.is_(None),
                    PotentialLead.room_count == 0,
                    PotentialLead.management_company.is_(None),
                    PotentialLead.management_company == "",
                )
            )
        q = q.order_by(PotentialLead.lead_score.desc().nullslast()).limit(limit)
        result = await s.execute(q)
        return list(result.scalars().all())


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare Gemini grounding vs Serper+Gemini head-to-head.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="How many leads to test (default: 5).",
    )
    parser.add_argument(
        "--lead-id",
        type=int,
        default=None,
        help="Test one specific lead by ID (overrides --limit / --missing-only).",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help=(
            "Only sample leads with at least one empty core field "
            "(opening_date, brand_tier, room_count, management_company). "
            "Best for showing where grounding actually helps."
        ),
    )
    parser.add_argument(
        "--save-json",
        metavar="PATH",
        default=None,
        help="Save full per-lead results to a JSON file.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra detail (grounding source titles, etc.).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the cost-confirmation prompt.",
    )
    args = parser.parse_args()

    leads = await _load_leads(
        limit=args.limit,
        lead_id=args.lead_id,
        missing_only=args.missing_only,
    )

    if not leads:
        print("No leads to test. Try --limit higher or check your filters.")
        return 1

    n = len(leads)
    estimated_cost = n * (SERPER_COST_PER_LEAD + GROUNDING_COST_PER_LEAD)
    print()
    print(f"Will test {n} lead(s) using model {MODEL!r}.")
    print(
        f"Estimated cost: ${estimated_cost:.2f}  "
        f"(Serper+Gemini: ~${n * SERPER_COST_PER_LEAD:.2f}, "
        f"Grounding: ~${n * GROUNDING_COST_PER_LEAD:.2f})"
    )

    if not args.yes:
        try:
            ans = input("Proceed? [y/N] ").strip().lower()
        except EOFError:
            ans = "n"
        if ans not in ("y", "yes"):
            print("Aborted.")
            return 0

    summaries: List[Dict[str, Any]] = []
    for lead in leads:
        try:
            summary = await compare_one_lead(lead, verbose=args.verbose)
            summaries.append(summary)
        except Exception as e:
            logger.exception(f"compare_one_lead({lead.id}) crashed: {e}")
            print(f"  ✗ Lead #{lead.id} crashed: {e}")

    # ── Final summary ──
    print()
    print("=" * 100)
    print(f"  FINAL — {len(summaries)} leads tested with {MODEL}")
    print("=" * 100)
    print()

    s_total = sum(s["serper_score"] for s in summaries)
    g_total = sum(s["grounding_score"] for s in summaries)
    s_lat = sum(s["serper"].get("elapsed_s", 0) for s in summaries)
    g_lat = sum(s["grounding"].get("elapsed_s", 0) for s in summaries)
    n_summaries = max(len(summaries), 1)
    max_possible = len(TARGET_FIELDS) * len(summaries)

    print(
        f"  Total fields filled  — Serper: {s_total}/{max_possible}  |  "
        f"Grounding: {g_total}/{max_possible}"
    )
    print(
        f"  Avg latency / lead   — Serper: {s_lat/n_summaries:.1f}s  |  "
        f"Grounding: {g_lat/n_summaries:.1f}s"
    )
    print(
        f"  Estimated total cost — Serper: ~${len(summaries)*SERPER_COST_PER_LEAD:.2f}  |  "
        f"Grounding: ~${len(summaries)*GROUNDING_COST_PER_LEAD:.2f}"
    )
    print()

    diff = g_total - s_total
    if diff > 0:
        print(f"  → Grounding filled {diff} more field(s) overall")
    elif diff < 0:
        print(f"  → Serper filled {-diff} more field(s) overall")
    else:
        print(f"  → Tied on field count")
    print()

    # Per-lead win/loss breakdown
    print("  Per-lead breakdown:")
    print(
        f"    {'LEAD':<6} {'NAME':<48} "
        f"{'SERPER':<8} {'GROUND':<8} {'WINNER':<10}"
    )
    print(f"    {'-'*6} {'-'*48} {'-'*8} {'-'*8} {'-'*10}")
    grounding_wins = 0
    serper_wins = 0
    ties = 0
    for s in summaries:
        if s["grounding_score"] > s["serper_score"]:
            winner = "Grounding"
            grounding_wins += 1
        elif s["serper_score"] > s["grounding_score"]:
            winner = "Serper"
            serper_wins += 1
        else:
            winner = "Tie"
            ties += 1
        name = (s["hotel_name"] or "?")[:46]
        print(
            f"    #{s['lead_id']:<5} {name:<48} "
            f"{s['serper_score']}/{len(TARGET_FIELDS):<6} "
            f"{s['grounding_score']}/{len(TARGET_FIELDS):<6} {winner}"
        )
    print()
    print(
        f"  Verdict: Grounding wins {grounding_wins}, "
        f"Serper wins {serper_wins}, ties {ties}"
    )
    print()

    if args.save_json:
        # Strip non-serializable bits (httpx exceptions, etc.) before saving
        def _clean(d):
            if isinstance(d, dict):
                return {k: _clean(v) for k, v in d.items() if k != "raw_response"}
            if isinstance(d, list):
                return [_clean(x) for x in d]
            try:
                json.dumps(d)
                return d
            except (TypeError, ValueError):
                return str(d)

        out = {
            "model": MODEL,
            "leads_tested": len(summaries),
            "totals": {
                "serper_fields_filled": s_total,
                "grounding_fields_filled": g_total,
                "max_possible": max_possible,
                "grounding_wins": grounding_wins,
                "serper_wins": serper_wins,
                "ties": ties,
            },
            "per_lead": [_clean(s) for s in summaries],
        }
        Path(args.save_json).write_text(json.dumps(out, indent=2))
        print(f"  Saved full results to {args.save_json}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
