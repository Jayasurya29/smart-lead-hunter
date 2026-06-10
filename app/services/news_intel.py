"""Hospitality intelligence news feed: USA + Caribbean, 4-star-and-up.

Daily pipeline (same doctrine as the contacts overhaul):
  1. FETCH      — Serper news vertical, fixed query ladder (appointments +
                  openings/acquisitions/rebrands/renovations)
  2. JUDGE      — one flash call per batch classifies each headline:
                  category, region, hotel, brand, person, luxury signal.
                  Names must be copied verbatim from the headline/snippet —
                  the model never invents.
  3. TRIANGULATE — appointment names run through relationship_intel
                  ("new GM at X — we know them from account Y"); hotel
                  names checked against potential_leads / existing_hotels
                  ("this property is already in our pipeline").
  4. PERSIST    — hotel_news table, ON CONFLICT (url) DO NOTHING, so the
                  scan is idempotent and the feed accumulates.

Entry point: run_news_scan(apply=...). The repo-root news_scan.py wraps it
for dry runs; the Celery task hotel_news_scan runs it daily.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
from sqlalchemy import text

from app.database import async_session
from app.services.ai_client import ai_generate

logger = logging.getLogger(__name__)

MODEL = "gemini-2.5-flash"

# Query ladder. Serper /news already biases to recent items; "gl: us"
# keeps the index US-centric, Caribbean terms pull the islands in.
NEWS_QUERIES = [
    # ── people moves (the triangulation feed) ──
    "hotel appoints general manager",
    "resort names new general manager",
    "hotel general manager appointed Caribbean",
    "appoints managing director hotel resort",
    "hotel names director of operations",
    "appoints hotel manager luxury resort",
    "new general manager luxury hotel",
    # ── market intelligence ──
    "luxury hotel opening",
    "new resort opening Caribbean",
    "hotel acquisition luxury",
    "hotel rebrand luxury",
    "resort renovation reopening",
    "hotel management agreement luxury",
    "new hotel construction Miami Florida",
    # ── education (universities / colleges — dining, housing, campus) ──
    "university new residence hall opening",
    "university dining services contract",
    "college new academic building opening",
    "university campus expansion construction",
    # ── healthcare (hospitals / medical centers) ──
    "new hospital opening Florida",
    "medical center expansion opening",
    "new healthcare facility opening",
    "hospital names chief executive",
    # ── general industry awareness (hot news the team should know) ──
    "hospitality industry news",
    "hotel industry trends",
    "luxury hotel brand launch",
    "hotel investment deal",
    "hotel company expansion",
    # ── innovation / reform / regulation (what's changing in the industry) ──
    "hotel industry technology innovation",
    "hospitality sustainability initiative",
    "hotel industry new regulation law",
    "hospitality labor law change",
    "hotel guest experience innovation",
]

CLASSIFY_PROMPT = """You are filtering an industry news feed for a company that
sells uniforms to three kinds of clients in the USA and the Caribbean:
  - upscale hotels and resorts (4-star and above)
  - universities and colleges
  - hospitals and medical centers / health systems

For EACH numbered item below, judge:
- relevant: true if EITHER (a) it is about a specific property/institution or
  company in one of those three sectors (USA or Caribbean), OR (b) it is a
  notable hospitality-industry development the team should be aware of — major
  deals/acquisitions, brand launches or expansions, significant company news,
  market/occupancy/investment trends, new technology or innovations,
  sustainability initiatives, regulatory or policy changes and reforms, design
  or operational shifts, or major industry reports/events.
  Set relevant: false for consumer travel tips, traveler deals/discounts,
  ranking listicles, budget/economy or vacation-rental consumer stories, K-12,
  and anything outside hospitality/education/healthcare.
- vertical: one of "hotel", "education", "healthcare", "other".
- category: one of "appointment", "opening", "acquisition", "rebrand",
  "renovation", "management_change", "industry", "other". Use "industry" for
  the (b) awareness stories that aren't a specific property/org event. For
  education/healthcare, "opening" covers new campuses/buildings/hospitals and
  "renovation" covers expansions/renovations.
- region: "usa", "caribbean", or "other".
- hotel_name: the specific property/institution name, copied from the text
  ("" if a company-level story).
- brand: the brand/chain/health-system if stated ("" otherwise).
- person_name / person_title: ONLY for category "appointment" /
  "management_change", copied VERBATIM from the text — never inferred,
  never expanded. "" if not stated.
- luxury: true if the text signals upscale/luxury/resort positioning (hotels only).

Respond with ONLY a JSON list, one object per item:
[{{"i": 1, "vertical": "hotel", "relevant": true, "category": "appointment",
   "region": "usa", "hotel_name": "", "brand": "", "person_name": "",
   "person_title": "", "luxury": true}}]

ITEMS:
{items}
"""


def serper_news(query: str, num: int = 10) -> list[dict[str, Any]]:
    """One Serper /news call -> [{title, link, snippet, date, source}]."""
    try:
        from app.services.outreach.config import SERPER_API_KEY
    except Exception:
        SERPER_API_KEY = None
    if not SERPER_API_KEY:
        logger.warning("news: SERPER_API_KEY not configured")
        return []
    try:
        resp = httpx.post(
            "https://google.serper.dev/news",
            headers={
                "X-API-KEY": SERPER_API_KEY,
                "Content-Type": "application/json",
            },
            json={"q": query, "num": num, "gl": "us"},
            timeout=15,
        )
        resp.raise_for_status()
        out = []
        for it in (resp.json().get("news") or [])[:num]:
            if it.get("link") and it.get("title"):
                out.append(
                    {
                        "title": it["title"],
                        "link": it["link"],
                        "snippet": it.get("snippet") or "",
                        "date": it.get("date") or "",
                        "source": it.get("source") or "",
                    }
                )
        return out
    except Exception as e:
        logger.warning(f"news: serper /news failed for {query!r}: {e}")
        return []


async def _classify(client: httpx.AsyncClient, items: list[dict]) -> list[dict]:
    """One flash call per <=20 items. Returns verdicts aligned by 'i'."""
    listing = "\n".join(
        f"{i + 1}. {it['title']} — {it['snippet'][:200]}" for i, it in enumerate(items)
    )
    try:
        raw = await ai_generate(
            client,
            CLASSIFY_PROMPT.format(items=listing),
            model=MODEL,
            temperature=0.1,
            max_tokens=3000,
        )
    except Exception as e:
        logger.warning(f"news: classify call failed: {e}")
        return []
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip()
    try:
        verdicts = json.loads(raw)
    except Exception:
        logger.warning("news: classify returned bad JSON")
        return []
    return verdicts if isinstance(verdicts, list) else []


async def _pipeline_match(db, hotel_name: str) -> tuple[bool, str | None]:
    """Is this property already in our pipeline (leads or existing hotels)?"""
    hn = (hotel_name or "").strip()
    if len(hn) < 5:
        return False, None
    pat = f"%{hn}%"
    try:
        row = (
            await db.execute(
                text(
                    "SELECT id, hotel_name FROM potential_leads "
                    "WHERE hotel_name ILIKE :pat LIMIT 1"
                ),
                {"pat": pat},
            )
        ).first()
        if row:
            return True, f"lead #{row.id} '{row.hotel_name}'"
        row = (
            await db.execute(
                text(
                    "SELECT id, hotel_name FROM existing_hotels "
                    "WHERE hotel_name ILIKE :pat LIMIT 1"
                ),
                {"pat": pat},
            )
        ).first()
        if row:
            return True, f"existing hotel #{row.id} '{row.hotel_name}'"
    except Exception as e:
        logger.warning(f"news: pipeline match failed for {hn!r}: {e}")
        await db.rollback()
    return False, None


async def run_news_scan(
    *,
    apply: bool = True,
    queries: list[str] | None = None,
    per_query: int = 10,
) -> dict[str, Any]:
    """Full scan. Returns a summary dict; prints nothing (callers print)."""
    queries = queries or NEWS_QUERIES

    # 1. FETCH (threaded — serper_news is sync httpx)
    fetched: list[dict] = []
    seen_urls: set[str] = set()
    results = await asyncio.gather(*[asyncio.to_thread(serper_news, q, per_query) for q in queries])
    for q, batch in zip(queries, results):
        for it in batch:
            if it["link"] not in seen_urls:
                seen_urls.add(it["link"])
                it["_query"] = q
                fetched.append(it)

    # drop URLs we already have — scan is incremental
    new_items = fetched
    async with async_session() as db:
        if fetched:
            existing = (
                (
                    await db.execute(
                        text("SELECT url FROM hotel_news WHERE url = ANY(:urls)"),
                        {"urls": [it["link"] for it in fetched]},
                    )
                )
                .scalars()
                .all()
            )
            known = set(existing)
            new_items = [it for it in fetched if it["link"] not in known]

    summary: dict[str, Any] = {
        "fetched": len(fetched),
        "new": len(new_items),
        "relevant": 0,
        "appointments": 0,
        "relationship_flags": 0,
        "items": [],
    }
    if not new_items:
        return summary

    # 2. JUDGE in batches of 20
    client = httpx.AsyncClient(timeout=90)
    judged: list[tuple[dict, dict]] = []
    try:
        for start in range(0, len(new_items), 20):
            chunk = new_items[start : start + 20]
            verdicts = await _classify(client, chunk)
            by_i = {v.get("i"): v for v in verdicts if isinstance(v, dict)}
            for idx, it in enumerate(chunk, start=1):
                v = by_i.get(idx)
                if v and v.get("relevant"):
                    judged.append((it, v))
    finally:
        await client.aclose()

    # 3. TRIANGULATE + 4. PERSIST
    from app.services.relationship_intel import find_known_relationships

    async with async_session() as db:
        for it, v in judged:
            person = (v.get("person_name") or "").strip()
            hotel = (v.get("hotel_name") or "").strip()
            rel_hits: list[dict] = []
            if person and " " in person:
                try:
                    rel_hits = await find_known_relationships(db, name=person, email=None)
                except Exception as e:
                    logger.warning(f"news: triangulation failed for {person!r}: {e}")
                    await db.rollback()  # keep the session usable
            in_pipe, pipe_ref = await _pipeline_match(db, hotel)

            record = {
                "url": it["link"],
                "title": it["title"],
                "snippet": it["snippet"][:1000],
                "source": it["source"][:160],
                "published_hint": it["date"][:80],
                "category": (v.get("category") or "other")[:40],
                "vertical": (v.get("vertical") or "hotel")[:20],
                "region": (v.get("region") or "other")[:20],
                "hotel_name": hotel[:300] or None,
                "brand": (v.get("brand") or "")[:160] or None,
                "person_name": person[:200] or None,
                "person_title": (v.get("person_title") or "")[:200] or None,
                "luxury": bool(v.get("luxury")),
                "in_pipeline": in_pipe,
                "pipeline_ref": pipe_ref,
                "query": (it.get("_query") or "")[:200] or None,
                "relationship_hits": rel_hits or None,
            }
            summary["relevant"] += 1
            if record["category"] in ("appointment", "management_change"):
                summary["appointments"] += 1
            if rel_hits:
                summary["relationship_flags"] += 1
            summary["items"].append(record)

            if apply:
                await db.execute(
                    text(
                        "INSERT INTO hotel_news (url, title, snippet, source, "
                        "published_hint, category, vertical, region, hotel_name, "
                        "brand, person_name, person_title, luxury, in_pipeline, "
                        "pipeline_ref, query, relationship_hits) "
                        "VALUES (:url, :title, :snippet, :source, "
                        ":published_hint, :category, :vertical, :region, "
                        ":hotel_name, :brand, :person_name, :person_title, "
                        ":luxury, :in_pipeline, :pipeline_ref, :query, "
                        "CAST(:rel AS jsonb)) "
                        "ON CONFLICT (url) DO NOTHING"
                    ),
                    {
                        **{k: record[k] for k in record if k != "relationship_hits"},
                        "rel": json.dumps(record["relationship_hits"])
                        if record["relationship_hits"]
                        else None,
                    },
                )
        if apply:
            await db.commit()
    return summary
