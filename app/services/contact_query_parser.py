"""app/services/contact_query_parser.py

Natural-language → structured-filter parser for the Contacts "AI search" mode.

The search box (AI mode) sends a plain-English question here; we translate it
into the SAME filters the list endpoint already understands
(is_decision_maker, contact_category, opportunity_level, order_by) plus a
residual free-text ``search`` for anything not captured (hotel name, brand,
city, person name).

On ANY failure (model error, timeout, bad JSON, empty parse) we fall back to
``{"search": <raw query>}`` so the box degrades to plain literal search instead
of breaking. Cheap model, low token budget, short timeout — this is interactive.
"""

from __future__ import annotations

import json
import logging

import httpx

from app.services.ai_client import ai_generate

logger = logging.getLogger(__name__)

MODEL = "gemini-2.5-flash-lite"

_VALID_CATEGORY = {"buyer", "seller", "competitor", "personal", "junk"}
_VALID_OPP = {"high"}
_VALID_ORDER = {"priority_score", "opportunity_score", "last_seen", "name"}

PROMPT = """You translate a natural-language search into JSON filters for JA
Uniforms' hotel-contact directory. JA SELLS uniforms to hotels and resorts.

Choose only the filters the query clearly implies; leave the rest null.
- is_decision_maker: true when the user wants people who BUY or specify uniforms
  ("decision-makers", "DMs", "buyers I should call", "who can I sell to",
  "purchasing/procurement people"). Otherwise null. Never set it to false.
- contact_category: "buyer" (hotels/resorts/hospitality-services companies),
  "seller" (suppliers to JA), "competitor", "personal", or null.
- opportunity_level: "high" for "high opportunity"/"hot"/"best leads", else null.
- order_by: "last_seen" for "recent"/"latest"/"recently active";
  "opportunity_score" for "best/highest opportunity"; otherwise null.
- search: free-text keywords for anything NOT covered above — a hotel or brand
  name, a city, a person's name. Space-separated, drop filler words ("at",
  "the", "in"). null if there is nothing left.

Return ONLY a JSON object, no markdown, no prose. Example:
{"is_decision_maker": true, "contact_category": null, "opportunity_level": null, "order_by": null, "search": "luxury miami"}

Query: """


async def parse_contact_query(q: str) -> dict:
    """Return a dict of validated filters. Always safe: falls back to literal
    search ({"search": q}) on any problem, and returns {} for an empty query."""
    q = (q or "").strip()
    if not q:
        return {}

    fallback = {"search": q}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            raw = await ai_generate(
                client,
                PROMPT + q,
                model=MODEL,
                temperature=0.0,
                max_tokens=256,
                timeout=15,
            )
    except Exception as exc:
        logger.warning(f"parse_contact_query: AI error: {exc}")
        return fallback

    if not raw:
        return fallback
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip() if "```" in raw else raw
    try:
        data = json.loads(raw)
    except Exception:
        logger.warning("parse_contact_query: could not parse model JSON")
        return fallback
    if not isinstance(data, dict):
        return fallback

    out: dict = {}
    if data.get("is_decision_maker") is True:
        out["is_decision_maker"] = True
    cat = data.get("contact_category")
    if isinstance(cat, str) and cat.strip().lower() in _VALID_CATEGORY:
        out["contact_category"] = cat.strip().lower()
    opp = data.get("opportunity_level")
    if isinstance(opp, str) and opp.strip().lower() in _VALID_OPP:
        out["opportunity_level"] = opp.strip().lower()
    order = data.get("order_by")
    if isinstance(order, str) and order.strip().lower() in _VALID_ORDER:
        out["order_by"] = order.strip().lower()
    s = data.get("search")
    if isinstance(s, str) and s.strip():
        out["search"] = s.strip()

    # Model captured nothing usable → literal search so a real query never
    # resolves to "no filters at all".
    if not out:
        return fallback
    return out
