"""
diag_news_actions.py  --  PHASE 0 (read-only, ZERO writes)
==========================================================
Before we wire the News feed into actions, this proves -- over the REAL
`hotel_news` rows already in the DB -- exactly what those actions WOULD do:

  (1) HOTEL ENQUEUE   For every story that names a hotel, is that hotel already
                      in our pipeline (existing_hotels / potential_leads)?  The
                      NEW ones are what would be enqueued as leads (status='new')
                      for the 9:50 AM auto_smart_fill to enrich next morning.
                      Uses the SAME dedup key as save_lead_to_db so "new" here
                      means "new" there.

  (2) PERSON FLAGS    For every appointment / mgmt-change story, did the scan
                      already match the person to someone we know
                      (relationship_hits)?  Those are the review flags -- a
                      known contact who just changed roles.

It writes NOTHING. No leads created, no rows touched. Just SELECT + a report so
we can see volume and quality before deciding auto-enqueue vs. queue-for-review.

USAGE (repo root, venv active, DATABASE_URL set)
------------------------------------------------
  python scripts/ops/diag_news_actions.py
  python scripts/ops/diag_news_actions.py --days 60 --top 40
  python scripts/ops/diag_news_actions.py --csv news_actions_preview.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402

# Reuse the REAL dedup normalizer so "is this hotel already in the pipeline?"
# matches exactly what save_lead_to_db() would decide.
try:
    from app.services.lead_factory import _normalize_for_dedup  # noqa: E402
except Exception:  # pragma: no cover - degrade gracefully
    import re

    def _normalize_for_dedup(name):  # type: ignore
        s = (name or "").lower()
        s = re.sub(r"[^a-z0-9 ]+", " ", s)
        return " ".join(s.split())


# A story is worth enqueuing when it's a real property EVENT (not generic
# industry chatter) in our sell-to geography. The scan already gated relevance;
# this is the second gate before we create a lead.
PROPERTY_EVENTS = {
    "opening", "appointment", "management_change", "renovation",
    "acquisition", "rebrand",
}
TARGET_REGIONS = {"usa", "caribbean"}


async def fetch(days: int):
    async with async_session() as s:
        news = (await s.execute(text(
            "SELECT id, url, title, source, category, vertical, region, "
            "hotel_name, brand, person_name, person_title, luxury, in_pipeline, "
            "relationship_hits, created_at FROM hotel_news "
            "WHERE created_at > NOW() - make_interval(days => :d) "
            "ORDER BY created_at DESC"
        ), {"d": days})).mappings().all()

        hotels = (await s.execute(text(
            "SELECT hotel_name, hotel_name_normalized FROM existing_hotels"
        ))).mappings().all()
        leads = (await s.execute(text(
            "SELECT hotel_name, hotel_name_normalized FROM potential_leads "
            "WHERE status NOT IN ('rejected','duplicate')"
        ))).mappings().all()
    return news, hotels, leads


def build_pipeline_keys(hotels, leads):
    keys = set()
    for r in list(hotels) + list(leads):
        keys.add(_normalize_for_dedup(r["hotel_name"] or ""))
    keys.discard("")
    return keys


def run_report(news, hotels, leads, args):
    pipeline = build_pipeline_keys(hotels, leads)
    p = print

    # ---- HOTEL ENQUEUE analysis ------------------------------------------
    with_hotel = [n for n in news if (n["hotel_name"] or "").strip()
                  and len((n["hotel_name"] or "").strip()) >= 5]
    already = []          # hotel already in pipeline
    new_qualified = {}    # dedup_key -> representative story (would enqueue)
    new_unqualified = {}  # new hotel but not a property-event / out of region
    for n in with_hotel:
        key = _normalize_for_dedup(n["hotel_name"])
        if not key:
            continue
        if key in pipeline:
            already.append(n)
            continue
        cat = (n["category"] or "other")
        region = (n["region"] or "other")
        vert = (n["vertical"] or "hotel")
        qualifies = (vert == "hotel" and region in TARGET_REGIONS
                     and (n["luxury"] or cat in PROPERTY_EVENTS))
        bucket = new_qualified if qualifies else new_unqualified
        # keep the first (newest) story per distinct hotel
        if key not in bucket:
            bucket[key] = n

    p("\n" + "=" * 74)
    p("PHASE 0 -- NEWS -> ACTIONS DIAGNOSTIC (read-only, nothing written)")
    p("=" * 74)
    p(f"  stories in window        : {len(news)}   "
      f"(pipeline: {len(hotels)} hotels + {len(leads)} leads)")
    p(f"  stories naming a hotel   : {len(with_hotel)}")

    p("")
    p("(1) HOTEL ENQUEUE  -- would any story create a NEW lead?")
    p("-" * 74)
    p(f"  already in pipeline (skip)         : {len(already)} stories")
    p(f"  NEW hotel, NOT a target event/geo  : {len(new_unqualified)} distinct  (skip)")
    p(f"  NEW hotel, QUALIFIED to enqueue    : {len(new_qualified)} distinct  <-- new leads")
    p("       (qualified = vertical hotel · region usa/caribbean · luxury or a")
    p("        property event: opening/appointment/mgmt-change/reno/acq/rebrand)")

    if new_qualified:
        p("")
        p(f"  Would-enqueue leads (top {args.top}):")
        rows = sorted(new_qualified.values(),
                      key=lambda n: (not n["luxury"], n["category"] or "z"))
        for n in rows[: args.top]:
            lux = "★" if n["luxury"] else " "
            p(f"   {lux} [{(n['category'] or 'other'):<16}] "
              f"{(n['hotel_name'] or '')[:44]:<44} "
              f"{(n['region'] or ''):<9} · {(n['source'] or '')[:20]}")

    if new_unqualified:
        p("")
        p(f"  Skipped new hotels (sample {min(args.top, len(new_unqualified))}) "
          f"-- eyeball for good ones the gate is dropping:")
        for n in list(new_unqualified.values())[: args.top]:
            p(f"     [{(n['category'] or 'other'):<16}] {(n['hotel_name'] or '')[:40]:<40} "
              f"region={n['region']} vert={n['vertical']} lux={bool(n['luxury'])}")

    # ---- PERSON FLAG analysis (from stored relationship_hits) ------------
    flagged = [n for n in news if n["relationship_hits"]]
    by_cat = Counter((n["category"] or "other") for n in flagged)
    strength = Counter()
    examples = []
    for n in flagged:
        hits = n["relationship_hits"] or []
        for h in hits:
            strength[h.get("strength", "?")] += 1
        top = hits[0] if hits else {}
        examples.append((
            n["person_name"], n["person_title"], n["hotel_name"],
            top.get("strength"), top.get("account") or top.get("organization"),
            n["category"],
        ))

    p("")
    p("(2) PERSON FLAGS  -- appointments naming someone we already know")
    p("-" * 74)
    p(f"  stories with a known-person hit    : {len(flagged)}")
    p("  by story type                      : "
      + ", ".join(f"{k}={v}" for k, v in by_cat.most_common()))
    p("  by match strength                  : "
      + ", ".join(f"{k}={v}" for k, v in strength.most_common()))
    if examples:
        p("")
        p(f"  Would-review flags (top {args.top}):")
        for person, title, hotel, strn, known, cat in examples[: args.top]:
            p(f"   · {(person or '?')[:22]:<22} — {(title or 'role?')[:22]:<22} "
              f"@ {(hotel or '?')[:24]:<24}  [{strn or '?'}: known from "
              f"{(known or '?')[:22]}]  ({cat})")

    p("")
    p("=" * 74)
    p(f"SUMMARY: {len(new_qualified)} new leads would be enqueued for smart-fill · "
      f"{len(flagged)} person review flags")
    p("Nothing was written.  --csv to dump the full candidate lists.")
    p("=" * 74 + "\n")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["action", "hotel_or_person", "category", "region",
                        "luxury", "detail", "url"])
            for n in new_qualified.values():
                w.writerow(["enqueue_hotel", n["hotel_name"], n["category"],
                            n["region"], bool(n["luxury"]), n["source"], n["url"]])
            for person, title, hotel, strn, known, cat in examples:
                w.writerow(["review_person", person, cat, "", "",
                            f"{title} @ {hotel} — known from {known} ({strn})", ""])
        p(f"  wrote {args.csv}")


async def main():
    ap = argparse.ArgumentParser(description="Phase 0 news->actions diagnostic (read-only)")
    ap.add_argument("--days", type=int, default=30, help="lookback window over hotel_news")
    ap.add_argument("--top", type=int, default=25, help="rows per list")
    ap.add_argument("--csv", type=str, default=None, help="dump candidate lists to CSV")
    args = ap.parse_args()

    print("Loading hotel_news + pipeline (read-only)...", flush=True)
    news, hotels, leads = await fetch(args.days)
    run_report(news, hotels, leads, args)


if __name__ == "__main__":
    asyncio.run(main())
