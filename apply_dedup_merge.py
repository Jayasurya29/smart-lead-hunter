"""
apply_dedup_merge.py
====================
One-time merge of the duplicate pipeline-lead groups surfaced by
dryrun_dedup_stregis.py. PREVIEW by default; writes only with --apply.

Strategy per group:
  1. Survivor = LOWEST id (oldest row; most likely to carry linked
     lead_contacts and accumulated source history). In the St. Regis case
     the lowest id also happens to hold the best location/year data.
  2. Field-merge every loser onto the survivor using the repo's own
     enrich_existing_lead() (non-empty wins, longer description wins,
     source_urls / source_extractions unioned, timeline_label recomputed).
  3. opening_year is set to the MODE of non-null years in the group
     (so 1209's lone 2026 can't beat the three 2027s).
  4. hotel_name is set to the cleanest variant (fewest chars, '&' spellings
     deprioritized) so the survivor isn't stuck with "The ... Resort at ...".
  5. lead_contacts.lead_id is re-pointed from each loser to the survivor.
  6. Losers are SOFT-deleted: status='deleted' (hidden by the pipeline list
     filter on line ~358 of routes/leads.py) + duplicate_of_id=<survivor>.
     No hard row delete — fully recoverable.

Safety:
  * --apply required to write. Default run only prints the plan + merged
    field preview.
  * Before any write, all affected rows are dumped to
    dedup_merge_backup_<timestamp>.json.
  * All writes happen in a single transaction; any error rolls back.

Run from repo root (venv active, DATABASE_URL set):
    python apply_dedup_merge.py --like "%st%regis%"            # preview
    python apply_dedup_merge.py --like "%st%regis%" --apply     # commit
    python apply_dedup_merge.py                                 # all leads (preview)
"""

import re
import sys
import json
import asyncio
from collections import defaultdict, Counter
from datetime import datetime

from sqlalchemy import select, update

from app.database import async_session
from app.models.potential_lead import PotentialLead
from app.models.lead_contact import LeadContact
from app.services.lead_factory import (
    _normalize_for_dedup,
    _strip_location_words,
    _names_match,
    _SMALL_COUNTRIES,
    enrich_existing_lead,
)
from app.services.utils import get_timeline_label

PIPELINE_STATUSES_EXCLUDED = {"expired", "rejected", "approved", "pushed", "deleted"}

MERGE_FIELDS = (
    "hotel_name", "brand", "city", "state", "country",
    "opening_date", "room_count", "contact_name", "contact_title",
    "contact_email", "contact_phone", "key_insights", "description",
    "management_company", "developer", "owner", "source_url",
)


def _fold_value(s: str) -> str:
    if not s:
        return ""
    s = s.lower().replace("&", "and").replace("'", "").replace("\u2019", "")
    return re.sub(r"\s+", " ", s).strip()


_GUARD_GENERIC = frozenset({
    "hotel","hotels","resort","resorts","spa","suites","suite","inn","lodge",
    "club","collection","residences","residence","the","and","at","of","by","a","an",
})


def _disjoint_identifiers(core_a, core_b):
    """Block bare-brand false merges: True when each name keeps a distinctive
    (>=4 char, non-generic) identifier the other lacks. Same guard the
    write-time patch installs."""
    if not core_a or not core_b:
        return False
    sa, sb = set(core_a.split()), set(core_b.split())
    shared = sa & sb
    a_only = {w for w in (sa - shared) if len(w) >= 4 and w not in _GUARD_GENERIC}
    b_only = {w for w in (sb - shared) if len(w) >= 4 and w not in _GUARD_GENERIC}
    return bool(a_only) and bool(b_only) and a_only.isdisjoint(b_only)



def _overlap(a: str, b: str) -> bool:
    a, b = _fold_value(a), _fold_value(b)
    return bool(a) and bool(b) and (a in b or b in a)


def _pool_overlap(r1, r2) -> bool:
    if _overlap(r1.state, r2.state):
        return True
    if _overlap(r1.city, r2.city):
        return True
    for src, other in ((r1, r2), (r2, r1)):
        co = _fold_value(src.country)
        if co and (not _fold_value(src.state) or co in _SMALL_COUNTRIES):
            if _overlap(src.country, other.country):
                return True
    return False


def _match(r1, r2) -> bool:
    c1 = _normalize_for_dedup(r1.hotel_name or "")
    c2 = _normalize_for_dedup(r2.hotel_name or "")
    if len(c1) <= 3 or len(c2) <= 3:
        return False
    if _disjoint_identifiers(c1, c2):
        return False
    locs = (r1.city, r1.state, r1.country, r2.city, r2.state, r2.country)
    return _names_match(
        _strip_location_words(c1, *locs), _strip_location_words(c2, *locs)
    )


def _canonical_name(group) -> str:
    """Cleanest name: fewest chars, '&' spellings sorted last."""
    names = [g.hotel_name for g in group if g.hotel_name]
    return min(names, key=lambda n: ("&" in n, len(n))) if names else ""


def _mode_year(group):
    years = [g.opening_year for g in group if g.opening_year]
    if not years:
        return None
    return Counter(years).most_common(1)[0][0]


def _loser_dict(loser) -> dict:
    """Build the lead_dict shape enrich_existing_lead expects."""
    return {f: getattr(loser, f, None) for f in MERGE_FIELDS}


async def main(name_like, do_apply):
    async with async_session() as db:
        q = select(PotentialLead).where(
            PotentialLead.status.notin_(list(PIPELINE_STATUSES_EXCLUDED))
        )
        if name_like:
            q = q.where(PotentialLead.hotel_name.ilike(name_like))
        rows = list((await db.execute(q)).scalars().all())

        n = len(rows)
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for i in range(n):
            for j in range(i + 1, n):
                if _pool_overlap(rows[i], rows[j]) and _match(rows[i], rows[j]):
                    parent[find(i)] = find(j)

        groups = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(rows[i])
        dupe_groups = [g for g in groups.values() if len(g) > 1]

        if not dupe_groups:
            print("No duplicate groups found.")
            return

        mode = "APPLY (writing)" if do_apply else "PREVIEW (no writes)"
        print(f"=== {mode} — {len(dupe_groups)} group(s) ===\n")

        backup = []
        plans = []
        for g in dupe_groups:
            g_sorted = sorted(g, key=lambda r: r.id)
            survivor = g_sorted[0]
            losers = g_sorted[1:]
            canonical = _canonical_name(g_sorted)
            year = _mode_year(g_sorted)
            plans.append((survivor, losers, canonical, year))

            backup.append({
                "survivor_id": survivor.id,
                "rows": [
                    {f: getattr(r, f, None) for f in (
                        "id", "hotel_name", "city", "state", "country",
                        "opening_year", "opening_date", "status",
                        "duplicate_of_id", "source_urls",
                    )}
                    for r in g_sorted
                ],
            })

            print(f"SURVIVOR id={survivor.id}: {survivor.hotel_name}")
            print(f"   final name : {canonical}")
            print(f"   final year : {year}  (was {survivor.opening_year})")
            print(f"   city/state : {survivor.city} / {survivor.state} / {survivor.country}")
            for ld in losers:
                print(f"   merge <- id={ld.id}: {ld.hotel_name} "
                      f"({ld.city}, {ld.state}, {ld.country}, {ld.opening_year}) "
                      f"=> status='deleted', duplicate_of_id={survivor.id}")
            print()

        if not do_apply:
            print("PREVIEW only. Re-run with --apply to commit.")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bpath = f"dedup_merge_backup_{ts}.json"
        with open(bpath, "w", encoding="utf-8") as f:
            json.dump(backup, f, indent=2, default=str)
        print(f"Backup written: {bpath}\n")

        for survivor, losers, canonical, year in plans:
            for ld in losers:
                enrich_existing_lead(survivor, _loser_dict(ld))

            if canonical:
                survivor.hotel_name = canonical
            if year:
                survivor.opening_year = year
            if survivor.opening_date:
                survivor.timeline_label = get_timeline_label(survivor.opening_date)

            loser_ids = [ld.id for ld in losers]
            # Re-point contacts to the survivor
            await db.execute(
                update(LeadContact)
                .where(LeadContact.lead_id.in_(loser_ids))
                .values(lead_id=survivor.id)
            )
            # Soft-delete losers
            await db.execute(
                update(PotentialLead)
                .where(PotentialLead.id.in_(loser_ids))
                .values(status="deleted", duplicate_of_id=survivor.id)
            )
            print(f"   merged {loser_ids} -> {survivor.id}")

        await db.commit()
        print("\nCommitted. Recompute revenue/timeline on survivors if needed.")


if __name__ == "__main__":
    like = None
    if "--like" in sys.argv:
        like = sys.argv[sys.argv.index("--like") + 1]
    apply = "--apply" in sys.argv
    asyncio.run(main(like, apply))
