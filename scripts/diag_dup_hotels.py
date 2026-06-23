#!/usr/bin/env python3
"""diag_dup_hotels.py -- READ-ONLY. Find duplicate existing_hotels rows.

Surfaces same-property rows split across two ids (e.g. 2990 "Vessence
Barbados" vs 3241 "Royalton Vessence Barbados"; 2991 vs 3037 Pyrmont). Two
high-precision signals, both property-specific (parent-operator domains like
marriott.com are skipped so real siblings don't cluster):

  WEB : two rows share the same property website host (not a parent domain)
  NAME: two rows in the same (city, state) with name similarity >= --thresh

Rows already linked via duplicate_of_id are skipped. No writes -- it only
prints clusters + a ready-to-edit UPDATE for each (you choose the keeper).

Usage (repo root, venv, DATABASE_URL set):
    python scripts/diag_dup_hotels.py
    python scripts/diag_dup_hotels.py --thresh 88 --limit 80
"""

import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402

# Parent / operator / aggregator domains that legitimately span many distinct
# properties -- never a dup signal on their own.
PARENT_DOMAINS = {
    "marriott.com", "hilton.com", "hyatt.com", "ihg.com", "choicehotels.com",
    "wyndham.com", "accor.com", "bookings.com", "booking.com", "expedia.com",
    "loews.com", "omnihotels.com", "fourseasons.com", "marriotthotels.com",
    "kimptonhotels.com", "autograph-hotels.marriott.com", "hilton.com",
}


def _url_key(url: str | None) -> str | None:
    """Normalized host+path identity for a property URL. Each brand property
    has a unique slug (ritzcarlton.com/.../miasb-... vs .../fllrz-...), so
    host+path never collides across distinct properties -- unlike a bare host.
    Bare-host (no path) is kept ONLY for non-parent property sites, so a real
    shared site like thepyrmontcuracao.com still links its two rows."""
    if not url:
        return None
    h = url.strip().lower()
    for p in ("https://", "http://"):
        if h.startswith(p):
            h = h[len(p):]
    if h.startswith("www."):
        h = h[4:]
    h = h.split("?")[0].split("#")[0].rstrip("/")
    for suf in ("/overview", "/index.html", "/index.do", "/default.htm", "/en", "/en-us"):
        if h.endswith(suf):
            h = h[: -len(suf)].rstrip("/")
    host = h.split("/")[0]
    if "/" not in h and host in PARENT_DOMAINS:
        return None  # bare brand host ("marriott.com") is not a property identity
    return h or None


import unicodedata  # noqa: E402

_LEGAL = {
    "the", "a", "an", "inc", "incorporated", "llc", "ltd", "limited", "co",
    "corp", "corporation", "company", "and",
}


def _name_key(name: str | None) -> str | None:
    """Exact-identity key: fold accents, drop punctuation + legal words,
    collapse ALL spacing. 'J.W. Marriott Indianapolis' == 'JW Marriott
    Indianapolis'; 'Hotel California' != 'California Hotel' (different content)."""
    if not name:
        return None
    s = unicodedata.normalize("NFKD", str(name).lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = "".join(c if c.isalnum() else " " for c in s)
    toks = [t for t in s.split() if t not in _LEGAL]
    return "".join(toks) or None


def cluster(rows: list[dict]) -> list[list[dict]]:
    """Union-find over WEB and NAME edges."""
    parent = {r["id"]: r["id"] for r in rows}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # WEB edges: same full host+path (a true property identity)
    by_host: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        h = _url_key(r.get("hotel_website"))
        if h:
            by_host[h].append(r["id"])
    for ids in by_host.values():
        for i in ids[1:]:
            union(ids[0], i)

    # NAME edges: same (city,state) + IDENTICAL normalized name (case /
    # punctuation / spacing / trailing-suffix variants of the SAME property,
    # e.g. "Marriott Hotels" vs "MARRIOTT HOTELS", "J.W. Marriott" vs "JW
    # Marriott"). No token overlap, no fuzzy -- exact equality only, so distinct
    # same-city properties never merge.
    by_loc: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        key = ((r.get("city") or "").strip().lower(), (r.get("state") or "").strip().lower())
        by_loc[key].append(r)
    for key, group in by_loc.items():
        if key == ("", "") or len(group) < 2:
            continue
        by_name: dict[str, list[int]] = defaultdict(list)
        for r in group:
            nk = _name_key(r.get("hotel_name"))
            if nk:
                by_name[nk].append(r["id"])
        for ids in by_name.values():
            for i in ids[1:]:
                union(ids[0], i)

    groups: dict[int, list[dict]] = defaultdict(list)
    by_id = {r["id"]: r for r in rows}
    for rid in parent:
        groups[find(rid)].append(by_id[rid])
    return [g for g in groups.values() if len(g) >= 2]


async def main(args: argparse.Namespace) -> int:
    async with async_session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT id, hotel_name, hotel_website, city, state, "
                    "duplicate_of_id FROM existing_hotels "
                    "WHERE COALESCE(hotel_name,'') <> ''"
                )
            )
        ).mappings().all()
    rows = [dict(r) for r in rows if r["duplicate_of_id"] is None]

    clusters = cluster(rows)
    clusters.sort(key=len, reverse=True)

    print(f"\nScanned {len(rows)} unlinked hotel rows -> {len(clusters)} duplicate clusters.\n")
    for g in clusters[: args.limit]:
        g = sorted(g, key=lambda r: r["id"])
        keep = g[0]["id"]  # lowest id as suggested keeper
        print(f"CLUSTER (keep {keep}?):")
        for r in g:
            print(f"    id={r['id']:<6} {r['hotel_name']}  [{r.get('city') or '?'}, {r.get('state') or '?'}]  {r.get('hotel_website') or ''}")
        dups = ",".join(str(r["id"]) for r in g if r["id"] != keep)
        print(f"    -> UPDATE existing_hotels SET duplicate_of_id={keep}, updated_at=NOW() WHERE id IN ({dups});\n")
    if len(clusters) > args.limit:
        print(f"... {len(clusters) - args.limit} more (raise --limit).")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=60)
    sys.exit(asyncio.run(main(ap.parse_args())))
