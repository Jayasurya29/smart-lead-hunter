"""
scripts/dedup_contacts.py
=========================
Group duplicate PEOPLE in the `contacts` table under a shared person_id, so the
same human stops appearing as two rows when they have two addresses — most
importantly the former-employer / current-employer split that the
secondary_email feature creates (we store the found new-employer address as
secondary on the old row; the moment they email from the new address a brand-new
row is created with no link back).

WHAT IT DOES
------------
Fuzzy-matches contacts that are the same person and assigns them a shared
`person_id` grouping key (migration 042). It NEVER deletes a row and never
overwrites any field — person_id is an additive, reversible tag. `--reset`
clears it. The primary email still anchors the thread history on each row.

MATCH TIERS
-----------
  STRONG     same email — PRIMARY or SECONDARY (auto-safe; this is the exact
             former/current-employer link)
  HIGH       same org + name similarity >= 90            (auto-safe)
  REVIEW     same org + name similarity in [min, 90)     (eyeball these)

Existing person_id groups are preserved and extended (transitive union), so
re-running never fragments earlier work. Dry-run by DEFAULT — review the
proposals, then re-run with --apply (exactly how dedup_persons was used).

USAGE (run from repo root)
--------------------------
  python scripts\\dedup_contacts.py                 # dry-run, show all proposals
  python scripts\\dedup_contacts.py --min-ratio 88  # stricter fuzzy threshold
  python scripts\\dedup_contacts.py --strong-only   # email matches only (safest)
  python scripts\\dedup_contacts.py --apply          # write person_id
  python scripts\\dedup_contacts.py --apply --strong-only   # write only email matches
  python scripts\\dedup_contacts.py --exclude 12,34 # leave specific ids OUT
  python scripts\\dedup_contacts.py --reset          # clear ALL person_id (undo)
  python scripts\\dedup_contacts.py --selftest       # offline logic check, no DB
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from rapidfuzz import fuzz  # noqa: E402

HIGH = 90  # >= this on same-org name match is auto-safe

_TITLE_RE = re.compile(r"\b(mr|mrs|ms|dr|prof|sir|madam)\.?\b")
_SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|phd|md|cpa)\b")
_ORG_SUFFIX_RE = re.compile(r"\b(llc|inc|corp|co|ltd|lp|llp|group|holdings|hotels|hospitality)\b")


def _strip_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def norm_name(s: str | None) -> str:
    if not s:
        return ""
    s = _strip_accents(s).lower()
    s = _TITLE_RE.sub(" ", s)
    s = _SUFFIX_RE.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def norm_org(s: str | None) -> str:
    if not s:
        return ""
    s = _strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = _ORG_SUFFIX_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def norm_email(s: str | None) -> str:
    return (s or "").strip().lower()


class UF:
    """Union-find with a min-id canonical."""

    def __init__(self):
        self.parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            lo, hi = (ra, rb) if ra < rb else (rb, ra)
            self.parent[hi] = lo  # canonical = smaller id


def build_groups(rows: list[dict], min_ratio: float, strong_only: bool):
    """rows: list of {id,name,email,secondary_email,organization,person_id}.
    Returns (uf, link_reason) where link_reason[(a,b)] = (tier, ratio)."""
    uf = UF()
    for r in rows:
        uf.find(r["id"])

    # seed existing person_id groups so prior work is preserved/extended
    by_pid: dict[int, list[int]] = defaultdict(list)
    for r in rows:
        if r.get("person_id") is not None:
            by_pid[r["person_id"]].append(r["id"])
    for ids in by_pid.values():
        for i in ids[1:]:
            uf.union(ids[0], i)

    link: dict[tuple, tuple] = {}

    # STRONG: same email (primary or secondary), regardless of org. This is the
    # former/current-employer link — one row's email == another's secondary.
    email_buckets: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        for e in (norm_email(r.get("email")), norm_email(r.get("secondary_email"))):
            if e and "@" in e:
                email_buckets[e].append(r["id"])
    for ids in email_buckets.values():
        u = sorted(set(ids))
        for i in u[1:]:
            uf.union(u[0], i)
            link[(u[0], i)] = ("STRONG", 100.0)

    if strong_only:
        return uf, link

    # HIGH/REVIEW: same normalized org + fuzzy name
    org_buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        org = norm_org(r.get("organization"))
        if org:
            org_buckets[org].append(r)
    for members in org_buckets.values():
        n = len(members)
        for i in range(n):
            ni = norm_name(members[i]["name"])
            if not ni:
                continue
            for j in range(i + 1, n):
                nj = norm_name(members[j]["name"])
                if not nj:
                    continue
                ratio = fuzz.token_sort_ratio(ni, nj)
                if ratio >= min_ratio:
                    a, b = members[i]["id"], members[j]["id"]
                    lo, hi = (a, b) if a < b else (b, a)
                    uf.union(lo, hi)
                    tier = "HIGH" if ratio >= HIGH else "REVIEW"
                    link[(lo, hi)] = (tier, round(ratio, 1))
    return uf, link


def components(uf: UF, rows: list[dict]) -> dict[int, list[dict]]:
    by_id = {r["id"]: r for r in rows}
    comp: dict[int, list[dict]] = defaultdict(list)
    for rid in by_id:
        comp[uf.find(rid)].append(by_id[rid])
    return {root: members for root, members in comp.items() if len(members) > 1}


# ─────────────────────────── DB I/O ────────────────────────────


def _compose_name(r: dict) -> str:
    dn = (r.get("display_name") or "").strip()
    if dn:
        return dn
    fn = (r.get("first_name") or "").strip()
    ln = (r.get("last_name") or "").strip()
    return f"{fn} {ln}".strip()


async def _load_rows() -> list[dict]:
    from sqlalchemy import text  # noqa: E402
    from app.database import async_session  # noqa: E402

    async with async_session() as s:
        res = await s.execute(
            text(
                "SELECT id, first_name, last_name, display_name, email, "
                "secondary_email, organization, person_id FROM contacts"
            )
        )
        out = []
        for m in res.mappings().all():
            d = dict(m)
            d["name"] = _compose_name(d)
            out.append(d)
        return out


async def _apply(uf: UF, comps: dict[int, list[dict]]) -> int:
    from sqlalchemy import text  # noqa: E402
    from app.database import async_session  # noqa: E402

    n = 0
    async with async_session() as s:
        for root, members in comps.items():
            ids = [int(m["id"]) for m in members]
            await s.execute(
                text("UPDATE contacts SET person_id = :root WHERE id = ANY(:ids)"),
                {"root": int(root), "ids": ids},
            )
            n += len(ids)
        await s.commit()
    return n


async def _reset() -> int:
    from sqlalchemy import text  # noqa: E402
    from app.database import async_session  # noqa: E402

    async with async_session() as s:
        res = await s.execute(
            text("UPDATE contacts SET person_id = NULL WHERE person_id IS NOT NULL")
        )
        await s.commit()
        return res.rowcount or 0


def _print_proposals(comps, link, min_ratio):
    if not comps:
        print("No duplicate people found at the current threshold.")
        return
    tier_counts: dict[str, int] = defaultdict(int)
    print(f"\nProposed merges (min fuzzy ratio = {min_ratio}):\n")
    for root in sorted(comps):
        members = sorted(comps[root], key=lambda m: m["id"])
        tiers = [
            link.get((min(a["id"], b["id"]), max(a["id"], b["id"])), (None, None))[0]
            for a in members
            for b in members
            if a["id"] < b["id"]
        ]
        tiers = [t for t in tiers if t]
        if "REVIEW" in tiers:
            gtier = "REVIEW"
        elif "HIGH" in tiers:
            gtier = "HIGH"
        elif "STRONG" in tiers:
            gtier = "STRONG"
        else:
            gtier = "PRESET"  # only a pre-existing person_id link — nothing new
        tier_counts[gtier] += 1
        print(f"  [{gtier}] person_id -> {root}  ({len(members)} rows)")
        for m in members:
            em = m.get("email") or m.get("secondary_email") or "—"
            org = m.get("organization") or "—"
            print(f"      #{m['id']:<7} {m['name'] or '—':<28} {em:<32} {org}")
        print()
    print("Summary: " + ", ".join(f"{k}={v}" for k, v in sorted(tier_counts.items())))
    print(f"Groups: {len(comps)}  |  rows affected: {sum(len(v) for v in comps.values())}")
    print("\nReview above, then:  python scripts\\dedup_contacts.py --apply")


# ─────────────────────────── selftest ──────────────────────────


def _selftest() -> int:
    # The matching engine is shared with dedup_persons; this proves the
    # contacts-shaped rows group the way we expect, incl. the secondary-email
    # former/current-employer link.
    rows = [
        {"id": 1, "name": "Robert De Niro", "email": "", "secondary_email": "", "organization": "Nobu Hospitality", "person_id": None},
        {"id": 2, "name": "Robert Di Nero", "email": "", "secondary_email": "", "organization": "Nobu Hospitality", "person_id": None},
        {"id": 3, "name": "Robert Redford", "email": "", "secondary_email": "", "organization": "Nobu Hospitality", "person_id": None},
        {"id": 4, "name": "Brendan Payze", "email": "brendan.payze@ritzcarlton.com", "secondary_email": "", "organization": "Ritz-Carlton Grand Cayman", "person_id": None},
        {"id": 5, "name": "Brendan Payze", "email": "bpayze@stregis.com", "secondary_email": "", "organization": "St. Regis", "person_id": None},
        {"id": 6, "name": "Nobu Matsuhisa", "email": "", "secondary_email": "", "organization": "Nobu Hospitality", "person_id": None},
        {"id": 7, "name": "Nobuyuki Matsuhisa", "email": "", "secondary_email": "", "organization": "Nobu Hospitality", "person_id": None},
    ]
    # Seed the former/current link the way the app stores it: the OLD row carries
    # the new address as secondary_email.
    rows[3]["secondary_email"] = "bpayze@stregis.com"
    uf, link = build_groups(rows, min_ratio=85, strong_only=False)
    comps = components(uf, rows)
    groups = {root: sorted(m["id"] for m in members) for root, members in comps.items()}
    flat = {frozenset(g) for g in groups.values()}
    ok = True

    def has(*ids):
        return frozenset(ids) in flat

    checks = [
        ("De Niro/Di Nero merge", has(1, 2)),
        ("Redford NOT merged with De Niro", not any(3 in g and 1 in g for g in flat)),
        ("Former/current employer merged via secondary_email", has(4, 5)),
        ("Nobu/Nobuyuki Matsuhisa merge", has(6, 7)),
    ]
    for label, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
        ok = ok and passed
    print("\nGroups:", groups)
    return 0 if ok else 1


async def _amain(args) -> int:
    if args.reset:
        n = await _reset()
        print(f"Reset: cleared person_id on {n} rows.")
        return 0
    rows = await _load_rows()
    print(f"Loaded {len(rows)} contacts.")
    excluded = {int(x) for x in args.exclude.split(",") if x.strip().isdigit()}
    if excluded:
        rows = [r for r in rows if r["id"] not in excluded]
        print(f"Excluding {len(excluded)} id(s) from matching: {sorted(excluded)}")
    uf, link = build_groups(rows, args.min_ratio, args.strong_only)
    comps = components(uf, rows)
    if args.apply:
        n = await _apply(uf, comps)
        print(f"Applied: grouped {n} rows into {len(comps)} person identities.")
        print("Next: collapse the Contacts list by person_id so the page shows one row per person.")
        return 0
    _print_proposals(comps, link, args.min_ratio)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Fuzzy person-dedup for the contacts table.")
    ap.add_argument("--apply", action="store_true", help="write person_id (default is dry-run)")
    ap.add_argument("--reset", action="store_true", help="clear ALL person_id (undo)")
    ap.add_argument("--strong-only", action="store_true", help="email matches only (safest)")
    ap.add_argument("--min-ratio", type=float, default=85.0, help="fuzzy name threshold (default 85)")
    ap.add_argument("--exclude", type=str, default="", help="comma-separated contact ids to leave OUT of matching")
    ap.add_argument("--selftest", action="store_true", help="offline logic check, no DB")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())
