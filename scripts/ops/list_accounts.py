"""
list_accounts.py
================
Read-only. Lists every account (company) A-Z the way the Contacts page groups
them, so we can eyeball whether the grouping is right.

    python -m scripts.ops.list_accounts               # A-Z, all
    python -m scripts.ops.list_accounts --split-only   # only show split/suspect ones
    python -m scripts.ops.list_accounts --letter T     # just companies starting with T

For each grouping key it shows: the display name, contact count, distinct
domains under it, and distinct org spellings. When one key has several
spellings OR one domain shows up under several keys, that is a grouping
problem worth seeing.
"""

import argparse
import asyncio
import re
import sys
from collections import defaultdict

from sqlalchemy import text

from app.database import async_session

SHARED = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "live.com", "msn.com", "comcast.net",
    "att.net", "bellsouth.net", "163.com", "qq.com", "mail.gmail.com",
}

SQL = """
    SELECT NULLIF(TRIM(organization),'') AS org,
           lower(split_part(email,'@',2)) AS domain
    FROM contacts
    WHERE email LIKE '%@%'
      AND COALESCE(manual_category, contact_category,'') != 'junk'
      AND NULLIF(TRIM(organization),'') IS NOT NULL
"""


def account_key(name: str) -> str:
    """Mirrors the frontend accountKey: lowercase, strip suffixes/punct/space."""
    s = (name or "").strip().lower()
    s = re.sub(r"^the\s+", "", s)
    for _ in range(4):
        s = re.sub(r"\s+(inc|llc|llp|lp|ltd|co|corp|corporation|company|group|"
                   r"hotels?|resorts?|suites?|inns?|villas?|clubs?|collection)\.?$", "", s).strip()
    s = re.sub(r"[.,'\"&]+", " ", s)
    s = re.sub(r"\s+", "", s)
    return s or "(none)"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split-only", action="store_true", help="only suspect groupings")
    ap.add_argument("--letter", type=str, default=None, help="filter to one starting letter")
    args = ap.parse_args()

    async with async_session() as s:
        rows = (await s.execute(text(SQL))).all()

    # key -> {spellings: Counter, domains: set, count}
    keys = defaultdict(lambda: {"spell": defaultdict(int), "dom": set(), "n": 0})
    dom_to_keys = defaultdict(set)
    for r in rows:
        k = account_key(r.org)
        keys[k]["spell"][r.org] += 1
        keys[k]["n"] += 1
        if r.domain not in SHARED:
            keys[k]["dom"].add(r.domain)
            dom_to_keys[r.domain].add(k)

    # domains that appear under more than one key = a company split apart
    split_domains = {d: ks for d, ks in dom_to_keys.items() if len(ks) > 1}

    # display name per key = most common spelling
    def disp(k):
        return max(keys[k]["spell"].items(), key=lambda x: x[1])[0]

    entries = []
    for k, v in keys.items():
        name = disp(k)
        if args.letter and not name.lower().startswith(args.letter.lower()):
            continue
        spellings = len(v["spell"])
        domains = len(v["dom"])
        suspect = spellings > 1 or any(d in split_domains for d in v["dom"])
        if args.split_only and not suspect:
            continue
        entries.append((name, v["n"], domains, spellings, v, suspect))

    entries.sort(key=lambda e: e[0].lower())

    print(f"\nACCOUNTS A-Z  ({len(entries)} shown of {len(keys):,} total)\n" + "=" * 78)
    for name, n, domains, spellings, v, suspect in entries:
        flag = "  <-- SPLIT/SUSPECT" if suspect else ""
        print(f"\n{name}   ({n} contacts, {domains} domain(s)){flag}")
        if spellings > 1:
            sp = sorted(v["spell"].items(), key=lambda x: -x[1])
            print("     spellings: " + ", ".join(f"{s!r}×{c}" for s, c in sp[:6]))
        if domains > 1 or suspect:
            print("     domains: " + ", ".join(sorted(v["dom"])[:6])
                  + (" ..." if domains > 6 else ""))

    print("\n" + "=" * 78)
    print(f"  domains split across >1 account key: {len(split_domains)}")
    if split_domains and not args.letter:
        print("  worst — same domain, multiple company names:")
        for d, ks in sorted(split_domains.items(), key=lambda x: -len(x[1]))[:15]:
            print(f"     {d:<32} -> {', '.join(disp(k) for k in ks)}")
    print("\n  Read-only.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
