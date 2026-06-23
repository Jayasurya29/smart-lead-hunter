#!/usr/bin/env python3
"""diag_account_keys.py -- READ-ONLY. Surface sibling-property over-merges.

The Contacts page groups accounts with a frontend key (accountKey in
ContactsPage.tsx): lowercase, fold accents, alias-map, strip legal suffixes,
singularize, collapse spaces. It is intentionally conservative -- but it can
only use the org STRING. When two different siblings carry the same string
(e.g. several "Hotel Monaco" rows with no city), they collapse into ONE
bucket even though they're distinct properties.

This script ports accountKey 1:1 and flags every key whose bucket spans more
than one real property -- i.e. >=2 distinct matched_hotel_id OR >=2 distinct
non-freemail email domains. Those are the Phase-D disambig candidates: real
data, not hypotheticals, so the fix can be targeted (alias split, a city
qualifier, or a per-domain sub-key).

No writes. Usage (repo root, venv active, DATABASE_URL set):
    python scripts/diag_account_keys.py
    python scripts/diag_account_keys.py --min-domains 2 --limit 60
    python scripts/diag_account_keys.py --include-leads
"""

import argparse
import asyncio
import re
import sys
import unicodedata
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

FREEMAIL = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com",
    "icloud.com", "me.com", "live.com", "msn.com", "comcast.net",
    "bellsouth.net", "att.net", "verizon.net", "sbcglobal.net",
}

# ── accountKey port (mirror frontend/src/pages/ContactsPage.tsx) ──────────
COMPANY_ALIASES = {
    "blue diamond resorts": "Royalton Hotels & Resorts",
    "curacao marriott and the pyrmont curacao": "The Pyrmont Curacao",
    "curacao marriott beach resort | the pyrmont curacao beach resort, "
    "an autograph collection all-inclusive": "The Pyrmont Curacao",
    "the pyrmont curacao an autograph collection hotel by marriott": "The Pyrmont Curacao",
    "the pyrmont curacao, an autograph collection all-inclusive resort": "The Pyrmont Curacao",
    "the pyrmontcuracao": "The Pyrmont Curacao",
}
_LEADING_THE = re.compile(r"^the\s+", re.I)
_ACCOUNT_SUFFIX = re.compile(
    r"\s+(?:&\s*co|and\s+co|co|company|inc|incorporated|llc|l\.l\.c|ltd|limited|"
    r"corp|corporation|group|holdings|management|mgmt|hospitality|properties|"
    r"enterprises|international|intl)\.?$",
    re.I,
)
_AND_PHRASE = re.compile(
    r"\s+(?:&|and)\s+(?:resorts?|suites?|spa|spas|villas?|residences?|hotels?|clubs?)$",
    re.I,
)
_PUNCT = re.compile(r"[.,'\"&]+")
_PLURAL = re.compile(r"\b(hotels|resorts|suites|inns|villas|clubs)\b")


def _fold(s: str) -> str:
    # strip combining marks -> ASCII letters (JS: NFKD + drop U+0300-036F)
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def account_key(name: str | None) -> str:
    if not name:
        return "No organization"
    s = _fold(str(name).strip().lower())
    if s in COMPANY_ALIASES:
        s = COMPANY_ALIASES[s].lower()
    s = _LEADING_THE.sub("", s)
    prev = None
    while prev != s:
        prev = s
        s = _ACCOUNT_SUFFIX.sub("", s).strip()
    s = _AND_PHRASE.sub("", s).strip()
    s = re.sub(r"\s+", " ", _PUNCT.sub(" ", s)).strip()
    s = _PLURAL.sub(lambda m: m.group(0)[:-1], s)
    s = re.sub(r"\s+", "", s)
    return s or "No organization"


def _domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    d = email.split("@")[-1].strip().lower()
    return d or None


async def main(args: argparse.Namespace) -> int:
    rows: list[dict] = []
    async with async_session() as s:
        cr = (
            await s.execute(
                text(
                    "SELECT organization, email, matched_hotel_id, matched_lead_id "
                    "FROM contacts WHERE organization IS NOT NULL AND organization <> '' "
                    "AND COALESCE(contact_category,'') <> 'junk'"
                )
            )
        ).mappings().all()
        rows.extend(dict(r) for r in cr)
        if args.include_leads:
            lr = (
                await s.execute(
                    text(
                        "SELECT organization, email, existing_hotel_id AS matched_hotel_id, "
                        "lead_id AS matched_lead_id FROM lead_contacts "
                        "WHERE organization IS NOT NULL AND organization <> ''"
                    )
                )
            ).mappings().all()
            rows.extend(dict(r) for r in lr)

    buckets: dict[str, dict] = defaultdict(
        lambda: {"orgs": defaultdict(int), "domains": set(), "hotels": set(), "leads": set(), "n": 0}
    )
    for r in rows:
        k = account_key(r["organization"])
        b = buckets[k]
        b["n"] += 1
        b["orgs"][r["organization"]] += 1
        d = _domain(r.get("email"))
        if d and d not in FREEMAIL:
            b["domains"].add(d)
        if r.get("matched_hotel_id"):
            b["hotels"].add(r["matched_hotel_id"])
        if r.get("matched_lead_id"):
            b["leads"].add(r["matched_lead_id"])

    flagged = []
    for k, b in buckets.items():
        multi_hotel = len(b["hotels"]) >= 2
        multi_domain = len(b["domains"]) >= args.min_domains and args.min_domains >= 2
        if multi_hotel or multi_domain:
            flagged.append((k, b, multi_hotel, multi_domain))

    # worst first: most distinct hotels, then most distinct domains
    flagged.sort(key=lambda t: (len(t[1]["hotels"]), len(t[1]["domains"]), t[1]["n"]), reverse=True)

    print(f"\nScanned {len(rows)} contact rows -> {len(buckets)} account keys.")
    print(f"Over-merge candidates (one key spanning multiple properties): {len(flagged)}\n")
    for k, b, mh, md in flagged[: args.limit]:
        why = []
        if mh:
            why.append(f"{len(b['hotels'])} hotel_ids: {sorted(b['hotels'])[:8]}")
        if md:
            why.append(f"{len(b['domains'])} domains: {sorted(b['domains'])[:8]}")
        print(f"KEY '{k}'  ({b['n']} rows)  -- {'; '.join(why)}")
        for org, n in sorted(b["orgs"].items(), key=lambda x: -x[1]):
            print(f"    {n:>4}x  {org}")
        print()
    if len(flagged) > args.limit:
        print(f"... {len(flagged) - args.limit} more (raise --limit).")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=40, help="max keys to print")
    ap.add_argument("--min-domains", type=int, default=2,
                    help="flag a key when it spans >= this many distinct operator domains (>=2)")
    ap.add_argument("--include-leads", action="store_true",
                    help="also scan lead_contacts (default: inbox contacts only)")
    sys.exit(asyncio.run(main(ap.parse_args())))
