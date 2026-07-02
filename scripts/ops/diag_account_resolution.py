"""
diag_account_resolution.py  --  PHASE 0 (read-only, ZERO writes)
================================================================
Before we add an `accounts` table, this proves what the canonical account map
would actually look like across the REAL data -- so we see the merges, the
splits, and the gap *before* committing any schema.

It does NOT create any table, column, or row. Pure SELECT + in-memory
resolution. Reuses production logic so it can't drift from the real app:
  - app.services.org_normalize.normalize_organization  (the grouping key)
  - app.services.client_resolver.is_vendor / is_competitor / is_personal

WHAT IT SIMULATES
-----------------
For every person row (contacts + lead_contacts) it picks the account it WOULD
resolve to, in this order of truth:
  1. matched_hotel_id      -> existing_hotels  (already canonical: LINK)
  2. matched_lead_id       -> potential_leads  (already canonical: LINK)
  3. known operator domain -> management_company account            (NEW)
  4. SAP customer match    -> sap_clients       (canonical buyer:  LINK)
  5. is_vendor()           -> vendor account    (flagged seller:   NEW)
  6. organization          -> org account by normalized_key        (NEW)
  7. work email domain     -> domain account (weak)                (NEW)
  8. otherwise             -> ORPHAN (freemail + no org, etc.)

Effective junk/personal (COALESCE(manual_category, contact_category)) are NOT
turned into accounts -- counted separately.

REPORTS
-------
  * coverage: how many people LINK to an existing canonical row vs need a NEW
    account vs orphan vs excluded
  * NEW accounts by type and by vertical (this is THE GAP -- orgs with no row)
  * the biggest accounts (eyeball the merges)
  * resolution-source breakdown
  * RISK: over-merge (one account spans many domains -- bare-brand collisions),
          under-merge (one domain split across many accounts -- should merge)

USAGE (run from repo root, venv active, DATABASE_URL set)
--------------------------------------------------------
  python scripts/ops/diag_account_resolution.py
  python scripts/ops/diag_account_resolution.py --top 40
  python scripts/ops/diag_account_resolution.py --limit 5000      # quick sample
  python scripts/ops/diag_account_resolution.py --csv accounts_preview.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sys
from collections import Counter, defaultdict
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
from app.services.org_normalize import normalize_organization  # noqa: E402

try:
    from app.services.client_resolver import is_vendor  # noqa: E402
except Exception:  # pragma: no cover - degrade gracefully

    def is_vendor(_org, _email):  # type: ignore
        return False


# Single-source-of-truth operator domains if importable; else a small seed.
try:
    from scripts.backfill_company_coverage import COMPANY_BY_DOMAIN  # noqa: E402
except Exception:
    COMPANY_BY_DOMAIN = {
        "townepark.com": "Towne Park",
        "spplus.com": "SP+",
        "metropolis.io": "Metropolis",
        "parkingmgt.com": "Parking Management Company",
        "reefparking.com": "REEF",
        "lazparking.com": "LAZ Parking",
    }

FREEMAIL = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com",
    "me.com", "mac.com", "msn.com", "live.com", "comcast.net", "att.net",
    "verizon.net", "sbcglobal.net", "bellsouth.net", "protonmail.com", "proton.me",
    "ymail.com", "gmx.com", "mail.com",
}

# Lightweight vertical classifier -- mirrors the frontend VERT_* regexes so the
# proposed account_type lines up with what the UI already shows.
VERT = {
    "parking_valet": re.compile(
        r"towne ?park|sp ?plus|\bsp\+|metropolis|laz parking|ace parking|impark|"
        r"propark|reef parking|\bvalet\b|parking", re.I),
    "healthcare": re.compile(
        r"hospital|medical cent|medical college|health ?care|health system|clinic|"
        r"medical group|physicians|surgical center|rehabilitation|infirmary|wellpath|"
        r"nursing home", re.I),
    "education": re.compile(
        r"university|universidad|college|institute of technology|polytechnic|"
        r"school district|academy|campus|seminary|chartwells", re.I),
    "grocery": re.compile(r"sedano|supermarket|grocery|food market|\bgrocer\b", re.I),
}
HOTELISH = re.compile(r"hotel|resort|\binn\b|suites|lodge|residences?|spa|beach club|"
                      r"sandals|ritz|four seasons|aman|rosewood|kimpton|edition", re.I)

# Single-token brand names that frequently collapse genuinely different
# properties together when used as a grouping key (the over-merge risk).
BARE_BRANDS = {
    "hilton", "marriott", "hyatt", "sheraton", "westin", "ritz", "sandals",
    "fairmont", "kimpton", "aman", "rosewood", "edition", "autograph", "curio",
    "hampton", "hilton worldwide", "marriott international", "ihg", "accor",
    "wyndham", "radisson", "loews", "omni", "fontainebleau",
}


def domain_of(email):
    if not email or "@" not in email:
        return None
    return email.split("@")[-1].strip().lower() or None


def work_domain(email):
    d = domain_of(email)
    return d if d and d not in FREEMAIL else None


def vertical_of(org, email):
    blob = f"{org or ''} {domain_of(email) or ''}"
    for v, rx in VERT.items():
        if rx.search(blob):
            return v
    return "hospitality" if HOTELISH.search(org or "") else "other"


def account_type_of(vertical, org, source):
    if source in ("existing_hotel", "potential_lead"):
        return "property"
    if source == "mgmt_domain":
        return "management_company"
    if source == "vendor":
        return "vendor"
    if vertical in ("parking_valet", "education", "healthcare", "grocery"):
        return vertical
    if HOTELISH.search(org or ""):
        return "property"
    return "other"


async def fetch_all():
    """Load every input table read-only. Returns dicts of lists."""
    out = {}
    async with async_session() as s:
        await s.execute(text("SELECT set_limit(0.3)"))  # noop-safe if pg_trgm on

        out["hotels"] = (await s.execute(text(
            "SELECT id, hotel_name, hotel_name_normalized, city, state, zone, "
            "brand_tier, chain, management_company, is_client, sap_bp_code "
            "FROM existing_hotels"
        ))).mappings().all()

        out["leads"] = (await s.execute(text(
            "SELECT id, hotel_name, hotel_name_normalized, city, state, zone, brand_tier "
            "FROM potential_leads WHERE status NOT IN ('rejected','duplicate')"
        ))).mappings().all()

        out["sap"] = (await s.execute(text(
            "SELECT customer_code, customer_name, customer_name_normalized, "
            "customer_type, is_hotel, city, state, email, hotel_website "
            "FROM sap_clients"
        ))).mappings().all()

        out["contacts"] = (await s.execute(text(
            "SELECT id, email, organization, organization_normalized, "
            "management_company, parent_company, brand_tier, matched_lead_id, "
            "matched_hotel_id, person_id, "
            "COALESCE(manual_category, contact_category) AS eff_category "
            "FROM contacts"
        ))).mappings().all()

        out["lead_contacts"] = (await s.execute(text(
            "SELECT id, email, organization, lead_id, existing_hotel_id "
            "FROM lead_contacts"
        ))).mappings().all()
    return out


def build_indexes(data):
    hotel_by_id = {h["id"]: h for h in data["hotels"]}
    lead_by_id = {ld["id"]: ld for ld in data["leads"]}

    # SAP lookup by normalized name + by email/website domain.
    sap_by_norm = {}
    sap_by_domain = {}
    for c in data["sap"]:
        nk = c["customer_name_normalized"] or normalize_organization(c["customer_name"])
        if nk:
            sap_by_norm.setdefault(nk, c)
        for fld in (c.get("email"), c.get("hotel_website")):
            d = domain_of(fld) if fld and "@" in (fld or "") else (
                (fld or "").replace("https://", "").replace("http://", "")
                .replace("www.", "").split("/")[0].strip().lower() or None)
            if d and d not in FREEMAIL:
                sap_by_domain.setdefault(d, c)
    return hotel_by_id, lead_by_id, sap_by_norm, sap_by_domain


def resolve_one(row, idx, is_lead_contact=False):
    """Return (source, key, label, account_type, vertical) or None to skip."""
    hotel_by_id, lead_by_id, sap_by_norm, sap_by_domain = idx
    org = (row.get("organization") or "").strip() or None
    email = row.get("email")
    dom = work_domain(email)
    vert = vertical_of(org, email)

    hid = row.get("matched_hotel_id") or row.get("existing_hotel_id")
    lid = row.get("matched_lead_id") or row.get("lead_id")

    if hid and hid in hotel_by_id:
        h = hotel_by_id[hid]
        return ("existing_hotel", f"H{hid}", h["hotel_name"], "property",
                "hospitality")
    if lid and lid in lead_by_id:
        ld = lead_by_id[lid]
        return ("potential_lead", f"L{lid}", ld["hotel_name"], "property",
                "hospitality")

    if dom and dom in COMPANY_BY_DOMAIN:
        name = COMPANY_BY_DOMAIN[dom]
        v = vertical_of(name, email)
        # A known operator/portfolio domain is a management_company by default,
        # but non-hospitality verticals (grocery/parking/edu/healthcare) keep
        # their own type so e.g. Sedano's lands as grocery, not mgmt company.
        vtype = {"grocery": "grocery", "parking_valet": "parking",
                 "education": "education", "healthcare": "healthcare"}
        return ("mgmt_domain", f"M:{dom}", name, vtype.get(v, "management_company"), v)

    nk = row.get("organization_normalized") or normalize_organization(org)
    if nk and nk in sap_by_norm:
        c = sap_by_norm[nk]
        return ("sap", f"S{c['customer_code']}", c["customer_name"],
                "property" if c.get("is_hotel") else (c.get("customer_type") or "other"),
                vert)
    if dom and dom in sap_by_domain:
        c = sap_by_domain[dom]
        return ("sap", f"S{c['customer_code']}", c["customer_name"],
                "property" if c.get("is_hotel") else (c.get("customer_type") or "other"),
                vert)

    if org and is_vendor(org, email or ""):
        return ("vendor", f"V:{nk or dom or org.lower()}", org, "vendor", "other")

    if org and nk:
        return ("org", f"O:{nk}", org, account_type_of(vert, org, "org"), vert)

    if dom:
        return ("domain", f"D:{dom}", dom, "other", vert)

    return None  # orphan


def run_report(data, args):
    idx = build_indexes(data)

    # account_key -> aggregate
    acct_members = defaultdict(list)     # key -> list of (label, domain, source, type, vert)
    acct_label = {}
    acct_type = {}
    acct_vert = {}
    source_counts = Counter()
    excluded = 0
    orphan = 0
    rows = []

    def ingest(person_rows, is_lead):
        nonlocal excluded, orphan
        for r in person_rows:
            if not is_lead:
                if (r.get("eff_category") or "") in ("junk", "personal"):
                    excluded += 1
                    continue
            res = resolve_one(r, idx, is_lead)
            if res is None:
                orphan += 1
                continue
            source, key, label, atype, vert = res
            acct_members[key].append(work_domain(r.get("email")))
            acct_label.setdefault(key, label)
            acct_type[key] = atype
            acct_vert[key] = vert
            source_counts[source] += 1
            rows.append((key, label, atype, vert, source,
                         r.get("email"), r.get("organization")))

    if args.limit:
        ingest(data["contacts"][: args.limit], False)
    else:
        ingest(data["contacts"], False)
    ingest(data["lead_contacts"], True)

    LINKED = {"existing_hotel", "potential_lead", "sap"}
    new_keys = [k for k in acct_members if not k.startswith(("H", "L", "S"))]
    linked_keys = [k for k in acct_members if k.startswith(("H", "L", "S"))]
    total_people = sum(len(v) for v in acct_members.values())

    p = print
    p("\n" + "=" * 72)
    p("PHASE 0 -- ACCOUNT RESOLUTION DIAGNOSTIC (read-only, no writes)")
    p("=" * 72)
    p(f"  inputs: {len(data['contacts']):>6} contacts   "
      f"{len(data['lead_contacts']):>5} lead_contacts   "
      f"{len(data['hotels']):>5} hotels   {len(data['sap']):>5} sap_clients")
    p("")
    p("COVERAGE")
    p("-" * 72)
    p(f"  resolved to a person->account assignment : {total_people:>6}")
    p(f"    - LINK to existing canonical row       : "
      f"{sum(len(acct_members[k]) for k in linked_keys):>6}  "
      f"({len(linked_keys)} distinct hotel/lead/sap accounts)")
    p(f"    - NEW account needed (the gap)         : "
      f"{sum(len(acct_members[k]) for k in new_keys):>6}  "
      f"({len(new_keys)} distinct NEW accounts)")
    p(f"  orphan (freemail + no org / no signal)   : {orphan:>6}")
    p(f"  excluded (effective junk/personal)       : {excluded:>6}")

    p("")
    p("NEW ACCOUNTS BY TYPE  (orgs that have NO canonical row today)")
    p("-" * 72)
    by_type = Counter(acct_type[k] for k in new_keys)
    for t, n in by_type.most_common():
        ppl = sum(len(acct_members[k]) for k in new_keys if acct_type[k] == t)
        p(f"  {t:<22} {n:>5} accounts   {ppl:>6} people")

    p("")
    p("NEW ACCOUNTS BY VERTICAL")
    p("-" * 72)
    by_vert = Counter(acct_vert[k] for k in new_keys)
    for v, n in by_vert.most_common():
        ppl = sum(len(acct_members[k]) for k in new_keys if acct_vert[k] == v)
        p(f"  {v:<22} {n:>5} accounts   {ppl:>6} people")

    p("")
    p("RESOLUTION SOURCE  (how each person resolved)")
    p("-" * 72)
    for src, n in source_counts.most_common():
        tag = "LINK" if src in LINKED else "NEW "
        p(f"  [{tag}] {src:<16} {n:>6}")

    p("")
    p(f"BIGGEST ACCOUNTS  (top {args.top} by people -- eyeball the merges)")
    p("-" * 72)
    biggest = sorted(acct_members.items(), key=lambda kv: -len(kv[1]))[: args.top]
    for key, members in biggest:
        doms = {d for d in members if d}
        flag = ""
        lk = (acct_label.get(key) or "").lower().strip()
        if normalize_organization(lk) in BARE_BRANDS or lk in BARE_BRANDS:
            flag = "  <-- BARE BRAND (likely over-merge)"
        elif not key.startswith(("H", "L", "S")) and len(doms) >= 3:
            flag = f"  <-- {len(doms)} domains (check over-merge)"
        p(f"  {len(members):>5}  [{acct_type.get(key,'?'):<18}] "
          f"{(acct_label.get(key) or key)[:46]:<46}{flag}")

    # RISK: under-merge -- one work-domain split across multiple NEW org accounts
    p("")
    p(f"RISK -- UNDER-MERGE  (one domain split across accounts; SHOULD merge) top {args.top}")
    p("-" * 72)
    dom_to_keys = defaultdict(set)
    for key in new_keys:
        if not key.startswith(("O:", "D:")):
            continue
        for d in acct_members[key]:
            if d:
                dom_to_keys[d].add(key)
    splits = sorted(((d, ks) for d, ks in dom_to_keys.items() if len(ks) >= 2),
                    key=lambda x: -len(x[1]))[: args.top]
    if not splits:
        p("  (none -- domains map cleanly to single accounts)")
    for d, ks in splits:
        labels = sorted({(acct_label.get(k) or k)[:28] for k in ks})
        p(f"  {d:<26} -> {len(ks)} accounts: {', '.join(labels[:4])}"
          f"{' ...' if len(labels) > 4 else ''}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["account_key", "account_label", "account_type", "vertical",
                        "resolution_source", "email", "organization"])
            w.writerows(rows)
        p("")
        p(f"  per-person preview written -> {args.csv}  ({len(rows)} rows)")

    p("")
    p("=" * 72)
    p("Nothing was written. Re-run with --csv to dump the full proposed map.")
    p("=" * 72 + "\n")


async def main():
    ap = argparse.ArgumentParser(description="Phase 0 account-resolution diagnostic (read-only)")
    ap.add_argument("--top", type=int, default=25, help="rows in each leaderboard")
    ap.add_argument("--limit", type=int, default=0, help="cap contacts for a quick sample")
    ap.add_argument("--csv", type=str, default=None, help="dump per-person proposed map to CSV")
    args = ap.parse_args()

    print("Loading tables (read-only)...", flush=True)
    data = await fetch_all()
    run_report(data, args)


if __name__ == "__main__":
    asyncio.run(main())
