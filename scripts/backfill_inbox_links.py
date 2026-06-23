#!/usr/bin/env python3
"""backfill_inbox_links.py -- link unmatched inbox contacts to their hotel/lead.

TIGHT matching (precision over recall -- a wrong link is worse than no link):
  DOMAIN: contact's email host must EQUAL the property's website host exactly,
          and that host must not be a shared brand/parent domain.
  NAME:   org normalized-name must EQUAL the property's normalized name AND
          carry a real property/city word -- bare brands ("Hyatt", "Four
          Seasons") are rejected, and a bare-brand TARGET row is never used.
Anything ambiguous or bare -> left for manual matching.

DRY-RUN by default. --apply to write. --verbose to list each match.
Usage:  python scripts/backfill_inbox_links.py [--verbose] [--apply]
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402
from app.services.org_normalize import normalize_organization  # noqa: E402

# Freemail + shared brand/parent/aggregator hosts -> never a property identity.
SKIP_HOSTS = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com",
    "me.com", "live.com", "msn.com", "comcast.net", "bellsouth.net", "att.net",
    "marriott.com", "hilton.com", "hyatt.com", "ihg.com", "wyndham.com", "wyndhamhotels.com",
    "accor.com", "choicehotels.com", "montage.com", "montagehotels.com", "aubergeresorts.com",
    "kimptonhotels.com", "sonesta.com", "loewshotels.com", "omnihotels.com", "fourseasons.com",
    "ritzcarlton.com", "fairmont.com", "marriotthotels.com", "thompsonhotels.com",
    "viceroyhotelsandresorts.com", "trumphotels.com", "standardhotels.com",
}
# Brand / chain tokens that carry no property identity on their own.
BRANDS = {
    "hyatt", "hilton", "marriott", "westin", "sheraton", "wyndham", "fourseasons",
    "montage", "kimpton", "sonesta", "four", "seasons", "loews", "omni", "ritz", "ritzcarlton", "carlton",
    "conrad", "sls", "auberge", "pendry", "thompson", "viceroy", "aloft", "renaissance",
    "intercontinental", "fairmont", "waldorf", "astoria", "andaz", "edition", "tapestry",
    "curio", "autograph", "doubletree", "embassy", "hampton", "courtyard", "residence",
    "ascend", "tribute", "gaylord", "novotel", "pestana", "dream", "royalton", "barcelo",
}
GENERIC = {
    "the", "a", "an", "of", "and", "by", "at", "hotel", "hotels", "resort", "resorts",
    "spa", "suites", "suite", "inn", "collection", "club", "beach", "group", "international",
    "hospitality", "residences", "tower", "lodge", "house",
}


def _host(url):
    h = (url or "").strip().lower()
    for p in ("https://", "http://"):
        if h.startswith(p):
            h = h[len(p):]
    if h.startswith("www."):
        h = h[4:]
    return h.split("/")[0].split("?")[0]


def _is_bare_brand(name):
    nrm = normalize_organization(name or "") or ""
    distinctive = [t for t in nrm.split() if t not in BRANDS and t not in GENERIC and len(t) > 2]
    return not distinctive


_HOST_INDEX = {}  # host -> (account_type, id, name); only unambiguous property hosts


async def _build_host_index(s):
    """Precompute every property's website host (existing_hotels + potential_leads).
    A host that maps to >1 distinct property (brand/shared) is dropped, as are
    skip/ISP/parent hosts. Broader + faster than per-contact ILIKE."""
    seen = {}
    for atype, table, extra in (
        ("existing_hotel", "existing_hotels", ""),
        ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
    ):
        for r in (await s.execute(text(
            f"SELECT id, hotel_name, hotel_website FROM {table} "
            f"WHERE COALESCE(hotel_website,'') <> ''{extra}"
        ))).all():
            h = _host(r.hotel_website)
            if not h or h in SKIP_HOSTS:
                continue
            if h not in seen:
                seen[h] = (atype, r.id, r.hotel_name)
            elif seen[h] is not None and seen[h][1] != r.id:
                seen[h] = None  # ambiguous -> disable
    for h, v in seen.items():
        if v:
            _HOST_INDEX[h] = v


async def _one(s, q, params):
    return (await s.execute(text(q), params)).all()


async def _resolve(s, domain, org):
    domain = (domain or "").strip().lower()
    # DOMAIN tier 0 -- precomputed known-hotel-domain index (exact host)
    if domain and domain in _HOST_INDEX:
        atype, aid, aname = _HOST_INDEX[domain]
        return (atype, aid, aname, "domain")
    # DOMAIN tier -- exact host, property-specific only (ILIKE fallback)
    if domain and domain not in SKIP_HOSTS:
        for atype, table, extra in (
            ("existing_hotel", "existing_hotels", ""),
            ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
        ):
            rows = await _one(
                s, f"SELECT id, hotel_name, hotel_website FROM {table} WHERE hotel_website ILIKE :p{extra}",
                {"p": f"%{domain}%"},
            )
            exact = [r for r in rows if _host(r.hotel_website) == domain]
            if len(exact) == 1:
                return ("existing_hotel" if atype == "existing_hotel" else "potential_lead",
                        exact[0].id, exact[0].hotel_name, "domain")
    # NAME tier -- exact normalized full name, no bare brands either side
    nrm = normalize_organization(org or "")
    if nrm and not _is_bare_brand(org):
        for atype, table, extra in (
            ("existing_hotel", "existing_hotels", ""),
            ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
        ):
            rows = await _one(
                s, f"SELECT id, hotel_name FROM {table} WHERE hotel_name_normalized = :n{extra}",
                {"n": nrm},
            )
            if len(rows) == 1 and not _is_bare_brand(rows[0].hotel_name):
                return (atype, rows[0].id, rows[0].hotel_name, "name")
    return None


async def main(args):
    print("[backfill_inbox_links v4: known-hotel-domain index + non-bare-brand name]")
    async with async_session() as s:
        await _build_host_index(s)
        print(f"  host index: {len(_HOST_INDEX)} unambiguous property domains")
        contacts = await _one(
            s,
            "SELECT id, email, organization FROM contacts "
            "WHERE matched_hotel_id IS NULL AND matched_lead_id IS NULL "
            "AND COALESCE(contact_category,'') <> 'junk'",
            {},
        )
        linked = by_domain = by_name = 0
        for c in contacts:
            email = c.email or ""
            domain = email.split("@")[-1] if "@" in email else ""
            hit = await _resolve(s, domain, c.organization or "")
            if not hit:
                continue
            atype, aid, aname, method = hit
            linked += 1
            by_domain += method == "domain"
            by_name += method == "name"
            if args.verbose:
                print(f"  #{c.id} {c.organization or email} -> {aname} [{atype}/{method}]")
            if args.apply:
                col = "matched_hotel_id" if atype == "existing_hotel" else "matched_lead_id"
                await s.execute(
                    text(f"UPDATE contacts SET {col} = :aid, updated_at = NOW() WHERE id = :id"),
                    {"aid": aid, "id": c.id},
                )
                await s.execute(
                    text(
                        "DELETE FROM contact_affiliations WHERE person_type='contact' "
                        "AND person_id=:pid AND relationship='covers' AND account_type=:at AND source='matched'"
                    ),
                    {"pid": c.id, "at": atype},
                )
                await s.execute(
                    text(
                        "INSERT INTO contact_affiliations (person_type, person_id, account_type, account_id, "
                        "account_name, relationship, source, confidence, notes, created_at, updated_at) "
                        "VALUES ('contact', :pid, :at, :aid, :nm, 'covers', 'matched', 0.9, :notes, NOW(), NOW())"
                    ),
                    {"pid": c.id, "at": atype, "aid": aid, "nm": aname, "notes": f"Auto-linked via {method}"},
                )
        if args.apply:
            await s.commit()

    mode = "APPLIED" if args.apply else "DRY-RUN (no writes)"
    print(f"\n{mode}: {len(contacts)} unmatched scanned -> {linked} linkable ({by_domain} domain, {by_name} name).")
    if not args.apply and linked:
        print("Re-run with --apply to write.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    asyncio.run(main(ap.parse_args()))
