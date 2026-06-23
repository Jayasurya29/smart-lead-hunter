"""contact_autolink.py -- link unmatched inbox contacts to a hotel/lead, or to a
management-company portfolio, and write the matching coverage edge.

Single source of truth for the logic the one-off backfill scripts proved:
  - DOMAIN: exact email host == a property's website host (precomputed,
    ambiguous/shared hosts dropped, ISP/brand hosts skipped).
  - NAME: exact normalized org name == a property's normalized name, with bare
    brands rejected on both sides.
  - COMPANY: curated operator/management domains -> a 'covers' edge at the
    management_company level (portfolio scope).

Idempotent: only touches still-unmatched contacts; coverage inserts use
ON CONFLICT DO NOTHING. Safe to run after every sync.
"""

from __future__ import annotations

from sqlalchemy import text

from app.database import async_session
from app.services.org_normalize import normalize_organization

SKIP_HOSTS = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "live.com",
    "msn.com",
    "comcast.net",
    "bellsouth.net",
    "att.net",
    "marriott.com",
    "hilton.com",
    "hyatt.com",
    "ihg.com",
    "wyndham.com",
    "wyndhamhotels.com",
    "accor.com",
    "choicehotels.com",
    "montage.com",
    "montagehotels.com",
    "aubergeresorts.com",
    "kimptonhotels.com",
    "sonesta.com",
    "loewshotels.com",
    "omnihotels.com",
    "fourseasons.com",
    "ritzcarlton.com",
    "fairmont.com",
    "marriotthotels.com",
    "thompsonhotels.com",
    "viceroyhotelsandresorts.com",
    "trumphotels.com",
    "standardhotels.com",
    "mohg.com",
    "stregis.com",
}
BRANDS = {
    "hyatt",
    "hilton",
    "marriott",
    "westin",
    "sheraton",
    "wyndham",
    "fourseasons",
    "four",
    "seasons",
    "montage",
    "kimpton",
    "sonesta",
    "loews",
    "omni",
    "ritz",
    "ritzcarlton",
    "carlton",
    "conrad",
    "sls",
    "auberge",
    "pendry",
    "thompson",
    "viceroy",
    "aloft",
    "renaissance",
    "intercontinental",
    "fairmont",
    "waldorf",
    "astoria",
    "andaz",
    "edition",
    "tapestry",
    "curio",
    "autograph",
    "doubletree",
    "embassy",
    "hampton",
    "courtyard",
    "residence",
    "ascend",
    "tribute",
    "gaylord",
    "novotel",
    "pestana",
    "dream",
    "royalton",
    "barcelo",
}
GENERIC = {
    "the",
    "a",
    "an",
    "of",
    "and",
    "by",
    "at",
    "hotel",
    "hotels",
    "resort",
    "resorts",
    "spa",
    "suites",
    "suite",
    "inn",
    "collection",
    "club",
    "beach",
    "group",
    "international",
    "hospitality",
    "residences",
    "tower",
    "lodge",
    "house",
}
COMPANY_BY_DOMAIN = {
    "townepark.com": "Towne Park",
    "spplus.com": "SP+",
    "metropolis.io": "Metropolis",
    "parkingmgt.com": "Parking Management Company",
    "reefparking.com": "REEF",
    "lazparking.com": "LAZ Parking",
    "denisonparking.com": "Denison Parking",
    "kwpmc.com": "KW Property Management",
    "schultehospitality.com": "Schulte Hospitality Group",
    "oasismarinas.com": "Oasis Marinas",
    "loewshotels.com": "Loews Hotels",
    "southbeachgroup.com": "South Beach Group",
    "rosenhotels.com": "Rosen Hotels & Resorts",
    "pyramidglobal.com": "Pyramid Global Hospitality",
    "reimaginedparking.com": "Reimagined Parking",
    "sedanos.com": "Sedano's",
    "compass-usa.com": "Compass Group",
}


def _host(url):
    h = (url or "").strip().lower()
    for p in ("https://", "http://"):
        if h.startswith(p):
            h = h[len(p) :]
    if h.startswith("www."):
        h = h[4:]
    return h.split("/")[0].split("?")[0]


def _is_bare_brand(name):
    nrm = normalize_organization(name or "") or ""
    return not [t for t in nrm.split() if t not in BRANDS and t not in GENERIC and len(t) > 2]


async def _build_host_index(s):
    seen, idx = {}, {}
    for atype, table, extra in (
        ("existing_hotel", "existing_hotels", ""),
        ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
    ):
        for r in (
            await s.execute(
                text(
                    f"SELECT id, hotel_name, hotel_website FROM {table} WHERE COALESCE(hotel_website,'') <> ''{extra}"
                )
            )
        ).all():
            h = _host(r.hotel_website)
            if not h or h in SKIP_HOSTS:
                continue
            if h not in seen:
                seen[h] = (atype, r.id, r.hotel_name)
            elif seen[h] is not None and seen[h][1] != r.id:
                seen[h] = None
    for h, v in seen.items():
        if v:
            idx[h] = v
    return idx


async def _cover_edge(s, pid, atype, aid, aname, src):
    await s.execute(
        text(
            "INSERT INTO contact_affiliations (person_type, person_id, account_type, account_id, "
            "account_name, relationship, scope, source, confidence, notes, created_at, updated_at) "
            "VALUES ('contact', :pid, :at, :aid, :nm, 'covers', :scope, 'matched', 0.9, :notes, NOW(), NOW()) "
            "ON CONFLICT DO NOTHING"
        ),
        {
            "pid": pid,
            "at": atype,
            "aid": aid,
            "nm": aname,
            "scope": "portfolio" if atype == "management_company" else "property",
            "notes": f"Auto-linked via {src}",
        },
    )


async def run_autolink() -> dict:
    """Link every still-unmatched, non-junk inbox contact it confidently can."""
    out = {"domain": 0, "name": 0, "company": 0}
    async with async_session() as s:
        idx = await _build_host_index(s)
        rows = (
            await s.execute(
                text(
                    "SELECT id, email, organization FROM contacts "
                    "WHERE matched_hotel_id IS NULL AND matched_lead_id IS NULL "
                    "AND COALESCE(contact_category,'') <> 'junk' AND email LIKE '%@%'"
                )
            )
        ).all()
        for r in rows:
            domain = (r.email or "").split("@")[-1].lower()
            # 1) management-company portfolio coverage (no single-hotel FK)
            company = COMPANY_BY_DOMAIN.get(domain)
            if company:
                await _cover_edge(s, r.id, "management_company", None, company, "company")
                out["company"] += 1
                continue
            # 2) single property by exact host
            hit = idx.get(domain) if domain and domain not in SKIP_HOSTS else None
            method = "domain" if hit else None
            # 3) else exact normalized name (no bare brands)
            if not hit and r.organization and not _is_bare_brand(r.organization):
                nrm = normalize_organization(r.organization)
                if nrm:
                    for atype, table, extra in (
                        ("existing_hotel", "existing_hotels", ""),
                        ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
                    ):
                        cand = (
                            await s.execute(
                                text(
                                    f"SELECT id, hotel_name FROM {table} WHERE hotel_name_normalized = :n{extra}"
                                ),
                                {"n": nrm},
                            )
                        ).all()
                        if len(cand) == 1 and not _is_bare_brand(cand[0].hotel_name):
                            hit, method = (atype, cand[0].id, cand[0].hotel_name), "name"
                            break
            if not hit:
                continue
            atype, aid, aname = hit
            col = "matched_hotel_id" if atype == "existing_hotel" else "matched_lead_id"
            await s.execute(
                text(f"UPDATE contacts SET {col} = :aid, updated_at = NOW() WHERE id = :id"),
                {"aid": aid, "id": r.id},
            )
            await _cover_edge(s, r.id, atype, aid, aname, method)
            out[method] += 1
        await s.commit()
    return out


async def retire_and_relink(s, person_type, person_id, new_org):
    """A person moved: retire their current 'covers' edge (old hotel becomes
    PAST via the separately-written 'former' edge) and link+cover the NEW hotel
    by exact name. Name-based, NOT domain -- the email often stays on the old
    employer's domain. Returns the new account_type linked, or None.
    [coverage follows the person on a move]"""
    await s.execute(
        text(
            "DELETE FROM contact_affiliations WHERE person_type=:pt AND person_id=:pid "
            "AND relationship='covers' AND source='matched'"
        ),
        {"pt": person_type, "pid": person_id},
    )
    if person_type == "contact":
        await s.execute(
            text("UPDATE contacts SET matched_hotel_id=NULL, matched_lead_id=NULL WHERE id=:id"),
            {"id": person_id},
        )
    if not new_org or _is_bare_brand(new_org):
        return None
    nrm = normalize_organization(new_org)
    if not nrm:
        return None
    for atype, table, extra in (
        ("existing_hotel", "existing_hotels", ""),
        ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
    ):
        cand = (
            await s.execute(
                text(f"SELECT id, hotel_name FROM {table} WHERE hotel_name_normalized = :n{extra}"),
                {"n": nrm},
            )
        ).all()
        if len(cand) == 1 and not _is_bare_brand(cand[0].hotel_name):
            if person_type == "contact":
                col = "matched_hotel_id" if atype == "existing_hotel" else "matched_lead_id"
                await s.execute(
                    text(f"UPDATE contacts SET {col} = :aid, updated_at = NOW() WHERE id = :id"),
                    {"aid": cand[0].id, "id": person_id},
                )
            await _cover_edge(s, person_id, atype, cand[0].id, cand[0].hotel_name, "move")
            return atype
    return None
