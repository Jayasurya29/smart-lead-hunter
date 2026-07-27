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
from app.services.org_classifier import classify_org_type_offline

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


async def _build_name_index(s):
    """[patch_autolink_namekey] normalized hotel name -> (atype, id, hotel_name).

    BOTH sides of the name comparison now use normalize_organization().

    The stored hotel_name_normalized column is written by normalize_hotel_name(),
    which preserves word order and keeps "the". normalize_organization() sorts
    tokens and drops article/legal stopwords. Comparing one against the other
    only matched when a name's tokens happened to already be alphabetical
    ("Cheeca Lodge") — which is why the NAME path returned zero. The stored
    column is the right key for hotel-vs-hotel dedup and the wrong key here.

    Semantics otherwise unchanged:
      - a normalized name matching >1 row WITHIN a table is ambiguous -> dropped
      - bare-brand names are never a valid target
      - existing_hotels wins over potential_leads on a cross-table collision
    """
    idx: dict = {}
    for atype, table, extra in (
        ("potential_lead", "potential_leads", " AND status <> 'rejected'"),
        ("existing_hotel", "existing_hotels", ""),
    ):
        seen: dict = {}
        for r in (
            await s.execute(
                text(
                    f"SELECT id, hotel_name FROM {table} "
                    f"WHERE COALESCE(hotel_name,'') <> ''{extra}"
                )
            )
        ).all():
            n = normalize_organization(r.hotel_name)
            if not n:
                continue
            if n not in seen:
                seen[n] = (atype, r.id, r.hotel_name)
            elif seen[n] is not None and seen[n][1] != r.id:
                seen[n] = None  # ambiguous within this table
        for n, v in seen.items():
            if v and not _is_bare_brand(v[2]):
                idx[n] = v
    return idx


async def run_autolink(
    *,
    dry_run: bool = False,
    limit=None,
    contact_ids=None,
    batch_size: int = 500,
) -> dict:
    """[patch_autolink_wire] Link every still-unmatched, non-junk inbox contact
    it confidently can.

    dry_run:     resolve every link, report it, write nothing (rolls back).
    contact_ids: restrict to these ids — the incremental post-sync path.
    limit:       cap rows examined (proof runs, backlog chunking).
    batch_size:  commit every N writes rather than holding one transaction
                 open across the whole contacts table.

    Returns counters plus up to 25 sample links for review.

    NOTE: the company path writes a portfolio 'covers' edge but no matched_*
    column (there is no single hotel id to point at), so those contacts stay
    in the unmatched pool and are re-examined on every run. The insert is
    ON CONFLICT DO NOTHING so this is wasted work, not duplicate data. A
    proper fix needs an autolink_checked_at column — deliberately out of
    scope here.
    """
    out = {
        "examined": 0,
        "domain": 0,
        "name": 0,
        "company": 0,
        # [patch_autolink_chain] company contacts already carrying their edge
        "company_edge_exists": 0,
        "unmatched": 0,
        "dry_run": dry_run,
    }
    samples: list = []
    async with async_session() as s:
        idx = await _build_host_index(s)
        nidx = await _build_name_index(s)

        # [patch_autolink_chain] The company path writes a portfolio 'covers'
        # edge but no matched_* column, so those contacts stay in the unmatched
        # pool and come back every run. Preloading the edges that already exist
        # turns thousands of ON CONFLICT DO NOTHING no-ops per run into one
        # SELECT.
        # [patch_autolink_edgecase] lowercased to match the unique index in
        # alembic 034, which keys on COALESCE(lower(account_name), '').
        _company_edges = {
            (row.person_id, (row.account_name or "").strip().lower())
            for row in (
                await s.execute(
                    text(
                        "SELECT person_id, account_name FROM contact_affiliations "
                        "WHERE person_type = 'contact' AND relationship = 'covers' "
                        "AND account_type = 'management_company'"
                    )
                )
            ).all()
        }

        sql = (
            "SELECT id, email, organization FROM contacts "
            "WHERE matched_hotel_id IS NULL AND matched_lead_id IS NULL "
            "AND COALESCE(contact_category,'') <> 'junk' AND email LIKE '%@%'"
        )
        params: dict = {}
        if contact_ids is not None:
            ids = [int(i) for i in contact_ids]
            if not ids:
                out["note"] = "contact_ids was empty — nothing to do"
                return out
            # int()-coerced above, so inlining is injection-safe and avoids
            # asyncpg array-parameter typing.
            sql += " AND id IN (" + ",".join(str(i) for i in ids) + ")"
        sql += " ORDER BY id"
        if limit:
            sql += " LIMIT :lim"
            params["lim"] = int(limit)

        rows = (await s.execute(text(sql), params)).all()
        out["examined"] = len(rows)

        pending = 0
        for r in rows:
            domain = (r.email or "").split("@")[-1].lower()

            # 1) management-company portfolio coverage (no single-hotel FK)
            company = COMPANY_BY_DOMAIN.get(domain)
            if company:
                out["company"] += 1
                # [patch_autolink_chain] already covered -> nothing to write
                _co_key = (r.id, company.strip().lower())  # [patch_autolink_edgecase]
                if _co_key in _company_edges:
                    out["company_edge_exists"] += 1
                    continue
                if len(samples) < 25:
                    samples.append(
                        {"id": r.id, "email": r.email, "via": "company", "target": company}
                    )
                if not dry_run:
                    await _cover_edge(s, r.id, "management_company", None, company, "company")
                    _company_edges.add(_co_key)  # [patch_autolink_edgecase]
                    pending += 1
                    if pending >= batch_size:
                        await s.commit()
                        pending = 0
                continue

            # 2) single property by exact host
            hit = idx.get(domain) if domain and domain not in SKIP_HOSTS else None
            method = "domain" if hit else None

            # 3) else exact normalized name (no bare brands)
            if not hit and r.organization and not _is_bare_brand(r.organization):
                nrm = normalize_organization(r.organization)
                if nrm:
                    cand = nidx.get(nrm)
                    if cand:
                        hit, method = cand, "name"

            if not hit:
                out["unmatched"] += 1
                continue

            atype, aid, aname = hit
            out[method] += 1
            if len(samples) < 25:
                samples.append(
                    {
                        "id": r.id,
                        "email": r.email,
                        "org": r.organization,
                        "via": method,
                        "target": aname,
                        "account": f"{atype}:{aid}",
                    }
                )
            if dry_run:
                continue

            col = "matched_hotel_id" if atype == "existing_hotel" else "matched_lead_id"
            await s.execute(
                text(f"UPDATE contacts SET {col} = :aid, updated_at = NOW() WHERE id = :id"),
                {"aid": aid, "id": r.id},
            )
            await _cover_edge(s, r.id, atype, aid, aname, method)
            pending += 1
            if pending >= batch_size:
                await s.commit()
                pending = 0

        if dry_run:
            await s.rollback()
        else:
            await s.commit()

    out["samples"] = samples
    return out


async def retire_and_relink(s, person_type, person_id, new_org):
    """A person moved: retire their current 'covers' edge (old hotel becomes
    PAST via the separately-written 'former' edge) and link+cover the NEW hotel
    by exact name. Name-based, NOT domain -- the email often stays on the old
    employer's domain. Returns the new account_type linked, or None.
    [coverage follows the person on a move]"""
    # [patch_move_flip_former] Old 'covers' edges become PAST, not deleted.
    # Flip covers->former so coverage history survives the move. Where a
    # 'former' row for that same account already exists (unique-index
    # collision), drop the now-redundant 'covers' row instead. No end_date:
    # we don't know when they actually left, so we never invent it.
    await s.execute(
        text(
            "DELETE FROM contact_affiliations cov "
            "WHERE cov.person_type=:pt AND cov.person_id=:pid "
            "AND cov.relationship='covers' AND cov.source='matched' "
            "AND EXISTS (SELECT 1 FROM contact_affiliations f "
            "WHERE f.person_type=cov.person_type AND f.person_id=cov.person_id "
            "AND f.account_type=cov.account_type "
            "AND COALESCE(f.account_id,-1)=COALESCE(cov.account_id,-1) "
            "AND COALESCE(lower(f.account_name),'')=COALESCE(lower(cov.account_name),'') "
            "AND f.relationship='former')"
        ),
        {"pt": person_type, "pid": person_id},
    )
    await s.execute(
        text(
            "UPDATE contact_affiliations SET relationship='former', "
            "notes=COALESCE(notes,'') || ' [retired on move]', updated_at=NOW() "
            "WHERE person_type=:pt AND person_id=:pid "
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
    # [patch_move_flip_former] Moved to a management company / operator?
    # Offline check only (no API on the click path). Attach a portfolio-scope
    # management_company edge and stop -- operators are never a hotel lead.
    # 'unknown' falls through to the hotel matcher below (today's behavior).
    if classify_org_type_offline(new_org) == "operator":
        await _cover_edge(s, person_id, "management_company", None, new_org, "move")
        return "management_company"
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

    # [patch_move_create_lead] No existing hotel/lead matches the new employer.
    # Don't orphan the contact (covers nothing, matched to nothing) and lose the
    # property -- create a lightweight potential_lead stub so the person has a
    # home and the hotel enters the pipeline. It flows through the SAME path as
    # any discovered lead (status='new'): Smart Fill / enrichment classifies it
    # and rejects/transfers it if it's a residence, sub-tier, or out-of-geo.
    # Deduped by normalized name; reuses a live stub instead of duplicating.
    existing_stub = (
        await s.execute(
            text(
                "SELECT id FROM potential_leads "
                "WHERE hotel_name_normalized = :n AND status <> 'rejected' LIMIT 1"
            ),
            {"n": nrm},
        )
    ).first()
    if existing_stub:
        new_lead_id = existing_stub.id
    else:
        new_lead_id = (
            await s.execute(
                text(
                    "INSERT INTO potential_leads "
                    "(hotel_name, hotel_name_normalized, status, data_source, notes, "
                    " created_at, updated_at) "
                    "VALUES (:hn, :n, 'new', 'contact_move', :notes, NOW(), NOW()) "
                    "RETURNING id"
                ),
                {
                    "hn": new_org,
                    "n": nrm,
                    "notes": "Auto-created: a known contact moved here (needs qualification).",
                },
            )
        ).scalar()
    if person_type == "contact":
        await s.execute(
            text("UPDATE contacts SET matched_lead_id = :aid, updated_at = NOW() WHERE id = :id"),
            {"aid": new_lead_id, "id": person_id},
        )
    await _cover_edge(s, person_id, "potential_lead", new_lead_id, new_org, "move")
    return "potential_lead"
