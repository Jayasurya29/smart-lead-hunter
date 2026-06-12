"""smart_fill.py — gap-driven enrichment router (2026-06-11).

One brain over the enrichment tools that already exist: profile what each
contact is MISSING, then dispatch the resolver that fills exactly that, in
buyer-first priority order, under explicit caps. Nothing here re-implements
an existing resolver — it routes to them.

  gap            resolver                                         cost
  ─────────────  ───────────────────────────────────────────────  ──────────────
  role / title   contact_tier2_enrichment.enrich_batch_deep       Serper + Gemini
                 (also opportunistically resolves NAMES on
                 nameless rows and writes provenance)
  linkedin_url   find_linkedin_url()  ← the one NEW resolver      1–2 Serper
  name           shrunk as a side-effect of the role pass;
                 the dedicated bulk web phase stays in
                 resolve_names.py (run on the host)               —
  email          inbox contacts always have one (they come FROM
                 email). The lead-side gap (saved lead contacts
                 with LinkedIn but no email) is served by the
                 existing POST /api/contacts/bulk-enrich-email
                 (Wiza, 2 credits/hit, hard-capped) — censused
                 here, never auto-spent.

Eligibility everywhere: junk / seller / competitor / personal / operational
and shared mailboxes are excluded — we don't spend quota on rows the
directory hides.

Priority order for every pass (who gets enriched first when capped):
  buyers → P1 → P2 → P3 → P4 → decision-makers → highest email volume.

Run again after a pass: the role pass names previously-nameless rows, which
makes them eligible for the LinkedIn pass on the next run.
"""

import asyncio
import logging
import re
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger("smart_fill")

# ── shared eligibility + priority fragments ────────────────────────────────

ELIGIBLE = (
    "(contact_category IS NULL OR contact_category NOT IN "
    "('junk','seller','competitor','personal','operational')) "
    "AND COALESCE(is_shared_mailbox, false) = false"
)

PRIORITY_ORDER = (
    "ORDER BY (contact_category = 'buyer') DESC, "
    "CASE procurement_priority WHEN 'P1' THEN 0 WHEN 'P2' THEN 1 "
    "WHEN 'P3' THEN 2 WHEN 'P4' THEN 3 ELSE 4 END, "
    "is_decision_maker DESC NULLS LAST, interaction_count DESC, id"
)

HAS_NAME = "(COALESCE(first_name,'') <> '' OR COALESCE(last_name,'') <> '')"
NO_NAME = (
    "COALESCE(first_name,'') = '' AND COALESCE(last_name,'') = '' "
    "AND (COALESCE(display_name,'') = '' OR display_name = email)"
)
NO_ROLE = "COALESCE(title,'') = '' AND COALESCE(inferred_role,'') = ''"
NO_LINKEDIN = "COALESCE(linkedin_url,'') = ''"

_LI_RE = re.compile(r"linkedin\.com/in/([^/?&#\s)]+)", re.IGNORECASE)


def _norm_linkedin(url_or_text: str) -> Optional[str]:
    """Canonical https://www.linkedin.com/in/{slug} from any URL or snippet
    (handles country subdomains the same way wiza_enrichment does)."""
    m = _LI_RE.search(url_or_text or "")
    if not m:
        return None
    slug = m.group(1).strip().rstrip(".,;")
    if not slug or len(slug) < 3:
        return None
    return f"https://www.linkedin.com/in/{slug}"


def find_linkedin_url(name: str, org: str, email: str = "") -> Optional[str]:
    """The one resolver that didn't exist anywhere: locate a MISSING LinkedIn
    profile for a named contact. 1–2 Serper queries; a candidate URL is only
    accepted when the person's last-name token appears in the same result
    (or in the profile slug) — guards against pinning the wrong person.
    """
    name = (name or "").strip()
    if not name:
        return None
    try:
        from app.services.outreach.researcher import smart_search
    except Exception as e:  # pragma: no cover — researcher unavailable
        logger.warning(f"smart_fill: researcher import failed: {e}")
        return None

    org = (org or "").strip()
    domain_word = (email or "").split("@")[-1].split(".")[0] if "@" in (email or "") else ""
    anchor = org or domain_word
    queries = [f'"{name}" {anchor} site:linkedin.com/in'.strip()]
    if anchor:
        queries.append(f'"{name}" {anchor} linkedin')

    last = name.split()[-1].lower()
    evidence = last if len(last) >= 3 else name.split()[0].lower()

    for q in queries:
        try:
            results = smart_search(q) or []
        except Exception as e:
            logger.debug(f"smart_fill: search failed {q!r}: {e}")
            continue
        for r in results:
            url = _norm_linkedin(r)
            if not url:
                continue
            slug = url.rsplit("/", 1)[-1].lower()
            if evidence in r.lower() or evidence in slug:
                return url
    return None


# ── census ──────────────────────────────────────────────────────────────────


async def gap_census() -> dict:
    """How many eligible contacts are missing what — the audit view."""
    from app.database import async_session

    sql = text(
        f"""
        SELECT
          COUNT(*)::int AS eligible,
          COUNT(*) FILTER (WHERE {NO_ROLE})::int AS missing_role,
          COUNT(*) FILTER (WHERE {NO_LINKEDIN} AND {HAS_NAME})::int AS missing_linkedin,
          COUNT(*) FILTER (WHERE {NO_NAME})::int AS missing_name,
          COUNT(*) FILTER (WHERE COALESCE(phone,'') = '')::int AS missing_phone
        FROM contacts WHERE {ELIGIBLE}
        """
    )
    lead_sql = text(
        "SELECT COUNT(*)::int FROM lead_contacts "
        "WHERE is_saved = true AND COALESCE(linkedin,'') <> '' AND email IS NULL"
    )
    async with async_session() as session:
        row = (await session.execute(sql)).one()
        lead_email_gap = (await session.execute(lead_sql)).scalar() or 0
    return {
        "eligible": row.eligible,
        "missing_role": row.missing_role,
        "missing_linkedin": row.missing_linkedin,
        "missing_name": row.missing_name,
        "missing_phone": row.missing_phone,
        "lead_email_gap": lead_email_gap,
    }


async def _pick(where_extra: str, limit: int, category: str, dm_only: bool) -> list:
    from app.database import async_session

    clauses = [ELIGIBLE, where_extra]
    params: dict = {"lim": limit}
    if category:
        clauses.append("contact_category = :cat")
        params["cat"] = category
    if dm_only:
        clauses.append("is_decision_maker = true")
    sql = text(
        "SELECT id, first_name, last_name, organization, email FROM contacts "
        f"WHERE {' AND '.join(clauses)} {PRIORITY_ORDER} LIMIT :lim"
    )
    async with async_session() as session:
        return (await session.execute(sql, params)).all()


# ── dispatch ────────────────────────────────────────────────────────────────


async def run_smart_fill(
    role_limit: int = 0,
    linkedin_limit: int = 0,
    category: str = "",
    dm_only: bool = False,
    dry_run: bool = False,
    progress=print,
) -> dict:
    """Census, then dispatch capped passes in priority order. Returns summary."""
    from app.database import async_session

    census = await gap_census()
    summary: dict = {"census": census, "roles": {}, "linkedin": {}}

    # ── role pass → existing tier-2 deep enrich ─────────────────────────────
    if role_limit > 0:
        rows = await _pick(NO_ROLE, role_limit, category, dm_only)
        ids = [r.id for r in rows]
        progress(f"Role pass: {len(ids)} candidate(s) (buyers/P1 first)")
        if dry_run:
            summary["roles"] = {"planned": len(ids), "ids": ids[:20]}
        elif ids:
            from app.services.contact_tier2_enrichment import enrich_batch_deep

            done = ok = err = names = 0
            CHUNK = 10
            for i in range(0, len(ids), CHUNK):
                chunk = ids[i : i + CHUNK]
                results = await enrich_batch_deep(chunk, find_email=False)
                for r in results:
                    done += 1
                    if r.get("error"):
                        err += 1
                    else:
                        ok += 1
                        if r.get("name"):
                            names += 1
                progress(f"  roles {done}/{len(ids)}  ok={ok} err={err}")
            summary["roles"] = {"processed": done, "ok": ok, "errors": err}

    # ── linkedin pass → the new finder ──────────────────────────────────────
    if linkedin_limit > 0:
        rows = await _pick(f"{NO_LINKEDIN} AND {HAS_NAME}", linkedin_limit, category, dm_only)
        progress(f"LinkedIn pass: {len(rows)} candidate(s)")
        if dry_run:
            summary["linkedin"] = {"planned": len(rows), "ids": [r.id for r in rows][:20]}
        elif rows:
            found = miss = 0
            async with async_session() as session:
                for n, r in enumerate(rows, 1):
                    name = f"{r.first_name or ''} {r.last_name or ''}".strip()
                    url = find_linkedin_url(name, r.organization or "", r.email or "")
                    if url:
                        await session.execute(
                            text(
                                "UPDATE contacts SET linkedin_url = :u "
                                "WHERE id = :id AND COALESCE(linkedin_url,'') = ''"
                            ),
                            {"u": url, "id": r.id},
                        )
                        found += 1
                    else:
                        miss += 1
                    if n % 25 == 0:
                        await session.commit()
                        progress(f"  linkedin {n}/{len(rows)}  found={found}")
                    await asyncio.sleep(0.4)  # be polite to Serper
                await session.commit()
            summary["linkedin"] = {"processed": len(rows), "found": found, "not_found": miss}

    return summary
