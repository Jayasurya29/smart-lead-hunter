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


def _serper_linkedin_raw(query: str) -> list[str]:
    """Direct Serper call that returns 'title :: snippet :: link' for EVERY
    organic result -- including LinkedIn profiles whose snippet is empty (Google
    hides LinkedIn snippets, and the normal smart_search drops snippet-less
    results, which is why LinkedIn lookups were silently finding nothing)."""
    try:
        import httpx as _httpx
        from app.services.outreach.researcher import SERPER_API_KEY
    except Exception:
        return []
    if not SERPER_API_KEY:
        return []
    try:
        resp = _httpx.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": 10},
            timeout=10,
        )
        data = resp.json()
        out = []
        for item in (data.get("organic", []) or [])[:10]:
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            link = item.get("link", "")
            out.append(f"{title} :: {snippet} :: {link}")
        return out
    except Exception as e:
        logger.warning(f"smart_fill: serper linkedin search failed {query!r}: {e}")
        return []


def find_linkedin_url(name: str, org: str, email: str = "") -> Optional[str]:
    """The one resolver that didn't exist anywhere: locate a MISSING LinkedIn
    profile for a named contact. 1-2 Serper queries; a candidate URL is only
    accepted when the person's last-name token appears in the same result
    (or in the profile slug) -- guards against pinning the wrong person.
    """
    name = (name or "").strip()
    if not name:
        return None

    org = (org or "").strip()
    local = (email or "").split("@")[0] if "@" in (email or "") else ""
    domain_word = (email or "").split("@")[-1].split(".")[0] if "@" in (email or "") else ""
    anchor = org or domain_word

    # Several query shapes -- people list nicknames vs. formal names ("Tony"
    # vs "Anthony"), and a profile's CURRENT employer may differ from our org,
    # so we don't over-constrain. Quoted name first (precise), then unquoted,
    # then the email localpart (often firstname.lastname).
    parts = name.split()
    first, last = parts[0], parts[-1]
    queries = [f'"{name}" {anchor} site:linkedin.com/in'.strip()]
    if anchor:
        queries.append(f'"{name}" {anchor} linkedin')
        queries.append(f"{first} {last} {anchor} linkedin")  # unquoted -> nickname tolerant
    if local and "." in local:
        queries.append(f'{local.replace(".", " ")} {anchor} linkedin')

    last_l = last.lower()
    first_l = first.lower()
    anchor_l = anchor.lower()

    def accept(result_text: str, slug: str) -> bool:
        """Accept a candidate only with corroborating evidence. The strongest
        signal is the LAST name appearing in the result or slug. But LinkedIn
        often ABBREVIATES surnames -- "Ruby Ozretic" shows as "Ruby O." with
        slug "rubyozr". So we also accept when the slug encodes first-name +
        a PREFIX of the last name (>=3 chars), which together with the org
        anchor is decisive. Still rejects a different 'J. Baylor'."""
        rl = result_text.lower()
        slug_compact = slug.replace("-", "").replace("_", "")
        last_full = len(last_l) >= 3 and (last_l in rl or last_l in slug)
        # firstname + last-name-prefix encoded in the slug (rubyozr, johnsmi...)
        last_prefix = (
            len(first_l) >= 2 and len(last_l) >= 3 and (first_l + last_l[:3]) in slug_compact
        )
        last_ok = last_full or last_prefix
        if not last_ok:
            return False
        first_ok = (
            first_l in rl
            or first_l in slug
            or (
                len(first_l) >= 3
                and any(
                    w.startswith(first_l[:3]) or first_l.startswith(w[:3])
                    for w in rl.split()
                    if w.isalpha() and len(w) >= 3
                )
            )
        )
        anchor_ok = bool(anchor_l) and anchor_l in rl
        # a slug-prefix match already implies the first name, so anchor alone is
        # enough corroboration there; a full-lastname match still wants first
        # name OR anchor.
        if last_prefix:
            return True
        return last_ok and (first_ok or anchor_ok)

    for q in queries:
        try:
            results = _serper_linkedin_raw(q) or []
        except Exception as e:
            logger.debug(f"smart_fill: search failed {q!r}: {e}")
            continue
        for r in results:
            url = _norm_linkedin(r)
            if not url:
                continue
            slug = url.rsplit("/", 1)[-1].lower()
            if accept(r, slug):
                return url
    return None


def find_linkedin_debug(name: str, org: str, email: str = "") -> dict:
    """Same search as find_linkedin_url(), but returns the full picture for the
    UI activity log: the accepted URL (if any) plus every linkedin.com/in
    candidate the search surfaced, so 'not found' is explainable -- you can see
    whether the search returned the right profile and the guard rejected it, or
    whether the profile never showed up at all.

    Returns {url: str|None, candidates: [str], queries: int}.
    """
    name = (name or "").strip()
    if not name:
        return {"url": None, "candidates": [], "queries": 0}
    try:
        from app.services.outreach.researcher import SERPER_API_KEY

        if not SERPER_API_KEY:
            return {"url": None, "candidates": [], "queries": 0}
    except Exception:
        return {"url": None, "candidates": [], "queries": 0}

    org_s = (org or "").strip()
    local = (email or "").split("@")[0] if "@" in (email or "") else ""
    domain_word = (email or "").split("@")[-1].split(".")[0] if "@" in (email or "") else ""
    anchor = org_s or domain_word
    parts = name.split()
    first, last = parts[0], parts[-1]
    queries = [f'"{name}" {anchor} site:linkedin.com/in'.strip()]
    if anchor:
        queries.append(f'"{name}" {anchor} linkedin')
        queries.append(f"{first} {last} {anchor} linkedin")
    if local and "." in local:
        queries.append(f'{local.replace(".", " ")} {anchor} linkedin')

    accepted = find_linkedin_url(name, org, email)
    seen: list[str] = []
    for q in queries:
        try:
            results = _serper_linkedin_raw(q) or []
        except Exception:
            continue
        for r in results:
            url = _norm_linkedin(r)
            if url and url not in seen:
                seen.append(url)
        if accepted:
            break  # we already have the answer; one query of candidates is plenty
    return {"url": accepted, "candidates": seen[:5], "queries": len(queries)}


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
                        progress(f"  [{n}/{len(rows)}] {name or '#'+str(r.id)} -> {url}")
                    else:
                        miss += 1
                        progress(f"  [{n}/{len(rows)}] {name or '#'+str(r.id)} -> no match")
                    if n % 25 == 0:
                        await session.commit()
                    await asyncio.sleep(0.4)  # be polite to Serper
                await session.commit()
            summary["linkedin"] = {"processed": len(rows), "found": found, "not_found": miss}

    return summary
