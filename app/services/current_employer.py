"""current_employer.py -- "where do they work now" for coverage freshness.

Proven approach (validated in scripts/probe_serper_batch.py -- 9-10/10 on real
stale buyers, right person + plausible employer):

  PRIMARY -- Serper (your flat-rate Google API, no per-lookup credit):
      Two cheap queries (the LinkedIn slug, then name+org), hand the REAL result
      snippets to the LLM to READ. Because the model only sees actual SERP
      snippets, it cannot invent a namesake (the failure mode that killed
      grounding). Returns current employer + title.

  FALLBACK -- Wiza profile-only reveal ("enrichment_level":"none", 1 credit,
      no email/phone spend): structured company/title keyed to the EXACT
      LinkedIn URL. Used on demand (a manual "Confirm with Wiza" button) to
      verify a move or when Serper is unsure. Costs 1 credit per reveal.

Grounding was tested head-to-head and DROPPED (scripts/probe_grounding_batch.py):
it resolves the contact's LinkedIn to the company page and refuses, or invents a
different person with the same name. Do not reintroduce it here.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# [patch_seat_successor_queries] bound Serper spend on the successor search.
SEAT_SUCCESSOR_MAX_QUERIES = 10


def _slug(linkedin: str) -> str:
    if linkedin and "/in/" in linkedin:
        return linkedin.split("/in/")[-1].strip("/")
    return ""


def _parse_verdict(text: str) -> dict:
    """Parse the LLM's 'CURRENT: <emp> | <title>' (or 'CURRENT: unknown')."""
    line = ""
    for ln in (text or "").splitlines():
        if ln.strip().upper().startswith("CURRENT:"):
            line = ln.strip()
            break
    if not line:
        line = (text or "").strip()
    body = re.sub(r"(?i)^current:\s*", "", line).strip()
    if not body or body.lower().startswith("unknown"):
        return {"found": False, "current_employer": "", "current_title": ""}
    parts = [p.strip() for p in body.split("|")]
    return {
        "found": bool(parts and parts[0]),
        "current_employer": parts[0] if parts else "",
        "current_title": parts[1] if len(parts) > 1 else "",
    }


async def serper_current_employer(name: str, org: str = "", linkedin: str = "") -> dict:
    """Serper-based current employer lookup. Returns:
    {found, current_employer, current_title, evidence, citations, source}.
    """
    out = {
        "found": False,
        "current_employer": "",
        "current_title": "",
        "evidence": "",
        "citations": [],
        "source": "serper",
    }
    name = (name or "").strip()
    if not name:
        return out

    try:
        from app.services.contact_enrichment import _search_serper
        from app.services.ai_client import ai_generate
        import httpx
    except Exception as e:
        logger.warning(f"current_employer: import failed: {e}")
        return out

    # [slug_first_v2] WITH a slug, that slug UNIQUELY identifies this person --
    # query the slug ONLY. No bare-name query (namesakes: a different 'Maria
    # Davila' at Southern California Edison, etc.) and no STALE on-file org
    # ('Loews' fuzzy-matched 'Lowe's'). Only WITHOUT a slug do we fall back to
    # name + on-file org (the sole disambiguator then).
    queries = []
    slug = _slug(linkedin)
    if slug:
        queries.append(f"{slug} linkedin")
    else:
        queries.append(f'"{name}" {org} linkedin'.strip())

    snippets: list[str] = []
    cites: list[str] = []
    for q in queries:
        try:
            results = await _search_serper(q, max_results=8)
        except Exception as e:
            logger.warning(f"current_employer: serper failed for {q!r}: {e}")
            results = []
        for r in results:
            t, sn, ln = r.get("title", ""), r.get("snippet", ""), r.get("url", "")
            snippets.append(f"{t} -- {sn} ({ln})")
            if ln:
                cites.append(ln)

    blob = "\n".join(f"- {s}" for s in snippets if s.strip())[:6000]
    if not blob:
        return out

    # [slug_first_v2] anchor the read on the EXACT LinkedIn profile when we have
    # the slug, so a namesake snippet (different person, same name) can't win.
    prompt = (
        f"Below are real Google search-result snippets about {name}"
        + (
            f". The CORRECT person is the one whose LinkedIn URL contains '/in/{slug}'"
            " -- use ONLY snippets about that exact profile and IGNORE any same-name"
            " person with a different LinkedIn URL."
            if slug
            else (f" (on file at {org})" if org else "")
        )
        + ". Using ONLY these snippets, identify THIS specific person's MOST RECENT "
        "employer and job title (prefer the newest dated/most-recent mention). "
        "IMPORTANT: a LinkedIn headline (the 'Name - <Title> at <Company>' line, or "
        "'<Title> at <Company>' shown right under the name) is the person's CURRENT "
        "role and OUTRANKS any older role mentioned in the body text or with an older "
        "date range. If a snippet shows a current headline, use THAT employer/title. "
        "Answer in EXACTLY one line:\n"
        "CURRENT: <employer> | <title>\n"
        "If the snippets don't clearly identify this specific person's current/recent "
        "job, answer exactly: CURRENT: unknown\n\n"
        f"SNIPPETS:\n{blob}"
    )
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            ans = await ai_generate(c, prompt, temperature=0.0, max_tokens=120)
    except Exception as e:
        logger.warning(f"current_employer: llm read failed for {name!r}: {e}")
        ans = ""

    verdict = _parse_verdict(ans or "")
    out.update(verdict)
    out["evidence"] = (ans or "").strip()[:200]
    out["citations"] = cites[:6]
    return out


async def wiza_current_employer(
    linkedin: str = "", name: str = "", org: str = "", domain: str = ""
) -> dict:
    """Wiza PROFILE-ONLY reveal (enrichment_level 'none', 1 credit, no email/phone).
    Structured + LinkedIn-keyed. Returns:
    {found, current_employer, current_title, company_domain, citations, source, credits}.
    """
    out = {
        "found": False,
        "current_employer": "",
        "current_title": "",
        "company_domain": "",
        "citations": [],
        "source": "wiza",
        "credits": None,
    }
    try:
        import httpx
        from app.services.wiza_enrichment import (
            _get_api_key,
            _normalize_linkedin_url,
            _post_reveal_and_poll,
        )
    except Exception as e:
        logger.warning(f"current_employer(wiza): import failed: {e}")
        return out

    key = _get_api_key()
    if not key:
        logger.warning("current_employer(wiza): no WIZA_API_KEY")
        return out

    reveal: Optional[dict] = None
    if linkedin:
        url = _normalize_linkedin_url(linkedin)
        if url:
            reveal = {"profile_url": url}
    if reveal is None and name and (org or domain):
        reveal = {"full_name": name.strip()}
        if domain:
            reveal["domain"] = domain.strip()
        else:
            reveal["company"] = org.strip()
    if reveal is None:
        logger.info("current_employer(wiza): insufficient input")
        return out

    body = {"individual_reveal": reveal, "enrichment_level": "none"}  # 1 credit, profile only
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            data = await _post_reveal_and_poll(client, headers, body, name or linkedin or "?")
    except Exception as e:
        logger.warning(f"current_employer(wiza): reveal failed: {e}")
        data = None

    if not data:
        return out
    company = (data.get("company") or "").strip()
    out.update(
        {
            "found": bool(company),
            "current_employer": company,
            "current_title": (data.get("title") or "").strip(),
            "company_domain": (data.get("company_domain") or "").strip(),
            "citations": [data.get("linkedin_profile_url") or linkedin]
            if (linkedin or data.get("linkedin_profile_url"))
            else [],
            "credits": data.get("credits"),
        }
    )
    return out


async def _judge_relationship(org: str, found: str) -> str:
    """Classify the on-file org vs the found employer: 'same' | 'parent' | 'moved'.
    Cheap _norm match short-circuits to 'same'; only ambiguous pairs hit the LLM
    (so a punctuation/parent-naming difference like 'Ritz-Carlton Club St. Thomas'
    vs 'Ritz-Carlton Hotel Company' is judged 'same', not a false 'moved')."""

    def _norm(s: str) -> str:
        s = (s or "").lower()
        for j in (
            " the ",
            "the ",
            " inc",
            " llc",
            " ltd",
            " corp",
            " company",
            " hotel",
            " hotels",
            " resort",
            " resorts",
            " & ",
            " and ",
            ",",
        ):
            s = s.replace(j, " ")
        return " ".join(s.split())

    if not (org and found):
        return "moved" if found else "unknown"
    if _norm(org) == _norm(found):
        return "same"
    # token-overlap fast path: strong shared brand token -> treat as same/parent
    a, b = set(_norm(org).split()), set(_norm(found).split())
    if a and b and len(a & b) >= 1 and (a <= b or b <= a):
        return "same"

    try:
        import httpx
        from app.services.ai_client import ai_generate

        prompt = (
            "Two company names for one person's employer. Decide their relationship.\n"
            f"ON FILE: {org}\nFOUND NOW: {found}\n"
            "Answer with ONE word only:\n"
            "SAME  - same employer (incl. rename, punctuation, or property-vs-parent "
            "of the SAME company/brand, e.g. a specific Ritz-Carlton property vs "
            "Ritz-Carlton Hotel Company, or 'X Resort & Villas' vs 'X Resort + Villas')\n"
            "MOVED - a genuinely DIFFERENT company (they changed jobs)\n"
            "Answer SAME or MOVED."
        )
        async with httpx.AsyncClient(timeout=40) as c:
            ans = await ai_generate(c, prompt, temperature=0.0, max_tokens=8)
        verdict = (ans or "").strip().upper()
        return "same" if verdict.startswith("SAME") else "moved"
    except Exception as e:
        logger.warning(f"current_employer: relationship judge failed: {e}")
        # conservative on failure: don't cry 'moved' on a maybe-same pair
        return "moved" if not (a & b) else "same"


async def find_current_employer(
    name: str,
    org: str = "",
    linkedin: str = "",
    domain: str = "",
    use_wiza: bool = False,
) -> dict:
    """Orchestrator. Serper first (free). If use_wiza (manual button) -> also run
    Wiza (1 credit) and prefer its structured answer when it found a company.
    'moved'/'same' decided by _judge_relationship (LLM for ambiguous pairs), so
    renames / parent-vs-property naming don't produce false 'moved' flags.
    """
    result = await serper_current_employer(name, org, linkedin)
    if use_wiza:
        w = await wiza_current_employer(linkedin=linkedin, name=name, org=org, domain=domain)
        if w.get("found"):
            result = w  # structured + exact-LinkedIn beats snippet read

    emp = result.get("current_employer") or ""
    rel = await _judge_relationship(org, emp) if emp else "unknown"
    result["relationship"] = rel
    result["moved"] = rel == "moved"
    result["same"] = rel == "same"
    return result


# [phase3_fill_the_seat]
async def find_seat_successor(
    org: str, title: str, former_holder: str = "", location: str = ""
) -> dict:
    """Phase 3 -- 'fill the seat'. Given a property/company and the role that was
    just vacated (the former_holder moved on), find who CURRENTLY holds that
    title now. Reuses the proven title-currency approach from the lead-gen
    researcher, but standalone (Serper + ai_generate, no researcher `state`).

    STRICT: only returns a name explicitly supported by appointment/news
    snippets. Absence of a mention is NOT a successor -- returns found=False.
    Never fabricates an email or a name.

    Returns:
      {found, successor_name, successor_title, evidence, citations, source}
    """
    out = {
        "found": False,
        "successor_name": "",
        "successor_title": title or "",
        "evidence": "",
        "citations": [],
        "source": "serper",
    }
    org = (org or "").strip()
    title = (title or "").strip()
    if not org or not title:
        return out

    try:
        from app.services.contact_enrichment import _search_serper
        from app.services.ai_client import ai_generate
        import httpx
    except Exception as e:
        logger.warning(f"seat_successor: import failed: {e}")
        return out

    # Appointment-news queries: cover "<Name> appointed <Title>" and
    # Plain "<org> <role title>" queries across same-department title variants.
    # LinkedIn surfaces the current holder for these far better than
    # appointment-news phrasing.
    _t = title.lower()
    if any(k in _t for k in ("f&b", "food", "beverage", "culinary", "restaurant", "outlet")):
        _titles = [
            "Food and Beverage Director",
            "Director of Food and Beverage",
            "Director of Outlets",
            "Assistant Director of Outlets",
            "F&B Manager",
            "Food and Beverage Manager",
        ]
    elif any(k in _t for k in ("human res", "hr", "people", "talent")):
        _titles = [
            "Director of Human Resources",
            "Human Resources Manager",
            "Director of People and Culture",
            "HR Director",
        ]
    elif any(k in _t for k in ("sales", "revenue", "commercial")):
        _titles = ["Director of Sales", "Director of Sales and Marketing", "Revenue Manager"]
    elif "general manager" in _t or "gm" in _t:
        _titles = ["General Manager", "Hotel Manager"]
    else:
        _titles = [title, f"Director of {title}"]
    # [patch_seat_successor_queries] diversify query shapes: synonym
    # coverage ("{org} {title}" per variant) + appointment news +
    # LinkedIn current-holder + leadership-page catch-all. De-duped,
    # Serper-spend bounded.
    _loc = (location or "").strip()
    _qorg = f"{org} {_loc}" if _loc else org
    queries = [f"{_qorg} {t}" for t in _titles]
    _primary = _titles[0] if _titles else title
    queries += [
        f'"{org}" "{_primary}" appointed OR named OR joins 2025 2026',
        f'site:linkedin.com/in "{_primary}" "{org}"',
        f"{org} leadership team executive committee",
    ]
    _seen_q: set[str] = set()
    queries = [q for q in queries if q.strip() and not (q in _seen_q or _seen_q.add(q))][
        :SEAT_SUCCESSOR_MAX_QUERIES
    ]
    snippets: list[str] = []
    cites: list[str] = []
    for q in queries:
        try:
            results = await _search_serper(q, max_results=6)
        except Exception as e:
            logger.warning(f"seat_successor: serper failed for {q!r}: {e}")
            results = []
        for r in results:
            t, sn, ln = r.get("title", ""), r.get("snippet", ""), r.get("url", "")
            snippets.append(f"{t} -- {sn} ({ln})")
            if ln:
                cites.append(ln)

    blob = "\n".join(f"- {s}" for s in snippets if s.strip())[:6000]
    if not blob:
        return out

    fh = (
        f"\nThe previous holder of this role was {former_holder}. {former_holder} HAS LEFT "
        f"this role -- DO NOT return {former_holder} as the answer under any circumstances. "
        f"You are looking for the DIFFERENT person who holds the role now.\n"
        if former_holder
        else ""
    )
    prompt = (
        "You are identifying who CURRENTLY holds a specific role (or the closest "
        "equivalent senior role in the same department) at a specific organization, "
        "as of 2025-2026, using ONLY the real Google snippets below.\n"
        f"ORGANIZATION: {org}\n"
        + (f"PROPERTY LOCATION: {location}\n" if (location or "").strip() else "")
        + f"ROLE / TITLE: {title}\n"
        f"{fh}\n"
        "RULES:\n"
        "1. Return who holds this role NOW at THIS EXACT property. If the exact title "
        "isn't named but a clear holder of the SAME department/function at this property "
        "is (e.g. 'Director of Outlets' for an F&B seat), return that person.\n"
        "2. ON-PROPERTY ONLY. Do NOT return Complex / Area / Regional / Corporate / "
        "Multi-property / above-property executives (e.g. 'Complex Managing Director', "
        "'Area General Manager', 'Regional VP', 'Corporate Director'). Those sit ABOVE "
        "the property and are NOT the seat holder. If the only named person holds such a "
        "role, return unknown.\n"
        "3. THIS PROPERTY ONLY. Only return someone explicitly tied to this property. "
        "Ignore names attached to a DIFFERENT property, even a same-brand sister hotel "
        "in another city.\n"
        "4. Prefer the MOST RECENT dated mention. A name on an OLD date range (e.g. "
        "'2022-2024') is a PAST holder -- do not return them.\n"
        "5. If the holder is described as 'interim' or 'acting', return them and append "
        "' (interim)' to their title.\n"
        "6. A job-board / careers / salary page (Indeed, SimplyHired, ZipRecruiter, "
        "Glassdoor) that merely lists a posting is NOT proof. Weight appointment "
        "announcements, staff/leadership pages, and dated news higher.\n"
        "7. Never return the previous holder named above.\n"
        "8. If no snippet clearly names a CURRENT on-property holder, say unknown. "
        "Absence is not a guess.\n\n"
        "Answer in EXACTLY one line:\n"
        "SUCCESSOR: <full name> | <their exact title>\n"
        "or exactly: SUCCESSOR: unknown\n\n"
        f"SNIPPETS:\n{blob}"
    )
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            ans = await ai_generate(c, prompt, temperature=0.0, max_tokens=120)
    except Exception as e:
        logger.warning(f"seat_successor: llm read failed for {org}/{title}: {e}")
        ans = ""

    line = (ans or "").strip()
    body = ""
    for ln in line.splitlines():
        if ln.strip().upper().startswith("SUCCESSOR:"):
            body = ln.split(":", 1)[1].strip()
            break
    if not body or body.lower() == "unknown":
        out["evidence"] = line[:200]
        out["citations"] = cites[:6]
        return out

    if "|" in body:
        nm, ti = body.split("|", 1)
        successor_name = nm.strip()
        successor_title = ti.strip() or title
    else:
        successor_name = body.strip()
        successor_title = title

    # Guard: never return the person who just left as their own successor.
    # Catches exact match, substring either direction, and shared first+last name
    # (handles 'Taylor Smith' vs 'Taylor Smith, SHRM-CP' etc).
    if former_holder:
        fn = former_holder.strip().lower()
        sn = successor_name.strip().lower()
        fset = set(fn.split())
        sset = set(sn.split())
        same_person = (
            sn == fn
            or (sn and fn and (sn in fn or fn in sn))
            or (len(fset) >= 2 and fset <= sset)
            or (len(sset) >= 2 and sset <= fset)
        )
        if same_person:
            out["evidence"] = f"(rejected former holder: {line[:160]})"
            out["citations"] = cites[:6]
            return out

    out["found"] = bool(successor_name)
    out["successor_name"] = successor_name
    out["successor_title"] = successor_title
    out["evidence"] = line[:200]
    out["citations"] = cites[:6]
    return out


# [phase3_fill_the_seat_apply]
def _norm_person_name(s: str) -> str:
    """Lightweight person-name normalization for successor dedup (suffix-tolerant)."""
    s = (s or "").lower().strip()
    for ch in (",", ".", "'", '"'):
        s = s.replace(ch, " ")
    drop = {"jr", "sr", "ii", "iii", "shrm", "cp", "mba", "phd", "cpa"}
    return " ".join(t for t in s.split() if t not in drop)


def _same_person(a: str, b: str) -> bool:
    """True if two name strings are plausibly the same person: same surname
    AND a genuine first-name overlap -- equal, a shared 4+ char prefix
    (Jenny/Jennifer), or one side is just an initial matching the other
    (J./Jennifer). A mere shared first LETTER is NOT enough (John != Jane)."""
    import re as _re

    ta = [t for t in _re.sub(r"[^a-z ]", " ", (a or "").lower()).split() if t]
    tb = [t for t in _re.sub(r"[^a-z ]", " ", (b or "").lower()).split() if t]
    if not ta or not tb or ta[-1] != tb[-1]:
        return False
    fa, fb = ta[0], tb[0]
    if fa == fb:
        return True
    if len(fa) <= 1 or len(fb) <= 1:
        return fa[0] == fb[0]
    return fa[:4] == fb[:4]


def _is_brandish(org: str) -> bool:
    """Heuristic: a multi-location brand likely to be ambiguous without a
    location (e.g. 'Great Wolf Lodge'). Reuses the known-brand allowlist."""
    try:
        from app.services.org_classifier import KNOWN_PROPERTY_BRANDS

        n = " ".join((org or "").lower().split())
        return any(b in n for b in KNOWN_PROPERTY_BRANDS)
    except Exception:
        return False


async def _mark_seat(session, contact_id: int, status: str) -> None:
    """Record the seat-search outcome on the vacated 'former' affiliation
    (migration 046). Bumps attempts + searched_at. Best-effort."""
    from sqlalchemy import text as _sql
    from app.services.contact_resolver import is_lead_id as _il, real_id as _ri

    pt = "lead_contact" if _il(contact_id) else "contact"
    pid = _ri(contact_id) if _il(contact_id) else contact_id
    try:
        await session.execute(
            _sql(
                "UPDATE contact_affiliations SET seat_status=:s, "
                "seat_searched_at=NOW(), seat_search_attempts=COALESCE(seat_search_attempts,0)+1 "
                "WHERE person_type=:pt AND person_id=:pid AND relationship='former'"
            ),
            {"s": status, "pt": pt, "pid": pid},
        )
    except Exception:
        pass


async def _harvest_property_leaders(session, former_org, location, holder, hid_row, lid_row):
    """No-successor fallback: pull the property's current department heads and
    auto-create the ones not already on file. Returns {created, leaders}."""
    from sqlalchemy import text as _sql

    summary = {"created": 0, "leaders": []}
    if not (hid_row or lid_row):
        return summary  # unknown property -> nowhere to file; skip
    res = await find_property_leaders(former_org, location=location, exclude=holder)
    leaders = res.get("leaders", [])
    summary["leaders"] = leaders
    if not leaders:
        return summary
    col = "existing_hotel_id" if hid_row else "lead_id"
    pid = (hid_row or lid_row).id
    src = (res.get("citations") or ["property_leaders"])[0]
    existing = (
        await session.execute(
            _sql(f"SELECT name FROM lead_contacts WHERE {col} = :pid"), {"pid": pid}
        )
    ).all()
    have = {_norm_person_name(r.name) for r in existing}
    for ld in leaders:
        key = _norm_person_name(ld["name"])
        if not key or key in have:
            continue
        have.add(key)
        await session.execute(
            _sql(
                f"INSERT INTO lead_contacts "
                f"(name, title, organization, {col}, scope, confidence, is_saved, "
                f" found_via, source_detail, evidence_url, created_at, updated_at) "
                f"VALUES (:nm, :ti, :org, :pid, 'hotel_specific', 'low', TRUE, "
                f" 'leadership_harvest', :sd, :url, NOW(), NOW())"
            ),
            {
                "nm": ld["name"],
                "ti": ld.get("title") or "",
                "org": former_org,
                "pid": pid,
                "sd": f"On-property leader harvested after {holder or 'a contact'} left; via {src}",
                "url": src if str(src).startswith("http") else None,
            },
        )
        summary["created"] += 1
    return summary


# Parent hotel companies (bare or with a CORPORATE suffix) that aren't a single
# searchable property. 'Hilton' / 'Hilton Worldwide' are vague; 'Hilton Orlando'
# (brand + city) is a specific property.
_PARENT_BASES = (
    "ihg",
    "marriott",
    "hilton",
    "hyatt",
    "accor",
    "wyndham",
    "choice",
    "radisson",
    "best western",
)
_CORP_SUFFIXES = (
    "worldwide",
    "international",
    "hotels",
    "resorts",
    "hotels & resorts",
    "hotels and resorts",
    "group",
    "hotels group",
    "inc",
    "corporation",
)
_EXACT_PARENTS = (
    "intercontinental hotels group",
    "marriott international",
    "hilton worldwide",
    "hyatt hotels",
)


def _is_vague_property(org: str) -> bool:
    """True if the org is a parent company / domain / bare multi-brand that
    can't pin one property (IHG, Marriott, Hilton Worldwide, a raw domain).
    A brand + city (Hilton Orlando) is a specific property and returns False."""
    n = " ".join((org or "").lower().split())
    if not n:
        return True
    if "." in n and " " not in n:  # bare domain (ihg.com)
        return True
    if n in _PARENT_BASES or n in _EXACT_PARENTS:
        return True
    for b in _PARENT_BASES:
        if n.startswith(b + " ") and n[len(b) + 1 :].strip() in _CORP_SUFFIXES:
            return True
    return False


async def resolve_former_property(person: str, title: str, vague_org: str) -> dict:
    """Grounded: given a person + the role they held + a vague employer (IHG),
    find the SPECIFIC hotel/property and city they worked at. Returns
    {found, property, location}. Reads their public profile/news."""
    out = {"found": False, "property": "", "location": ""}
    person = (person or "").strip()
    if not person:
        return out
    try:
        import httpx
        from app.services.contact_enrichment import (
            _build_grounding_url,
            _CONTACT_GROUNDING_TIMEOUT_S,
        )
        from app.services.gemini_client import get_gemini_headers
        from app.services.ai_client import _get_config, _ensure_init

        _ensure_init()
        cfg = _get_config()
        url, _ = _build_grounding_url(cfg["vertex_project_id"], cfg["model"])
        headers = get_gemini_headers()
    except Exception as e:
        logger.warning(f"resolve_former_property: setup failed: {type(e).__name__}: {e}")
        return out

    prompt = (
        f"{person} previously worked as {title} at {vague_org} (a parent hotel "
        f"company with many properties). Using their public LinkedIn/profile and "
        f"news, identify the SPECIFIC hotel/property and the CITY where they held "
        f"that role -- e.g. 'InterContinental New York Times Square, New York'. "
        f"Output exactly one line: PROPERTY | CITY STATE. If you cannot tell the "
        f"specific property, output exactly: unknown."
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 200,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    try:
        async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
            resp = await gc.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        cand = resp.json()["candidates"][0]
        parts = (cand.get("content") or {}).get("parts") or []
        text_out = re.sub(r"[*`]+", "", ((parts[0].get("text") if parts else "") or "").strip())
    except Exception as e:
        _st = getattr(getattr(e, "response", None), "status_code", "")
        logger.warning(
            f"resolve_former_property ERROR {person}/{vague_org}: {type(e).__name__} {_st}".rstrip()
        )
        return out

    line = ""
    for ln in (text_out or "").splitlines():
        if "|" in ln:
            line = ln.strip()
            break
    if not line or line.lower() == "unknown":
        return out
    prop, _, loc = line.partition("|")
    prop, loc = prop.strip(), loc.strip()
    if not prop or prop.lower() == "unknown":
        return out
    out.update(found=True, property=prop, location=loc)
    logger.info(f"[phase3] resolved vague '{vague_org}' -> '{prop}' ({loc}) for {person}")
    return out


async def apply_seat_successor(session, contact_id: int) -> dict:
    """Phase 3 APPLY. For a confirmed mover (contact with a 'former' affiliation),
    find who holds the vacated seat now and persist the finding:

      - ALWAYS: write a 'successor' affiliation edge on the ORIGINAL contact
        (the warm-path link: "<successor> now holds <title> at <org>,
        replaced <mover>"). Idempotent on (person, account_name, relationship).
      - IF the former property resolves to a known existing_hotel / potential_lead:
        find-or-create a lead_contact stub for the successor there (deduped by
        normalized name; fill-empty on match), marked unverified
        (found_via='successor_discovery', confidence='low').
      - ELSE: note-only (the link above); no orphan stub (lead_id XOR
        existing_hotel_id forbids it).

    Returns a dict describing what happened. Caller commits.
    Never fabricates an email. Never auto-promotes priority.
    """
    from sqlalchemy import text

    out = {
        "found": False,
        "successor_name": "",
        "successor_title": "",
        "former_org": "",
        "former_holder": "",
        "action": "none",
        "stub_id": None,
        "property_kind": None,
        "property_id": None,
        "citations": [],
    }

    from app.services.contact_resolver import (
        is_lead_id as _is_lead,
        real_id as _real_id,
    )  # [patch_leadgen_successor]

    row = (
        await session.execute(
            text(
                "SELECT COALESCE(NULLIF(a.title,''), c.title) AS title, c.organization AS new_org, "
                "  COALESCE(NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), ''), "
                "           c.display_name, '') AS holder, "
                "  a.account_name AS former_org "
                "FROM contacts c "
                "JOIN contact_affiliations a "
                "  ON a.person_id = c.id AND a.person_type='contact' AND a.relationship='former' "
                "WHERE c.id = :id "
                "ORDER BY a.created_at DESC NULLS LAST LIMIT 1"
            ),
            {"id": contact_id},
        )
    ).first()
    if row is None and _is_lead(contact_id):  # [patch_leadgen_successor] lead-gen mover
        row = (
            await session.execute(
                text(
                    "SELECT COALESCE(NULLIF(a.title,''), lc.title) AS title, lc.organization AS new_org, lc.name AS holder, "
                    "  a.account_name AS former_org "
                    "FROM lead_contacts lc "
                    "JOIN contact_affiliations a "
                    "  ON a.person_id = lc.id AND a.person_type='lead_contact' AND a.relationship='former' "
                    "WHERE lc.id = :id "
                    "ORDER BY a.created_at DESC NULLS LAST LIMIT 1"
                ),
                {"id": _real_id(contact_id)},
            )
        ).first()
    if not row:
        out["action"] = "not_a_mover"
        return out

    title = (row.title or "").strip()
    former_org = (row.former_org or "").strip()
    holder = (row.holder or "").strip()
    out["former_org"] = former_org
    out["former_holder"] = holder
    if not title or not former_org:
        out["action"] = "insufficient_seat"
        return out

    # [phase3_v2] Org-type gate: only search actual hospitality PROPERTIES.
    # An out-of-industry org (Colliers, Email Outreach) or a third-party
    # operator/owner (Hartling Group, Langham) is never a property seat to
    # refill -- skip the search instead of manufacturing junk contacts.
    try:
        from app.services.org_classifier import classify_org_type_offline

        _kind = classify_org_type_offline(former_org)
    except Exception:
        _kind = "unknown"
    if _kind in ("out_of_industry", "operator"):
        await _mark_seat(session, contact_id, "searched_unknown")
        out["action"] = f"skipped_{_kind}"
        return out

    # [phase3_v2] Resolve the former property to a known hotel/lead FIRST so we
    # can (a) pull its city/state to disambiguate multi-location brands and
    # (b) know where to file the successor. property_kind/id reused below.
    _hid = (
        await session.execute(
            text("SELECT id, city, state FROM existing_hotels WHERE lower(name)=lower(:o) LIMIT 1"),
            {"o": former_org},
        )
    ).first()
    _lid = None
    if not _hid:
        _lid = (
            await session.execute(
                text(
                    "SELECT id, city, state FROM potential_leads WHERE lower(hotel_name)=lower(:o) LIMIT 1"
                ),
                {"o": former_org},
            )
        ).first()
    _row_loc = _hid or _lid
    location = " ".join(
        p for p in ((_row_loc.city if _row_loc else ""), (_row_loc.state if _row_loc else "")) if p
    ).strip()

    # [phase3_resolve] If the former org is a vague parent brand/domain and we
    # have no location, resolve the SPECIFIC hotel+city first (IHG -> 'Inter-
    # Continental New York'). Without this, searching a parent company yields
    # nothing -- there is no single successor across thousands of properties.
    if not location and _is_vague_property(former_org):
        _rp = await resolve_former_property(holder, title, former_org)
        if _rp.get("found"):
            former_org = _rp["property"]
            location = _rp.get("location", "")
            out["resolved_property"] = former_org
            out["resolved_location"] = location
        else:
            # couldn't pin a property -> don't search a parent company blindly
            await _mark_seat(session, contact_id, "ambiguous")
            out["action"] = "ambiguous_parent"
            return out

    # [phase3_v2] GROUNDED-FIRST, snippet fallback. Grounded+location beat the
    # snippet path on every live test; snippets only cover the rare case where
    # grounding has no citations.
    res = await grounded_seat_successor(
        org=former_org, title=title, former_holder=holder, location=location
    )
    _engine = "grounded"
    if res.get("error"):
        logger.info(
            f"[phase3] grounded errored ({res['error']}) for {former_org}/{title} -> trying snippet"
        )
    if not res.get("found"):
        res = await find_seat_successor(
            org=former_org, title=title, former_holder=holder, location=location
        )
        _engine = "snippet"
    if res.get("found"):
        logger.info(
            f"[phase3] successor via {_engine}: {res.get('successor_name')} "
            f"for {former_org}/{title} (loc={location or '-'})"
        )
    if not res.get("found"):
        # No successor -> HARVEST the property's other current department heads.
        harvested = await _harvest_property_leaders(
            session, former_org, location, holder, _hid, _lid
        )
        seat_outcome = (
            "ambiguous" if (not location and _is_brandish(former_org)) else "searched_unknown"
        )
        await _mark_seat(session, contact_id, seat_outcome)
        out["action"] = "harvested" if harvested.get("created") else "no_successor"
        out["citations"] = res.get("citations", [])
        out["harvested"] = harvested
        return out

    succ = res["successor_name"]
    succ_title = res.get("successor_title") or title
    src = res["citations"][0] if res.get("citations") else "seat_successor"
    out.update(
        found=True,
        successor_name=succ,
        successor_title=succ_title,
        citations=res.get("citations", []),
    )

    # NOTE: contact_affiliations cannot hold a 'successor' edge -- its CHECK
    # constraints restrict relationship to {employed_by, stationed_at, covers,
    # former} and account_type to {existing_hotel, potential_lead,
    # management_company}. So we do NOT write a successor affiliation row.
    # The warm-path link ("X replaced Taylor") is carried on the created stub's
    # source_detail (below) and returned to the UI. No schema change needed.
    note = f"{succ} now holds {title} at {former_org}" + (f" (replaced {holder})" if holder else "")
    out["link_note"] = note

    # 2) resolve the former property; stub only if it's a known hotel/lead.
    hid = (
        await session.execute(
            text("SELECT id FROM existing_hotels WHERE lower(name)=lower(:o) LIMIT 1"),
            {"o": former_org},
        )
    ).scalar()
    lid = None
    if not hid:
        lid = (
            await session.execute(
                text("SELECT id FROM potential_leads WHERE lower(hotel_name)=lower(:o) LIMIT 1"),
                {"o": former_org},
            )
        ).scalar()

    if not hid and not lid:
        out["action"] = "linked_note_only"
        return out

    kind = "existing_hotel" if hid else "lead"
    pid = hid or lid
    col = "existing_hotel_id" if hid else "lead_id"
    out["property_kind"] = kind
    out["property_id"] = pid

    # dedup by normalized name among that property's contacts
    rows = await session.execute(
        text(f"SELECT id, name FROM lead_contacts WHERE {col} = :pid"), {"pid": pid}
    )
    target = _norm_person_name(succ)
    match_id = None
    for r in rows:
        if _norm_person_name(r.name) == target:
            match_id = r.id
            break

    if match_id:
        # [phase3_v2] Role-UPDATE: if they're on file under a DIFFERENT/old title,
        # overwrite it (they were promoted/moved into this seat). Org/source
        # stay fill-empty. B12.
        await session.execute(
            text(
                "UPDATE lead_contacts SET "
                "  title = :ti, "
                "  organization = COALESCE(NULLIF(organization,''), :org), "
                "  source_detail = COALESCE(NULLIF(source_detail,''), :sd), "
                "  updated_at = NOW() "
                "WHERE id = :id"
            ),
            {
                "id": match_id,
                "ti": succ_title,
                "org": former_org,
                "sd": f"Replaced {holder or 'prior contact'}; via {src}",
            },
        )
        await _mark_seat(session, contact_id, "filled")
        out["action"] = "merged_stub"
        out["stub_id"] = match_id
        return out

    ins = await session.execute(
        text(
            f"INSERT INTO lead_contacts "
            f"(name, title, organization, {col}, scope, confidence, is_saved, "
            f" found_via, source_detail, evidence_url, created_at, updated_at) "
            f"VALUES (:nm, :ti, :org, :pid, 'hotel_specific', 'low', TRUE, "
            f" 'successor_discovery', :sd, :url, NOW(), NOW()) RETURNING id"
        ),
        {
            "nm": succ,
            "ti": succ_title,
            "org": former_org,
            "pid": pid,
            "sd": f"Replaced {holder or 'prior contact'} (who moved on); via {src}",
            "url": src if src.startswith("http") else None,
        },
    )
    await _mark_seat(session, contact_id, "filled")
    out["action"] = "created_stub"
    out["stub_id"] = ins.scalar()
    return out


# [phase3_grounded_successor]
async def grounded_seat_successor(
    org: str, title: str, former_holder: str = "", location: str = ""
) -> dict:
    """Ask grounded Gemini (Google Search) who CURRENTLY holds <title> at <org>.
    Org+role question -> no namesake risk. Citation-gated. Returns same shape as
    find_seat_successor.
    """
    out = {
        "found": False,
        "successor_name": "",
        "successor_title": title or "",
        "evidence": "",
        "citations": [],
        "source": "grounded",
    }
    org, title = (org or "").strip(), (title or "").strip()
    if not org or not title:
        return out
    try:
        import httpx
        from app.services.contact_enrichment import (
            _build_grounding_url,
            _CONTACT_GROUNDING_TIMEOUT_S,
        )
        from app.services.gemini_client import get_gemini_headers
        from app.services.ai_client import _get_config, _ensure_init

        _ensure_init()
        cfg = _get_config()
        url, _ = _build_grounding_url(cfg["vertex_project_id"], cfg["model"])
        headers = get_gemini_headers()
    except Exception as e:
        logger.warning(f"grounded_successor: setup failed: {type(e).__name__}: {e}")
        out["error"] = f"setup: {type(e).__name__}"
        return out

    fh = (
        f" The previous holder, {former_holder}, has left -- do NOT name them."
        if former_holder
        else ""
    )
    prompt = (
        f"Search the web for who CURRENTLY holds the role of {title} (or the closest "
        f"equivalent senior role in the same department) at {org}"
        + (f" located in {location}" if (location or "").strip() else "")
        + f" right now, in 2025-2026.{fh} Requirements: (a) the person must work AT "
        f"THIS SPECIFIC property/location"
        + (f" ({org}, {location})" if (location or "").strip() else f" ({org})")
        + " -- NOT a different same-brand property in another city; (b) return the "
        f"ON-SITE role holder. A Complex/Area Managing Director counts ONLY if they "
        f"directly run THIS property; ignore purely corporate, regional, or "
        f"above-property executives who do not; (c) ignore outdated/past roles -- only "
        f"who holds it NOW; (d) if {org} is a MULTI-LOCATION brand/chain and no "
        f"specific location is given above, you CANNOT identify one holder -- say you "
        f"don't know. Give their full name and exact title. If you cannot find a "
        f"clearly current person, say you don't know."
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,
            "maxOutputTokens": 1024,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    try:
        async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
            resp = await gc.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        cand = data["candidates"][0]
        parts = (cand.get("content") or {}).get("parts") or []
        text = re.sub(r"[*`]+", "", ((parts[0].get("text") if parts else "") or "").strip())
        meta = cand.get("groundingMetadata", {}) or {}
        cites = [
            c.get("web", {}).get("uri")
            for c in (meta.get("groundingChunks", []) or [])[:5]
            if c.get("web", {}).get("uri")
        ]
    except Exception as e:
        _st = getattr(getattr(e, "response", None), "status_code", "")
        logger.warning(
            f"grounded_successor ERROR {org}/{title}: {type(e).__name__} {_st} {e}".rstrip()
        )
        out["error"] = f"{type(e).__name__} {_st}".strip()
        return out
    if not cites or not text:
        out["evidence"] = "(no grounding)"
        return out

    # ask the cheap LLM to extract name|title from the grounded prose
    try:
        from app.services.ai_client import ai_generate

        async with httpx.AsyncClient(timeout=40) as c:
            ext = await ai_generate(
                c,
                "From this text, output ONE line 'NAME | TITLE' naming the person who "
                f"currently holds the {title} (or the equivalent senior role in the same "
                f"department) at {org}, or exactly 'unknown'. Never name "
                f"{former_holder or '---'}.\n\n{text[:3000]}",
                temperature=0.0,
                max_tokens=60,
            )
    except Exception:
        ext = ""
    body = (ext or "").strip().splitlines()[0] if ext else ""
    if not body or body.lower() == "unknown":
        out["evidence"] = text[:200]
        out["citations"] = cites
        return out
    nm, _, ti = body.partition("|")
    nm, ti = nm.strip(), ti.strip()
    if former_holder and _same_person(nm, former_holder):
        out["evidence"] = "(rejected former holder)"
        out["citations"] = cites
        return out
    out.update(
        found=bool(nm),
        successor_name=nm,
        successor_title=ti or title,
        evidence=text[:200],
        citations=cites,
    )
    return out


# [phase3_harvest] find_property_leaders — the "eye for an eye" fallback.
# When a vacated seat has NO successor (open/unknown), don't walk away empty:
# harvest the property's CURRENT crucial department heads so coverage of the
# account survives the departure. Grounded + location-aware (same engine that
# beat snippets in live tests). Returns a list of {name, title} dicts.
HARVEST_ROLES = (
    "General Manager",
    "Director of Operations",
    "Director of Food and Beverage",
    "Director of Rooms",
    "Director of Sales and Marketing",
    "Director of Human Resources",
    "Director of Purchasing",
)


async def find_property_leaders(org: str, location: str = "", exclude: str = "") -> dict:
    """Grounded harvest of the CURRENT on-property leadership team at a SPECIFIC
    property. Returns {found: bool, leaders: [{name, title}], citations: [...]}.

    Location-aware: a bare multi-location brand with no location returns empty
    (we will not invent contacts for an unidentified property). `exclude` is the
    departed person's name, never returned.
    """
    out = {"found": False, "leaders": [], "citations": [], "evidence": ""}
    org = (org or "").strip()
    location = (location or "").strip()
    if not org:
        return out

    try:
        import httpx
        from app.services.contact_enrichment import (
            _build_grounding_url,
            _CONTACT_GROUNDING_TIMEOUT_S,
        )
        from app.services.gemini_client import get_gemini_headers
        from app.services.ai_client import _get_config, _ensure_init, ai_generate

        _ensure_init()
        cfg = _get_config()
        url, _ = _build_grounding_url(cfg["vertex_project_id"], cfg["model"])
        headers = get_gemini_headers()
    except Exception as e:
        logger.warning(f"find_property_leaders: setup failed: {e}")
        return out

    roles_txt = ", ".join(HARVEST_ROLES)
    where = f"{org}" + (f", located in {location}" if location else "")
    ex = f" Do NOT name {exclude}." if exclude else ""
    prompt = (
        f"Search the web for the CURRENT (2025-2026) on-property leadership team at "
        f"{where}. For EACH of these roles that currently has a named holder AT THIS "
        f"SPECIFIC property, give the person: {roles_txt}.{ex} Rules: (a) the person "
        f"must work AT THIS property/location, not a different same-brand property; "
        f"(b) on-site holders only -- ignore purely corporate/regional/above-property "
        f"executives; (c) only people in the role NOW, not past holders; (d) if {org} "
        f"is a multi-location brand/chain and no location is given, return nothing. "
        f"List each as a line 'ROLE: Full Name'. Omit roles with no clear current holder."
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,
            "maxOutputTokens": 1024,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    try:
        async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
            resp = await gc.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        cand = data["candidates"][0]
        parts = (cand.get("content") or {}).get("parts") or []
        text = re.sub(r"[*`]+", "", ((parts[0].get("text") if parts else "") or "").strip())
        meta = cand.get("groundingMetadata", {}) or {}
        cites = [
            c.get("web", {}).get("uri")
            for c in (meta.get("groundingChunks", []) or [])[:6]
            if c.get("web", {}).get("uri")
        ]
    except Exception as e:
        _st = getattr(getattr(e, "response", None), "status_code", "")
        logger.warning(
            f"find_property_leaders ERROR {org}/{location}: {type(e).__name__} {_st} {e}".rstrip()
        )
        out["error"] = f"{type(e).__name__} {_st}".strip()
        return out
    if not cites or not text:
        out["evidence"] = "(no grounding)"
        return out

    # Extract structured Name | Title lines from the grounded prose.
    try:
        async with httpx.AsyncClient(timeout=40) as c:
            ext = await ai_generate(
                c,
                "From the text below, output ONE line per CURRENT leader at "
                f"{where}, formatted exactly 'Full Name | Their Title'. Only people "
                "clearly holding the role now; skip past holders and corporate/"
                f"regional execs.{ex} If none, output exactly 'none'.\n\n{text[:3500]}",
                temperature=0.0,
                max_tokens=300,
            )
    except Exception:
        ext = ""

    ex_norm = _norm_person_name(exclude) if exclude else ""
    seen: set[str] = set()
    leaders: list[dict] = []
    for ln in (ext or "").splitlines():
        ln = ln.strip().lstrip("-\u2022* ").strip()
        if not ln or ln.lower() == "none" or "|" not in ln:
            continue
        nm, _, ti = ln.partition("|")
        nm, ti = nm.strip(), ti.strip()
        key = _norm_person_name(nm)
        if not nm or not key or key in seen:
            continue
        if ex_norm and key == ex_norm:
            continue
        seen.add(key)
        leaders.append({"name": nm, "title": ti})

    out["leaders"] = leaders
    out["found"] = bool(leaders)
    out["citations"] = cites
    out["evidence"] = text[:200]
    return out
