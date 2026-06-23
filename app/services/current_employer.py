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

    queries = []
    slug = _slug(linkedin)
    if slug:
        queries.append(f"{slug} linkedin")
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

    prompt = (
        f"Below are real Google search-result snippets about {name}"
        + (f" (on file at {org})" if org else "")
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
async def find_seat_successor(org: str, title: str, former_holder: str = "") -> dict:
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
    queries = [f"{org} {t}" for t in _titles]
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
        f"ROLE / TITLE: {title}\n"
        f"{fh}\n"
        "RULES:\n"
        "1. Return the person who holds this role NOW. If the exact title isn't named "
        "but a clear senior holder of the SAME department is (e.g. 'Director of Human "
        "Resources' when the seat is 'Human Resources Manager'), return that person.\n"
        "2. Prefer the MOST RECENT dated mention. A name attached to an OLD date range "
        "(e.g. '2022-2024') is a PAST holder -- do not return them.\n"
        "3. A job-board / careers / salary page (Indeed, SimplyHired, ZipRecruiter, "
        "Glassdoor) that merely lists a posting is NOT proof of who holds the role. "
        "Weight appointment announcements, staff/leadership pages, and dated news higher.\n"
        "4. Never return the previous holder named above.\n"
        "5. If no snippet clearly names a CURRENT holder, say unknown. Absence is not a guess.\n\n"
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

    row = (
        await session.execute(
            text(
                "SELECT c.title, c.organization AS new_org, "
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

    res = await find_seat_successor(org=former_org, title=title, former_holder=holder)
    if not res.get("found"):
        out["action"] = "no_successor"
        out["citations"] = res.get("citations", [])
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
        # fill-empty merge (don't clobber, don't fabricate)
        await session.execute(
            text(
                "UPDATE lead_contacts SET "
                "  title = COALESCE(NULLIF(title,''), :ti), "
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
    out["action"] = "created_stub"
    out["stub_id"] = ins.scalar()
    return out


# [phase3_grounded_successor]
async def grounded_seat_successor(org: str, title: str, former_holder: str = "") -> dict:
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
        logger.warning(f"grounded_successor: setup failed: {e}")
        return out

    fh = (
        f" The previous holder, {former_holder}, has left -- do NOT name them."
        if former_holder
        else ""
    )
    prompt = (
        f"Search the web for the CURRENT leadership of the food & beverage department "
        f"at {org}. Who currently holds the role of {title} (or the equivalent senior "
        f"F&B/outlets leader) there right now, in 2025-2026?{fh} "
        f"Give me their full name and exact title. If you cannot find a clearly current "
        f"person, say you don't know."
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
        logger.warning(f"grounded_successor ERROR {org}/{title}: {e}")
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
                f"currently holds the {title} (or senior F&B/outlets) role at {org}, or "
                f"exactly 'unknown'. Never name {former_holder or '---'}.\n\n{text[:3000]}",
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
    if former_holder and nm.lower() == former_holder.strip().lower():
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
