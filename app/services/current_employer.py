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
