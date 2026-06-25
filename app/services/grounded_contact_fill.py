"""grounded_contact_fill.py -- Google-grounded backup for missing contact fields.

Raw Serper search cannot surface LinkedIn profile URLs for low-profile people
(LinkedIn blocks them from the index), and often misses role/org too. But a
GROUNDED Gemini call -- the same googleSearch tool the lead-generator already
uses -- reasons over the live results and reconstructs the profile URL, role,
and org even when they're buried (it found linkedin.com/in/rubyozr that raw
Serper never returned).

This is the per-contact backup: given a name + org + email, ask grounded Gemini
for ONLY the fields that are missing, REQUIRE citations (groundingChunks), and
return what it verifiably found. Used by contact_tier2_enrichment as a fallback
when Serper snippets come up empty.

Pure-additive: new file, mirrors the proven _ground_property_level_rescue
payload/citation pattern in contact_enrichment.py.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger("grounded_contact_fill")

_LI_RE = re.compile(r"linkedin\.com/in/([^/?&#\s)\"']+)", re.IGNORECASE)


def _norm_li(url: str) -> Optional[str]:
    if not url:
        return None
    m = _LI_RE.search(url)
    if not m:
        return None
    slug = m.group(1).strip().rstrip(".,;/`*)\"'")
    # Reject placeholder / junk slugs: Gemini sometimes writes an EXAMPLE like
    # "linkedin.com/in/..." or "linkedin.com/in/<slug>" or "/in/username" in
    # prose. A real slug is alphanumeric (with hyphens), has a letter or digit,
    # and isn't a generic placeholder word.
    if not slug or len(slug) < 3:
        return None
    if not re.search(r"[a-z0-9]", slug, re.IGNORECASE):
        return None
    if not re.fullmatch(r"[A-Za-z0-9\-_%]+", slug):
        return None
    if slug.lower() in {"username", "slug", "profile", "yourname", "name", "in", "example"}:
        return None
    return f"https://www.linkedin.com/in/{slug}"


_ROLE_RE = re.compile(
    r"\b(?:is|as|serves as|works as|holds the (?:role|position) of|title is|"
    r"currently the|the)\s+(?:the\s+)?"
    r"((?:[A-Z][A-Za-z&/'\-]+\s+){0,3}?"
    r"(?:Director|Manager|President|Chief|Coordinator|Supervisor|Officer|"
    r"Executive|Controller|Buyer|Head|GM|VP|Superintendent)"
    r"(?:\s+of\s+(?:[A-Z][A-Za-z&'\-]+(?:\s+(?:and|&)\s+[A-Z][A-Za-z&'\-]+)?))?)"
    r"\b",
)


def _clean_title(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip().rstrip(",.;").strip()
    # cut trailing location/company fragments the regex sometimes catches
    s = re.split(r"\s+(?:at|for|with|in)\s+", s)[0].strip()
    return s[:60]


def _extract_role(text: str, name: str) -> str:
    """Pull a job title from prose. Conservative -- returns '' if unclear so
    the deep-enrich synthesis (which is better at this) can handle it."""
    if not text:
        return ""
    m = _ROLE_RE.search(text)
    if m:
        cand = _clean_title(m.group(1))
        if 3 <= len(cand) <= 60:
            return cand
    return ""


async def grounded_fill(
    name: str,
    org: str = "",
    email: str = "",
    want_linkedin: bool = True,
    want_role: bool = True,
) -> dict:
    """Grounded Gemini lookup for ONE contact's missing fields.

    Returns {linkedin_url, role, organization, citations: [..], grounded: bool}.
    Only fields the model VERIFIABLY found (with citations) are populated; if
    groundingChunks is empty the model answered from memory and we return a
    grounded=False, empty result rather than trust a hallucination.
    """
    name = (name or "").strip()
    if not name:
        return {
            "grounded": False,
            "linkedin_url": None,
            "role": "",
            "organization": "",
            "citations": [],
        }

    try:
        import httpx
        from app.services.contact_enrichment import (
            _build_grounding_url,
            _CONTACT_GROUNDING_TIMEOUT_S,
        )
        from app.services.gemini_client import get_gemini_headers
        from app.services.ai_client import _get_config, _ensure_init

        _ensure_init()
        config = _get_config()
        project = config["vertex_project_id"]
        model = config["model"]
        url, _ = _build_grounding_url(project, model)
        headers = get_gemini_headers()
    except Exception as e:
        logger.warning(f"grounded_fill: cannot build URL/headers: {e}")
        return {
            "grounded": False,
            "linkedin_url": None,
            "role": "",
            "organization": "",
            "citations": [],
        }

    domain = email.split("@")[-1] if "@" in email else ""
    org_bit = (
        f" who works at {org}" if org else (f" whose work email is at {domain}" if domain else "")
    )
    email_bit = f" Their email address is {email}." if email else ""

    asks = []
    if want_role:
        asks.append("What is their current job title and which company do they work for?")
    if want_linkedin:
        asks.append(
            "What is the exact URL of their personal LinkedIn profile page "
            "(the linkedin.com/in/... page for this specific person)?"
        )
    asks_txt = " ".join(asks)

    # NATURAL-LANGUAGE prompt -- NOT a JSON instruction. A JSON-format demand
    # makes Gemini answer from memory (0 citations, no real search). A plain
    # conversational ask makes it actually run Google Search and cite sources.
    # (Documented lesson from the lead-gen grounding path.) We parse the URL +
    # role out of the prose afterward.
    prompt = (
        f"I'm trying to find information about a specific person: {name}{org_bit}.{email_bit} "
        f"Please search the web and tell me about this exact person -- the one connected to "
        f"{org or domain or 'this contact'}, not a different individual who happens to share the name. "
        f"{asks_txt} "
        f"If you find their LinkedIn profile, please include the full linkedin.com/in/ URL in your answer. "
        f"If you genuinely cannot find this specific person, just say so."
    )

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,  # required for grounding per Vertex docs
            "maxOutputTokens": 2048,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    logger.info(f"grounded_fill[{name}]: calling Gemini grounding (org={org!r} email={email!r})")
    try:
        async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
            resp = await gc.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"grounded_fill ERROR for {name!r}: {e}")
        return {
            "grounded": False,
            "linkedin_url": None,
            "role": "",
            "organization": "",
            "citations": [],
        }

    try:
        candidate = data["candidates"][0]
        parts = (candidate.get("content") or {}).get("parts") or []
        text = ((parts[0].get("text") if parts else "") or "").strip()
    except (KeyError, IndexError):
        logger.info(f"grounded_fill[{name}]: bad response shape (no candidate/text)")
        return {
            "grounded": False,
            "linkedin_url": None,
            "role": "",
            "organization": "",
            "citations": [],
        }

    # strip markdown emphasis (**bold**, *italic*, backticks) so regex extraction
    # of role/URL works on the plain words.
    text = re.sub(r"[*`]+", "", text)

    # citation gate: no groundingChunks => answered from memory => don't trust
    meta = candidate.get("groundingMetadata", {}) or {}
    citations = []
    for chunk in (meta.get("groundingChunks", []) or [])[:5]:
        web = chunk.get("web", {}) or {}
        if web.get("uri"):
            citations.append(web.get("uri"))
    grounded = len(citations) > 0

    # What did Gemini actually SEARCH? (visibility into whether search fired)
    searched = meta.get("webSearchQueries", []) or meta.get("searchQueries", []) or []
    logger.info(
        f"grounded_fill[{name}]: searched={searched} | "
        f"found {len(citations)} citation URLs: {citations}"
    )

    # Prose response now (natural-language ask). Extract the LinkedIn URL from
    # the answer text OR the citation URLs, and the role from the prose.
    logger.info(f"grounded_fill[{name}]: answer={text[:280]!r}")

    # 1) LinkedIn URL: prefer one found in the answer text; else scan citations.
    li = None
    if want_linkedin:
        li = _norm_li(text)
        if not li:
            for c in citations:
                li = _norm_li(c)
                if li:
                    break

    # 2) Role/org/location: extract the structured BASICS from the grounded
    # prose we already paid for. A cheap JSON call turns the answer (which
    # names the specific employer + city) into stored fields -- instead of
    # dropping them like the old regex-only path did.
    role = ""
    organization = ""
    city = ""
    state = ""
    if text and grounded:
        try:
            import json as _json
            import httpx as _httpx
            from app.services.ai_client import ai_generate as _ai

            async with _httpx.AsyncClient(timeout=40) as _c:
                _raw = await _ai(
                    _c,
                    "From the text below about " + name + ", extract their CURRENT "
                    "job. Output ONLY compact JSON, no prose, with keys: "
                    "organization (the SPECIFIC hotel/property or company -- NOT a "
                    'parent brand like "IHG" if a specific property is named), '
                    'title, city, state. Use "" for anything not clearly stated.\n\n' + text[:2500],
                    temperature=0.0,
                    max_tokens=160,
                )
            _s = (_raw or "").strip()
            if _s.startswith("```"):
                _s = _s.strip("`").split("\n", 1)[-1]
            _s = _s[_s.find("{") : _s.rfind("}") + 1] if "{" in _s else ""
            _d = _json.loads(_s) if _s else {}
            organization = (_d.get("organization") or "").strip()
            role = (_d.get("title") or "").strip()
            city = (_d.get("city") or "").strip()
            state = (_d.get("state") or "").strip()
        except Exception as _e:
            logger.warning(f"grounded_fill[{name}]: basics extract failed: {_e}")
    if want_role and not role and text:
        role = _extract_role(text, name)

    # The model was asked to say so if it couldn't find the person. Treat an
    # explicit "could not find / no information" as a non-match.
    tl = text.lower()
    not_found = (
        any(
            p in tl
            for p in (
                "cannot find",
                "could not find",
                "couldn't find",
                "no information",
                "unable to find",
                "i don't have",
                "i do not have",
                "no specific",
            )
        )
        and not li
        and not role
    )

    if not_found:
        logger.info(f"grounded_fill[{name}]: model reports not found")
        return {
            "grounded": grounded,
            "linkedin_url": None,
            "role": "",
            "organization": "",
            "city": "",
            "state": "",
            "confidence": "low",
            "citations": citations,
        }

    # Trust requires real grounding (citations). Without them the model spoke
    # from memory -- keep nothing.
    if not grounded:
        logger.info(f"grounded_fill[{name}]: DROPPED -- no citations (answered from memory)")
        li, role, organization, city, state = None, "", "", "", ""

    logger.info(
        f"grounded_fill[{name}]: RESULT grounded={grounded} ({len(citations)} cites) "
        f"li={li!r} role={role!r} org={organization!r} loc={(city + ' ' + state).strip()!r}"
    )

    return {
        "grounded": grounded,
        "linkedin_url": li,
        "role": role,
        "organization": organization,
        "city": city,
        "state": state,
        "confidence": "high" if (grounded and li) else ("medium" if grounded else "low"),
        "citations": citations,
        "evidence": text[:300],
    }


async def grounded_job_history(
    name: str, org: str = "", email: str = "", linkedin_url: str = ""
) -> dict:
    """Grounded DATED work-history lookup for ONE person (coverage freshness).

    Returns:
      {
        "grounded": bool,                # real search happened (citations present)
        "roles": [                       # newest-first; [] if not grounded/parse-fail
            {"employer", "title", "start", "end", "is_current"}
        ],
        "current_employer": str | None,  # set ONLY when an OPEN ("Present") role exists
        "current_known": bool,           # False => current employer is UNKNOWN
        "citations": [..],
        "evidence": str,
      }

    Never guesses a current employer: if every found role has an end date, the
    current employer is UNKNOWN -- this is the case where someone has left a job
    and no current active role can be found.
    """
    import asyncio
    import json

    name = (name or "").strip()
    empty = {
        "grounded": False,
        "roles": [],
        "current_employer": None,
        "current_known": False,
        "citations": [],
        "evidence": "",
    }
    if not name:
        return empty

    # --- Step 1: grounded PROSE fetch (natural language -> real Google search) -
    try:
        import httpx
        from app.services.contact_enrichment import (
            _build_grounding_url,
            _CONTACT_GROUNDING_TIMEOUT_S,
        )
        from app.services.gemini_client import get_gemini_headers
        from app.services.ai_client import _get_config, _ensure_init, ai_generate

        _ensure_init()
        config = _get_config()
        headers = get_gemini_headers()
    except Exception as e:
        logger.warning(f"grounded_job_history: cannot init: {e}")
        return empty

    domain = email.split("@")[-1] if "@" in email else ""
    li = (linkedin_url or "").strip()
    # Disambiguators, strongest first: a LinkedIn URL pins the exact person (no
    # name collision); email is next; the org is treated as a POSSIBLY-FORMER
    # employer, NOT a current-employer filter. Over-anchoring on a stale org made
    # the model hedge and return nothing for common names like "Taylor Smith".
    li_bit = f" Their LinkedIn profile: {li}." if li else ""
    email_bit = f" Their work email is {email}." if email else ""
    org_bit = (
        f" They were at one point associated with {org} (possibly a FORMER employer "
        f"-- do NOT assume they are still there)."
        if org
        else (f" Their work email domain is {domain}." if domain else "")
    )
    # Short + direct, mirroring the phrasing that works in Gemini's AI mode. A long
    # "read the exact profile" instruction made the model hedge about being unable to
    # open LinkedIn; the dated history is right there in the search-result previews /
    # rich card, so we tell it plainly to report what the results show, not refuse.
    prompt = (
        f"Give the full work timeline of this specific person: {name}.{li_bit}{email_bit}{org_bit} "
        f"For each company they worked at, list the job title and the start and end dates "
        f"(month and year), from most recent to oldest. "
        f"Do NOT refuse or hedge about being unable to open the LinkedIn page -- the dates appear in "
        f"search results and profile previews, so just report what the results show. "
        f"Rank strictly by the dates. A role is CURRENT only if it is explicitly 'Present' or has no "
        f"end date; if the most recent role has already ENDED and there is no current/Present role, say "
        f"the current employer is UNKNOWN (do not assume a past employer is current). "
        f"If you cannot identify this specific person at all, say so."
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,
            "maxOutputTokens": 2048,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    # Model fallback: primary (env, e.g. gemini-3.5-flash), then 2.5-flash,
    # then lite -- separate quota pools, so a 429 on one often clears on the
    # next. _build_grounding_url picks the right endpoint per model.
    _primary = config["model"]
    _models = [_primary]
    for _m in ("gemini-2.5-flash", "gemini-2.5-flash-lite"):
        if _m != _primary:
            _models.append(_m)
    data = None
    _last = None
    for _mi, _model in enumerate(_models):
        try:
            _url, _ = _build_grounding_url(config["vertex_project_id"], _model)
        except Exception as e:
            _last = e
            continue
        for _attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
                    resp = await gc.post(_url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                _last = e
                _is429 = "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e)
                logger.warning(
                    f"grounded_job_history[{name}]: {_model} try {_attempt + 1} "
                    f"failed ({'429' if _is429 else 'err'}): {str(e)[:120]}"
                )
                if _is429:
                    await asyncio.sleep(3 * (_attempt + 1))
                    continue
                break
        if data is not None:
            if _mi > 0:
                logger.info(f"grounded_job_history[{name}]: ok on fallback {_model}")
            break
    if data is None:
        logger.warning(f"grounded_job_history[{name}]: all models failed: {str(_last)[:160]}")
        return empty
    try:
        candidate = data["candidates"][0]
        parts = (candidate.get("content") or {}).get("parts") or []
        prose = re.sub(r"[*`]+", "", ((parts[0].get("text") if parts else "") or "").strip())
    except Exception as e:
        logger.warning(f"grounded_job_history[{name}]: bad response shape: {e}")
        return empty

    meta = candidate.get("groundingMetadata", {}) or {}
    citations = [
        (c.get("web") or {}).get("uri")
        for c in (meta.get("groundingChunks", []) or [])[:6]
        if (c.get("web") or {}).get("uri")
    ]
    grounded = len(citations) > 0
    logger.info(
        f"grounded_job_history[{name}]: grounded={grounded} "
        f"cites={len(citations)} prose={prose[:200]!r}"
    )
    if not grounded or not prose:
        return {**empty, "grounded": grounded, "citations": citations, "evidence": prose[:300]}

    tl = prose.lower()
    if (
        any(
            p in tl
            for p in (
                "cannot find",
                "could not find",
                "couldn't find",
                "unable to find",
                "no information",
            )
        )
        and "unknown" not in tl
    ):
        return {**empty, "grounded": grounded, "citations": citations, "evidence": prose[:300]}

    # --- Step 2: structure the prose into JSON (ungrounded; JSON safe here) ----
    struct_prompt = (
        "From the research notes below, extract the person's work history as STRICT JSON and nothing "
        "else (no markdown, no commentary). Schema:\n"
        '{"roles":[{"employer":"","title":"","start":"YYYY-MM","end":"YYYY-MM","is_current":false}],'
        '"current_known":false,"current_employer":""}\n'
        "Rules: newest role first. 'start'/'end' are YYYY-MM (empty string if unknown). "
        "Leave 'end' as an empty string ONLY for a role that is explicitly current/Present (still ongoing). "
        "is_current=true ONLY for such an open role. If the MOST RECENT role has an end date in the past "
        "(the person has left and no current active role is stated), set EVERY role is_current=false, "
        "current_known=false, and current_employer to an empty string. Set current_known=true and "
        "current_employer to the open role's employer ONLY when there is a genuinely current/Present role.\n\n"
        f"NOTES:\n{prose}"
    )
    roles, current_known, current_employer = [], False, None
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            raw = await ai_generate(client, struct_prompt, temperature=0.0, max_tokens=1200)
        if raw:
            js = raw.strip()
            if js.startswith("```"):
                js = js.split("```")[1]
                js = js[4:].strip() if js.lstrip().lower().startswith("json") else js.strip()
            parsed = json.loads(js)
            for it in parsed.get("roles") or []:
                emp = (it.get("employer") or "").strip()
                if not emp:
                    continue
                end = (it.get("end") or "").strip()
                roles.append(
                    {
                        "employer": emp,
                        "title": (it.get("title") or "").strip(),
                        "start": (it.get("start") or "").strip(),
                        "end": end,
                        "is_current": bool(it.get("is_current")) or end == "",
                    }
                )
            open_roles = [x for x in roles if x["is_current"]]
            if bool(parsed.get("current_known")) and open_roles:
                current_known = True
                current_employer = (
                    parsed.get("current_employer") or open_roles[0]["employer"]
                ).strip() or None
            else:
                # honest default: they have left / current unknown -- no open role,
                # so do NOT name a current employer.
                current_known = False
                current_employer = None
                for x in roles:
                    x["is_current"] = False
    except Exception as e:
        logger.warning(f"grounded_job_history[{name}]: structuring failed: {e}")

    logger.info(
        f"grounded_job_history[{name}]: {len(roles)} roles, "
        f"current_known={current_known} current={current_employer!r}"
    )
    return {
        "grounded": grounded,
        "roles": roles,
        "current_employer": current_employer,
        "current_known": current_known,
        "citations": citations,
        "evidence": prose[:300],
    }
