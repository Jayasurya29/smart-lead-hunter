"""Tier-2 deep contact enrichment — the "ultimate contacts" research pipeline.

Composes the engines SLH already has into one per-contact dossier builder:

    contact (name + email/org)
      → web research   (Serper)                              [role, background]
      → email/LinkedIn (Wiza individual_reveals)            [verified email, LI]
      → synthesis      (Gemini / Vertex)                    [clean structured bio]
      → write-back with provenance + confidence + enriched_at

This is the EXPENSIVE tier (real web + Wiza credits), so it is meant to run on
the contacts that matter — a specific person on demand, or a filtered batch
(e.g. all decision-makers, or one company's people) — NOT the whole table.
Tier-1 (signals) stays the cheap, run-on-everyone pass.

Reuses, never reinvents:
  - app.services.outreach.researcher.smart_search  (Serper + DDG)
  - app.services.wiza_enrichment.enrich_contact_email
  - app.services.ai_client.ai_generate              (Gemini)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import text

from app.database import async_session
from app.services.ai_client import ai_generate

logger = logging.getLogger(__name__)

MODEL = "gemini-2.5-flash"  # synthesis wants the fuller model, not lite
GROUNDED_CONFIDENCE_FLOOR = 0.6  # grounded research outranks signals (cap 0.7)

SYNTH_PROMPT = """You are building a CRM dossier for a sales team at JA Uniforms
(sells uniforms to hotels / resorts / hospitality-services companies).

Given a contact and raw web snippets about them, produce ONLY this JSON:
{"role":"","seniority":"","department":"","is_decision_maker":false,
 "person_first_name":"","person_last_name":"",
 "current_employer":"","current_title":"","employer_changed":false,"former_employer":"",
 "background":"","relevant_to_uniforms":true,"confidence":0.0}

Rules:
- role: their current job title, normalized. "" if the snippets don't show it.
- seniority: c_suite | director | manager | staff | unknown.
- department: procurement | operations | housekeeping | food_beverage | sales |
  finance | hr | it | marketing | other | unknown.
- is_decision_maker: true if they likely influence buying uniforms/supplies.
- background: a substantive professional summary of 3-6 sentences, written for a
  salesperson who is about to reach out. Synthesize EVERYTHING relevant the
  snippets support, in this order when available:
    1. Current role and company, and what that company does.
    2. What their role/department likely controls or influences (especially
       anything touching purchasing, operations, housekeeping, F&B, uniforms).
    3. Career history / prior roles or companies mentioned.
    4. Location, tenure, credentials, or other concrete facts.
    5. Any detail useful for a uniform-sales conversation (property type, brand,
       size, role in procurement).
  Use ALL the snippet facts you can; do not collapse a rich result into one
  line. Write in plain factual prose. Never invent facts not in the snippets —
  if a section has no support, simply omit it. "" only if the snippets contain
  nothing usable about this person.
- person_first_name / person_last_name: the contact's REAL full name when
  the snippets clearly identify the owner of this email or role (staff
  directory, LinkedIn profile, press release naming them at this company).
  "" when not clearly established — NEVER guess a name from the email
  pattern alone.
- relevant_to_uniforms: true if this person/company could be a JA customer.
- current_employer: the company this person works at NOW, exactly as the snippets
  name it. Determine this from the DATES in the snippets -- the most recent role
  (a LinkedIn entry marked "Present", "current", or a dated role with no end date,
  or the latest start date) is their CURRENT employer. IMPORTANT: the organization
  shown in CONTACT is only what we have ON FILE and MAY BE STALE or a FORMER
  employer -- do NOT assume it is current, and do NOT let the contact's email
  domain decide this. Trust the dated evidence in the snippets over the CONTACT
  org. "" only if the snippets don't clearly establish a current employer.
- current_title: their job title at that current employer. "" if unclear.
- employer_changed: true ONLY when the snippets EXPLICITLY show that the most
  recent (current) employer is a DIFFERENT company from the organization given in
  CONTACT (different organization, not just a reworded spelling of the same one).
  Example: CONTACT says "The Ritz-Carlton" but the snippets show a more recent
  role "Director at The St. Regis San Francisco (Present)" -- that is
  employer_changed=true, current_employer="The St. Regis San Francisco",
  former_employer="The Ritz-Carlton". Be conservative about the DIRECTION but do
  not let the stale CONTACT org override clearly more-recent dated evidence: if
  the only employer in the snippets matches the one on file, or there are no dates
  to compare, set employer_changed=false. A wrong "moved" is worse than a missed
  one -- but a stale org on file is not evidence that they still work there.
- former_employer: when employer_changed is true, the company they LEFT (usually
  the organization given in CONTACT). "" otherwise.
- confidence: 0.0-1.0 based on how much the snippets actually told you.
Do not invent facts not supported by the snippets.

CONTACT:
{contact}

WEB SNIPPETS:
{snippets}
"""


def _now():
    return datetime.now(timezone.utc)


def _domain(email: str) -> str:
    return email.split("@")[-1].lower().strip() if email and "@" in email else ""


async def _web_research(name: str, org: str, domain: str, email: str = "") -> list[str]:
    """Reuse the Outreach researcher's Serper search for material on a person."""
    try:
        from app.services.outreach.researcher import smart_search
    except Exception as e:
        logger.warning(f"tier2: researcher import failed: {e}")
        return []
    queries = []
    # Nameless contact (2026-06-04): hunt the email address itself — staff
    # directories, press releases and indexed profiles reveal who owns it
    # (fching@rosenplaza.com → "Frank Ching, Chief Engineer, Rosen Plaza").
    local = (email or "").split("@", 1)[0]
    if not name and email:
        queries.append(f'"{email}"')
        if local and org:
            queries.append(f'"{local}" "{org}"')
        if local and domain:
            queries.append(f'"{local}" {domain.split(".")[0]}')
    if name and org:
        queries.append(f'"{name}" {org}')
        queries.append(f'"{name}" {org} title role')
    if name and domain:
        queries.append(f'"{name}" {domain}')
    if name:
        queries.append(f'"{name}" LinkedIn')
        queries.append(f'"{name}" {org} experience background')
    snippets: list[str] = []
    seen: set[str] = set()
    for q in queries:
        try:
            for s in smart_search(q) or []:
                if s not in seen:
                    seen.add(s)
                    snippets.append(s)
        except Exception as e:
            logger.debug(f"tier2: search failed {q!r}: {e}")
        if len(snippets) >= 20:
            break
    return snippets[:20]


async def _find_email_via_wiza(
    name: str, org: str, domain: str, linkedin_url: Optional[str]
) -> Optional[dict]:
    """Reuse Wiza to find a verified email when one is missing."""
    try:
        from app.services.wiza_enrichment import enrich_contact_email
    except Exception as e:
        logger.warning(f"tier2: wiza import failed: {e}")
        return None
    try:
        return await enrich_contact_email(
            linkedin_url=linkedin_url,
            contact_name=name,
            name=name,
            company=org or None,
            domain=domain or None,
        )
    except Exception as e:
        logger.warning(f"tier2: wiza reveal failed for {name!r}: {e}")
        return None


async def enrich_contact_deep(contact_id: int, find_email: bool = False) -> dict:
    """Deep-enrich ONE contact. Returns the dossier dict (also persisted)."""
    async with async_session() as session:
        row = (
            await session.execute(
                text(
                    "SELECT id, email, first_name, last_name, display_name, title, "
                    "organization, linkedin_url, last_inbound_at, "
                    "EXISTS(SELECT 1 FROM contact_affiliations a WHERE a.person_type='contact' "
                    "AND a.person_id = contacts.id AND a.relationship='former') AS has_former_employer "
                    "FROM contacts WHERE id = :id"
                ),
                {"id": contact_id},
            )
        ).one_or_none()
        if not row:
            return {"error": "not found"}

    name = " ".join([p for p in (row.first_name, row.last_name) if p]) or (row.display_name or "")
    org = row.organization or ""
    domain = _domain(row.email or "")

    # Email-only fallback (2026-06-12): when we have no org, infer one from the
    # email domain so the search + synthesis have something to anchor on
    # (rosenplaza.com -> "Rosen Plaza"). This inferred org is used ONLY to help
    # recover the name/role; it is NOT written to the DB as fact unless the
    # model independently confirms it.
    inferred_org = org
    if not inferred_org:
        try:
            from app.services.inbox_sync import _infer_org

            inferred_org = _infer_org(domain) or ""
        except Exception:
            inferred_org = ""

    # 1) Web research (use the inferred org so email-only rows still get hits)
    snippets = await _web_research(name, inferred_org, domain, row.email or "")

    # [patch_tier2_current_employer] resolve current employer up front for stale rows (Serper+judge,
    # proven reliable where synthesis can invert current/former). Used to
    # ANCHOR the bio and to drive the move verdict below.
    ce_result = None
    ce_is_stale = False
    try:
        _li0 = getattr(row, "last_inbound_at", None)
        if _li0 is not None:
            from datetime import datetime as _dt0, timezone as _tz0

            _r0 = _li0 if _li0.tzinfo else _li0.replace(tzinfo=_tz0.utc)
            ce_is_stale = (_dt0.now(_tz0.utc) - _r0).days > 547
    except Exception:
        ce_is_stale = False
    if ce_is_stale:
        try:
            from app.services.current_employer import find_current_employer

            ce_result = await find_current_employer(
                name=name, org=org, linkedin=(row.linkedin_url or ""), domain=domain
            )
        except Exception as e:
            logger.warning(f"tier2: current-employer lookup failed for {contact_id}: {e}")
            ce_result = None

    # [move_domain_veto] you don't keep emailing from a company you left. If a
    # move is claimed but the contact's CURRENT email domain matches the org
    # they supposedly LEFT (on-file / domain-inferred org), the move is a
    # same-name web hit -- suppress it. Matches the FORMER org (not the new
    # employer) so acronym domains on real movers (nyac.org -> New York
    # Athletic Club) are NOT falsely vetoed. Fires only on a positive match.
    if ce_result and ce_result.get("moved") and ce_result.get("current_employer"):
        _dom = (domain or "").lower().strip()
        _FREE = {
            "gmail.com",
            "yahoo.com",
            "hotmail.com",
            "outlook.com",
            "aol.com",
            "icloud.com",
            "live.com",
            "msn.com",
            "comcast.net",
            "me.com",
            "protonmail.com",
            "ymail.com",
            "mac.com",
        }
        if _dom and _dom not in _FREE:
            _label = _dom.split("@")[-1].rsplit(".", 1)[0].replace("-", " ")
            _dom_toks = {t for t in _label.split() if len(t) > 2}
            _lab = _label.replace(" ", "")

            def _matches(_name: str) -> bool:
                _n = (_name or "").lower()
                _nt = {
                    t
                    for t in _n.replace("&", " ").replace(",", " ").replace(".", " ").split()
                    if len(t) > 2
                }
                return bool(_dom_toks & _nt) or (bool(_lab) and _lab in _n.replace(" ", ""))

            _left_org = (org or "") or (inferred_org or "")
            if _left_org and _matches(_left_org):
                logger.info(
                    f"tier2: [move_domain_veto] suppressing move for {contact_id}: "
                    f"email domain '{_dom}' still matches org-left '{_left_org}' -- "
                    f"claimed move to '{ce_result.get('current_employer')}' is a namesake."
                )
                ce_result["moved"] = False
                ce_result["_domain_vetoed"] = True

    # 2) Wiza — only when we lack a USABLE email. 'Usable' = present AND not
    # known-former. A moved contact's on-file email is just a reference, so we
    # look up the current one. We never call Wiza when a good current email
    # already exists (no reason to spend a credit). [patch_wiza_primary]
    found_email = None
    _has_email = bool(row.email and "@" in (row.email or ""))
    _email_is_former = bool(getattr(row, "has_former_employer", False))
    _need_email = (not _has_email) or _email_is_former
    if find_email and _need_email:
        wiza = await _find_email_via_wiza(name, org, domain, row.linkedin_url)
        if wiza and wiza.get("email"):
            found_email = wiza["email"]

    # 3) Synthesis
    contact_blob = json.dumps(
        {
            "name": name,
            "email": row.email,
            "organization_on_file": org
            or (f"(likely {inferred_org}, inferred from email domain)" if inferred_org else ""),
            "title_on_file": row.title,
            "note": "organization_on_file/title_on_file are what we currently have stored "
            "and MAY BE OUT OF DATE; verify against the dated snippets.",
            **(
                {
                    "CONFIRMED_current_employer": ce_result.get("current_employer"),
                    "CONFIRMED_current_title": ce_result.get("current_title"),
                    "confirmed_note": "CONFIRMED_* fields were verified against this "
                    "person's LinkedIn via search; treat them as the CURRENT employer/title "
                    "and write the background bio accordingly. The on-file org is the FORMER "
                    "employer if it differs.",
                }
                if (
                    ce_result and ce_result.get("moved") and ce_result.get("current_employer")
                )  # [patch_tier2_current_employer]
                else {}
            ),
        },
        ensure_ascii=False,
    )
    prompt = SYNTH_PROMPT.replace("{contact}", contact_blob).replace(
        "{snippets}", "\n".join(f"- {s}" for s in snippets) or "(none found)"
    )

    client = httpx.AsyncClient(timeout=90)
    try:
        raw = await ai_generate(client, prompt, model=MODEL, temperature=0.25, max_tokens=1200)
    except Exception as e:
        logger.warning(f"tier2: synthesis failed for {contact_id}: {e}")
        raw = None
    finally:
        await client.aclose()

    dossier = {
        "role": "",
        "seniority": "unknown",
        "department": "unknown",
        "is_decision_maker": False,
        "person_first_name": "",
        "person_last_name": "",
        "current_employer": "",
        "current_title": "",
        "employer_changed": False,
        "former_employer": "",
        "background": "",
        "relevant_to_uniforms": True,
        "confidence": 0.0,
    }
    if raw:
        r = raw.strip()
        if r.startswith("```"):
            r = r.split("```")[1].lstrip("json").strip() if "```" in r else r
        try:
            dossier.update(json.loads(r))
        except Exception:
            logger.warning(f"tier2: bad synthesis JSON for {contact_id}")

    conf = max(
        float(dossier.get("confidence", 0.0)),
        GROUNDED_CONFIDENCE_FLOOR if dossier.get("background") else 0.0,
    )

    # Name resolution (2026-06-04): if the row had no name and research
    # clearly identified the person, fill it — and replace a display_name
    # that was just the email address.
    _nf = (dossier.get("person_first_name") or "").strip()
    _nl = (dossier.get("person_last_name") or "").strip()
    _row_nameless = not ((row.first_name or "").strip() or (row.last_name or "").strip())
    do_name_fill = bool(_row_nameless and _nf and _nl)
    if do_name_fill:
        logger.info(f"tier2: resolved name for contact {contact_id} " f"({row.email}): {_nf} {_nl}")

    # Grounded backup (2026-06-12): raw Serper can't surface LinkedIn profile
    # URLs and often misses role. If we still lack a role or a LinkedIn URL,
    # ask grounded Gemini (googleSearch) -- the same engine the lead-generator
    # uses -- which reasons over live results and reconstructs them. Citation-
    # gated inside grounded_fill, so it won't write hallucinated facts.
    grounded_li = None
    need_role = not (dossier.get("role") or "").strip()
    need_li = not (row.linkedin_url or "").strip()
    if need_role or need_li:
        try:
            from app.services.grounded_contact_fill import grounded_fill

            g = await grounded_fill(
                name=(f"{_nf} {_nl}".strip() or name),
                org=inferred_org or org,
                email=row.email or "",
                want_linkedin=need_li,
                want_role=need_role,
            )
            if g.get("grounded"):
                if need_role and g.get("role"):
                    dossier["role"] = g["role"]
                    if not (dossier.get("background") or "").strip() and g.get("evidence"):
                        dossier["background"] = g["evidence"]
                # [capture_basics] store the SPECIFIC org + location grounding found,
                # so Phase 2/3 get 'InterContinental, New York' instead of 'IHG'/''.
                _g_org = (g.get("organization") or "").strip()
                if _g_org:
                    dossier["grounded_org"] = _g_org
                _g_city = (g.get("city") or "").strip()
                _g_state = (g.get("state") or "").strip()
                if _g_city or _g_state:
                    dossier["grounded_city"] = _g_city
                    dossier["grounded_state"] = _g_state
                if need_li and g.get("linkedin_url"):
                    grounded_li = g["linkedin_url"]
                conf = max(conf, GROUNDED_CONFIDENCE_FLOOR)
        except Exception as e:
            logger.warning(f"tier2: grounded backup failed for {contact_id}: {e}")

    # Phase 2 (coverage freshness, 2026-06-19): for a STALE contact (no inbound
    # reply in ~18 months) fetch the DATED work history. Relabel the headline to
    # a new employer ONLY when there is a genuinely current/open ("Present") role.
    # When the person has clearly LEFT and the current employer is UNKNOWN we do
    # NOT guess -- the dated roles are recorded as affiliations (below) and the UI
    # shows "current: unknown" from the absence of any open role. This supersedes
    # the earlier single-org relabel, which could mis-stamp an ended role current.
    _li = getattr(row, "last_inbound_at", None)
    is_stale = False
    if _li is not None:
        try:
            from datetime import datetime, timezone

            _ref = _li if _li.tzinfo else _li.replace(tzinfo=timezone.utc)
            is_stale = (datetime.now(timezone.utc) - _ref).days > 547
        except Exception:
            is_stale = False
    job_history = None
    if is_stale:
        try:
            from app.services.grounded_contact_fill import grounded_job_history

            job_history = await grounded_job_history(
                name=(f"{_nf} {_nl}".strip() or name),
                org=org,
                email=row.email or "",
                linkedin_url=(row.linkedin_url or grounded_li or ""),
            )
        except Exception as e:
            logger.warning(f"tier2: job-history lookup failed for {contact_id}: {e}")
        # [patch_tier2_current_employer] grounded_job_history kept ONLY for its dated-roles affiliations
        # side-effect below; it no longer overrides the headline employer
        # (it returned grounded=False on the hard cases and inverted nothing it
        # got right). The headline now comes from find_current_employer.
        if job_history and job_history.get("grounded"):
            conf = max(conf, GROUNDED_CONFIDENCE_FLOOR)
        # apply the PROVEN current-employer verdict (Serper + same/moved judge)
        if ce_result and ce_result.get("found"):
            conf = max(conf, GROUNDED_CONFIDENCE_FLOOR)
            _cur = (ce_result.get("current_employer") or "").strip()
            if ce_result.get("moved") and _cur:
                dossier["current_employer"] = _cur
                dossier["current_title"] = (
                    ce_result.get("current_title") or dossier.get("current_title") or ""
                )
                dossier["employer_changed"] = True
                if not (dossier.get("former_employer") or "").strip():
                    dossier["former_employer"] = org
                logger.info(
                    f"tier2: contact {contact_id} ({row.email}) moved "
                    f"{org!r} -> {_cur!r} (via {ce_result.get('source')})"
                )
            elif ce_result.get("same"):
                dossier["employer_changed"] = False

    # 3b) Job-change detection (2026-06-16): when research shows this person has
    # moved to a DIFFERENT current employer than the org on file, re-file them to
    # the current employer and preserve the old org as a FORMER affiliation. The
    # email (e.g. brendan.payze@ritzcarlton.com) is NOT touched -- the inbox
    # relationship stays; only the headline org/title reflect current truth.
    # Conservative gate: the model must explicitly flag employer_changed, name a
    # current_employer that differs (after light normalization) from the stored
    # org, and clear the confidence floor. Bias to NO change.
    # [move_verified_employer] the move decision obeys the VERIFIED current
    # employer (ce_result from Serper/Wiza), NOT the synthesis LLM's dossier
    # field (which can hallucinate a company, e.g. 'Sheraton Orlando North').
    # The dossier value is only a fallback when there is no verified result.
    _ce_emp = (ce_result.get("current_employer") or "").strip() if ce_result else ""
    _ce_found = bool(ce_result and ce_result.get("found") and _ce_emp)
    if _ce_found:
        new_employer = _ce_emp
        new_title = (ce_result.get("current_title") or dossier.get("current_title") or "").strip()
    else:
        new_employer = (dossier.get("current_employer") or "").strip()
        new_title = (dossier.get("current_title") or "").strip()
    former_org = (dossier.get("former_employer") or "").strip() or org

    # [persist_specific_seat] If the former_org is a vague parent/domain (IHG,
    # marriott.com) but grounding captured the SPECIFIC property the person
    # actually worked at (InterContinental New York Times Square), record the
    # specific property as the seat -- that is what Phase 3 can search. We only
    # override vagueness; a specific former_org is left untouched.
    _g_org = (dossier.get("grounded_org") or "").strip()
    if _g_org and _g_org.lower() != former_org.lower():
        try:
            from app.services.current_employer import _is_vague_property

            if _is_vague_property(former_org) and not _is_vague_property(_g_org):
                logger.info(
                    f"[persist_specific_seat] seat {former_org!r} -> {_g_org!r} "
                    f"(captured specific property) for contact {contact_id}"
                )
                former_org = _g_org
        except Exception as _e:
            logger.warning(f"persist_specific_seat check failed: {_e}")

    def _norm_org(s: str) -> str:
        s = (s or "").lower()
        for junk in (
            "the ",
            " inc",
            " llc",
            " ltd",
            " corporation",
            " corp",
            " company",
            " co.",
            " hotel",
            " hotels",
            " resort",
            " resorts",
            " & ",
            " and ",
        ):
            s = s.replace(junk, " ")
        return " ".join(s.split())

    def _same_employer(a: str, b: str) -> bool:
        """True when a and b are the SAME employer despite formatting: a spaced
        name vs its glued domain label, a sub-string, or joiner words. Catches
        'Corporate Concierge Services' vs 'Corporateconcierge' and 'The Colonnade
        at Beckett Lake' vs 'Colonnadebeckettlake' WITHOUT touching genuinely
        different employers ('Reef Parking' vs 'University of Miami')."""

        def _flat(s: str) -> str:
            s = (s or "").lower()
            for j in (" the ", "the ", " at ", " of ", " and ", " on ", " a "):
                s = s.replace(j, " ")
            return "".join(ch for ch in s if ch.isalpha())

        fa, fb = _flat(a), _flat(b)
        if not fa or not fb:
            return False
        if fa == fb:
            return True
        short, lng = (fa, fb) if len(fa) <= len(fb) else (fb, fa)
        return len(short) >= 6 and short in lng

    # [move_trust_gate] The lookup is namesake-SAFE only when we have a LinkedIn
    # slug to anchor on (find_current_employer queries that exact profile). With
    # a slug, a detected move is TRUSTED and auto-applied (this is how Hakeem
    # Harvey's real Towne Park -> Liberty Parking was caught). WITHOUT a slug,
    # the lookup is a name+org search that can grab a same-name stranger or a
    # past job -- so a detected move is NOT written; it is QUEUED in
    # pending_moves for a human to approve/reject.
    has_slug = bool((row.linkedin_url or "").strip())
    _moved_candidate = bool(
        new_employer
        and org
        and _norm_org(new_employer) != _norm_org(org)
        and (
            (ce_result.get("moved") if _ce_found else False)
            or (dossier.get("employer_changed") and conf >= GROUNDED_CONFIDENCE_FLOOR)
        )
    )
    queue_pending_move = False
    if _moved_candidate and _ce_found and has_slug:
        job_changed = True  # slug-verified -> trust -> auto-apply
    elif _moved_candidate:
        job_changed = False  # unverified -> do NOT write; queue for review below
        queue_pending_move = True
    else:
        job_changed = False
    # [reformat_veto] A spaced org name vs its glued domain label (or a sub-string
    # reformat) is the SAME employer, not a move. Catches 'The Stella Hotel' ->
    # 'The Stella Hotel', 'Corporate Concierge Services' -> 'Corporateconcierge'.
    # Safe: genuinely different employers (Reef Parking -> University of Miami)
    # are not affected.
    if job_changed and _same_employer(new_employer, org):
        logger.info(
            f"tier2: [reformat_veto] contact {contact_id} {org!r} ~= "
            f"{new_employer!r} (same employer, formatting only); not a move"
        )
        job_changed = False
        left_industry = False
    # also suppress reformats / still-at-email-domain from the review queue --
    # those aren't moves at all (Lauren Roberts still @royalparkhotel.net)
    if queue_pending_move:
        _dom = (domain or "").split(".")[0].lower()
        _dom_flat = "".join(ch for ch in _dom if ch.isalpha())
        _new_flat = "".join(ch for ch in (new_employer or "").lower() if ch.isalpha())
        if _same_employer(new_employer, org) or (len(_dom_flat) >= 5 and _dom_flat in _new_flat):
            logger.info(
                f"tier2: [queue_suppress] contact {contact_id} {new_employer!r} "
                f"is the same place / email-domain employer; not queuing"
            )
            queue_pending_move = False

    # [departed_out_of_industry] If the new employer is clearly NOT hospitality
    # (realtor, insurance, tech, etc.), the person LEFT the industry: they are no
    # longer a sellable contact. Keep the FORMER affiliation (history + successor
    # path) but do NOT relabel their org to the non-hotel employer.
    _ne = (new_employer or "").lower()
    _nt = (new_title or "").lower()
    _NON_HOSPITALITY = (
        "realty",
        "realtor",
        "real estate",
        "keller williams",
        "compass",
        "insurance",
        "allstate",
        "state farm",
        "law",
        "attorney",
        "consulting",
        "recruit",
        "staffing",
        "bank",
        "mortgage",
        "automotive",
        "dealership",
    )
    left_industry = job_changed and any(k in _ne or k in _nt for k in _NON_HOSPITALITY)
    if left_industry:
        job_changed = False  # don't re-file under the non-hotel employer
        logger.info(
            f"tier2: contact {contact_id} ({row.email}) LEFT hospitality "
            f"-> {new_employer!r}; keeping former affiliation only, not re-filing org."
        )
    if job_changed:
        logger.info(
            f"tier2: contact {contact_id} ({row.email}) appears to have moved: "
            f"{org!r} -> {new_employer!r} (title {new_title!r})"
        )

    # 3c) Find CURRENT email for a job-changer (2026-06-16): when the person has
    # moved, the email on file is their FORMER employer's address and likely dead.
    # If the caller asked to find an email (find_email=true) and we detected a
    # move, look the person up AT THE NEW EMPLOYER (new org + its domain) and keep
    # the result as a SECONDARY email -- never overwrite the primary, which anchors
    # the thread history. Only kept if found and different from the primary.
    secondary_email = None
    if find_email and job_changed:
        try:
            _new_domain = ""
            _li = grounded_li or (row.linkedin_url or "")
            wiza2 = await _find_email_via_wiza(
                (f"{_nf} {_nl}".strip() or name), new_employer, _new_domain, _li
            )
            if wiza2 and wiza2.get("email"):
                _cand = wiza2["email"].strip().lower()
                if _cand and _cand != (row.email or "").lower():
                    # [moved_email_primary] a moved contact's NEW email is the
                    # PRIMARY (the old one is preserved as former_email in the
                    # former-affiliation notes). Promote it to the primary email
                    # field instead of parking it in secondary_email.
                    found_email = _cand
                    logger.info(
                        f"tier2: found current email for moved contact {contact_id}: "
                        f"{_cand} (at {new_employer!r}) -> primary"
                    )
        except Exception as e:
            logger.warning(f"tier2: current-email lookup failed for {contact_id}: {e}")

    # 4) Write back (grounded source; only fill email if Wiza found one)
    # [bio_trust_gate] Only store the background bio when we can verify it's the
    # RIGHT person -- i.e. a real LinkedIn slug anchored the lookup (existing on
    # file, or newly grounded this run). Without a slug the dossier bio can be a
    # namesake's life story or the LLM narrating that it couldn't ID anyone;
    # storing that pollutes the AI panel with confident-but-wrong info. Same
    # trust principle as the move gate.
    def _is_real_slug(u: str) -> bool:
        u = (u or "").lower()
        return "linkedin.com/" in u and ("/in/" in u or "/pub/" in u)

    bio_verified = _is_real_slug(row.linkedin_url or "") or _is_real_slug(grounded_li or "")
    bg_to_write = (dossier.get("background") or "") if bio_verified else ""
    if (dossier.get("background") or "").strip() and not bio_verified:
        logger.info(
            f"tier2: [bio_unverified] contact {contact_id} has a background bio "
            f"but no LinkedIn slug to verify the person; NOT storing it."
        )
    async with async_session() as session:
        await session.execute(
            text(
                "UPDATE contacts SET "
                "inferred_role = COALESCE(NULLIF(:role,''), inferred_role), "
                "seniority = COALESCE(NULLIF(:sen,''), seniority), "
                "department = COALESCE(NULLIF(:dept,''), department), "
                "is_decision_maker = :dm, background = NULLIF(:bg,''), "
                "enrichment_source = 'grounded', enrichment_confidence = :conf, "
                "enriched_at = :now, enrichment_model = :model "
                + (
                    ", linkedin_url = COALESCE(NULLIF(linkedin_url,''), :grounded_li)"
                    if grounded_li
                    else ""
                )
                # [patch_wiza_primary] Wiza only runs when we have no usable
                # email, so its result is the PRIMARY. When the old email is
                # empty or former/stale, REPLACE it (COALESCE would refuse).
                + (", email = :found_email" if found_email else "")
                + (
                    ", first_name = :nf, last_name = :nl, "
                    "display_name = CASE WHEN COALESCE(display_name,'') = '' "
                    "OR display_name = email THEN :nd ELSE display_name END"
                    if do_name_fill
                    else ""
                )
                + (
                    ", organization = :new_employer, title = :new_title"
                    ", parent_company = NULL, brand_tier = NULL"
                    ", management_company = NULL, matched_hotel_id = NULL"
                    ", matched_lead_id = NULL"
                    if job_changed
                    else ""
                )
                + (", secondary_email = :secondary_email" if secondary_email else "")
                + " WHERE id = :id"
            ),
            {
                "role": dossier.get("role") or "",
                "sen": dossier.get("seniority") or "",
                "dept": dossier.get("department") or "",
                "dm": bool(dossier.get("is_decision_maker")),
                "bg": bg_to_write,
                "conf": conf,
                "now": _now(),
                "model": MODEL,
                "id": contact_id,
                **({"found_email": found_email} if found_email else {}),
                **({"grounded_li": grounded_li} if grounded_li else {}),
                **({"nf": _nf, "nl": _nl, "nd": f"{_nf} {_nl}"} if do_name_fill else {}),
                **(
                    {
                        "new_employer": new_employer,
                        "new_title": new_title or dossier.get("role") or "",
                    }
                    if job_changed
                    else {}
                ),
                **({"secondary_email": secondary_email} if secondary_email else {}),
            },
        )
        # Preserve the OLD org as a FORMER affiliation so the inbox relationship
        # and history aren't lost when the headline re-files to the new employer.
        # relationship='former' is the schema-valid value (contact_affiliations
        # CHECK: employed_by/stationed_at/covers/former). Idempotent + non-fatal:
        # the headline re-file is the important part; the edge is a bonus.
        if (job_changed or left_industry) and former_org:
            try:
                exists = (
                    await session.execute(
                        text(
                            "SELECT 1 FROM contact_affiliations "
                            "WHERE person_type = 'contact' AND person_id = :pid "
                            "AND relationship = 'former' "
                            "AND lower(COALESCE(account_name,'')) = lower(:nm) LIMIT 1"
                        ),
                        {"pid": contact_id, "nm": former_org},
                    )
                ).one_or_none()
                if not exists:
                    await session.execute(
                        text(
                            "INSERT INTO contact_affiliations "
                            "(person_type, person_id, account_type, account_name, "
                            "relationship, source, confidence, notes, title, created_at, updated_at) "
                            "VALUES ('contact', :pid, 'management_company', :nm, "
                            "'former', 'grounded', :conf, :notes, :ftitle, :now, :now)"
                        ),
                        {
                            "pid": contact_id,
                            "nm": former_org,
                            "conf": conf,
                            "ftitle": (row.title or "").strip(),
                            # [preserve_old_email] keep the stale address in history
                            # (row.email is still the original on-file email here).
                            "notes": (
                                f"Moved to {new_employer} (per deep-enrich research)"
                                + (
                                    f" | former_email={row.email}"
                                    if (row.email and "@" in (row.email or ""))
                                    else ""
                                )
                            ),
                            "now": _now(),
                        },
                    )
                    logger.info(
                        f"tier2: recorded former affiliation {former_org!r} "
                        f"for contact {contact_id}"
                    )
            except Exception as e:
                logger.warning(f"tier2: former-affiliation write failed for {contact_id}: {e}")
        # [queue_unverified_move] No slug to verify the person -> do NOT touch the
        # live contact. Park the candidate in pending_moves for human review.
        if queue_pending_move and new_employer:
            try:
                await session.execute(
                    text(
                        "INSERT INTO pending_moves "
                        "(contact_id, email, name, from_org, to_org, to_title, "
                        " evidence, citations, reason, status, created_at) "
                        "VALUES (:cid, :em, :nm, :fo, :to, :tt, :ev, :ci, "
                        "'no_linkedin_slug', 'pending', :now) "
                        "ON CONFLICT (contact_id) WHERE status='pending' "
                        "DO UPDATE SET to_org=EXCLUDED.to_org, to_title=EXCLUDED.to_title, "
                        "evidence=EXCLUDED.evidence, citations=EXCLUDED.citations, "
                        "created_at=EXCLUDED.created_at"
                    ),
                    {
                        "cid": contact_id,
                        "em": row.email,
                        "nm": name,
                        "fo": org,
                        "to": new_employer,
                        "tt": (new_title or "").strip(),
                        "ev": (
                            ce_result.get("evidence")
                            if _ce_found
                            else dossier.get("background") or ""
                        )[:300]
                        if (ce_result or dossier)
                        else "",
                        "ci": ",".join((ce_result.get("citations") or [])[:5]) if _ce_found else "",
                        "now": _now(),
                    },
                )
                logger.info(
                    f"tier2: [queued_move] contact {contact_id} {org!r} -> "
                    f"{new_employer!r} queued for review (no slug to verify)"
                )
            except Exception as e:
                logger.warning(f"tier2: pending_moves queue failed for {contact_id}: {e}")
        if job_changed:  # [patch_move_coverage_transition] coverage follows the person
            try:
                from app.services.contact_autolink import retire_and_relink

                await retire_and_relink(session, "contact", contact_id, new_employer)
            except Exception as _e:
                logger.warning(f"tier2: coverage transition failed for {contact_id}: {_e}")
        # Phase 2: record the DATED work history as affiliations (045 columns).
        # relationship 'employed_by' = open/current role (end_date NULL), 'former'
        # = ended role. The ABSENCE of any open row is what tells the UI the
        # current employer is unknown. Idempotent on (person, name, relationship).
        if job_history and (job_history.get("roles") or []):

            def _ym(s):
                s = (s or "").strip()
                if len(s) >= 7 and s[4] == "-":
                    try:
                        from datetime import date as _date

                        return _date(int(s[:4]), int(s[5:7]), 1)
                    except Exception:
                        return None
                return None

            for _role in job_history["roles"]:
                _emp = (_role.get("employer") or "").strip()
                if not _emp:
                    continue
                _rel = "employed_by" if _role.get("is_current") else "former"
                try:
                    _seen = (
                        await session.execute(
                            text(
                                "SELECT id FROM contact_affiliations "
                                "WHERE person_type = 'contact' AND person_id = :pid "
                                "AND lower(COALESCE(account_name,'')) = lower(:nm) "
                                "AND relationship = :rel LIMIT 1"
                            ),
                            {"pid": contact_id, "nm": _emp, "rel": _rel},
                        )
                    ).one_or_none()
                    if _seen:
                        await session.execute(
                            text(
                                "UPDATE contact_affiliations SET "
                                "title = COALESCE(NULLIF(:title,''), title), "
                                "start_date = COALESCE(CAST(:sd AS date), start_date), "
                                "end_date = COALESCE(CAST(:ed AS date), end_date), "
                                "updated_at = :now WHERE id = :aid"
                            ),
                            {
                                "title": _role.get("title") or "",
                                "sd": _ym(_role.get("start")),
                                "ed": _ym(_role.get("end")),
                                "now": _now(),
                                "aid": _seen[0],
                            },
                        )
                    else:
                        await session.execute(
                            text(
                                "INSERT INTO contact_affiliations "
                                "(person_type, person_id, account_type, account_name, "
                                "title, relationship, source, confidence, start_date, "
                                "end_date, notes, created_at, updated_at) VALUES "
                                "('contact', :pid, 'management_company', :nm, :title, "
                                ":rel, 'grounded', :conf, CAST(:sd AS date), CAST(:ed AS date), "
                                ":notes, :now, :now)"
                            ),
                            {
                                "pid": contact_id,
                                "nm": _emp,
                                "title": _role.get("title") or "",
                                "rel": _rel,
                                "conf": conf,
                                "sd": _ym(_role.get("start")),
                                "ed": _ym(_role.get("end")),
                                "notes": "deep-enrich job history",
                                "now": _now(),
                            },
                        )
                except Exception as e:
                    logger.warning(
                        f"tier2: role-affiliation write failed for {contact_id} " f"({_emp!r}): {e}"
                    )
            logger.info(
                f"tier2: recorded {len(job_history['roles'])} dated role(s) for "
                f"contact {contact_id}"
            )
        await session.commit()

    return {
        "contact_id": contact_id,
        "name": name,
        "role": dossier.get("role"),
        "seniority": dossier.get("seniority"),
        "department": dossier.get("department"),
        "is_decision_maker": dossier.get("is_decision_maker"),
        "background": dossier.get("background"),
        "found_email": found_email,
        "linkedin_url": grounded_li,
        "confidence": conf,
        "sources_used": len(snippets),
        "employer_changed": job_changed,
        "left_industry": left_industry,
        "current_employer": new_employer
        if job_changed
        else (new_employer if left_industry else None),
        "former_employer": former_org if (job_changed or left_industry) else None,
        "secondary_email": secondary_email,
    }


async def enrich_batch_deep(contact_ids: list[int], find_email: bool = False) -> list[dict]:
    """Deep-enrich a chosen set (e.g. one company's contacts, or all DMs).

    Logs live per-contact progress so long CLI / uvicorn runs show their work
    instead of sitting silent ("[42/200] #1731 Ruby Ozretic -> role + linkedin").
    """
    out = []
    total = len(contact_ids)
    for i, cid in enumerate(contact_ids, 1):
        try:
            r = await enrich_contact_deep(cid, find_email=find_email)
        except Exception as e:
            logger.warning(f"[{i}/{total}] #{cid} enrich FAILED: {e}")
            out.append({"contact_id": cid, "error": str(e)})
            continue
        got = []
        if r.get("role"):
            got.append(f"role={r['role']}")
        if r.get("linkedin_url"):
            got.append("linkedin")
        if r.get("found_email"):
            got.append("email")
        nm = (r.get("name") or "").strip() or "(no name)"
        summary = ", ".join(got) if got else "no new fields"
        logger.info(f"[{i}/{total}] #{cid} {nm} -> {summary}")
        out.append(r)
    return out
