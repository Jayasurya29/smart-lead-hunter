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
                    "organization, linkedin_url FROM contacts WHERE id = :id"
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

    # 2) Wiza — only if asked and we lack a usable email
    found_email = None
    if find_email and (not row.email or "@" not in (row.email or "")):
        wiza = await _find_email_via_wiza(name, org, domain, row.linkedin_url)
        if wiza and wiza.get("email"):
            found_email = wiza["email"]

    # 3) Synthesis
    contact_blob = json.dumps(
        {
            "name": name,
            "email": row.email,
            "organization": org
            or (f"(likely {inferred_org}, inferred from email domain)" if inferred_org else ""),
            "current_title": row.title,
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
                if need_li and g.get("linkedin_url"):
                    grounded_li = g["linkedin_url"]
                conf = max(conf, GROUNDED_CONFIDENCE_FLOOR)
        except Exception as e:
            logger.warning(f"tier2: grounded backup failed for {contact_id}: {e}")

    # 4) Write back (grounded source; only fill email if Wiza found one)
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
                + (", email = COALESCE(email, :found_email)" if found_email else "")
                + (
                    ", first_name = :nf, last_name = :nl, "
                    "display_name = CASE WHEN COALESCE(display_name,'') = '' "
                    "OR display_name = email THEN :nd ELSE display_name END"
                    if do_name_fill
                    else ""
                )
                + " WHERE id = :id"
            ),
            {
                "role": dossier.get("role") or "",
                "sen": dossier.get("seniority") or "",
                "dept": dossier.get("department") or "",
                "dm": bool(dossier.get("is_decision_maker")),
                "bg": dossier.get("background") or "",
                "conf": conf,
                "now": _now(),
                "model": MODEL,
                "id": contact_id,
                **({"found_email": found_email} if found_email else {}),
                **({"grounded_li": grounded_li} if grounded_li else {}),
                **({"nf": _nf, "nl": _nl, "nd": f"{_nf} {_nl}"} if do_name_fill else {}),
            },
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
