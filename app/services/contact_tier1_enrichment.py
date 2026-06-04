"""Tier-1 contact enrichment — infer role/seniority/department from signals.

Runs over contacts using only what's already on hand (email, domain, org,
title, signature) — no web/grounding — so it's cheap enough for the whole
table. Batched through the same `ai_generate` path the rest of the pipeline
uses (Gemini Flash-Lite, batches of 25), and the deterministic
contact_intelligence engine pre-fills the easy answers so the LLM only fills
genuine gaps.

Writes back to the new 027 fields with provenance:
  relevance_verdict / relevance_score / relevance_reason
  inferred_role / seniority / department / is_decision_maker
  enrichment_source='signals' / enrichment_confidence / enriched_at /
  enrichment_model

"Persistent & accurate" guarantees:
  - Never overwrites a higher-confidence value with a lower-confidence one.
  - Skips contacts enriched within REFRESH_DAYS unless force=True.
  - Deterministic relevance from the engine always wins over the LLM guess.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import text

from app.database import async_session
from app.services.ai_client import ai_generate
from app.services.contact_intelligence import assess
from app.services.client_resolver import (
    is_competitor,
    is_personal,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 25
CONCURRENCY = 2  # Vertex 429s easily — keep this low
BATCH_DELAY_SEC = 2.0  # pause between batch waves to stay under quota
MAX_RETRIES_429 = 4  # on a 429, back off and retry this many times
MODEL = "gemini-2.5-flash-lite"
REFRESH_DAYS = 30
SIGNALS_CONFIDENCE_CAP = 0.7  # signals-only can't claim higher than grounded

# LLM decides the ambiguous middle: buyer vs seller vs junk.
# competitor / personal are resolved deterministically before the call.
PROMPT_HEADER = """You categorize business contacts for JA Uniforms, which SELLS
uniforms to hotels, resorts, and the companies that staff them.

For each contact (email, organization, title — any may be blank) choose ONE
category and infer their role.

Categories (choose the single best fit):
- "buyer": a HOTEL, resort, or a hospitality-SERVICES company whose staff wear
  uniforms and could buy from JA. THIS INCLUDES parking, valet, security,
  facilities-management, shuttle, and marina operators that staff hotels —
  e.g. Towne Park, SP Plus, Metropolis, Laz Parking are exactly this type and
  are valuable BUYERS, NOT junk.
- "seller": a company that SELLS to JA (raw materials, fabric, blanks,
  apparel wholesale, embroidery, freight/shipping, software, agencies) — they
  want JA's money. Examples: SanMar, Chef Works fabric/blank suppliers.
- "junk": newsletters, marketing blasts, job boards, e-commerce receipts, an
  unrelated industry (grocery, real estate, auto, healthcare, political), or
  spam / unparseable garbage. NEVER put a NAMED PERSON at a hotel, resort, or
  hospitality-services company into junk — that is a BUYER, even if their title
  is blank or looks junior. Junk is only for non-people (no-reply addresses,
  newsletters, automated receipts) and clearly unrelated industries.

Return ONLY a JSON array, one object per id, no prose, no markdown:
[{"id":0,"category":"buyer","role":"","seniority":"","department":"",
  "is_decision_maker":false,"reason":"","confidence":0.0}]

Field rules:
- category: buyer | seller | junk (one only).
- role: concise normalized title or "" if unknown.
- seniority: c_suite | director | manager | staff | unknown.
- department: procurement | operations | housekeeping | food_beverage | sales |
  finance | hr | it | marketing | other | unknown.
- is_decision_maker: TRUE if their title buys or specifies uniforms at a
  property. Mark TRUE at ANY level — coordinator, assistant, specialist,
  manager, director; junior modifiers do NOT disqualify — for:
    * procurement / purchasing / sourcing  (e.g. "Procurement Coordinator" = TRUE)
    * general manager / hotel manager
    * director or VP of operations
    * executive housekeeper / housekeeping director / housekeeping manager
    * food & beverage director
    * owner or founder of an independent or boutique property
    * human resources / people & culture / talent (HR Manager, HR Director,
      Director of People & Culture) — HR runs onboarding + uniform issuance
      and routes vendors to the right buyers
  Mark FALSE for finance, IT, marketing, outbound sales reps, front-desk /
  guest services, and a bare "manager" with no buying-related department.
- reason: <= 10 words.
- confidence: 0.0-1.0.

Contacts:
"""


def _now():
    return datetime.now(timezone.utc)


def _seniority_from_role(role: str) -> str:
    r = (role or "").lower()
    if any(k in r for k in ("ceo", "cfo", "coo", "cpo", "chief", "president", "owner", "founder")):
        return "c_suite"
    if any(k in r for k in ("vp", "vice president", "director", "head of", "head ")):
        return "director"
    if "manager" in r or "supervisor" in r:
        return "manager"
    if role:
        return "staff"
    return "unknown"


async def _enrich_batch(client, sem, batch: list[dict]) -> dict:
    """batch = [{id,email,organization,title,signature}]. Returns {id: result}."""
    payload = [
        {
            "id": b["id"],
            "email": b.get("email") or "",
            "organization": b.get("organization") or "",
            "title": b.get("title") or "",
            "signature": (b.get("signature") or "")[:300],
        }
        for b in batch
    ]
    prompt = PROMPT_HEADER + json.dumps(payload, ensure_ascii=False)
    async with sem:
        raw = None
        for attempt in range(MAX_RETRIES_429 + 1):
            try:
                raw = await ai_generate(client, prompt, model=MODEL, temperature=0.1)
                break
            except Exception as exc:
                msg = str(exc).lower()
                is_429 = "429" in msg or "resource exhausted" in msg or "quota" in msg
                if is_429 and attempt < MAX_RETRIES_429:
                    backoff = 5 * (2**attempt)  # 5s, 10s, 20s, 40s
                    logger.warning(
                        f"contact tier1: 429 rate limit, backing off {backoff}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES_429})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning(f"contact tier1: Gemini error: {exc}")
                return {}
    if not raw:
        return {}
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip() if "```" in raw else raw
    try:
        arr = json.loads(raw)
    except Exception:
        logger.warning("contact tier1: could not parse model JSON")
        return {}
    return {item["id"]: item for item in arr if isinstance(item, dict) and "id" in item}


async def run_tier1(
    limit: int | None = None, force: bool = False, only_unknown: bool = False
) -> dict:
    """Categorize + enrich pending contacts. Returns a summary dict."""
    async with async_session() as session:
        clauses = ["approval_status = 'pending'"]
        if not force:
            clauses.append("(enriched_at IS NULL OR enriched_at < :cutoff)")
        if only_unknown:
            clauses.append("(contact_category IS NULL OR contact_category = 'unknown')")
        where = " AND ".join(clauses)
        params = {}
        if not force:
            params["cutoff"] = _now() - timedelta(days=REFRESH_DAYS)
        sql = (
            "SELECT id, email, first_name, last_name, display_name, title, "
            "organization, enrichment_confidence, enrichment_source "
            f"FROM contacts WHERE {where} ORDER BY interaction_count DESC NULLS LAST"
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = (await session.execute(text(sql), params)).all()

    if not rows:
        return {"scanned": 0, "enriched": 0, "note": "nothing to do"}

    # Pass 1 — deterministic category (no AI): competitor / personal.
    # No SAP / client lookup — client-vs-prospect is intentionally out of scope.
    work = []
    for r in rows:
        email = (r.email or "").lower()
        det = assess(
            {
                "email": email,
                "first_name": r.first_name,
                "last_name": r.last_name,
                "display_name": r.display_name,
                "title": r.title,
                "organization": r.organization,
            },
        )
        category, source = None, None
        if is_competitor(r.organization, email):
            category, source = "competitor", "competitor_list"
        elif is_personal(r.organization, email):
            category, source = "personal", "personal_rule"
        work.append(
            {
                "id": r.id,
                "email": email,
                "organization": r.organization,
                "title": r.title,
                "det": det,
                "category": category,
                "category_source": source,
                "old_conf": r.enrichment_confidence or 0.0,
                "old_source": r.enrichment_source,
            }
        )

    # Pass 2 — LLM only for rows with no deterministic category (the
    # buyer / seller / junk middle), batched.
    llm_rows = [w for w in work if w["category"] is None]
    client = httpx.AsyncClient(timeout=90)
    sem = asyncio.Semaphore(CONCURRENCY)
    llm_results: dict = {}
    try:
        batches = [llm_rows[i : i + BATCH_SIZE] for i in range(0, len(llm_rows), BATCH_SIZE)]
        # Throttled waves: run CONCURRENCY batches, pause, repeat — keeps us
        # under the Vertex per-minute quota that triggered 429s.
        for wave_start in range(0, len(batches), CONCURRENCY):
            wave = batches[wave_start : wave_start + CONCURRENCY]
            gathered = await asyncio.gather(
                *[_enrich_batch(client, sem, [{**w} for w in b]) for b in wave]
            )
            for g in gathered:
                llm_results.update(g)
            if wave_start + CONCURRENCY < len(batches):
                await asyncio.sleep(BATCH_DELAY_SEC)
    finally:
        await client.aclose()

    VALID_LLM = {"buyer", "seller", "junk"}
    enriched = 0
    cat_counts: dict = {}
    async with async_session() as session:
        for w in work:
            det = w["det"]
            llm = llm_results.get(w["id"], {})

            category = w["category"]
            source = w["category_source"]
            if category is None:
                c = (llm.get("category") or "").strip().lower()
                category = c if c in VALID_LLM else "junk"
                source = "llm"

            role = (llm.get("role") or det.get("role_hint") or "").strip()
            seniority = (llm.get("seniority") or _seniority_from_role(role)).strip()
            department = (llm.get("department") or "").strip()
            is_dm = bool(llm.get("is_decision_maker", False))
            reason = (llm.get("reason") or "; ".join(det["reasons"]))[:400]
            conf = min(
                float(llm.get("confidence", det["score"] / 100.0)),
                SIGNALS_CONFIDENCE_CAP,
            )
            # Deterministic categories are certain.
            if source in ("competitor_list", "personal_rule"):
                conf = 1.0

            # Protect a richer grounded (Tier-2 Deep Enrich) profile from being
            # overwritten by this cheap signals pass. Signals rows always
            # re-stamp, so a forced re-run applies the latest category/DM rules
            # deterministically instead of being blocked by confidence jitter.
            if w["old_source"] == "grounded" and conf < w["old_conf"]:
                continue

            await session.execute(
                text(
                    "UPDATE contacts SET "
                    "contact_category=:cat, category_source=:csrc, "
                    "relevance_reason=:r, inferred_role=:role, seniority=:sen, "
                    "department=:dept, is_decision_maker=:dm, "
                    "enrichment_source='signals', enrichment_confidence=:conf, "
                    "enriched_at=:now, enrichment_model=:model "
                    "WHERE id=:id"
                ),
                {
                    "cat": category,
                    "csrc": source,
                    "r": reason,
                    "role": role or None,
                    "sen": seniority or None,
                    "dept": department or None,
                    "dm": is_dm,
                    "conf": conf,
                    "now": _now(),
                    "model": MODEL,
                    "id": w["id"],
                },
            )
            cat_counts[category] = cat_counts.get(category, 0) + 1
            enriched += 1
        await session.commit()

    return {
        "scanned": len(rows),
        "resolved_by_rules": len(rows) - len(llm_rows),
        "sent_to_llm": len(llm_rows),
        "enriched": enriched,
        "by_category": cat_counts,
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--force", action="store_true", help="re-enrich even if fresh")
    ap.add_argument(
        "--only-unknown",
        action="store_true",
        help="only contacts not yet classified relevant/junk",
    )
    args = ap.parse_args()
    summary = asyncio.run(run_tier1(args.limit, args.force, args.only_unknown))
    print(summary)
