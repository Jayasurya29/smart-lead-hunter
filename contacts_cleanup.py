"""contacts_cleanup.py — backfill names + canonicalize organizations (2026-06-04).

Fixes two data problems in the inbox `contacts` table:

  1. NAMELESS CONTACTS — rows with no first/last/display name (the UI falls
     back to showing the raw email). Derives names from email localparts:
     jay.finkelstein@ → Jay Finkelstein. Single-token localparts (fching@)
     are left alone — guessing splits is worse than no name.

  2. ORG FRAGMENTATION — same email domain split across org variants:
     'Rosenplaza' / 'rosenplaza.com' / 'Rosen Plaza Hotel' / 'Rosen Plaza'
     = 4 directory accounts for one hotel. For each non-freemail domain the
     most frequent PROPER org becomes canonical; rows whose org is missing,
     a bare-domain echo, or a compressed variant of the canonical get
     rewritten. Rows with a genuinely DIFFERENT org on a shared domain
     (e.g. 'River Market Hotel' under crestlinehotels.com) are untouched.

Usage (PowerShell, repo root, venv active):
  $env:DATABASE_URL='postgresql+asyncpg://...' ; python contacts_cleanup.py            # dry run
  $env:DATABASE_URL='postgresql+asyncpg://...' ; python contacts_cleanup.py --apply    # write changes
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections import Counter, defaultdict

from sqlalchemy import text

from app.database import async_session
from app.services.contact_dedup import (
    derive_name_from_email,
    is_degenerate_org,
    is_freemail_domain,
    pick_canonical_org,
)

OWN_DOMAINS = {"jauniforms.com"}
SAMPLE_LIMIT = 25


def _compressed(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


_GENERIC_ORG_TOKENS = {
    "hotel", "hotels", "resort", "resorts", "inn", "suites", "suite",
    "lodge", "spa", "club", "clubs", "the", "and", "amp", "co", "inc", "llc", "ltd",
    "corp", "corporation", "company", "group", "valet", "services",
    "service", "intl", "international",
    # saved-contact pollution (2026-06-05): phone-book entries like
    # 'Chefworks President', 'Chefworks Login', 'Edwards Marketing' store
    # a role next to the company name — never a distinct organization
    "president", "login", "marketing", "sales", "orders", "support",
    "accounting", "billing", "custom", "customer", "info", "office",
    "team", "dept", "department", "garment", "garments", "uniforms",
}


def _tokens(s: str) -> set:
    return {t for t in re.split(r"[^a-z0-9]+", (s or "").lower()) if t}


def _is_variant_of(org: str, canonical: str, domain: str) -> bool:
    """Should `org` be rewritten to `canonical`?

    2026-06-04 refinement after dry-run review:
      • org LESS specific than canonical → merge ('Towne' → 'Towne Park',
        'DENISON' → 'Denison Parking', case variants).
      • org MORE specific → merge ONLY when the extra tokens are generic
        ('Rosen Plaza Hotel' → 'Rosen Plaza': extra={hotel} ✓;
        'Denison Parking, Inc.' → extra={inc} ✓) — but distinctive extras
        mean a DIFFERENT sub-property and must be preserved
        ('GRAND BEACH HOTEL SURFSIDE' stays — Surfside ≠ Miami Beach).
    """
    if not org or org == canonical:
        return False
    if is_degenerate_org(org, domain):
        # 2026-06-05 guard: an org equal to the domain root is the BRAND,
        # not a junk label, and big-brand corporate domains (hilton.com,
        # hyatt.com) host staff from MANY properties. A bare-brand org may
        # only merge into a canonical that is the dressed-up brand itself —
        # every distinctive token of the canonical must live inside the
        # root ('Rosenhotels' -> 'Rosen Hotels & Resorts' ok;
        # 'Hyatt' -> 'Grand Hyatt Grand Cayman' NEVER; 'Hilton' ->
        # 'Hotel Maren' NEVER).
        root = re.sub(r"[^a-z0-9]", "", domain.split(".")[0].lower())
        if _compressed(org) == root:
            canon_core = {
                t
                for t in _tokens(canonical)
                if t not in _GENERIC_ORG_TOKENS and len(t) >= 2
            }
            return bool(canon_core) and all(t in root for t in canon_core)
        return True
    # saved-contact nicknames in quotes are never part of an org name:
    # 'Edwards Garments Custom "Bishop"' / 'Edwards Marketing "Taraynn"'
    org = re.sub(r"[\"\u201c\u201d'\u2018\u2019]+[^\"\u201c\u201d'\u2018\u2019]*[\"\u201c\u201d'\u2018\u2019]+", " ", org).strip()
    a, b = _compressed(org), _compressed(canonical)
    if not a or not b:
        return False
    # bare-brand guard AFTER quote-stripping (2026-06-05): 'Hyatt "Bonita
    # Springs, FL"' strips to bare 'Hyatt' — at a multi-property corporate
    # domain that must NOT flow into a specific property via containment.
    _root = re.sub(r"[^a-z0-9]", "", domain.split(".")[0].lower())
    if a == _root:
        _canon_core = {
            t
            for t in _tokens(canonical)
            if t not in _GENERIC_ORG_TOKENS and len(t) >= 2
        }
        return bool(_canon_core) and all(t in _root for t in _canon_core)
    if a in b:
        return True  # less specific / case variant → safe merge
    if b in a:
        # extra = org tokens not present in canonical (tolerating fused
        # tokens: 'TOWNEPARK VALET SERVICES' → 'townepark' ⊂ compressed
        # canonical, so only {valet, services} count as extra)
        extra = {
            t for t in _tokens(org) if t not in _tokens(canonical) and t not in b
        }
        return bool(extra) and extra <= _GENERIC_ORG_TOKENS
    # path C (2026-06-05): after stripping quotes/roles, the org's
    # DISTINCTIVE tokens are a subset of the canonical's distinctive tokens
    # ('Edwards Marketing "Taraynn"' -> {edwards} ⊆ {edwards, garment})
    org_core = {t for t in _tokens(org) if t not in _GENERIC_ORG_TOKENS}
    canon_core = {t for t in _tokens(canonical) if t not in _GENERIC_ORG_TOKENS}
    return bool(org_core) and org_core <= canon_core



async def main(apply: bool, use_ai: bool = True, refresh_ai: bool = False) -> None:
    async with async_session() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT id, email, first_name, last_name, display_name, "
                        "organization, contact_category, approval_status "
                        "FROM contacts WHERE email IS NOT NULL "
                        "AND email LIKE '%@%'"
                    )
                )
            )
            .mappings()
            .all()
        )
        print(f"Loaded {len(rows)} contacts\n")

        # ── Phase 1: names from localparts ──────────────────────────
        name_updates: list[tuple[int, str, str, str, str]] = []
        single_token_nameless = 0
        for r in rows:
            email = r["email"].lower()
            disp = (r["display_name"] or "").strip()
            nameless = not (r["first_name"] or "").strip() and not (
                r["last_name"] or ""
            ).strip()
            disp_is_email = disp.lower() in ("", email, email.split("@", 1)[0])
            if not (nameless and disp_is_email):
                continue
            first, last, display = derive_name_from_email(email)
            if first and last:
                name_updates.append((r["id"], email, first, last, display))
            else:
                single_token_nameless += 1

        print("── Phase 1: NAMES ──")
        print(f"  derivable from localpart : {len(name_updates)}")
        print(f"  unparseable (left as-is) : {single_token_nameless}")
        for cid, email, first, last, _ in name_updates[:SAMPLE_LIMIT]:
            print(f"    #{cid:<6} {email:<45} → {first} {last}")
        if len(name_updates) > SAMPLE_LIMIT:
            print(f"    ... and {len(name_updates) - SAMPLE_LIMIT} more")

        # ── Phase 1b: company-name-as-first-name ("Chefworks Kaplan") ──
        # Saved-contact pollution: first_name is the company. If the email
        # localpart ends with the surname's initial, the rest is the real
        # first name (jeffk + Kaplan -> Jeff); otherwise blank the fake one.
        name_fixes_1b: list[tuple[int, str, str, str]] = []
        for r in rows:
            fn = (r["first_name"] or "").strip()
            ln = (r["last_name"] or "").strip()
            if not fn or not ln or "@" not in (r["email"] or ""):
                continue
            local, dom = r["email"].lower().split("@", 1)
            root = re.sub(r"[^a-z0-9]", "", dom.split(".")[0])
            fnc = re.sub(r"[^a-z0-9]", "", fn.lower())
            if not root or fnc != root:
                continue
            loc = re.sub(r"[^a-z0-9]", "", local)
            new_first = ""
            if loc.endswith(ln[0].lower()) and len(loc) >= 4:
                cand = loc[:-1]
                # must look like a human first name: alphabetic, sane length,
                # contains a vowel (rejects acronym localparts like 'frmbc')
                if (
                    cand.isalpha()
                    and 2 < len(cand) <= 12
                    and any(v in cand for v in "aeiouy")
                ):
                    new_first = cand.title()
            new_disp = f"{new_first} {ln}".strip() if new_first else ln
            name_fixes_1b.append((r["id"], r["email"], new_first, new_disp))
        print("\n── Phase 1b: COMPANY-AS-FIRST-NAME ──")
        print(f"  polluted rows : {len(name_fixes_1b)}")
        for cid, em, nf, nd in name_fixes_1b[:SAMPLE_LIMIT]:
            print(f"    #{cid:<6} {em:<45} -> first={nf or '(blank)'}  display={nd!r}")

        # ── Phase 2: org canonicalization per domain ────────────────
        by_domain: dict[str, list] = defaultdict(list)
        for r in rows:
            domain = r["email"].lower().split("@", 1)[1]
            if is_freemail_domain(domain) or domain in OWN_DOMAINS:
                continue
            by_domain[domain].append(r)

        org_updates: list[tuple[int, str, str, str]] = []  # id, email, old, new
        domains_unified = 0
        for domain, members in by_domain.items():
            if len(members) < 2:
                continue
            proper = Counter(
                (m["organization"] or "").strip()
                for m in members
                if (m["organization"] or "").strip()
                and not is_degenerate_org(m["organization"], domain)
            )
            if not proper:
                continue
            canonical = pick_canonical_org(dict(proper), domain)
            if not canonical:
                continue
            domain_changed = False
            for m in members:
                org = (m["organization"] or "").strip()
                if org == canonical:
                    continue
                if not org or _is_variant_of(org, canonical, domain):
                    org_updates.append((m["id"], m["email"], org or "∅", canonical))
                    domain_changed = True
            if domain_changed:
                domains_unified += 1

        # ── Phase 2b: AI adjudication for what rules can't decide ────
        # Deterministic rules only make provably-safe merges. Whatever
        # fragmentation survives (multiple distinct-looking orgs on one
        # domain) is a JUDGMENT call about the world — 'Hyatt "Bonita
        # Springs"' vs 'Grand Hyatt Grand Cayman' needs to know what Hyatt
        # is. One flash call per ambiguous domain judges: same entity,
        # distinct property, or role-label. Hard constraints in code:
        # canonical must be one of the existing strings, never cross-domain,
        # and everything prints with the model's reason for the dry-run
        # human gate. Disable with --no-ai.
        ai_updates: list[tuple[int, str, str, str, str]] = []
        if use_ai:
            import json as _json

            import httpx as _httpx

            from app.services.ai_client import ai_generate

            mapped_ids = {u[0] for u in org_updates}
            ambiguous: dict[str, dict[str, list]] = {}
            for domain, members in by_domain.items():
                groups: dict[str, list] = defaultdict(list)
                for m in members:
                    if m["id"] in mapped_ids:
                        continue
                    org = (m["organization"] or "").strip()
                    if org:
                        groups[org].append(m)
                if len(groups) >= 2:
                    ambiguous[domain] = groups

            print("\n── Phase 2b: AI ADJUDICATION ──")
            print(f"  ambiguous domains : {len(ambiguous)}")
            # verdict cache: --apply must write EXACTLY what the dry run
            # showed, and re-runs shouldn't re-bill 187 calls. Delete the
            # file (or --refresh-ai) to re-judge.
            import time as _time
            from pathlib import Path as _Path

            cache_path = _Path(".ai_org_verdicts.json")
            cache: dict = {}
            if cache_path.exists() and not refresh_ai:
                try:
                    cache = _json.loads(cache_path.read_text(encoding="utf-8"))
                    print(f"  cached verdicts    : {len(cache)} domain(s) (from {cache_path})")
                except Exception:
                    cache = {}
            if ambiguous:
                _client = _httpx.AsyncClient(timeout=90)
                _sem = asyncio.Semaphore(3)
                _t0 = _time.time()
                _done = [0]

                async def _judge(domain, groups):
                    listing = "\n".join(
                            f'- "{org}" ({len(ms)} contact(s); e.g. '
                            + ", ".join(m["email"] for m in ms[:2])
                            + ")"
                        for org, ms in sorted(
                            groups.items(), key=lambda kv: -len(kv[1])
                        )
                    )
                    prompt = (
                            "You are cleaning a CRM. All contacts below share the "
                            f"email domain {domain}. Decide which organization "
                            "strings refer to the SAME entity and which are "
                            "genuinely DISTINCT (different hotel properties, "
                            "sub-brands, business units).\n\n"
                            "Rules:\n"
                            "- Hotel-brand corporate domains (hilton.com, "
                            "hyatt.com, marriott.com...) host staff of MANY "
                            "different properties: distinct properties MUST stay "
                            "distinct, and a bare brand name stays the brand.\n"
                            "- Saved-contact labels like 'Company Sales', "
                            "'Company \"Nickname\"' are the same entity as the "
                            "company.\n"
                            "- canonical MUST be copied EXACTLY from the strings "
                            "below — never invent a new name.\n\n"
                            f"ORGS ON {domain}:\n{listing}\n\n"
                        "Respond ONLY with JSON: a list of objects "
                        '{"org": "<exact string>", "canonical": "<exact '
                        'string it should become>", "distinct": false, '
                        '"reason": "<10 words>"} — use "distinct": true '
                        "(canonical = org itself) for strings that must "
                        "stay separate."
                    )
                    if domain in cache:
                        verdicts = cache[domain]
                    else:
                        async with _sem:
                            raw = None
                            for attempt in range(3):
                                try:
                                    raw = await ai_generate(
                                        _client, prompt, model="gemini-2.5-flash",
                                        temperature=0.1, max_tokens=2000,
                                    )
                                    break
                                except Exception as e:
                                    if "429" in str(e) and attempt < 2:
                                        wait = 20 * (attempt + 1)
                                        print(f"    {domain}: quota 429 — backing off {wait}s")
                                        await asyncio.sleep(wait)
                                        continue
                                    print(f"    [{_done[0]+1}/{len(ambiguous)}] {domain}: AI call failed ({e})")
                                    _done[0] += 1
                                    return
                            if raw is None:
                                _done[0] += 1
                                return
                        raw = (raw or "").strip()
                        if raw.startswith("```"):
                            raw = raw.split("```")[1].lstrip("json").strip()
                        try:
                            verdicts = _json.loads(raw)
                        except Exception:
                            print(f"    [{_done[0]+1}/{len(ambiguous)}] {domain}: bad JSON from model")
                            _done[0] += 1
                            return
                        cache[domain] = verdicts
                    valid = set(groups.keys())
                    n_merge = 0
                    for v in verdicts if isinstance(verdicts, list) else []:
                        org = (v.get("org") or "").strip()
                        canon = (v.get("canonical") or "").strip()
                        if (
                            v.get("distinct")
                            or org not in valid
                            or canon not in valid
                            or org == canon
                        ):
                            continue
                        # canonical quality gate (2026-06-05): the existing-
                        # strings-only constraint backfired on mohg.com where
                        # the most frequent string was 'MIAMI'. A canonical
                        # must carry at least one distinctive token.
                        _c_core = {
                            t
                            for t in _tokens(canon)
                            if t not in _GENERIC_ORG_TOKENS
                            and t
                            not in {"miami", "orlando", "tampa", "us", "usa"}
                            and len(t) >= 3
                        }
                        if not _c_core:
                            continue
                        reason = (v.get("reason") or "").strip()[:60]
                        for m in groups[org]:
                            ai_updates.append(
                                (m["id"], m["email"], org, canon, reason)
                            )
                            n_merge += 1
                    _done[0] += 1
                    print(
                        f"    [{_done[0]}/{len(ambiguous)}] {domain}: "
                        f"{len(groups)} orgs -> {n_merge} merge row(s)"
                    )

                try:
                    await asyncio.gather(
                        *[_judge(d, g) for d, g in sorted(ambiguous.items())]
                    )
                finally:
                    await _client.aclose()
                cache_path.write_text(
                    _json.dumps(cache, indent=1), encoding="utf-8"
                )
                print(
                    f"  verdicts cached to {cache_path} "
                    f"({_time.time() - _t0:.0f}s elapsed)"
                )
            for cid, email, old_o, new_o, why in ai_updates[:SAMPLE_LIMIT]:
                print(f"    #{cid:<6} {email:<42} {old_o!r} → {new_o!r}  [{why}]")
            if len(ai_updates) > SAMPLE_LIMIT:
                print(f"    ... and {len(ai_updates) - SAMPLE_LIMIT} more")
            print(f"  AI-judged rewrites : {len(ai_updates)}")

        print("\n── Phase 2: ORGANIZATIONS ──")
        print(f"  domains with fragmentation : {domains_unified}")
        print(f"  rows to rewrite            : {len(org_updates)}")
        for cid, email, old, new in org_updates[:SAMPLE_LIMIT]:
            print(f"    #{cid:<6} {email:<45} '{old}' → '{new}'")
        if len(org_updates) > SAMPLE_LIMIT:
            print(f"    ... and {len(org_updates) - SAMPLE_LIMIT} more")

        # ── Phase 3: duplicate-person report (report-only) ──────────
        # Same email / different names is impossible (email is the unique
        # key — names merged into one row). Same NAME / different emails is
        # real: jsmith@ + jay.smith@ aliases on one domain, or old-hotel +
        # new-hotel addresses (a job-change signal). Report, never auto-merge.
        def _person_key(r) -> str:
        # one-letter initials + Jr/Sr stripped, matching enrichment dedup
            nm = " ".join(
                p for p in [(r["first_name"] or ""), (r["last_name"] or "")] if p
            ).strip() or (r["display_name"] or "")
            toks = re.split(r"[^a-z]+", nm.lower())
            return " ".join(
                t for t in toks
                if t and len(t) > 1 and t not in {"jr", "sr", "ii", "iii", "iv"}
            )

        by_person: dict[str, list] = defaultdict(list)
        for r in rows:
            k = _person_key(r)
            if k and " " in k:  # need first+last to be meaningful
                by_person[k].append(r)

        alias_clusters, review_clusters = [], []
        for k, members in by_person.items():
            if len(members) < 2:
                continue
            doms = {m["email"].lower().split("@", 1)[1] for m in members}
            (alias_clusters if len(doms) == 1 else review_clusters).append(
                (k, members)
            )

        print("\n── Phase 3: DUPLICATE PEOPLE (report only — no changes) ──")
        print(f"  same name + same domain (likely aliases) : {len(alias_clusters)}")
        for k, members in alias_clusters[:SAMPLE_LIMIT]:
            ems = ", ".join(m["email"] for m in members)
            print(f"    {k:<28} {ems}")
        print(f"  same name + different domains (review)   : {len(review_clusters)}")
        for k, members in review_clusters[:SAMPLE_LIMIT]:
            parts = ", ".join(
                f"{m['email']} ({(m['organization'] or '?')[:30]})" for m in members
            )
            print(f"    {k:<28} {parts}")
        if review_clusters:
            print(
                "    ↑ different domains = possible job change OR namesake — "
                "eyeball before merging anything"
            )

        # ── Phase 4: junk sweep (legacy rows vs today's hard filters) ──
        # dell_technologies@comms.dell.com, list@wordfence.com and
        # dmarcreport@microsoft.com predate the harvest filters. Re-check
        # every row; failures get contact_category='junk' (hidden by default
        # in the directory, never deleted — reversible). Rows a human already
        # approved/pushed are skipped.
        try:
            from app.services.inbox_sync import _passes_hard_filters
        except Exception as e:
            print(f"\n── Phase 4: JUNK SWEEP skipped (import failed: {e}) ──")
            _passes_hard_filters = None

        junk_updates: list[tuple[int, str, str]] = []
        junk_skipped_approved = 0
        if _passes_hard_filters:
            for r in rows:
                if (r["contact_category"] or "") == "junk":
                    continue
                ok, why = _passes_hard_filters(r["email"].lower(), "")
                if ok:
                    continue
                if (r["approval_status"] or "") in (
                    "approved",
                    "pushed_to_insightly",
                ):
                    junk_skipped_approved += 1
                    continue
                junk_updates.append((r["id"], r["email"], why))

            from collections import Counter as _Counter

            by_reason = _Counter(w for _, _, w in junk_updates)
            print("\n── Phase 4: JUNK SWEEP ──")
            print(f"  rows failing today's filters : {len(junk_updates)}")
            for reason, n in by_reason.most_common():
                print(f"    {reason:<18} {n}")
            print(f"  skipped (human-approved)     : {junk_skipped_approved}")
            for cid, em, why in junk_updates[:SAMPLE_LIMIT]:
                print(f"    #{cid:<6} {em:<50} [{why}]")
            if len(junk_updates) > SAMPLE_LIMIT:
                print(f"    ... and {len(junk_updates) - SAMPLE_LIMIT} more")

        # ── Phase 5: SUSPECTS for human review (report only) ────────
        # Things filters can't decide alone: rows whose "name" is just the
        # company ("Dell Technologies"), and nameless+orgless orphans.
        # Review the list, then mark the bad ids:
        #   python contacts_cleanup.py --mark-junk "123,456,789"
        suspects = []
        for r in rows:
            if (r["contact_category"] or "") == "junk":
                continue
            if any(r["id"] == j[0] for j in junk_updates):
                continue
            disp = (r["display_name"] or "").strip().lower()
            org = (r["organization"] or "").strip().lower()
            nameless = not (
                (r["first_name"] or "").strip() or (r["last_name"] or "").strip()
            )
            if disp and org and disp == org and nameless:
                suspects.append((r["id"], r["email"], "name = company name"))
            elif nameless and not org and not disp:
                suspects.append((r["id"], r["email"], "no name, no org"))
        print("\n── Phase 5: SUSPECTS — review by hand, then --mark-junk ──")
        print(f"  suspicious rows : {len(suspects)}")
        for cid, em, why in suspects[:SAMPLE_LIMIT * 2]:
            print(f"    #{cid:<6} {em:<50} [{why}]")
        if len(suspects) > SAMPLE_LIMIT * 2:
            print(f"    ... and {len(suspects) - SAMPLE_LIMIT * 2} more")

        if not apply:
            print(
                f"\nDRY RUN — nothing written. Re-run with --apply to write: "
                f"{len(name_updates)} names, {len(name_fixes_1b)} company-as-name fixes, "
                f"{len(org_updates)} rule-based org updates, "
                f"{len(ai_updates)} AI-adjudicated org updates"
                + (
                    f", {len(junk_updates)} junk categorizations."
                    if junk_updates
                    else "."
                )
            )
            return

        # ── Apply ────────────────────────────────────────────────────
        for cid, _, first, last, display in name_updates:
            await session.execute(
                text(
                    "UPDATE contacts SET first_name = :f, last_name = :l, "
                    "display_name = :d, updated_at = NOW() WHERE id = :id"
                ),
                {"f": first, "l": last, "d": display, "id": cid},
            )
        for cid, _, _, new in org_updates:
            await session.execute(
                text(
                    "UPDATE contacts SET organization = :o, updated_at = NOW() "
                    "WHERE id = :id"
                ),
                {"o": new, "id": cid},
            )
        for cid, _, _old, new_o, _why in ai_updates:
            await session.execute(
                text(
                    "UPDATE contacts SET organization = :org, "
                    "updated_at = NOW() WHERE id = :id"
                ),
                {"org": new_o, "id": cid},
            )
        for cid, _, nf, nd in name_fixes_1b:
            await session.execute(
                text(
                    "UPDATE contacts SET first_name = NULLIF(:nf,''), "
                    "display_name = :nd, updated_at = NOW() WHERE id = :id"
                ),
                {"nf": nf, "nd": nd, "id": cid},
            )
        for cid, _, why in junk_updates:
            await session.execute(
                text(
                    "UPDATE contacts SET contact_category = 'junk', "
                    "updated_at = NOW() WHERE id = :id"
                ),
                {"id": cid},
            )
        await session.commit()
        print(
            f"\nAPPLIED: {len(name_updates)} names backfilled, "
            f"{len(org_updates)} organizations unified, "
            f"{len(ai_updates)} AI-adjudicated, "
            f"{len(junk_updates)} rows categorized as junk."
        )


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write changes")
    parser.add_argument(
        "--no-ai", action="store_true",
        help="skip Phase 2b AI adjudication of ambiguous org clusters",
    )
    parser.add_argument(
        "--refresh-ai", action="store_true",
        help="ignore .ai_org_verdicts.json cache and re-judge all domains",
    )
    parser.add_argument(
        "--mark-junk",
        default="",
        help="comma-separated contact ids to categorize as junk (human review)",
    )
    args = parser.parse_args()
    if args.mark_junk.strip():
        ids = [int(x) for x in args.mark_junk.split(",") if x.strip().isdigit()]

        async def _mark() -> None:
            async with async_session() as session:
                for cid in ids:
                    await session.execute(
                        text(
                            "UPDATE contacts SET contact_category = 'junk', "
                            "updated_at = NOW() WHERE id = :id"
                        ),
                        {"id": cid},
                    )
                await session.commit()
            print(f"Marked {len(ids)} contact(s) as junk: {ids}")

        asyncio.run(_mark())
    else:
        asyncio.run(
            main(apply=args.apply, use_ai=not args.no_ai, refresh_ai=args.refresh_ai)
        )
