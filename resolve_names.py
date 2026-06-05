"""Batch name resolver: find real names for nameless contacts via the web.

    python resolve_names.py                    # dry run, first 20 nameless
    python resolve_names.py --limit 50         # bigger batch
    python resolve_names.py --limit 50 --apply # write resolved names
    python resolve_names.py --buyers-only      # only contact_category buyer/empty

kmoorhead@themanchesterky.com -> searches '"kmoorhead@themanchesterky.com"'
and '"kmoorhead" "The Manchester"' -> snippets name Katie Moorhead, Guest
Services / Front Office Manager -> first/last/display filled.

Name-only and cheap-ish (2 Serper queries + 1 flash call per contact, no
Wiza), but it still spends API credits on every candidate — including dry
runs, which do the research and just skip the write. Run in batches.
The model is told to return a name ONLY when snippets clearly identify the
email's owner — no guessing from the localpart pattern.
"""

import argparse
import asyncio
import json
import logging
import sys

import httpx
from sqlalchemy import text

from app.database import async_session
from app.services.ai_client import ai_generate

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(level=logging.WARNING)

MODEL = "gemini-2.5-flash"

# Role/shared mailboxes (accountspayable@, purchasing@, frontdesk@...) are
# real correspondents but not PEOPLE — resolving a name onto a shared box
# is misleading and wastes credits. Skipped by default; --include-role to
# attempt them anyway.
_ROLE_SUBSTRINGS = (
    "payable", "accounting", "purchasing", "invoice", "reservation",
    "payroll", "frontdesk", "concierge", "banquet", "catering",
    "support", "customer", "pricing", "information",
)
# token-only role words (NOT substrings — 'sales' as a substring would
# nuke real surnames like rosales@, gonsales@)
_ROLE_TOKENS_EXTRA = {
    "accounts", "account", "sales", "service", "services", "finance",
    "admin", "info", "csr", "ops", "pm", "gm", "apm", "web",
    "orders", "billing", "quotes", "inquiry", "communications",
}


def _is_role_mailbox(local: str) -> bool:
    import re as _re

    try:
        from app.services.inbox_sync import ROLE_LOCALPARTS
    except Exception:
        ROLE_LOCALPARTS = set()
    low = local.lower()
    tokens = [t for t in _re.split(r"[._+\-]", low) if t]
    if any(
        t in ROLE_LOCALPARTS or t in _ROLE_TOKENS_EXTRA or t == "ap"
        for t in tokens
    ):
        return True
    if any(sub in low for sub in _ROLE_SUBSTRINGS):
        return True
    return low.endswith("ap")

PROMPT = """Identify the REAL full name of the person who owns this email
address, using ONLY the web snippets below.

EMAIL: {email}
ORGANIZATION (may be empty): {org}

Respond with ONLY this JSON, nothing else:
{{"first_name":"","last_name":"","title":"","confidence":0.0,"evidence":""}}

Rules:
- Fill first_name/last_name ONLY if the snippets clearly identify the owner
  of this exact email or this person at this organization (staff directory,
  LinkedIn, news article, hotel page). A matching surname + matching
  organization counts; a bare guess from the localpart does NOT.
- NEVER invent or expand a name from the email pattern alone
  (kmoorhead could be Katie, Kevin, Karl — only the snippets decide).
- title: their job title if the snippets state it, else "".
- confidence: 0.0-1.0. Below 0.7 the name will be discarded, so be honest.
- evidence: the snippet fragment (<= 20 words) that names this person —
  ideally the one showing the email address itself.
"""


def _name_candidates(local: str) -> list[tuple[str, str]]:
    """Decompose a localpart into (first-ish, surname) candidates the way
    Google rewrites these queries: jcadwallader -> (j, cadwallader),
    adsouza -> (a, dsouza), maria.lopez -> (maria, lopez)."""
    import re as _re

    toks = [t for t in _re.split(r"[._+\-]", local.lower()) if t.isalpha()]
    out: list[tuple[str, str]] = []
    if len(toks) == 2 and len(toks[0]) >= 2 and len(toks[1]) >= 2:
        out.append((toks[0], toks[1]))
    if len(toks) == 1 and len(toks[0]) >= 5:
        out.append((toks[0][0], toks[0][1:]))  # leading-initial + surname
    elif len(toks) == 2 and len(toks[0]) == 1:
        out.append((toks[0], toks[1]))
    return out


def _web_snippets(email: str, org: str) -> tuple[list[str], bool, int]:
    """Escalating query ladder — keep trying angles like a human would,
    stop early the moment a snippet shows the exact email address.
    Returns (snippets, exact_email_seen, queries_tried)."""
    from app.services.outreach.researcher import smart_search

    local = email.split("@", 1)[0]
    domain = email.split("@", 1)[1]
    root = domain.split(".")[0]
    orgq = f'"{org}"' if org else root
    queries = [f'"{email}"', email]  # quoted, then unquoted (indexes differ)
    for _ini, surname in _name_candidates(local)[:1]:
        queries.append(f"{surname} {orgq}")
        queries.append(f"{surname} {domain}")
        queries.append(f"zoominfo {surname} {root}")
        queries.append(f"linkedin {surname} {root}")
    queries.append(f'"{local}" "{org}"' if org else f'"{local}" {root}')
    queries.append(f"{local} {root}")

    snippets: list[str] = []
    seen: set[str] = set()
    email_seen = False
    tried = 0
    for q in queries:
        tried += 1
        try:
            for s in smart_search(q) or []:
                if s not in seen:
                    seen.add(s)
                    snippets.append(s)
                    if email.lower() in s.lower():
                        email_seen = True
        except Exception as e:
            print(f"    search failed {q!r}: {e}")
        # exact email in hand = ground truth found, stop spending
        if email_seen and len(snippets) >= 4:
            break
        if len(snippets) >= 18:
            break
    return snippets[:18], email_seen, tried


def _consistent_with_localpart(local: str, fn: str, ln: str) -> bool:
    """Anti-hallucination guard: the resolved name must be derivable from the
    localpart (surname embedded, or first name embedded). Joshua Cadwallader
    fits jcadwallader; a made-up 'John Smith' does not."""
    import re as _re

    def n(x: str) -> str:
        return _re.sub(r"[^a-z]", "", (x or "").lower())

    lcl, fst, lst = n(local), n(fn), n(ln)
    if not lcl or not (fst and lst):
        return False
    if lst in lcl:
        return True
    # first-name rule: only for localparts that ARE the first name
    # (alexane@, berenicegarcia@) — NOT embedded mid-string, where the
    # match is sitting in the surname slot (prichard = p + richard must
    # not accept a 'Richard Anyone').
    if lcl.startswith(fst) and len(fst) >= 3:
        return True
    # first.last localparts where the snippet spelling differs slightly
    cands = _name_candidates(local)
    return any(lst.startswith(sur[:5]) or sur.startswith(lst[:5]) for _i, sur in cands)


async def _grounded_lookup(client, email: str, org: str) -> dict | None:
    """Fallback: one Gemini googleSearch call — the same grounding the
    Google AI Overview uses. Returns parsed JSON dict or None."""
    try:
        from app.services.ai_client import _ensure_init, _get_config
        from app.services.contact_enrichment import _build_grounding_url
        from app.services.gemini_client import get_gemini_headers

        _ensure_init()
        config = _get_config()
        url, _loc = _build_grounding_url(config["vertex_project_id"], config["model"])
        headers = get_gemini_headers()
    except Exception as e:
        print(f"    grounded fallback unavailable: {e}")
        return None

    prompt = PROMPT.format(email=email, org=org) + (
        "\nSearch the web for this email address and for the surname it "
        "likely contains, combined with the organization/domain. Use what "
        "you find; respond with ONLY the JSON."
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
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        parts = (data["candidates"][0].get("content") or {}).get("parts") or []
        raw = "".join(p.get("text") or "" for p in parts).strip()
    except Exception as e:
        print(f"    grounded call failed: {e}")
        return None
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip() if "```" in raw else raw
    try:
        return json.loads(raw)
    except Exception:
        return None


_NONHUMAN_NAME_TOKENS = {
    "team", "sales", "service", "services", "support", "customer",
    "finance", "accounting", "accounts", "payable", "payables", "billing",
    "invoice", "orders", "office", "dept", "department", "group", "admin",
    "reservations", "frontdesk", "concierge", "property", "tower",
    "assistant", "manager", "info", "noreply", "reply", "marketing",
    "hr", "payroll", "purchasing", "procurement", "remit", "details",
    "world", "internet", "login", "center",
}


def _looks_like_person(nm: str) -> bool:
    """Header display names on role boxes are labels ('Finance Team',
    'Assistant Property Tower 2'), not people — never write those."""
    import re as _re

    toks = nm.lower().split()
    if any(_re.search(r"\d", t) for t in toks):
        return False
    return not any(
        t.strip(".,&|-'\"‘’“”") in _NONHUMAN_NAME_TOKENS for t in toks
    )


def _inbox_header_lookup(email: str) -> tuple[str, str] | None:
    """Tier 0: search OUR OWN mailboxes for this address. A display name in
    a From/To/Cc header ('Kayla Mangione <kmangione@...>') is first-party
    ground truth — free, instant, and beats anything the web says.
    Returns (full_name, source_tag) or None."""
    try:
        from app.services.inbox_sync import _gmail, _normalize_header_name
        from app.services.mailbox_discovery import list_active_mailboxes
    except Exception as e:
        print(f"    inbox lookup unavailable: {e}")
        return None
    from collections import Counter
    from email.utils import getaddresses

    target = email.lower()
    votes: Counter = Counter()
    try:
        mailboxes = list_active_mailboxes()
    except Exception as e:
        print(f"    mailbox discovery failed: {e}")
        return None
    for mb in mailboxes:
        try:
            svc = _gmail(mb)
            res = (
                svc.users()
                .messages()
                .list(
                    userId="me",
                    q=f"from:{email} OR to:{email} OR cc:{email}",
                    maxResults=8,
                )
                .execute()
            )
            for m in res.get("messages", []) or []:
                msg = (
                    svc.users()
                    .messages()
                    .get(
                        userId="me",
                        id=m["id"],
                        format="metadata",
                        metadataHeaders=["From", "To", "Cc"],
                    )
                    .execute()
                )
                headers = (msg.get("payload") or {}).get("headers") or []
                for h in headers:
                    for name, addr in getaddresses([h.get("value") or ""]):
                        if addr.lower() != target:
                            continue
                        nm = _normalize_header_name((name or "").strip())
                        if (
                            nm
                            and "@" not in nm
                            and " " in nm
                            and len(nm) >= 5
                            and _looks_like_person(nm)
                        ):
                            votes[nm] += 1
        except Exception as e:
            print(f"    inbox lookup failed on {mb}: {e}")
        if votes:
            break  # first mailbox with evidence is enough
    if not votes:
        return None
    best, n = votes.most_common(1)[0]
    return best, f"inbox-header x{n}"


async def _serper_attempt(client, email: str, org: str) -> tuple[dict, bool, int, bool]:
    """Serper-snippet tier. Returns (data, exact_email_seen, queries_tried, had_snippets)."""
    data: dict = {}
    snippets, email_seen, tried = _web_snippets(email, org)
    if not snippets:
        return data, email_seen, tried, False
    prompt = PROMPT.format(email=email, org=org) + "\nSNIPPETS:\n" + "\n".join(
        f"- {s}" for s in snippets
    )
    try:
        raw = await ai_generate(
            client, prompt, model=MODEL, temperature=0.1, max_tokens=300
        )
    except Exception as e:
        print(f"    synthesis failed: {e}")
        raw = None
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip() if "```" in raw else raw
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    return data, email_seen, tried, True


def _ok(data: dict) -> bool:
    return bool(
        (data.get("first_name") or "").strip()
        and (data.get("last_name") or "").strip()
        and float(data.get("confidence") or 0.0) >= 0.7
    )


async def main(
    limit: int, apply: bool, buyers_only: bool, include_role: bool,
    grounded_first: bool, after_id: int, inbox_only: bool,
) -> None:
    cat_clause = (
        "AND (contact_category IS NULL OR contact_category = 'buyer') "
        if buyers_only
        else "AND contact_category IS DISTINCT FROM 'junk' "
    )
    async with async_session() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT id, email, organization, display_name FROM contacts "
                    "WHERE COALESCE(first_name,'') = '' "
                    "AND COALESCE(last_name,'') = '' "
                    "AND email IS NOT NULL AND email LIKE '%@%' "
                    "AND id > :after "
                    + cat_clause
                    + "ORDER BY id LIMIT :lim"
                ),
                {"lim": limit, "after": after_id},
            )
        ).all()

    print(f"{len(rows)} nameless contact(s) in this batch\n")
    resolved = 0
    skipped_role = 0
    client = httpx.AsyncClient(timeout=90)
    try:
        for r in rows:
            local = r.email.split("@", 1)[0]
            if not include_role and _is_role_mailbox(local):
                skipped_role += 1
                print(f"  #{r.id:<6} {r.email:<48} [role mailbox — skipped]")
                continue
            org = r.organization or ""
            data: dict = {}
            email_seen, tried, snippets = False, 0, False
            via = "serper"
            inbox_src = ""
            hit = _inbox_header_lookup(r.email)
            if hit:
                nm, inbox_src = hit
                parts = nm.split()
                data = {
                    "first_name": parts[0],
                    "last_name": " ".join(parts[1:]),
                    "title": "",
                    "confidence": 1.0,
                    "evidence": inbox_src,
                }
                via = "inbox"
            elif inbox_only:
                print(f"  #{r.id:<6} {r.email:<48} not in archive headers")
                continue
            elif grounded_first:
                g = await _grounded_lookup(client, r.email, org)
                if g and _ok(g):
                    data, via = g, "grounded"
                else:
                    data, email_seen, tried, snippets = await _serper_attempt(
                        client, r.email, org
                    )
                    via = "serper"
            elif not grounded_first:
                data, email_seen, tried, snippets = await _serper_attempt(
                    client, r.email, org
                )
                if not _ok(data):
                    g = await _grounded_lookup(client, r.email, org)
                    if g and _ok(g):
                        data, via = g, "grounded"
            fn = (data.get("first_name") or "").strip()
            ln = (data.get("last_name") or "").strip()
            conf = float(data.get("confidence") or 0.0)
            title = (data.get("title") or "").strip()
            evidence = (data.get("evidence") or "").strip()
            if not (fn and ln) or conf < 0.7:
                if tried and not snippets:
                    print(f"  #{r.id:<6} {r.email:<48} no web results ({tried} queries tried)")
                else:
                    print(f"  #{r.id:<6} {r.email:<48} unresolved (conf {conf:.2f})")
                continue
            # Evidence tiers — strong evidence overrides the derivability rule:
            #   1. exact email seen in OUR snippets (programmatic ground truth)
            #   2. name derivable from the localpart (surname-anchor)
            #   3. grounded model quotes the exact email in its evidence + high conf
            derivable = _consistent_with_localpart(local, fn, ln)
            email_in_evidence = r.email.lower() in evidence.lower()
            # tier 4: role-phrase localparts (directorofsecurity@, finance@)
            # can't derive a NAME — but if the found person's TITLE matches
            # the role the mailbox describes, at this org, that's the person
            # behind the box (Reuben Gilkes, Director of Security, behind
            # directorofsecurity@silversandsgrenada.com).
            _title_words = [
                w for w in __import__("re").findall(r"[a-z]{5,}", title.lower())
            ]
            role_title_hits = [w for w in _title_words if w in local.lower()]
            # initial-consistency check for surname-only matches: roflores
            # matching 'Carlos Flores' (r != c) is probably a DIFFERENT
            # Flores at the same company — review, don't write.
            initial_conflict = False
            if derivable and fn and ln:
                import re as _re2

                lcl_n = _re2.sub(r"[^a-z]", "", local.lower())
                ln_n = _re2.sub(r"[^a-z]", "", ln.lower())
                idx = lcl_n.find(ln_n)
                if idx > 0:
                    prefix = lcl_n[:idx]
                    if fn[0].lower() not in prefix:
                        initial_conflict = True
            if via == "inbox":
                tag = inbox_src
            elif email_seen and via == "serper":
                tag = "email-verified"
            elif derivable and initial_conflict:
                print(
                    f"  #{r.id:<6} {r.email:<48} found '{fn} {ln}' but "
                    f"localpart initial disagrees — review by hand"
                )
                continue
            elif derivable:
                tag = "name-match"
            elif email_in_evidence and conf >= 0.85:
                tag = "evidence-cites-email"
            elif role_title_hits and conf >= 0.85:
                tag = f"role-title-match:{'+'.join(role_title_hits)}"
            else:
                print(
                    f"  #{r.id:<6} {r.email:<48} found '{fn} {ln}' but "
                    f"unverifiable against localpart/email — skipped"
                )
                continue
            resolved += 1
            extra = f" — {title}" if title else ""
            print(f"  #{r.id:<6} {r.email:<48} -> {fn} {ln}{extra}  (conf {conf:.2f}, {via}, {tag})")
            if apply:
                async with async_session() as session:
                    await session.execute(
                        text(
                            "UPDATE contacts SET first_name = :fn, last_name = :ln, "
                            "display_name = CASE WHEN COALESCE(display_name,'') = '' "
                            "OR display_name = email "
                            "OR display_name !~ ' ' THEN :nd ELSE display_name END, "
                            "title = COALESCE(NULLIF(title,''), NULLIF(:title,'')), "
                            "updated_at = NOW() WHERE id = :id"
                        ),
                        {
                            "fn": fn,
                            "ln": ln,
                            "nd": f"{fn} {ln}",
                            "title": title,
                            "id": r.id,
                        },
                    )
                    await session.commit()
    finally:
        await client.aclose()

    attempted = len(rows) - skipped_role
    print(
        f"\n{resolved}/{attempted} resolved ({skipped_role} role mailboxes skipped)."
        + ("" if apply else "  DRY RUN — nothing written; re-run with --apply.")
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--apply", action="store_true", help="write resolved names")
    parser.add_argument("--buyers-only", action="store_true")
    parser.add_argument(
        "--include-role", action="store_true",
        help="also attempt role/shared mailboxes (accountspayable@, ...)",
    )
    parser.add_argument(
        "--grounded-first", action="store_true",
        help="lead with Gemini googleSearch grounding, Serper as fallback",
    )
    parser.add_argument(
        "--after-id", type=int, default=0,
        help="skip ids <= N (use the last id from the previous batch to advance)",
    )
    parser.add_argument(
        "--inbox-only", action="store_true",
        help="tier-0 only: resolve from our own mailbox headers, zero web spend",
    )
    args = parser.parse_args()
    asyncio.run(
        main(
            limit=args.limit,
            apply=args.apply,
            buyers_only=args.buyers_only,
            include_role=args.include_role,
            grounded_first=args.grounded_first,
            after_id=args.after_id,
            inbox_only=args.inbox_only,
        )
    )
