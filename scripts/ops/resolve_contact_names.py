"""
resolve_contact_names.py
=======================
Resolves real person names for cleared / name-less contacts using GROUNDED
Gemini (the same googleSearch + citation gate as grounded_contact_fill) plus a
Serper LinkedIn lookup. It NEVER guesses: a name is written only when the
grounded answer is cited AND the name verifiably ties to the email.

Targets (resolvable contacts):
  - first_name IS NULL AND display_name IS NULL   (cleared / never had a name)
  - has an organization, and real interaction evidence
  - NOT a role/shared inbox (ar@, accounting@, sales@, info@, ...)
  - buyers first (contact_category = 'buyer'), then the rest

Conservative WRITE gate (all must hold):
  - grounded (>= 1 citation; no citation => answered from memory => skip)
  - a candidate name is extractable AND its SURNAME appears in the email
    local-part  (jbell -> "Bell"); this is what makes extraction self-correct
  - AND (the first initial lines up  OR  a citation domain matches the email
    domain) -- a second independent signal

On write: first_name/last_name/display_name, linkedin_url (if found & valid),
enrichment_source='grounded_name' so it isn't re-resolved.

DRY-RUN by default. Flags:
  --apply            actually write
  --limit N          cap contacts this run (default 50)
  --all              include non-buyers too (default: buyers first only)
  --concurrency N    parallel lookups (default 4)

Run from repo root, venv active, DATABASE_URL set, workers' creds available:
    python resolve_contact_names.py                 # preview 50 buyers
    python resolve_contact_names.py --apply --limit 200
"""

import asyncio
import re
import sys
import time

import httpx
from sqlalchemy import text

from app.database import async_session
from app.services.name_validation import is_role_inbox, _is_clean_personal_local
from app.services.smart_fill import _serper_linkedin_raw, _norm_linkedin

# --- live progress logging (prints flush + mirrors to a timestamped file) -----
_DONE = [0]          # contacts processed so far
_TOTAL = [0]         # total to process this run
_LOGF = [None]       # open log file handle (set in main)


def _log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    if _LOGF[0]:
        _LOGF[0].write(line + "\n")
        _LOGF[0].flush()

# function/system local-parts that aren't a person, but slip past is_role_inbox
# (matched as >=5-char substrings so short surnames like 'dunlap' aren't hit).
_FUNCTION_SUBSTR = (
    "payment", "payments", "invoice", "invoices", "billing", "tracking",
    "wireroom", "marketplace", "salesagent", "pathguide", "noreply", "emailack",
    "remittance", "remit", "lockbox", "disbursement", "statements", "mailroom",
    "webmaster", "postmaster", "notification", "automated", "donotreply",
)

# Role / job-function words: when a local-part token is one of these, the
# address is a ROLE inbox (food.and.beverage.managers@, housekeeping@,
# manager.on.duty@), not a person. Writing a "name" for these produces junk
# like "Beverage Managers".
_ROLE_WORDS = frozenset({
    "manager", "managers", "management", "assistant", "assistants", "supervisor",
    "director", "directors", "coordinator", "housekeeping", "housekeeper",
    "concierge", "reservations", "reservation", "frontdesk", "front", "desk",
    "duty", "above", "food", "beverage", "fnb", "fandb", "culinary", "banquet",
    "catering", "events", "sales", "marketing", "accounting", "accounts",
    "payroll", "purchasing", "procurement", "receiving", "security", "valet",
    "maintenance", "engineering", "operations", "ops", "gm", "agm", "hr",
    "recruiting", "careers", "jobs", "team", "staff", "department", "office",
    "admin", "general", "executive", "guest", "guestservices", "laundry",
})


def _looks_like_person(email: str) -> bool:
    """True only when the email LOCAL-part is shaped like a real person's name:
    a dotted first.last, or a single initial+surname token (jbell, cbenitez).
    Rejects role/function/system addresses so we never spend a lookup on them."""
    if not email or "@" not in email or is_role_inbox(email):
        return False
    domain = email.split("@", 1)[1].lower()
    if any(b in domain for b in _BILLING_DOMAINS):
        return False  # invoice.plateiq.com etc. -> account portal, not a person
    local = email.split("@")[0].lower().split("+")[0]
    if local.startswith("www."):          # malformed scrape prefix -> strip it
        local = local[4:]
    if re.search(r"\d{3,}", local):          # us7-eb704..., long digit runs = system
        return False
    flat = re.sub(r"[^a-z]", "", local)
    if any(fn in flat for fn in _FUNCTION_SUBSTR):
        return False
    if flat.endswith("ap") or flat.endswith("ar"):   # floridaap, regionar = AP/AR inboxes
        return False
    toks0 = [t for t in re.split(r"[._\-]+", local) if t]
    # role inbox: any token is a job-function word (food.and.beverage.managers@,
    # housekeeping@, manager.on.duty@). 4+ tokens is also role-phrase-shaped.
    if any(re.sub(r"[^a-z]", "", t) in _ROLE_WORDS for t in toks0):
        return False
    # also catch role words GLUED into a single token (laundrymanager@,
    # frontdeskmanager@) -- check the longest role words as substrings.
    if any(rw in flat for rw in (
        "manager", "housekeeping", "concierge", "reservations", "frontdesk",
        "banquet", "catering", "purchasing", "procurement", "maintenance",
        "engineering", "operations", "supervisor", "director", "coordinator",
    )):
        return False
    if flat in _ROLE_WORDS or flat in {"party", "info", "hello", "contact", "booking", "bookings"}:
        return False
    if len(toks0) >= 4:                     # first.middle.last is 3 max; 4+ = a phrase
        return False
    if any(re.sub(r"[^a-z]", "", t.lower()) in _COMPANY_WORDS for t in toks0):
        return False  # american.airlines, parking.services -> company, not a person
    if _is_clean_personal_local(local):      # tanja.steinhofer, c.benitez -> best
        return True
    toks = [t for t in re.split(r"[._\-]+", local) if t]
    # initial.surname (c.benitez) or first.last with a short first token
    if len(toks) >= 2 and all(t.isalpha() for t in toks) and any(len(t) >= 3 for t in toks):
        return True
    if len(toks) == 1 and toks[0].isalpha() and 4 <= len(toks[0]) <= 16:
        return bool(re.search(r"[aeiou]", toks[0]))   # jbell, cbenitez (has a vowel)
    return False

APPLY = "--apply" in sys.argv
DEBUG_SKIPS = "--debug-skips" in sys.argv
ALL = "--all" in sys.argv


def _arg(flag, default):
    if flag in sys.argv:
        try:
            return int(sys.argv[sys.argv.index(flag) + 1])
        except Exception:
            return default
    return default


LIMIT = _arg("--limit", 50)
CONC = _arg("--concurrency", 4)


async def _grounded_find_name(email: str, org: str) -> dict:
    """Grounded Gemini lookup. Returns {text, citations, grounded}."""
    from app.services.contact_enrichment import _build_grounding_url, _CONTACT_GROUNDING_TIMEOUT_S
    from app.services.gemini_client import get_gemini_headers
    from app.services.ai_client import _get_config, _ensure_init

    _ensure_init()
    cfg = _get_config()
    url, _ = _build_grounding_url(cfg["vertex_project_id"], cfg["model"])
    headers = get_gemini_headers()
    domain = email.split("@")[-1] if "@" in email else ""
    prompt = (
        f"Identify the specific real person whose work email address is {email}"
        f"{(', who works at ' + org) if org else (', at the organization using the domain ' + domain)}. "
        f"The local-part of the email usually encodes their name (e.g. 'j.smith' = a "
        f"first initial J and surname Smith; 'mary.jones' = Mary Jones). Use web "
        f"search to find the actual person at {org or domain} whose name matches that "
        f"local-part -- NOT a different person who merely shares the same initials or "
        f"surname, and NOT a generic role or department. "
        f"Cross-check that the surname you return is consistent with the email local-part. "
        f"Respond with ONE line exactly: 'FULL NAME: <First Last>'. "
        f"If web results do not let you verifiably identify this exact person, respond "
        f"'FULL NAME: UNKNOWN' -- never guess from memory. "
        f"If you find their LinkedIn profile, add a second line with the full "
        f"https://www.linkedin.com/in/ URL."
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {"temperature": 1.0, "maxOutputTokens": 1024, "thinkingConfig": {"thinkingBudget": 0}},
    }
    try:
        async with httpx.AsyncClient(timeout=_CONTACT_GROUNDING_TIMEOUT_S) as gc:
            resp = await gc.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        cand = data["candidates"][0]
        parts = (cand.get("content") or {}).get("parts") or []
        out = re.sub(r"[*`]+", "", ((parts[0].get("text") if parts else "") or "").strip())
        meta = cand.get("groundingMetadata", {}) or {}
        cites = [c.get("web", {}).get("uri") for c in (meta.get("groundingChunks", []) or [])[:5]
                 if c.get("web", {}).get("uri")]
        return {"text": out, "citations": cites, "grounded": bool(cites)}
    except Exception as e:
        return {"text": f"(error: {e})", "citations": [], "grounded": False}


# words that are NOT part of a name but are capitalized and often follow it,
# so the greedy match doesn't glue them on (e.g. "Jennifer Bell Role ...").
_STOP = {
    "role", "title", "linkedin", "profile", "url", "director", "manager",
    "president", "vp", "ceo", "cfo", "coo", "officer", "head", "lead",
    "the", "company", "full", "name", "unknown", "at", "is", "marketing",
    "sales", "operations", "purchasing", "department", "inc", "llc", "corp",
    "hotel", "hotels", "resort", "resorts", "group", "international",
}


def _trim_name(seq: str) -> str:
    """Keep only the leading run of real name tokens, dropping trailing
    role/company words a greedy match may have glued on."""
    toks = [t for t in re.split(r"\s+", seq.strip()) if t]
    kept = []
    for t in toks:
        if re.sub(r"[^a-z]", "", t.lower()) in _STOP:
            break
        kept.append(t)
    return " ".join(kept)


def _candidate_names(prose: str) -> list[str]:
    """All 'First Last' / 'First Middle Last' capitalized sequences, with
    trailing role/company words trimmed off."""
    out = []
    m = re.search(r"full\s+name:\s*([A-Z][A-Za-z'\-]+(?:\s+[A-Za-z'\-]+){1,3})", prose, re.I)
    if m and m.group(1).strip().upper() != "UNKNOWN":
        out.append(_trim_name(m.group(1)))
    for seq in re.findall(r"\b([A-Z][a-z'\-]+(?:\s+[A-Z][a-z'\-]+){1,3})\b", prose):
        out.append(_trim_name(seq))
    seen, uniq = set(), []
    for n in out:
        n = n.strip()
        if n and len(n.split()) >= 2 and n.lower() not in seen:
            seen.add(n.lower())
            uniq.append(n)
    return uniq


_COMPANY_WORDS = {
    "airlines", "airline", "hotel", "hotels", "resort", "resorts", "group",
    "inc", "llc", "corp", "company", "parking", "services", "service",
    "international", "management", "associates", "partners", "holdings",
    "industries", "systems", "solutions", "global", "american", "national",
    # brand/generic tokens seen masquerading as people in grounded results
    "security", "verified", "cloud", "google", "intuit", "coupa", "graduate",
    "hilton", "marriott", "hyatt", "special", "events", "habeas", "data",
    "eastern", "florida", "team", "support", "billing", "accounts",
}

# billing / invoice-aggregator domains — never a person, always an account portal
_BILLING_DOMAINS = (
    "plateiq", "invoice.", "binvoices", "avidbill", "avidxchange",
    "capturemybills", "corcentric", "bill.com", "coupa", "concur",
)


def _is_real_person_name(first: str, last: str) -> bool:
    """Reject Serper-derived junk that passes the email-tie check but isn't a
    real person: doubled words ('Carnero Carnero'), a label word as the first
    name ('Contact Grabarits'), or place/brand/publication phrases
    ('Red Rock', 'Miami Alum', 'Towne Post', 'Estudios Superiores')."""
    fn, ln = (first or "").strip().lower(), (last or "").strip().lower()
    if not fn or not ln:
        return False
    if fn == ln:  # 'Carnero Carnero'
        return False
    _LABEL_WORDS = {
        "contact", "manager", "team", "staff", "admin", "office", "owner",
        "director", "guest", "front", "info", "sales", "support", "the", "while",
        "department", "general", "executive", "assistant", "reservations",
    }
    if fn in _LABEL_WORDS or ln in _LABEL_WORDS:
        return False
    # place / brand / publication / generic words -> not a person name.
    # If EITHER token is one of these, reject (Red Rock, Miami Alum, Towne Post,
    # Estudios Superiores, Billygan Club, Miambiance Magazine, Port Orange).
    _NON_PERSON_WORDS = {
        "red", "rock", "miami", "alum", "towne", "post", "port", "orange",
        "club", "magazine", "estudios", "superiores", "account", "phishing",
        "protect", "phish", "spam", "hotel", "resort", "casino", "city",
        "beach", "group", "company", "inc", "llc", "corp", "services", "center",
        "north", "south", "east", "west", "san", "los", "las", "new", "view",
        "my", "your", "university", "college", "school", "association", "global",
        "national", "american", "international", "world", "worldwide", "plaza",
        "tower", "park", "grand", "royal", "bay", "lake", "river", "valley",
        "mountain", "house", "inn", "suites", "lodge", "spa", "restaurant",
        "cafe", "bar", "grill", "kitchen", "bistro", "lounge", "downtown",
    }
    if fn in _NON_PERSON_WORDS or ln in _NON_PERSON_WORDS:
        return False
    # a real name's tokens are mostly alphabetic and reasonable length
    if not (fn.isalpha() and ln.isalpha()):
        return False
    if len(fn) < 2 or len(ln) < 2:
        return False
    return True


def _surname_only_candidate(email: str, names: list[str]) -> str | None:
    """Find a candidate whose SURNAME ties to the email + org but whose first
    initial does NOT match the local-part (r.brady@ -> 'Lauren Brady': Brady
    matches, but L != R). These are namesake-risky -> queue for human review,
    never auto-write. Returns the candidate 'First Last' or None."""
    raw_local = email.split("@")[0].lower().strip(".")
    if raw_local.startswith("www."):
        raw_local = raw_local[4:]
    local_alpha = re.sub(r"[^a-z]", "", raw_local)
    sep_toks = [t for t in re.split(r"[._\-]+", raw_local) if t]
    for n in names:
        toks = [t for t in re.split(r"[\s'\-]+", n) if t]
        if len(toks) < 2:
            continue
        first, last = toks[0], toks[-1]
        if any(re.sub(r"[^a-z]", "", t.lower()) in _COMPANY_WORDS for t in (first, last)):
            continue
        surname = re.sub(r"[^a-z]", "", last.lower())
        firstflat = re.sub(r"[^a-z]", "", first.lower())
        if len(surname) < 3 or not firstflat:
            continue
        # surname must tie to the local somewhere
        surname_ties = (surname in local_alpha
                        or any(surname in t or t in surname for t in sep_toks))
        if not surname_ties:
            continue
        # first initial must NOT match (that's what makes it a review case, not a write)
        first_initial_matches = (
            local_alpha[:1] == firstflat[:1]
            or any(t == firstflat or (len(t) == 1 and t == firstflat[:1]) for t in sep_toks)
        )
        if not first_initial_matches and _is_real_person_name(first, last):
            return f"{first} {last}"
    return None


def _strict_email_match(email: str, first: str, last: str) -> bool:
    """Stricter than _resolve_against_email for the Serper write path: the LAST
    token (surname) must tie to the email local-part, AND the first initial must
    match. Blocks 'Brady Willardson' for r.brady@ (Brady is first, Willardson
    doesn't tie) and any first-name-position surname coincidences."""
    raw_local = email.split("@")[0].lower().strip(".")
    if raw_local.startswith("www."):
        raw_local = raw_local[4:]
    local_alpha = re.sub(r"[^a-z]", "", raw_local)
    sep_toks = [t for t in re.split(r"[._\-]+", raw_local) if t]
    surname = re.sub(r"[^a-z]", "", (last or "").lower())
    firstflat = re.sub(r"[^a-z]", "", (first or "").lower())
    if len(surname) < 3 or not firstflat:
        return False

    def _tie(tok: str) -> bool:
        if not tok:
            return False
        if surname in tok or tok in surname:
            return True
        n = min(len(tok), len(surname))
        return n >= 4 and tok[:n] == surname[:n]

    if len(sep_toks) >= 2:
        # dotted: a token equals the first name or its initial, ANOTHER ties surname
        fn_ok = any(t == firstflat or (len(t) == 1 and t == firstflat[:1]) for t in sep_toks)
        sn_ok = any(_tie(t) for t in sep_toks if not (len(t) == 1))
        return fn_ok and sn_ok
    # glued: exactly initial+surname or firstname+surname (allow truncated tail)
    if local_alpha in (firstflat[:1] + surname, firstflat + surname):
        return True
    return local_alpha[:1] == firstflat[:1] and _tie(local_alpha[1:])


def _resolve_against_email(email: str, names: list[str]) -> tuple[str, str] | None:
    """Pick the candidate whose name STRUCTURALLY matches the email local-part.
    The first initial (or full first name) must line up with the local -- a
    surname match alone is NOT enough (that wrote 'Ernie Camilo' onto l.camilo@).
    Accepts two local shapes:
      - glued initial+surname (jbell, mbabb): local_alpha == firstinitial+surname
        or == firstname+surname (tonyarner)
      - dotted first.last (k.palmer, tanja.steinhofer): a separator token equals
        the first name or its initial, and another contains the surname
    Returns First + Last only."""
    raw_local = email.split("@")[0].lower().strip(".")
    local_alpha = re.sub(r"[^a-z]", "", raw_local)
    sep_toks = [t for t in re.split(r"[._\-]+", raw_local) if t]
    for n in names:
        toks = [t for t in re.split(r"[\s'\-]+", n) if t]
        if len(toks) < 2:
            continue
        first, last = toks[0], toks[-1]
        # reject company/brand names masquerading as a person
        if any(re.sub(r"[^a-z]", "", t.lower()) in _COMPANY_WORDS for t in (first, last)):
            continue
        surname = re.sub(r"[^a-z]", "", last.lower())
        firstflat = re.sub(r"[^a-z]", "", first.lower())

        def _surname_matches(token: str) -> bool:
            """Surname ties to a token if it's contained, OR the token is a
            truncation of the surname (l.jaime <-> Jaimes), OR vice versa.
            Requires >=4 shared leading chars so it stays namesake-safe."""
            if not token or len(surname) < 3:
                return False
            if surname in token or token in surname:
                return True
            # prefix overlap: 'jaime' vs 'jaimes', 'rodrigez' vs 'rodriguez'
            n = min(len(token), len(surname))
            if n >= 4 and token[:n] == surname[:n]:
                return True
            return False

        # top-level guard: surname must tie to the local somewhere. For dotted
        # locals check each token (so 'jaime' ~ 'Jaimes'); for glued, the whole.
        if len(surname) < 3 or not firstflat:
            continue
        if len(sep_toks) >= 2:
            if not any(_surname_matches(t) for t in sep_toks):
                continue
        else:
            if not _surname_matches(local_alpha):
                continue
        if len(sep_toks) >= 2:
            # dotted: first token must BE the first name or its initial
            fn_ok = any(t == firstflat or (len(t) == 1 and t == firstflat[:1])
                        for t in sep_toks)
            sn_ok = any(_surname_matches(t) for t in sep_toks)
            ok = fn_ok and sn_ok
        else:
            # glued single token: initial+surname or firstname+surname,
            # allowing a truncated surname tail (ljaime <-> l+jaimes)
            ok = (local_alpha in (firstflat[:1] + surname, firstflat + surname)
                  or _surname_matches(local_alpha[1:]) and local_alpha[:1] == firstflat[:1])
        if ok:
            return (first, last)
    return None


def _linkedin_from(prose: str, serp: list[str]) -> str | None:
    m = re.search(r"https?://[\w.]*linkedin\.com/in/[\w\-%/]+", prose)
    if m:
        return _norm_linkedin(m.group(0))
    for line in serp:
        if "linkedin.com/in/" in line:
            mm = re.search(r"https?://[\w.]*linkedin\.com/in/[\w\-%/]+", line)
            if mm:
                return _norm_linkedin(mm.group(0))
    return None


async def _process(row, sem) -> dict:
    email, org = row["email"], row["organization"] or ""
    async with sem:
        g = await _grounded_find_name(email, org)
        serp = _serper_linkedin_raw(f'"{org}" {email.split("@")[0]} linkedin') if org else []
    result = {"email": email, "org": org, "decision": "skip", "name": None, "linkedin": None,
              "grounded": g["grounded"], "id": row["id"]}
    if not g["grounded"]:
        # [serper_name_fallback] Grounding fell back to memory (no citation).
        # Before giving up, check Serper's web results (already fetched): if they
        # contain a name that TIES to the email (first-initial + surname match)
        # and passes the real-name sanity filter, that organic result IS a real
        # web source -- so we write it as grounded. Same bar, second source.
        _serp_name = _resolve_against_email(email, _candidate_names(" ".join(serp)))
        if (_serp_name and _is_real_person_name(_serp_name[0], _serp_name[1])
                and _strict_email_match(email, _serp_name[0], _serp_name[1])):
            result.update({"decision": "write", "name": _serp_name,
                           "linkedin": _linkedin_from("", serp),
                           "source": "serper_name",
                           "reason": "serper web result + surname matches local-part"})
        else:
            result["reason"] = "no citations (memory answer)"
            # [name_review_queue] no confident write, but is there a surname+org
            # match whose first-initial doesn't fit (r.brady -> Lauren Brady)?
            # Queue it for human review instead of dropping -- catches nicknames
            # / middle-name / surname-first schemes without auto-writing namesakes.
            _allnames = _candidate_names(" ".join(serp)) + _candidate_names(g["text"])
            _review = _surname_only_candidate(email, _allnames)
            if _review:
                result["review_name"] = _review
                result["reason"] = "surname+org match, first-initial mismatch (review)"
            if DEBUG_SKIPS:
                _guess = _resolve_against_email(email, _candidate_names(g["text"]))
                if _guess:
                    result["debug_guess"] = f"{_guess[0]} {_guess[1]}"
                else:
                    _raw = _candidate_names(g["text"])
                    result["debug_guess"] = (f"{_raw[0]} (no email-tie)" if _raw else "(model returned nothing)")
                if _serp_name:
                    result["debug_serper"] = f"{_serp_name[0]} {_serp_name[1]} (failed sanity)"
    else:
        name = _resolve_against_email(email, _candidate_names(g["text"]))
        if not name:
            result["reason"] = "no name verifiably tied to the address"
        else:
            result.update({"decision": "write", "name": name,
                           "linkedin": _linkedin_from(g["text"], serp),
                           "reason": "grounded + surname matches local-part"})
    # live progress (prints as each finishes, not all-at-once at the end)
    _DONE[0] += 1
    tag = "WRITE" if result["decision"] == "write" else "skip "
    nm = f"{result['name'][0]} {result['name'][1]}" if result["name"] else "-"
    _dbg = f"   [memory: {result['debug_guess']}]" if result.get("debug_guess") else ""
    _dbg += f"  [serper: {result['debug_serper']}]" if result.get("debug_serper") else ""
    _log(f"  [{_DONE[0]:>4}/{_TOTAL[0]}] {tag} {email:<36} -> {nm:<22} {result.get('reason','')}{_dbg}")
    return result


async def main() -> None:
    where_buyer = "" if ALL else "AND contact_category = 'buyer'"
    sql = text(
        "SELECT id, email, organization FROM contacts "
        "WHERE first_name IS NULL AND display_name IS NULL "
        "AND organization IS NOT NULL AND email LIKE '%@%' "
        "AND COALESCE(interaction_count,0) >= 1 "
        f"{where_buyer} "
        "ORDER BY interaction_count DESC NULLS LAST LIMIT 4000"
    )
    async with async_session() as s:
        pool = (await s.execute(sql)).mappings().all()
        # keep ONLY person-shaped local-parts (jbell, tanja.steinhofer); drop
        # role/function/system addresses before spending any lookup on them.
        people = [r for r in pool if _looks_like_person(r["email"])]
        # dotted first.last is highest-confidence -> resolve those first
        people.sort(key=lambda r: "." not in r["email"].split("@")[0])
        rows = people[:LIMIT]

        print("=" * 72)
        print(f" NAME RESOLVER  ({'APPLY' if APPLY else 'DRY-RUN'})  "
              f"{'all' if ALL else 'buyers-first'}  limit={LIMIT} conc={CONC}")
        print(f" person-shaped candidates: {len(people)} (showing {len(rows)})  "
              f"[scanned {len(pool)} name-less]")
        print("=" * 72)

        # open a timestamped log file so the run is watchable + auditable
        logname = f"resolve_names_{time.strftime('%Y%m%d_%H%M%S')}.log"
        _LOGF[0] = open(logname, "w", encoding="utf-8")
        _TOTAL[0] = len(rows)
        _DONE[0] = 0
        _log(f" start: {len(rows)} contacts, conc={CONC}, mode={'APPLY' if APPLY else 'DRY-RUN'}")

        sem = asyncio.Semaphore(CONC)
        # progress prints live from inside _process as each finishes
        results = await asyncio.gather(*[_process(r, sem) for r in rows])

        writes = [r for r in results if r["decision"] == "write"]
        reviews = [r for r in results if r.get("review_name")]
        _log(f" would write {len(writes)} / {len(results)} "
             f"(skipped {len(results)-len(writes)}; {len(reviews)} for name-review)")

        if not APPLY:
            _log(" DRY-RUN — nothing written.")
            if reviews:
                _log(" name-review candidates (surname+org match, initial mismatch):")
                for r in reviews:
                    _log(f"    #{r['id']}  {r['email']}  ?-> {r['review_name']}")
            _LOGF[0].close()
            print(f"\n  log saved: {logname}")
            return

        n = 0
        for r in writes:
            fn, ln = r["name"]
            sets = {"fn": fn, "ln": ln, "dn": f"{fn} {ln}".strip(),
                    "src": r.get("source") or "grounded_name", "id": r["id"]}
            q = ("UPDATE contacts SET first_name=:fn, last_name=:ln, display_name=:dn, "
                 "enrichment_source=:src")
            if r["linkedin"]:
                q += ", linkedin_url=:li"
                sets["li"] = r["linkedin"]
            q += " WHERE id=:id"
            await s.execute(text(q), sets)
            n += 1
            if n % 50 == 0:
                await s.commit()
                _log(f" committed {n}/{len(writes)} writes")
        # park near-miss candidates in the name-review queue (idempotent per contact)
        nq = 0
        for r in reviews:
            try:
                await s.execute(text(
                    "INSERT INTO pending_names (contact_id, email, org, candidate_name, "
                    "source, reason, status, created_at) VALUES (:cid, :em, :org, :cn, "
                    "'resolver', 'surname+org match, first-initial mismatch', 'pending', now()) "
                    "ON CONFLICT (contact_id) WHERE status='pending' "
                    "DO UPDATE SET candidate_name=EXCLUDED.candidate_name, created_at=now()"),
                    {"cid": r["id"], "em": r["email"], "org": r["org"], "cn": r["review_name"]})
                nq += 1
            except Exception as e:
                _log(f"  pending_names insert failed for {r['id']}: {e}")
        await s.commit()
        _log(f" DONE — wrote {n} resolved names; queued {nq} for name-review.")
        _LOGF[0].close()
        print(f"\n  log saved: {logname}")


if __name__ == "__main__":
    asyncio.run(main())
