"""Contact Intelligence — understand a contact from sparse, messy data.

Deterministic (no external calls) so it can run over the whole contacts table
cheaply and repeatably. It is the FIRST, high-coverage pass; the ambiguous
"unknown" bucket it produces is exactly the slice worth sending to Gemini later
for a precise verdict, instead of paying for an LLM call on all ~5k rows.

Five jobs:
  1. relevance()            — hotel-relevant / junk / unknown, with reasons.
  2. parse_name_from_email()— derive (first, last) from the local part.
  3. infer_role_from_email()— role hint from a role-style local part (gm@, buyer@).
  4. email_patterns()       — name + domain -> likely corporate email formats.
  5. assess()               — run them all into one verdict dict for a contact.

It reuses existing hospitality knowledge (BrandRegistry, procurement gateways,
management-company intel) so "relevant" means the same thing across the app.
"""

import re
from typing import Optional

from app.config.brand_registry import BrandRegistry
from app.config.procurement_intelligence import (
    match_gateway_contact,
    gateway_for_brand,
    get_management_company_intel,
)

# ── Hospitality signals (org or title) ────────────────────────────────────
HOSPITALITY_KW = (
    "hotel",
    "hotels",
    "resort",
    "resorts",
    "inn",
    "suites",
    "lodge",
    "motel",
    "hospitality",
    "hostel",
    "ryokan",
    "chalet",
    "villa",
    "villas",
    "spa",
    "casino",
    "beach club",
    "country club",
    "golf club",
    "golf resort",
    "residences",
    "residence club",
    "guesthouse",
    "guest house",
    "boutique hotel",
    "bed and breakfast",
    "the collection",
    "hoteles",
    "hôtel",
    "pousada",
    "all-inclusive",
    "all inclusive",
)
# Hospitality operational roles — supporting signal (weaker than org).
HOSPITALITY_ROLE_KW = (
    "housekeeping",
    "front office",
    "front desk",
    "concierge",
    "rooms division",
    "food and beverage",
    "food & beverage",
    "f&b",
    "banquet",
    "innkeeper",
    "guest services",
    "general manager",
    "hotel manager",
    "resort manager",
    "director of operations",
    "executive housekeeper",
)

# ── Junk signals ──────────────────────────────────────────────────────────
# TLDs overwhelmingly used by cold-outreach / spam in this dataset.
JUNK_TLDS = {
    "info",
    "biz",
    "xyz",
    "top",
    "buzz",
    "click",
    "link",
    "online",
    "site",
    "website",
    "store",
    "live",
    "icu",
    "today",
    "fun",
    "monster",
}
# Non-hospitality industry tokens in the org name.
JUNK_ORG_KW = (
    "agency",
    "ventures",
    "venture",
    "capital",
    "consult",
    "consulting",
    "consultancy",
    "advisory",
    "software",
    "saas",
    "technologies",
    "labs",
    "outreach",
    "lead gen",
    "leadgen",
    "growth",
    "seo",
    "ecommerce",
    "e-commerce",
    "fintech",
    "crypto",
    "dealership",
    "motors",
    "automotive",
    "ford",
    "chevrolet",
    "toyota",
    "honda",
    "nissan",
    "insurance",
    "mortgage",
    "recruiting",
    "staffing",
    "training hub",
    "academy",
    "university",
    "college",
    "church",
    "ministries",
    "for congress",
    " pac",
    "digitizing",
    "webinar",
    "marketing",
    "media group",
    "digital agency",
)
# Cold-outreach titles — only count as junk when there's NO hospitality signal
# (a "Founder/Owner" at an independent hotel is the BUYER, not junk).
JUNK_TITLE_KW = (
    "account executive",
    "business development",
    "bdr",
    "sdr",
    "partnerships",
    "growth",
    "ai strategist",
    "sales representative",
    "outreach specialist",
    "demand generation",
    "seo specialist",
    "digital marketing",
)

PERSONAL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "msn.com",
    "live.com",
    "proton.me",
    "protonmail.com",
}

# Local-part -> role hint (when the address itself encodes a function).
ROLE_HINTS = {
    "purchasing": "Purchasing",
    "procurement": "Procurement",
    "buyer": "Purchasing",
    "sourcing": "Procurement",
    "gm": "General Manager",
    "generalmanager": "General Manager",
    "housekeeping": "Housekeeping",
    "exechousekeeper": "Executive Housekeeper",
    "reservations": "Reservations",
    "frontdesk": "Front Office",
    "frontoffice": "Front Office",
    "fom": "Front Office",
    "sales": "Sales",
    "catering": "Catering",
    "events": "Events",
    "banquets": "Banquets",
    "fnb": "Food & Beverage",
    "fb": "Food & Beverage",
    "chef": "Kitchen",
    "kitchen": "Kitchen",
    "hr": "Human Resources",
    "accounting": "Accounting",
    "ap": "Accounts Payable",
    "finance": "Finance",
}

_TLD_RE = re.compile(r"\.([a-z]{2,})$")


def _domain(email: str) -> str:
    return email.split("@")[-1].lower().strip() if "@" in email else ""


def _tld(domain: str) -> str:
    m = _TLD_RE.search(domain)
    return m.group(1) if m else ""


def _has_any(text: str, kws) -> Optional[str]:
    t = (text or "").lower()
    for kw in kws:
        if kw in t:
            return kw
    return None


def relevance(
    organization: Optional[str],
    title: Optional[str],
    email: str,
    known_hotel_domains: Optional[set] = None,
) -> dict:
    """Return {verdict: relevant|junk|unknown, score: 0-100, reasons: [...]}.

    Order: a positive hospitality signal wins; otherwise strong junk signals
    classify as junk; everything else is 'unknown' (the LLM-review bucket)."""
    org = organization or ""
    domain = _domain(email)
    reasons: list[str] = []

    # ── POSITIVE: hospitality signals ──
    known_hotel_domains = known_hotel_domains or set()
    if domain and domain in known_hotel_domains:
        return {
            "verdict": "relevant",
            "score": 95,
            "reasons": ["domain matches a known hotel/lead in your data"],
        }
    if org:
        bi = BrandRegistry.lookup(org)
        if getattr(bi, "parent_company", "Unknown") != "Unknown":
            return {
                "verdict": "relevant",
                "score": 92,
                "reasons": [f"known hotel brand ({bi.parent_company})"],
            }
        if match_gateway_contact(org, None, domain) or gateway_for_brand(org, None):
            return {
                "verdict": "relevant",
                "score": 90,
                "reasons": ["procurement gateway / centralized buyer"],
            }
        if get_management_company_intel(org):
            return {
                "verdict": "relevant",
                "score": 88,
                "reasons": ["known hotel management company"],
            }
    hk = _has_any(org, HOSPITALITY_KW) or _has_any(title or "", HOSPITALITY_KW)
    if hk:
        return {
            "verdict": "relevant",
            "score": 80,
            "reasons": [f"hospitality keyword: '{hk}'"],
        }
    rk = _has_any(title or "", HOSPITALITY_ROLE_KW)
    if rk and domain not in PERSONAL_DOMAINS:
        return {
            "verdict": "relevant",
            "score": 65,
            "reasons": [f"hospitality role: '{rk}'"],
        }

    # ── NEGATIVE: junk signals ──
    tld = _tld(domain)
    if tld in JUNK_TLDS:
        reasons.append(f"spam-prone TLD .{tld}")
    jo = _has_any(org, JUNK_ORG_KW)
    if jo:
        reasons.append(f"non-hospitality industry: '{jo}'")
    jt = _has_any(title or "", JUNK_TITLE_KW)
    if jt:
        reasons.append(f"cold-outreach title: '{jt}'")
    if domain in PERSONAL_DOMAINS and not org:
        reasons.append("personal email, no organization")

    # Two or more junk signals, or a junk TLD with any other → junk.
    if len(reasons) >= 2 or (tld in JUNK_TLDS and reasons):
        return {"verdict": "junk", "score": 8, "reasons": reasons}
    if reasons:
        # single soft signal — not confident enough to auto-junk
        return {"verdict": "unknown", "score": 35, "reasons": reasons}

    return {
        "verdict": "unknown",
        "score": 50,
        "reasons": ["no hospitality signal, no clear junk signal"],
    }


def parse_name_from_email(email: str) -> dict:
    """Best-effort (first, last) from the local part. Confidence reflects how
    cleanly it split — 'first.last' is high; a single concatenated blob is low."""
    local = email.split("@")[0] if "@" in email else email
    local = re.split(r"[+]", local)[0]  # drop +tags
    local = re.sub(r"\d+$", "", local)  # drop trailing digits
    tokens = [t for t in re.split(r"[._\-]", local) if t and not t.isdigit()]
    tokens = [t for t in tokens if len(t) > 1 or len(tokens) <= 2]

    def cap(t):
        return t[:1].upper() + t[1:] if t else t

    if len(tokens) >= 2:
        first, last = tokens[0], tokens[-1]
        # firstname + single-initial like "michaelromano.h" -> keep first only
        if len(last) == 1:
            return {"first": cap(first), "last": "", "confidence": 0.5}
        return {"first": cap(first), "last": cap(last), "confidence": 0.85}
    if len(tokens) == 1:
        return {"first": cap(tokens[0]), "last": "", "confidence": 0.35}
    return {"first": "", "last": "", "confidence": 0.0}


def infer_role_from_email(email: str) -> Optional[str]:
    """Role hint when the local part is a function, e.g. purchasing@ -> Purchasing."""
    local = email.split("@")[0].lower() if "@" in email else email.lower()
    key = re.sub(r"[._\-]", "", local)
    for token, role in ROLE_HINTS.items():
        if key == token or key.startswith(token):
            return role
    return None


def email_patterns(first: str, last: str, domain: str) -> list[str]:
    """Likely corporate email formats for a name + domain (name -> email)."""
    f, ln, d = first.lower().strip(), last.lower().strip(), domain.lower().strip()
    if not d:
        return []
    out = []
    if f and ln:
        out += [
            f"{f}.{ln}@{d}",
            f"{f}{ln}@{d}",
            f"{f[0]}{ln}@{d}",
            f"{f}{ln[0]}@{d}",
            f"{f}_{ln}@{d}",
            f"{f[0]}.{ln}@{d}",
            f"{f}@{d}",
            f"{ln}@{d}",
        ]
    elif f:
        out += [f"{f}@{d}"]
    elif ln:
        out += [f"{ln}@{d}"]
    seen, uniq = set(), []
    for e in out:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq


def assess(
    contact: dict,
    known_hotel_domains: Optional[set] = None,
) -> dict:
    """One-shot intelligence for a contact dict (email/organization/title/
    first_name/last_name)."""
    email = (contact.get("email") or "").lower()
    org = contact.get("organization")
    title = contact.get("title")
    rel = relevance(org, title, email, known_hotel_domains)

    has_name = bool(
        (
            contact.get("first_name")
            or contact.get("last_name")
            or contact.get("display_name")
        )
    )
    parsed = parse_name_from_email(email) if not has_name else None
    role_hint = infer_role_from_email(email) if not (title or "").strip() else None

    return {
        "verdict": rel["verdict"],
        "score": rel["score"],
        "reasons": rel["reasons"],
        "inferred_first": parsed["first"] if parsed else None,
        "inferred_last": parsed["last"] if parsed else None,
        "name_confidence": parsed["confidence"] if parsed else None,
        "role_hint": role_hint,
        "domain": _domain(email),
        "email_type": (
            "personal"
            if _domain(email) in PERSONAL_DOMAINS
            else "corporate"
            if "@" in email
            else "invalid"
        ),
    }
