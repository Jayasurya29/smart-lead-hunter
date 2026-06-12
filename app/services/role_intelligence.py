"""role_intelligence.py -- the role dictionary brain (2026-06-12).

A stored, cumulative map from role text -> (vertical, priority, relevance). Three
layers, cheapest first:

  1. LOOKUP   normalize the role, check contact_roles. A hit is instant and
              authoritative -- especially a human-reviewed row, which always wins.
  2. RULES    classify_role_rule(): the in-code keyword logic, EXTENDED beyond
              hospitality to parking / healthcare / education / grocery / corporate
              procurement & ops vocabulary. Reuses inbox_sync._classify_priority
              for the hospitality ladder (no drift) and adds the other verticals.
  3. LLM      label_roles_llm(): for titles the rules can't place, one batched
              Gemini call via the same ai_generate path tier1 uses. Used by the
              miner to fill the dictionary, not on the hot path.

The dictionary is the source of truth once populated; rules/LLM only seed it and
fill genuinely new titles. A reviewed=true row is never auto-overwritten.
"""

import json
import logging
import re
from typing import Optional

logger = logging.getLogger("role_intel")

VALID_PRIORITY = {"P1", "P2", "P3", "P4", "P_unknown"}
VALID_VERTICAL = {
    "hospitality",
    "parking_valet",
    "education",
    "healthcare",
    "grocery",
    "corporate",
    "other",
    "unknown",
}

#  normalization

_PUNCT = re.compile(r"[^a-z0-9& ]+")
_WS = re.compile(r"\s+")
# Strip leading "Org - " / "Name |" decoration the scraper sometimes prepends.
_DECOR = re.compile(r"^[^a-z0-9]*", re.IGNORECASE)


def normalize_role(raw: Optional[str]) -> str:
    """Lowercase, drop punctuation, collapse whitespace -> the lookup key.
    'Director of Purchasing ' / 'DIRECTOR OF PURCHASING' -> 'director of purchasing'."""
    if not raw:
        return ""
    s = raw.strip().lower()
    s = s.replace("&", " and ")
    s = _PUNCT.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


#  cross-vertical keyword layer (what the hospitality lists lacked)
# These mirror the spirit of the in-code hospitality lists, extended to JA's
# other buying verticals. Priority semantics are identical to the hotel ladder:
#   P1 = direct procurement / uniform-owning ops, P2 = operational decision-maker,
#   P3 = secondary internal, P4 = known non-buyer department.

# Universal procurement/supply terms -- P1 in ANY vertical.
_PROC_P1 = (
    "procurement",
    "purchasing",
    "buyer",
    "sourcing",
    "supply chain",
    "materials manager",
    "materials management",
    "inventory",
    "storeroom",
    "stores manager",
    "category manager",
    "strategic sourcing",
    "warehouse manager",
    "supply manager",
    "commodity manager",
    "vendor manager",
    "supplier",
)
# Parking / valet operators (Towne Park, SP+, LAZ, Metropolis, Reef, MVP...).
_PARKING_P2 = (
    "general manager",
    "regional manager",
    "district manager",
    "operations manager",
    "director of operations",
    "regional vice president",
    "region vice president",
    "area manager",
    "vice president operations",
    "vp operations",
    "account manager",
    "facility manager",
    "valet manager",
)
# Healthcare operational/buying roles (EVS, materials mgmt, linen, dietary...).
_HEALTH_P1 = (
    "materials management",
    "central sterile",
    "supply chain",
    "value analysis",
    "linen services",
    "laundry services",
    "purchasing",
)
_HEALTH_P2 = (
    "environmental services",
    "evs director",
    "evs manager",
    "director of nursing",
    "chief nursing",
    "support services",
    "facilities director",
    "director of food",
    "nutrition services",
    "dietary services",
    "patient services",
)
# Education (campus services, dining, residence life, athletics, auxiliary).
_EDU_P2 = (
    "auxiliary services",
    "campus services",
    "dining services",
    "residence life",
    "residential life",
    "facilities director",
    "director of operations",
    "athletics director",
    "director of athletics",
    "housekeeping",
)
_EDU_P3 = ("dean", "provost", "registrar", "bursar")
# Grocery / supermarket (Sedano's, etc.).
_GROCERY_P2 = (
    "store manager",
    "store director",
    "operations manager",
    "district manager",
    "regional manager",
    "perishables",
    "grocery manager",
    "front end manager",
)
# Generic corporate C-suite / owner -- relevant as authority, but vertical
# 'corporate' until org context narrows it.
_CORP_P2 = (
    "owner",
    "president",
    "ceo",
    "chief executive",
    "founder",
    "principal",
    "managing partner",
    "managing director",
    "chief operating officer",
    "coo",
    "vice president",
    "general manager",
    "proprietor",
)

# Universal operations / management roles -- operational decision-makers in ANY
# vertical (hotel, parking, hospital, campus, store). Checked after the
# vertical-specific tables but before giving up, so a bare "Director of
# Operations" with no org hint still lands P2 instead of P_unknown.
_UNIVERSAL_P2 = (
    "director of operations",
    "operations manager",
    "operations director",
    "general manager",
    "regional manager",
    "district manager",
    "area manager",
    "regional director",
    "regional vice president",
    "region vice president",
    "operations supervisor",
    "facility manager",
    "facilities manager",
    "site manager",
    "branch manager",
    "managing director",
    "operations lead",
)

# Roles that are NEVER uniform buyers regardless of vertical -> not relevant.
_IRRELEVANT = (
    "account executive",
    "sales representative",
    "sales rep",
    "business development",
    "solutions engineer",
    "software",
    "developer",
    "marketing manager",
    "social media",
    "seo",
    "recruiter",
    "realtor",
    "insurance agent",
    "financial advisor",
    "consultant",
    "registered agent",
    "attorney",
    "counsel",
)

VERT_HINTS = {
    "parking_valet": ("parking", "valet", "towne park", "sp plus", "laz", "metropolis", "reef"),
    "healthcare": ("hospital", "medical", "health", "clinic", "nursing", "wellpath"),
    "education": ("university", "college", "campus", "school", "chartwells"),
    "grocery": ("grocery", "supermarket", "sedano", "market"),
}


def _vertical_from_org(org: str) -> str:
    o = (org or "").lower()
    for vert, hints in VERT_HINTS.items():
        if any(h in o for h in hints):
            return vert
    if any(h in o for h in ("hotel", "resort", "inn", "suites", "lodge", "hospitality", "casino")):
        return "hospitality"
    return "unknown"


def classify_role_rule(role: str, org: str = "") -> dict:
    """Best-effort (vertical, priority, is_relevant, seniority) from rules alone.
    Returns priority='P_unknown' when nothing matches -- the signal to send the
    role to the LLM labeler. Never raises."""
    norm = normalize_role(role)
    if not norm:
        return {
            "vertical": "unknown",
            "priority": "P_unknown",
            "is_relevant": None,
            "seniority": "unknown",
            "confidence": 0.0,
        }

    vert = _vertical_from_org(org)

    # explicit non-buyers first
    if any(k in norm for k in _IRRELEVANT) and not any(k in norm for k in _PROC_P1):
        return {
            "vertical": vert if vert != "unknown" else "corporate",
            "priority": "P4",
            "is_relevant": False,
            "seniority": _seniority(norm),
            "confidence": 0.7,
        }

    # universal procurement -> P1 in any vertical
    if any(k in norm for k in _PROC_P1):
        return {
            "vertical": vert if vert != "unknown" else "corporate",
            "priority": "P1",
            "is_relevant": True,
            "seniority": _seniority(norm),
            "confidence": 0.9,
        }

    # hospitality: defer to the canonical in-code classifier (no drift)
    if vert in ("hospitality", "unknown"):
        try:
            from app.services.inbox_sync import _classify_priority

            p, _ = _classify_priority(role, None)
        except Exception:
            p = "P_unknown"
        if p != "P_unknown":
            return {
                "vertical": "hospitality",
                "priority": p,
                "is_relevant": p in ("P1", "P2", "P3"),
                "seniority": _seniority(norm),
                "confidence": 0.85,
            }

    # other verticals
    tables = {
        "parking_valet": [(_PARKING_P2, "P2")],
        "healthcare": [(_HEALTH_P1, "P1"), (_HEALTH_P2, "P2")],
        "education": [(_EDU_P2, "P2"), (_EDU_P3, "P3")],
        "grocery": [(_GROCERY_P2, "P2")],
    }
    for kws, p in tables.get(vert, []):
        if any(k in norm for k in kws):
            return {
                "vertical": vert,
                "priority": p,
                "is_relevant": p != "P4",
                "seniority": _seniority(norm),
                "confidence": 0.8,
            }

    # universal operations / management layer -- fires in ANY vertical, even
    # with no org hint. An operations decision-maker is a P2 buyer-influencer
    # whether the property is a hotel, a parking deck, a hospital, or a campus.
    # This catches the long tail the hospitality-only fallback missed:
    # "DIRECTOR OF OPERATIONS", "OPERATIONS MANAGER", "REGIONAL MANAGER" etc.
    if any(k in norm for k in _UNIVERSAL_P2):
        return {
            "vertical": vert if vert != "unknown" else "corporate",
            "priority": "P2",
            "is_relevant": True,
            "seniority": _seniority(norm),
            "confidence": 0.7,
        }

    # generic corporate authority (owner/president/VP) at an unknown vertical
    if any(k in norm for k in _CORP_P2):
        return {
            "vertical": vert if vert != "unknown" else "corporate",
            "priority": "P2",
            "is_relevant": True,
            "seniority": _seniority(norm),
            "confidence": 0.55,
        }

    return {
        "vertical": vert,
        "priority": "P_unknown",
        "is_relevant": None,
        "seniority": _seniority(norm),
        "confidence": 0.0,
    }


def _seniority(norm: str) -> str:
    if any(
        k in norm
        for k in (
            "ceo",
            "cfo",
            "coo",
            "chief",
            "president",
            "owner",
            "founder",
            "vp ",
            "vice president",
        )
    ):
        return "c_suite"
    if "director" in norm or "head of" in norm:
        return "director"
    if "manager" in norm or "supervisor" in norm or "lead" in norm:
        return "manager"
    if norm:
        return "staff"
    return "unknown"


#  table lookup


async def lookup_role(session, role: str) -> Optional[dict]:
    """Return the stored dictionary row for this role, or None. Reviewed rows
    are authoritative; unreviewed rows are still a valid cache hit."""
    from sqlalchemy import text

    norm = normalize_role(role)
    if not norm:
        return None
    row = (
        await session.execute(
            text(
                "SELECT role_normalized, vertical, priority, is_relevant, seniority, "
                "source, reviewed, confidence FROM contact_roles "
                "WHERE role_normalized = :n"
            ),
            {"n": norm},
        )
    ).first()
    if not row:
        return None
    return {
        "vertical": row.vertical,
        "priority": row.priority,
        "is_relevant": row.is_relevant,
        "seniority": row.seniority,
        "source": row.source,
        "reviewed": row.reviewed,
        "confidence": row.confidence,
    }


#  LLM labeler (used by the miner for rule-unknown roles)

_LLM_HEADER = (
    "You label job titles for a UNIFORM & APPAREL supplier's CRM. The supplier "
    "sells staff uniforms to hotels, parking/valet operators, hospitals, "
    "universities, and grocery chains. For each role decide:\n"
    "- vertical: one of hospitality, parking_valet, education, healthcare, "
    "grocery, corporate, other, unknown (the buying context the title implies; "
    "use the org hint).\n"
    "- priority: P1 = procurement/purchasing or the manager who directly owns & "
    "reorders uniforms (housekeeping, EVS, materials, linen, stores); "
    "P2 = operational decision-maker (GM, director of ops, regional/district "
    "manager, owner/president of a small operator); P3 = secondary internal "
    "contact (HR, chef, sales/events director, front office); P4 = known "
    "non-buyer dept (legal, IT, marketing, outside sales/vendor reps).\n"
    "- is_relevant: true if this person could plausibly buy or influence buying "
    "uniforms; false for outside salespeople, software/marketing/recruiting, "
    "and other non-buyers.\n"
    "- seniority: c_suite|director|manager|staff|unknown.\n"
    "Return ONLY a JSON array, one object per input, same order, keys exactly: "
    "role, vertical, priority, is_relevant, seniority. No prose.\n\nINPUT:\n"
)


async def label_roles_llm(client, items: list[dict], model: str = "gemini-2.5-flash-lite") -> dict:
    """items: [{role, org_hint}]. Returns {normalized_role: {vertical, priority,
    is_relevant, seniority}}. Best-effort; returns {} on parse failure."""
    from app.services.ai_client import ai_generate

    if not items:
        return {}
    payload = [{"role": it["role"], "org_hint": it.get("org_hint", "")} for it in items]
    prompt = _LLM_HEADER + json.dumps(payload, ensure_ascii=False)
    try:
        raw = await ai_generate(client, prompt, model=model, temperature=0.1)
    except Exception as e:
        logger.warning(f"role_intel: LLM error: {e}")
        return {}
    if not raw:
        return {}
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip() if "```" in raw else raw
    try:
        arr = json.loads(raw)
    except Exception:
        logger.warning("role_intel: could not parse LLM JSON")
        return {}
    out = {}
    for obj in arr if isinstance(arr, list) else []:
        r = normalize_role(obj.get("role", ""))
        if not r:
            continue
        pri = obj.get("priority", "P_unknown")
        vert = obj.get("vertical", "unknown")
        out[r] = {
            "vertical": vert if vert in VALID_VERTICAL else "unknown",
            "priority": pri if pri in VALID_PRIORITY else "P_unknown",
            "is_relevant": bool(obj.get("is_relevant"))
            if obj.get("is_relevant") is not None
            else None,
            "seniority": obj.get("seniority") or "unknown",
        }
    return out
