"""org_classifier.py — decide what KIND of organization a contact moved to.

Used by the move-apply path and the moved-contact backfill to route a mover
correctly:

  property         → a single hotel/resort at one address. Link to an existing
                     hotel/lead, or create a potential_lead stub.
  operator         → a third-party management company / hotel group that runs
                     MANY properties (Crescent Hotels & Resorts, Aimbridge,
                     Langham Hospitality Group...). NEVER a hotel lead — the
                     person stays a contact with a management_company affiliation.
  out_of_industry  → not hospitality at all (Colliers, an email-outreach vendor).
                     Skip; just retire the old hotel to 'former'.
  unknown          → couldn't tell. Caller should skip-and-flag for manual review
                     rather than guess.

Decision order (cheapest first):
  1. known operator list  (procurement_intelligence.MANAGEMENT_COMPANY_INTEL)
  2. operator-suffix heuristic on the normalized name (".. management",
     ".. hospitality group", ".. hotels & resorts" with no city) — catches the
     Crescent trap: a name containing "Hotels"/"Resorts" can still be an operator
  3. out-of-industry keyword set
  4. grounded tiebreaker: ONE Serper query + ai_generate, constrained to a single
     token (PROPERTY / OPERATOR / OTHER / UNKNOWN)

The grounded step only fires for genuinely unknown orgs, so cost stays low.
"""

from __future__ import annotations

import logging
import re

import httpx

from app.config.procurement_intelligence import get_management_company_intel

logger = logging.getLogger(__name__)

# Out-of-hospitality markers (seeded from the _NON_HOSP / _NON_HOSPITALITY sets
# that currently live as function locals in contacts.py and contact_tier2).
OUT_OF_INDUSTRY = (
    "colliers",
    "cbre",
    "jll",
    "cushman",
    "newmark",
    "marcus & millichap",
    "real estate",
    "realty",
    "brokerage",
    "law firm",
    "attorney",
    "consulting",
    "consultancy",
    "outreach",
    "marketing agency",
    "advertising",
    "recruiting",
    "staffing",
    "insurance",
    "bank",
    "financial",
    "software",
    "saas",
    "university",
    "school",
    "hospital",
    "clinic",
    "church",
    "nonprofit",
)

# Tokens that, when a name ENDS in / contains them as a company descriptor,
# strongly imply a multi-property operator rather than a single hotel.
_OPERATOR_SUFFIXES = (
    "management",
    "hospitality group",
    "hotel group",
    "hotels & resorts",
    "hotels and resorts",
    "hospitality llc",
    "hospitality inc",
    "lodging",
    "operating company",
    "hotel management",
)

# Single-property signals — if any of these appear the name is probably ONE hotel,
# which should override a soft operator-suffix guess (e.g. "Crescent Hotel,
# Beverly Hills" has a city; "Crescent Hotels & Resorts" does not).
_PROPERTY_HINTS = (
    " at ",
    " in ",
    " beach",
    " downtown",
    " resort &",
    " spa",
    " inn",
    " by ",
    " - ",
)


# Branded hotel/resort CHAINS you sell TO — these are properties/brands, not
# third-party operators, even though they span many locations. Resolved offline
# so they never hit (and get mis-read by) the grounded "chain => operator" trap.
KNOWN_PROPERTY_BRANDS = (
    "great wolf lodge",
    "ritz-carlton",
    "ritz carlton",
    "st regis",
    "st. regis",
    "four seasons",
    "waldorf astoria",
    "conrad",
    "edition",
    "fairmont",
    "montage",
    "rosewood",
    "auberge",
    "1 hotel",
    "1hotel",
    "nobu hotel",
    "kimpton",
    "thompson hotel",
    "w hotel",
    "westin",
    "sheraton",
    "marriott",
    "hilton",
    "hyatt",
    "loews hotel",
    "omni hotel",
    "sonesta",
    "drury",
)


# Owner / developer / investment entities — they may OWN hotels but are not a
# sellable property. Treat as operator (contact, no lead), never a hotel stub.
_OWNER_SUFFIXES = (
    "group",
    "development",
    "developments",
    "holdings",
    "holding",
    "partners",
    "capital",
    "investments",
    "investment",
    "ventures",
    "properties",
    "realty",
    "real estate",
    "trust",
    "reit",
    "enterprises",
)


def _norm(s: str) -> str:
    return " ".join((s or "").lower().split())


def _ends_with_owner_suffix(n: str) -> bool:
    """True if the LAST word is an owner/developer marker (e.g. 'The Hartling
    Group'), not just contains it ('Group Nine Hotel' wouldn't trip)."""
    last = n.split()[-1] if n.split() else ""
    two = " ".join(n.split()[-2:]) if len(n.split()) >= 2 else ""
    return last in _OWNER_SUFFIXES or two in _OWNER_SUFFIXES


def classify_org_type_offline(org: str) -> str:
    """List + heuristic only (no network). Returns property/operator/
    out_of_industry/unknown. 'unknown' means escalate to the grounded check."""
    n = _norm(org)
    if not n:
        return "unknown"

    # 0. known property/resort brand (chain you sell to) -> property
    if any(b in n for b in KNOWN_PROPERTY_BRANDS):
        return "property"

    # 1. known operator
    if get_management_company_intel(org):
        return "operator"

    # 2. out of industry (word-boundary match so 'hospital' != 'hospitality')
    if any(re.search(rf"\b{re.escape(k)}\b", n) for k in OUT_OF_INDUSTRY):
        return "out_of_industry"

    # 3. owner / developer / investment entity -> operator (contact, never a lead)
    if _ends_with_owner_suffix(n):
        return "operator"

    # 4. operator-suffix heuristic, unless a single-property hint is present
    has_operator_suffix = any(suf in n for suf in _OPERATOR_SUFFIXES)
    looks_like_one_property = any(h in n for h in _PROPERTY_HINTS)
    if has_operator_suffix and not looks_like_one_property:
        return "operator"

    return "unknown"


async def classify_org_type(org: str, *, allow_grounding: bool = True) -> str:
    """Full classifier: offline list/heuristic first, then ONE grounded Serper +
    ai_generate call for anything still unknown.

    Returns one of: 'property', 'operator', 'out_of_industry', 'unknown'.
    """
    verdict = classify_org_type_offline(org)
    if verdict != "unknown" or not allow_grounding:
        return verdict

    # lazy imports to avoid a heavy import chain at module load
    from app.services.contact_enrichment import _search_serper
    from app.services.ai_client import ai_generate

    try:
        results = await _search_serper(f"{org} hotel company", max_results=4)
    except Exception as e:  # network/quotas — stay safe
        logger.warning(f"org_classifier serper failed for {org!r}: {e}")
        return "unknown"

    snippets = "\n".join(f"- {r.get('title', '')}: {r.get('snippet', '')}" for r in results).strip()
    if not snippets:
        return "unknown"

    prompt = (
        "You are classifying a single organization for a hotel-supplier sales CRM.\n"
        f"Organization: {org}\n\n"
        "Search results:\n"
        f"{snippets}\n\n"
        "Reply with EXACTLY ONE word, no punctuation, no explanation:\n"
        "  PROPERTY  = a SPECIFIC, named, locatable hotel/resort, OR a branded\n"
        "              hotel/resort chain that markets its OWN properties under\n"
        "              its own name (e.g. Great Wolf Lodge, Four Seasons, St\n"
        "              Regis). A multi-location BRAND is still PROPERTY.\n"
        "  OPERATOR  = a company BEHIND hotels rather than a sellable hotel:\n"
        "              a third-party MANAGEMENT company (Crescent, Aimbridge,\n"
        "              Highgate) OR an OWNER / DEVELOPER / INVESTMENT entity\n"
        "              (names ending in Group, Development, Holdings, Partners,\n"
        "              Capital, Properties, REIT). Pick OPERATOR even if it owns\n"
        "              hotels -- it is not itself a sellable property.\n"
        "  OTHER     = not a hotel business at all\n"
        "  UNKNOWN   = the results don't make it clear\n"
    )

    try:
        async with httpx.AsyncClient(timeout=40) as c:
            ans = await ai_generate(c, prompt, temperature=0.0, max_tokens=8)
    except Exception as e:
        logger.warning(f"org_classifier ai_generate failed for {org!r}: {e}")
        return "unknown"

    token = (ans or "").strip().upper().split()[:1]
    token = token[0] if token else ""
    mapping = {
        "PROPERTY": "property",
        "OPERATOR": "operator",
        "OTHER": "out_of_industry",
        "UNKNOWN": "unknown",
    }
    return mapping.get(token, "unknown")
