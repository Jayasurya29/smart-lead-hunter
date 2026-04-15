"""
LEAD GENERATOR — Procurement Intelligence
==========================================
Determines the correct contact search strategy for each lead based on:
  - Brand operating model (managed / franchised / collection / independent)
  - Procurement model (Avendra GPO / brand-managed / owner-managed / fully open)
  - Pre-opening timeline (no team hired yet vs team being assembled)
  - Management company involvement

WHY THIS MATTERS:
  Wasting enrichment credits on construction project managers or corporate
  lawyers burns budget with zero ROI. The correct contact depends entirely
  on how the brand controls procurement.

OPERATING MODELS:
  managed       → Brand company operates the hotel (Four Seasons, Aman, Rosewood)
                  Corporate brand ops/procurement controls EVERYTHING pre-opening.
                  Property GM approves but brand sets vendor standards.

  franchised    → Owner pays for brand name, hires own management company
                  (Most Marriott upper upscale, Hilton upscale)
                  Management company (like Crescent) controls procurement.
                  GPO (Avendra, HSM) often mandates approved vendor list.

  collection    → Loose brand affiliation, max owner flexibility
                  (Autograph, Curio, Tribute, Tapestry)
                  Management company is KING here.
                  Avendra still used but less restrictive.

  all_inclusive → Parent company controls all procurement centrally
                  (Hyatt Inclusive Collection, Apple Leisure Group)
                  No point contacting property — go straight to corporate.

  independent   → No brand, owner/management company decides everything.
                  Management company is the only target.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ProspectingStrategy:
    """
    Strategic guidance for who to contact at a given lead.
    Used by SmartQueryBuilder to generate the RIGHT searches.
    """

    # The single most important contact path
    primary_target: str  # e.g. "Management company corporate procurement"
    primary_titles: list[str]  # Best titles to search for
    primary_org: str  # Which org to search within

    # Secondary contacts (property-level once hired)
    secondary_titles: list[str]

    # GPO flag — if set, JA must be approved on this platform to sell
    gpo: Optional[str] = None
    gpo_note: Optional[str] = None

    # Sales approach note
    approach_note: str = ""

    # Extra search queries specifically for this brand/model
    extra_queries: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════
# GPO KNOWLEDGE BASE
# When a brand uses a GPO, JA needs approved-vendor status to
# access their properties easily. Flag this in every search.
# ═══════════════════════════════════════════════════════════════
GPO_NOTES = {
    "Avendra": (
        "Avendra GPO — Marriott's preferred procurement platform. "
        "JA Uniforms should verify Avendra-approved status. "
        "Being on Avendra simplifies vendor approval across ALL Marriott properties."
    ),
    "Hilton Supply Management": (
        "Hilton Supply Management (HSM) controls vendor lists for Hilton properties. "
        "Contact HSM directly for preferred vendor status."
    ),
    "IHG Purchasing": (
        "IHG Merlin procurement platform. Preferred vendor approval opens all IHG properties."
    ),
    "Accor Purchasing": (
        "Accor Purchasing Hub — centralized procurement. "
        "Regional VP Operations approves vendor additions."
    ),
    "Hyatt Purchasing": (
        "Hyatt Purchasing Team — centralized for managed properties. "
        "VP Procurement at Hyatt corporate approves new vendors."
    ),
}


# ═══════════════════════════════════════════════════════════════
# MANAGEMENT COMPANY KNOWLEDGE BASE
# Known corporate procurement contacts at major hotel management
# companies. These are the people who control vendor decisions
# across ALL their managed properties.
# ═══════════════════════════════════════════════════════════════
MANAGEMENT_COMPANY_INTEL = {
    "crescent hotels": {
        "procurement_titles": [
            "SVP Procurement Operations",
            "VP Procurement",
            "Senior Vice President Procurement",
            "Director of Procurement",
            "VP of Supply Chain",
        ],
        "ops_titles": [
            "SVP Operations",
            "VP Hotel Operations",
            "Regional VP Operations",
            "VP of Operations",
        ],
        "known_contacts": [
            "Adam Butts",  # SVP Procurement Operations
        ],
        "hq_location": "Fairfax, Virginia",
        "portfolio_size": "120+ properties",
        "note": "Large management company — corporate procurement controls preferred vendor lists for all properties.",
    },
    "sage hospitality": {
        "procurement_titles": [
            "VP Procurement",
            "Director of Purchasing",
            "Director of Procurement",
        ],
        "ops_titles": ["COO", "VP Operations", "SVP Operations"],
        "hq_location": "Denver, Colorado",
        "portfolio_size": "50+ properties",
    },
    "white lodging": {
        "procurement_titles": ["VP Procurement", "Director of Purchasing"],
        "ops_titles": ["COO", "VP Hotel Operations"],
        "hq_location": "Merrillville, Indiana",
        "portfolio_size": "60+ properties",
    },
    "pivot hotels": {
        "procurement_titles": ["Director of Procurement", "VP Purchasing"],
        "ops_titles": ["VP Operations", "COO"],
        "portfolio_size": "30+ properties",
    },
    "highgate hotels": {
        "procurement_titles": ["VP Procurement", "Director of Purchasing"],
        "ops_titles": ["EVP Operations", "COO", "SVP Operations"],
        "hq_location": "New York, NY",
        "portfolio_size": "100+ properties",
    },
    "remington hotels": {
        "procurement_titles": ["VP Procurement", "Director of Procurement"],
        "ops_titles": ["COO", "VP Operations"],
        "hq_location": "Addison, Texas",
        "portfolio_size": "100+ properties",
    },
    "aimbridge hospitality": {
        "procurement_titles": [
            "SVP Procurement",
            "VP Procurement",
            "Director of Procurement",
        ],
        "ops_titles": ["COO", "EVP Operations", "SVP Operations"],
        "hq_location": "Plano, Texas",
        "portfolio_size": "1,500+ properties",
        "note": "Largest third-party operator — getting on their preferred vendor list = massive opportunity.",
    },
    "davidson hospitality": {
        "procurement_titles": ["VP Procurement", "Director of Procurement"],
        "ops_titles": ["COO", "SVP Operations"],
        "hq_location": "Atlanta, Georgia",
        "portfolio_size": "70+ properties",
    },
    "concord hospitality": {
        "procurement_titles": ["VP Procurement", "Director of Purchasing"],
        "ops_titles": ["COO", "VP Operations"],
        "hq_location": "Raleigh, North Carolina",
        "portfolio_size": "100+ properties",
    },
    "coury hospitality": {
        "procurement_titles": ["Director of Procurement", "VP Operations"],
        "ops_titles": ["President", "COO", "VP Operations"],
        "hq_location": "Fort Worth, Texas",
        "portfolio_size": "10+ properties",
    },
    "dimension hospitality": {
        "procurement_titles": ["VP Procurement", "Director of Purchasing"],
        "ops_titles": ["COO", "VP Operations"],
        "portfolio_size": "Regional operator",
    },
    "aparium hotel group": {
        "procurement_titles": ["Director of Procurement", "VP Operations"],
        "ops_titles": ["COO", "Managing Partner", "VP Operations"],
        "hq_location": "Chicago, Illinois",
        "portfolio_size": "20+ boutique properties",
        "note": "Independent/boutique focus — expect more direct owner involvement.",
    },
    "auberge resorts": {
        "procurement_titles": ["VP Operations", "Director of Operations"],
        "ops_titles": ["President", "COO", "SVP Operations"],
        "hq_location": "Mill Valley, California",
        "portfolio_size": "25+ luxury properties",
        "note": "Ultra-luxury independent collection — property GMs have significant autonomy.",
    },
    "loews hotels": {
        "procurement_titles": ["VP Procurement", "Director of Purchasing"],
        "ops_titles": ["COO", "SVP Operations"],
        "hq_location": "New York, NY",
        "portfolio_size": "25+ properties",
    },
    "omni hotels": {
        "procurement_titles": [
            "VP Procurement",
            "Director of Purchasing",
            "SVP Procurement",
        ],
        "ops_titles": ["COO", "SVP Hotel Operations"],
        "hq_location": "Dallas, Texas",
        "portfolio_size": "50+ properties",
        "note": "Omni is both owner and operator — direct corporate procurement relationship.",
    },
    "hyatt inclusive collection": {
        "procurement_titles": ["VP Procurement", "Director of Procurement"],
        "ops_titles": ["COO", "SVP Operations", "VP All-Inclusive Operations"],
        "hq_location": "Cancun/Miami",
        "portfolio_size": "100+ all-inclusive resorts",
        "note": "Fully centralized procurement — all vendor decisions made at corporate level.",
    },
}


def get_management_company_intel(management_company: str) -> Optional[dict]:
    """Look up known intel for a management company."""
    if not management_company:
        return None
    key = management_company.lower().strip()
    # Direct match
    if key in MANAGEMENT_COMPANY_INTEL:
        return MANAGEMENT_COMPANY_INTEL[key]
    # Partial match
    for k, v in MANAGEMENT_COMPANY_INTEL.items():
        if k in key or key in k:
            return v
    return None


def build_prospecting_strategy(
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    operating_model: Optional[str],
    procurement_model: Optional[str],
    gpo: Optional[str],
    months_until_opening: Optional[float],
) -> ProspectingStrategy:
    """
    Given what we know about a lead, determine the optimal contact search strategy.

    This is the core intelligence that prevents us from wasting effort on
    construction managers or junior staff with no procurement authority.
    """
    mgmt_intel = get_management_company_intel(management_company or "")
    gpo_note = GPO_NOTES.get(gpo or "", None) if gpo else None

    # ── STRATEGY BY OPERATING MODEL ──────────────────────────────────

    if operating_model == "all_inclusive":
        # Hyatt Inclusive, Apple Leisure, etc. — 100% centralized
        corp = management_company or brand or ""
        return ProspectingStrategy(
            primary_target=f"Corporate procurement at {corp}",
            primary_titles=[
                "VP Procurement",
                "Director of Procurement",
                "SVP Operations",
                "COO",
                "VP All-Inclusive Operations",
            ],
            primary_org=corp,
            secondary_titles=["General Manager", "Director of Operations"],
            gpo=gpo,
            gpo_note=gpo_note,
            approach_note=(
                f"All-inclusive properties are 100% centrally managed. "
                f"Contact {corp} corporate procurement — property GMs have minimal purchasing authority."
            ),
            extra_queries=[
                f'"{corp}" "VP Procurement" OR "Director of Procurement" site:linkedin.com',
                f'"{corp}" "pre-opening" OR "new property" {hotel_name}',
            ],
        )

    elif operating_model == "managed":
        # Brand-managed: Four Seasons, Aman, Rosewood, EDITION, etc.
        # Brand's regional ops team controls standards + procurement
        brand_org = brand or management_company or ""
        return ProspectingStrategy(
            primary_target=f"Brand regional operations at {brand_org}",
            primary_titles=[
                "VP Operations",
                "Regional VP Operations",
                "Pre-Opening Director",
                "VP Hotel Development",
                "Director of Operations",
                "VP Luxury Operations",
                "Regional Director of Operations",
            ],
            primary_org=brand_org,
            secondary_titles=[
                "General Manager",
                "Pre-Opening General Manager",
                "Director of Rooms",
                "Director of Housekeeping",
            ],
            gpo=gpo,
            gpo_note=gpo_note,
            approach_note=(
                f"{brand_org} is a brand-managed property. The brand's regional VP Operations "
                f"controls vendor approval and sets uniform standards. Property GM will be hired "
                f"6-12 months before opening and will have input on final selection."
            ),
            extra_queries=[
                f'"{brand_org}" "VP Operations" OR "Pre-Opening" OR "Regional Director" site:linkedin.com',
                f'"{brand_org}" {hotel_name} "General Manager" OR "appointed" OR "opening"',
                f'"{hotel_name}" "pre-opening" team OR staff OR appointment 2026',
            ],
        )

    elif operating_model in ("franchised", "collection"):
        # Franchised/Collection: Autograph, Curio, Tribute, Tapestry, etc.
        # Management company is the key — they run day-to-day and control procurement
        mgmt_org = management_company or brand or ""

        if mgmt_intel:
            primary_titles = mgmt_intel.get("procurement_titles", []) + mgmt_intel.get(
                "ops_titles", []
            )
            portfolio_note = mgmt_intel.get("note", "")
            portfolio_size = mgmt_intel.get("portfolio_size", "")
        else:
            primary_titles = [
                "SVP Procurement",
                "VP Procurement",
                "Director of Procurement",
                "VP of Operations",
                "COO",
                "SVP Operations",
            ]
            portfolio_note = ""
            portfolio_size = ""

        extra = [
            f'"{mgmt_org}" "procurement" OR "purchasing" OR "vendor" site:linkedin.com',
            f'"{mgmt_org}" {hotel_name} "pre-opening" OR "opening" OR "appointed"',
            f'"{hotel_name}" "General Manager" OR "appointed" OR "opening team" 2025 OR 2026',
        ]
        if mgmt_intel and mgmt_intel.get("known_contacts"):
            for contact in mgmt_intel["known_contacts"]:
                extra.append(f'"{contact}" "{mgmt_org}" site:linkedin.com')

        avendra_note = ""
        if procurement_model == "avendra_gpo" or gpo == "Avendra":
            avendra_note = (
                " NOTE: This brand uses Avendra GPO. "
                "JA should verify Avendra-approved vendor status — it unlocks all properties on this platform."
            )

        return ProspectingStrategy(
            primary_target=f"Corporate procurement at {mgmt_org} (management company)",
            primary_titles=primary_titles,
            primary_org=mgmt_org,
            secondary_titles=[
                "General Manager",
                "Pre-Opening General Manager",
                "Director of Housekeeping",
                "Executive Housekeeper",
                "Purchasing Manager",
                "Director of F&B",
                "Director of Human Resources",
            ],
            gpo=gpo,
            gpo_note=(gpo_note or "") + avendra_note
            if (gpo_note or avendra_note)
            else None,
            approach_note=(
                f"{mgmt_org} is the management company running this {operating_model} property. "
                f"{'Portfolio: ' + portfolio_size + '. ' if portfolio_size else ''}"
                f"Corporate procurement team controls vendor relationships across all their properties. "
                f"{'Note: ' + portfolio_note + ' ' if portfolio_note else ''}"
                f"Property-level team will be hired as opening approaches."
                + avendra_note
            ),
            extra_queries=extra,
        )

    else:
        # Independent or unknown — management company if available, else developer
        primary_org = management_company or hotel_name
        return ProspectingStrategy(
            primary_target=f"Management company or developer ({primary_org})",
            primary_titles=[
                "General Manager",
                "Pre-Opening General Manager",
                "COO",
                "VP Operations",
                "Director of Operations",
                "Purchasing Manager",
                "Director of Housekeeping",
            ],
            primary_org=primary_org,
            secondary_titles=[
                "Director of Rooms",
                "Executive Housekeeper",
                "Director of F&B",
                "Director of Human Resources",
            ],
            gpo=gpo,
            gpo_note=gpo_note,
            approach_note=(
                f"Independent property — management company ({management_company or 'unknown'}) "
                f"controls procurement. Focus on whoever is running operations."
            ),
            extra_queries=[
                f'"{hotel_name}" "General Manager" OR "appointed" site:linkedin.com',
                f'"{hotel_name}" opening team OR staff OR leadership 2026',
                f'"{primary_org}" "General Manager" OR "Director" hotel',
            ],
        )
