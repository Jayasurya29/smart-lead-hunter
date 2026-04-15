"""
LEAD GENERATOR — Project Type Intelligence
==========================================
Classifies hotel leads by project type and maps to the correct
contact enrichment phase strategy.

PROJECT TYPES:
  new_opening      → Brand new construction. No existing staff, no supplier
                     relationships. Full phase cascade: Phase 3→2→1.

  renovation       → Existing hotel closes temporarily for refurbishment.
                     Existing GM likely stays. Target GM during closure.
                     Phase 2 first (GM already known), then Phase 1.

  rebrand          → Existing hotel changes brand flag.
                     MANDATORY uniform replacement — highest urgency.
                     Skip straight to Phase 3 (existing team stays, just needs
                     new uniforms NOW). If opening is imminent → call today.

  ownership_change → Hotel sold to new owner.
                     If management company changes → treat like new_opening.
                     If same management → target existing GM (Phase 2).

  unknown          → Default. Treat like new_opening.

PHASE MAPPING BY TIMELINE (timeline_label) + PROJECT TYPE:

  new_opening:
    COOL/WARM  → Phase 1 (mgmt company corporate)
    HOT        → Phase 2 (GM hunt) → fallback Phase 1
    URGENT     → Phase 3 (dept heads) → fallback Phase 2 → fallback Phase 1

  renovation:
    ANY        → Phase 2 first (existing GM during closure)
                 → fallback Phase 1 if GM not findable

  rebrand:
    ANY        → Phase 3 immediately (existing team needs new uniforms NOW)
                 → fallback Phase 2 → fallback Phase 1

  ownership_change:
    mgmt changed → same as new_opening
    mgmt same    → Phase 2 (existing GM)
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ProjectTypeResult:
    """Result of project type classification."""

    project_type: str  # new_opening / renovation / rebrand / ownership_change / unknown
    confidence: str  # high / medium / low
    signals: list[str]  # keywords that triggered the classification
    starting_phase: int  # 1, 2, or 3
    phase_reason: str  # human-readable explanation
    urgency_boost: bool  # True if rebrand (mandatory uniform replacement)


# ═══════════════════════════════════════════════════════════════
# KEYWORD SIGNALS
# ═══════════════════════════════════════════════════════════════

NEW_OPENING_SIGNALS = [
    "breaks ground",
    "groundbreaking",
    "ground breaking",
    "under construction",
    "new construction",
    "new build",
    "ground-up",
    "will open",
    "set to open",
    "scheduled to open",
    "opening in 2026",
    "opening in 2027",
    "opening in 2028",
    "anticipated opening",
    "brand new",
    "newly built",
    "new hotel",
    "new resort",
    "new development",
    "new property",
    "inaugural",
    "first hotel",
    "first property",
    "debut property",
    "topped out",
    "topping out",
    "construction milestone",
]

RENOVATION_SIGNALS = [
    "renovation",
    "renovating",
    "renovated",
    "refurbishment",
    "refurbished",
    "remodel",
    "remodeling",
    "remodeled",
    "refresh",
    "refreshed",
    "restoration",
    "restored",
    "revitalization",
    "revitalized",
    "closes for",
    "temporarily closed",
    "closed for renovation",
    "reopen",
    "reopening",
    "grand reopening",
    "re-opening",
    "after renovation",
    "following renovation",
    "post-renovation",
    "multi-million dollar renovation",
    "major renovation",
    "property transformation",
    "gut renovation",
]

REBRAND_SIGNALS = [
    "rebrands as",
    "rebranding as",
    "rebranded as",
    "joins autograph",
    "joins curio",
    "joins tribute",
    "joins the collection",
    "becomes part of",
    "affiliated with",
    "converts to",
    "conversion to",
    "converting to",
    "transitions to",
    "transitioning to",
    "transitioned to",
    "now part of",
    "now under",
    "new flag",
    "formerly known as",
    "was previously",
    "changing its name",
    "new brand",
    "new identity",
    "brand conversion",
    "soft brand",
    "joins marriott",
    "joins hilton",
    "joins hyatt",
    "joins ihg",
    "joins accor",
    "will operate as",
    "to operate under",
]

OWNERSHIP_SIGNALS = [
    "acquired by",
    "acquisition by",
    "acquires",
    "sold to",
    "sells to",
    "purchase by",
    "new ownership",
    "new owner",
    "change of ownership",
    "changes hands",
    "purchased by",
    "buys the",
    "investment in",
    "takes over",
    "takeover",
    "private equity",
    "new management",
]


def classify_project_type(
    hotel_name: str = "",
    description: str = "",
    project_type: str = "",  # field from lead extractor
    source_text: str = "",  # raw article text
    timeline_label: str = "",  # URGENT / HOT / WARM / COOL
    management_company: str = "",
) -> ProjectTypeResult:
    """
    Classify project type from available text signals and determine
    the optimal starting phase for contact enrichment.
    """
    # Combine all text for signal matching
    combined = " ".join(
        [
            hotel_name or "",
            description or "",
            project_type or "",
            source_text or "",
        ]
    ).lower()

    found_signals = []

    # ── Detect signals ──
    rebrand_hits = [s for s in REBRAND_SIGNALS if s in combined]
    renovation_hits = [s for s in RENOVATION_SIGNALS if s in combined]
    new_opening_hits = [s for s in NEW_OPENING_SIGNALS if s in combined]
    ownership_hits = [s for s in OWNERSHIP_SIGNALS if s in combined]

    # ── Priority order: rebrand > new_opening > renovation > ownership ──
    # Rebrand is highest urgency — mandatory uniform replacement
    if rebrand_hits:
        ptype = "rebrand"
        confidence = "high" if len(rebrand_hits) >= 2 else "medium"
        found_signals = rebrand_hits[:3]
        starting_phase = 3  # Existing team already there — need uniforms NOW
        reason = (
            "Rebrand detected — existing staff team in place but uniforms MUST change. "
            "Start with Phase 3 (department heads already hired). "
            "This is the highest-urgency scenario — contact immediately."
        )
        urgency_boost = True

    elif new_opening_hits and not renovation_hits:
        ptype = "new_opening"
        confidence = "high" if len(new_opening_hits) >= 2 else "medium"
        found_signals = new_opening_hits[:3]
        # Phase based on timeline
        starting_phase = _phase_from_timeline(timeline_label)
        reason = _new_opening_reason(timeline_label, starting_phase)
        urgency_boost = False

    elif renovation_hits:
        # Could be renovation with reopening
        ptype = "renovation"
        confidence = "high" if len(renovation_hits) >= 2 else "medium"
        found_signals = renovation_hits[:3]
        starting_phase = 2  # Existing GM during closure
        reason = (
            "Renovation/reopen detected. Existing GM likely staying — target them "
            "during the closure period when they have time to plan. "
            "Phase 2 (GM search) first, Phase 1 corporate fallback."
        )
        urgency_boost = False

    elif ownership_hits:
        ptype = "ownership_change"
        confidence = "medium"
        found_signals = ownership_hits[:3]
        # If management company field changed, treat like new opening
        starting_phase = 1 if not management_company else 2
        reason = "Ownership change detected. " + (
            "Management company unknown — treating like new opening, start Phase 1."
            if not management_company
            else f"Management company is {management_company} — target existing GM, Phase 2."
        )
        urgency_boost = False

    else:
        # Default: assume new opening, use timeline
        ptype = "unknown"
        confidence = "low"
        found_signals = []
        starting_phase = _phase_from_timeline(timeline_label)
        reason = (
            f"No strong project type signals found. "
            f"Defaulting to new_opening behavior. "
            f"Phase {starting_phase} based on {timeline_label or 'unknown'} timeline."
        )
        urgency_boost = False

    return ProjectTypeResult(
        project_type=ptype,
        confidence=confidence,
        signals=found_signals,
        starting_phase=starting_phase,
        phase_reason=reason,
        urgency_boost=urgency_boost,
    )


def _phase_from_timeline(timeline_label: str) -> int:
    """Map timeline bucket to starting phase for new openings."""
    tl = (timeline_label or "").upper()
    if tl == "URGENT":
        return 3  # 3-6 months — department heads being hired now
    elif tl == "HOT":
        return 2  # 6-12 months — GM being hired
    else:
        return 1  # WARM/COOL/TBD/unknown — management company corporate


def _new_opening_reason(timeline_label: str, phase: int) -> str:
    tl = (timeline_label or "").upper()
    if tl == "URGENT":
        return (
            "New opening, URGENT (3-6 months out). "
            "Start Phase 3 — department heads (Housekeeping, HR, F&B) being hired now. "
            "Auto-cascade to Phase 2 (GM) → Phase 1 (corporate) if nothing found."
        )
    elif tl == "HOT":
        return (
            "New opening, HOT (6-12 months out). "
            "Start Phase 2 — GM likely being hired or just announced. "
            "Auto-cascade to Phase 1 (management company corporate) if GM not found."
        )
    else:
        return (
            f"New opening, {tl or 'WARM/COOL'} (12+ months out). "
            "Start Phase 1 — management company corporate procurement. "
            "Property team not hired yet — go straight to the decision makers."
        )


# ═══════════════════════════════════════════════════════════════
# PHASE QUERIES — what to search at each phase per project type
# ═══════════════════════════════════════════════════════════════


def get_phase_queries(
    phase: int,
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    city: Optional[str],
    state: Optional[str],
    project_type: str = "new_opening",
) -> list[str]:
    """
    Generate the right search queries for a given phase + project type.
    Returns ordered list of queries — most valuable first.
    """
    queries = []
    location = " ".join(filter(None, [city, state]))
    mgmt = management_company or brand or ""

    if phase == 1:
        # ── PHASE 1: Management Company Corporate ──
        # Target SVP/VP Procurement, President of Hospitality, COO
        # For known companies, search by name directly
        if mgmt:
            queries += [
                f'"{mgmt}" "SVP Procurement" OR "VP Procurement" OR "Director of Procurement" site:linkedin.com',
                f'"{mgmt}" "VP Operations" OR "COO" OR "President" OR "EVP Operations" site:linkedin.com',
                f'"{mgmt}" "pre-opening" OR "new opening" {hotel_name}',
                f'"{mgmt}" procurement OR purchasing OR vendor 2026 site:linkedin.com',
            ]
        # Generic developer/owner search — kept generic, no per-lead names
        # leaking from prior debug iterations.
        if mgmt:
            queries.append(f'"{hotel_name}" developer OR owner OR "{mgmt}" leadership')
        else:
            queries.append(f'"{hotel_name}" developer OR owner OR leadership')
        queries.append(f'"{hotel_name}" opening 2026 management OR operations team')

    elif phase == 2:
        # ── PHASE 2: General Manager Hunt ──
        # Monitor for GM appointment announcements
        queries += [
            f'"{hotel_name}" "General Manager" appointed OR hired OR joins OR named site:linkedin.com',
            f'"{hotel_name}" "Pre-Opening General Manager" OR "Opening General Manager" site:linkedin.com',
            f'"{hotel_name}" {location} "General Manager" 2026',
            f'{hotel_name} appoints OR names OR hires "General Manager" 2026',
            f'"{hotel_name}" "General Manager" announcement OR appointment',
        ]
        if mgmt:
            queries.append(f'"{mgmt}" "General Manager" {location} OR "{hotel_name}"')

    elif phase == 3:
        # ── PHASE 3: Department Heads ──
        # The property team is being assembled — search each key role.
        # Run the FULL list of buyer-relevant dept titles. Previously
        # capped at [:6] which silently dropped Director of Purchasing,
        # Director of Rooms, Purchasing Manager, Director of Operations
        # — all SAP-confirmed buyers.
        dept_titles = [
            "Director of Housekeeping",
            "Executive Housekeeper",
            "Director of Human Resources",
            "Director of People and Culture",
            "Director of Food and Beverage",
            "F&B Director",
            "Purchasing Manager",
            "Director of Purchasing",
            "Director of Rooms",
            "Director of Operations",
        ]

        if project_type == "rebrand":
            # For rebrands, also search current staff by hotel name
            queries += [
                f'"{hotel_name}" site:linkedin.com',
                f'"{hotel_name}" {location} leadership OR team OR staff',
            ]

        for title in dept_titles:
            queries.append(f'"{hotel_name}" "{title}" site:linkedin.com')

        queries += [
            f'"{hotel_name}" {location} team OR staff OR leadership 2026',
            f'"{hotel_name}" hiring OR joins OR appointed department director',
        ]

    return queries


# ═══════════════════════════════════════════════════════════════
# PHASE TITLES — what contact titles to look for per phase
# ═══════════════════════════════════════════════════════════════

PHASE_TITLES = {
    1: [
        # Management company corporate — the pre-opening decision makers
        "SVP Procurement",
        "VP Procurement",
        "Senior Vice President Procurement",
        "VP Operations",
        "COO",
        "Chief Operating Officer",
        "EVP Operations",
        "SVP Operations",
        "President of Hospitality",
        "VP Hotel Operations",
        "Director of Procurement",
        "Pre-Opening Director",
        "Opening Director",
        "VP Development",
        "SVP Development",
    ],
    2: [
        # Property GM — the on-site decision maker
        "General Manager",
        "Pre-Opening General Manager",
        "Opening General Manager",
        "Task Force General Manager",
        "Hotel Manager",
        "Managing Director",
        "Resort Manager",
        "Property Manager",
        "Hotel Director",
    ],
    3: [
        # Department heads — uniform-specific buyers
        "Director of Housekeeping",
        "Executive Housekeeper",
        "Housekeeping Manager",
        "Director of Rooms",
        "Director of Human Resources",
        "Director of People and Culture",
        "Director of People & Culture",
        "HR Director",
        "Director of Food and Beverage",
        "F&B Director",
        "Director of F&B",
        "Executive Chef",
        "Purchasing Manager",
        "Director of Purchasing",
        "Director of Operations",
        "Operations Manager",
        "Rooms Division Manager",
        "Director of Front Office",
    ],
}


def get_phase_titles(phase: int) -> list[str]:
    """Get contact titles to search for at a given phase."""
    return PHASE_TITLES.get(phase, PHASE_TITLES[1])
