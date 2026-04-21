"""
LEAD GENERATOR — Project Type Intelligence
==========================================
Classifies hotel leads by project type and maps to the correct
contact enrichment phase strategy.

PROJECT TYPES:
  residences_only  → Branded-residences condo tower with no hotel rooms.
                     NOT a hotel lead. Early-reject. Example: "EDITION
                     Residences Miami Edgewater", "Cipriani Residences".
                     Uniforms opportunity = ZERO. Don't spend research cycles.

  new_opening      → Brand-new ground-up construction. No existing staff,
                     no supplier relationships. Full phase cascade per
                     timeline.

  reopening        → Existing property closed then returning. Staff
                     redeployed or on leave; corporate is the decision
                     maker for uniform refresh. Different from "renovation
                     while operating". Example: Sandals Montego Bay
                     reopening Dec 18, 2026 after Hurricane Melissa.
                     Phase 1 (corporate) first. Do NOT hunt property GM —
                     they already exist in the corporate org.

  renovation       → Existing hotel undergoing updates WHILE STILL OPEN.
                     Current GM in place and involved. Often phased
                     renovation. Example: The Driskill Austin — operating
                     since 1886, renovating through Summer 2026.
                     Phase 2 (current GM) first.

  rebrand          → Existing hotel changes brand flag.
                     MANDATORY uniform replacement — highest urgency.
                     Phase 3 (existing team stays, needs new uniforms NOW).

  ownership_change → Hotel sold to new owner.
                     If management company changes → treat like new_opening.
                     If same management → target existing GM (Phase 2).

  unknown          → Default. Treat like new_opening.

PHASE MAPPING BY TIMELINE (timeline_label) + PROJECT TYPE:

  residences_only:
    ANY        → REJECT. should_reject=True, rejection_reason='residences_only'

  new_opening:
    COOL/WARM  → Phase 1 (mgmt company corporate)
    HOT        → Phase 2 (GM hunt) → fallback Phase 1
    URGENT     → Phase 3 (dept heads) → fallback Phase 2 → fallback Phase 1

  reopening:
    ANY        → Phase 1 (corporate) — they make uniform decisions for
                 reopenings since the property was previously operating
                 under their playbook.

  renovation (while operating):
    ANY        → Phase 2 first (existing GM on site) → fallback Phase 1

  rebrand:
    ANY        → Phase 3 immediately (existing team needs new uniforms NOW)
                 → fallback Phase 2 → fallback Phase 1

  ownership_change:
    mgmt changed → same as new_opening
    mgmt same    → Phase 2 (existing GM)
"""

from dataclasses import dataclass
from typing import Optional
import re


@dataclass
class ProjectTypeResult:
    """Result of project type classification."""

    project_type: str  # residences_only / new_opening / reopening / renovation / rebrand / ownership_change / unknown
    confidence: str  # high / medium / low
    signals: list[str]  # keywords that triggered the classification
    starting_phase: int  # 1, 2, or 3 (irrelevant if should_reject=True)
    phase_reason: str  # human-readable explanation
    urgency_boost: bool  # True if rebrand (mandatory uniform replacement)
    # NEW FIELDS (backward-compatible defaults — existing callers unaffected)
    should_reject: bool = False  # True if lead shouldn't be pursued at all
    rejection_reason: Optional[str] = None  # e.g. 'residences_only_not_hotel'


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
    "new-build",  # hyphenated variant — Hyatt Vivid Punta Cana, etc.
    "ground-up",
    "will open",
    "will offer",
    "will feature",
    "will have",
    "will include",
    "will debut",
    "will comprise",
    "decision to open",
    "plans to debut",  # Canyon Ranch Austin
    "plans to open",
    "to debut a",
    "plans for a new",
    "property features",  # The Jordan San Gabriel
    "features a",  # generic feature-description pattern
    "offers stylish",  # Avila Beach House
    "offering stylish",
    "offering luxury",
    "offering an",
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
    "will debut",
    "debuts",
    "debut in",
    "coming 2025",
    "coming 2026",
    "coming 2027",
    "coming 2028",
    "topped out",
    "topping out",
    "construction milestone",
    "accepting reservations",
    "now taking reservations",
    "beachfront tower",  # Casa Cipriani, Andaz style
]

# ─── REOPENING signals ────────────────────────────────────────
# Property was operating, then CLOSED, and is now coming back.
# Distinct from "renovation while operating" (RENOVATION_OPERATING_SIGNALS).
#
# The difference matters for contact strategy:
#  - Reopening: property staff were redeployed or on leave; corporate
#    makes procurement decisions during the closure window
#  - Renovation while open: current GM on-site, involved in decisions
REOPENING_SIGNALS = [
    "reopening on",
    "reopens on",
    "reopening december",
    "reopening november",
    "reopening october",
    "reopening january",
    "reopening february",
    "reopening march",
    "reopening april",
    "reopening may",
    "reopening june",
    "reopening july",
    "reopening august",
    "reopening september",
    "grand reopening",
    "will reopen",
    "to reopen",
    "set to reopen",
    "scheduled to reopen",
    "temporarily closed",
    "closed for renovation",
    "closed since",
    "closed after",
    "welcomes back",
    "welcome back",
    "hurricane melissa",
    "post-hurricane",
    "hurricane recovery",
    "storm damage",
    # NOTE: "reimagined"/"reimagining" REMOVED — too ambiguous. Marketing copy
    # uses "reimagining" for brand-new hotels too (e.g. Hotel Flora Santa Fe
    # "Reimagining Santa Fe lifestyle"). Use "sandals 2.0" for the specific
    # Sandals renovation branding instead.
    "sandals 2.0",
    "reopens following",
    "returns in",
    "reopens after",
]

# ─── RENOVATION WHILE OPERATING signals ───────────────────────
# Hotel STAYS OPEN during the renovation. Current GM on-site.
# Counter-signals — if present alongside reopening signals, the lead is
# MORE likely a renovation-while-operating than a reopening.
RENOVATION_OPERATING_SIGNALS = [
    "remain open",
    "remains open",
    "will remain open",
    "remains operational",
    "open throughout",
    "open during",
    "continues to welcome guests",
    "continues operations",
    "phased renovation",
    "phased restoration",
    "phase one",
    "phase two",
    "phase 1",
    "phase 2",
    "partial renovation",
    "ongoing renovation",
    "multi-phase",
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
    "after renovation",
    "following renovation",
    "post-renovation",
    "multi-million dollar renovation",
    "major renovation",
    "property transformation",
    "gut renovation",
    "full renovation",
]

REBRAND_SIGNALS = [
    "rebrands as",
    "rebranding as",
    "rebranded as",
    "joins autograph",
    "joins curio",
    "joins tribute",
    "joins the collection",
    "joining the leading hotels",  # e.g. Ritz-Carlton T&C joining LHW
    "joins leading hotels",
    "joining leading hotels of the world",
    "joining the unbound collection",
    "joins the unbound collection",
    "joining small luxury hotels",
    "joins small luxury hotels",
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

# ─── RESIDENCES-ONLY signals ──────────────────────────────────
# Condo towers with NO hotel rooms. Zero uniform opportunity.
# We early-reject these so the researcher doesn't waste cycles.
RESIDENCES_ONLY_SIGNALS = [
    "residences only",
    "residential tower",
    "residential-only",
    "condo tower",
    "condominium tower",
    "branded condos",
    "luxury condo tower",
    "for sale condominiums",
    "condos for sale",
    "residential offering",
    "residence offering",
    "residences available",
    "condo development",
    "condominium development",
    "luxury residences for purchase",
    "units for purchase",
    "units for sale",
    "ultra-luxury condominiums",
    "limited collection of residences",
]

# Counter-signals — presence means it's a mixed hotel+residences project
# which we treat as a hotel lead (just flagged as mixed).
HOTEL_PRESENCE_SIGNALS = [
    "hotel rooms",
    "guest rooms",
    "guestrooms",
    "guest suites",
    "hotel suites",
    "keys",
    "boutique hotel",
    "hotel and residences",
    "hotel & residences",
    "hotel & private residences",
    "hotel and private residences",
    "all-suite hotel",
]

# ─── CONVERSION signals ───────────────────────────────────────
# Existing building being gutted and re-flagged under new brand.
# Different from pure rebrand (which keeps staff + operations). In a
# conversion, the building remains but FF&E, staff, and operations
# are essentially rebuilt. Example: Saguaro Scottsdale → Hyatt JdV
# Solaya. Building survives, everything inside changes.
#
# Procurement reality: operator has existing corporate procurement,
# but pre-opening GM is hired fresh and drives property-level decisions.
# Start at Phase 2 (GM) with Phase 1 (corporate) as parallel target.
CONVERSION_SIGNALS = [
    "transformation of the former",
    "transformation of former",
    "conversion of the former",
    "conversion of former",
    "converted from",
    "convert it under",  # Trailborn: "plans to renovate and convert it under the Trailborn brand"
    "convert under the",
    "repositioned as",
    "adaptive reuse",
    "reimagined as",  # safer than "reimagined" alone (which was removed)
    "transforms former",
    "transforming the former",
    "the former will",
    "former will become",
    "will become the",  # e.g. "the former office building will become the..."
]

# ─── Regex patterns for room-count detection ──────────────────
# "XXX-room hotel", "XXX-key resort", "XXX guestrooms" — strong
# signals that the text describes a specific hotel property
# (not a condo tower, not abstract). Used to boost hotel_presence
# when counter-signals are ambiguous.
_ROOM_COUNT_PATTERNS = [
    r"\b\d{2,4}[- ](?:hotel[- ])?room[s]?\b",  # "170-room", "236 rooms"
    r"\b\d{2,4}[- ]key\b",  # "180-key", "194 keys"
    r"\b\d{2,4}[- ]guestroom[s]?\b",  # "170 guestrooms"
    r"\b\d{2,4}[- ]guest[- ]room[s]?\b",  # "170 guest rooms", "170-guest-room"
    r"\b\d{2,4}[- ]suite[s]?\b",  # "40-suite hotel"
]


def classify_project_type(
    hotel_name: str = "",
    description: str = "",
    project_type: str = "",
    source_text: str = "",
    timeline_label: str = "",
    management_company: str = "",
) -> ProjectTypeResult:
    """
    Classify project type from available text signals and determine
    the optimal starting phase for contact enrichment.

    Classification priority:
      1. residences_only (early-reject — don't hunt contacts at all)
      2. rebrand         (mandatory uniform replacement, highest urgency)
      3. reopening       (closed property coming back — corporate-led)
      4. new_opening     (ground-up new construction)
      5. renovation      (operating hotel, phased updates)
      6. ownership_change
      7. unknown         (fallback)
    """
    combined = " ".join(
        [
            hotel_name or "",
            description or "",
            project_type or "",
            source_text or "",
        ]
    ).lower()

    # ── Detect all signals once ──
    residences_hits = [s for s in RESIDENCES_ONLY_SIGNALS if s in combined]

    # Hotel-presence hits need NEGATION filtering — "no guest rooms" shouldn't
    # count as "guest rooms". Require each match NOT to be preceded by "no "
    # or "zero " or "0 " within the 15 chars before it.
    def _is_negated(text: str, signal: str) -> bool:
        idx = text.find(signal)
        if idx == -1:
            return False
        # Look at the 15 chars before the signal
        prefix = text[max(0, idx - 15) : idx].strip().lower()
        # Check for negation words at the tail of the prefix
        negators = ("no ", "zero ", "not ", "without ", "0 ")
        return any(prefix.endswith(neg.strip()) for neg in negators) or any(
            f" {neg}" in prefix or prefix.startswith(neg) for neg in negators
        )

    hotel_presence_hits = [
        s
        for s in HOTEL_PRESENCE_SIGNALS
        if s in combined and not _is_negated(combined, s)
    ]

    # Regex-based room-count detection — "170-room", "180-key", "194 guestrooms"
    # These hospitality-industry phrases strongly imply a hotel property
    # (not a condo tower, not abstract language).
    room_count_hits = []
    for pattern in _ROOM_COUNT_PATTERNS:
        matches = re.findall(pattern, combined)
        if matches:
            room_count_hits.extend(matches[:2])  # cap to avoid noise
    # Append room counts to hotel_presence so residences_only check sees them
    if room_count_hits and not any(
        "residences only" in combined or "condo" in combined for _ in [0]
    ):
        # Only treat room count as hotel-presence evidence if the text doesn't
        # also say "residences only" or "condo". Mixed projects (Casa Cipriani
        # with "40 hotel rooms AND 23 condos") will still register hotel presence.
        hotel_presence_hits = hotel_presence_hits + [f"room_count:{room_count_hits[0]}"]

    conversion_hits = [s for s in CONVERSION_SIGNALS if s in combined]
    rebrand_hits = [s for s in REBRAND_SIGNALS if s in combined]
    reopening_hits = [s for s in REOPENING_SIGNALS if s in combined]
    renovation_operating_hits = [
        s for s in RENOVATION_OPERATING_SIGNALS if s in combined
    ]
    renovation_hits = [s for s in RENOVATION_SIGNALS if s in combined]
    new_opening_hits = [s for s in NEW_OPENING_SIGNALS if s in combined]
    ownership_hits = [s for s in OWNERSHIP_SIGNALS if s in combined]

    # ── Hotel_name-based residences detection (strong signal) ──
    name_lower = (hotel_name or "").lower()
    name_says_residences = "residences" in name_lower
    name_says_hotel_too = (
        "hotel and residences" in name_lower
        or "hotel & residences" in name_lower
        or "hotel and private residences" in name_lower
    )
    name_says_residences_only = name_says_residences and not name_says_hotel_too

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 1: residences_only — early reject
    # ══════════════════════════════════════════════════════════════
    if name_says_residences_only and not hotel_presence_hits:
        return ProjectTypeResult(
            project_type="residences_only",
            confidence="high",
            signals=[f"name: '{hotel_name}'"] + residences_hits[:2],
            starting_phase=0,
            phase_reason=(
                "Residences-only project detected from hotel name. No guest rooms — "
                "this is a branded condo tower, not a hotel. Zero uniform opportunity. "
                "Rejecting to save research cycles. If this is wrong, override manually."
            ),
            urgency_boost=False,
            should_reject=True,
            rejection_reason="residences_only_not_hotel",
        )

    if residences_hits and not hotel_presence_hits:
        confidence = "high" if len(residences_hits) >= 2 else "medium"
        return ProjectTypeResult(
            project_type="residences_only",
            confidence=confidence,
            signals=residences_hits[:3],
            starting_phase=0,
            phase_reason=(
                f"Residences-only signals detected ({len(residences_hits)} hits) with "
                f"NO hotel-presence counter-signals. This appears to be a condo "
                f"development, not a hotel. Rejecting. Review manually if uncertain."
            ),
            urgency_boost=False,
            should_reject=True,
            rejection_reason="residences_only_not_hotel",
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 2: rebrand
    # ══════════════════════════════════════════════════════════════
    if rebrand_hits:
        return ProjectTypeResult(
            project_type="rebrand",
            confidence="high" if len(rebrand_hits) >= 2 else "medium",
            signals=rebrand_hits[:3],
            starting_phase=3,
            phase_reason=(
                "Rebrand detected — existing staff team in place but uniforms MUST "
                "change. Start with Phase 3 (department heads already hired). This "
                "is the highest-urgency scenario — contact immediately."
            ),
            urgency_boost=True,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 2.5: conversion (existing building, new flag, new FF&E)
    # ══════════════════════════════════════════════════════════════
    # Distinct from rebrand: conversions gut the building and rebuild
    # operations, so staff are hired fresh. Distinct from new_opening:
    # building exists, operator is known, GM hired earlier than for
    # ground-up construction.
    if conversion_hits:
        return ProjectTypeResult(
            project_type="conversion",
            confidence="high" if len(conversion_hits) >= 2 else "medium",
            signals=conversion_hits[:3],
            starting_phase=2,  # Pre-opening GM hired earlier than new-builds
            phase_reason=(
                "Conversion detected — existing building being gutted and re-flagged "
                "under new brand. Unlike a rebrand (staff retained), conversions "
                "rebuild operations: staff hired fresh, FF&E replaced, concepts "
                "redesigned. Pre-opening GM is usually announced 6-9 months before "
                "reopening. Start Phase 2 (GM hunt) with Phase 1 (operator corporate) "
                "as parallel target — both are buyers."
            ),
            urgency_boost=False,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 3: reopening
    # ══════════════════════════════════════════════════════════════
    if reopening_hits and not renovation_operating_hits:
        confidence = "high" if len(reopening_hits) >= 2 else "medium"
        return ProjectTypeResult(
            project_type="reopening",
            confidence=confidence,
            signals=reopening_hits[:3],
            starting_phase=1,
            phase_reason=(
                "Reopening detected — existing property was closed and is returning. "
                "Property staff were redeployed during closure; corporate makes "
                "procurement decisions for the reopening uniform refresh. Start "
                "Phase 1 (corporate — operations VPs, procurement directors, "
                "regional ops). Skip property-GM hunt; GM already exists in the "
                "corporate org and will rejoin at reopening. Strong opportunity: "
                "new concepts usually debut with reopenings (new F&B, new bars, "
                "redesigned pools) which means NEW uniform SKUs."
            ),
            urgency_boost=False,
        )

    # Reopening signals + renovation-operating counter-signals → renovation
    if reopening_hits and renovation_operating_hits:
        combined_hits = (reopening_hits + renovation_operating_hits)[:4]
        return ProjectTypeResult(
            project_type="renovation",
            confidence="medium",
            signals=combined_hits,
            starting_phase=2,
            phase_reason=(
                "Mixed reopening + renovation signals. Counter-signals ('remains open', "
                "'phased') indicate the hotel is OPERATING during renovation, not "
                "closed. Treating as renovation-while-operating. Current GM is on-site "
                "and involved in procurement. Phase 2 (GM) first, Phase 1 corporate "
                "fallback."
            ),
            urgency_boost=False,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 4: new_opening
    # ══════════════════════════════════════════════════════════════
    if new_opening_hits and not renovation_hits:
        starting_phase = _phase_from_timeline(timeline_label)
        return ProjectTypeResult(
            project_type="new_opening",
            confidence="high" if len(new_opening_hits) >= 2 else "medium",
            signals=new_opening_hits[:3],
            starting_phase=starting_phase,
            phase_reason=_new_opening_reason(timeline_label, starting_phase),
            urgency_boost=False,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 5: renovation (operating hotel, no reopening signals)
    # ══════════════════════════════════════════════════════════════
    if renovation_hits:
        return ProjectTypeResult(
            project_type="renovation",
            confidence="high" if len(renovation_hits) >= 2 else "medium",
            signals=renovation_hits[:3],
            starting_phase=2,
            phase_reason=(
                "Renovation detected (hotel likely operating during updates). "
                "Current GM on-site and involved in procurement. Phase 2 (GM) first, "
                "Phase 1 corporate fallback."
            ),
            urgency_boost=False,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 6: ownership_change
    # ══════════════════════════════════════════════════════════════
    if ownership_hits:
        starting_phase = 1 if not management_company else 2
        return ProjectTypeResult(
            project_type="ownership_change",
            confidence="medium",
            signals=ownership_hits[:3],
            starting_phase=starting_phase,
            phase_reason=(
                "Ownership change detected. "
                + (
                    "Management company unknown — treating like new opening, start Phase 1."
                    if not management_company
                    else f"Management company is {management_company} — target existing GM, Phase 2."
                )
            ),
            urgency_boost=False,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 6.5: weak new_opening from room count alone
    # ══════════════════════════════════════════════════════════════
    # If text has an explicit room/key/guestroom count AND the lead has
    # an active timeline (URGENT/HOT/WARM/COOL), this is very likely a
    # pre-opening hotel property. Classify as new_opening with low
    # confidence rather than unknown — gives sales brief better context.
    if room_count_hits and timeline_label in ("URGENT", "HOT", "WARM", "COOL"):
        starting_phase = _phase_from_timeline(timeline_label)
        return ProjectTypeResult(
            project_type="new_opening",
            confidence="low",
            signals=[f"room_count:{room_count_hits[0]}"],
            starting_phase=starting_phase,
            phase_reason=(
                f"Weak signal: description mentions a room/key count "
                f"({room_count_hits[0]}), suggesting a hotel property. "
                f"Timeline is {timeline_label}. Defaulting to new_opening. "
                f"Review manually — Gemini enrichment should provide "
                f"stronger signals next research cycle."
            ),
            urgency_boost=False,
        )

    # ══════════════════════════════════════════════════════════════
    # PRIORITY 7: unknown — fallback
    # ══════════════════════════════════════════════════════════════
    starting_phase = _phase_from_timeline(timeline_label)
    return ProjectTypeResult(
        project_type="unknown",
        confidence="low",
        signals=[],
        starting_phase=starting_phase,
        phase_reason=(
            f"No strong project type signals found. "
            f"Defaulting to new_opening behavior. "
            f"Phase {starting_phase} based on {timeline_label or 'unknown'} timeline."
        ),
        urgency_boost=False,
    )


def _phase_from_timeline(timeline_label: str) -> int:
    """Map timeline bucket to starting phase for new openings."""
    tl = (timeline_label or "").upper()
    if tl == "URGENT":
        return 3
    elif tl == "HOT":
        return 2
    else:
        return 1


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
    """
    queries = []
    location = " ".join(filter(None, [city, state]))
    mgmt = management_company or brand or ""

    # ───────────────────────────────────────────────────────────
    # CONVERSION special case — existing building, new flag
    # ───────────────────────────────────────────────────────────
    # Conversions: hunt operator corporate + pre-opening GM simultaneously.
    # The building exists so the GM is hired earlier than for ground-up.
    # Also often mentions the "former" property — useful for context.
    if project_type == "conversion" and phase in (1, 2):
        if mgmt:
            queries += [
                f'"{hotel_name}" "pre-opening General Manager" OR "opening General Manager" site:linkedin.com',
                f'"{hotel_name}" opens 2026 general manager OR appointed',
                f'{mgmt} "general manager" {location} 2026',
                f'{mgmt} procurement OR "vice president" operations',
                f'"{hotel_name}" transformation announcement management team',
            ]
        queries.append(f'"{hotel_name}" opening team OR pre-opening 2026')
        return queries

    # ───────────────────────────────────────────────────────────
    # REOPENING special case — corporate-focused, skip property GM hunt
    # ───────────────────────────────────────────────────────────
    # For reopenings, the property team already exists in the corporate
    # org. The buyers are corporate operations and procurement leadership.
    # We override phase 2 (usually GM hunt) to also target corporate.
    if project_type == "reopening" and phase in (1, 2):
        if mgmt:
            queries += [
                f'"{hotel_name}" reopening announcement executive OR vice president OR president',
                f'{mgmt} reopening operations OR procurement OR "vice president"',
                f'{mgmt} "corporate procurement" OR "VP Operations" OR "SVP"',
                f'{mgmt} regional "vice president" operations',
                f'"{hotel_name}" reimagining OR renovation project team',
                f'"{hotel_name}" "opening team" OR "pre-opening team" reopening',
            ]
        queries.append(f'"{hotel_name}" reopening 2026 leadership OR executive')
        queries.append(f'"{hotel_name}" closed temporarily reopening management')
        return queries

    if phase == 1:
        if mgmt:
            queries += [
                f'{mgmt} appointment OR appointed OR "vice president" OR "senior vice president"',
                f'{mgmt} leadership announcement OR "new role" OR "joins as"',
                f'{mgmt} procurement OR purchasing OR sourcing director OR "vp procurement"',
                f'{mgmt} "food and beverage" OR "F&B operations" director OR vice president',
                f'{mgmt} operations "vice president" OR "regional vice president"',
                f'{mgmt} interview "vice president" OR "senior vice president"',
            ]
        if mgmt:
            queries.append(f'"{hotel_name}" developer OR owner OR "{mgmt}" leadership')
        else:
            queries.append(f'"{hotel_name}" developer OR owner OR leadership')
        queries.append(f'"{hotel_name}" opening 2026 management OR operations team')

    elif phase == 2:
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
