"""
Existing Hotel Scoring (Option B — Account Fit, v2)
====================================================

Pure scoring function. No I/O, no async, no DB.

Important context — JA Uniforms targets 4-star+ properties only:
  Tier 1 (Ultra Luxury), Tier 2 (Luxury), Tier 3 (Upper Upscale),
  Tier 4 (Upscale).

Tier 5+ (Upper Midscale, Midscale, Economy — collectively "tier5_skip"
in the codebase's canonical tiers) are AUTO-REJECTED at scrape time by
app/tasks/scraping_tasks.py via _scorer.is_budget_brand(). They never
enter potential_leads OR existing_hotels. Confirmed by the production
distribution (zero out-of-scope rows in existing_hotels).

That means this scorer's weighting can treat the 4-star+ universe as
"the whole universe" — Tier 4 is the FLOOR of scope, not "low priority."
The defensive `out_of_scope_warning` flag in the breakdown only fires
if a Tier 5+ row somehow slips through — at which point it's a
"please review" hint for sales, not a re-rejection (we don't unilaterally
reject things post-hoc).

Scoring model (out of 100)
--------------------------
  Brand tier   40   (T1=40, T2=35, T3=28, T4=20, unknown=8)
  Zone         35
  Room count   15
  Hotel type   10

Total          100

Field coverage in production data:
  - brand_tier:   ~100% populated  → solid 40-point anchor
  - zone:         ~100% populated  → solid 35-point anchor
  - room_count:    34% populated   → 15-pt component, unknown=4
  - hotel_type:    21% populated   → 10-pt component, unknown=5

Sparse fields use small-positive "unknown" floors so unenriched hotels
aren't punished — they just don't get the bonus. As Smart Fill /
Full Refresh runs on each hotel, hotel_type and room_count populate
and scores auto-update via the Smart Fill apply path.

Created: 2026-04-28
v2:      2026-04-28 — 4-star+ scope-aware weights, hotel_type tweak
"""

from __future__ import annotations

import logging
from typing import Any, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Brand tier (40 pts)
# ─────────────────────────────────────────────────────────────────────────────
#
# v2 weights reflect 4-star+ scope: Tier 4 is the floor at 20 pts (not 12),
# because every hotel in the universe is already Upscale or above.
#
# Tier 5+ entries kept defensively at score=5 — they shouldn't appear,
# but if they ever do (classification drift, manual override), they
# trigger the out_of_scope_warning in the breakdown.

_BRAND_TIER_POINTS = {
    "tier1_ultra_luxury": 40,
    "tier2_luxury": 35,
    "tier3_upper_upscale": 28,
    "tier4_upscale": 20,
    # Defensive — out of scope, shouldn't appear (auto-rejected at scrape):
    "tier5_skip": 5,
    "tier5_upper_midscale": 5,
    "tier6_midscale": 5,
    "tier7_economy": 5,
}
_OUT_OF_SCOPE_TIERS = {
    "tier5_skip",
    "tier5_upper_midscale",
    "tier6_midscale",
    "tier7_economy",
}
_BRAND_TIER_UNKNOWN = 8  # NULL / unrecognized — likely needs reclassification


def _score_brand_tier(brand_tier: str | None) -> tuple[int, str, bool]:
    """Return (points, label, out_of_scope_warning).

    out_of_scope_warning fires only if a Tier 5+ value is observed —
    used by the breakdown to tell sales "this row may have slipped past
    the scrape-time filter, please review."
    """
    if not brand_tier:
        return _BRAND_TIER_UNKNOWN, "unknown (NULL)", False
    key = brand_tier.strip().lower()
    if key in _BRAND_TIER_POINTS:
        warning = key in _OUT_OF_SCOPE_TIERS
        return _BRAND_TIER_POINTS[key], key, warning
    return _BRAND_TIER_UNKNOWN, f"unknown ({brand_tier!r})", False


# ─────────────────────────────────────────────────────────────────────────────
# Zone (35 pts)
# ─────────────────────────────────────────────────────────────────────────────

_ZONE_PREMIUM = {
    "south florida": 35,
    "florida keys": 35,
    "puerto rico": 35,
    "cayman islands": 35,
    "bermuda": 35,
    "bahamas": 35,
    "caribbean": 35,
}
_ZONE_STRONG_RESORT = {
    "orlando": 32,
    "tampa bay": 32,
    "southwest fl": 32,
    "napa & sonoma": 32,
    "palm springs / coachella valley": 32,
    "monterey & carmel": 32,
}
_ZONE_URBAN_COASTAL = {
    "los angeles metro": 28,
    "sf bay area": 28,
    "orange county": 28,
    "san diego": 28,
    "santa barbara & central coast": 28,
}
_ZONE_SECONDARY = {
    "north fl": 22,
    "panhandle": 22,
    "space coast": 22,
    "lake tahoe (ca side)": 22,
}
_ZONE_INLAND = {
    "sacramento & wine country east": 15,
}
_ZONE_OUT_OR_UNKNOWN = 10

_CARIBBEAN_COUNTRIES = {
    "bahamas",
    "bs",
    "barbados",
    "bb",
    "bermuda",
    "bm",
    "cayman islands",
    "ky",
    "cy",
    "grand cayman",
    "puerto rico",
    "pr",
    "us virgin islands",
    "vi",
    "british virgin islands",
    "vg",
    "turks and caicos",
    "turks and caicos islands",
    "tc",
    "jamaica",
    "jm",
    "dominican republic",
    "do",
    "aruba",
    "aw",
    "curacao",
    "curaçao",
    "cw",
    "saint lucia",
    "st. lucia",
    "lc",
    "saint kitts and nevis",
    "st. kitts and nevis",
    "kn",
    "anguilla",
    "ai",
    "antigua and barbuda",
    "ag",
    "grenada",
    "gd",
    "dominica",
    "dm",
    "trinidad and tobago",
    "tt",
    "saint vincent and the grenadines",
    "st. vincent & grenadines",
    "vc",
    "sint maarten",
    "saint martin",
    "st. martin / sint maarten",
    "sx",
    "mf",
    "caribbean",
}


def _score_zone(zone: str | None, country: str | None) -> tuple[int, str]:
    """Caribbean fallback: country in Caribbean → 35 pts even if zone is
    missing or 'Out of State'."""
    if country:
        c = country.strip().lower()
        if c in _CARIBBEAN_COUNTRIES:
            return 35, f"caribbean country: {country!r}"

    if not zone:
        return _ZONE_OUT_OR_UNKNOWN, "unknown (NULL zone)"

    z = zone.strip().lower()
    for table, label in (
        (_ZONE_PREMIUM, "premium"),
        (_ZONE_STRONG_RESORT, "strong_resort"),
        (_ZONE_URBAN_COASTAL, "urban_coastal"),
        (_ZONE_SECONDARY, "secondary"),
        (_ZONE_INLAND, "inland"),
    ):
        if z in table:
            return table[z], f"{label}: {zone!r}"

    return _ZONE_OUT_OR_UNKNOWN, f"out_of_territory: {zone!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Room count (15 pts)
# ─────────────────────────────────────────────────────────────────────────────


def _score_room_count(room_count: int | None) -> tuple[int, str]:
    if room_count is None or room_count <= 0:
        return 4, "unknown"
    if room_count >= 500:
        return 15, "500+"
    if room_count >= 300:
        return 13, "300-499"
    if room_count >= 200:
        return 11, "200-299"
    if room_count >= 100:
        return 8, "100-199"
    if room_count >= 50:
        return 5, "50-99"
    return 2, "<50"


# ─────────────────────────────────────────────────────────────────────────────
# Hotel type (10 pts)
# ─────────────────────────────────────────────────────────────────────────────
#
# v2 weights reflect 4-star+ scope: every property is already luxury,
# so urban "hotel" type isn't penalized hard (7 pts vs old 6 — different
# role mix, not lesser quality).


def _score_hotel_type(hotel_type: str | None) -> tuple[int, str]:
    """Substring matching on freeform values like 'luxury hotel and
    residences', 'all-inclusive resort', 'boutique hotel'."""
    if not hotel_type:
        return 5, "unknown (NULL)"

    t = hotel_type.strip().lower()
    if not t:
        return 5, "unknown (empty)"

    if "all-inclusive" in t or "all_inclusive" in t or "all inclusive" in t:
        return 10, f"all_inclusive: {hotel_type!r}"
    if "resort" in t:
        return 10, f"resort: {hotel_type!r}"
    if "boutique" in t:
        return 9, f"boutique: {hotel_type!r}"
    if "lodge" in t:
        return 7, f"lodge: {hotel_type!r}"
    if "inn" in t:
        return 5, f"inn: {hotel_type!r}"
    if "hotel" in t:
        return 7, f"hotel: {hotel_type!r}"

    return 5, f"unknown_type: {hotel_type!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Main scoring API
# ─────────────────────────────────────────────────────────────────────────────


def score_existing_hotel(hotel: Any) -> Tuple[int, dict]:
    """Compute the existing-hotels fit score (0-100).

    Accepts any object with attributes: brand_tier, zone, country,
    room_count, hotel_type. Works on ExistingHotel ORM rows OR plain
    dict-like objects (use SimpleNamespace if calling with a dict).

    Returns (score, breakdown_dict). The breakdown is persisted to
    existing_hotels.score_breakdown and rendered by the score popover.
    """
    bt_pts, bt_label, bt_warn = _score_brand_tier(getattr(hotel, "brand_tier", None))
    zn_pts, zn_label = _score_zone(
        getattr(hotel, "zone", None),
        getattr(hotel, "country", None),
    )
    rc_pts, rc_label = _score_room_count(getattr(hotel, "room_count", None))
    ht_pts, ht_label = _score_hotel_type(getattr(hotel, "hotel_type", None))

    final = bt_pts + zn_pts + rc_pts + ht_pts

    warnings: list[str] = []
    if bt_warn:
        warnings.append(
            "Out-of-scope brand tier — JA targets 4-star+ only. "
            "This row may have slipped past the scrape-time filter; please review."
        )

    breakdown: dict = {
        "version": "v2",
        "brand_tier": {"points": bt_pts, "max": 40, "label": bt_label},
        "zone": {"points": zn_pts, "max": 35, "label": zn_label},
        "room_count": {"points": rc_pts, "max": 15, "label": rc_label},
        "hotel_type": {"points": ht_pts, "max": 10, "label": ht_label},
        "final_score": final,
    }
    if warnings:
        breakdown["warnings"] = warnings

    return final, breakdown


def apply_score_to_hotel(hotel: Any) -> Tuple[int, dict]:
    """Compute score AND write lead_score + score_breakdown back to
    the given ORM row. Caller commits the session."""
    score, breakdown = score_existing_hotel(hotel)
    hotel.lead_score = score
    hotel.score_breakdown = breakdown
    return score, breakdown
