"""
SMART LEAD HUNTER — Unified Contact Scoring
=============================================
Single source of truth for contact scoring. Called from every place that
creates or updates a LeadContact — enrichment pipeline, manual add, edit
button, scope toggle.

Before this module existed, 4 different code paths computed contact scores
using 4 different formulas:
  1. Enrichment (contact_enrichment.py): tier × scope_mult, priority floor
  2. Edit Contact (routes/contacts.py:update_contact): 30 | 12 | 8 | 5
  3. Toggle Scope (routes/contacts.py:toggle_contact_scope): 30 | 12 | 5
  4. Manual Add (routes/contacts.py:add_contact): 30 | 12 | 8 | 5

That meant editing a contact's email could RESCORE them from 28 → 5 based
on a completely different formula. Elie Khoury stuck at P1/5 LOW is the
smoking gun. Now one formula lives here and every caller uses it.

SCORING FORMULA:
    base_score = TIER_SCORES[tier]          # 0-20 from SAP-trained classifier
    scope_mult = SCOPE_MULTIPLIERS[scope]    # 1.0-3.0
    title_score = int(base_score * scope_mult)

    if strategist_priority (P1-P4):
        floor = PRIORITY_FLOOR[priority]     # 28 | 18 | 10 | 2
        final_score = max(title_score, floor)
    else:
        final_score = title_score

CONFIDENCE:
    Derived from scope + whether strategist verdict exists:
      - strategist P1/P2 → "high"
      - hotel_specific → "high"
      - management_corporate / owner → "high"
      - chain_area → "medium"
      - chain_corporate → "medium"
      - unknown → "low"
"""

import logging
from typing import Optional

from app.config.sap_title_classifier import BuyerTier, title_classifier

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# SCORING CONSTANTS — single source of truth
# ═══════════════════════════════════════════════════════════════


# Scope multipliers — applied on top of tier base score.
# hotel_specific is highest because they work AT the property.
# management_corporate is high because operator corporate IS the buyer
#   for soft-brand properties (Crescent, Aimbridge, Highgate, Pyramid).
# chain_corporate is low because brand-parent HQ (Marriott, Hilton)
#   rarely controls procurement for soft-brand properties.
# owner is mid-high because they write the checks but aren't
#   operational — their influence is financial approval.
SCOPE_MULTIPLIERS = {
    "hotel_specific": 3.0,
    "chain_area": 2.0,
    "management_corporate": 1.5,
    "chain_corporate": 1.2,
    "owner": 1.5,
    "unknown": 1.0,
}

# Floor scores applied when the Iter 6 strategist has assigned a priority.
# The strategist sees the whole picture (timeline, stage, company
# verification, role verification, region fit) and its verdict wins.
# A P1 contact is ALWAYS at least 28 even if their title isn't a
# classic buyer role — because the strategist said they matter.
PRIORITY_FLOOR = {
    "P1": 28,
    "P2": 18,
    "P3": 10,
    "P4": 2,
}

# Fallback score when we can't classify at all (no title, exception, etc.)
UNKNOWN_FALLBACK_SCORE = 5


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════


def score_contact(
    title: Optional[str],
    scope: Optional[str] = None,
    strategist_priority: Optional[str] = None,
) -> dict:
    """
    Score a contact. THE ONE TRUE WAY.

    Args:
        title:               Contact's job title (e.g. "SVP Procurement")
        scope:               hotel_specific | chain_area | management_corporate
                             | chain_corporate | owner | unknown
        strategist_priority: Optional Iter 6 verdict: "P1" | "P2" | "P3" | "P4"
                             When present, floor is applied — final score is
                             max(title-based score, priority floor).

    Returns:
        dict with keys:
          score            → final int score
          tier             → BuyerTier enum name (e.g. "TIER2_PURCHASING")
          confidence       → "high" | "medium" | "low"
          breakdown        → JSON-serializable dict for score_breakdown column
          formula          → human-readable explanation string

    Example:
        >>> score_contact("SVP Procurement", "management_corporate", "P1")
        {
          "score": 28,
          "tier": "TIER2_PURCHASING",
          "confidence": "high",
          "breakdown": {
            "title": {"value": "SVP Procurement", "tier": "TIER2_PURCHASING",
                      "base_points": 15},
            "scope": {"value": "management_corporate", "multiplier": 1.5},
            "title_score": 22,
            "strategist": {"priority": "P1", "floor": 28, "applied": True},
            "final_score": 28
          },
          "formula": "TIER2_PURCHASING (15) × management_corporate (×1.5) = 22, "
                     "then floored to P1 (28) = 28"
        }
    """
    scope_norm = (scope or "unknown").lower().strip()
    if scope_norm not in SCOPE_MULTIPLIERS:
        scope_norm = "unknown"

    priority_norm = (strategist_priority or "").upper().strip() or None
    if priority_norm and priority_norm not in PRIORITY_FLOOR:
        priority_norm = None

    # ── 1. Classify title into a tier ──
    title_for_log = (title or "").strip()
    if title_for_log:
        try:
            classification = title_classifier.classify(title_for_log)
            tier = classification.tier
            tier_name = tier.name
            base_points = title_classifier.TIER_SCORES.get(tier, UNKNOWN_FALLBACK_SCORE)
        except Exception as ex:
            logger.debug(f"Title classification failed for {title_for_log!r}: {ex}")
            tier = BuyerTier.UNKNOWN
            tier_name = "UNKNOWN"
            base_points = UNKNOWN_FALLBACK_SCORE
    else:
        tier = BuyerTier.UNKNOWN
        tier_name = "UNKNOWN"
        base_points = UNKNOWN_FALLBACK_SCORE

    # ── 2. Apply scope multiplier ──
    scope_mult = SCOPE_MULTIPLIERS[scope_norm]
    title_score = int(base_points * scope_mult)

    # ── 3. Apply strategist priority floor ──
    floor_applied = False
    priority_floor_value = None
    if priority_norm:
        priority_floor_value = PRIORITY_FLOOR[priority_norm]
        if priority_floor_value > title_score:
            final_score = priority_floor_value
            floor_applied = True
        else:
            final_score = title_score
    else:
        final_score = title_score

    # ── 4. Derive confidence ──
    confidence = _derive_confidence(scope_norm, priority_norm)

    # ── 5. Build breakdown for score_breakdown column ──
    breakdown = {
        "title": {
            "value": title_for_log or None,
            "tier": tier_name,
            "base_points": base_points,
        },
        "scope": {
            "value": scope_norm,
            "multiplier": scope_mult,
        },
        "title_score": title_score,
        "strategist": {
            "priority": priority_norm,
            "floor": priority_floor_value,
            "applied": floor_applied,
        },
        "final_score": final_score,
    }

    # ── 6. Human-readable formula ──
    formula = _build_formula(
        tier_name=tier_name,
        base_points=base_points,
        scope_norm=scope_norm,
        scope_mult=scope_mult,
        title_score=title_score,
        priority=priority_norm,
        priority_floor_value=priority_floor_value,
        floor_applied=floor_applied,
        final_score=final_score,
    )
    breakdown["formula"] = formula

    return {
        "score": final_score,
        "tier": tier_name,
        "confidence": confidence,
        "breakdown": breakdown,
        "formula": formula,
    }


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════


def _derive_confidence(scope: str, priority: Optional[str]) -> str:
    """Derive confidence label from scope + strategist verdict."""
    # Strategist P1/P2 = high confidence by definition (strategist saw it)
    if priority in ("P1", "P2"):
        return "high"
    if priority == "P3":
        return "medium"
    if priority == "P4":
        return "low"

    # No strategist verdict — use scope
    if scope == "hotel_specific":
        return "high"
    if scope in ("management_corporate", "owner"):
        return "high"
    if scope == "chain_area":
        return "medium"
    if scope == "chain_corporate":
        return "medium"
    return "low"


def _build_formula(
    tier_name: str,
    base_points: int,
    scope_norm: str,
    scope_mult: float,
    title_score: int,
    priority: Optional[str],
    priority_floor_value: Optional[int],
    floor_applied: bool,
    final_score: int,
) -> str:
    """Build the human-readable formula string shown in UI."""
    core = f"{tier_name} ({base_points}) × {scope_norm} (×{scope_mult}) = {title_score}"
    if priority and floor_applied:
        return f"{core}, then floored to {priority} ({priority_floor_value}) = {final_score}"
    if priority and not floor_applied:
        return (
            f"{core} beats {priority} floor ({priority_floor_value}) — "
            f"kept at {final_score}"
        )
    return f"{core} = {final_score}"


# ═══════════════════════════════════════════════════════════════
# CONVENIENCE WRAPPERS — for common call sites
# ═══════════════════════════════════════════════════════════════


def apply_score_to_contact(
    contact_obj, title=None, scope=None, strategist_priority=None
):
    """
    Score a contact and apply the results to a LeadContact SQLAlchemy object.

    Convenience for call sites that have a LeadContact and want to update
    score/tier/confidence/score_breakdown in one call.

    Returns the breakdown dict (also stored on contact.score_breakdown).

    Example:
        from app.services.contact_scoring import apply_score_to_contact
        apply_score_to_contact(
            contact,
            title=contact.title,
            scope=contact.scope,
            strategist_priority=contact.strategist_priority,
        )
    """
    result = score_contact(
        title=title if title is not None else getattr(contact_obj, "title", None),
        scope=scope if scope is not None else getattr(contact_obj, "scope", None),
        strategist_priority=(
            strategist_priority
            if strategist_priority is not None
            else getattr(contact_obj, "strategist_priority", None)
        ),
    )
    contact_obj.score = result["score"]
    contact_obj.tier = result["tier"]
    contact_obj.confidence = result["confidence"]
    # Only set score_breakdown if the column exists (after migration 013)
    if hasattr(contact_obj, "score_breakdown"):
        contact_obj.score_breakdown = result["breakdown"]
    return result


def score_contact_dict(contact_dict: dict) -> dict:
    """
    Score a raw contact dict from the enrichment pipeline.
    Mutates the dict in place, adding:
      _validation_score       (score)
      _buyer_tier             (tier name)
      _validation_confidence  (confidence label)
      _score_breakdown        (breakdown dict)

    Returns the breakdown dict for convenience.

    Used by the enrichment pipeline where contacts are raw dicts, not
    SQLAlchemy objects, and the existing code already reads from the
    _validation_score / _buyer_tier / _validation_confidence keys.
    """
    result = score_contact(
        title=contact_dict.get("title"),
        scope=contact_dict.get("scope"),
        strategist_priority=contact_dict.get("_final_priority"),
    )
    contact_dict["_validation_score"] = result["score"]
    contact_dict["_buyer_tier"] = result["tier"]
    contact_dict["_validation_confidence"] = result["confidence"]
    contact_dict["_score_breakdown"] = result["breakdown"]
    return result["breakdown"]
