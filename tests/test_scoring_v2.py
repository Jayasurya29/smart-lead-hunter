"""
Tests for opening_date regression guard + existing_hotel_scorer.

These cover the new behavior added 2026-04-29:
  - opening_date_specificity / should_accept_opening_date in utils.py
  - score_existing_hotel in existing_hotel_scorer.py
  - get_hotel_type_score in scorer.py (new component)
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


# ═══════════════════════════════════════════════════════════════════════
# OPENING DATE SPECIFICITY
# ═══════════════════════════════════════════════════════════════════════


class TestOpeningDateSpecificity:
    """Tests for opening_date_specificity() — used by the regression guard."""

    def test_full_date_is_5(self):
        from app.services.utils import opening_date_specificity
        assert opening_date_specificity("2026-09-15") == 5
        assert opening_date_specificity("September 15, 2026") == 5
        assert opening_date_specificity("Sep 15 2026") == 5

    def test_month_year_is_4(self):
        from app.services.utils import opening_date_specificity
        assert opening_date_specificity("September 2026") == 4
        assert opening_date_specificity("Sep 2026") == 4
        assert opening_date_specificity("December 2027") == 4

    def test_quarter_is_3(self):
        from app.services.utils import opening_date_specificity
        assert opening_date_specificity("Q3 2026") == 3
        assert opening_date_specificity("Q1 2027") == 3

    def test_season_is_2(self):
        from app.services.utils import opening_date_specificity
        assert opening_date_specificity("Late 2026") == 2
        assert opening_date_specificity("Spring 2026") == 2
        assert opening_date_specificity("Fall 2026") == 2
        assert opening_date_specificity("H2 2026") == 2
        assert opening_date_specificity("Summer 2027") == 2

    def test_bare_year_is_1(self):
        from app.services.utils import opening_date_specificity
        assert opening_date_specificity("2026") == 1
        assert opening_date_specificity("2030") == 1

    def test_vague_or_empty_is_0(self):
        from app.services.utils import opening_date_specificity
        assert opening_date_specificity("") == 0
        assert opening_date_specificity(None) == 0
        assert opening_date_specificity("TBD") == 0
        assert opening_date_specificity("2026 or 2027") == 0


# ═══════════════════════════════════════════════════════════════════════
# REGRESSION GUARD
# ═══════════════════════════════════════════════════════════════════════


class TestShouldAcceptOpeningDate:
    """Tests for should_accept_opening_date() — Smart Fill regression guard."""

    def test_empty_current_accepts_anything(self):
        from app.services.utils import should_accept_opening_date
        ok, _ = should_accept_opening_date("", "2026")
        assert ok is True
        ok, _ = should_accept_opening_date(None, "September 2026")
        assert ok is True

    def test_empty_candidate_rejected(self):
        from app.services.utils import should_accept_opening_date
        ok, _ = should_accept_opening_date("Late 2026", "")
        assert ok is False
        ok, _ = should_accept_opening_date("2026", None)
        assert ok is False

    def test_no_change_rejected(self):
        from app.services.utils import should_accept_opening_date
        ok, _ = should_accept_opening_date("Late 2026", "Late 2026")
        assert ok is False

    def test_refinement_accepted(self):
        """More-specific candidate should be accepted."""
        from app.services.utils import should_accept_opening_date
        # Year → Season
        ok, _ = should_accept_opening_date("2026", "Spring 2026")
        assert ok is True
        # Year → Month
        ok, _ = should_accept_opening_date("2026", "September 2026")
        assert ok is True
        # Season → Month
        ok, _ = should_accept_opening_date("Late 2026", "October 2026")
        assert ok is True
        # Quarter → Month
        ok, _ = should_accept_opening_date("Q3 2026", "September 2026")
        assert ok is True

    def test_regression_rejected(self):
        """Less-specific candidate should be rejected (the main bug fix)."""
        from app.services.utils import should_accept_opening_date
        # Season → Year (the case Jay specifically called out)
        ok, _ = should_accept_opening_date("Late 2026", "2026")
        assert ok is False
        # Month → Quarter
        ok, _ = should_accept_opening_date("September 2026", "Q3 2026")
        assert ok is False
        # Month → Year
        ok, _ = should_accept_opening_date("September 2026", "2026")
        assert ok is False

    def test_year_shifted_back_without_specificity_gain_rejected(self):
        from app.services.utils import should_accept_opening_date
        # 2027 → 2026 (same specificity, year backward → suspicious)
        ok, _ = should_accept_opening_date("2027", "2026")
        assert ok is False

    def test_year_shifted_back_with_specificity_gain_accepted(self):
        """Correction case: '2027' → 'September 2026' is OK because the
        new value is more specific, suggesting a real correction."""
        from app.services.utils import should_accept_opening_date
        ok, _ = should_accept_opening_date("2027", "September 2026")
        assert ok is True


# ═══════════════════════════════════════════════════════════════════════
# EXISTING HOTEL SCORER (Option B — account fit)
# ═══════════════════════════════════════════════════════════════════════


def _hotel(**kwargs):
    """Build a SimpleNamespace with all the fields the scorer reads."""
    defaults = {
        "brand_tier": None,
        "zone": None,
        "country": None,
        "room_count": None,
        "hotel_type": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestExistingHotelScorer:
    """Tests for score_existing_hotel() — Option B account-fit scoring."""

    def test_top_tier_caribbean_resort(self):
        from app.services.existing_hotel_scorer import score_existing_hotel
        h = _hotel(
            brand_tier="tier1_ultra_luxury",
            zone="South Florida",
            country="US",
            room_count=400,
            hotel_type="resort",
        )
        score, breakdown = score_existing_hotel(h)
        assert score == 40 + 35 + 13 + 10  # = 98
        assert breakdown["brand_tier"]["points"] == 40
        assert breakdown["zone"]["points"] == 35
        assert breakdown["hotel_type"]["points"] == 10

    def test_caribbean_country_fallback(self):
        """Cayman Islands should score 35 zone pts even with zone='Out of State'."""
        from app.services.existing_hotel_scorer import score_existing_hotel
        h = _hotel(
            brand_tier="tier2_luxury",
            zone="Out of State",
            country="Cayman Islands",
            room_count=80,
            hotel_type="boutique",
        )
        score, breakdown = score_existing_hotel(h)
        assert breakdown["zone"]["points"] == 35

    def test_unknown_inputs_get_floor(self):
        """All-unknown hotel still scores ~25-30, not 0."""
        from app.services.existing_hotel_scorer import score_existing_hotel
        h = _hotel()
        score, breakdown = score_existing_hotel(h)
        assert 20 <= score <= 35
        assert breakdown["brand_tier"]["points"] == 8  # unknown floor
        assert breakdown["zone"]["points"] == 10       # unknown floor

    def test_tier4_floor_not_basement(self):
        """Tier 4 should still score reasonably — it's the floor of JA scope."""
        from app.services.existing_hotel_scorer import score_existing_hotel
        h = _hotel(
            brand_tier="tier4_upscale",
            zone="South Florida",
            country="US",
            room_count=200,
            hotel_type="hotel",
        )
        score, _ = score_existing_hotel(h)
        # Tier4 (20) + premium zone (35) + 200rm (11) + hotel (7) = 73
        assert score == 73

    def test_out_of_scope_warning_for_tier5(self):
        """Tier 5 entries (shouldn't appear, but defensive) trigger warning."""
        from app.services.existing_hotel_scorer import score_existing_hotel
        h = _hotel(brand_tier="tier5_skip", zone="South Florida")
        _, breakdown = score_existing_hotel(h)
        assert "warnings" in breakdown
        assert any("scope" in w.lower() for w in breakdown["warnings"])

    def test_breakdown_has_v2_marker(self):
        from app.services.existing_hotel_scorer import score_existing_hotel
        _, breakdown = score_existing_hotel(_hotel())
        assert breakdown.get("version") == "v2"

    def test_substring_matches_freeform_hotel_type(self):
        """'all-inclusive resort' / 'luxury resort' should match correctly."""
        from app.services.existing_hotel_scorer import score_existing_hotel
        h = _hotel(
            brand_tier="tier2_luxury",
            zone="South Florida",
            country="US",
            room_count=600,
            hotel_type="all-inclusive resort",
        )
        _, breakdown = score_existing_hotel(h)
        assert breakdown["hotel_type"]["points"] == 10


# ═══════════════════════════════════════════════════════════════════════
# NEW HOTELS SCORER — hotel_type component
# ═══════════════════════════════════════════════════════════════════════


class TestHotelTypeScorerNewHotels:
    """Tests for the get_hotel_type_score added to scorer.py (new-hotels side)."""

    def test_resort_scores_9(self):
        from app.services.scorer import get_hotel_type_score
        pts, _ = get_hotel_type_score("resort")
        assert pts == 9

    def test_all_inclusive_scores_9(self):
        from app.services.scorer import get_hotel_type_score
        pts, _ = get_hotel_type_score("all-inclusive resort")
        assert pts == 9

    def test_boutique_scores_8(self):
        from app.services.scorer import get_hotel_type_score
        pts, _ = get_hotel_type_score("boutique hotel")
        assert pts == 8

    def test_hotel_scores_6(self):
        from app.services.scorer import get_hotel_type_score
        pts, _ = get_hotel_type_score("luxury hotel")
        assert pts == 6

    def test_unknown_floor(self):
        from app.services.scorer import get_hotel_type_score
        pts, _ = get_hotel_type_score(None)
        assert pts == 4
        pts, _ = get_hotel_type_score("")
        assert pts == 4
