"""
Smart Lead Hunter - Unit Tests
Tests for scorer, brand matching, timing, location, deduplication.
Run: pytest tests/test_scorer.py -v
"""

from datetime import datetime


# =====================================================================
# BRAND TIER TESTS
# =====================================================================


class TestBrandTier:
    """Test get_brand_tier() for correct classification."""

    def test_tier1_ultra_luxury(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("Aman New York")
        assert tier == 1
        assert pts == 25

    def test_tier2_luxury(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("W South Beach Hotel")
        assert tier == 2
        assert pts == 20

    def test_tier2_four_seasons(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("Four Seasons Miami Beach")
        assert tier == 2
        assert pts == 20

    def test_tier3_upper_upscale(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("Hyatt Regency Downtown Convention Center")
        assert tier == 3
        assert pts == 15

    def test_tier4_upscale(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("Embassy Suites Orlando")
        assert tier == 4
        assert pts == 10

    def test_tier5_budget_skip(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("Motel 6 Highway Exit")
        assert tier == 5
        assert pts == 0

    def test_unknown_brand(self):
        from app.services.scorer import get_brand_tier

        tier, name, pts = get_brand_tier("Totally Unknown Boutique Hotel")
        assert tier == 0
        assert name == "Unknown"
        assert pts == 5

    def test_short_brand_no_false_positive(self):
        """'tru' should not match 'Trump' (word boundary)."""
        from app.services.scorer import get_brand_tier

        tier, _, _ = get_brand_tier("Trump International Hotel")
        assert tier != 5

    def test_disney_budget_properties(self):
        """Disney All-Star should be tier 5, not tier 3."""
        from app.services.scorer import get_brand_tier

        tier, _, _ = get_brand_tier("Disney All-Star Movies Resort")
        assert tier == 5

    def test_disney_luxury_properties(self):
        """Disney Grand Floridian should stay tier 3+."""
        from app.services.scorer import get_brand_tier

        tier, _, _ = get_brand_tier("Disney Grand Floridian Resort")
        assert tier <= 3

    def test_exact_match_performance(self):
        """Exact brand name should hit O(1) dict lookup."""
        from app.services.scorer import get_brand_tier, _BRAND_TIER_MAP

        assert "motel 6" in _BRAND_TIER_MAP
        tier, _, _ = get_brand_tier("motel 6")
        assert tier == 5

    def test_hilton_garden_inn_is_budget(self):
        """Hilton Garden Inn is tier 5 (budget/limited service)."""
        from app.services.scorer import get_brand_tier

        tier, _, _ = get_brand_tier("Hilton Garden Inn Orlando")
        assert tier == 5


# =====================================================================
# TIMING SCORE TESTS
# get_timing_score returns (points, tier_str, year)
# =====================================================================


class TestTimingScore:
    """Test get_timing_score() for correct year handling."""

    def test_current_year(self):
        from app.services.scorer import get_timing_score

        current_year = datetime.now().year
        pts, tier, year = get_timing_score(str(current_year))
        assert pts >= 20

    def test_next_year(self):
        from app.services.scorer import get_timing_score

        next_year = datetime.now().year + 1
        pts, tier, year = get_timing_score(str(next_year))
        assert pts >= 10

    def test_far_future(self):
        from app.services.scorer import get_timing_score

        pts, tier, year = get_timing_score("2032")
        assert pts <= 10

    def test_no_date(self):
        from app.services.scorer import get_timing_score

        pts, tier, year = get_timing_score("")
        assert pts >= 0  # Unknown date gets minimal points

    def test_multiple_years_takes_latest(self):
        """'Opening 2025, delayed to 2027' should score on 2027."""
        from app.services.scorer import get_timing_score

        pts1, _, year1 = get_timing_score("Opening 2025, delayed to 2027")
        pts2, _, year2 = get_timing_score("2027")
        assert year1 == 2027
        assert pts1 == pts2

    def test_q_format(self):
        from app.services.scorer import get_timing_score

        current_year = datetime.now().year
        pts, _, year = get_timing_score(f"Q3 {current_year}")
        assert year == current_year
        assert pts >= 10

    def test_season_format(self):
        from app.services.scorer import get_timing_score

        next_year = datetime.now().year + 1
        pts, _, year = get_timing_score(f"Spring {next_year}")
        assert year == next_year
        assert pts >= 10


# =====================================================================
# LOCATION SCORE TESTS
# =====================================================================


class TestLocationScore:
    """Test get_location_score() for correct geographic handling."""

    def test_florida_keyword(self):
        from app.services.scorer import get_location_score

        pts, reason, loc_type = get_location_score("Miami", "FL", "USA")
        assert pts >= 10

    def test_florida_fl_no_false_positive(self):
        """'fl' should NOT match 'buffalo'."""
        from app.services.scorer import get_location_score

        pts, reason, loc_type = get_location_score("Buffalo", "NY", "USA")
        assert "florida" not in reason.lower()

    def test_caribbean(self):
        from app.services.scorer import get_location_score

        pts, reason, loc_type = get_location_score(
            "Nassau", "New Providence", "Bahamas"
        )
        assert pts >= 10

    def test_international_skip(self):
        from app.services.scorer import get_location_score

        pts, reason, loc_type = get_location_score(
            "London", "England", "United Kingdom"
        )
        assert pts == -1

    def test_empty_location_assumes_us(self):
        """Empty state/country should assume US, not trigger international skip."""
        from app.services.scorer import get_location_score

        pts, reason, loc_type = get_location_score("Rome", "", "")
        assert pts >= 0

    def test_us_state(self):
        from app.services.scorer import get_location_score

        pts, reason, loc_type = get_location_score("Austin", "Texas", "USA")
        assert pts >= 10


# =====================================================================
# LEAD SCORING INTEGRATION TESTS
# Uses score_with_breakdown() which returns a ScoreBreakdown object
# =====================================================================


class TestLeadScoring:
    """Test LeadScorer end-to-end."""

    def test_high_quality_lead(self):
        from app.services.scorer import LeadScorer

        scorer = LeadScorer()
        hotel = {
            "hotel_name": "W Miami Beach Resort",
            "brand": "W Hotels",
            "city": "Miami Beach",
            "state": "Florida",
            "country": "USA",
            "opening_date": str(datetime.now().year + 1),
            "room_count": 200,
        }
        result = scorer.score_with_breakdown(hotel)
        assert result.total >= 40

    def test_budget_hotel_low_score(self):
        from app.services.scorer import LeadScorer

        scorer = LeadScorer()
        hotel = {
            "hotel_name": "Motel 6 Highway Rest Stop",
            "city": "Somewhere",
            "state": "TX",
            "country": "USA",
            "opening_date": str(datetime.now().year + 1),
        }
        result = scorer.score_with_breakdown(hotel)
        assert result.total < 30

    def test_international_lead_low_score(self):
        from app.services.scorer import LeadScorer

        scorer = LeadScorer()
        hotel = {
            "hotel_name": "Luxury Hotel Tokyo",
            "city": "Tokyo",
            "state": "Tokyo",
            "country": "Japan",
            "opening_date": "2027",
        }
        result = scorer.score_with_breakdown(hotel)
        assert result.total < 20


# =====================================================================
# CIRCUIT BREAKER TESTS
# =====================================================================


class TestCircuitBreaker:
    """Test GeminiCircuitBreaker state transitions."""

    def test_initial_state_closed(self):
        from app.services.intelligent_pipeline import GeminiCircuitBreaker

        cb = GeminiCircuitBreaker()
        assert cb.state == "closed"
        assert cb.can_call() is True

    def test_opens_after_threshold(self):
        from app.services.intelligent_pipeline import GeminiCircuitBreaker

        cb = GeminiCircuitBreaker()
        for _ in range(5):
            cb.record_failure()
        assert cb.state == "open"
        assert cb.can_call() is False

    def test_success_resets(self):
        from app.services.intelligent_pipeline import GeminiCircuitBreaker

        cb = GeminiCircuitBreaker()
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.state == "closed"
        assert cb.failure_count == 0

    def test_half_open_after_timeout(self):
        import time
        from app.services.intelligent_pipeline import GeminiCircuitBreaker

        cb = GeminiCircuitBreaker()
        cb.RECOVERY_TIMEOUT = 0.1
        for _ in range(5):
            cb.record_failure()
        assert cb.state == "open"
        time.sleep(0.15)
        assert cb.can_call() is True
        assert cb.state == "half_open"

    def test_half_open_success_closes(self):
        import time
        from app.services.intelligent_pipeline import GeminiCircuitBreaker

        cb = GeminiCircuitBreaker()
        cb.RECOVERY_TIMEOUT = 0.1
        for _ in range(5):
            cb.record_failure()
        time.sleep(0.15)
        cb.can_call()
        cb.record_success()
        assert cb.state == "closed"

    def test_half_open_failure_reopens(self):
        import time
        from app.services.intelligent_pipeline import GeminiCircuitBreaker

        cb = GeminiCircuitBreaker()
        cb.RECOVERY_TIMEOUT = 0.1
        for _ in range(5):
            cb.record_failure()
        time.sleep(0.15)
        cb.can_call()
        cb.record_failure()
        assert cb.state == "open"
