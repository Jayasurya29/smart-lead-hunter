"""
Smart Lead Hunter — Scraping & Config Tests
=============================================
Tests for scraping config, source seeding, location lists,
intelligence config, and scraping route setup logic.

No database or network needed.
"""

import pytest


# ═══════════════════════════════════════════════════════════════════════
# LOCATION CONFIG
# ═══════════════════════════════════════════════════════════════════════


class TestLocationConfig:
    """Tests for the shared location lists used in filtering."""

    def test_south_florida_cities_exist(self):
        from app.config.locations import SOUTH_FLORIDA_CITIES
        assert len(SOUTH_FLORIDA_CITIES) > 10
        assert "miami" in SOUTH_FLORIDA_CITIES
        assert "miami beach" in SOUTH_FLORIDA_CITIES
        assert "fort lauderdale" in SOUTH_FLORIDA_CITIES
        assert "doral" in SOUTH_FLORIDA_CITIES

    def test_caribbean_countries_exist(self):
        from app.config.locations import CARIBBEAN_COUNTRIES
        assert len(CARIBBEAN_COUNTRIES) > 5
        assert "bahamas" in CARIBBEAN_COUNTRIES
        assert "jamaica" in CARIBBEAN_COUNTRIES

    def test_southeast_states_exist(self):
        from app.config.locations import SOUTHEAST_STATES
        assert "georgia" in SOUTHEAST_STATES
        assert "south carolina" in SOUTHEAST_STATES

    def test_mountain_states_exist(self):
        from app.config.locations import MOUNTAIN_STATES
        assert "colorado" in MOUNTAIN_STATES
        assert "utah" in MOUNTAIN_STATES

    def test_all_locations_lowercase(self):
        """All location values should be lowercase for case-insensitive matching."""
        from app.config.locations import (
            SOUTH_FLORIDA_CITIES, CARIBBEAN_COUNTRIES,
            SOUTHEAST_STATES, MOUNTAIN_STATES,
        )
        for loc_list in [SOUTH_FLORIDA_CITIES, CARIBBEAN_COUNTRIES,
                         SOUTHEAST_STATES, MOUNTAIN_STATES]:
            for item in loc_list:
                assert item == item.lower(), f"Not lowercase: {item}"


# ═══════════════════════════════════════════════════════════════════════
# INTELLIGENCE CONFIG
# ═══════════════════════════════════════════════════════════════════════


class TestIntelligenceConfig:
    """Tests for scoring thresholds and skip patterns."""

    def test_score_thresholds_exist(self):
        from app.config.intelligence_config import (
            SCORE_HOT_THRESHOLD,
            SCORE_WARM_THRESHOLD,
            SCORE_COOL_THRESHOLD,
        )
        assert SCORE_HOT_THRESHOLD > SCORE_WARM_THRESHOLD
        assert SCORE_WARM_THRESHOLD > SCORE_COOL_THRESHOLD
        assert SCORE_HOT_THRESHOLD <= 100
        assert SCORE_COOL_THRESHOLD >= 0

    def test_skip_url_patterns_exist(self):
        from app.config.intelligence_config import SKIP_URL_PATTERNS
        assert isinstance(SKIP_URL_PATTERNS, (list, set, tuple))
        assert len(SKIP_URL_PATTERNS) > 0


# ═══════════════════════════════════════════════════════════════════════
# ENRICHMENT CONFIG
# ═══════════════════════════════════════════════════════════════════════


class TestEnrichmentConfig:
    """Tests for contact search priorities."""

    def test_contact_search_priorities_exist(self):
        from app.config.enrichment_config import CONTACT_SEARCH_PRIORITIES
        assert isinstance(CONTACT_SEARCH_PRIORITIES, (list, dict))
        assert len(CONTACT_SEARCH_PRIORITIES) > 0

    def test_hr_director_in_priorities(self):
        """HR Director must be in contact search priorities (critical role)."""
        from app.config.enrichment_config import CONTACT_SEARCH_PRIORITIES
        # Check if HR-related titles appear anywhere in priorities
        priorities_str = str(CONTACT_SEARCH_PRIORITIES).lower()
        assert any(term in priorities_str for term in [
            "human resources", "hr director", "hr manager",
        ]), "HR Director missing from CONTACT_SEARCH_PRIORITIES"


# ═══════════════════════════════════════════════════════════════════════
# SCRAPE SHARED STATE
# ═══════════════════════════════════════════════════════════════════════


class TestScrapeSharedState:
    """Tests for active_scrapes, cancellation, and lifecycle helpers."""

    @pytest.mark.asyncio
    async def test_cleanup_stale_scrapes(self):
        import time
        from app.shared import (
            active_scrapes, cleanup_stale_scrapes,
            _scrape_lock, _SCRAPE_TTL,
        )

        # Add a stale entry
        stale_id = "test-stale-scrape"
        async with _scrape_lock:
            active_scrapes[stale_id] = {
                "status": "running",
                "_started": time.monotonic() - _SCRAPE_TTL - 100,
            }

        await cleanup_stale_scrapes()

        # Should have been cleaned up
        assert stale_id not in active_scrapes

    @pytest.mark.asyncio
    async def test_cleanup_keeps_fresh_scrapes(self):
        import time
        from app.shared import (
            active_scrapes, cleanup_stale_scrapes, _scrape_lock,
        )

        fresh_id = "test-fresh-scrape"
        async with _scrape_lock:
            active_scrapes[fresh_id] = {
                "status": "running",
                "_started": time.monotonic(),
            }

        await cleanup_stale_scrapes()

        # Fresh entry should remain
        assert fresh_id in active_scrapes

        # Cleanup
        async with _scrape_lock:
            active_scrapes.pop(fresh_id, None)

    def test_cancellation_set(self):
        from app.shared import scrape_cancellations
        test_id = "test-cancel-id"
        scrape_cancellations.add(test_id)
        assert test_id in scrape_cancellations
        scrape_cancellations.discard(test_id)
        assert test_id not in scrape_cancellations


# ═══════════════════════════════════════════════════════════════════════
# MERGED LEAD CONVERSION
# ═══════════════════════════════════════════════════════════════════════


class TestMergedLeadConversion:
    """Tests for merged_lead_to_dict helper."""

    def test_basic_conversion(self):
        from app.shared import merged_lead_to_dict
        from unittest.mock import MagicMock

        ml = MagicMock()
        ml.hotel_name = "Test Hotel"
        ml.brand = "Test Brand"
        ml.property_type = "hotel"
        ml.city = "Miami"
        ml.state = "Florida"
        ml.country = "USA"
        ml.opening_date = "Q3 2027"
        ml.room_count = 200
        ml.contact_name = "John"
        ml.contact_title = "GM"
        ml.contact_email = "john@test.com"
        ml.contact_phone = None
        ml.source_urls = ["https://test.com"]
        ml.source_names = ["Test Source"]
        ml.key_insights = "Great hotel"
        ml.confidence_score = 0.9
        ml.qualification_score = 85
        ml.merged_from_count = 1

        d = merged_lead_to_dict(ml)
        assert d["hotel_name"] == "Test Hotel"
        assert d["city"] == "Miami"
        assert d["source_url"] == "https://test.com"

    def test_merged_from_multiple_sources(self):
        from app.shared import merged_lead_to_dict
        from unittest.mock import MagicMock

        ml = MagicMock()
        ml.hotel_name = "Test Hotel"
        ml.brand = None
        ml.property_type = None
        ml.city = "Miami"
        ml.state = "Florida"
        ml.country = "USA"
        ml.opening_date = None
        ml.room_count = None
        ml.contact_name = None
        ml.contact_title = None
        ml.contact_email = None
        ml.contact_phone = None
        ml.source_urls = ["a.com", "b.com"]
        ml.source_names = ["Source A", "Source B"]
        ml.key_insights = ""
        ml.confidence_score = 0.7
        ml.qualification_score = 0
        ml.merged_from_count = 3

        d = merged_lead_to_dict(ml)
        assert "Merged from 3 sources" in d["key_insights"]

    def test_empty_source_urls_uses_fallback(self):
        from app.shared import merged_lead_to_dict
        from unittest.mock import MagicMock

        ml = MagicMock()
        ml.hotel_name = "Test"
        ml.brand = None
        ml.property_type = None
        ml.city = None
        ml.state = None
        ml.country = None
        ml.opening_date = None
        ml.room_count = None
        ml.contact_name = None
        ml.contact_title = None
        ml.contact_email = None
        ml.contact_phone = None
        ml.source_urls = []
        ml.source_names = []
        ml.key_insights = ""
        ml.confidence_score = 0
        ml.qualification_score = 0
        ml.merged_from_count = 1

        d = merged_lead_to_dict(ml, fallback_url="https://fallback.com")
        assert d["source_url"] == "https://fallback.com"


# ═══════════════════════════════════════════════════════════════════════
# SOURCE CONFIG / SEED
# ═══════════════════════════════════════════════════════════════════════


class TestSourceConfig:
    """Tests for source_config patterns and seed data."""

    def test_source_patterns_importable(self):
        from app.services.source_config import SOURCE_PATTERNS
        assert isinstance(SOURCE_PATTERNS, dict)
        assert len(SOURCE_PATTERNS) > 0

    def test_get_patterns_returns_none_for_unknown(self):
        from app.services.source_config import get_patterns
        assert get_patterns("nonexistent_source_xyz") is None

    def test_list_configured_sources(self):
        from app.services.source_config import list_configured_sources
        sources = list_configured_sources()
        assert isinstance(sources, list)
        assert len(sources) > 0

    def test_has_patterns(self):
        from app.services.source_config import has_patterns, list_configured_sources
        sources = list_configured_sources()
        if sources:
            assert has_patterns(sources[0]) is True
        assert has_patterns("nonexistent_source_xyz") is False

    def test_source_seed_importable(self):
        from app.services.source_seed import SOURCES
        assert isinstance(SOURCES, list)
        assert len(SOURCES) > 0

    def test_seed_sources_have_required_fields(self):
        from app.services.source_seed import SOURCES
        for src in SOURCES[:5]:  # Check first 5
            assert "name" in src, f"Source missing 'name': {src}"
            assert "base_url" in src or "url" in src, f"Source missing URL: {src}"


# ═══════════════════════════════════════════════════════════════════════
# APP SETTINGS
# ═══════════════════════════════════════════════════════════════════════


class TestAppSettings:
    """Tests for config/settings correctness."""

    def test_settings_loads(self):
        from app.config import settings
        assert settings.database_url is not None
        assert len(settings.database_url) > 0

    def test_scoring_weights_sum_to_100(self):
        from app.config import settings
        total = (
            settings.score_brand_tier_max
            + settings.score_location_max
            + settings.score_timing_max
            + settings.score_room_count_max
            + settings.score_contact_max
            + settings.score_hotel_type_max
        )
        assert total == 100, f"Scoring weights sum to {total}, expected 100"

    def test_thresholds_ordering(self):
        from app.config import settings
        assert settings.hot_lead_threshold > settings.warm_lead_threshold
        assert settings.warm_lead_threshold > settings.min_score_threshold

    def test_has_gemini_property(self):
        from app.config import settings
        # Should be a bool, not crash
        assert isinstance(settings.has_gemini, bool)

    def test_get_best_ai_provider(self):
        from app.config import settings
        provider = settings.get_best_ai_provider()
        assert provider in ("gemini", "ollama")
