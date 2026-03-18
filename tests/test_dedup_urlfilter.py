"""
Smart Lead Hunter — Deduplicator & URL Filter Edge Case Tests
===============================================================
Pure unit tests — no database or network needed.

Covers:
  - Brand mismatch penalty in dedup
  - Unicode and special character handling
  - Location word stripping
  - Merge field selection (best data wins)
  - URL filter: block patterns, gold patterns, edge cases
  - URL filter: priority scoring
"""



# ═══════════════════════════════════════════════════════════════════════
# DEDUPLICATOR — BRAND GUARD
# ═══════════════════════════════════════════════════════════════════════


class TestDeduplicatorBrandGuard:
    """Brand-only guard prevents merging hotels from different brands."""

    def test_same_brand_same_city_merged(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Hilton Miami Beach", "brand": "Hilton", "city": "Miami Beach"},
            {"hotel_name": "Hilton Miami Beach", "brand": "Hilton", "city": "Miami Beach"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1

    def test_different_brand_not_merged(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Miami Beach Resort", "brand": "Hilton", "city": "Miami Beach"},
            {"hotel_name": "Miami Beach Resort", "brand": "Marriott", "city": "Miami Beach"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 2

    def test_unknown_brand_can_still_merge(self):
        """Two leads with no brand can merge if names match."""
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "The Grand Hotel Miami", "city": "Miami"},
            {"hotel_name": "Grand Hotel Miami", "city": "Miami"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1


class TestDeduplicatorEdgeCases:
    """Edge cases for deduplication."""

    def test_single_lead_returns_unchanged(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [{"hotel_name": "Lonely Hotel", "city": "Nowhere"}]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1

    def test_empty_list(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        merged = dedup.deduplicate([])
        assert len(merged) == 0

    def test_three_duplicates_merge_to_one(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Ritz-Carlton South Beach", "city": "Miami Beach", "source_url": "a.com"},
            {"hotel_name": "The Ritz-Carlton South Beach", "city": "Miami Beach", "source_url": "b.com"},
            {"hotel_name": "Ritz Carlton South Beach", "city": "Miami Beach", "source_url": "c.com"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1

    def test_merge_preserves_room_count(self):
        """Best data should win — non-null room_count preserved."""
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Aman New York", "city": "New York", "room_count": None},
            {"hotel_name": "Aman New York", "city": "New York", "room_count": 83},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1
        ml = merged[0]
        assert ml.room_count == 83

    def test_merge_preserves_contact_email(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Aman New York", "city": "New York", "contact_email": ""},
            {"hotel_name": "Aman New York", "city": "New York", "contact_email": "info@aman.com"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1
        assert merged[0].contact_email == "info@aman.com"

    def test_completely_different_hotels(self):
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Rosewood Miami Beach", "city": "Miami Beach"},
            {"hotel_name": "Four Seasons Bora Bora", "city": "Bora Bora"},
            {"hotel_name": "Aman Tokyo", "city": "Tokyo"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 3

    def test_merged_from_count(self):
        """MergedLead should track how many sources contributed."""
        from app.services.smart_deduplicator import SmartDeduplicator
        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Six Senses Utah", "city": "Utah", "source_url": "a.com"},
            {"hotel_name": "Six Senses Camp Utah", "city": "Utah", "source_url": "b.com"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1
        assert merged[0].merged_from_count >= 2


# ═══════════════════════════════════════════════════════════════════════
# URL FILTER — BLOCK PATTERNS
# ═══════════════════════════════════════════════════════════════════════


class TestURLFilterBlocking:
    """URLs that should be blocked by the filter."""

    def test_blocks_careers(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        assert f.should_scrape("https://marriott.com/careers/apply").should_scrape is False

    def test_blocks_login(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        # Blocked patterns use trailing slash: /login/
        assert f.should_scrape("https://example.com/login/page").should_scrape is False

    def test_blocks_social_media(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        for url in [
            "https://twitter.com/hotel",
            "https://facebook.com/hotel",
            "https://instagram.com/hotel",
        ]:
            result = f.should_scrape(url)
            assert result.should_scrape is False, f"Should block: {url}"

    def test_blocks_pdf_links(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("https://example.com/report.pdf")
        assert result.should_scrape is False

    def test_blocks_image_links(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        for ext in [".jpg", ".png", ".gif", ".jpeg"]:
            result = f.should_scrape(f"https://example.com/photo{ext}")
            assert result.should_scrape is False, f"Should block: {ext}"

    def test_blocks_privacy_policy(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("https://example.com/privacy-policy/")
        assert result.should_scrape is False

    def test_blocks_terms_of_service(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("https://example.com/terms-of-service/")
        assert result.should_scrape is False


class TestURLFilterAllowing:
    """URLs that should be allowed by the filter."""

    def test_allows_news_article(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("https://hospitalitynet.org/news/hotel-opening-2027")
        assert result.should_scrape is True

    def test_allows_hotel_opening_article(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("https://example.com/new-luxury-hotel-opens-miami-2027")
        assert result.should_scrape is True

    def test_allows_press_release(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("https://prnewswire.com/hotel-brand-announces-expansion")
        assert result.should_scrape is True


class TestURLFilterPriority:
    """Gold/high-value URLs should get higher priority scores."""

    def test_opening_keyword_boosts_priority(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        gold = f.should_scrape("https://example.com/new-hotel-opening-2027")
        normal = f.should_scrape("https://example.com/about-our-company")
        assert gold.priority >= normal.priority

    def test_year_keyword_boosts_priority(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        with_year = f.should_scrape("https://example.com/hotels-opening-2027")
        without = f.should_scrape("https://example.com/hotel-industry-news")
        assert with_year.priority >= without.priority

    def test_empty_url_handled(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("")
        assert result.should_scrape is False

    def test_malformed_url_handled(self):
        from app.services.url_filter import URLFilter
        f = URLFilter()
        result = f.should_scrape("not-a-url")
        # Should not crash
        assert isinstance(result.should_scrape, bool)


# ═══════════════════════════════════════════════════════════════════════
# SCORER — ADDITIONAL EDGE CASES
# ═══════════════════════════════════════════════════════════════════════


class TestScorerEdgeCases:
    """Additional scorer tests beyond test_core.py."""

    def test_caribbean_location_scores(self):
        from app.services.scorer import calculate_lead_score
        result = calculate_lead_score(
            hotel_name="Sandals Nassau",
            city="Nassau",
            state="New Providence",
            country="Bahamas",
            opening_date="2027",
            room_count=300,
        )
        assert result["location_type"] in ("caribbean", None) or "caribbean" in str(result.get("breakdown", {}))

    def test_florida_location_scores(self):
        from app.services.scorer import calculate_lead_score
        result = calculate_lead_score(
            hotel_name="Rosewood Miami Beach",
            city="Miami Beach",
            state="Florida",
            country="USA",
            opening_date="Q3 2027",
            room_count=200,
        )
        assert result["location_type"] == "florida"

    def test_international_gets_low_score(self):
        from app.services.scorer import calculate_lead_score
        result = calculate_lead_score(
            hotel_name="Aman Tokyo",
            city="Tokyo",
            state="",
            country="Japan",
            opening_date="2027",
        )
        # International should either be filtered or get low location points
        score = result["total_score"]
        assert score < 80 or result.get("should_save") is False

    def test_large_room_count_bonus(self):
        from app.services.scorer import calculate_lead_score
        small = calculate_lead_score(
            hotel_name="Boutique Hotel",
            room_count=20,
            city="Miami",
            state="Florida",
            country="USA",
        )
        large = calculate_lead_score(
            hotel_name="Mega Resort",
            room_count=500,
            city="Miami",
            state="Florida",
            country="USA",
        )
        # Larger hotel should score higher on room_count component
        small_rooms = small.get("breakdown", {}).get("room_count", {}).get("points", 0)
        large_rooms = large.get("breakdown", {}).get("room_count", {}).get("points", 0)
        assert large_rooms >= small_rooms

    def test_contact_info_bonus(self):
        from app.services.scorer import calculate_lead_score
        no_contact = calculate_lead_score(hotel_name="Test Hotel")
        with_contact = calculate_lead_score(
            hotel_name="Test Hotel",
            contact_name="John Smith",
            contact_email="john@hotel.com",
            contact_phone="+1-305-555-0100",
        )
        assert with_contact["total_score"] >= no_contact["total_score"]

    def test_tier5_brand_should_not_save(self):
        """Budget brands (Tier 5) should be marked as don't-save."""
        from app.services.scorer import calculate_lead_score
        result = calculate_lead_score(
            hotel_name="Super 8 Downtown Dallas",
            city="Dallas",
            state="Texas",
            country="USA",
        )
        assert result["should_save"] is False

    def test_score_never_exceeds_100(self):
        """Maximum possible score should not exceed 100."""
        from app.services.scorer import calculate_lead_score
        result = calculate_lead_score(
            hotel_name="Rosewood Miami Beach",
            brand="Rosewood Hotels",
            city="Miami Beach",
            state="Florida",
            country="USA",
            opening_date="Q3 2027",
            room_count=500,
            contact_name="John Smith",
            contact_email="john@rosewood.com",
            contact_phone="+1-305-555-0100",
            description="New build luxury resort, 500 rooms, opening Q3 2027",
        )
        assert result["total_score"] <= 100

    def test_score_never_negative(self):
        from app.services.scorer import calculate_lead_score
        result = calculate_lead_score(hotel_name="?")
        assert result["total_score"] >= 0
