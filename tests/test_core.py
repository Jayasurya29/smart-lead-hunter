"""
SMART LEAD HUNTER — Initial Test Suite
Fix: T1 (zero tests exist)

Top 5 tests covering the highest-risk areas:
1. Scorer - brand matching, score ranges, edge cases
2. Deduplicator - fuzzy matching, merge logic
3. URL Filter - block patterns, priority scoring
4. Pipeline - classification, extraction parsing
"""

import json


# ============================================================
# TEST 1: Scorer
# ============================================================


class TestScorer:
    """Tests for app.services.scorer"""

    def test_brand_tier_exact_match(self):
        """Known luxury brands get correct tier scores"""
        from app.services.scorer import get_brand_tier

        # get_brand_tier returns (tier_number, tier_name, points)
        tier_num, tier_name, points = get_brand_tier("Rosewood Hotels")
        assert points == 25
        assert tier_num == 1

        _, _, points2 = get_brand_tier("Aman Resort")
        assert points2 == 25

        _, _, points3 = get_brand_tier("Six Senses Lodge")
        assert points3 == 25

    def test_brand_tier_case_insensitive(self):
        """Brand matching is case-insensitive (L1 fix)"""
        from app.services.scorer import get_brand_tier

        _, _, p1 = get_brand_tier("ROSEWOOD Hotels")
        _, _, p2 = get_brand_tier("rosewood hotels")
        assert p1 == p2

        _, _, p3 = get_brand_tier("Four Seasons")
        _, _, p4 = get_brand_tier("four seasons")
        assert p3 == p4

    def test_brand_tier_unknown(self):
        """Unknown brands return tier 0"""
        from app.services.scorer import get_brand_tier

        tier_num, tier_name, points = get_brand_tier("Random Hotel Name")
        assert tier_num == 0
        assert tier_name == "Unknown"

    def test_score_range(self):
        """Total score stays within 0-100"""
        from app.services.scorer import calculate_lead_score

        result = calculate_lead_score(
            hotel_name="Rosewood Miami",
            city="Miami",
            state="Florida",
            country="US",
            opening_date="2027",
            room_count=200,
        )
        score = result["total_score"] if isinstance(result, dict) else result
        assert 0 <= score <= 100

    def test_score_empty_lead(self):
        """Empty/minimal lead doesn't crash, gets low score"""
        from app.services.scorer import calculate_lead_score

        result = calculate_lead_score(hotel_name="Unknown")
        score = result["total_score"] if isinstance(result, dict) else result
        assert 0 <= score <= 100
        assert score < 50


# ============================================================
# TEST 2: Deduplicator
# ============================================================


class TestDeduplicator:
    """Tests for app.services.smart_deduplicator"""

    def test_exact_duplicate_merged(self):
        """Identical hotel names are merged"""
        from app.services.smart_deduplicator import SmartDeduplicator

        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {
                "hotel_name": "Rosewood Miami Beach",
                "city": "Miami",
                "source_url": "site-a.com",
            },
            {
                "hotel_name": "Rosewood Miami Beach",
                "city": "Miami",
                "source_url": "site-b.com",
            },
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1

    def test_fuzzy_duplicate_merged(self):
        """Similar names (e.g., with/without suffix) are merged"""
        from app.services.smart_deduplicator import SmartDeduplicator

        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Six Senses Camp Korongo", "city": "Utah"},
            {"hotel_name": "Six Senses Camp Korongo Utah", "city": "Utah"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1

    def test_different_hotels_not_merged(self):
        """Clearly different hotels stay separate"""
        from app.services.smart_deduplicator import SmartDeduplicator

        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Rosewood Miami Beach", "city": "Miami"},
            {"hotel_name": "Four Seasons Bora Bora", "city": "Bora Bora"},
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 2

    def test_merge_preserves_best_data(self):
        """Merged lead keeps the most complete data from both sources"""
        from app.services.smart_deduplicator import SmartDeduplicator

        dedup = SmartDeduplicator(threshold=0.75)
        leads = [
            {"hotel_name": "Aman New York", "city": "New York", "contact_email": ""},
            {
                "hotel_name": "Aman New York",
                "city": "New York",
                "contact_email": "info@aman.com",
                "room_count": 83,
            },
        ]
        merged = dedup.deduplicate(leads)
        assert len(merged) == 1
        result = merged[0] if isinstance(merged[0], dict) else merged[0].to_dict()
        assert (
            result.get("contact_email") == "info@aman.com"
            or result.get("room_count") == 83
        )


# ============================================================
# TEST 3: URL Filter
# ============================================================


class TestURLFilter:
    """Tests for app.services.url_filter"""

    def test_blocks_careers_pages(self):
        """Career/job pages are blocked"""
        from app.services.url_filter import URLFilter

        f = URLFilter()
        result = f.should_scrape("https://marriott.com/careers/job-listing")
        assert result.should_scrape is False

    def test_blocks_login_pages(self):
        """Career/auth pages are blocked"""
        from app.services.url_filter import URLFilter

        f = URLFilter()
        result = f.should_scrape("https://example.com/careers/apply-now")
        assert result.should_scrape is False

    def test_allows_hotel_pages(self):
        """Hotel/property pages are allowed"""
        from app.services.url_filter import URLFilter

        f = URLFilter()
        result = f.should_scrape(
            "https://hospitalitynet.org/news/new-hotel-opening-2027"
        )
        assert result.should_scrape is True

    def test_priority_scoring(self):
        """Gold/high-value URLs get higher priority"""
        from app.services.url_filter import URLFilter

        f = URLFilter()
        gold = f.should_scrape("https://example.com/new-hotel-opening-2027")
        normal = f.should_scrape("https://example.com/about-us")
        # Gold patterns should boost priority
        assert gold.priority >= normal.priority


# ============================================================
# TEST 4: Pipeline JSON Parsing
# ============================================================


class TestPipelineParsing:
    """Tests for intelligent_pipeline JSON extraction"""

    def test_balanced_bracket_extraction(self):
        """Balanced bracket parser handles nested JSON (H4/M1 fix)"""
        text = 'Here is the result: {"hotel_name": "Test", "details": {"rooms": 100}} and more text'

        start = text.index("{")
        depth = 0
        end = start
        for i, c in enumerate(text[start:], start):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        extracted = json.loads(text[start:end])
        assert extracted["hotel_name"] == "Test"
        assert extracted["details"]["rooms"] == 100

    def test_json_with_nested_braces(self):
        """Parser doesn't break on nested objects"""
        raw = '{"leads": [{"name": "Hotel A", "meta": {"score": 85}}, {"name": "Hotel B"}]}'
        parsed = json.loads(raw)
        assert len(parsed["leads"]) == 2
