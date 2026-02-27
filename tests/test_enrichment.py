"""
Tests for contact enrichment improvements:
- Title recovery
- Scope scoring (Gemini trust)
- Contact directory parsing (RocketReach, etc.)
- No-title penalty
- Unknown scope penalty
- Corporate title override
- Query diversification
- Title cleanup
"""

import re

from app.services.contact_validator import ContactValidator, ContactScore
from app.config.sap_title_classifier import title_classifier


# =====================================================================
# TITLE CLASSIFIER
# =====================================================================


class TestTitleClassifier:
    def test_empty_title_low_score(self):
        result = title_classifier.classify("")
        assert result.score <= 5, "Empty title should score low"

    def test_resort_manager_classified(self):
        result = title_classifier.classify("Resort Manager")
        assert result.score >= 8

    def test_hotel_manager_classified(self):
        result = title_classifier.classify("Hotel Manager")
        assert result.score >= 8

    def test_operations_manager_classified(self):
        result = title_classifier.classify("Operations Manager")
        assert result.score >= 8

    def test_director_of_housekeeping(self):
        result = title_classifier.classify("Director of Housekeeping")
        assert result.score >= 10

    def test_director_of_food_beverage(self):
        result = title_classifier.classify("Director of Food & Beverage")
        assert result.score >= 8

    def test_purchasing_manager(self):
        result = title_classifier.classify("Purchasing Manager")
        assert result.score >= 10


# =====================================================================
# CONTACT VALIDATOR — SCOPE SCORING
# =====================================================================


class TestScopeScoring:
    def setup_method(self):
        self.validator = ContactValidator()
        self.hotel_name = "Grand Hyatt Grand Cayman Resort & Spa"
        self.brand = "Hyatt"

    def _score(self, contact):
        return self.validator._score_contact(
            contact,
            self.hotel_name,
            set(),
            self.brand,
            None,
            None,
            None,
            None,
        )

    def test_gemini_hotel_specific_trusted(self):
        """Gemini-confirmed hotel_specific should get full bonus."""
        contact = {
            "name": "Himanshu Jethi",
            "title": "Resort Manager",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "hotel_specific",
        }
        score = self._score(contact)
        assert score.scope_tag == "hotel_specific"
        assert score.total_score >= 25

    def test_gemini_hotel_specific_bypasses_corporate_override(self):
        """Gemini-confirmed contacts shouldn't be downgraded even with corporate-sounding titles."""
        contact = {
            "name": "Steven Andre",
            "title": "Managing Director",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "hotel_specific",
        }
        score = self._score(contact)
        assert score.scope_tag == "hotel_specific"
        assert "corporate_at_property_org" not in score.flags

    def test_non_gemini_corporate_title_downgraded(self):
        """Without Gemini confirmation, corporate titles should be downgraded."""
        contact = {
            "name": "John Smith",
            "title": "Vice President of Operations",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "",
        }
        score = self._score(contact)
        assert score.scope_tag == "chain_corporate"

    def test_unknown_scope_penalty(self):
        """Unknown scope contacts should get penalized."""
        contact = {
            "name": "Unknown Person",
            "title": "Housekeeping Manager",
            "organization": "",
            "scope": "",
        }
        score = self._score(contact)
        # Empty org = unknown scope, gets -5 penalty
        assert score.total_score <= 15

    def test_chain_level_scores_lower_than_hotel_specific(self):
        """Chain-level contacts should score lower than hotel-specific."""
        hotel_contact = {
            "name": "Person A",
            "title": "Resort Manager",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "hotel_specific",
        }
        chain_contact = {
            "name": "Person B",
            "title": "Resort Manager",
            "organization": "Hilton Hotels",
            "scope": "",
        }
        hotel_score = self._score(hotel_contact)
        chain_score = self._score(chain_contact)
        assert hotel_score.total_score > chain_score.total_score


# =====================================================================
# CONTACT VALIDATOR — NO-TITLE PENALTY
# =====================================================================


class TestNoTitlePenalty:
    def setup_method(self):
        self.validator = ContactValidator()

    def _score(self, contact):
        return self.validator._score_contact(
            contact,
            "Grand Hyatt Grand Cayman Resort & Spa",
            set(),
            "Hyatt",
            None,
            None,
            None,
            None,
        )

    def test_no_title_gets_penalty(self):
        """Contacts with no title should get penalized."""
        contact = {
            "name": "Frank Cavella",
            "title": "",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "hotel_specific",
        }
        score = self._score(contact)
        assert "no_title" in score.flags
        assert score.total_score < 15

    def test_title_contact_scores_higher(self):
        """Contacts with titles should score much higher than without."""
        with_title = {
            "name": "Himanshu Jethi",
            "title": "Resort Manager",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "hotel_specific",
        }
        without_title = {
            "name": "Frank Cavella",
            "title": "",
            "organization": "Grand Hyatt Grand Cayman Resort & Spa",
            "scope": "hotel_specific",
        }
        score_with = self._score(with_title)
        score_without = self._score(without_title)
        assert score_with.total_score > score_without.total_score + 10


# =====================================================================
# CORPORATE TITLE DETECTION
# =====================================================================


class TestCorporateTitleDetection:
    def setup_method(self):
        self.validator = ContactValidator()

    def test_property_titles_not_corporate(self):
        """Property-level titles should NOT be flagged as corporate."""
        property_titles = [
            "General Manager",
            "Resort Manager",
            "Hotel Manager",
            "Director of Operations",
            "Director of Housekeeping",
            "Director of Rooms",
            "Director of Food & Beverage",
            "Executive Housekeeper",
            "Purchasing Manager",
            "Front Office Manager",
            "Operations Manager",
            "Assistant General Manager",
        ]
        for title in property_titles:
            assert not self.validator._is_corporate_title(
                title
            ), f"{title} should NOT be corporate"

    def test_corporate_titles_detected(self):
        """Corporate/executive titles SHOULD be flagged."""
        corporate_titles = [
            "CEO",
            "Vice President of Operations",
            "Chief Operating Officer",
            "President",
            "Regional Director",
            "SVP Hospitality",
        ]
        for title in corporate_titles:
            assert self.validator._is_corporate_title(
                title
            ), f"{title} SHOULD be corporate"


# =====================================================================
# ORG MATCH
# =====================================================================


class TestOrgMatch:
    def setup_method(self):
        self.validator = ContactValidator()
        self.hotel_name = "Grand Hyatt Grand Cayman Resort & Spa"
        self.brand = "Hyatt"

    def test_exact_hotel_match(self):
        bonus, scope = self.validator._check_org_match(
            "Grand Hyatt Grand Cayman Resort & Spa",
            self.hotel_name,
            self.brand,
            None,
        )
        assert scope == "hotel_specific"
        assert bonus >= 10

    def test_partial_hotel_match(self):
        bonus, scope = self.validator._check_org_match(
            "Grand Hyatt Grand Cayman",
            self.hotel_name,
            self.brand,
            None,
        )
        assert scope == "hotel_specific"

    def test_brand_only_match(self):
        bonus, scope = self.validator._check_org_match(
            "Marriott",
            "W Miami Beach",
            "Marriott",
            None,
        )
        assert scope == "chain_level"

    def test_no_match(self):
        bonus, scope = self.validator._check_org_match(
            "Marriott International",
            self.hotel_name,
            self.brand,
            None,
        )
        assert scope == "unrelated"
        assert bonus < 0

    def test_empty_org(self):
        bonus, scope = self.validator._check_org_match(
            "",
            self.hotel_name,
            self.brand,
            None,
        )
        assert scope == "unknown"


# =====================================================================
# CONTACT DIRECTORY TITLE CLEANUP
# =====================================================================


class TestTitleCleanup:
    """Test the regex patterns used for cleaning contact directory titles."""

    def _clean_title(self, raw_title):
        """Replicate the cleanup logic from contact_enrichment.py."""
        cd_title = raw_title
        if re.search(r"based in|is currently|is a |is the ", cd_title, re.IGNORECASE):
            cd_title = re.sub(
                r"^.*?(?:is\s+)?(?:currently\s+)?(?:a\s+|an\s+|the\s+)?(?=(?:Director|Manager|Head|Chief|Executive|Assistant|General|Resort|Hotel|Front|Purchasing|Operations|Housekeeping|Coordinator|Supervisor)\b)",
                "",
                cd_title,
                flags=re.IGNORECASE,
            ).strip()
        cd_title = re.sub(
            r"\s*[-–—]\s+.*$",
            "",
            cd_title,
        ).strip()
        cd_title = re.sub(
            r"^(?:a|an|the)\s+",
            "",
            cd_title,
            flags=re.IGNORECASE,
        ).strip()
        return cd_title

    def test_strip_based_in_prefix(self):
        raw = "based in Cayman Islands, is currently a Director Of Housekeeping - Kimpton Seafire Resort and Spa"
        assert self._clean_title(raw) == "Director Of Housekeeping"

    def test_normal_title_untouched(self):
        assert (
            self._clean_title("Director of Food & Beverage")
            == "Director of Food & Beverage"
        )

    def test_strip_trailing_org_after_dash(self):
        assert (
            self._clean_title("Purchasing Manager - Hilton Hotels")
            == "Purchasing Manager"
        )

    def test_general_manager_untouched(self):
        # "General" is in keyword list, so it stays
        result = self._clean_title("General Manager")
        assert "Manager" in result

    def test_executive_housekeeper_untouched(self):
        assert self._clean_title("Executive Housekeeper") == "Executive Housekeeper"

    def test_strip_article_prefix(self):
        assert self._clean_title("a Director of Rooms") == "Director of Rooms"

    def test_currently_a_prefix(self):
        raw = "is currently a Hotel Manager at Some Hotel"
        cleaned = self._clean_title(raw)
        assert cleaned.startswith("Hotel Manager")


# =====================================================================
# CONTACT DIRECTORY SNIPPET PARSING
# =====================================================================


class TestContactDirectoryParsing:
    """Test regex patterns for extracting contacts from directory snippets."""

    def _extract(self, snippet):
        """Replicate the extraction logic from contact_enrichment.py."""
        patterns = [
            r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3}),?\s+(?:based in .+?,\s+)?is (?:currently |a |the )?(.*?)\s+at\s+(.+?)(?:\.\s|\s+\w+ brings)",
            r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s*(?:is|,)\s+(?:currently\s+)?(?:a\s+|the\s+)?(.*?)\s+at\s+(.+?)(?:\.|$)",
            r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3}),\s+(.*?)\s+at\s+(.+?)(?:,\s+has|\.\s|$)",
            r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s+·\s+(.*?)\s+·\s+(.+?)(?:\s+·|\.|$)",
            r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s+-\s+(.*?)\s+-\s+(.+?)(?:\.|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, snippet)
            if match:
                return match.group(1), match.group(2), match.group(3).rstrip(".")
        return None, None, None

    def test_rocketreach_standard_format(self):
        snippet = "Himanshu Jethi, based in Cayman Islands, is currently a Resort Manager at Grand Hyatt Grand Cayman Resort & Spa. Himanshu brings experience"
        name, title, org = self._extract(snippet)
        assert name == "Himanshu Jethi"
        assert "Resort Manager" in title
        assert "Grand Hyatt" in org

    def test_simple_is_format(self):
        snippet = "Carlos Noboa is a Director of Housekeeping at The Ritz-Carlton, Grand Cayman."
        name, title, org = self._extract(snippet)
        assert name == "Carlos Noboa"
        assert "Director" in title
        assert "Ritz-Carlton" in org

    def test_comma_format(self):
        snippet = "Dale Dcruz, Director of Food and Beverage at Grand Hyatt Grand Cayman, has been in hospitality."
        name, title, org = self._extract(snippet)
        assert name == "Dale Dcruz"
        assert "Director" in title or "Food" in title

    def test_zoominfo_dot_format(self):
        snippet = "Steven Andre · Director of Operations · Grand Hyatt Grand Cayman"
        name, title, org = self._extract(snippet)
        assert name == "Steven Andre"
        assert "Director" in title
        assert "Grand Hyatt" in org

    def test_dash_format(self):
        snippet = (
            "Rocky Gonzalez - Restaurants General Manager - Grand Hyatt Grand Cayman"
        )
        name, title, org = self._extract(snippet)
        assert name == "Rocky Gonzalez"
        assert "Manager" in title
        assert "Grand Hyatt" in org


# =====================================================================
# QUERY DIVERSIFICATION
# =====================================================================


class TestQueryDiversification:
    """Test hotel name variant generation for search queries."""

    def _get_variants(self, hotel_name, location_str, brand=None):
        short_hotel_name = re.sub(
            r"\s+(?:Resort|Hotel|Spa|Suites?|Residences?|Inn|Lodge|&)+(?:\s+(?:Resort|Hotel|Spa|Suites?|Residences?|Inn|Lodge|&))*\s*$",
            "",
            hotel_name,
            flags=re.IGNORECASE,
        ).strip()
        hotel_variants = [f"{hotel_name} {location_str}"]
        if short_hotel_name and short_hotel_name.lower() != hotel_name.lower():
            hotel_variants.append(f"{short_hotel_name} {location_str}")
        if brand:
            brand_location = f"{brand} {location_str}"
            if brand_location.lower() not in [v.lower() for v in hotel_variants]:
                hotel_variants.append(brand_location)
        return hotel_variants

    def test_resort_spa_stripped(self):
        variants = self._get_variants(
            "Grand Hyatt Grand Cayman Resort & Spa",
            "Grand Cayman, Cayman Islands",
            "Hyatt",
        )
        assert len(variants) == 3
        assert "Grand Hyatt Grand Cayman Grand Cayman, Cayman Islands" in variants
        assert "Hyatt Grand Cayman, Cayman Islands" in variants

    def test_hotel_stripped(self):
        variants = self._get_variants(
            "The Nora Hotel",
            "New York, NY",
            "BD Hotels",
        )
        assert any("Nora" in v and "Hotel" not in v for v in variants)

    def test_no_suffix_no_extra_variant(self):
        variants = self._get_variants(
            "Westin Cocoa Beach",
            "Cocoa Beach, FL",
            "Marriott",
        )
        # No suffix to strip, so only full name + brand
        assert len(variants) == 2

    def test_brand_not_duplicated(self):
        variants = self._get_variants(
            "Hyatt Place Miami",
            "Miami, FL",
            "Hyatt",
        )
        # Brand+location should not duplicate full name+location
        lower_variants = [v.lower() for v in variants]
        assert len(lower_variants) == len(set(lower_variants))


# =====================================================================
# FILTER AND RANK
# =====================================================================


class TestFilterAndRank:
    def setup_method(self):
        self.validator = ContactValidator()

    def test_min_score_filter(self):
        """Contacts below min_score should be filtered out."""
        scored = [
            ContactScore(contact={"name": "Good"}, total_score=25),
            ContactScore(contact={"name": "Bad"}, total_score=3),
        ]
        results = self.validator.filter_and_rank(scored, min_score=5)
        names = [r.contact["name"] for r in results]
        assert "Good" in names
        assert "Bad" not in names

    def test_max_contacts_limit(self):
        """Should not return more than max_contacts."""
        scored = [
            ContactScore(contact={"name": f"Person {i}"}, total_score=20 - i)
            for i in range(10)
        ]
        results = self.validator.filter_and_rank(scored, min_score=5, max_contacts=5)
        assert len(results) <= 5

    def test_all_above_min_score_kept(self):
        """All contacts above min_score should be kept."""
        scored = [
            ContactScore(contact={"name": "Low"}, total_score=10),
            ContactScore(contact={"name": "High"}, total_score=30),
            ContactScore(contact={"name": "Mid"}, total_score=20),
        ]
        results = self.validator.filter_and_rank(scored, min_score=5)
        assert len(results) == 3
