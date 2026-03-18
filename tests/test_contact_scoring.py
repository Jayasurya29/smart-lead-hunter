"""
Smart Lead Hunter — Title Classifier & Contact Validator Tests
================================================================
Pure unit tests — no database or network needed.

Covers:
  - SAP-trained title classification (BuyerTier mapping)
  - Decision maker detection
  - Contact validation scoring
  - Name collision detection
  - Scope tagging (hotel_specific vs chain_area vs corporate)
  - HR Director inclusion (critical title per memory)
"""



# ═══════════════════════════════════════════════════════════════════════
# TITLE CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════


class TestTitleClassifier:
    """Tests for SAP-trained BuyerTier classification."""

    def test_housekeeping_is_tier1(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("Director of Housekeeping")
        assert result.tier == BuyerTier.TIER1_UNIFORM_DIRECT

    def test_executive_housekeeper_is_tier1(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("Executive Housekeeper")
        assert result.tier == BuyerTier.TIER1_UNIFORM_DIRECT

    def test_purchasing_manager_is_tier2(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("Purchasing Manager")
        assert result.tier == BuyerTier.TIER2_PURCHASING

    def test_general_manager_is_tier3(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("General Manager")
        assert result.tier == BuyerTier.TIER3_GM_OPS

    def test_gm_abbreviation(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("GM")
        assert result.tier in (BuyerTier.TIER3_GM_OPS, BuyerTier.UNKNOWN)

    def test_fb_director_is_tier4(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("Director of Food & Beverage")
        assert result.tier == BuyerTier.TIER4_FB

    def test_hr_director_is_tier5(self):
        """HR Director is a critical uniform purchasing role."""
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("Director of Human Resources")
        assert result.tier == BuyerTier.TIER5_HR

    def test_hr_manager_is_tier5(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("HR Manager")
        assert result.tier == BuyerTier.TIER5_HR

    def test_vp_of_human_resources_is_tier5(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("VP of Human Resources")
        assert result.tier == BuyerTier.TIER5_HR

    def test_marketing_is_irrelevant(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("Marketing Director")
        assert result.tier == BuyerTier.TIER7_IRRELEVANT

    def test_empty_title_is_unknown(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify("")
        assert result.tier == BuyerTier.UNKNOWN

    def test_none_title_is_unknown(self):
        from app.config.sap_title_classifier import title_classifier, BuyerTier
        result = title_classifier.classify(None)
        assert result.tier == BuyerTier.UNKNOWN

    def test_classification_returns_score(self):
        from app.config.sap_title_classifier import title_classifier
        result = title_classifier.classify("Director of Housekeeping")
        assert isinstance(result.score, int)
        assert result.score > 0

    def test_classification_returns_reason(self):
        from app.config.sap_title_classifier import title_classifier
        result = title_classifier.classify("General Manager")
        assert isinstance(result.reason, str)
        assert len(result.reason) > 0

    def test_case_insensitive(self):
        from app.config.sap_title_classifier import title_classifier
        r1 = title_classifier.classify("DIRECTOR OF HOUSEKEEPING")
        r2 = title_classifier.classify("director of housekeeping")
        assert r1.tier == r2.tier

    def test_decision_maker_flag(self):
        from app.config.sap_title_classifier import title_classifier
        result = title_classifier.classify("General Manager")
        assert result.is_decision_maker is True

    def test_non_decision_maker_flag(self):
        from app.config.sap_title_classifier import title_classifier
        result = title_classifier.classify("Accounts Payable Clerk")
        assert result.is_decision_maker is False


# ═══════════════════════════════════════════════════════════════════════
# RESCORE — DECISION MAKER DETECTION
# ═══════════════════════════════════════════════════════════════════════


class TestDecisionMakerDetection:
    """Tests for _is_decision_maker in rescore module."""

    def test_gm_is_decision_maker(self):
        from app.services.rescore import _is_decision_maker
        assert _is_decision_maker("General Manager") is True

    def test_director_housekeeping_is_dm(self):
        from app.services.rescore import _is_decision_maker
        assert _is_decision_maker("Director of Housekeeping") is True

    def test_purchasing_manager_is_dm(self):
        from app.services.rescore import _is_decision_maker
        assert _is_decision_maker("Purchasing Manager") is True

    def test_empty_is_not_dm(self):
        from app.services.rescore import _is_decision_maker
        assert _is_decision_maker("") is False
        assert _is_decision_maker(None) is False


# ═══════════════════════════════════════════════════════════════════════
# RESCORE — SCORE ENRICHED CONTACTS
# ═══════════════════════════════════════════════════════════════════════


class TestScoreEnrichedContacts:
    """Tests for score_enriched_contacts() in rescore module."""

    def test_empty_contacts_returns_zero(self):
        from app.services.rescore import score_enriched_contacts
        result = score_enriched_contacts([])
        assert result["points"] == 0
        assert result["detail"]["total_contacts"] == 0

    def test_hotel_specific_high_confidence_scores_high(self):
        from app.services.rescore import score_enriched_contacts
        from unittest.mock import MagicMock

        contact = MagicMock()
        contact.scope = "hotel_specific"
        contact.confidence = "high"
        contact.title = "General Manager"
        contact.email = "gm@hotel.com"
        contact.name = "John Smith"
        contact.score = 85

        result = score_enriched_contacts([contact])
        assert result["points"] > 0
        assert result["detail"]["hotel_specific"] == 1
        assert result["detail"]["has_email"] is True
        assert result["detail"]["has_decision_maker"] is True

    def test_chain_area_scores_lower(self):
        from app.services.rescore import score_enriched_contacts
        from unittest.mock import MagicMock

        hotel_contact = MagicMock()
        hotel_contact.scope = "hotel_specific"
        hotel_contact.confidence = "high"
        hotel_contact.title = "General Manager"
        hotel_contact.email = "gm@hotel.com"
        hotel_contact.name = "John"
        hotel_contact.score = 85

        chain_contact = MagicMock()
        chain_contact.scope = "chain_area"
        chain_contact.confidence = "medium"
        chain_contact.title = "Regional VP"
        chain_contact.email = "vp@chain.com"
        chain_contact.name = "Jane"
        chain_contact.score = 50

        hotel_result = score_enriched_contacts([hotel_contact])
        chain_result = score_enriched_contacts([chain_contact])

        assert hotel_result["points"] > chain_result["points"]

    def test_no_email_detected(self):
        from app.services.rescore import score_enriched_contacts
        from unittest.mock import MagicMock

        contact = MagicMock()
        contact.scope = "hotel_specific"
        contact.confidence = "medium"
        contact.title = "Director"
        contact.email = ""
        contact.name = "Test"
        contact.score = 50

        result = score_enriched_contacts([contact])
        assert result["detail"]["has_email"] is False


# ═══════════════════════════════════════════════════════════════════════
# CONTACT VALIDATOR
# ═══════════════════════════════════════════════════════════════════════


class TestContactValidator:
    """Tests for ContactValidator.validate_and_score()."""

    def test_valid_hotel_contact_scores_positive(self):
        from app.services.contact_validator import ContactValidator
        validator = ContactValidator()
        contacts = [{
            "name": "John Smith",
            "title": "General Manager",
            "email": "john@rosewood.com",
            "organization": "Rosewood Miami Beach",
        }]
        scored = validator.validate_and_score(
            contacts=contacts,
            hotel_name="Rosewood Miami Beach",
            brand="Rosewood",
            city="Miami",
        )
        assert len(scored) > 0
        assert scored[0].total_score > 0

    def test_empty_contacts_returns_empty(self):
        from app.services.contact_validator import ContactValidator
        validator = ContactValidator()
        scored = validator.validate_and_score(
            contacts=[],
            hotel_name="Test Hotel",
        )
        assert len(scored) == 0

    def test_irrelevant_title_scores_low(self):
        from app.services.contact_validator import ContactValidator
        validator = ContactValidator()

        # Good contact
        good = [{
            "name": "Jane Doe",
            "title": "Director of Housekeeping",
            "email": "jane@hotel.com",
        }]
        # Bad contact
        bad = [{
            "name": "Bob PR",
            "title": "Social Media Coordinator",
            "email": "bob@hotel.com",
        }]

        good_scored = validator.validate_and_score(good, "Test Hotel")
        bad_scored = validator.validate_and_score(bad, "Test Hotel")

        if good_scored and bad_scored:
            assert good_scored[0].total_score > bad_scored[0].total_score

    def test_contacts_sorted_by_score(self):
        from app.services.contact_validator import ContactValidator
        validator = ContactValidator()
        contacts = [
            {"name": "Low Priority", "title": "PR Coordinator"},
            {"name": "High Priority", "title": "Director of Housekeeping",
             "email": "high@hotel.com"},
        ]
        scored = validator.validate_and_score(contacts, "Test Hotel", city="Miami")
        if len(scored) >= 2:
            assert scored[0].total_score >= scored[1].total_score
