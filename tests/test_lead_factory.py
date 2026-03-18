"""
Smart Lead Hunter — Lead Factory Tests
========================================
Tests for prepare_lead(): the single entry point for all lead creation.

Covers:
  - Junk hotel name detection (article titles, summaries)
  - Brand tier skip logic (tier 5 budget brands)
  - Score range enforcement
  - Opening year extraction
  - Timeline label computation on save
  - Normalization of hotel names
"""



class TestJunkDetection:
    """Article titles / market summaries should be rejected."""

    def test_rejects_forecast_title(self):
        from app.services.lead_factory import prepare_lead
        lead, skip_reason, _ = prepare_lead({
            "hotel_name": "10 New Hotels Opening in 2027",
            "city": "Various",
            "source_url": "https://test.com",
            "source_site": "test.com",
        })
        assert lead is None or skip_reason is not None

    def test_rejects_pipeline_summary(self):
        from app.services.lead_factory import prepare_lead
        lead, skip_reason, _ = prepare_lead({
            "hotel_name": "Hotel Pipeline Forecast for 2027",
            "city": "N/A",
            "source_url": "https://test.com",
            "source_site": "test.com",
        })
        assert lead is None or skip_reason is not None

    def test_rejects_construction_report(self):
        from app.services.lead_factory import prepare_lead
        lead, skip_reason, _ = prepare_lead({
            "hotel_name": "Hotel Construction in Miami",
            "city": "Miami",
            "source_url": "https://test.com",
            "source_site": "test.com",
        })
        assert lead is None or skip_reason is not None

    def test_accepts_real_hotel_name(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, skip_reason, score_result = prepare_lead(sample_lead_dict)
        assert lead is not None
        assert skip_reason is None


class TestBudgetBrandSkip:
    """Tier 5 budget brands should be filtered out."""

    def test_motel_6_skipped(self):
        from app.services.lead_factory import prepare_lead
        lead, skip_reason, _ = prepare_lead({
            "hotel_name": "Motel 6 Downtown",
            "city": "Dallas",
            "state": "Texas",
            "country": "USA",
            "source_url": "https://test.com",
            "source_site": "test.com",
        })
        assert lead is None or skip_reason is not None

    def test_luxury_brand_accepted(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, skip_reason, _ = prepare_lead(sample_lead_dict)
        assert lead is not None
        assert skip_reason is None


class TestScoreIntegration:
    """Score is computed during prepare_lead."""

    def test_score_is_set(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, _, score_result = prepare_lead(sample_lead_dict)
        assert lead is not None
        assert lead.lead_score is not None
        assert 0 <= lead.lead_score <= 100

    def test_score_breakdown_stored(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.score_breakdown is not None
        assert isinstance(lead.score_breakdown, dict)

    def test_brand_tier_set(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.brand_tier is not None
        assert "tier" in lead.brand_tier.lower() or lead.brand_tier == "unknown"


class TestOpeningYearExtraction:
    """Opening year should be extracted from opening_date text."""

    def test_year_extracted_from_quarter(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        sample_lead_dict["opening_date"] = "Q3 2027"
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.opening_year == 2027

    def test_year_extracted_from_bare_year(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        sample_lead_dict["opening_date"] = "2028"
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.opening_year == 2028

    def test_no_year_gives_none(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        sample_lead_dict["opening_date"] = ""
        lead, _, _ = prepare_lead(sample_lead_dict)
        if lead:
            assert lead.opening_year is None


class TestTimelineLabelOnPrepare:
    """Timeline label should be set during prepare_lead."""

    def test_timeline_label_set(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.timeline_label is not None

    def test_timeline_label_valid_value(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        valid = {"HOT", "URGENT", "WARM", "COOL", "LATE", "EXPIRED", "TBD"}
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.timeline_label in valid


class TestNameNormalization:
    """hotel_name_normalized should be set for dedup."""

    def test_normalized_name_set(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert lead.hotel_name_normalized is not None
        assert lead.hotel_name_normalized == lead.hotel_name_normalized.lower()

    def test_normalized_strips_special(self, sample_lead_dict):
        from app.services.lead_factory import prepare_lead
        sample_lead_dict["hotel_name"] = "Ritz-Carlton® Miami"
        lead, _, _ = prepare_lead(sample_lead_dict)
        assert "-" not in lead.hotel_name_normalized
        assert "®" not in lead.hotel_name_normalized
