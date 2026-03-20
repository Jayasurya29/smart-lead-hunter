"""
Smart Lead Hunter — Utility Function Tests
============================================
Pure unit tests for shared helpers. No database or network needed.

Covers:
  - normalize_hotel_name (dedup normalization)
  - escape_like (SQL injection prevention)
  - safe_error (error sanitization)
  - months_to_opening (date parsing)
  - get_timeline_label (lead urgency classification)
  - local_now (timezone correctness)
  - extract_year (lead_factory helper)
"""

from datetime import datetime

import pytest


# ═══════════════════════════════════════════════════════════════════════
# NORMALIZE HOTEL NAME
# ═══════════════════════════════════════════════════════════════════════


class TestNormalizeHotelName:
    """Tests for dedup name normalization."""

    def test_basic_normalization(self):
        from app.services.utils import normalize_hotel_name
        assert normalize_hotel_name("Ritz-Carlton Miami") == "ritzcarlton miami"

    def test_strips_special_chars(self):
        from app.services.utils import normalize_hotel_name
        assert normalize_hotel_name("Four Seasons® Orlando") == "four seasons orlando"

    def test_collapses_whitespace(self):
        from app.services.utils import normalize_hotel_name
        assert normalize_hotel_name("  The  St.  Regis  ") == "the st regis"

    def test_empty_string(self):
        from app.services.utils import normalize_hotel_name
        assert normalize_hotel_name("") == ""

    def test_none_input(self):
        from app.services.utils import normalize_hotel_name
        assert normalize_hotel_name(None) == ""

    def test_unicode_handling(self):
        from app.services.utils import normalize_hotel_name
        result = normalize_hotel_name("Hôtel de Crillon")
        # Should strip accented chars (non-alphanumeric)
        assert "htel" in result or "hotel" in result.replace("ô", "o")

    def test_numeric_preserved(self):
        from app.services.utils import normalize_hotel_name
        result = normalize_hotel_name("1 Hotel South Beach")
        assert "1" in result
        assert "hotel south beach" in result


# ═══════════════════════════════════════════════════════════════════════
# ESCAPE LIKE
# ═══════════════════════════════════════════════════════════════════════


class TestEscapeLike:
    """Tests for SQL LIKE wildcard escaping (prevents injection)."""

    def test_escapes_percent(self):
        from app.shared import escape_like
        assert "%" not in escape_like("50% off").replace("\\%", "")

    def test_escapes_underscore(self):
        from app.shared import escape_like
        assert "_" not in escape_like("test_name").replace("\\_", "")

    def test_plain_text_unchanged(self):
        from app.shared import escape_like
        assert escape_like("Four Seasons") == "Four Seasons"

    def test_escapes_backslash(self):
        from app.shared import escape_like
        result = escape_like("path\\to\\file")
        assert "\\\\" in result

    def test_combined_special_chars(self):
        from app.shared import escape_like
        result = escape_like("100% match_score")
        # Both % and _ should be escaped
        assert "\\%" in result
        assert "\\_" in result


# ═══════════════════════════════════════════════════════════════════════
# SAFE ERROR
# ═══════════════════════════════════════════════════════════════════════


class TestSafeError:
    """Tests for error message sanitization."""

    def test_strips_urls(self):
        from app.shared import safe_error
        msg = safe_error(Exception("Failed at https://api.secret.com/v1/key"))
        assert "https://" not in msg
        assert "[URL removed]" in msg

    def test_redacts_long_tokens(self):
        from app.shared import safe_error
        msg = safe_error(Exception("Key: abcdefghijklmnopqrstuvwx"))
        assert "abcdefghijklmnopqrstuvwx" not in msg
        assert "[REDACTED]" in msg

    def test_truncates_long_messages(self):
        from app.shared import safe_error
        long_msg = "x" * 500
        result = safe_error(Exception(long_msg))
        assert len(result) <= 125  # 120 + "..."

    def test_fallback_on_empty(self):
        from app.shared import safe_error
        result = safe_error(Exception(""))
        assert result == "Operation failed"

    def test_normal_message_preserved(self):
        from app.shared import safe_error
        result = safe_error(Exception("Database timeout"))
        assert "Database timeout" in result


# ═══════════════════════════════════════════════════════════════════════
# MONTHS TO OPENING
# ═══════════════════════════════════════════════════════════════════════


class TestMonthsToOpening:
    """Tests for opening date text → months calculation."""

    def test_future_quarter(self):
        from app.services.utils import months_to_opening
        months = months_to_opening("Q3 2030")
        assert months > 12  # Well in the future

    def test_past_date_negative(self):
        from app.services.utils import months_to_opening
        months = months_to_opening("January 2020")
        assert months < 0

    def test_empty_returns_99(self):
        from app.services.utils import months_to_opening
        assert months_to_opening("") == 99
        assert months_to_opening(None) == 99

    def test_bare_year(self):
        from app.services.utils import months_to_opening
        months = months_to_opening("2030")
        assert months > 0

    def test_dual_year_format(self):
        """'2026/27' should use the later year."""
        from app.services.utils import months_to_opening
        m_single = months_to_opening("2027")
        m_dual = months_to_opening("2026/27")
        # Both should resolve to 2027
        assert abs(m_single - m_dual) <= 1

    def test_month_name_parsing(self):
        from app.services.utils import months_to_opening
        m = months_to_opening("June 2030")
        assert m > 0

    def test_season_words(self):
        from app.services.utils import months_to_opening
        for word in ["early 2030", "spring 2030", "summer 2030", "fall 2030"]:
            m = months_to_opening(word)
            assert m > 0, f"Failed for: {word}"


# ═══════════════════════════════════════════════════════════════════════
# TIMELINE LABELS
# ═══════════════════════════════════════════════════════════════════════


class TestTimelineLabels:
    """Tests for get_timeline_label() — lead urgency classification."""

    def test_empty_is_tbd(self):
        from app.services.utils import get_timeline_label
        assert get_timeline_label("") == "TBD"
        assert get_timeline_label(None) == "TBD"

    def test_past_date_is_expired(self):
        from app.services.utils import get_timeline_label
        assert get_timeline_label("January 2020") == "EXPIRED"

    def test_far_future_is_cool(self):
        from app.services.utils import get_timeline_label
        label = get_timeline_label("Q4 2032")
        assert label == "COOL"

    def test_bare_current_year_is_tbd(self):
        from app.services.utils import get_timeline_label
        current_year = str(datetime.now().year)
        assert get_timeline_label(current_year) == "TBD"

    def test_ambiguous_dual_year_is_tbd(self):
        from app.services.utils import get_timeline_label
        assert get_timeline_label("2026 or 2027") == "TBD"

    def test_valid_labels_only(self):
        """All outputs must be one of the defined labels."""
        from app.services.utils import get_timeline_label
        valid = {"HOT", "URGENT", "WARM", "COOL", "LATE", "EXPIRED", "TBD"}
        test_inputs = [
            "Q1 2025", "Q3 2027", "2030", "June 2020",
            "", None, "2026 or 2027", "summer 2029",
        ]
        for inp in test_inputs:
            label = get_timeline_label(inp)
            assert label in valid, f"Invalid label '{label}' for input '{inp}'"


# ═══════════════════════════════════════════════════════════════════════
# LOCAL NOW
# ═══════════════════════════════════════════════════════════════════════


class TestLocalNow:
    """Tests for timezone-aware local time."""

    def test_returns_aware_datetime(self):
        from app.services.utils import local_now
        now = local_now()
        assert now.tzinfo is not None

    def test_eastern_timezone(self):
        from app.services.utils import local_now, LOCAL_TZ
        now = local_now()
        assert str(now.tzinfo) == str(LOCAL_TZ) or "Eastern" in str(now.tzinfo) or "US/Eastern" in str(now.tzinfo) or "America/New_York" in str(now.tzinfo)


# ═══════════════════════════════════════════════════════════════════════
# EXTRACT YEAR
# ═══════════════════════════════════════════════════════════════════════


class TestExtractYear:
    """Tests for lead_factory.extract_year()."""

    def test_quarter_format(self):
        from app.services.lead_factory import extract_year
        assert extract_year("Q3 2027") == 2027

    def test_month_format(self):
        from app.services.lead_factory import extract_year
        assert extract_year("June 2026") == 2026

    def test_bare_year(self):
        from app.services.lead_factory import extract_year
        assert extract_year("2028") == 2028

    def test_none_input(self):
        from app.services.lead_factory import extract_year
        assert extract_year(None) is None

    def test_no_year_in_string(self):
        from app.services.lead_factory import extract_year
        assert extract_year("sometime soon") is None

    def test_mixed_text(self):
        from app.services.lead_factory import extract_year
        assert extract_year("Expected to open in late 2029") == 2029


# ═══════════════════════════════════════════════════════════════════════
# CHECKED JSON & REQUIRE AJAX
# ═══════════════════════════════════════════════════════════════════════


class TestSecurityHelpers:
    """Tests for CSRF and body size checks."""

    @pytest.mark.asyncio
    async def test_checked_json_rejects_oversized(self):
        from app.shared import checked_json
        from fastapi import HTTPException
        from unittest.mock import AsyncMock

        request = AsyncMock()
        request.body = AsyncMock(return_value=b"x" * 2_000_000)

        with pytest.raises(HTTPException) as exc_info:
            await checked_json(request)
        assert exc_info.value.status_code == 413

    def test_require_ajax_accepts_json_content_type(self):
        from app.shared import require_ajax
        from unittest.mock import MagicMock

        request = MagicMock()
        request.headers = {"content-type": "application/json", "x-requested-with": ""}
        assert require_ajax(request) is True

    def test_require_ajax_rejects_plain_browser(self):
        from app.shared import require_ajax
        from fastapi import HTTPException
        from unittest.mock import MagicMock

        request = MagicMock()
        # Empty x-requested-with, non-JSON content type

        request.headers = {"content-type": "text/html", "x-requested-with": ""}

        with pytest.raises(HTTPException) as exc_info:
            require_ajax(request)
        assert exc_info.value.status_code == 403


# ═══════════════════════════════════════════════════════════════════════
# PENDING CONFIG STORE
# ═══════════════════════════════════════════════════════════════════════


class TestPendingStore:
    """Tests for scrape config pending store with TTL."""

    def test_store_and_pop(self):
        from app.shared import store_pending, pop_pending
        store = {}
        store_pending(store, "abc", {"mode": "full"})
        val = pop_pending(store, "abc")
        assert val == {"mode": "full"}
        # Second pop returns default
        assert pop_pending(store, "abc") is None

    def test_expired_entries_evicted(self):
        import time
        from app.shared import store_pending, _PENDING_TTL
        store = {}
        # Insert entry with fake old timestamp
        store["old-key"] = {"_v": "data", "_t": time.monotonic() - _PENDING_TTL - 10}
        # New store_pending should evict it
        store_pending(store, "new-key", "new-data")
        assert "old-key" not in store
        assert "new-key" in store
