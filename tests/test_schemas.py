"""
Smart Lead Hunter — Schema & Model Tests
==========================================
Tests for Pydantic schemas (request/response validation) and
SQLAlchemy model methods.

Covers:
  - LeadCreate / LeadUpdate / LeadResponse validation
  - SourceCreate validation
  - Input validation: empty names, bad emails, score ranges, room counts,
    status enums, brand tiers, URL formats, string lengths, whitespace
  - Auth schema validation: email format, name required, OTP format, role enum
  - PotentialLead.to_dict()
  - User.to_dict()
  - LeadContact.to_dict()
  - StatsResponse shape
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError


# ═══════════════════════════════════════════════════════════════════════
# LEAD SCHEMAS — BASIC
# ═══════════════════════════════════════════════════════════════════════


class TestLeadCreateSchema:
    """Tests for LeadCreate validation."""

    def test_valid_minimal(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test Hotel")
        assert lead.hotel_name == "Test Hotel"
        assert lead.country == "USA"  # Default

    def test_valid_full(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(
            hotel_name="Rosewood Miami",
            city="Miami",
            state="Florida",
            country="USA",
            opening_date="Q3 2027",
            room_count=200,
            brand="Rosewood",
            lead_score=85,
        )
        assert lead.room_count == 200

    def test_missing_hotel_name_fails(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError):
            LeadCreate(city="Miami")

    def test_defaults_applied(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test")
        assert lead.contact_email is None
        assert lead.room_count is None
        assert lead.source_site is None


class TestLeadUpdateSchema:
    """Tests for LeadUpdate (all fields optional)."""

    def test_empty_update(self):
        from app.schemas import LeadUpdate
        update = LeadUpdate()
        assert update.model_dump(exclude_unset=True) == {}

    def test_partial_update(self):
        from app.schemas import LeadUpdate
        update = LeadUpdate(status="approved", notes="Great lead")
        data = update.model_dump(exclude_unset=True)
        assert data == {"status": "approved", "notes": "Great lead"}

    def test_score_field(self):
        from app.schemas import LeadUpdate
        update = LeadUpdate(lead_score=92)
        assert update.lead_score == 92


class TestLeadResponseSchema:
    """Tests for LeadResponse serialization."""

    def test_from_attributes(self):
        from app.schemas import LeadResponse
        from unittest.mock import MagicMock
        lead = MagicMock()
        lead.id = 1
        lead.hotel_name = "Test Hotel"
        lead.status = "new"
        lead.lead_score = 75
        lead.city = "Miami"
        lead.state = "Florida"
        lead.country = "USA"
        lead.created_at = datetime.now(timezone.utc)
        lead.updated_at = None
        lead.score_breakdown = {"brand": 20}
        lead.source_url = "https://test.com"
        lead.source_site = "test.com"
        lead.source_urls = ["https://test.com"]
        lead.source_extractions = {"https://test.com": {"city": "Miami"}}
        lead.contact_email = None
        lead.contact_phone = None
        lead.contact_name = None
        lead.contact_title = None
        lead.opening_date = "Q3 2027"
        lead.room_count = 200
        lead.hotel_type = None
        lead.brand = "Test"
        lead.brand_tier = "tier2_luxury"
        lead.location_type = "florida"
        lead.management_company = "Test Management"
        lead.developer = "Test Developer"
        lead.owner = "Test Owner"
        lead.hotel_website = None
        lead.description = None
        lead.notes = None

        resp = LeadResponse.model_validate(lead)
        assert resp.id == 1
        assert resp.hotel_name == "Test Hotel"
        assert resp.status == "new"


class TestLeadListResponseSchema:
    """Tests for paginated lead list."""

    def test_pagination_fields(self):
        from app.schemas import LeadListResponse
        resp = LeadListResponse(
            leads=[], total=0, page=1, per_page=20, pages=1
        )
        assert resp.total == 0
        assert resp.pages == 1


# ═══════════════════════════════════════════════════════════════════════
# LEAD SCHEMAS — INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════════════


class TestLeadNameValidation:
    """hotel_name must be non-empty after stripping."""

    def test_empty_string_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="empty"):
            LeadCreate(hotel_name="")

    def test_whitespace_only_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="empty"):
            LeadCreate(hotel_name="   ")

    def test_whitespace_stripped(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="  Rosewood Miami  ")
        assert lead.hotel_name == "Rosewood Miami"

    def test_max_length_enforced(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="255"):
            LeadCreate(hotel_name="X" * 300)


class TestEmailValidation:
    """Email fields must be valid format when provided."""

    def test_valid_email_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", contact_email="john@hotel.com")
        assert lead.contact_email == "john@hotel.com"

    def test_invalid_email_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="email"):
            LeadCreate(hotel_name="Test", contact_email="not-an-email")

    def test_email_missing_domain_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="email"):
            LeadCreate(hotel_name="Test", contact_email="john@")

    def test_email_missing_at_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="email"):
            LeadCreate(hotel_name="Test", contact_email="john.hotel.com")

    def test_none_email_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", contact_email=None)
        assert lead.contact_email is None

    def test_empty_email_becomes_none(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", contact_email="")
        assert lead.contact_email is None

    def test_email_lowercased(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", contact_email="John@Hotel.COM")
        assert lead.contact_email == "john@hotel.com"

    def test_update_email_validated(self):
        from app.schemas import LeadUpdate
        with pytest.raises(ValidationError, match="email"):
            LeadUpdate(contact_email="garbage")


class TestScoreValidation:
    """Lead score must be 0-100."""

    def test_score_101_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="100"):
            LeadCreate(hotel_name="Test", lead_score=101)

    def test_score_negative_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="0"):
            LeadCreate(hotel_name="Test", lead_score=-1)

    def test_score_0_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", lead_score=0)
        assert lead.lead_score == 0

    def test_score_100_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", lead_score=100)
        assert lead.lead_score == 100

    def test_score_none_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", lead_score=None)
        assert lead.lead_score is None

    def test_update_score_validated(self):
        from app.schemas import LeadUpdate
        with pytest.raises(ValidationError):
            LeadUpdate(lead_score=999)


class TestRoomCountValidation:
    """Room count must be non-negative."""

    def test_negative_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="negative"):
            LeadCreate(hotel_name="Test", room_count=-5)

    def test_zero_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", room_count=0)
        assert lead.room_count == 0

    def test_positive_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", room_count=500)
        assert lead.room_count == 500


class TestStatusValidation:
    """LeadUpdate.status must be from valid set."""

    def test_valid_statuses_accepted(self):
        from app.schemas import LeadUpdate, VALID_STATUSES
        for status in VALID_STATUSES:
            update = LeadUpdate(status=status)
            assert update.status == status

    def test_invalid_status_rejected(self):
        from app.schemas import LeadUpdate
        with pytest.raises(ValidationError, match="status"):
            LeadUpdate(status="super_approved")

    def test_none_status_accepted(self):
        from app.schemas import LeadUpdate
        update = LeadUpdate(status=None)
        assert update.status is None


class TestBrandTierValidation:
    """brand_tier must be from known tiers."""

    def test_valid_tiers_accepted(self):
        from app.schemas import LeadCreate
        for tier in ["tier1_ultra_luxury", "tier2_luxury", "tier3_upper_upscale",
                      "tier4_upscale", "tier5_skip", "unknown"]:
            lead = LeadCreate(hotel_name="Test", brand_tier=tier)
            assert lead.brand_tier == tier

    def test_invalid_tier_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="brand tier"):
            LeadCreate(hotel_name="Test", brand_tier="platinum_elite")


class TestURLValidation:
    """URLs must start with http:// or https://."""

    def test_valid_url_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", source_url="https://test.com/article")
        assert lead.source_url == "https://test.com/article"

    def test_http_url_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", hotel_website="http://hotel.com")
        assert lead.hotel_website == "http://hotel.com"

    def test_no_protocol_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="URL"):
            LeadCreate(hotel_name="Test", source_url="test.com/article")

    def test_ftp_rejected(self):
        from app.schemas import LeadCreate
        with pytest.raises(ValidationError, match="URL"):
            LeadCreate(hotel_name="Test", hotel_website="ftp://files.hotel.com")

    def test_none_url_accepted(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", source_url=None)
        assert lead.source_url is None


class TestStringStripping:
    """All string fields should be stripped of whitespace."""

    def test_city_stripped(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", city="  Miami  ")
        assert lead.city == "Miami"

    def test_empty_string_becomes_none(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", city="", state="  ", brand="")
        assert lead.city is None
        assert lead.state is None
        assert lead.brand is None

    def test_notes_length_capped(self):
        from app.schemas import LeadCreate
        lead = LeadCreate(hotel_name="Test", notes="x" * 10000)
        assert len(lead.notes) <= 5000


# ═══════════════════════════════════════════════════════════════════════
# SOURCE SCHEMAS — INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════════════


class TestSourceCreateSchema:
    """Tests for SourceCreate validation."""

    def test_valid_source(self):
        from app.schemas import SourceCreate
        src = SourceCreate(
            name="Hotel News",
            base_url="https://hotelnews.com",
        )
        assert src.source_type == "aggregator"
        assert src.priority == 5
        assert src.is_active is True

    def test_missing_url_fails(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError):
            SourceCreate(name="Test")

    def test_missing_name_fails(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError):
            SourceCreate(base_url="https://test.com")

    def test_empty_name_rejected(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError, match="empty"):
            SourceCreate(name="", base_url="https://test.com")

    def test_bad_url_rejected(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError, match="URL"):
            SourceCreate(name="Test", base_url="not-a-url")

    def test_invalid_source_type_rejected(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError, match="source type"):
            SourceCreate(name="Test", base_url="https://test.com", source_type="xyz")

    def test_priority_out_of_range_rejected(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError, match="1 and 10"):
            SourceCreate(name="Test", base_url="https://test.com", priority=99)

    def test_priority_zero_rejected(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError):
            SourceCreate(name="Test", base_url="https://test.com", priority=0)

    def test_invalid_frequency_rejected(self):
        from app.schemas import SourceCreate
        with pytest.raises(ValidationError, match="frequency"):
            SourceCreate(name="Test", base_url="https://test.com", scrape_frequency="hourly")

    def test_entry_urls_validated(self):
        from app.schemas import SourceCreate
        src = SourceCreate(
            name="Test",
            base_url="https://test.com",
            entry_urls=["https://test.com/page1", "garbage", "https://test.com/page2"],
        )
        # Invalid URLs should be filtered out
        assert len(src.entry_urls) == 2


# ═══════════════════════════════════════════════════════════════════════
# AUTH SCHEMAS — INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════════════


class TestAuthSchemaValidation:
    """Tests for auth request schema validation."""

    def test_login_bad_email_rejected(self):
        from app.routes.auth import LoginRequest
        with pytest.raises(ValidationError, match="email"):
            LoginRequest(email="garbage", password="test")

    def test_login_empty_password_rejected(self):
        from app.routes.auth import LoginRequest
        with pytest.raises(ValidationError, match="empty"):
            LoginRequest(email="test@jauniforms.com", password="")

    def test_register_empty_first_name_rejected(self):
        from app.routes.auth import RegisterRequest
        with pytest.raises(ValidationError, match="empty"):
            RegisterRequest(
                first_name="", last_name="User",
                email="t@jauniforms.com", password="Test1234",
            )

    def test_register_long_name_rejected(self):
        from app.routes.auth import RegisterRequest
        with pytest.raises(ValidationError, match="100"):
            RegisterRequest(
                first_name="X" * 150, last_name="User",
                email="t@jauniforms.com", password="Test1234",
            )

    def test_register_bad_role_rejected(self):
        from app.routes.auth import RegisterRequest
        with pytest.raises(ValidationError, match="role"):
            RegisterRequest(
                first_name="T", last_name="U",
                email="t@jauniforms.com", password="Test1234", role="superadmin",
            )

    def test_verify_code_not_digits_rejected(self):
        from app.routes.auth import VerifyRequest
        with pytest.raises(ValidationError, match="6 digits"):
            VerifyRequest(email="t@jauniforms.com", code="abcdef")

    def test_verify_code_wrong_length_rejected(self):
        from app.routes.auth import VerifyRequest
        with pytest.raises(ValidationError, match="6 digits"):
            VerifyRequest(email="t@jauniforms.com", code="123")

    def test_verify_code_valid(self):
        from app.routes.auth import VerifyRequest
        v = VerifyRequest(email="t@jauniforms.com", code="123456")
        assert v.code == "123456"

    def test_resend_bad_email_rejected(self):
        from app.routes.auth import ResendRequest
        with pytest.raises(ValidationError, match="email"):
            ResendRequest(email="not-email")

    def test_email_stripped_and_lowered(self):
        from app.routes.auth import LoginRequest
        req = LoginRequest(email="  Test@JaUniforms.com  ", password="test")
        assert req.email == "test@jauniforms.com"

    def test_name_stripped(self):
        from app.routes.auth import RegisterRequest
        req = RegisterRequest(
            first_name="  Jay  ", last_name="  Test  ",
            email="j@jauniforms.com", password="Test1234",
        )
        assert req.first_name == "Jay"
        assert req.last_name == "Test"


# ═══════════════════════════════════════════════════════════════════════
# STATS RESPONSE
# ═══════════════════════════════════════════════════════════════════════


class TestStatsResponseSchema:
    """Tests for StatsResponse shape."""

    def test_all_fields_present(self):
        from app.schemas import StatsResponse
        stats = StatsResponse(
            total_leads=100, new_leads=50, approved_leads=30,
            pending_leads=5, rejected_leads=15, hot_leads=10,
            urgent_leads=8, warm_leads=12, cool_leads=20,
            total_sources=25, active_sources=20, healthy_sources=18,
            leads_today=3, leads_this_week=15,
        )
        assert stats.total_leads == 100
        assert stats.hot_leads == 10


# ═══════════════════════════════════════════════════════════════════════
# MODEL TO_DICT
# ═══════════════════════════════════════════════════════════════════════


class TestUserModel:
    """Tests for User model methods."""

    def test_to_dict_excludes_password(self):
        from app.models.user import User
        user = User(
            id=1, first_name="Jay", last_name="Test",
            email="jay@jauniforms.com", password_hash="$2b$12$hashed",
            role="admin", is_active=True,
        )
        d = user.to_dict()
        assert "password_hash" not in d
        assert "password" not in d
        assert d["email"] == "jay@jauniforms.com"

    def test_to_dict_has_required_fields(self):
        from app.models.user import User
        user = User(
            id=1, first_name="Test", last_name="User",
            email="test@jauniforms.com", password_hash="hash", role="sales",
        )
        d = user.to_dict()
        required = {"id", "first_name", "last_name", "email", "role", "is_active"}
        assert required.issubset(set(d.keys()))


class TestLeadContactModel:
    """Tests for LeadContact model methods."""

    def test_to_dict_shape(self):
        from app.models.lead_contact import LeadContact
        contact = LeadContact(
            id=1, lead_id=10, name="Jane Smith",
            title="Director of Housekeeping", email="jane@hotel.com",
            scope="hotel_specific", confidence="high",
            score=85, is_saved=False, is_primary=True,
        )
        d = contact.to_dict()
        assert d["name"] == "Jane Smith"
        assert d["is_primary"] is True
        assert d["score"] == 85

    def test_repr_includes_name(self):
        from app.models.lead_contact import LeadContact
        contact = LeadContact(name="John Doe", title="GM", is_saved=True)
        repr_str = repr(contact)
        assert "John Doe" in repr_str
        assert "SAVED" in repr_str


class TestPotentialLeadModel:
    """Tests for PotentialLead model methods."""

    def test_to_dict_shape(self):
        from app.models.potential_lead import PotentialLead
        lead = PotentialLead(
            id=1, hotel_name="Test Hotel", brand="Test Brand",
            city="Miami", state="Florida", country="USA",
            status="new", lead_score=75,
        )
        d = lead.to_dict()
        assert d["hotel_name"] == "Test Hotel"
        assert d["status"] == "new"
        assert "id" in d

    def test_to_dict_handles_none_timestamps(self):
        from app.models.potential_lead import PotentialLead
        lead = PotentialLead(id=1, hotel_name="Test", status="new")
        d = lead.to_dict()
        assert d["created_at"] is None or isinstance(d["created_at"], str)

    def test_repr_useful(self):
        from app.models.potential_lead import PotentialLead
        lead = PotentialLead(
            id=42, hotel_name="Rosewood Miami", lead_score=85, status="approved"
        )
        r = repr(lead)
        assert "Rosewood Miami" in r
        assert "85" in r
