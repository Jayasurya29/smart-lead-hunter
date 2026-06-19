"""
Smart Lead Hunter — Extraction-path coverage (added 2026-06-17)
================================================================
Pins the behavior of the contact-extraction code that previously had ZERO
tests, right before the 17-month / 29-mailbox historical backfill. A silent
bug in any of these paths would bake bad data into months of rows, so these
are regression guards on the exact rules the pipeline relies on:

  - name_validation.name_fits_email   (OK / ROLE / MISMATCH gate)
  - inbox_sync._passes_hard_filters   (echo / spam-TLD / role / bulk / consumer)
  - inbox_sync._is_junk_name          (homoglyph / URL-blob / SEO-spam)
  - inbox_sync._parse_vcards          (.vcf business-card parsing)
  - inbox_sync._track_comm_dates      (first / last-inbound / last-outbound)
  - inbox_sync._clean_ingest_name     ("Org - Person" recovery)

These are pure functions — no DB. If the heavy inbox_sync import chain isn't
available in a given environment, the inbox_sync tests skip cleanly (the
name_validation tests still run, since that module is dependency-free).

Run:  pytest tests/test_extraction_2026_06_17.py -v
"""

import datetime as _dt

import pytest

# name_validation is dependency-free → always importable.
from app.services.name_validation import name_fits_email

# inbox_sync drags in google-api / settings; skip the whole inbox_sync block
# if it can't import (matches the repo's "skip if unavailable" convention).
try:
    import app.services.inbox_sync as isync

    _ISYNC = True
except Exception:  # pragma: no cover - env-dependent
    _ISYNC = False

_skip_isync = pytest.mark.skipif(not _ISYNC, reason="inbox_sync import chain unavailable")

UTC = _dt.timezone.utc


# ====================================================================
# name_fits_email — the OK / ROLE / MISMATCH gate
# ====================================================================
class TestNameFitsEmail:
    def test_plausible_personal_name_is_ok(self):
        v = name_fits_email("Diana", "Lopez", "Diana Lopez", "dlopez@kimptonsurfcomber.com")
        assert v.code == "OK"

    def test_role_inbox_rejects_structured_identity(self):
        # A real-looking name on a role address must NOT keep a personal identity.
        v = name_fits_email("John", "Smith", "John Smith", "sales@marriott.com")
        assert v.code == "ROLE"

    def test_frontdesk_role_is_nonpersonal(self):
        # frontdesk@ is a role inbox AND the label itself is non-personal →
        # caller clears the display label too.
        v = name_fits_email("Front", "Desk", "Front Desk", "frontdesk@hotel.com")
        assert v.code == "ROLE"
        assert v.nonpersonal is True

    def test_clean_personal_local_mismatched_name_is_mismatch(self):
        # Two-part clean personal local that shares no token with the scraped
        # name → the name was lifted from the wrong thread; clear it.
        v = name_fits_email("Robert", "Deniro", "Robert Deniro", "patricia.williams@gmail.com")
        assert v.code == "MISMATCH"

    def test_empty_name_is_ok(self):
        # Nothing to validate → OK (nothing gets wiped).
        v = name_fits_email("", "", "", "someone@hotelco.com")
        assert v.code == "OK"


# ====================================================================
# _passes_hard_filters — the email validation gauntlet
# ====================================================================
@_skip_isync
class TestPassesHardFilters:
    MB = "sales@jauniforms.com"

    def test_real_external_contact_passes(self):
        ok, reason = isync._passes_hard_filters("diana@kimptonsurfcomber.com", self.MB)
        assert ok is True
        assert reason == "ok"

    def test_echo_address_rejected(self):
        # Vendor ESP encodes the recipient domain as the localpart.
        ok, reason = isync._passes_hard_filters("jauniforms.com@em5486.sanmar.com", self.MB)
        assert ok is False
        assert reason == "echo_address"

    def test_spam_tld_rejected(self):
        ok, reason = isync._passes_hard_filters("guy@meeteoccrew.cfd", self.MB)
        assert ok is False
        assert reason == "spam_tld"

    def test_role_address_rejected(self):
        ok, reason = isync._passes_hard_filters("orders@hotel.com", self.MB)
        assert ok is False
        assert reason == "role_address"

    def test_own_company_rejected(self):
        ok, reason = isync._passes_hard_filters("gm@jauniforms.com", self.MB)
        assert ok is False
        assert reason == "own_company"

    def test_bulk_subdomain_rejected(self):
        # Numbered ESP label (em5875.x.com) must be caught even on a real brand.
        ok, reason = isync._passes_hard_filters("x@em5875.marriott.com", self.MB)
        assert ok is False
        assert reason == "bulk_subdomain"


# ====================================================================
# _is_junk_name — name-based junk the domain filters can't see
# ====================================================================
@_skip_isync
class TestIsJunkName:
    def test_clean_name_not_junk(self):
        assert isync._is_junk_name({"display_name": "Diana Lopez"}) is False

    def test_url_blob_is_junk(self):
        assert isync._is_junk_name({"display_name": "http://evil.com/track/0?context=x"}) is True

    def test_seo_spam_is_junk(self):
        assert isync._is_junk_name({"display_name": "Outreach via rankmint"}) is True

    def test_cyrillic_homoglyph_is_junk(self):
        # Leading char is Cyrillic 'А' (U+0410), not Latin 'A'.
        assert isync._is_junk_name({"display_name": "\u0410pple Support"}) is True

    def test_empty_name_not_junk(self):
        assert isync._is_junk_name({"display_name": ""}) is False


# ====================================================================
# _parse_vcards — .vcf business-card parsing
# ====================================================================
@_skip_isync
class TestParseVcards:
    VCF = (
        "BEGIN:VCARD\r\n"
        "VERSION:3.0\r\n"
        "FN:Carlos Mendez\r\n"
        "N:Mendez;Carlos\r\n"
        "ORG:Conrad Miami\r\n"
        "TITLE:Director of Procurement\r\n"
        "EMAIL:cmendez@conradmiami.com\r\n"
        "TEL;TYPE=CELL:305-555-0142\r\n"
        "END:VCARD\r\n"
    )

    def test_parses_full_card(self):
        cards = isync._parse_vcards(self.VCF)
        assert len(cards) == 1
        c = cards[0]
        assert c["email"] == "cmendez@conradmiami.com"
        assert c["first_name"] == "Carlos"
        assert c["last_name"] == "Mendez"
        assert c["organization"] == "Conrad Miami"
        assert c["title"] == "Director of Procurement"
        # CELL → mobile, not phone
        assert c.get("mobile") == "305-555-0142"

    def test_card_without_email_dropped(self):
        # email is the dedup key — a card with no email cannot be persisted.
        out = isync._parse_vcards("BEGIN:VCARD\r\nFN:No Email\r\nEND:VCARD\r\n")
        assert out == []

    def test_email_lowercased(self):
        out = isync._parse_vcards(
            "BEGIN:VCARD\r\nFN:A B\r\nEMAIL:Mixed.Case@Hotel.COM\r\nEND:VCARD\r\n"
        )
        assert out[0]["email"] == "mixed.case@hotel.com"


# ====================================================================
# _track_comm_dates — real relationship timeline
# ====================================================================
@_skip_isync
class TestTrackCommDates:
    def test_inbound_sets_first_and_last_inbound(self):
        entry = {}
        d = _dt.datetime(2025, 6, 1, tzinfo=UTC)
        isync._track_comm_dates(entry, d, inbound=True)
        assert entry["first_message_at"] == d
        assert entry["last_inbound_at"] == d
        assert entry.get("last_outbound_at") is None

    def test_outbound_does_not_touch_inbound(self):
        entry = {}
        din = _dt.datetime(2025, 6, 1, tzinfo=UTC)
        dout = _dt.datetime(2026, 1, 1, tzinfo=UTC)
        isync._track_comm_dates(entry, din, inbound=True)
        isync._track_comm_dates(entry, dout, inbound=False)
        assert entry["last_inbound_at"] == din
        assert entry["last_outbound_at"] == dout
        # first_message_at stays the earliest seen
        assert entry["first_message_at"] == din

    def test_earlier_message_lowers_first_only(self):
        entry = {}
        d1 = _dt.datetime(2025, 6, 1, tzinfo=UTC)
        d0 = _dt.datetime(2025, 1, 1, tzinfo=UTC)
        isync._track_comm_dates(entry, d1, inbound=True)
        isync._track_comm_dates(entry, d0, inbound=True)
        assert entry["first_message_at"] == d0
        # last_inbound stays the latest (d1), not the just-seen-earlier d0
        assert entry["last_inbound_at"] == d1


# ====================================================================
# _clean_ingest_name — "Org - Person" recovery
# ====================================================================
@_skip_isync
class TestCleanIngestName:
    def test_recovers_person_from_org_dash_name(self):
        out = isync._clean_ingest_name(
            "Towne Park - Cindy Wetzel", "Towne Park", "cwetzel@townepark.com"
        )
        assert out == "Cindy Wetzel"

    def test_plain_name_passthrough(self):
        out = isync._clean_ingest_name("Cindy Wetzel", "Towne Park", "cwetzel@townepark.com")
        assert out == "Cindy Wetzel"
