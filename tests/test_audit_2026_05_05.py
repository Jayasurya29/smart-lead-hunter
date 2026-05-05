"""
Smart Lead Hunter — Phase 4 Regression Tests
==============================================
Covers the lifecycle invariants and behavioral contracts that the
2026-05-05 audit (bugs #1-#38) restored.

These tests are intentionally PURE-PYTHON / NO-DB so they can run in any
environment without a live PostgreSQL. Behavioral tests against real
sessions live in test_lifecycle_db.py (skipped unless TEST_DATABASE_URL
is set).

Run:
    DATABASE_URL=postgresql+asyncpg://test:test@localhost/test \\
    python -m pytest tests/test_audit_2026_05_05.py -v
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test"
)
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-testing-only-32chars!")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_AUTH_KEY", "test-api-key-12345")


# ─────────────────────────────────────────────────────────────────────────────
# Bug #1 — prepare_lead must NEVER create status='expired' rows.
# Even if the timeline is EXPIRED, the row is built with status='new' and
# save_lead_to_db's direct-to-existing branch graduates it.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug1_PrepareLeadNeverCreatesExpired:
    def test_expired_timeline_still_status_new(self):
        from app.services.lead_factory import prepare_lead

        # An opening_date 1 month in the past — clearly EXPIRED bucket.
        lead, skip_reason, _ = prepare_lead(
            {
                "hotel_name": "Past Opening Hotel",
                "city": "Miami",
                "state": "FL",
                "country": "USA",
                "opening_date": "January 2020",
                "source_url": "https://test.com",
                "source_site": "test.com",
            }
        )
        if lead is None:
            pytest.skip(f"prepare_lead skipped (junk filter): {skip_reason}")
        # The whole point of bug #1: never persist status='expired' on a
        # potential_leads row. Status must always be 'new' from prepare_lead;
        # the EXPIRED-graduation logic lives in save_lead_to_db.
        assert lead.status == "new", (
            f"prepare_lead created lead with status={lead.status!r} for "
            f"EXPIRED timeline — bug #1 regression"
        )

    def test_future_opening_status_new(self):
        from app.services.lead_factory import prepare_lead

        lead, skip_reason, _ = prepare_lead(
            {
                "hotel_name": "Future Opening Hotel",
                "city": "Miami",
                "state": "FL",
                "country": "USA",
                "opening_date": "December 2027",
                "source_url": "https://test.com",
                "source_site": "test.com",
            }
        )
        if lead is None:
            pytest.skip(f"prepare_lead skipped: {skip_reason}")
        assert lead.status == "new"


# ─────────────────────────────────────────────────────────────────────────────
# Bug #7 — canonical 5-tier brand system. Validators must reject the
# non-canonical 7-tier strings; tier_points_map must score the 5 canonical
# values; insightly tier_display only maps 4 brand-named keys.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug7_CanonicalFiveTier:
    def test_schemas_valid_brand_tiers_is_canonical_five(self):
        from app.schemas import VALID_BRAND_TIERS

        canonical = {
            "tier1_ultra_luxury",
            "tier2_luxury",
            "tier3_upper_upscale",
            "tier4_upscale",
            "tier5_skip",
        }
        # Every canonical value must be accepted
        for tier in canonical:
            assert tier in VALID_BRAND_TIERS, (
                f"VALID_BRAND_TIERS missing canonical {tier!r}"
            )
        # Non-canonical values must NOT be accepted
        for tier in (
            "tier5_upper_midscale",
            "tier6_midscale",
            "tier7_economy",
        ):
            assert tier not in VALID_BRAND_TIERS, (
                f"VALID_BRAND_TIERS still contains non-canonical {tier!r} — "
                f"5-tier rollback regression"
            )

    def test_existing_hotel_scorer_normalizes_non_canonical(self):
        from app.services.existing_hotel_scorer import _score_brand_tier

        # Canonical values score normally
        pts, label, warning = _score_brand_tier("tier1_ultra_luxury")
        assert pts == 40 and warning is False

        # Canonical out-of-scope (tier5_skip)
        pts, label, warning = _score_brand_tier("tier5_skip")
        assert pts == 5 and warning is True

        # Non-canonical values normalize to tier5_skip with warning
        for bad in ("tier5_upper_midscale", "tier6_midscale", "tier7_economy"):
            pts, label, warning = _score_brand_tier(bad)
            assert pts == 5, f"{bad!r} should score 5 (tier5_skip)"
            assert warning is True, (
                f"{bad!r} should fire out_of_scope_warning"
            )
            assert "tier5_skip" in label, (
                f"label should show normalization: got {label!r}"
            )

    def test_insightly_tier_display_only_canonical(self):
        # Smoke-check the dict keys via source inspection (no DB needed)
        import app.services.insightly as insightly_mod
        import inspect

        src = inspect.getsource(insightly_mod)
        assert "tier5_upper_midscale" not in src.replace(
            "non-canonical tier (tier5_upper_midscale", ""
        ), "insightly.py still references non-canonical tier outside comments"


# ─────────────────────────────────────────────────────────────────────────────
# Bug #9 — _find_existing_hotel_match requires city AND state, not OR.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug9_StrictExistingHotelMatch:
    def test_city_state_match_logic_in_source(self):
        # Source-level guard against regression. We can't exercise this
        # without a live DB, but we can confirm the OR fallback was
        # removed and the AND check is in place.
        import inspect
        from app.services.lead_transfer import _find_existing_hotel_match

        src = inspect.getsource(_find_existing_hotel_match)
        # The dangerous OR logic must be gone
        assert "country.lower()" not in src or "country" not in src.split(
            "Strict match"
        )[0], "country fallback still in loose match"
        # The single-candidate fallback must be gone
        assert "len(candidates) == 1" not in src, (
            "single-candidate fallback still present (bug #9 regression)"
        )
        # AND-based match must be present
        assert "lead_city == cand_city" in src
        assert "lead_state == cand_state" in src


# ─────────────────────────────────────────────────────────────────────────────
# Bug #19 — normalize_person_name strips honorifics, keeps hyphens/apostrophes.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug19_NormalizePersonName:
    def test_strips_honorifics(self):
        from app.services.utils import normalize_person_name

        assert normalize_person_name("Mr. John Smith") == "john smith"
        assert normalize_person_name("Dr. Jane Doe") == "jane doe"
        assert normalize_person_name("Mrs. Sarah Johnson") == "sarah johnson"
        assert normalize_person_name("Ms. Lee") == "lee"
        assert normalize_person_name("Prof. Smith") == "smith"

    def test_keeps_hyphens_and_apostrophes(self):
        from app.services.utils import normalize_person_name

        assert normalize_person_name("Mary-Anne Smith") == "mary-anne smith"
        assert normalize_person_name("Sean O'Brien") == "sean o'brien"

    def test_strips_diacritics(self):
        from app.services.utils import normalize_person_name

        assert normalize_person_name("Élise Martin") == "elise martin"
        assert normalize_person_name("José García") == "jose garcia"

    def test_empty_input(self):
        from app.services.utils import normalize_person_name

        assert normalize_person_name("") == ""
        assert normalize_person_name(None) == ""

    def test_collapses_whitespace(self):
        from app.services.utils import normalize_person_name

        assert normalize_person_name("  John   Smith  ") == "john smith"

    def test_does_not_strip_brand_words(self):
        # Critical: unlike normalize_hotel_name, this must NOT strip
        # "Inn"/"Hotel"/"Resort" suffixes from a person's name.
        from app.services.utils import normalize_person_name

        # "Bob Inn" should stay "bob inn" — both tokens preserved
        result = normalize_person_name("Bob Inn")
        assert "inn" in result, (
            f"normalize_person_name stripped 'Inn' from {result!r} — "
            "bug #19 regression (using hotel normalizer for person name)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bug #21 — _SSE_PATHS membership covers smart-fill-stream / enrich-stream
# / outreach generate-stream via prefix matching.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug21_SSEPathsCoverage:
    def test_dashboard_paths_in_set(self):
        from app.main import _SSE_PATHS

        assert "/api/dashboard/scrape/stream" in _SSE_PATHS
        assert "/api/dashboard/extract-url/stream" in _SSE_PATHS
        assert "/api/dashboard/discovery/stream" in _SSE_PATHS

    def test_outreach_path_in_set(self):
        from app.main import _SSE_PATHS

        assert "/api/outreach/generate-stream" in _SSE_PATHS

    def test_smart_fill_stream_prefix_match(self):
        from app.main import _SSE_PATHS

        # Parameterized routes — must match by suffix prefix
        assert "/api/leads/123/smart-fill-stream" in _SSE_PATHS
        assert "/api/existing-hotels/456/smart-fill-stream" in _SSE_PATHS

    def test_enrich_stream_prefix_match(self):
        from app.main import _SSE_PATHS

        assert "/api/existing-hotels/789/enrich-stream" in _SSE_PATHS

    def test_unrelated_paths_not_in_set(self):
        from app.main import _SSE_PATHS

        assert "/api/leads" not in _SSE_PATHS
        assert "/api/dashboard" not in _SSE_PATHS
        assert "/" not in _SSE_PATHS
        assert "" not in _SSE_PATHS


# ─────────────────────────────────────────────────────────────────────────────
# Bug #22 — /stats only in EXCLUDE_PREFIXES, not in PROTECTED_PREFIXES.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug22_StatsPolicySingleSource:
    def test_stats_in_exclude_only(self):
        from app.middleware.auth import APIKeyMiddleware

        assert "/stats" in APIKeyMiddleware.EXCLUDE_PREFIXES, (
            "/stats removed from EXCLUDE — behavior change without intent"
        )
        assert "/stats" not in APIKeyMiddleware.PROTECTED_PREFIXES, (
            "/stats still in both EXCLUDE and PROTECTED — bug #22 regression"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bug #5 — existing_hotels_parity router is registered in main.py.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug5_ParityRouterRegistered:
    def test_parity_router_imported(self):
        # Confirm main.py imports the parity router
        import inspect
        import app.main as main_mod

        src = inspect.getsource(main_mod)
        assert "existing_hotels_parity" in src, (
            "main.py no longer imports existing_hotels_parity — bug #5 regression"
        )
        assert "include_router(existing_hotels_parity_router)" in src, (
            "main.py no longer registers parity router — frontend toggle-scope/"
            "enrich-email/rescore would 404"
        )

    def test_parity_routes_exist(self):
        from app.routes.existing_hotels_parity import router as parity_router

        paths = {r.path for r in parity_router.routes}
        # The 3 documented endpoints
        assert any("toggle-scope" in p for p in paths)
        assert any("enrich-email" in p for p in paths)
        assert any("/rescore" in p for p in paths)


# ─────────────────────────────────────────────────────────────────────────────
# Bug #17 — auth middleware caches User.is_active with TTL.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug17_IsActiveCache:
    def test_cache_helpers_exist(self):
        from app.middleware.auth import (
            _is_user_active,
            _clear_user_active_cache,
            _USER_ACTIVE_TTL_SECONDS,
        )

        assert callable(_is_user_active)
        assert callable(_clear_user_active_cache)
        assert _USER_ACTIVE_TTL_SECONDS > 0
        assert _USER_ACTIVE_TTL_SECONDS <= 300, (
            "is_active cache TTL is too long — deactivated users keep "
            "access for too long after deactivation"
        )

    @pytest.mark.asyncio
    async def test_invalid_user_id_returns_false(self):
        from app.middleware.auth import _is_user_active

        # Empty string and non-int strings are conservatively rejected
        assert await _is_user_active("") is False
        assert await _is_user_active("not-an-int") is False

    def test_clear_cache_callable_with_and_without_arg(self):
        from app.middleware.auth import _clear_user_active_cache

        # Both signatures work without raising
        _clear_user_active_cache("123")
        _clear_user_active_cache(None)
        _clear_user_active_cache()


# ─────────────────────────────────────────────────────────────────────────────
# Bug #24 — ScrapeLog.status column is wide enough for 'completed_with_errors'.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug24_ScrapeLogStatusWidth:
    def test_status_column_width(self):
        from app.models.scrape_log import ScrapeLog

        col = ScrapeLog.__table__.columns["status"]
        # Old width was 20; longest valid value is 'completed_with_errors' (21)
        assert col.type.length >= 21, (
            f"ScrapeLog.status width {col.type.length} cannot fit "
            f"'completed_with_errors' (21 chars)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bug #26 — LeadContact.to_dict includes existing_hotel_id and last_enriched_at.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug26_LeadContactToDictKeys:
    def test_to_dict_has_existing_hotel_id_and_last_enriched_at(self):
        # Use a stub object so we don't need a DB
        from app.models.lead_contact import LeadContact

        contact = LeadContact()
        contact.id = 1
        contact.lead_id = 100
        contact.existing_hotel_id = None
        contact.name = "Test"
        contact.title = None
        contact.email = None
        contact.phone = None
        contact.linkedin = None
        contact.organization = None
        contact.scope = "unknown"
        contact.confidence = "medium"
        contact.tier = None
        contact.score = 0
        contact.is_saved = False
        contact.is_primary = False
        contact.found_via = None
        contact.source_detail = None
        contact.evidence_url = None
        contact.created_at = None
        contact.updated_at = None
        contact.last_enriched_at = None
        contact.strategist_priority = None
        contact.strategist_reasoning = None
        contact.score_breakdown = None
        contact.evidence = None

        d = contact.to_dict()
        assert "existing_hotel_id" in d, "bug #26 regression"
        assert "last_enriched_at" in d, "bug #26 regression"


# ─────────────────────────────────────────────────────────────────────────────
# Bug #29 — /leads/{id} PATCH requires admin.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug29_AdminGateOnLeadsPatch:
    def test_patch_route_has_admin_dep(self):
        from app.routes.leads import router

        # Find the PATCH /leads/{lead_id} route
        match = None
        for r in router.routes:
            if r.path == "/leads/{lead_id}" and "PATCH" in (
                getattr(r, "methods", set()) or set()
            ):
                match = r
                break
        assert match is not None, "PATCH /leads/{lead_id} route not found"

        # FastAPI stores deps on the route's dependant tree; check params
        # for a dependency whose call is _require_admin / require_admin.
        dependant = match.dependant
        all_deps = []

        def walk(d):
            all_deps.append(d)
            for sub in d.dependencies:
                walk(sub)

        walk(dependant)
        names = {d.call.__name__ if d.call else "" for d in all_deps}
        assert "require_admin" in names or any(
            "admin" in n.lower() for n in names
        ), (
            f"PATCH /leads/{{id}} has no admin dep (found deps: {names}) — "
            "bug #29 regression"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bug #34 — prepare_lead caps description and key_insights at 5000 chars.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug34_DescriptionLengthCap:
    def test_description_capped(self):
        from app.services.lead_factory import prepare_lead

        big = "x" * 20_000
        lead, skip_reason, _ = prepare_lead(
            {
                "hotel_name": "Big Description Hotel",
                "city": "Miami",
                "state": "FL",
                "country": "USA",
                "opening_date": "December 2027",
                "description": big,
                "source_url": "https://test.com",
                "source_site": "test.com",
            }
        )
        if lead is None:
            pytest.skip(f"prepare_lead skipped: {skip_reason}")
        assert (
            lead.description is None or len(lead.description) <= 5000
        ), f"description not capped (len={len(lead.description or '')})"

    def test_key_insights_capped(self):
        from app.services.lead_factory import prepare_lead

        big = "y" * 20_000
        lead, skip_reason, _ = prepare_lead(
            {
                "hotel_name": "Big Insights Hotel",
                "city": "Miami",
                "state": "FL",
                "country": "USA",
                "opening_date": "December 2027",
                "key_insights": big,
                "source_url": "https://test.com",
                "source_site": "test.com",
            }
        )
        if lead is None:
            pytest.skip(f"prepare_lead skipped: {skip_reason}")
        assert (
            lead.key_insights is None or len(lead.key_insights) <= 5000
        ), f"key_insights not capped (len={len(lead.key_insights or '')})"


# ─────────────────────────────────────────────────────────────────────────────
# Bug #38 — _fetch_all_leads has 10k cap.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug38_InsightlyFetchCap:
    def test_max_leads_constant_in_source(self):
        import inspect
        from app.services.insightly import InsightlyClient

        src = inspect.getsource(InsightlyClient._fetch_all_leads)
        assert "_MAX_LEADS = 10_000" in src or "_MAX_LEADS = 10000" in src, (
            "_fetch_all_leads no longer has 10k cap — bug #38 regression"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bug #25 — single-worker warning comment present on rate-limit store.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug25_RateLimitWarning:
    def test_single_worker_warning_in_main(self):
        import inspect
        import app.main

        src = inspect.getsource(app.main)
        # Comment must mention single-worker requirement
        assert "SINGLE-WORKER" in src or "single-worker" in src, (
            "rate-limit store missing single-worker warning comment"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bug #36 — unknown /api/* paths return JSON 404, not the SPA shell.
# ─────────────────────────────────────────────────────────────────────────────


class TestBug36_ApiNotFoundReturnsJSON:
    def test_api_404_handler_in_source(self):
        # The /api/{full_path:path} route only registers in app.main if
        # _FRONTEND_DIR.is_dir() — i.e. when frontend has been built. In
        # this test environment that's typically false, so check the
        # source-level handler is in place ahead of the SPA fallback.
        import inspect
        import app.main

        src = inspect.getsource(app.main)
        # Find positions of api_404 handler and serve_spa
        api_404_pos = src.find('async def api_404')
        spa_pos = src.find('async def serve_spa')
        assert api_404_pos != -1, (
            "api_404 handler missing — bug #36 regression "
            "(unknown /api/* paths would return SPA shell)"
        )
        assert spa_pos != -1, "serve_spa handler missing"
        assert api_404_pos < spa_pos, (
            "api_404 handler must be registered BEFORE serve_spa "
            "or the catch-all shadows it"
        )
        # Confirm it returns JSON 404
        api_404_block = src[api_404_pos : api_404_pos + 400]
        assert "JSONResponse" in api_404_block
        assert "status_code=404" in api_404_block
