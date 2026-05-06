"""
Regression tests for the 2026-05-06 audit fix batch.

Locks in the contracts that the CRIT-1, CRIT-2, CRIT-3, HIGH-1, HIGH-2,
HIGH-3, HV-1, HV-2, HV-4, HV-5 fixes restored. Run as part of CI:

    pytest tests/test_audit_2026_05_06.py -v
"""

from __future__ import annotations

import inspect
import re
from datetime import datetime
from pathlib import Path

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# CRIT-1: /revenue and /discovery now protected by the auth middleware
# ─────────────────────────────────────────────────────────────────────────────


class TestCrit1AuthProtectsRevenueAndDiscovery:
    def test_protected_prefixes_includes_revenue(self):
        from app.middleware.auth import APIKeyMiddleware

        assert "/revenue" in APIKeyMiddleware.PROTECTED_PREFIXES, (
            "PROTECTED_PREFIXES must include /revenue (CRIT-1 fix)"
        )

    def test_protected_prefixes_includes_discovery(self):
        from app.middleware.auth import APIKeyMiddleware

        assert "/discovery" in APIKeyMiddleware.PROTECTED_PREFIXES, (
            "PROTECTED_PREFIXES must include /discovery (CRIT-1 fix)"
        )

    def test_revenue_not_in_exclude_prefixes(self):
        from app.middleware.auth import APIKeyMiddleware

        assert "/revenue" not in APIKeyMiddleware.EXCLUDE_PREFIXES
        assert "/discovery" not in APIKeyMiddleware.EXCLUDE_PREFIXES

    def test_rate_limit_prefixes_match_auth(self):
        # Keep auth + rate-limit prefix lists in sync — drift here is
        # how the original gap appeared.
        from app import main as main_mod

        assert "/revenue" in main_mod._RATE_LIMITED_PREFIXES
        assert "/discovery" in main_mod._RATE_LIMITED_PREFIXES


# ─────────────────────────────────────────────────────────────────────────────
# CRIT-2 + HV-1: 5-tier coercion in lead_data_enrichment.py
# ─────────────────────────────────────────────────────────────────────────────


class TestCrit2CanonicalTierCoercion:
    def test_grounding_valid_tiers_is_canonical_five(self):
        from app.services.lead_data_enrichment import _GROUNDING_VALID_TIERS

        assert _GROUNDING_VALID_TIERS == {
            "tier1_ultra_luxury",
            "tier2_luxury",
            "tier3_upper_upscale",
            "tier4_upscale",
            "tier5_skip",
        }

    def test_coercer_maps_non_canonical_to_skip(self):
        from app.services.lead_data_enrichment import (
            _coerce_brand_tier_to_canonical,
        )

        for bad in (
            "tier5_upper_midscale",
            "tier6_midscale",
            "tier7_economy",
            "tier8_budget",
        ):
            assert _coerce_brand_tier_to_canonical(bad) == "tier5_skip", (
                f"{bad!r} must coerce to tier5_skip"
            )

    def test_coercer_passes_through_canonical(self):
        from app.services.lead_data_enrichment import (
            _coerce_brand_tier_to_canonical,
        )

        for good in (
            "tier1_ultra_luxury",
            "tier2_luxury",
            "tier3_upper_upscale",
            "tier4_upscale",
            "tier5_skip",
        ):
            assert _coerce_brand_tier_to_canonical(good) == good

    def test_coercer_drops_unrecognized_and_pipes(self):
        from app.services.lead_data_enrichment import (
            _coerce_brand_tier_to_canonical,
        )

        for bad in (None, "", "garbage", "tier3 | tier4", 5, ["tier2_luxury"]):
            assert _coerce_brand_tier_to_canonical(bad) is None

    def test_grounding_prompt_only_lists_canonical(self):
        # Ensure no future edit re-introduces a 7-tier enum line in the
        # grounding prompt builder.
        from app.services.lead_data_enrichment import _build_grounding_prompt

        prompt = _build_grounding_prompt(
            "Test Hotel", "Miami", "FL", "USA", "Marriott"
        )
        for bad in ("tier5_upper_midscale", "tier6_midscale", "tier7_economy"):
            assert bad not in prompt, (
                f"grounding prompt must not mention {bad!r} (CRIT-2)"
            )

    def test_six_stage_schema_only_lists_canonical(self):
        # Source-level guard: scan the module text.
        path = Path("app/services/lead_data_enrichment.py")
        src = path.read_text(encoding="utf-8")
        # Scope the check to the schema enum block (line 1565ish).
        # If anyone resurrects a 7-tier list anywhere, fail loudly.
        offenders = [
            "tier5_upper_midscale",
            "tier6_midscale",
            "tier7_economy",
        ]
        # Allow the alias map (which intentionally references them as
        # input keys to coerce away to tier5_skip).
        forbidden_blocks = re.findall(
            r"\"enum\":\s*\[[^\]]+\]", src
        )
        for block in forbidden_blocks:
            for bad in offenders:
                assert bad not in block, (
                    f"JSON-schema enum still contains {bad!r} (CRIT-2)"
                )


# ─────────────────────────────────────────────────────────────────────────────
# HV-5: apply layer refuses to downgrade a valid tier to tier5_skip
# ─────────────────────────────────────────────────────────────────────────────


class TestHv5NoDowngrade:
    def test_apply_layer_source_has_downgrade_guard(self):
        # Source-level — the apply functions have the explicit
        # is_downgrade_to_skip guard added in HV-5. We can't easily
        # exercise the full apply layer without the DB, so we verify
        # the guard string is present in the apply path.
        path = Path("app/services/lead_data_enrichment.py")
        src = path.read_text(encoding="utf-8")
        assert "is_downgrade_to_skip" in src, (
            "HV-5 guard `is_downgrade_to_skip` must be present in "
            "lead_data_enrichment.py apply layer"
        )
        # And appears in BOTH apply functions (full-refresh + result
        # builder).
        assert src.count("is_downgrade_to_skip") >= 2, (
            "HV-5 guard must apply to both apply paths"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CRIT-3: indexes declared on the model layer (migration 023 mirrors them)
# ─────────────────────────────────────────────────────────────────────────────


class TestCrit3Indexes:
    def test_potential_lead_hot_columns_indexed(self):
        from app.models.potential_lead import PotentialLead

        for col_name in (
            "hotel_name_normalized",
            "timeline_label",
            "brand_tier",
            "zone",
            "opening_year",
            "last_user_review_at",
        ):
            col = getattr(PotentialLead, col_name)
            # SQLAlchemy InstrumentedAttribute has .property.columns[0]
            sa_col = col.property.columns[0]
            assert sa_col.index is True, (
                f"PotentialLead.{col_name} must declare index=True (CRIT-3)"
            )

    def test_existing_hotel_hot_columns_indexed(self):
        from app.models.existing_hotel import ExistingHotel

        for col_name in (
            "hotel_name_normalized",
            "brand_tier",
            "zone",
        ):
            col = getattr(ExistingHotel, col_name)
            sa_col = col.property.columns[0]
            assert sa_col.index is True, (
                f"ExistingHotel.{col_name} must declare index=True (CRIT-3)"
            )

    def test_migration_023_exists(self):
        path = Path("alembic/versions/023_hot_path_indexes_and_unaccent.py")
        assert path.exists(), "migration 023 missing"
        src = path.read_text(encoding="utf-8")
        # Spot-check the key DDL is present.
        assert "ix_potential_leads_hotel_name_normalized" in src
        assert "ix_existing_hotels_hotel_name_normalized" in src
        assert "CREATE EXTENSION IF NOT EXISTS unaccent" in src
        assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in src
        assert "last_user_review_at" in src


# ─────────────────────────────────────────────────────────────────────────────
# HIGH-1: PATCH /auth/users/{id} clears the active-cache on is_active flip
# ─────────────────────────────────────────────────────────────────────────────


class TestHigh1ClearsActiveCache:
    def test_update_user_calls_clear_user_active_cache(self):
        from app.routes.auth import update_user

        src = inspect.getsource(update_user)
        assert "_clear_user_active_cache" in src, (
            "update_user must invalidate _user_active_cache when is_active "
            "flips (HIGH-1 fix)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# HIGH-2 / HV-3: diacritic-aware search via unaccent_ilike
# ─────────────────────────────────────────────────────────────────────────────


class TestHigh2HV3UnaccentIlike:
    def test_unaccent_ilike_emits_unaccent_on_postgres(self):
        from sqlalchemy import Column, MetaData, String, Table, select
        from sqlalchemy.dialects import postgresql, sqlite
        from app.shared import unaccent_ilike

        md = MetaData()
        t = Table("x", md, Column("name", String))
        q = select(t).where(unaccent_ilike(t.c.name, "Café"))

        pg = str(
            q.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )
        lite = str(
            q.compile(
                dialect=sqlite.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )

        # Postgres MUST wrap with unaccent on both sides
        assert pg.count("unaccent") == 2, f"PG SQL missing unaccent: {pg}"
        # SQLite degrades to plain lower(...) (no extension)
        assert "unaccent" not in lite, f"SQLite SQL must not call unaccent: {lite}"

    def test_apply_lead_filters_uses_unaccent_for_search(self):
        from app import shared

        src = inspect.getsource(shared.apply_lead_filters)
        assert "unaccent_ilike" in src, (
            "apply_lead_filters must use unaccent_ilike for the search "
            "filter (HIGH-2 / HV-3)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# HIGH-3: /leads/export caps at 25k rows
# ─────────────────────────────────────────────────────────────────────────────


class TestHigh3ExportRowCap:
    def test_leads_export_has_row_cap(self):
        path = Path("app/routes/leads.py")
        src = path.read_text(encoding="utf-8")
        assert "EXPORT_ROW_CAP = 25_000" in src, (
            "/leads/export must cap at 25,000 rows (HIGH-3)"
        )
        assert "X-Result-Truncated" in src

    def test_existing_hotels_export_csv_has_row_cap(self):
        path = Path("app/routes/existing_hotels.py")
        src = path.read_text(encoding="utf-8")
        assert "EXPORT_ROW_CAP = 25_000" in src, (
            "existing-hotels export-csv must cap at 25,000 rows (HIGH-3)"
        )

    def test_existing_hotels_export_csv_uses_canonical_phone(self):
        # HIGH-3 sweep also fixed `h.phone` → contact_phone/gm_phone.
        path = Path("app/routes/existing_hotels.py")
        src = path.read_text(encoding="utf-8")
        # Strip comments before checking — historical comments are
        # allowed to mention the old name. Code lines must not.
        code_lines = [
            ln for ln in src.splitlines() if not ln.lstrip().startswith("#")
        ]
        # ` h.phone` (with a space or operator before) shouldn't appear
        # outside comments. The pattern is "h.phone" preceded by either
        # whitespace, `(`, or `[` and not by an underscore (avoiding
        # h.contact_phone matches).
        bad_re = re.compile(r"(?<![A-Za-z_])h\.phone\b")
        for ln in code_lines:
            assert not bad_re.search(ln), (
                f"h.phone was dropped in migration 018 — must use "
                f"h.contact_phone. Offending line: {ln!r}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# HV-2: pre-opening digest renders + skips silently when not configured
# ─────────────────────────────────────────────────────────────────────────────


class TestHv2PreOpeningDigest:
    def test_build_digest_html_with_crossings(self):
        from app.services.notifications import build_digest_html

        crossings = [
            {
                "hotel_name": "Sandals Montego Bay",
                "city": "Montego Bay",
                "state": "St James",
                "country": "Jamaica",
                "brand_tier": "tier2_luxury",
                "opening_date": "December 2026",
                "months_out": 7,
                "lead_score": 85,
            }
        ]
        html = build_digest_html(crossings, datetime(2026, 5, 6))
        assert "Sandals Montego Bay" in html
        assert "December 2026" in html
        assert "85" in html
        # Date in human format
        assert "May 06, 2026" in html or "May 6, 2026" in html

    def test_build_digest_html_empty(self):
        from app.services.notifications import build_digest_html

        html = build_digest_html([], datetime(2026, 5, 6))
        assert "No new lead crossings" in html

    @pytest.mark.asyncio
    async def test_send_digest_no_recipients_is_noop_success(self):
        from app.services.notifications import send_digest_email

        ok = await send_digest_email([], [{"hotel_name": "X", "months_out": 7}])
        assert ok is True, (
            "missing recipients should be a no-op (return True), not failure"
        )

    def test_celery_beat_includes_digest_task(self):
        from app.tasks.celery_app import celery_app

        sched = celery_app.conf.beat_schedule
        assert "pre-opening-digest" in sched
        entry = sched["pre-opening-digest"]
        assert entry["task"] == "pre_opening_digest"


# ─────────────────────────────────────────────────────────────────────────────
# HV-4: last_user_review_at stamping + stale_review filter wiring
# ─────────────────────────────────────────────────────────────────────────────


class TestHv4LastUserReview:
    def test_potential_lead_has_last_user_review_at(self):
        from app.models.potential_lead import PotentialLead

        col = PotentialLead.last_user_review_at.property.columns[0]
        assert col.nullable is True
        assert col.index is True

    def test_dashboard_handlers_stamp_review(self):
        path = Path("app/routes/dashboard.py")
        src = path.read_text(encoding="utf-8")
        # The stamp must appear in approve, reject, restore, and edit.
        # Crude but stable: we expect at least 4 distinct sets of the
        # stamp line.
        assert src.count("lead.last_user_review_at = local_now()") >= 4, (
            "approve/reject/restore/edit handlers must all stamp "
            "last_user_review_at (HV-4)"
        )

    def test_leads_route_supports_review_stale_days(self):
        path = Path("app/routes/leads.py")
        src = path.read_text(encoding="utf-8")
        assert "review_stale_days" in src
        assert "last_user_review_at" in src
        # The sort key is exposed
        assert '"review_stale"' in src

    def test_potential_lead_to_dict_includes_last_user_review_at(self):
        from app.models.potential_lead import PotentialLead

        # Build a transient instance — to_dict() shouldn't need a session.
        lead = PotentialLead(hotel_name="Test", status="new")
        d = lead.to_dict()
        assert "last_user_review_at" in d
        assert d["last_user_review_at"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Smoke: every modified module compiles cleanly
# ─────────────────────────────────────────────────────────────────────────────


class TestSmokeModulesCompile:
    @pytest.mark.parametrize(
        "module_path",
        [
            "app/middleware/auth.py",
            "app/main.py",
            "app/services/lead_data_enrichment.py",
            "app/services/notifications.py",
            "app/config/canonical_tiers.py",
            "app/routes/auth.py",
            "app/routes/leads.py",
            "app/routes/dashboard.py",
            "app/routes/existing_hotels.py",
            "app/shared.py",
            "app/models/potential_lead.py",
            "app/models/existing_hotel.py",
            "app/tasks/autonomous_tasks.py",
            "app/tasks/celery_app.py",
            "alembic/versions/023_hot_path_indexes_and_unaccent.py",
        ],
    )
    def test_module_compiles(self, module_path):
        import py_compile

        py_compile.compile(module_path, doraise=True)
