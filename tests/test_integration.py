"""
Smart Lead Hunter - Integration Tests (SDLC-Compliant)
=====================================================
All async — single event loop, no connection conflicts.
Run: pytest tests/test_integration.py -v
"""

import pytest
from datetime import datetime, timezone


# =====================================================================
# HEALTH & STATUS
# =====================================================================


class TestHealthEndpoints:
    @pytest.mark.asyncio
    async def test_root_returns_app_info(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "status" in resp.json()

    @pytest.mark.asyncio
    async def test_health_returns_components(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ["healthy", "degraded", "unhealthy"]
        assert "database" in data["components"]
        assert "timestamp" in data

    @pytest.mark.asyncio
    async def test_health_timestamp_valid(self, client):
        data = (await client.get("/health")).json()
        assert "T" in data["timestamp"]

    @pytest.mark.asyncio
    async def test_openapi_has_paths(self, client):
        data = (await client.get("/openapi.json")).json()
        assert len(data["paths"]) > 10

    @pytest.mark.asyncio
    async def test_docs_renders(self, client):
        assert (await client.get("/docs")).status_code == 200


# =====================================================================
# LEADS API - READ
# =====================================================================


class TestLeadsRead:
    @pytest.mark.asyncio
    async def test_list_leads(self, client):
        resp = await client.get("/leads")
        assert resp.status_code == 200
        assert isinstance(resp.json(), (dict, list))

    @pytest.mark.asyncio
    async def test_pagination(self, client):
        assert (await client.get("/leads?page=1&per_page=2")).status_code == 200

    @pytest.mark.asyncio
    async def test_invalid_page_rejected(self, client):
        assert (await client.get("/leads?page=0")).status_code == 422

    @pytest.mark.asyncio
    async def test_hot_leads(self, client):
        assert (await client.get("/leads/hot")).status_code == 200

    @pytest.mark.asyncio
    async def test_florida_leads(self, client):
        assert (await client.get("/leads/florida")).status_code == 200

    @pytest.mark.asyncio
    async def test_caribbean_leads(self, client):
        assert (await client.get("/leads/caribbean")).status_code == 200

    @pytest.mark.asyncio
    async def test_nonexistent_lead_404(self, client):
        assert (await client.get("/leads/999999")).status_code == 404


# =====================================================================
# SOURCES API - READ
# =====================================================================


class TestSourcesRead:
    @pytest.mark.asyncio
    async def test_list_sources(self, client):
        resp = await client.get("/sources")
        assert resp.status_code == 200
        assert isinstance(resp.json(), (dict, list))

    @pytest.mark.asyncio
    async def test_healthy_sources(self, client):
        assert (await client.get("/sources/healthy")).status_code == 200

    @pytest.mark.asyncio
    async def test_problem_sources(self, client):
        assert (await client.get("/sources/problems")).status_code == 200


# =====================================================================
# DASHBOARD
# =====================================================================


class TestDashboard:
    @pytest.mark.asyncio
    async def test_dashboard_renders_html(self, client):
        resp = await client.get("/dashboard")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    @pytest.mark.asyncio
    async def test_stats_partial(self, client):
        resp = await client.get("/api/dashboard/stats")
        assert resp.status_code == 200
        assert len(resp.content) > 0

    @pytest.mark.asyncio
    async def test_leads_partial(self, client):
        assert (
            await client.get("/api/dashboard/leads?tab=pipeline")
        ).status_code == 200

    @pytest.mark.asyncio
    async def test_sources_list(self, client):
        assert (await client.get("/api/dashboard/sources/list")).status_code == 200


# =====================================================================
# LEAD LIFECYCLE (real DB CRUD with auto-cleanup)
# =====================================================================


class TestLeadLifecycle:
    @pytest.mark.asyncio
    async def test_lead_created_with_correct_fields(self, sample_lead):
        assert "Four Seasons Miami Beach" in sample_lead.hotel_name
        assert sample_lead.brand == "Four Seasons"
        assert str(sample_lead.brand_tier) == "2"
        assert sample_lead.lead_score == 78
        assert sample_lead.status == "new"
        assert sample_lead.id is not None

    @pytest.mark.asyncio
    async def test_lead_linked_to_source(self, sample_lead, sample_source):
        assert sample_lead.source_id == sample_source.id

    @pytest.mark.asyncio
    async def test_score_breakdown_stored(self, sample_lead):
        assert sample_lead.score_breakdown is not None
        assert sample_lead.score_breakdown["brand"] == 20
        assert sample_lead.score_breakdown["location"] == 20

    @pytest.mark.asyncio
    async def test_approve_lead(self, db_session, sample_lead):
        sample_lead.status = "approved"
        await db_session.commit()
        await db_session.refresh(sample_lead)
        assert sample_lead.status == "approved"

    @pytest.mark.asyncio
    async def test_reject_lead_with_reason(self, db_session, sample_lead):
        sample_lead.status = "rejected"
        sample_lead.rejection_reason = "Not in target market"
        await db_session.commit()
        await db_session.refresh(sample_lead)
        assert sample_lead.status == "rejected"
        assert sample_lead.rejection_reason == "Not in target market"

    @pytest.mark.asyncio
    async def test_lead_retrieve_by_id(self, db_session, sample_lead):
        from app.models.potential_lead import PotentialLead
        from sqlalchemy import select

        result = await db_session.execute(
            select(PotentialLead).where(PotentialLead.id == sample_lead.id)
        )
        fetched = result.scalar_one()
        assert fetched.hotel_name == sample_lead.hotel_name

    @pytest.mark.asyncio
    async def test_lead_timestamps(self, sample_lead):
        assert sample_lead.created_at is not None
        assert sample_lead.updated_at is not None


# =====================================================================
# SOURCE LIFECYCLE
# =====================================================================


class TestSourceLifecycle:
    @pytest.mark.asyncio
    async def test_source_created_correctly(self, sample_source):
        assert "Hospitality News" in sample_source.name
        assert sample_source.is_active is True
        assert sample_source.health_status == "healthy"
        assert sample_source.id is not None

    @pytest.mark.asyncio
    async def test_toggle_active(self, db_session, sample_source):
        sample_source.is_active = False
        await db_session.commit()
        await db_session.refresh(sample_source)
        assert sample_source.is_active is False

    @pytest.mark.asyncio
    async def test_health_degradation(self, db_session, sample_source):
        sample_source.consecutive_failures = 5
        sample_source.health_status = "failing"
        await db_session.commit()
        await db_session.refresh(sample_source)
        assert sample_source.consecutive_failures == 5
        assert sample_source.health_status == "failing"

    @pytest.mark.asyncio
    async def test_leads_count_increment(self, db_session, sample_source):
        sample_source.leads_found = 10
        await db_session.commit()
        await db_session.refresh(sample_source)
        assert sample_source.leads_found == 10

    @pytest.mark.asyncio
    async def test_source_timestamps(self, sample_source):
        assert sample_source.created_at is not None


# =====================================================================
# BATCH DATA TESTS
# =====================================================================


class TestBatchOperations:
    @pytest.mark.asyncio
    async def test_batch_correct_count(self, sample_leads_batch):
        assert len(sample_leads_batch) == 5

    @pytest.mark.asyncio
    async def test_batch_varied_statuses(self, sample_leads_batch):
        statuses = {lead.status for lead in sample_leads_batch}
        assert "new" in statuses
        assert "approved" in statuses
        assert "rejected" in statuses

    @pytest.mark.asyncio
    async def test_batch_score_range(self, sample_leads_batch):
        scores = [lead.lead_score for lead in sample_leads_batch]
        assert min(scores) < 30
        assert max(scores) >= 80

    @pytest.mark.asyncio
    async def test_filter_by_status(self, db_session, sample_leads_batch):
        from app.models.potential_lead import PotentialLead
        from sqlalchemy import select

        result = await db_session.execute(
            select(PotentialLead).where(
                PotentialLead.status == "new",
                PotentialLead.hotel_name.like("__TEST__%"),
            )
        )
        assert len(result.scalars().all()) == 2

    @pytest.mark.asyncio
    async def test_filter_by_score(self, db_session, sample_leads_batch):
        from app.models.potential_lead import PotentialLead
        from sqlalchemy import select

        result = await db_session.execute(
            select(PotentialLead).where(
                PotentialLead.lead_score >= 70,
                PotentialLead.hotel_name.like("__TEST__%"),
            )
        )
        assert len(result.scalars().all()) >= 2


# =====================================================================
# DATA INTEGRITY
# =====================================================================


class TestDataIntegrity:
    @pytest.mark.asyncio
    async def test_lead_requires_hotel_name(self, db_session, sample_source):
        from app.models.potential_lead import PotentialLead
        from sqlalchemy.exc import IntegrityError

        lead = PotentialLead(
            hotel_name=None,
            status="new",
            source_id=sample_source.id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(lead)
        with pytest.raises((IntegrityError, Exception)):
            await db_session.commit()
        await db_session.rollback()


# =====================================================================
# SECURITY
# =====================================================================


class TestSecurity:
    @pytest.mark.asyncio
    async def test_scrape_endpoint_responds(self, client):
        assert (await client.post("/api/dashboard/scrape")).status_code == 200

    @pytest.mark.asyncio
    async def test_htmx_header_accepted(self, client):
        resp = await client.post(
            "/api/dashboard/scrape",
            headers={"HX-Request": "true"},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_no_stack_traces(self, client):
        body = (await client.get("/leads/not-a-number")).text
        assert "Traceback" not in body
        assert "\\Users\\" not in body


# =====================================================================
# RESPONSE SCHEMAS
# =====================================================================


class TestResponseSchemas:
    @pytest.mark.asyncio
    async def test_health_schema(self, client):
        data = (await client.get("/health")).json()
        assert {"status", "timestamp", "components"}.issubset(data.keys())
        assert data["status"] in ["healthy", "degraded", "unhealthy"]

    @pytest.mark.asyncio
    async def test_root_schema(self, client):
        assert "status" in (await client.get("/")).json()

    @pytest.mark.asyncio
    async def test_sources_schema(self, client):
        assert isinstance((await client.get("/sources")).json(), (dict, list))


# =====================================================================
# ERROR HANDLING
# =====================================================================


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_404_unknown_route(self, client):
        assert (await client.get("/api/nonexistent")).status_code == 404

    @pytest.mark.asyncio
    async def test_405_wrong_method(self, client):
        assert (await client.delete("/health")).status_code == 405

    @pytest.mark.asyncio
    async def test_422_invalid_id(self, client):
        assert (await client.get("/leads/abc")).status_code in [404, 422]

    @pytest.mark.asyncio
    async def test_405_post_to_get(self, client):
        assert (await client.post("/health")).status_code == 405
