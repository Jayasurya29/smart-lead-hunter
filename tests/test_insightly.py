"""
Smart Lead Hunter — Insightly CRM Tests
=========================================
Tests for the CRM integration service with mocked HTTP.

Covers:
  - push_contacts_as_leads (success + failure)
  - delete_leads_by_slh_id (safety guards)
  - Disabled client behavior (no API key)
  - Contact name splitting
  - Custom field mapping
  - Error handling for network failures
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ═══════════════════════════════════════════════════════════════════════
# CLIENT INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════


class TestInsightlyClientInit:
    """Client creation and configuration."""

    def test_enabled_with_key(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test-key-123", pod="na1")
        assert client.enabled is True

    def test_disabled_without_key(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="", pod="na1")
        assert client.enabled is False

    def test_base_url_uses_pod(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="key", pod="na2")
        assert "na2" in client.base_url

    def test_headers_contain_auth(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test-key", pod="na1")
        headers = client.headers
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")


# ═══════════════════════════════════════════════════════════════════════
# PUSH CONTACTS
# ═══════════════════════════════════════════════════════════════════════


class TestPushContacts:
    """Tests for push_contacts_as_leads()."""

    @pytest.mark.asyncio
    async def test_disabled_client_returns_empty(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="", pod="na1")
        result = await client.push_contacts_as_leads(
            contacts=[{"name": "John Doe", "title": "GM"}],
            hotel_name="Test Hotel",
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_skips_contacts_without_name(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test", pod="na1")

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"LEAD_ID": 100}

        with patch("app.services.insightly.httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=mock_resp)
            mock_http.__aenter__ = AsyncMock(return_value=mock_http)
            mock_http.__aexit__ = AsyncMock()
            MockClient.return_value = mock_http

            result = await client.push_contacts_as_leads(
                contacts=[{"name": "", "title": "GM"}, {"name": "  ", "title": "Dir"}],
                hotel_name="Test Hotel",
            )
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_successful_push_returns_lead_id(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test", pod="na1")

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"LEAD_ID": 42}

        with patch("app.services.insightly.httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=mock_resp)
            mock_http.__aenter__ = AsyncMock(return_value=mock_http)
            mock_http.__aexit__ = AsyncMock()
            MockClient.return_value = mock_http

            result = await client.push_contacts_as_leads(
                contacts=[{"name": "Jane Smith", "title": "Director of Housekeeping"}],
                hotel_name="Rosewood Miami",
                brand="Rosewood",
                slh_lead_id=7,
            )

        assert len(result) == 1
        assert result[0] == ("Jane Smith", 42)

    @pytest.mark.asyncio
    async def test_failed_push_returns_none_id(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test", pod="na1")

        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "Bad Request"

        with patch("app.services.insightly.httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=mock_resp)
            mock_http.__aenter__ = AsyncMock(return_value=mock_http)
            mock_http.__aexit__ = AsyncMock()
            MockClient.return_value = mock_http

            result = await client.push_contacts_as_leads(
                contacts=[{"name": "John Doe"}],
                hotel_name="Test Hotel",
            )

        assert len(result) == 1
        assert result[0][1] is None  # Lead ID is None on failure

    @pytest.mark.asyncio
    async def test_network_error_handled(self):
        import httpx
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test", pod="na1")

        dummy_request = httpx.Request("POST", "http://test.com")
        error = httpx.RequestError("timeout", request=dummy_request)

        # Create a proper async context manager mock
        mock_http = AsyncMock()
        mock_http.post.side_effect = error

        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_http
        mock_cm.__aexit__.return_value = False

        with patch("app.services.insightly.httpx.AsyncClient", return_value=mock_cm):
            result = await client.push_contacts_as_leads(
                contacts=[{"name": "Test Contact"}],
                hotel_name="Test Hotel",
            )

        assert len(result) == 1
        assert result[0][1] is None


# ═══════════════════════════════════════════════════════════════════════
# DELETE SAFETY GUARD
# ═══════════════════════════════════════════════════════════════════════


class TestDeleteSafetyGuard:
    """delete_leads_by_slh_id must check source ID + custom field."""

    @pytest.mark.asyncio
    async def test_disabled_client_returns_zero(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="", pod="na1")
        result = await client.delete_leads_by_slh_id(42)
        assert result == 0

    @pytest.mark.asyncio
    async def test_only_deletes_matching_slh_leads(self):
        from app.services.insightly import InsightlyClient, SLH_LEAD_SOURCE_ID
        client = InsightlyClient(api_key="test", pod="na1")

        # Simulate 3 leads: one SLH match, one SLH different ID, one non-SLH
        mock_leads = [
            {
                "LEAD_ID": 100,
                "LEAD_SOURCE_ID": SLH_LEAD_SOURCE_ID,
                "CUSTOMFIELDS": [
                    {"FIELD_NAME": "SLH_Lead_ID__c", "FIELD_VALUE": 7},
                ],
            },
            {
                "LEAD_ID": 101,
                "LEAD_SOURCE_ID": SLH_LEAD_SOURCE_ID,
                "CUSTOMFIELDS": [
                    {"FIELD_NAME": "SLH_Lead_ID__c", "FIELD_VALUE": 99},
                ],
            },
            {
                "LEAD_ID": 102,
                "LEAD_SOURCE_ID": 999999,  # Not SLH
                "CUSTOMFIELDS": [],
            },
        ]

        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.json.return_value = mock_leads

        mock_del_resp = MagicMock()
        mock_del_resp.status_code = 202

        with patch("app.services.insightly.httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.get = AsyncMock(return_value=mock_get_resp)
            mock_http.delete = AsyncMock(return_value=mock_del_resp)
            mock_http.__aenter__ = AsyncMock(return_value=mock_http)
            mock_http.__aexit__ = AsyncMock()
            MockClient.return_value = mock_http

            deleted = await client.delete_leads_by_slh_id(7)

        # Should only delete Lead 100 (matching SLH source + SLH_Lead_ID)
        assert deleted == 1
        # Verify only one delete call was made
        mock_http.delete.assert_called_once()
        call_url = mock_http.delete.call_args[0][0]
        assert "100" in call_url


# ═══════════════════════════════════════════════════════════════════════
# CUSTOM FIELD MAPPING
# ═══════════════════════════════════════════════════════════════════════


class TestCustomFieldMapping:
    """Verify Insightly custom fields are correctly populated."""

    @pytest.mark.asyncio
    async def test_custom_fields_include_slh_id(self):
        from app.services.insightly import InsightlyClient
        client = InsightlyClient(api_key="test", pod="na1")

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"LEAD_ID": 1}

        captured_payload = {}

        async def capture_post(url, headers, json):
            captured_payload.update(json)
            return mock_resp

        with patch("app.services.insightly.httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(side_effect=capture_post)
            mock_http.__aenter__ = AsyncMock(return_value=mock_http)
            mock_http.__aexit__ = AsyncMock()
            MockClient.return_value = mock_http

            await client.push_contacts_as_leads(
                contacts=[{"name": "Test Contact"}],
                hotel_name="Test Hotel",
                brand="Hilton",
                slh_lead_id=42,
            )

        # Check SLH_Lead_ID custom field
        custom_fields = captured_payload.get("CUSTOMFIELDS", [])
        slh_field = next(
            (f for f in custom_fields if f["FIELD_NAME"] == "SLH_Lead_ID__c"), None
        )
        assert slh_field is not None
        assert slh_field["FIELD_VALUE"] == 42

        # Check Brand custom field
        brand_field = next(
            (f for f in custom_fields if f["FIELD_NAME"] == "Brand__c"), None
        )
        assert brand_field is not None
        assert brand_field["FIELD_VALUE"] == "Hilton"
