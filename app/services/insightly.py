"""
Insightly CRM Integration Service
---------------------------------
Pushes approved leads to Insightly's Potential_Leads__c custom object.

API: https://api.insightly.com/v3.1/Help
Endpoint: /Potential_Leads__c
Auth: Basic (API key + blank password)
"""

import base64
import httpx
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class InsightlyClient:
    """Client for Insightly CRM API v3.1 — Potential Leads custom object."""

    # Maps our DB fields → Insightly custom field API names
    FIELD_MAP = {
        "brand": "Brand__c",
        "brand_tier": "Brand_Tier__c",
        "city": "City__c",
        "state": "State__c",
        "country": "Country__c",
        "opening_date": "Opening_Date__c",
        "room_count": "Room_Count__c",
        "lead_score": "Lead_Score__c",
        "description": "Description__c",
        "source_url": "Source_URL__c",
        "management_company": "Management_Company__c",
        "developer": "Developer__c",
        "owner": "Owner_Company__c",
        "status": "Status__c",
        "id": "SLH_Lead_ID__c",
    }

    def __init__(self, api_key: str, pod: str = "na1"):
        self.api_key = api_key
        self.base_url = f"https://api.{pod}.insightly.com/v3.1"
        self.enabled = bool(api_key)
        if not self.enabled:
            logger.warning("Insightly API key not set — CRM sync disabled.")

    @property
    def headers(self) -> Dict[str, str]:
        auth = base64.b64encode(f"{self.api_key}:".encode()).decode()
        return {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _build_custom_fields(self, lead_data: Dict[str, Any]) -> list:
        """Convert our lead dict to Insightly CUSTOMFIELDS array."""
        fields = []
        for our_key, insightly_key in self.FIELD_MAP.items():
            value = lead_data.get(our_key)
            if value is not None and value != "":
                fields.append(
                    {
                        "FIELD_NAME": insightly_key,
                        "FIELD_VALUE": value,
                    }
                )
        return fields

    async def push_lead(self, lead_data: Dict[str, Any]) -> Optional[Dict]:
        """
        Push a lead to Insightly Potential_Leads__c.

        Args:
            lead_data: Dict with hotel_name, brand, city, state, etc.

        Returns:
            Insightly response dict with RECORD_ID, or None on failure.
        """
        if not self.enabled:
            logger.warning("Insightly not configured — skipping push.")
            return None

        record = {
            "RECORD_NAME": lead_data.get("hotel_name", "Unknown Hotel"),
            "CUSTOMFIELDS": self._build_custom_fields(lead_data),
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{self.base_url}/Potential_Leads__c",
                    headers=self.headers,
                    json=record,
                )

                if resp.status_code == 200:
                    result = resp.json()
                    logger.info(
                        f"Insightly: pushed '{lead_data.get('hotel_name')}' "
                        f"→ RECORD_ID {result.get('RECORD_ID')}"
                    )
                    return result
                else:
                    logger.error(
                        f"Insightly push failed: {resp.status_code} — {resp.text[:300]}"
                    )
                    return None

        except httpx.RequestError as e:
            logger.error(f"Insightly request error: {e}")
            return None

    async def update_lead(
        self, record_id: int, lead_data: Dict[str, Any]
    ) -> Optional[Dict]:
        """
        Update an existing Potential Lead in Insightly.

        Args:
            record_id: Insightly RECORD_ID
            lead_data: Dict with fields to update
        """
        if not self.enabled:
            return None

        record = {
            "RECORD_ID": record_id,
            "RECORD_NAME": lead_data.get("hotel_name", ""),
            "CUSTOMFIELDS": self._build_custom_fields(lead_data),
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.put(
                    f"{self.base_url}/Potential_Leads__c",
                    headers=self.headers,
                    json=record,
                )

                if resp.status_code == 200:
                    logger.info(f"Insightly: updated RECORD_ID {record_id}")
                    return resp.json()
                else:
                    logger.error(
                        f"Insightly update failed: {resp.status_code} — {resp.text[:300]}"
                    )
                    return None

        except httpx.RequestError as e:
            logger.error(f"Insightly request error: {e}")
            return None

    async def delete_lead(self, record_id: int) -> bool:
        """Delete a Potential Lead from Insightly."""
        if not self.enabled:
            return False

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.delete(
                    f"{self.base_url}/Potential_Leads__c/{record_id}",
                    headers=self.headers,
                )
                if resp.status_code == 202:
                    logger.info(f"Insightly: deleted RECORD_ID {record_id}")
                    return True
                else:
                    logger.error(f"Insightly delete failed: {resp.status_code}")
                    return False

        except httpx.RequestError as e:
            logger.error(f"Insightly request error: {e}")
            return False

    async def test_connection(self) -> Dict[str, Any]:
        """Test Insightly API connection."""
        if not self.enabled:
            return {"connected": False, "error": "API key not set"}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{self.base_url}/Users/Me",
                    headers=self.headers,
                )
                if resp.status_code == 200:
                    user = resp.json()
                    return {
                        "connected": True,
                        "user": user.get("EMAIL_ADDRESS"),
                    }
                else:
                    return {"connected": False, "error": f"HTTP {resp.status_code}"}

        except httpx.RequestError as e:
            return {"connected": False, "error": str(e)}


# ── Singleton ──
_client: Optional[InsightlyClient] = None


def get_insightly_client() -> InsightlyClient:
    """Get or create the Insightly client singleton."""
    global _client
    if _client is None:
        from app.config_app import settings

        _client = InsightlyClient(
            api_key=settings.insightly_api_key,
            pod=settings.insightly_pod,
        )
    return _client
