"""
Insightly CRM Integration Service
---------------------------------
Pushes approved hotel contacts as Insightly Leads.

Flow: Dashboard Approve → each contact becomes an Insightly Lead
      with hotel info (brand, rooms, opening) + contact info (name, title, LinkedIn)

API: https://api.insightly.com/v3.1/Help
Auth: Basic (API key + blank password)
"""

import base64
import httpx
import logging
from typing import Optional, Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

# Smart Lead Hunter Lead Source ID (from Insightly)
SLH_LEAD_SOURCE_ID = 3859952


class InsightlyClient:
    """Client for Insightly CRM API v3.1 — pushes contacts as Leads."""

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

    async def push_contacts_as_leads(
        self,
        contacts: List[Dict],
        hotel_name: str,
        brand: str = "",
        brand_tier: str = "",
        city: str = "",
        state: str = "",
        country: str = "USA",
        opening_date: str = "",
        room_count: int = 0,
        lead_score: int = 0,
        description: str = "",
        source_url: str = "",
        management_company: str = "",
        developer: str = "",
        owner: str = "",
        slh_lead_id: int = 0,
    ) -> List[Tuple[str, Optional[int]]]:
        """
        Push enriched contacts as Insightly Leads.

        Each contact becomes one Lead with:
        - Contact info: name, title, email, phone, LinkedIn
        - Hotel info: brand, rooms, opening, score (shared across all contacts)

        Args:
            contacts: List of contact dicts from LeadContact.to_dict()
            hotel_name: Hotel name → ORGANISATION_NAME
            All other args: hotel-level data shared across contacts

        Returns:
            List of (contact_name, insightly_lead_id) tuples
        """
        if not self.enabled:
            logger.warning("Insightly not configured — skipping push.")
            return []

        # Tier display names
        tier_display = {
            "tier1_ultra_luxury": "Ultra Luxury",
            "tier2_luxury": "Luxury",
            "tier3_upper_upscale": "Upper Upscale",
            "tier4_upscale": "Upscale",
        }

        results = []
        for contact in contacts:
            name = (contact.get("name") or "").strip()
            if not name:
                continue

            # Split name into first/last
            parts = name.split(None, 1)
            first_name = parts[0] if parts else ""
            last_name = parts[1] if len(parts) > 1 else first_name

            # Build description with hotel context
            desc_parts = []
            if description:
                desc_parts.append(description)
            if opening_date:
                desc_parts.append(f"Opening: {opening_date}")
            if room_count:
                desc_parts.append(f"Rooms: {room_count}")
            if management_company:
                desc_parts.append(f"Management: {management_company}")
            if developer:
                desc_parts.append(f"Developer: {developer}")
            if contact.get("scope"):
                desc_parts.append(f"Contact scope: {contact['scope']}")
            if contact.get("confidence"):
                desc_parts.append(f"Confidence: {contact['confidence']}")
            if contact.get("found_via"):
                desc_parts.append(f"Found via: {contact['found_via']}")

            lead_record = {
                "FIRST_NAME": first_name,
                "LAST_NAME": last_name,
                "TITLE": contact.get("title") or "",
                "EMAIL": contact.get("email") or "",
                "PHONE": contact.get("phone") or "",
                "ORGANISATION_NAME": hotel_name,
                "WEBSITE": contact.get("linkedin") or "",
                "ADDRESS_CITY": city,
                "ADDRESS_STATE": state,
                "ADDRESS_COUNTRY": country or "United States",
                "INDUSTRY": "Hotel",
                "LEAD_SOURCE_ID": SLH_LEAD_SOURCE_ID,
                "LEAD_DESCRIPTION": "\n".join(desc_parts)
                if desc_parts
                else f"Contact at {hotel_name}",
                "CUSTOMFIELDS": [
                    {"FIELD_NAME": "Brand__c", "FIELD_VALUE": brand or ""},
                    {
                        "FIELD_NAME": "Brand_Tier__c",
                        "FIELD_VALUE": tier_display.get(brand_tier, brand_tier or ""),
                    },
                    {"FIELD_NAME": "Lead_Score__c", "FIELD_VALUE": lead_score or 0},
                    {
                        "FIELD_NAME": "Management_Company__c",
                        "FIELD_VALUE": management_company or "",
                    },
                    {"FIELD_NAME": "Source_URL__c", "FIELD_VALUE": source_url or ""},
                    {"FIELD_NAME": "Developer__c", "FIELD_VALUE": developer or ""},
                    {"FIELD_NAME": "Owner_Company__c", "FIELD_VALUE": owner or ""},
                    {"FIELD_NAME": "Room_Count__c", "FIELD_VALUE": room_count or 0},
                    {
                        "FIELD_NAME": "Opening_Date__c",
                        "FIELD_VALUE": opening_date or "",
                    },
                    {"FIELD_NAME": "SLH_Lead_ID__c", "FIELD_VALUE": slh_lead_id or 0},
                ],
            }

            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        f"{self.base_url}/Leads",
                        headers=self.headers,
                        json=lead_record,
                    )

                    if resp.status_code in (200, 201):
                        result = resp.json()
                        lead_id = result.get("LEAD_ID")
                        logger.info(
                            f"Insightly: pushed '{name}' ({contact.get('title', '')}) "
                            f"at {hotel_name} → Lead ID {lead_id}"
                        )
                        results.append((name, lead_id))
                    else:
                        logger.error(
                            f"Insightly: failed to push '{name}': "
                            f"{resp.status_code} — {resp.text[:200]}"
                        )
                        results.append((name, None))

            except httpx.RequestError as e:
                logger.error(f"Insightly: request error pushing '{name}': {e}")
                results.append((name, None))

        return results

    async def check_duplicate(self, slh_lead_id: int) -> bool:
        """Check if a lead with this SLH ID already exists in Insightly."""
        if not self.enabled:
            return False

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{self.base_url}/Leads/Search",
                    headers=self.headers,
                    params={
                        "$filter": f"SLH_Lead_ID__c eq {slh_lead_id}",
                        "$top": 1,
                    },
                )
                if resp.status_code == 200:
                    return len(resp.json()) > 0
        except httpx.RequestError:
            pass
        return False

    async def delete_leads_by_slh_id(self, slh_lead_id: int) -> int:
        """Delete only Smart Lead Hunter leads matching an SLH Lead ID.

        SAFETY: Only deletes leads where LEAD_SOURCE_ID matches SLH
        AND SLH_Lead_ID custom field matches. Never touches other leads.
        """
        if not self.enabled:
            return 0

        deleted = 0
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Get ALL leads and filter locally (Search endpoint is unreliable)
                resp = await client.get(
                    f"{self.base_url}/Leads",
                    headers=self.headers,
                )
                if resp.status_code == 200:
                    for lead in resp.json():
                        # SAFETY: Only touch Smart Lead Hunter leads
                        if lead.get("LEAD_SOURCE_ID") != SLH_LEAD_SOURCE_ID:
                            continue

                        # Match by SLH_Lead_ID custom field
                        slh_id_match = False
                        for cf in lead.get("CUSTOMFIELDS", []):
                            if cf.get("FIELD_NAME") == "SLH_Lead_ID__c":
                                if int(cf.get("FIELD_VALUE", 0)) == slh_lead_id:
                                    slh_id_match = True
                                break

                        if not slh_id_match:
                            continue

                        lid = lead.get("LEAD_ID")
                        del_resp = await client.delete(
                            f"{self.base_url}/Leads/{lid}",
                            headers=self.headers,
                        )
                        if del_resp.status_code == 202:
                            deleted += 1
                            logger.info(
                                f"Insightly: deleted SLH Lead ID {lid} (SLH #{slh_lead_id})"
                            )
                else:
                    logger.error(
                        f"Insightly: failed to fetch leads for cleanup: {resp.status_code}"
                    )
        except httpx.RequestError as e:
            logger.error(f"Insightly: error deleting leads: {e}")

        return deleted

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
