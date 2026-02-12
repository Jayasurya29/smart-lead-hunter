"""
Insightly CRM Integration Service
---------------------------------
Handles all interactions with Insightly API v3.1

H-06 FIX: OData filter injection — single quotes in hotel names/values are now
escaped before being interpolated into OData $filter expressions.
H-07 FIX: _get_lead_source_id() now caches the result so it's not fetched
from the API on every convert_to_lead() call.

API Documentation: https://api.insightly.com/v3.1/Help
"""

import httpx
import logging
from typing import Optional, Dict, Any, List

from ..config import settings

logger = logging.getLogger(__name__)


def _escape_odata_string(value: str) -> str:
    """H-06: Escape single quotes for OData filter expressions.

    OData uses single quotes to delimit strings. A literal single quote
    inside a value must be doubled: O'Brien → O''Brien.
    Without this, a hotel name like "Marriott's Resort" would break the
    filter syntax, and a malicious value could inject arbitrary filter logic.
    """
    if not value:
        return value
    return value.replace("'", "''")


class InsightlyClient:
    """
    Client for Insightly CRM API v3.1

    Usage:
        client = InsightlyClient()

        # Create a potential lead
        lead_data = {
            "hotel_name": "Four Seasons Naples",
            "contact_email": "info@fsnaples.com",
            "city": "Naples",
            "state": "Florida",
            "lead_score": 85
        }
        result = await client.create_potential_lead(lead_data)

        # Move approved lead to standard Leads
        await client.convert_to_lead(potential_lead_id=123)
    """

    def __init__(self):
        """Initialize Insightly client with API credentials"""
        self.api_key = settings.insightly_api_key
        self.base_url = settings.insightly_api_url

        # H-07: Cache for lead source ID (fetched once, reused)
        self._lead_source_id_cache: Dict[str, Optional[int]] = {}

        # Validate configuration
        if not self.api_key:
            logger.warning("Insightly API key not configured. CRM sync disabled.")
            self.enabled = False
        else:
            self.enabled = True
            logger.info(f"Insightly client initialized: {self.base_url}")

    @property
    def headers(self) -> Dict[str, str]:
        """Standard headers for Insightly API requests"""
        return {
            "Authorization": f"Basic {self._encode_api_key()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _encode_api_key(self) -> str:
        """Encode API key for Basic auth (key:blank password)"""
        import base64

        credentials = f"{self.api_key}:"
        return base64.b64encode(credentials.encode()).decode()

    # -------------------------------------------------------------------------
    # Potential Leads (Custom Object)
    # -------------------------------------------------------------------------

    async def create_potential_lead(self, lead_data: Dict[str, Any]) -> Optional[Dict]:
        """
        Create a new record in the custom "Potential Leads" object

        Args:
            lead_data: Dictionary containing lead information

        Returns:
            Created record data or None if failed
        """
        if not self.enabled:
            logger.warning("Insightly not configured. Skipping create_potential_lead.")
            return None

        record = {
            "RECORD_NAME": lead_data.get("hotel_name", "Unknown Hotel"),
            "CUSTOMFIELDS": self._build_custom_fields(lead_data),
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/CustomObjects/Potential_Leads",
                    headers=self.headers,
                    json=record,
                )

                if response.status_code == 201:
                    result = response.json()
                    logger.info(
                        f"Created potential lead: {lead_data.get('hotel_name')} (ID: {result.get('RECORD_ID')})"
                    )
                    return result
                else:
                    logger.error(
                        f"Failed to create potential lead: {response.status_code} - {response.text}"
                    )
                    return None

        except httpx.RequestError as e:
            logger.error(f"Request error creating potential lead: {e}")
            return None

    async def update_potential_lead(
        self, record_id: int, updates: Dict[str, Any]
    ) -> Optional[Dict]:
        """Update an existing potential lead record"""
        if not self.enabled:
            return None

        record = {
            "RECORD_ID": record_id,
            "CUSTOMFIELDS": self._build_custom_fields(updates),
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.put(
                    f"{self.base_url}/CustomObjects/Potential_Leads",
                    headers=self.headers,
                    json=record,
                )

                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Updated potential lead ID: {record_id}")
                    return result
                else:
                    logger.error(
                        f"Failed to update potential lead: {response.status_code}"
                    )
                    return None

        except httpx.RequestError as e:
            logger.error(f"Request error updating potential lead: {e}")
            return None

    async def get_potential_lead(self, record_id: int) -> Optional[Dict]:
        """Fetch a single potential lead by ID"""
        if not self.enabled:
            return None

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.base_url}/CustomObjects/Potential_Leads/{record_id}",
                    headers=self.headers,
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(
                        f"Failed to get potential lead: {response.status_code}"
                    )
                    return None

        except httpx.RequestError as e:
            logger.error(f"Request error getting potential lead: {e}")
            return None

    async def search_potential_leads(
        self, filters: Optional[Dict[str, Any]] = None, top: int = 100
    ) -> List[Dict]:
        """
        Search potential leads with optional filters.

        H-06: String values are now escaped for OData safety.
        """
        if not self.enabled:
            return []

        try:
            params = {"$top": top}

            # Build OData filter if provided
            if filters:
                filter_parts = []
                for field_name, value in filters.items():
                    if isinstance(value, str):
                        # H-06: Escape single quotes to prevent OData injection
                        safe_value = _escape_odata_string(value)
                        filter_parts.append(f"{field_name} eq '{safe_value}'")
                    else:
                        filter_parts.append(f"{field_name} eq {value}")
                params["$filter"] = " and ".join(filter_parts)

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.base_url}/CustomObjects/Potential_Leads/Search",
                    headers=self.headers,
                    params=params,
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(
                        f"Failed to search potential leads: {response.status_code}"
                    )
                    return []

        except httpx.RequestError as e:
            logger.error(f"Request error searching potential leads: {e}")
            return []

    # -------------------------------------------------------------------------
    # Standard Leads (Convert approved potential leads)
    # -------------------------------------------------------------------------

    async def convert_to_lead(
        self, potential_lead_id: int, additional_data: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict]:
        """
        Convert an approved potential lead to a standard Lead.

        H-07: Lead source ID is now cached after first lookup.
        """
        if not self.enabled:
            return None

        # First, fetch the potential lead data
        potential_lead = await self.get_potential_lead(potential_lead_id)
        if not potential_lead:
            logger.error(
                f"Cannot convert: potential lead {potential_lead_id} not found"
            )
            return None

        # Extract custom fields from potential lead
        custom_fields = {
            cf["FIELD_NAME"]: cf["FIELD_VALUE"]
            for cf in potential_lead.get("CUSTOMFIELDS", [])
        }

        # Build standard Lead record
        lead_record = {
            "LEAD_NAME": potential_lead.get("RECORD_NAME", "Unknown"),
            "FIRST_NAME": custom_fields.get("Contact_First_Name", ""),
            "LAST_NAME": custom_fields.get("Contact_Last_Name", ""),
            "EMAIL": custom_fields.get("Contact_Email", ""),
            "PHONE": custom_fields.get("Contact_Phone", ""),
            "ORGANISATION_NAME": potential_lead.get("RECORD_NAME", ""),
            "WEBSITE": custom_fields.get("Hotel_Website", ""),
            "ADDRESS_CITY": custom_fields.get("City", ""),
            "ADDRESS_STATE": custom_fields.get("State", ""),
            "ADDRESS_COUNTRY": custom_fields.get("Country", ""),
            "LEAD_DESCRIPTION": self._build_lead_description(custom_fields),
            "LEAD_SOURCE_ID": await self._get_lead_source_id("Smart Lead Hunter"),
            "CUSTOMFIELDS": [
                {
                    "FIELD_NAME": "Source_URL__c",
                    "FIELD_VALUE": custom_fields.get("Source_URL", ""),
                },
                {
                    "FIELD_NAME": "Lead_Score__c",
                    "FIELD_VALUE": custom_fields.get("Lead_Score", 0),
                },
                {
                    "FIELD_NAME": "Opening_Date__c",
                    "FIELD_VALUE": custom_fields.get("Opening_Date", ""),
                },
                {
                    "FIELD_NAME": "Room_Count__c",
                    "FIELD_VALUE": custom_fields.get("Room_Count", 0),
                },
                {
                    "FIELD_NAME": "Hotel_Type__c",
                    "FIELD_VALUE": custom_fields.get("Hotel_Type", ""),
                },
            ],
        }

        # Merge additional data if provided
        if additional_data:
            lead_record.update(additional_data)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/Leads", headers=self.headers, json=lead_record
                )

                if response.status_code == 201:
                    result = response.json()
                    lead_id = result.get("LEAD_ID")
                    logger.info(
                        f"Converted potential lead {potential_lead_id} to Lead {lead_id}"
                    )

                    # Update potential lead status to "Converted"
                    await self.update_potential_lead(
                        potential_lead_id,
                        {"Status": "Converted", "Converted_Lead_ID": lead_id},
                    )

                    return result
                else:
                    logger.error(
                        f"Failed to create Lead: {response.status_code} - {response.text}"
                    )
                    return None

        except httpx.RequestError as e:
            logger.error(f"Request error creating Lead: {e}")
            return None

    async def check_duplicate_lead(
        self, hotel_name: str, city: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Check if a lead already exists in Insightly.

        H-06: Hotel name is escaped for OData safety.
        """
        if not self.enabled:
            return None

        # Search in Potential Leads first
        potential_results = await self.search_potential_leads(
            filters={"RECORD_NAME": hotel_name}, top=5
        )

        for result in potential_results:
            if city:
                custom_fields = {
                    cf["FIELD_NAME"]: cf["FIELD_VALUE"]
                    for cf in result.get("CUSTOMFIELDS", [])
                }
                if custom_fields.get("City", "").lower() == city.lower():
                    logger.info(f"Duplicate found in Potential Leads: {hotel_name}")
                    return {"type": "potential_lead", "record": result}
            else:
                logger.info(f"Duplicate found in Potential Leads: {hotel_name}")
                return {"type": "potential_lead", "record": result}

        # Search in standard Leads
        try:
            # H-06: Escape single quotes in hotel name for OData filter
            safe_name = _escape_odata_string(hotel_name)

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.base_url}/Leads/Search",
                    headers=self.headers,
                    params={"$filter": f"LEAD_NAME eq '{safe_name}'", "$top": 5},
                )

                if response.status_code == 200:
                    leads = response.json()
                    for lead in leads:
                        if city:
                            if lead.get("ADDRESS_CITY", "").lower() == city.lower():
                                logger.info(f"Duplicate found in Leads: {hotel_name}")
                                return {"type": "lead", "record": lead}
                        else:
                            logger.info(f"Duplicate found in Leads: {hotel_name}")
                            return {"type": "lead", "record": lead}

        except httpx.RequestError as e:
            logger.error(f"Request error checking duplicate lead: {e}")

        return None

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _build_custom_fields(self, data: Dict[str, Any]) -> List[Dict]:
        """Build Insightly custom fields array from our data"""
        field_mapping = {
            "contact_email": "Contact_Email__c",
            "contact_phone": "Contact_Phone__c",
            "contact_first_name": "Contact_First_Name__c",
            "contact_last_name": "Contact_Last_Name__c",
            "city": "City__c",
            "state": "State__c",
            "country": "Country__c",
            "opening_date": "Opening_Date__c",
            "room_count": "Room_Count__c",
            "hotel_type": "Hotel_Type__c",
            "brand": "Brand__c",
            "lead_score": "Lead_Score__c",
            "source_url": "Source_URL__c",
            "notes": "Notes__c",
            "status": "Status__c",
            "hotel_website": "Hotel_Website__c",
            "slh_id": "SLH_ID__c",
        }

        custom_fields = []
        for our_field, insightly_field in field_mapping.items():
            if our_field in data and data[our_field] is not None:
                custom_fields.append(
                    {"FIELD_NAME": insightly_field, "FIELD_VALUE": data[our_field]}
                )

        return custom_fields

    def _build_lead_description(self, custom_fields: Dict[str, Any]) -> str:
        """Build a formatted description for the Lead record"""
        parts = []

        if custom_fields.get("Opening_Date"):
            parts.append(f"Opening Date: {custom_fields['Opening_Date']}")
        if custom_fields.get("Room_Count"):
            parts.append(f"Rooms: {custom_fields['Room_Count']}")
        if custom_fields.get("Hotel_Type"):
            parts.append(f"Type: {custom_fields['Hotel_Type']}")
        if custom_fields.get("Brand"):
            parts.append(f"Brand: {custom_fields['Brand']}")
        if custom_fields.get("Lead_Score"):
            parts.append(f"Lead Score: {custom_fields['Lead_Score']}")
        if custom_fields.get("Notes"):
            parts.append(f"\nNotes: {custom_fields['Notes']}")

        return "\n".join(parts) if parts else "Imported from Smart Lead Hunter"

    async def _get_lead_source_id(self, source_name: str) -> Optional[int]:
        """
        Get the Lead Source ID for "Smart Lead Hunter".

        H-07: Result is cached after first successful lookup so we don't
        hit the API on every convert_to_lead() call.
        """
        # H-07: Return cached value if available
        if source_name in self._lead_source_id_cache:
            return self._lead_source_id_cache[source_name]

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.base_url}/LeadSources", headers=self.headers
                )

                if response.status_code == 200:
                    sources = response.json()
                    for source in sources:
                        if source.get("LEAD_SOURCE") == source_name:
                            source_id = source.get("LEAD_SOURCE_ID")
                            # H-07: Cache for future calls
                            self._lead_source_id_cache[source_name] = source_id
                            return source_id

                    logger.warning(
                        f"Lead source '{source_name}' not found in Insightly."
                    )
                    # Cache None too so we don't keep retrying
                    self._lead_source_id_cache[source_name] = None
                    return None

        except httpx.RequestError as e:
            logger.error(f"Request error getting lead sources: {e}")

        return None

    # -------------------------------------------------------------------------
    # Batch Operations
    # -------------------------------------------------------------------------

    async def bulk_create_potential_leads(
        self, leads: List[Dict[str, Any]], skip_duplicates: bool = True
    ) -> Dict[str, Any]:
        """Create multiple potential leads with duplicate checking"""
        results = {"created": 0, "skipped": 0, "failed": 0, "details": []}

        for lead_data in leads:
            hotel_name = lead_data.get("hotel_name", "Unknown")
            city = lead_data.get("city")

            if skip_duplicates:
                existing = await self.check_duplicate_lead(hotel_name, city)
                if existing:
                    results["skipped"] += 1
                    results["details"].append(
                        {
                            "hotel_name": hotel_name,
                            "status": "skipped",
                            "reason": f"Duplicate found in {existing['type']}",
                        }
                    )
                    continue

            created = await self.create_potential_lead(lead_data)
            if created:
                results["created"] += 1
                results["details"].append(
                    {
                        "hotel_name": hotel_name,
                        "status": "created",
                        "record_id": created.get("RECORD_ID"),
                    }
                )
            else:
                results["failed"] += 1
                results["details"].append(
                    {
                        "hotel_name": hotel_name,
                        "status": "failed",
                        "reason": "API error",
                    }
                )

        logger.info(
            f"Bulk create complete: {results['created']} created, "
            f"{results['skipped']} skipped, {results['failed']} failed"
        )

        return results

    # -------------------------------------------------------------------------
    # Health Check
    # -------------------------------------------------------------------------

    async def test_connection(self) -> Dict[str, Any]:
        """Test the Insightly API connection"""
        if not self.enabled:
            return {"connected": False, "error": "API key not configured"}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.base_url}/Users/Me", headers=self.headers
                )

                if response.status_code == 200:
                    user = response.json()
                    return {
                        "connected": True,
                        "user": user.get("EMAIL_ADDRESS"),
                        "instance": self.base_url,
                    }
                elif response.status_code == 401:
                    return {"connected": False, "error": "Invalid API key"}
                else:
                    return {"connected": False, "error": f"HTTP {response.status_code}"}

        except httpx.RequestError as e:
            return {"connected": False, "error": str(e)}


# Lazy singleton — avoids initializing before .env is loaded (e.g., Celery worker startup)
_insightly_client = None


def get_insightly_client() -> InsightlyClient:
    """Get or create the singleton InsightlyClient instance."""
    global _insightly_client
    if _insightly_client is None:
        _insightly_client = InsightlyClient()
    return _insightly_client


# Backward-compatible alias (reads trigger lazy init via __getattr__ won't work for module vars,
# so callers should migrate to get_insightly_client() — this is kept for existing imports)
insightly_client = None  # type: ignore


def __getattr__(name):
    """Module-level lazy access: `from insightly_crm import insightly_client`"""
    if name == "insightly_client":
        return get_insightly_client()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
