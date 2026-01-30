"""
SMART LEAD HUNTER - LEAD EXTRACTION PIPELINE V3
================================================
NOW WITH GOOGLE GEMINI!

AI Providers:
- PRIMARY: Google Gemini (fast, generous free tier)
- FALLBACK: Ollama (local, unlimited but slow)
- REMOVED: Groq (rate limits too aggressive)

Improved extraction with:
1. Context-aware extraction (no random data grabbing)
2. Lead priority scoring (HOT/WARM/COLD based on opening date)
3. Management company extraction (CRITICAL for finding contacts!)
4. Better contact filtering (operations vs PR)
5. Room count validation (only for THIS specific project)

Compatible with existing orchestrator.py
"""

import asyncio
import json
import logging
import re
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND CONSTANTS
# =============================================================================

class LeadPriority(Enum):
    """Lead priority based on opening date"""
    HOT = "🔴 HOT"           # 0-9 months - ACT NOW!
    WARM = "🟠 WARM"         # 9-18 months - Build relationship
    DEVELOPING = "🟡 DEVELOPING"  # 18-24 months - Monitor
    COLD = "🔵 COLD"         # 24+ months - Track only
    MISSED = "⚫ MISSED"     # Already opened or <3 months
    UNKNOWN = "⚪ UNKNOWN"   # No opening date


class ContactRelevance(Enum):
    """Contact relevance for uniform sales"""
    HIGH = "HIGH"       # GM, Exec Housekeeper, Purchasing Director
    MEDIUM = "MEDIUM"   # Director of Rooms, HR Director
    LOW = "LOW"         # PR, Marketing, Communications
    CORPORATE = "CORPORATE"  # Corporate level


# Budget brands to skip
SKIP_BRANDS = [
    "motel 6", "super 8", "days inn", "red roof", "econo lodge",
    "rodeway inn", "travelodge", "knights inn", "americas best value",
    "quality inn", "comfort inn"  # These are midscale, might want to include
]

# International locations to skip
SKIP_COUNTRIES = [
    "china", "japan", "korea", "india", "thailand", "vietnam", "singapore",
    "malaysia", "indonesia", "philippines", "australia", "new zealand",
    "uk", "united kingdom", "england", "france", "germany", "italy", "spain",
    "portugal", "greece", "netherlands", "belgium", "switzerland", "austria",
    "saudi arabia", "uae", "dubai", "qatar", "egypt", "morocco",
    "brazil", "argentina", "chile", "colombia", "peru"
]

# Caribbean locations (INCLUDE these)
CARIBBEAN_LOCATIONS = [
    "bahamas", "jamaica", "cayman islands", "turks and caicos", "bermuda",
    "barbados", "st. lucia", "saint lucia", "antigua", "aruba", "puerto rico",
    "us virgin islands", "british virgin islands", "st. kitts", "anguilla",
    "dominican republic", "curacao", "bonaire", "grenada", "st. martin",
    "st. maarten", "martinique", "guadeloupe", "trinidad", "tobago"
]


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ExtractedLead:
    """A lead extracted from an article - compatible with orchestrator"""
    # Basic Info
    hotel_name: str = ""
    brand: str = ""
    
    # Location
    city: str = ""
    state: str = ""
    country: str = "USA"
    
    # Project Details
    opening_date: str = ""
    room_count: Optional[int] = None
    room_count_confidence: str = ""  # "confirmed", "estimated", "unclear"
    hotel_type: str = ""
    project_type: str = ""  # "new_construction", "renovation", "conversion"
    
    # Key Stakeholders (NEW!)
    management_company: str = ""
    developer: str = ""
    owner: str = ""
    
    # Investment
    investment_amount: str = ""
    
    # Amenities & Features
    amenities: str = ""
    key_insights: str = ""
    
    # Contact Info
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    contact_company: str = ""  # Which company they work for
    contact_relevance: str = ""  # HIGH, MEDIUM, LOW
    
    # Lead Priority (NEW!)
    lead_priority: str = ""
    lead_priority_reason: str = ""
    months_to_opening: Optional[int] = None
    uniform_decision_window: str = ""  # "NOW", "SOON", "LATER", "MISSED"
    
    # Qualification (NEW!)
    qualification_score: int = 0
    brand_tier: str = ""  # tier1, tier2, tier3, tier4, unknown
    location_type: str = ""  # florida, caribbean, usa, international
    estimated_revenue: int = 0  # Estimated uniform order value in $
    estimated_staff: int = 0  # Estimated staff count
    revenue_breakdown: dict = field(default_factory=dict)  # Full breakdown
    skip_reason: str = ""  # Why lead was filtered out (if any)
    
    # Meta
    confidence_score: float = 0.0
    source_url: str = ""
    source_name: str = ""
    source_urls: str = ""  # Comma-separated list for merged leads
    source_names: str = ""  # Comma-separated list for merged leads
    merged_from_count: int = 1  # Number of leads merged into this one
    scraped_at: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)


@dataclass
class ExtractionResult:
    """Result from extraction - compatible with orchestrator"""
    leads: List[ExtractedLead] = field(default_factory=list)
    success: bool = False
    error: Optional[str] = None
    source_url: str = ""
    source_name: str = ""


# =============================================================================
# LEAD PRIORITY CALCULATOR
# =============================================================================

class LeadPriorityCalculator:
    """
    Calculates lead priority based on opening date.
    
    UNIFORM SALES TIMELINE:
    - Procurement decisions: 6-12 months before opening
    - Uniform orders: 3-6 months before opening
    - Final delivery: 1-2 months before opening
    
    PRIORITY MAPPING:
    - HOT (0-9 months): Decisions happening NOW - urgent outreach!
    - WARM (9-18 months): Perfect timing for proposals
    - DEVELOPING (18-24 months): Build relationships now
    - COLD (24+ months): Too early, just track
    - MISSED (<3 months or opened): Too late for new vendor
    """
    
    MONTH_MAPPING = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12,
    }
    
    QUARTER_MAPPING = {
        'q1': 2, 'q2': 5, 'q3': 8, 'q4': 11,
        'first quarter': 2, 'second quarter': 5,
        'third quarter': 8, 'fourth quarter': 11,
    }
    
    SEASON_MAPPING = {
        'spring': 4, 'summer': 7, 'fall': 10, 'autumn': 10, 'winter': 1,
        'early': 3, 'mid': 6, 'late': 10,
    }
    
    def parse_opening_date(self, date_str: str) -> Optional[Tuple[int, int]]:
        """
        Parse opening date string to (year, month).
        
        Handles:
        - "2026" -> (2026, 6)  # Assume mid-year
        - "Q2 2026" -> (2026, 5)
        - "early 2026" -> (2026, 3)
        - "June 2026" -> (2026, 6)
        - "spring 2026" -> (2026, 4)
        """
        if not date_str:
            return None
        
        date_lower = date_str.lower().strip()
        
        # Already opened?
        if any(word in date_lower for word in ['opened', 'open now', 'recently opened', 'just opened', 'last week', 'this week']):
            return (datetime.now().year, datetime.now().month)
        
        # Extract year
        year_match = re.search(r'20\d{2}', date_str)
        if not year_match:
            return None
        year = int(year_match.group())
        
        # Try to extract month
        month = 6  # Default to mid-year
        
        # Check for specific month
        for month_name, month_num in self.MONTH_MAPPING.items():
            if month_name in date_lower:
                month = month_num
                break
        else:
            # Check for quarter
            for quarter, month_num in self.QUARTER_MAPPING.items():
                if quarter in date_lower:
                    month = month_num
                    break
            else:
                # Check for season/early/mid/late
                for season, month_num in self.SEASON_MAPPING.items():
                    if season in date_lower:
                        month = month_num
                        break
        
        return (year, month)
    
    def calculate_months_to_opening(self, date_str: str) -> Optional[int]:
        """Calculate months from now until opening"""
        parsed = self.parse_opening_date(date_str)
        if not parsed:
            return None
        
        year, month = parsed
        now = datetime.now()
        
        opening_date = datetime(year, month, 15)  # Assume mid-month
        months = (opening_date.year - now.year) * 12 + (opening_date.month - now.month)
        
        return months
    
    def calculate_priority(self, date_str: str) -> Tuple[LeadPriority, str, Optional[int], str]:
        """
        Calculate lead priority.
        
        Returns: (priority, reason, months_to_opening, decision_window)
        """
        months = self.calculate_months_to_opening(date_str)
        
        if months is None:
            return (
                LeadPriority.UNKNOWN, 
                "No opening date found",
                None,
                "UNKNOWN"
            )
        
        if months < 0:
            return (
                LeadPriority.MISSED,
                f"Already opened ({abs(months)} months ago)",
                months,
                "MISSED"
            )
        
        if months < 3:
            return (
                LeadPriority.MISSED,
                f"Opening in {months} months - uniforms likely purchased",
                months,
                "MISSED"
            )
        
        if months <= 9:
            return (
                LeadPriority.HOT,
                f"Opening in {months} months - UNIFORM DECISIONS NOW!",
                months,
                "NOW"
            )
        
        if months <= 18:
            return (
                LeadPriority.WARM,
                f"Opening in {months} months - perfect timing",
                months,
                "SOON"
            )
        
        if months <= 24:
            return (
                LeadPriority.DEVELOPING,
                f"Opening in {months} months - build relationship",
                months,
                "LATER"
            )
        
        return (
            LeadPriority.COLD,
            f"Opening in {months} months - too early",
            months,
            "LATER"
        )


# =============================================================================
# CONTACT RELEVANCE CLASSIFIER
# =============================================================================

class ContactRelevanceClassifier:
    """Classifies contacts by relevance for uniform sales"""
    
    HIGH_RELEVANCE_TITLES = [
        'general manager', 'gm',
        'executive housekeeper', 'director of housekeeping',
        'director of purchasing', 'purchasing manager', 'procurement',
        'director of operations', 'operations manager',
        'pre-opening director', 'pre-opening manager',
        'director of rooms', 'rooms division',
        'hotel manager', 'resident manager',
    ]
    
    MEDIUM_RELEVANCE_TITLES = [
        'hr director', 'human resources',
        'food and beverage director', 'f&b director',
        'front office manager',
        'director of finance', 'controller',
        'chief engineer', 'director of engineering',
        'regional manager', 'area manager',
    ]
    
    LOW_RELEVANCE_TITLES = [
        'vp communications', 'communications director',
        'pr manager', 'public relations',
        'marketing director', 'marketing manager',
        'social media', 'brand manager',
        'investor relations', 'media relations',
        'svp', 'senior vice president',  # Usually too high level
        'ceo', 'cfo', 'coo',  # C-suite usually won't handle uniform purchasing
    ]
    
    @classmethod
    def classify(cls, title: str) -> Tuple[str, str]:
        """
        Classify contact relevance.
        
        Returns: (relevance_level, reason)
        """
        if not title:
            return ContactRelevance.MEDIUM.value, "No title provided"
        
        title_lower = title.lower()
        
        for t in cls.HIGH_RELEVANCE_TITLES:
            if t in title_lower:
                return ContactRelevance.HIGH.value, f"Key decision maker"
        
        for t in cls.MEDIUM_RELEVANCE_TITLES:
            if t in title_lower:
                return ContactRelevance.MEDIUM.value, f"Influencer"
        
        for t in cls.LOW_RELEVANCE_TITLES:
            if t in title_lower:
                return ContactRelevance.LOW.value, f"PR/Marketing - not purchasing"
        
        return ContactRelevance.MEDIUM.value, "Relevance unclear"


# =============================================================================
# MAIN EXTRACTION PIPELINE (Compatible with orchestrator)
# =============================================================================

class LeadExtractionPipeline:
    """
    AI-powered lead extraction with V3 improvements.
    
    AI PROVIDERS:
    - PRIMARY: Google Gemini (fast, $300 free credits)
    - FALLBACK: Ollama (local, unlimited but slow)
    
    Compatible with existing orchestrator.py
    """
    
    def __init__(self, gemini_api_key: Optional[str] = None, use_ollama: bool = True):
        """
        Initialize the extraction pipeline.
        
        Args:
            gemini_api_key: Google Gemini API key (or set GEMINI_API_KEY env var)
            use_ollama: Whether to use Ollama as fallback
        """
        # Get API key from parameter or environment
        self.gemini_api_key = gemini_api_key or os.getenv("GEMINI_API_KEY")
        self.use_ollama = use_ollama
        self.priority_calculator = LeadPriorityCalculator()
        
        # Rate limiting for Gemini - IMPORTANT!
        # Free tier: ~10-15 RPM, so we need 4+ seconds between calls
        self.min_delay = 4.0  # 4 seconds = 15 requests per minute max
        self.last_ai_call = 0.0
        
        # Track which provider is being used
        self.gemini_available = bool(self.gemini_api_key)
        self.gemini_consecutive_errors = 0
        self.gemini_cooldown_until = 0.0
        
        if self.gemini_available:
            logger.info("✅ Gemini API key found - using Gemini as PRIMARY")
        else:
            logger.warning("⚠️ No Gemini API key - using Ollama only")
    
    def _get_extraction_prompt(self, content: str, source_url: str) -> str:
        """Generate the improved AI extraction prompt"""
        
        return f"""You are an expert at extracting hotel development information from news articles.

CRITICAL RULES:

1. CONTEXT MATTERS - Only extract info SPECIFICALLY about the hotel being announced:
   - Do NOT grab company-wide statistics
   - Do NOT grab data about other properties mentioned
   - ONLY extract data that explicitly refers to THIS specific project

2. ROOM COUNT RULES:
   - Only extract room_count if explicitly stated for THIS hotel (e.g., "176-room hotel")
   - If the article says "company has 3,950 rooms total" - that's NOT the room count for this hotel
   - Set room_count_confidence: "confirmed" if explicit, "unclear" if ambiguous

3. MANAGEMENT COMPANY (CRITICAL!):
   - Look for: "operated by", "managed by", "management company"
   - Examples: Sage Hospitality, Aimbridge, Highgate, Crescent Hotels, CoralTree, Pyramid Global
   - This is KEY for finding contacts later!

4. CONTACTS:
   - Extract ALL contacts mentioned
   - Note which COMPANY they work for (the hotel? management company? brand corporate?)
   - Prioritize operational roles: GM, Executive Housekeeper, Purchasing Director
   - Flag PR/Communications contacts as low relevance

5. TARGET MARKETS ONLY:
   - USA (all 50 states) ✅
   - Caribbean (Bahamas, Jamaica, Cayman Islands, etc.) ✅
   - Skip: Europe, Asia, Middle East, South America ❌

6. SKIP BUDGET BRANDS:
   - Motel 6, Super 8, Days Inn, Red Roof, Econo Lodge ❌
   - Focus on 4-star+ (Marriott, Hilton, Hyatt, IHG luxury brands, etc.) ✅

Return JSON array. For each hotel:

```json
[
  {{
    "hotel_name": "Exact hotel name",
    "brand": "Brand (Ritz-Carlton, Four Seasons, etc.)",
    "city": "City",
    "state": "Full state name",
    "country": "USA or Caribbean country",
    "opening_date": "Exact wording (e.g., 'early 2028', 'Q2 2026')",
    "room_count": 176,
    "room_count_confidence": "confirmed",
    "hotel_type": "luxury/upscale/lifestyle/boutique/resort",
    "project_type": "new_construction/renovation/conversion",
    "management_company": "Who operates it (Sage, Aimbridge, etc.)",
    "developer": "Who is developing it",
    "owner": "Who owns it",
    "investment_amount": "$X million if mentioned",
    "amenities": "Key amenities (spa, pool, meeting space sq ft, restaurants)",
    "key_insights": "Any unique details (target market, unique features)",
    "contact_name": "Name if found",
    "contact_title": "Title",
    "contact_email": "Email if found",
    "contact_phone": "Phone if found",
    "contact_company": "Which company they work for"
  }}
]
```

If NO hotels found in target markets, return: []
If ONLY budget brands found, return: []

ARTICLE:
{content[:12000]}

SOURCE: {source_url}
"""
    
    async def _call_gemini(self, prompt: str) -> Optional[str]:
        """Call Google Gemini API"""
        import time
        
        if not self.gemini_api_key:
            return None
        
        # Check cooldown
        now = time.time()
        if now < self.gemini_cooldown_until:
            remaining = int(self.gemini_cooldown_until - now)
            logger.debug(f"Gemini in cooldown, {remaining}s remaining")
            return None
        
        # Enforce minimum delay
        elapsed = now - self.last_ai_call
        if elapsed < self.min_delay:
            await asyncio.sleep(self.min_delay - elapsed)
        
        # Retry logic for 503 errors (model overloaded)
        max_retries = 3
        retry_delay = 5  # Start with 5 seconds
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key={self.gemini_api_key}",
                        json={
                            "contents": [{
                                "parts": [{
                                    "text": prompt
                                }]
                            }],
                            "generationConfig": {
                                "temperature": 0.1,
                                "maxOutputTokens": 4000,
                            }
                        },
                        headers={"Content-Type": "application/json"}
                    )
                    
                    self.last_ai_call = time.time()
                    
                    if response.status_code == 200:
                        data = response.json()
                        # Extract text from Gemini response
                        try:
                            text = data["candidates"][0]["content"]["parts"][0]["text"]
                            self.gemini_consecutive_errors = 0  # Reset on success
                            return text
                        except (KeyError, IndexError) as e:
                            logger.error(f"Gemini response parsing error: {e}")
                            return None
                    
                    elif response.status_code == 503:
                        # Model overloaded - retry after delay
                        if attempt < max_retries - 1:
                            logger.info(f"⏳ Gemini overloaded, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff: 5s, 10s, 20s
                            continue
                        else:
                            logger.warning(f"⚠️ Gemini still overloaded after {max_retries} retries")
                            return None
                    
                    elif response.status_code == 429:
                        # Rate limited
                        self.gemini_consecutive_errors += 1
                        cooldown = min(60 * (1.5 ** (self.gemini_consecutive_errors - 1)), 300)
                        self.gemini_cooldown_until = time.time() + cooldown
                        logger.warning(f"⚠️ Gemini rate limited! Cooldown: {cooldown:.0f}s")
                        return None
                    
                    else:
                        logger.error(f"Gemini error: HTTP {response.status_code} - {response.text[:200]}")
                        return None
                        
            except Exception as e:
                self.last_ai_call = time.time()
                logger.error(f"Gemini error: {e}")
                return None
        
        return None
    
    async def _call_ollama(self, prompt: str) -> Optional[str]:
        """Call local Ollama as fallback"""
        import time
        
        if not self.use_ollama:
            return None
        
        # Enforce minimum delay
        now = time.time()
        elapsed = now - self.last_ai_call
        if elapsed < self.min_delay:
            await asyncio.sleep(self.min_delay - elapsed)
        
        try:
            async with httpx.AsyncClient(timeout=180.0) as client:
                response = await client.post(
                    "http://localhost:11434/api/generate",
                    json={
                        "model": "llama3.2",
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1}
                    }
                )
                self.last_ai_call = time.time()
                
                if response.status_code == 200:
                    return response.json().get("response", "")
                    
        except asyncio.TimeoutError:
            logger.warning("⏳ Ollama timeout (180s)")
        except Exception as e:
            logger.error(f"Ollama error: {e}")
        
        self.last_ai_call = time.time()
        return None
    
    def _parse_response(self, response: str) -> List[Dict]:
        """Parse AI response to JSON"""
        if not response:
            return []
        
        try:
            # Find JSON array in response
            match = re.search(r'\[[\s\S]*\]', response)
            if match:
                return json.loads(match.group())
        except json.JSONDecodeError:
            pass
        
        try:
            result = json.loads(response)
            return result if isinstance(result, list) else [result] if isinstance(result, dict) else []
        except:
            pass
        
        return []
    
    def _is_valid_location(self, state: str, country: str) -> bool:
        """Check if location is in target market"""
        country_lower = (country or "").lower()
        
        # Skip international
        for skip in SKIP_COUNTRIES:
            if skip in country_lower:
                return False
        
        # Allow Caribbean
        for caribbean in CARIBBEAN_LOCATIONS:
            if caribbean in country_lower:
                return True
        
        # Allow USA
        if country_lower in ["usa", "us", "united states", ""]:
            return True
        
        return False
    
    def _is_valid_brand(self, brand: str) -> bool:
        """Check if brand is 4-star+"""
        brand_lower = (brand or "").lower()
        for skip in SKIP_BRANDS:
            if skip in brand_lower:
                return False
        return True
    
    def _process_lead(self, raw: Dict, source_url: str, source_name: str) -> Optional[ExtractedLead]:
        """Process raw extraction into ExtractedLead with priority scoring"""
        
        # Validate location
        if not self._is_valid_location(raw.get("state", ""), raw.get("country", "")):
            return None
        
        # Validate brand
        if not self._is_valid_brand(raw.get("brand", "")):
            return None
        
        # Calculate priority
        priority, reason, months, window = self.priority_calculator.calculate_priority(
            raw.get("opening_date", "")
        )
        
        # Classify contact relevance
        contact_relevance, _ = ContactRelevanceClassifier.classify(
            raw.get("contact_title", "")
        )
        
        # Validate room count (sanity check)
        room_count = raw.get("room_count")
        if room_count:
            try:
                room_count = int(room_count)
                if room_count > 5000:  # Probably company-wide, not this hotel
                    room_count = None
            except (ValueError, TypeError):
                room_count = None
        
        lead = ExtractedLead(
            hotel_name=raw.get("hotel_name", ""),
            brand=raw.get("brand", ""),
            city=raw.get("city", ""),
            state=raw.get("state", ""),
            country=raw.get("country", "USA"),
            opening_date=raw.get("opening_date", ""),
            room_count=room_count,
            room_count_confidence=raw.get("room_count_confidence", ""),
            hotel_type=raw.get("hotel_type", ""),
            project_type=raw.get("project_type", ""),
            management_company=raw.get("management_company", ""),
            developer=raw.get("developer", ""),
            owner=raw.get("owner", ""),
            investment_amount=raw.get("investment_amount", ""),
            amenities=raw.get("amenities", ""),
            key_insights=raw.get("key_insights", ""),
            contact_name=raw.get("contact_name", ""),
            contact_title=raw.get("contact_title", ""),
            contact_email=raw.get("contact_email", ""),
            contact_phone=raw.get("contact_phone", ""),
            contact_company=raw.get("contact_company", ""),
            contact_relevance=contact_relevance,
            lead_priority=priority.value,
            lead_priority_reason=reason,
            months_to_opening=months,
            uniform_decision_window=window,
            confidence_score=self._calculate_confidence(raw),
            source_url=source_url,
            source_name=source_name,
            scraped_at=datetime.now().isoformat()
        )
        
        return lead
    
    def _calculate_confidence(self, raw: Dict) -> float:
        """Calculate confidence score for extracted data"""
        score = 0.0
        
        # Essential fields
        if raw.get("hotel_name"): score += 0.20
        if raw.get("city"): score += 0.10
        if raw.get("state"): score += 0.10
        
        # Project details
        if raw.get("opening_date"): score += 0.15
        if raw.get("room_count"): score += 0.10
        if raw.get("room_count_confidence") == "confirmed": score += 0.05
        
        # Key stakeholders (very valuable!)
        if raw.get("management_company"): score += 0.10
        if raw.get("developer"): score += 0.05
        
        # Contact info
        if raw.get("contact_name"): score += 0.10
        if raw.get("contact_email"): score += 0.05
        
        return min(score, 1.0)
    
    async def extract(
        self,
        content: str,
        source_url: str = "",
        source_name: str = ""
    ) -> ExtractionResult:
        """
        Extract leads from content.
        
        Uses Gemini as PRIMARY, Ollama as FALLBACK.
        Compatible with orchestrator.py
        """
        import time
        
        result = ExtractionResult(source_url=source_url, source_name=source_name)
        
        if not content or len(content) < 100:
            result.error = "Content too short"
            return result
        
        # Generate prompt
        prompt = self._get_extraction_prompt(content, source_url)
        
        response = None
        
        # Try Gemini first (PRIMARY)
        now = time.time()
        if self.gemini_available and now >= self.gemini_cooldown_until:
            response = await self._call_gemini(prompt)
            if response:
                logger.debug("✅ Using Gemini response")
        elif self.gemini_cooldown_until > now:
            remaining = int(self.gemini_cooldown_until - now)
            logger.debug(f"⏸️ Gemini in cooldown ({remaining}s left), using Ollama")
        
        # Fallback to Ollama
        if not response:
            logger.info("⏳ Using Ollama (fallback)...")
            response = await self._call_ollama(prompt)
        
        if not response:
            result.error = "No AI response"
            return result
        
        # Parse response
        raw_leads = self._parse_response(response)
        
        # Process each lead
        for raw in raw_leads:
            lead = self._process_lead(raw, source_url, source_name)
            if lead and lead.hotel_name:
                result.leads.append(lead)
        
        result.success = True
        return result


# =============================================================================
# DEDUPLICATOR (Compatible with orchestrator)
# =============================================================================

class LeadDeduplicator:
    """Deduplicate leads based on hotel name and location"""
    
    @staticmethod
    def normalize_name(name: str) -> str:
        if not name:
            return ""
        name = name.lower()
        name = re.sub(r'[^\w\s]', '', name)
        stopwords = ['the', 'hotel', 'resort', 'spa', 'and', 'at', 'by', 'a']
        words = [w for w in name.split() if w not in stopwords]
        return ' '.join(sorted(words))
    
    @staticmethod
    def are_duplicates(lead1: ExtractedLead, lead2: ExtractedLead) -> bool:
        name1 = LeadDeduplicator.normalize_name(lead1.hotel_name)
        name2 = LeadDeduplicator.normalize_name(lead2.hotel_name)
        
        if not name1 or not name2:
            return False
        
        words1, words2 = set(name1.split()), set(name2.split())
        if not words1 or not words2:
            return False
        
        similarity = len(words1 & words2) / len(words1 | words2)
        
        # Safe comparison handling None values
        same_location = False
        if lead1.city and lead2.city and lead1.state and lead2.state:
            same_location = (
                lead1.city.lower() == lead2.city.lower() and
                lead1.state.lower() == lead2.state.lower()
            )
        elif lead1.city and lead2.city:
            # Just compare cities if states are missing
            same_location = lead1.city.lower() == lead2.city.lower()
        
        # Also check brand + state match (catches "Six Senses Utah" variants)
        same_brand_state = False
        if lead1.brand and lead2.brand and lead1.state and lead2.state:
            same_brand_state = (
                lead1.brand.lower() == lead2.brand.lower() and
                lead1.state.lower() == lead2.state.lower()
            )
        elif lead1.brand and lead2.brand:
            # Just compare brands if states are missing
            same_brand_state = lead1.brand.lower() == lead2.brand.lower()
        
        # Also check country match for Caribbean leads (often no state)
        same_country = False
        if lead1.country and lead2.country:
            same_country = lead1.country.lower() == lead2.country.lower()
        
        return (
            (similarity > 0.6 and same_location) or 
            (similarity > 0.6 and same_country) or
            similarity > 0.8 or 
            (same_brand_state and similarity > 0.4)
        )
    
    @staticmethod
    def merge_leads(lead1: ExtractedLead, lead2: ExtractedLead) -> ExtractedLead:
        """Merge duplicates, keeping best data"""
        merged = lead1 if lead1.confidence_score >= lead2.confidence_score else lead2
        other = lead2 if lead1.confidence_score >= lead2.confidence_score else lead1
        
        # Fill gaps
        if not merged.room_count and other.room_count:
            merged.room_count = other.room_count
        if not merged.management_company and other.management_company:
            merged.management_company = other.management_company
        if not merged.developer and other.developer:
            merged.developer = other.developer
        if not merged.contact_name and other.contact_name:
            merged.contact_name = other.contact_name
            merged.contact_title = other.contact_title
            merged.contact_email = other.contact_email
        if not merged.opening_date and other.opening_date:
            merged.opening_date = other.opening_date
        
        return merged
    
    @staticmethod
    def deduplicate(leads: List[ExtractedLead]) -> List[ExtractedLead]:
        """Remove duplicates from lead list"""
        if not leads:
            return []
        
        unique = []
        for lead in leads:
            is_dup = False
            for i, existing in enumerate(unique):
                if LeadDeduplicator.are_duplicates(lead, existing):
                    unique[i] = LeadDeduplicator.merge_leads(existing, lead)
                    is_dup = True
                    break
            if not is_dup:
                unique.append(lead)
        
        return unique


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def create_pipeline(gemini_api_key: Optional[str] = None) -> LeadExtractionPipeline:
    """Create extraction pipeline with best available AI"""
    return LeadExtractionPipeline(
        gemini_api_key=gemini_api_key,
        use_ollama=True  # Always have Ollama as fallback
    )


# =============================================================================
# TEST
# =============================================================================

async def test_extraction():
    """Test the extraction pipeline"""
    
    print("=" * 60)
    print("LEAD EXTRACTION PIPELINE V3 - TEST")
    print("=" * 60)
    
    # Check for API key
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        print(f"✅ Gemini API key found: {gemini_key[:10]}...")
    else:
        print("⚠️ No GEMINI_API_KEY in environment")
        print("   Set it with: set GEMINI_API_KEY=your-key-here")
    
    pipeline = LeadExtractionPipeline()
    
    # Test content
    test_content = """
    Ritz-Carlton to Open New Luxury Hotel in Indianapolis
    
    The Ritz-Carlton Hotel Company announced plans for a new 176-room luxury hotel 
    in downtown Indianapolis, expected to open in early 2028. The property will be 
    developed by Boxcar Development and operated by Sage Hospitality Group.
    
    The hotel will feature 11,500 square feet of meeting space, a full-service spa,
    and multiple dining outlets.
    
    "We're excited to bring the Ritz-Carlton brand to Indianapolis," said Kathy Heneghan,
    General Manager of the nearby Signia by Hilton hotel.
    """
    
    print("\n🔍 Testing extraction...")
    result = await pipeline.extract(test_content, "https://test.com/article", "Test Source")
    
    if result.success and result.leads:
        lead = result.leads[0]
        print(f"\n✅ Extracted lead:")
        print(f"   Hotel: {lead.hotel_name}")
        print(f"   Location: {lead.city}, {lead.state}")
        print(f"   Opening: {lead.opening_date}")
        print(f"   Rooms: {lead.room_count}")
        print(f"   Management: {lead.management_company}")
        print(f"   Priority: {lead.lead_priority}")
    else:
        print(f"\n❌ Extraction failed: {result.error}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(test_extraction())