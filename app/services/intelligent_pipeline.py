"""
SMART LEAD HUNTER - INTELLIGENT MULTI-STAGE PIPELINE
=====================================================
The brain of the system - understands content, doesn't just filter words.

PHILOSOPHY:
- Don't try to block infinite junk (impossible)
- Teach AI to UNDERSTAND and DECIDE relevance
- Only extract from pages that matter
- Learn from every page processed

STAGES:
0. Quick Reject (FREE) - Structural junk URLs
1. Content Understanding (CHEAP) - What is this page about?
2. Relevance Decision (CHEAP) - Is this a new hotel opening?
3. Lead Extraction (FULL) - Extract all details
4. Lead Qualification (FULL) - Score for uniform sales potential
5. Learning (FREE) - Remember what worked

Author: Smart Lead Hunter
Version: 1.0
"""

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from pathlib import Path

import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import custom handlers for site-specific extraction
try:
    from app.services.custom_handlers import (
        has_custom_handler, 
        get_handler, 
        extract_with_handler,
        ExtractedLead as HandlerExtractedLead
    )
    CUSTOM_HANDLERS_AVAILABLE = True
    logger.info("✅ Custom site handlers loaded")
except ImportError:
    CUSTOM_HANDLERS_AVAILABLE = False
    logger.warning("⚠️ Custom handlers not available - using AI extraction only")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class PipelineConfig:
    """Configuration for the intelligent pipeline"""
    # AI Model settings
    gemini_api_key: str = ""
    classifier_model: str = "gemini-2.5-flash-lite"  # Cheapest for classification
    extractor_model: str = "gemini-2.5-flash-lite"   # Can upgrade to 2.5-flash if needed
    
    # Thresholds
    relevance_threshold: float = 0.7  # Minimum confidence to extract
    qualification_threshold: float = 50  # Minimum score (0-100) to keep lead
    
    # Cost tracking
    track_costs: bool = True
    
    # Learning
    learning_enabled: bool = True
    learning_file: str = "data/learnings/pipeline_learnings.json"


# =============================================================================
# STAGE 0: QUICK REJECT (FREE - No AI)
# =============================================================================

class QuickRejectFilter:
    """
    Stage 0: Instantly reject URLs that are NEVER useful.
    
    This is NOT about content (airlines, cruises, etc.)
    This is about STRUCTURE (login pages, archives, etc.)
    
    Cost: FREE
    Speed: <1ms per URL
    """
    
    # Structural patterns that NEVER contain hotel opening news
    STRUCTURAL_JUNK = [
        # Authentication & User pages
        r'/login', r'/signin', r'/signup', r'/register',
        r'/logout', r'/signout', r'/auth', r'/oauth',
        r'/password', r'/forgot', r'/reset',
        r'/account', r'/profile', r'/settings',
        r'/my-', r'/user/',
        
        # Site infrastructure
        r'/contact', r'/about-us', r'/about$',
        r'/privacy', r'/terms', r'/legal', r'/disclaimer',
        r'/cookie', r'/gdpr', r'/accessibility',
        r'/sitemap', r'/robots', r'/feed', r'/rss',
        r'/cdn-cgi/', r'/wp-admin', r'/wp-login',
        r'/admin', r'/_next/', r'/_nuxt/',
        
        # Media & files
        r'/video-gallery', r'/photo-gallery', r'/gallery',
        r'/podcast', r'/webinar', r'/download',
        r'\.pdf$', r'\.jpg$', r'\.png$', r'\.mp4$',
        
        # Social & external
        r'facebook\.com', r'twitter\.com', r'instagram\.com',
        r'linkedin\.com', r'youtube\.com', r'tiktok\.com',
        r'mailto:', r'tel:', r'javascript:',
        
        # E-commerce
        r'/cart', r'/checkout', r'/shop', r'/store',
        r'/subscribe', r'/membership', r'/pricing',
        r'/donate', r'/sponsor',
        
        # Search & archives (index pages, not content)
        r'/search\?', r'/search$', r'\?s=',
        r'/archives$', r'/archive$',
        r'/page/\d+$',  # Pagination without content
        
        # Misc junk
        r'/tag/', r'/tags/', r'/author/',
        r'/category$', r'/categories$',
        r'#', r'\?replyto', r'\?share',
    ]
    
    def __init__(self):
        # Compile patterns for speed
        self._patterns = [re.compile(p, re.IGNORECASE) for p in self.STRUCTURAL_JUNK]
        self._stats = {'checked': 0, 'rejected': 0, 'passed': 0}
    
    def should_reject(self, url: str) -> Tuple[bool, str]:
        """
        Check if URL should be instantly rejected.
        
        Returns:
            (should_reject, reason)
        """
        self._stats['checked'] += 1
        
        for pattern in self._patterns:
            if pattern.search(url):
                self._stats['rejected'] += 1
                return True, f"Structural junk: {pattern.pattern}"
        
        self._stats['passed'] += 1
        return False, ""
    
    def filter_urls(self, urls: List[str]) -> List[str]:
        """Filter a list of URLs, returning only non-junk ones"""
        return [url for url in urls if not self.should_reject(url)[0]]
    
    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()


# =============================================================================
# STAGE 1 & 2: CONTENT CLASSIFIER (Cheap AI)
# =============================================================================

@dataclass
class ClassificationResult:
    """Result from content classification"""
    url: str
    summary: str                    # What is this page about?
    is_hotel_opening: bool          # Is this about a NEW hotel opening?
    confidence: float               # 0.0 - 1.0
    reasoning: str                  # Why this decision?
    processing_time_ms: int
    tokens_used: int = 0
    
    @property
    def should_extract(self) -> bool:
        return self.is_hotel_opening and self.confidence >= 0.6


class ContentClassifier:
    """
    Stage 1 & 2: Understand content and decide relevance.
    
    Instead of filtering keywords, we ASK the AI:
    1. "What is this page about?" (understand)
    2. "Is this about a NEW hotel opening?" (decide)
    
    Cost: ~$0.0001 per page (very cheap!)
    Speed: ~500ms per page
    """
    
    CLASSIFIER_PROMPT = """You are a hotel industry analyst. Your job is to quickly classify web pages.

TASK: Analyze this content and determine if it contains information about NEW HOTEL OPENINGS in 2026 OR LATER.

TARGET LOCATIONS (we ONLY care about these):
- Florida (Miami, Orlando, Tampa, Naples, Fort Lauderdale, etc.)
- Caribbean (Bahamas, Jamaica, Turks & Caicos, Aruba, Puerto Rico, etc.)
- Other USA states

WHAT COUNTS AS RELEVANT:
✅ A NAMED hotel/resort opening in 2026 or later in USA or Caribbean
✅ A hotel under construction or announced for USA or Caribbean
✅ A LIST or ROUNDUP of new hotel openings for 2026+ in USA/Caribbean
✅ Press releases about new hotel developments in USA/Caribbean

WHAT DOES NOT COUNT:
❌ Hotels that ALREADY OPENED (2025 or earlier) - NOT RELEVANT
❌ International hotels (Europe, Asia, Middle East, Africa) - NOT RELEVANT
❌ Executive appointments or personnel changes
❌ Corporate earnings reports
❌ Hotel reviews of existing properties
❌ Restaurant/bar openings at existing hotels
❌ Airline/cruise news
❌ Category/navigation pages with no content

KEY TESTS:
1. Is this about a NEW hotel (opening 2026 or later)?
2. Is it in USA or Caribbean?

If BOTH are YES, mark as TRUE. Otherwise FALSE.

CONTENT TO ANALYZE:
---
{content}
---

Respond in this EXACT JSON format (no other text):
{{
    "summary": "One sentence describing what this page is about",
    "is_new_hotel_opening": true/false,
    "confidence": 0.0-1.0,
    "reasoning": "Brief explanation - if true, name the hotel and location"
}}"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.api_key = config.gemini_api_key or os.getenv("GEMINI_API_KEY", "")
        self._stats = {
            'classified': 0,
            'relevant': 0,
            'not_relevant': 0,
            'errors': 0,
            'total_tokens': 0
        }
    
    async def classify(self, url: str, content: str) -> ClassificationResult:
        """
        Classify a page's content.
        
        Args:
            url: The page URL (for reference)
            content: The page text content (first ~2000 chars is enough)
        
        Returns:
            ClassificationResult with decision
        """
        start_time = time.time()
        
        # Truncate content to save tokens (classification doesn't need full text)
        truncated_content = content[:3000] if len(content) > 3000 else content
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self.config.classifier_model}:generateContent?key={self.api_key}",
                    json={
                        "contents": [{
                            "parts": [{
                                "text": self.CLASSIFIER_PROMPT.format(content=truncated_content)
                            }]
                        }],
                        "generationConfig": {
                            "temperature": 0.1,  # Low temperature for consistent classification
                            "maxOutputTokens": 200,  # Short response needed
                        }
                    }
                )
                
                processing_time = int((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    result = response.json()
                    text = result['candidates'][0]['content']['parts'][0]['text']
                    
                    # Parse JSON response
                    json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
                    if json_match:
                        data = json.loads(json_match.group())
                        
                        is_relevant = data.get('is_new_hotel_opening', False)
                        self._stats['classified'] += 1
                        if is_relevant:
                            self._stats['relevant'] += 1
                        else:
                            self._stats['not_relevant'] += 1
                        
                        return ClassificationResult(
                            url=url,
                            summary=data.get('summary', 'Unknown'),
                            is_hotel_opening=is_relevant,
                            confidence=float(data.get('confidence', 0.5)),
                            reasoning=data.get('reasoning', ''),
                            processing_time_ms=processing_time
                        )
                
                # Failed to parse
                self._stats['errors'] += 1
                return ClassificationResult(
                    url=url,
                    summary="Classification failed",
                    is_hotel_opening=False,
                    confidence=0.0,
                    reasoning=f"API error: {response.status_code}",
                    processing_time_ms=processing_time
                )
                
        except Exception as e:
            self._stats['errors'] += 1
            processing_time = int((time.time() - start_time) * 1000)
            return ClassificationResult(
                url=url,
                summary="Classification error",
                is_hotel_opening=False,
                confidence=0.0,
                reasoning=str(e),
                processing_time_ms=processing_time
            )
    
    async def classify_batch(self, pages: List[Tuple[str, str]]) -> List[ClassificationResult]:
        """Classify multiple pages in parallel"""
        tasks = [self.classify(url, content) for url, content in pages]
        return await asyncio.gather(*tasks)
    
    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()


# =============================================================================
# STAGE 3: LEAD EXTRACTION (Full AI)
# =============================================================================

@dataclass
class ExtractedLead:
    """A lead extracted from a classified page"""
    # Hotel info
    hotel_name: str
    brand: str = ""
    property_type: str = ""  # resort, hotel, boutique, etc.
    
    # Location
    city: str = ""
    state: str = ""
    country: str = ""
    
    # Timeline
    opening_date: str = ""
    opening_status: str = ""  # announced, under construction, opening soon, just opened
    
    # Size & details
    room_count: int = 0
    
    # Management
    management_company: str = ""
    developer: str = ""
    
    # Contacts
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    
    # Source
    source_url: str = ""
    source_name: str = ""
    extracted_at: str = ""
    
    # Quality
    confidence_score: float = 0.0
    qualification_score: int = 0  # 0-100
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'hotel_name': self.hotel_name,
            'brand': self.brand,
            'property_type': self.property_type,
            'city': self.city,
            'state': self.state,
            'country': self.country,
            'opening_date': self.opening_date,
            'opening_status': self.opening_status,
            'room_count': self.room_count,
            'management_company': self.management_company,
            'developer': self.developer,
            'contact_name': self.contact_name,
            'contact_title': self.contact_title,
            'contact_email': self.contact_email,
            'contact_phone': self.contact_phone,
            'source_url': self.source_url,
            'source_name': self.source_name,
            'extracted_at': self.extracted_at,
            'confidence_score': self.confidence_score,
            'qualification_score': self.qualification_score,
        }


class LeadExtractor:
    """
    Stage 3: Full extraction from relevant pages.
    
    This ONLY runs on pages that passed classification.
    Extracts all useful details about the hotel opening.
    
    Cost: ~$0.001 per page
    Speed: 2-3 seconds per page
    """
    
    EXTRACTION_PROMPT = """You are a hotel industry data extraction specialist.

Extract information about the NEW HOTEL OPENING from this article.

IMPORTANT RULES:
1. Only extract info about the NEW hotel being announced/opened
2. Do NOT extract info about existing hotels mentioned as references
3. If multiple new hotels are mentioned, extract each separately
4. Leave fields empty if information is not clearly stated
5. For opening_date, use format "Month YYYY" or "Q1 2026" or "2026"

CONTENT:
---
{content}
---

Respond with a JSON array of hotels (even if just one):
[
    {{
        "hotel_name": "Full official name of the new hotel",
        "brand": "Hotel brand (Marriott, Hilton, Hyatt, etc.)",
        "property_type": "resort/hotel/boutique/all-inclusive/etc",
        "city": "City name",
        "state": "State/Province (if applicable)",
        "country": "Country name",
        "opening_date": "When it opens (Month YYYY, Q1 2026, 2026, etc.)",
        "opening_status": "announced/under construction/opening soon/just opened",
        "room_count": number or 0 if unknown,
        "management_company": "Company managing the hotel",
        "developer": "Company developing/building the hotel",
        "contact_name": "Any contact person mentioned",
        "contact_title": "Their job title",
        "contact_email": "Email if found",
        "contact_phone": "Phone if found"
    }}
]

Return ONLY the JSON array, no other text."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.api_key = config.gemini_api_key or os.getenv("GEMINI_API_KEY", "")
        self._stats = {
            'pages_processed': 0,
            'leads_extracted': 0,
            'errors': 0
        }
    
    async def extract(self, url: str, content: str, source_name: str = "") -> List[ExtractedLead]:
        """Extract leads from a page that passed classification"""
        self._stats['pages_processed'] += 1
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self.config.extractor_model}:generateContent?key={self.api_key}",
                    json={
                        "contents": [{
                            "parts": [{
                                "text": self.EXTRACTION_PROMPT.format(content=content[:8000])
                            }]
                        }],
                        "generationConfig": {
                            "temperature": 0.2,
                            "maxOutputTokens": 2000,
                        }
                    }
                )
                
                if response.status_code == 200:
                    result = response.json()
                    text = result['candidates'][0]['content']['parts'][0]['text']
                    
                    # Parse JSON response
                    json_match = re.search(r'\[.*\]', text, re.DOTALL)
                    if json_match:
                        hotels = json.loads(json_match.group())
                        
                        leads = []
                        for hotel in hotels:
                            if hotel.get('hotel_name'):
                                lead = ExtractedLead(
                                    hotel_name=hotel.get('hotel_name', ''),
                                    brand=hotel.get('brand', ''),
                                    property_type=hotel.get('property_type', ''),
                                    city=hotel.get('city', ''),
                                    state=hotel.get('state', ''),
                                    country=hotel.get('country', ''),
                                    opening_date=hotel.get('opening_date', ''),
                                    opening_status=hotel.get('opening_status', ''),
                                    room_count=hotel.get('room_count', 0) or 0,
                                    management_company=hotel.get('management_company', ''),
                                    developer=hotel.get('developer', ''),
                                    contact_name=hotel.get('contact_name', ''),
                                    contact_title=hotel.get('contact_title', ''),
                                    contact_email=hotel.get('contact_email', ''),
                                    contact_phone=hotel.get('contact_phone', ''),
                                    source_url=url,
                                    source_name=source_name,
                                    extracted_at=datetime.now().isoformat()
                                )
                                leads.append(lead)
                        
                        self._stats['leads_extracted'] += len(leads)
                        return leads
                
                self._stats['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            self._stats['errors'] += 1
            return []
    
    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()


# =============================================================================
# STAGE 4: LEAD QUALIFICATION (Scoring)
# =============================================================================

class LeadQualifier:
    """
    Stage 4: Score leads based on uniform sales potential.
    
    Scoring criteria (0-100):
    - Location (Florida/Caribbean = high)
    - Brand tier (luxury/upscale = high)
    - Size (more rooms = higher potential)
    - Timeline (opening soon = more urgent)
    - Contact availability
    - Revenue potential estimation
    
    FILTERS OUT:
    - International locations (not USA/Caribbean)
    - Old openings (2025 or earlier)
    - Budget/midscale hotels (below 4-star)
    """
    
    # ==========================================================================
    # 5-TIER BRAND SYSTEM - Only Tier 1-4 qualify, Tier 5 = SKIP
    # ==========================================================================
    
    # TIER 1: ULTRA LUXURY (+30 points) - Highest uniform spend
    TIER1_ULTRA_LUXURY = [
        'aman', 'amangiri', 'amanera', 'amanyara',
        'bulgari',
        'one&only', 'one & only',
        'peninsula',
        'raffles',
        'six senses',
        'mandarin oriental',
        'rosewood',
        'faena',
        'capella',
        'oetker collection',
        'belmond',
        'dorchester collection',
        'rocco forte',
    ]
    
    # TIER 2: LUXURY (+25 points) - Top brands of major chains
    TIER2_LUXURY = [
        'four seasons',
        'ritz-carlton', 'ritz carlton',
        'st. regis', 'st regis',
        'luxury collection',
        'waldorf astoria', 'waldorf-astoria',
        'conrad',
        'lxr',
        'park hyatt',
        'montage',
        'auberge',
        'nobu hotel',
        'edition',
    ]
    
    # TIER 3: UPPER UPSCALE (+20 points) - Premium lifestyle
    TIER3_UPPER_UPSCALE = [
        'jw marriott',
        'w hotel', 'w hotels',
        'andaz',
        'grand hyatt',
        'fairmont',
        'sofitel',
        'intercontinental',
        'regent',
        'signia',
        'thompson',
        'kimpton',
        '1 hotel', '1hotel',
        'delano',
        'sls',
        'dream hotel',
        'virgin hotels',
    ]
    
    # TIER 4: UPSCALE (+10 points) - Full-service 4-star
    TIER4_UPSCALE = [
        'marriott',  # Generic Marriott (not JW)
        'hilton',    # Generic Hilton (not Waldorf/Conrad)
        'hyatt regency',
        'hyatt',     # Generic Hyatt
        'westin',
        'sheraton',
        'omni',
        'loews',
        'renaissance',
        'le meridien',
        'autograph collection', 'autograph',
        'curio collection', 'curio',
        'tribute portfolio', 'tribute',
        'tapestry collection', 'tapestry',
        'canopy',
        'doubletree',
        'hard rock',
        'embassy suites',
    ]
    
    # TIER 5: SKIP - Budget/Midscale (below 4-star) - FILTER OUT!
    TIER5_SKIP = [
        # Hilton Budget
        'hampton inn', 'hampton by hilton', 'hampton',
        'hilton garden inn', 'hilton garden',
        'home2 suites', 'home2',
        'tru by hilton', 'tru',
        'spark by hilton', 'spark',
        'homewood suites',
        # Marriott Budget
        'courtyard by marriott', 'courtyard',
        'fairfield inn', 'fairfield by marriott', 'fairfield',
        'springhill suites', 'springhill',
        'townplace suites', 'townplace',
        'residence inn',
        'ac hotel', 'ac hotels',  # Midscale
        'aloft',
        'element',
        'moxy',
        'four points', 'four points by sheraton',
        'protea',
        # Hyatt Budget
        'hyatt place',
        'hyatt house',
        'caption by hyatt',
        # IHG Budget
        'holiday inn express', 'holiday inn',
        'crowne plaza',
        'even hotels',
        'avid hotels',
        'staybridge suites',
        'candlewood suites',
        # Wyndham
        'days inn',
        'super 8',
        'la quinta',
        'microtel',
        'ramada',
        'wingate',
        'baymont',
        'hawthorn suites',
        'travelodge',
        'howard johnson',
        # Choice
        'comfort inn', 'comfort suites',
        'quality inn',
        'sleep inn',
        'clarion',
        'econo lodge',
        'rodeway inn',
        'suburban',
        'mainstay suites',
        'woodspring suites',
        # Other Budget
        'best western',
        'motel 6',
        'red roof',
        'extended stay',
        'studio 6',
        'intown suites',
        'livaway suites',
    ]
    
    # Target locations
    FLORIDA_CITIES = [
        'miami', 'fort lauderdale', 'orlando', 'tampa', 'naples', 'west palm beach',
        'boca raton', 'key west', 'jacksonville', 'sarasota', 'clearwater',
        'palm beach', 'delray beach', 'fort myers', 'st petersburg', 'destin',
        'pensacola', 'daytona', 'gainesville', 'tallahassee', 'florida keys',
        'hollywood', 'coral gables', 'aventura', 'doral', 'sunny isles',
        'south beach', 'coconut grove', 'brickell', 'fisher island',
    ]
    
    CARIBBEAN_COUNTRIES = [
        'bahamas', 'jamaica', 'dominican republic', 'puerto rico', 'aruba',
        'cayman islands', 'turks and caicos', 'barbados', 'st lucia', 'antigua',
        'usvi', 'us virgin islands', 'bvi', 'british virgin islands', 'curacao',
        'st kitts', 'nevis', 'grenada', 'trinidad', 'tobago', 'martinique',
        'guadeloupe', 'st martin', 'st maarten', 'anguilla', 'bermuda',
        'bonaire', 'st vincent', 'st barts', 'montserrat', 'cancun',
        'riviera maya', 'playa del carmen', 'tulum', 'cozumel', 'los cabos',
        'punta cana', 'nassau', 'paradise island', 'turks', 'caicos',
    ]
    
    # Revenue estimation per uniform (based on brand tier and room count)
    # Based on World Tourism Organization staffing ratios + industry uniform costs
    STAFFING_RATIOS = {
        'tier1': 2.5,   # Ultra luxury: 2.5 staff per room
        'tier2': 1.8,   # Luxury: 1.8 staff per room
        'tier3': 1.3,   # Upper upscale: 1.3 staff per room
        'tier4': 1.0,   # Upscale: 1.0 staff per room
        'unknown': 1.2, # Unknown: assume mid-range
    }
    
    UNIFORM_COST_PER_EMPLOYEE = {
        'tier1': 500,   # Ultra luxury: $500/employee (custom/premium)
        'tier2': 400,   # Luxury: $400/employee
        'tier3': 275,   # Upper upscale: $275/employee
        'tier4': 200,   # Upscale: $200/employee
        'unknown': 250, # Unknown: assume mid-range
    }
    
    # Resort bonus (resorts have more F&B, spa, pool staff)
    RESORT_KEYWORDS = ['resort', 'beach', 'spa', 'island', 'golf']
    
    def _get_brand_tier(self, hotel_name: str, brand: str) -> tuple:
        """
        Determine brand tier. Returns (tier_name, points, skip).
        Check most specific brands first to avoid misclassification.
        """
        combined = f"{brand} {hotel_name}".lower()
        
        # CHECK SKIP TIER FIRST - Budget/midscale hotels we don't want
        if any(b in combined for b in self.TIER5_SKIP):
            return ('skip', 0, True)  # FILTER OUT
        
        # Check tiers in order of specificity
        if any(b in combined for b in self.TIER1_ULTRA_LUXURY):
            return ('tier1', 30, False)
        if any(b in combined for b in self.TIER2_LUXURY):
            return ('tier2', 25, False)
        if any(b in combined for b in self.TIER3_UPPER_UPSCALE):
            return ('tier3', 20, False)
        if any(b in combined for b in self.TIER4_UPSCALE):
            return ('tier4', 10, False)
        
        # Unknown brand - assume 4-star independent, keep it
        return ('unknown', 15, False)
    
    def _is_target_location(self, lead: ExtractedLead) -> tuple:
        """
        Check if location is in our target market.
        Returns (is_target, location_type, points)
        """
        location_lower = f"{lead.city} {lead.state} {lead.country}".lower()
        state_lower = (lead.state or '').lower()
        country_lower = (lead.country or '').lower()
        
        # Florida - TOP PRIORITY
        if state_lower in ['fl', 'florida'] or 'florida' in location_lower:
            return (True, 'florida', 30)
        if any(city in location_lower for city in self.FLORIDA_CITIES):
            return (True, 'florida', 30)
        
        # Caribbean - HIGH PRIORITY
        if any(country in location_lower for country in self.CARIBBEAN_COUNTRIES):
            return (True, 'caribbean', 25)
        
        # Other USA - GOOD
        if country_lower in ['usa', 'united states', 'us', 'u.s.', 'america']:
            return (True, 'usa', 15)
        
        # USA states check
        usa_states = ['california', 'new york', 'texas', 'georgia', 'tennessee', 
                      'south carolina', 'north carolina', 'virginia', 'maryland',
                      'colorado', 'arizona', 'nevada', 'hawaii', 'louisiana']
        if any(state in location_lower for state in usa_states):
            return (True, 'usa', 15)
        
        # INTERNATIONAL - FILTER OUT
        return (False, 'international', 0)
    
    def _is_valid_opening_year(self, opening_date: str) -> tuple:
        """
        Check if opening date is 2026 or later.
        Returns (is_valid, year, points)
        """
        opening_lower = (opening_date or '').lower()
        
        # OLD OPENINGS - FILTER OUT
        old_years = ['2020', '2021', '2022', '2023', '2024', '2025']
        if any(year in opening_lower for year in old_years):
            return (False, 'old', 0)
        
        # TARGET YEARS
        if '2026' in opening_lower:
            return (True, '2026', 20)  # PRIME TARGET
        if '2027' in opening_lower:
            return (True, '2027', 15)
        if '2028' in opening_lower:
            return (True, '2028', 10)
        if '2029' in opening_lower or '2030' in opening_lower:
            return (True, '2029+', 8)
        
        # Unknown date - might be new, keep with lower score
        return (True, 'unknown', 5)
    
    def _estimate_revenue(self, lead: ExtractedLead, tier: str) -> dict:
        """
        Estimate potential uniform revenue based on:
        - Room count (or estimate if unknown)
        - Brand tier (staffing ratio)
        - Property type (resort bonus)
        
        Formula: Revenue = Rooms × Staff_Ratio × Uniform_Cost_Per_Employee
        
        Returns dict with breakdown for transparency.
        """
        # Get room count (estimate if unknown)
        room_count = lead.room_count if lead.room_count and lead.room_count > 0 else 150
        
        # Get staffing ratio based on tier
        staff_ratio = self.STAFFING_RATIOS.get(tier, 1.2)
        
        # Check if resort (resorts have 0.5 higher staff ratio)
        hotel_lower = (lead.hotel_name or '').lower()
        property_type_lower = (lead.property_type or '').lower()
        is_resort = any(kw in hotel_lower or kw in property_type_lower for kw in self.RESORT_KEYWORDS)
        
        if is_resort:
            staff_ratio += 0.5
        
        # Calculate estimated staff count
        estimated_staff = int(room_count * staff_ratio)
        
        # Get uniform cost per employee
        cost_per_employee = self.UNIFORM_COST_PER_EMPLOYEE.get(tier, 250)
        
        # Calculate total revenue estimate
        estimated_revenue = estimated_staff * cost_per_employee
        
        # Build breakdown for transparency
        revenue_breakdown = {
            'estimated_revenue': estimated_revenue,
            'room_count': room_count,
            'room_count_estimated': lead.room_count is None or lead.room_count == 0,
            'staff_ratio': staff_ratio,
            'estimated_staff': estimated_staff,
            'cost_per_employee': cost_per_employee,
            'is_resort': is_resort,
            'brand_tier': tier,
        }
        
        return revenue_breakdown
    
    def qualify(self, lead: ExtractedLead) -> ExtractedLead:
        """
        Score and filter a lead for uniform sales potential.
        
        HARD FILTERS (instant disqualification):
        - Budget/midscale brands (Tier 5)
        - International locations (not USA/Caribbean)
        - Old openings (2025 or earlier)
        
        SCORING (0-100):
        - Location: max 30 points
        - Brand tier: max 30 points
        - Room count: max 20 points
        - Timeline: max 20 points
        """
        score = 0
        lead.skip_reason = ""  # Track why leads are skipped
        
        # === HARD FILTER 1: BRAND TIER ===
        tier_name, tier_points, should_skip = self._get_brand_tier(
            lead.hotel_name or '', lead.brand or ''
        )
        if should_skip:
            lead.qualification_score = 0
            lead.skip_reason = f"Budget/midscale brand (Tier 5)"
            lead.estimated_revenue = 0
            return lead
        
        score += tier_points
        
        # === HARD FILTER 2: LOCATION ===
        is_target, location_type, location_points = self._is_target_location(lead)
        if not is_target:
            lead.qualification_score = 0
            lead.skip_reason = f"International location (not USA/Caribbean)"
            lead.estimated_revenue = 0
            return lead
        
        score += location_points
        
        # === HARD FILTER 3: OPENING YEAR ===
        is_valid_year, year, year_points = self._is_valid_opening_year(lead.opening_date or '')
        if not is_valid_year:
            lead.qualification_score = 0
            lead.skip_reason = f"Old opening ({year})"
            lead.estimated_revenue = 0
            return lead
        
        score += year_points
        
        # === ROOM COUNT (max 20 points) ===
        if lead.room_count and lead.room_count >= 300:
            score += 20  # Large property = big order
        elif lead.room_count and lead.room_count >= 200:
            score += 15
        elif lead.room_count and lead.room_count >= 100:
            score += 10
        elif lead.room_count and lead.room_count >= 50:
            score += 5
        else:
            score += 8  # Unknown size - assume medium
        
        # === ESTIMATE REVENUE (using proper model) ===
        revenue_data = self._estimate_revenue(lead, tier_name)
        lead.estimated_revenue = revenue_data['estimated_revenue']
        lead.estimated_staff = revenue_data['estimated_staff']
        lead.revenue_breakdown = revenue_data  # Full breakdown for transparency
        
        # Store metadata
        lead.brand_tier = tier_name
        lead.location_type = location_type
        lead.qualification_score = min(score, 100)
        
        return lead
    
    def qualify_batch(self, leads: List[ExtractedLead]) -> List[ExtractedLead]:
        """Qualify multiple leads, filtering out disqualified ones"""
        qualified = []
        skipped = 0
        
        for lead in leads:
            qualified_lead = self.qualify(lead)
            if qualified_lead.qualification_score > 0:
                qualified.append(qualified_lead)
            else:
                skipped += 1
                logger.debug(f"⏭️ Skipped: {lead.hotel_name} - {qualified_lead.skip_reason}")
        
        if skipped > 0:
            logger.info(f"   🚫 Filtered out {skipped} leads (budget brands, international, or old openings)")
        
        return qualified


# =============================================================================
# STAGE 5: LEARNING SYSTEM
# =============================================================================

@dataclass
class PageLearning:
    """What we learned from processing a page"""
    url: str
    domain: str
    url_pattern: str  # Generalized pattern
    was_relevant: bool
    lead_count: int
    lead_quality_avg: float
    timestamp: str


class PipelineLearner:
    """
    Stage 5: Learn from every page processed.
    
    Tracks:
    - Which URL patterns produce leads
    - Which sources are most valuable
    - What content types are relevant
    
    Over time, this data can be used to:
    - Prioritize high-value URLs
    - Skip patterns that never produce leads
    - Focus scraping on best sources
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.learnings: Dict[str, List[PageLearning]] = {}
        self._load_learnings()
    
    def _load_learnings(self):
        """Load existing learnings from file"""
        try:
            path = Path(self.config.learning_file)
            if path.exists():
                with open(path) as f:
                    data = json.load(f)
                    # Convert back to PageLearning objects
                    for domain, pages in data.items():
                        self.learnings[domain] = [
                            PageLearning(**p) for p in pages
                        ]
                logger.info(f"✅ Loaded learnings for {len(self.learnings)} domains")
        except Exception as e:
            logger.warning(f"Could not load learnings: {e}")
    
    def _save_learnings(self):
        """Save learnings to file"""
        try:
            path = Path(self.config.learning_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert to dicts for JSON
            data = {}
            for domain, pages in self.learnings.items():
                data[domain] = [asdict(p) for p in pages[-100:]]  # Keep last 100 per domain
            
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save learnings: {e}")
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.replace('www.', '')
        except:
            return 'unknown'
    
    def _extract_pattern(self, url: str) -> str:
        """Convert URL to a pattern (replace IDs with placeholders)"""
        pattern = url
        # Replace date patterns: /2026/01/30/ → /YYYY/MM/DD/
        pattern = re.sub(r'/\d{4}/\d{2}/\d{2}/', '/YYYY/MM/DD/', pattern)
        # Replace numeric IDs: /12345/ → /ID/
        pattern = re.sub(r'/\d+/', '/ID/', pattern)
        # Replace article slugs with placeholder
        pattern = re.sub(r'/[a-z0-9-]{20,}/', '/SLUG/', pattern)
        return pattern
    
    def record(self, url: str, was_relevant: bool, leads: List[ExtractedLead] = None):
        """Record what we learned from processing a page"""
        domain = self._extract_domain(url)
        pattern = self._extract_pattern(url)
        
        lead_count = len(leads) if leads else 0
        lead_quality = sum(l.qualification_score for l in leads) / len(leads) if leads else 0
        
        learning = PageLearning(
            url=url,
            domain=domain,
            url_pattern=pattern,
            was_relevant=was_relevant,
            lead_count=lead_count,
            lead_quality_avg=lead_quality,
            timestamp=datetime.now().isoformat()
        )
        
        if domain not in self.learnings:
            self.learnings[domain] = []
        self.learnings[domain].append(learning)
        
        self._save_learnings()
    
    def get_domain_stats(self, domain: str) -> Dict:
        """Get statistics for a domain"""
        if domain not in self.learnings:
            return {'pages': 0, 'relevant': 0, 'leads': 0}
        
        pages = self.learnings[domain]
        relevant = [p for p in pages if p.was_relevant]
        
        return {
            'pages': len(pages),
            'relevant': len(relevant),
            'relevance_rate': len(relevant) / len(pages) if pages else 0,
            'leads': sum(p.lead_count for p in pages),
            'avg_quality': sum(p.lead_quality_avg for p in pages) / len(pages) if pages else 0
        }
    
    def get_pattern_stats(self, domain: str) -> Dict[str, Dict]:
        """Get statistics per URL pattern for a domain"""
        if domain not in self.learnings:
            return {}
        
        patterns = {}
        for page in self.learnings[domain]:
            if page.url_pattern not in patterns:
                patterns[page.url_pattern] = {
                    'count': 0, 'relevant': 0, 'leads': 0
                }
            patterns[page.url_pattern]['count'] += 1
            if page.was_relevant:
                patterns[page.url_pattern]['relevant'] += 1
            patterns[page.url_pattern]['leads'] += page.lead_count
        
        # Calculate rates
        for pattern, stats in patterns.items():
            stats['relevance_rate'] = stats['relevant'] / stats['count'] if stats['count'] else 0
        
        return patterns


# =============================================================================
# MAIN PIPELINE ORCHESTRATOR
# =============================================================================

@dataclass
class PipelineResult:
    """Result from running the full pipeline"""
    # Input
    source_name: str
    pages_scraped: int
    
    # Stage 0
    urls_rejected_quick: int
    
    # Stage 1+2
    pages_classified: int
    pages_relevant: int
    pages_not_relevant: int
    
    # Stage 3
    leads_extracted: int
    
    # Stage 4
    leads_qualified: int
    leads_high_quality: int  # Score >= 70
    leads_medium_quality: int  # Score 40-69
    leads_low_quality: int  # Score < 40
    
    # Output
    final_leads: List[ExtractedLead]
    
    # Meta
    total_time_seconds: float
    classification_time_ms: int
    extraction_time_ms: int


class IntelligentPipeline:
    """
    The main intelligent pipeline that orchestrates all stages.
    
    Usage:
        pipeline = IntelligentPipeline()
        results = await pipeline.process_pages(scraped_pages)
    """
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        
        # Initialize all stages
        self.quick_reject = QuickRejectFilter()
        self.classifier = ContentClassifier(self.config)
        self.extractor = LeadExtractor(self.config)
        self.qualifier = LeadQualifier()
        self.learner = PipelineLearner(self.config)
        
        self._stats = {
            'total_runs': 0,
            'total_pages': 0,
            'total_leads': 0
        }
    
    async def process_pages(
        self, 
        pages: List[Dict[str, str]],  # [{'url': ..., 'content': ..., 'source': ...}, ...]
        source_name: str = ""
    ) -> PipelineResult:
        """
        Process scraped pages through the intelligent pipeline.
        
        Args:
            pages: List of dicts with 'url', 'content', and optionally 'source'
            source_name: Name of the source being processed
        
        Returns:
            PipelineResult with all leads and statistics
        """
        start_time = time.time()
        self._stats['total_runs'] += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🧠 INTELLIGENT PIPELINE - Processing {len(pages)} pages")
        logger.info(f"{'='*60}")
        
        # === STAGE 0: Quick Reject ===
        # (Already done during scraping, but can double-check)
        urls_rejected = 0
        
        # === STAGE 1+2: Classification ===
        logger.info(f"\n📊 STAGE 1+2: Classifying {len(pages)} pages...")
        classification_start = time.time()
        
        relevant_pages = []
        not_relevant_pages = []
        
        for page in pages:
            url = page.get('url', '')
            content = page.get('content', '') or page.get('text', '')
            
            if not content or len(content) < 100:
                logger.debug(f"Skipping {url[:50]}... (no content)")
                continue
            
            result = await self.classifier.classify(url, content)
            
            if result.should_extract:
                relevant_pages.append({
                    'url': url,
                    'content': content,
                    'source': page.get('source', source_name),
                    'classification': result
                })
                logger.info(f"✅ RELEVANT: {result.summary[:60]}...")
            else:
                not_relevant_pages.append({
                    'url': url,
                    'classification': result
                })
                logger.debug(f"❌ Skip: {result.summary[:60]}...")
            
            # Record learning
            self.learner.record(url, result.should_extract, [])
        
        classification_time = int((time.time() - classification_start) * 1000)
        logger.info(f"✅ Classification complete: {len(relevant_pages)} relevant, {len(not_relevant_pages)} skipped")
        
        # === STAGE 3: Extraction ===
        logger.info(f"\n🔍 STAGE 3: Extracting from {len(relevant_pages)} relevant pages...")
        extraction_start = time.time()
        
        all_leads = []
        
        # Check for custom handlers first (site-specific extraction)
        custom_handler_urls = set()
        if CUSTOM_HANDLERS_AVAILABLE:
            for page in pages:
                url = page.get('url', '')
                content = page.get('content', '') or page.get('text', '')
                
                if has_custom_handler(url):
                    custom_handler_urls.add(url)
                    handler = get_handler(url)
                    logger.info(f"🔧 Using custom handler: {handler.name} for {url[:50]}...")
                    
                    try:
                        handler_leads = await handler.extract(content, url)
                        
                        # Convert handler leads to pipeline ExtractedLead format
                        for hl in handler_leads:
                            lead = ExtractedLead(
                                hotel_name=hl.hotel_name or '',
                                brand=hl.brand or '',
                                property_type=hl.property_type or '',
                                city=hl.city or '',
                                state=hl.state or '',
                                country=hl.country or '',
                                opening_date=hl.opening_date or str(hl.opening_year) if hl.opening_year else '',
                                opening_status='announced',
                                room_count=hl.room_count or 0,
                                management_company='',
                                developer='',
                                contact_name=hl.contact_name or '',
                                contact_title='',
                                contact_email=hl.contact_email or '',
                                contact_phone=hl.contact_phone or '',
                                source_url=url,
                                source_name=page.get('source', source_name),
                                extracted_at=datetime.now().isoformat()
                            )
                            all_leads.append(lead)
                        
                        logger.info(f"   ✅ Custom handler extracted {len(handler_leads)} leads")
                        
                        # Record learning (pass empty list - learning happens after qualification)
                        self.learner.record(url, True, [])
                        
                    except Exception as e:
                        logger.error(f"   ❌ Custom handler failed: {e}")
        
        # Process remaining relevant pages with AI extraction
        for page in relevant_pages:
            url = page['url']
            
            # Skip if already processed by custom handler
            if url in custom_handler_urls:
                continue
            
            leads = await self.extractor.extract(
                page['url'], 
                page['content'],
                page['source']
            )
            all_leads.extend(leads)
            
            # Update learning with actual leads
            self.learner.record(page['url'], True, leads)
        
        extraction_time = int((time.time() - extraction_start) * 1000)
        logger.info(f"✅ Extracted {len(all_leads)} leads")
        
        # === STAGE 4: Qualification ===
        logger.info(f"\n⭐ STAGE 4: Qualifying {len(all_leads)} leads...")
        
        qualified_leads = self.qualifier.qualify_batch(all_leads)
        
        # Filter by minimum score
        final_leads = [l for l in qualified_leads if l.qualification_score >= self.config.qualification_threshold]
        
        # Categorize by quality
        high_quality = [l for l in final_leads if l.qualification_score >= 70]
        medium_quality = [l for l in final_leads if 40 <= l.qualification_score < 70]
        low_quality = [l for l in final_leads if l.qualification_score < 40]
        
        total_time = time.time() - start_time
        
        # === Summary ===
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 PIPELINE COMPLETE")
        logger.info(f"{'='*60}")
        logger.info(f"📊 Input: {len(pages)} pages")
        logger.info(f"🧠 Classified: {len(relevant_pages)} relevant ({len(relevant_pages)/len(pages)*100:.0f}%)")
        logger.info(f"📝 Extracted: {len(all_leads)} leads")
        logger.info(f"✅ Qualified: {len(final_leads)} leads (score >= {self.config.qualification_threshold})")
        logger.info(f"   🔴 High quality (70+): {len(high_quality)}")
        logger.info(f"   🟠 Medium quality (40-69): {len(medium_quality)}")
        logger.info(f"   🔵 Low quality (<40): {len(low_quality)}")
        logger.info(f"⏱️  Time: {total_time:.1f}s (classify: {classification_time}ms, extract: {extraction_time}ms)")
        
        self._stats['total_pages'] += len(pages)
        self._stats['total_leads'] += len(final_leads)
        
        return PipelineResult(
            source_name=source_name,
            pages_scraped=len(pages),
            urls_rejected_quick=urls_rejected,
            pages_classified=len(pages),
            pages_relevant=len(relevant_pages),
            pages_not_relevant=len(not_relevant_pages),
            leads_extracted=len(all_leads),
            leads_qualified=len(final_leads),
            leads_high_quality=len(high_quality),
            leads_medium_quality=len(medium_quality),
            leads_low_quality=len(low_quality),
            final_leads=final_leads,
            total_time_seconds=total_time,
            classification_time_ms=classification_time,
            extraction_time_ms=extraction_time
        )
    
    def get_stats(self) -> Dict:
        """Get overall pipeline statistics"""
        return {
            'pipeline': self._stats,
            'quick_reject': self.quick_reject.get_stats(),
            'classifier': self.classifier.get_stats(),
            'extractor': self.extractor.get_stats()
        }


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def test_classification():
    """Test the classifier with sample content"""
    config = PipelineConfig()
    classifier = ContentClassifier(config)
    
    # Test cases
    test_cases = [
        ("https://example.com/airlines", """
        American Airlines Adds More St Croix Flights
        American Airlines has added more flights to St Croix.
        The airline is now running two daily flights from Miami.
        "We welcome the operation," said Vicki Locke, Director of Sales 
        at The Buccaneer Beach & Golf Resort.
        """),
        
        ("https://example.com/hotel-opening", """
        JW Marriott Announces New Dominican Republic Resort
        Marriott International today announced plans for a new JW Marriott 
        resort in Cap Cana, Dominican Republic. The 200-room luxury resort 
        is expected to open in Q2 2026. The development is part of Marriott's 
        expansion in the Caribbean region.
        """),
        
        ("https://example.com/travel-guide", """
        10 Best Beaches in Jamaica
        Jamaica is known for its beautiful beaches. Here are our top picks
        for your next vacation. 1. Seven Mile Beach in Negril offers 
        stunning sunsets and calm waters.
        """),
    ]
    
    print("\n" + "="*60)
    print("🧪 TESTING CLASSIFIER")
    print("="*60)
    
    for url, content in test_cases:
        result = await classifier.classify(url, content)
        print(f"\n📄 URL: {url}")
        print(f"   Summary: {result.summary}")
        print(f"   Is Hotel Opening: {'✅ YES' if result.is_hotel_opening else '❌ NO'}")
        print(f"   Confidence: {result.confidence:.0%}")
        print(f"   Reasoning: {result.reasoning}")


async def main():
    """Main entry point for testing"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        await test_classification()
    else:
        print("""
INTELLIGENT PIPELINE
====================

Usage:
    python -m app.services.intelligent_pipeline test    # Test classifier

This module is meant to be imported by the orchestrator.
        """)


if __name__ == "__main__":
    asyncio.run(main())