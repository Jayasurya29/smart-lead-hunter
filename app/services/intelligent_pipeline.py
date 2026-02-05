"""
SMART LEAD HUNTER - UNIFIED INTELLIGENT PIPELINE
=================================================
Single pipeline for all AI-powered lead extraction.

REPLACES:
- intelligent_pipeline.py (old)
- lead_extraction_pipeline.py (old)

STAGES:
1. Quick Reject (FREE) - Filter junk URLs
2. Classification (CHEAP) - Is this about a new hotel opening?
3. Extraction (FULL) - Extract all hotel details
4. Qualification (FREE) - Score using scorer.py
5. Priority & Contact Analysis - Timing and contact relevance

AI PROVIDER: Google Gemini
- Classifier: gemini-2.5-flash-lite (fast, cheap)
- Extractor: gemini-2.5-flash (more capable)

Usage:
    from app.services.intelligent_pipeline import IntelligentPipeline
    
    pipeline = IntelligentPipeline()
    result = await pipeline.process_pages(pages)
    
    for lead in result.final_leads:
        print(lead.hotel_name, lead.qualification_score, lead.lead_priority)
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

import httpx

# Import scorer for qualification (no more duplicate brand lists!)
from app.services.scorer import (
    calculate_lead_score,
    should_skip_brand,
    should_skip_location,
    get_brand_tier,
    get_brand_tier_name,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    # API Key (from env if not provided)
    gemini_api_key: str = ""
    
    # Models
    classifier_model: str = "gemini-2.5-flash-lite"  #for classification
    extractor_model: str = "gemini-2.5-flash-lite"   # for extraction
    
    # Thresholds
    classification_confidence: float = 0.6  # Min confidence to extract
    qualification_threshold: int = 30       # Min score to keep lead
    
    # Rate limiting
    min_delay_seconds: float = 1.0  # Between API calls
    
    def __post_init__(self):
        if not self.gemini_api_key:
            self.gemini_api_key = os.getenv("GEMINI_API_KEY", "")


# =============================================================================
# ENUMS
# =============================================================================

class LeadPriority(Enum):
    """Lead priority based on opening timeline"""
    HOT = "🔴 HOT"           # 0-9 months - ACT NOW!
    WARM = "🟠 WARM"         # 9-18 months - Build relationship
    DEVELOPING = "🟡 DEVELOPING"  # 18-24 months - Monitor
    COLD = "🔵 COLD"         # 24+ months - Track only
    MISSED = "⚫ MISSED"     # Already opened
    UNKNOWN = "⚪ UNKNOWN"   # No opening date


class ContactRelevance(Enum):
    """Contact relevance for uniform sales"""
    HIGH = "HIGH"       # GM, Exec Housekeeper, Purchasing Director
    MEDIUM = "MEDIUM"   # Director of Rooms, HR Director
    LOW = "LOW"         # PR, Marketing, Communications
    UNKNOWN = "UNKNOWN"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ClassificationResult:
    """Result from content classification"""
    url: str
    summary: str
    is_relevant: bool
    confidence: float
    reasoning: str
    processing_time_ms: int = 0
    
    @property
    def should_extract(self) -> bool:
        return self.is_relevant and self.confidence >= 0.6


@dataclass 
class ExtractedLead:
    """
    A hotel lead extracted from content.
    This is the SINGLE lead class used throughout the system.
    """
    # Hotel Identity
    hotel_name: str = ""
    brand: str = ""
    property_type: str = ""  # resort, hotel, boutique, etc.
    
    # Location
    city: str = ""
    state: str = ""
    country: str = "USA"
    
    # Timeline
    opening_date: str = ""
    opening_status: str = ""  # announced, under construction, opening soon
    
    # Size
    room_count: int = 0
    
    # Stakeholders
    management_company: str = ""
    developer: str = ""
    owner: str = ""
    
    # Contact Info
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    contact_relevance: str = ""  # HIGH, MEDIUM, LOW
    
    # Insights
    key_insights: str = ""
    amenities: str = ""
    investment_amount: str = ""
    
    # Source Tracking
    source_url: str = ""
    source_name: str = ""
    source_urls: List[str] = field(default_factory=list)
    source_names: List[str] = field(default_factory=list)
    merged_from_count: int = 1
    extracted_at: str = ""
    
    # Quality Scores
    confidence_score: float = 0.0
    qualification_score: int = 0
    
    # Qualification Details (from scorer.py)
    brand_tier: str = ""
    location_type: str = ""
    opening_year: Optional[int] = None
    skip_reason: str = ""
    
    # Priority (from LeadPriorityCalculator)
    lead_priority: str = ""  # HOT, WARM, DEVELOPING, COLD, MISSED
    lead_priority_reason: str = ""
    months_to_opening: Optional[int] = None
    uniform_decision_window: str = ""  # NOW, SOON, LATER, MISSED
    
    # Revenue Estimates
    estimated_revenue: int = 0
    estimated_staff: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'hotel_name': self.hotel_name,
            'brand': self.brand,
            'property_type': self.property_type,
            'hotel_type': self.property_type,  # Alias for compatibility
            'city': self.city,
            'state': self.state,
            'country': self.country,
            'opening_date': self.opening_date,
            'opening_status': self.opening_status,
            'room_count': self.room_count,
            'management_company': self.management_company,
            'developer': self.developer,
            'owner': self.owner,
            'contact_name': self.contact_name,
            'contact_title': self.contact_title,
            'contact_email': self.contact_email,
            'contact_phone': self.contact_phone,
            'contact_relevance': self.contact_relevance,
            'key_insights': self.key_insights,
            'amenities': self.amenities,
            'investment_amount': self.investment_amount,
            'source_url': self.source_url,
            'source_name': self.source_name,
            'source_urls': ' | '.join(self.source_urls) if self.source_urls else self.source_url,
            'source_names': ' | '.join(self.source_names) if self.source_names else self.source_name,
            'merged_from_count': self.merged_from_count,
            'extracted_at': self.extracted_at,
            'confidence_score': self.confidence_score,
            'qualification_score': self.qualification_score,
            'brand_tier': self.brand_tier,
            'location_type': self.location_type,
            'opening_year': self.opening_year,
            'lead_priority': self.lead_priority,
            'lead_priority_reason': self.lead_priority_reason,
            'months_to_opening': self.months_to_opening,
            'uniform_decision_window': self.uniform_decision_window,
            'estimated_revenue': self.estimated_revenue,
            'estimated_staff': self.estimated_staff,
        }


@dataclass
class ExtractionResult:
    """Result from extraction (for compatibility)"""
    leads: List[ExtractedLead] = field(default_factory=list)
    success: bool = False
    error: Optional[str] = None
    source_url: str = ""
    source_name: str = ""


@dataclass
class PipelineResult:
    """Result from full pipeline run"""
    source_name: str = ""
    pages_scraped: int = 0
    pages_classified: int = 0
    pages_relevant: int = 0
    pages_not_relevant: int = 0
    leads_extracted: int = 0
    leads_qualified: int = 0
    leads_high_quality: int = 0
    leads_medium_quality: int = 0
    leads_low_quality: int = 0
    final_leads: List[ExtractedLead] = field(default_factory=list)
    total_time_seconds: float = 0.0
    classification_time_ms: int = 0
    extraction_time_ms: int = 0


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
    
    PRIORITY:
    - HOT (0-9 months): Decisions happening NOW!
    - WARM (9-18 months): Perfect timing for proposals
    - DEVELOPING (18-24 months): Build relationships
    - COLD (24+ months): Too early, just track
    - MISSED (<3 months or opened): Too late
    """
    
    MONTH_MAPPING = {
        'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
        'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
        'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
        'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
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
        """Parse opening date to (year, month)"""
        if not date_str:
            return None
        
        date_lower = date_str.lower().strip()
        
        # Already opened?
        if any(word in date_lower for word in ['opened', 'open now', 'recently opened']):
            return (datetime.now().year, datetime.now().month)
        
        # Extract year
        year_match = re.search(r'20\d{2}', date_str)
        if not year_match:
            return None
        year = int(year_match.group())
        
        # Try to extract month
        month = 6  # Default mid-year
        
        for month_name, month_num in self.MONTH_MAPPING.items():
            if month_name in date_lower:
                month = month_num
                break
        else:
            for quarter, month_num in self.QUARTER_MAPPING.items():
                if quarter in date_lower:
                    month = month_num
                    break
            else:
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
        opening_date = datetime(year, month, 15)
        months = (opening_date.year - now.year) * 12 + (opening_date.month - now.month)
        
        return months
    
    def calculate_priority(self, date_str: str) -> Tuple[LeadPriority, str, Optional[int], str]:
        """
        Calculate lead priority.
        Returns: (priority, reason, months_to_opening, decision_window)
        """
        months = self.calculate_months_to_opening(date_str)
        
        if months is None:
            return (LeadPriority.UNKNOWN, "No opening date", None, "UNKNOWN")
        
        if months < 0:
            return (LeadPriority.MISSED, f"Opened {abs(months)} months ago", months, "MISSED")
        
        if months < 3:
            return (LeadPriority.MISSED, f"Opens in {months} months - too late", months, "MISSED")
        
        if months <= 9:
            return (LeadPriority.HOT, f"Opens in {months} months - DECIDE NOW!", months, "NOW")
        
        if months <= 18:
            return (LeadPriority.WARM, f"Opens in {months} months - perfect timing", months, "SOON")
        
        if months <= 24:
            return (LeadPriority.DEVELOPING, f"Opens in {months} months - build relationship", months, "LATER")
        
        return (LeadPriority.COLD, f"Opens in {months} months - too early", months, "LATER")


# =============================================================================
# CONTACT RELEVANCE CLASSIFIER
# =============================================================================

class ContactRelevanceClassifier:
    """
    Classifies contacts by relevance for uniform sales.
    
    HIGH: Decision makers - GM, Exec Housekeeper, Purchasing
    MEDIUM: Influencers - HR, F&B Director, Rooms Director
    LOW: Not decision makers - PR, Marketing, Communications
    """
    
    HIGH_TITLES = [
        'general manager', 'gm', 'hotel manager', 'resident manager',
        'executive housekeeper', 'director of housekeeping',
        'director of purchasing', 'purchasing manager', 'procurement',
        'director of operations', 'operations manager',
        'pre-opening director', 'pre-opening manager',
        'director of rooms', 'rooms division',
    ]
    
    MEDIUM_TITLES = [
        'hr director', 'human resources', 'director of hr',
        'food and beverage director', 'f&b director', 'f & b',
        'front office manager', 'front desk manager',
        'director of finance', 'controller', 'cfo',
        'chief engineer', 'director of engineering',
        'regional manager', 'area manager', 'regional director',
        'assistant general manager', 'agm',
    ]
    
    LOW_TITLES = [
        'vp communications', 'communications director', 'communications manager',
        'pr manager', 'public relations', 'media relations',
        'marketing director', 'marketing manager', 'brand manager',
        'social media', 'digital marketing',
        'investor relations',
        'svp', 'senior vice president',
        'ceo', 'president', 'chairman',  # Too high level for uniform purchasing
    ]
    
    @classmethod
    def classify(cls, title: str) -> Tuple[ContactRelevance, str]:
        """
        Classify contact relevance.
        Returns: (relevance, reason)
        """
        if not title:
            return (ContactRelevance.UNKNOWN, "No title provided")
        
        title_lower = title.lower()
        
        for t in cls.HIGH_TITLES:
            if t in title_lower:
                return (ContactRelevance.HIGH, "Key decision maker")
        
        for t in cls.MEDIUM_TITLES:
            if t in title_lower:
                return (ContactRelevance.MEDIUM, "Influencer")
        
        for t in cls.LOW_TITLES:
            if t in title_lower:
                return (ContactRelevance.LOW, "PR/Marketing - not purchasing")
        
        return (ContactRelevance.MEDIUM, "Unknown role - assume influencer")


# =============================================================================
# STAGE 1: QUICK REJECT FILTER (FREE)
# =============================================================================

class QuickRejectFilter:
    """
    Stage 1: Instantly reject URLs that are NEVER useful.
    Cost: FREE (no AI)
    """
    
    JUNK_PATTERNS = [
        # Auth & user pages
        r'/login', r'/signin', r'/signup', r'/register', r'/logout',
        r'/account', r'/profile', r'/settings', r'/password',
        
        # Site infrastructure  
        r'/contact', r'/about-us', r'/about$', r'/privacy', r'/terms',
        r'/cookie', r'/sitemap', r'/robots', r'/feed', r'/rss',
        r'/wp-admin', r'/wp-login', r'/admin', r'/_next/', r'/_nuxt/',
        
        # Media
        r'/video-gallery', r'/photo-gallery', r'/gallery', r'/podcast',
        r'\.pdf$', r'\.jpg$', r'\.png$', r'\.mp4$',
        
        # Social
        r'facebook\.com', r'twitter\.com', r'instagram\.com', r'linkedin\.com',
        r'youtube\.com', r'mailto:', r'tel:', r'javascript:',
        
        # E-commerce
        r'/cart', r'/checkout', r'/shop', r'/store', r'/subscribe',
        
        # Navigation
        r'/search\?', r'/search$', r'/tag/', r'/tags/', r'/author/',
        r'/page/\d+$', r'#',
    ]
    
    def __init__(self):
        self._patterns = [re.compile(p, re.IGNORECASE) for p in self.JUNK_PATTERNS]
        self._stats = {'checked': 0, 'rejected': 0, 'passed': 0}
    
    def should_reject(self, url: str) -> Tuple[bool, str]:
        """Check if URL should be rejected"""
        self._stats['checked'] += 1
        
        for pattern in self._patterns:
            if pattern.search(url):
                self._stats['rejected'] += 1
                return True, f"Junk pattern: {pattern.pattern}"
        
        self._stats['passed'] += 1
        return False, ""
    
    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()


# =============================================================================
# STAGE 2: CONTENT CLASSIFIER (CHEAP AI)
# =============================================================================

class ContentClassifier:
    """
    Stage 2: Classify if content is about a new hotel opening.
    Cost: ~$0.0001 per page
    """
    
    PROMPT = """You are a hotel industry analyst. Classify this content.

TASK: Is this about a NEW HOTEL OPENING in 2026 or later?

TARGET LOCATIONS (we ONLY care about):
- Florida (Miami, Orlando, Tampa, etc.)
- Caribbean (Bahamas, Jamaica, Puerto Rico, etc.)
- Other USA states

RELEVANT:
✅ A NAMED hotel opening in 2026+ in USA/Caribbean
✅ Hotel under construction in USA/Caribbean
✅ Press release about new hotel development in USA/Caribbean

NOT RELEVANT:
❌ Hotels already opened (2025 or earlier)
❌ International hotels (Europe, Asia, Middle East)
❌ Executive appointments
❌ Hotel reviews
❌ Airlines/cruises

CONTENT:
---
{content}
---

Respond in JSON only:
{{
    "summary": "One sentence about this page",
    "is_new_hotel_opening": true/false,
    "confidence": 0.0-1.0,
    "reasoning": "Brief explanation"
}}"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.api_key = config.gemini_api_key
        self._last_call = 0.0
        self._stats = {'classified': 0, 'relevant': 0, 'errors': 0}
    
    async def classify(self, url: str, content: str) -> ClassificationResult:
        """Classify content relevance"""
        start = time.time()
        
        # Truncate for classification
        truncated = content[:3000] if len(content) > 3000 else content
        
        try:
            # Rate limiting
            elapsed = time.time() - self._last_call
            if elapsed < self.config.min_delay_seconds:
                await asyncio.sleep(self.config.min_delay_seconds - elapsed)
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self.config.classifier_model}:generateContent?key={self.api_key}",
                    json={
                        "contents": [{"parts": [{"text": self.PROMPT.format(content=truncated)}]}],
                        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 200}
                    }
                )
                self._last_call = time.time()
                
                if response.status_code == 200:
                    result = response.json()
                    text = result['candidates'][0]['content']['parts'][0]['text']

                    # Clean markdown code blocks
                    text = text.replace("```json", "").replace("```", "").strip()
                    
                    # Parse JSON
                    json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
                    if json_match:
                        data = json.loads(json_match.group())
                        
                        is_relevant = data.get('is_new_hotel_opening', False)
                        self._stats['classified'] += 1
                        if is_relevant:
                            self._stats['relevant'] += 1
                        
                        return ClassificationResult(
                            url=url,
                            summary=data.get('summary', 'Unknown'),
                            is_relevant=is_relevant,
                            confidence=float(data.get('confidence', 0.5)),
                            reasoning=data.get('reasoning', ''),
                            processing_time_ms=int((time.time() - start) * 1000)
                        )
                
                self._stats['errors'] += 1
                return ClassificationResult(
                    url=url, summary="Classification failed",
                    is_relevant=False, confidence=0.0,
                    reasoning=f"API error: {response.status_code}",
                    processing_time_ms=int((time.time() - start) * 1000)
                )
                
        except Exception as e:
            self._stats['errors'] += 1
            return ClassificationResult(
                url=url, summary="Error",
                is_relevant=False, confidence=0.0,
                reasoning=str(e),
                processing_time_ms=int((time.time() - start) * 1000)
            )
    
    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()


# =============================================================================
# STAGE 3: LEAD EXTRACTOR (FULL AI)
# =============================================================================

class LeadExtractor:
    """
    Stage 3: Extract hotel details from relevant content.
    Cost: ~$0.001 per page
    """
    
    PROMPT = """You are a hotel data extraction specialist.

Extract information about NEW HOTEL OPENINGS from this article.

RULES:
1. Only extract NEW hotels being announced (not existing hotels)
2. Leave fields empty if not clearly stated
3. For opening_date use format "Month YYYY" or "Q1 2026" or "2026"
4. key_insights is REQUIRED - include staffing numbers, amenities, investment

KEY INSIGHTS TO CAPTURE (critical for uniform sales!):
- Staff hiring numbers (e.g., "hiring 300 employees" = big order!)
- Number of restaurants/bars (each needs different uniforms)
- Spa facilities (spa staff need specific uniforms)
- Pool/beach staff mentioned
- Management company (some have uniform programs)
- Construction timeline
- Investment/budget amounts

CONTENT:
---
{content}
---

SOURCE: {source_url}

Return JSON array (even if just one hotel):
[
    {{
        "hotel_name": "Full official name",
        "brand": "Brand (Marriott, Hilton, etc.)",
        "property_type": "resort/hotel/boutique",
        "city": "City",
        "state": "State",
        "country": "Country (default USA)",
        "opening_date": "When it opens",
        "opening_status": "announced/under construction/opening soon",
        "room_count": number or 0,
        "management_company": "Who operates it",
        "developer": "Who is building it",
        "owner": "Who owns it",
        "contact_name": "Contact person if mentioned",
        "contact_title": "Their title",
        "contact_email": "Email if found",
        "contact_phone": "Phone if found",
        "key_insights": "REQUIRED: Staff numbers, F&B outlets, spa, investment, unique features",
        "amenities": "Key amenities",
        "investment_amount": "$ amount if mentioned"
    }}
]

Return [] if no new hotels found."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.api_key = config.gemini_api_key
        self._last_call = 0.0
        self._stats = {'extracted': 0, 'leads': 0, 'errors': 0}
    
    async def extract(self, url: str, content: str, source_name: str = "") -> List[ExtractedLead]:
        """Extract leads from content"""
        self._stats['extracted'] += 1
        
        try:
            # Rate limiting
            elapsed = time.time() - self._last_call
            if elapsed < self.config.min_delay_seconds:
                await asyncio.sleep(self.config.min_delay_seconds - elapsed)
            
            # Truncate content
            truncated = content[:8000] if len(content) > 8000 else content
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self.config.extractor_model}:generateContent?key={self.api_key}",
                    json={
                        "contents": [{"parts": [{"text": self.PROMPT.format(content=truncated, source_url=url)}]}],
                        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 8000}
                    }
                )
                self._last_call = time.time()
                
                if response.status_code == 200:
                    result = response.json()
                    text = result['candidates'][0]['content']['parts'][0]['text']
                    
                    
                    # Parse JSON array
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
                                    country=hotel.get('country', 'USA'),
                                    opening_date=hotel.get('opening_date', ''),
                                    opening_status=hotel.get('opening_status', ''),
                                    room_count=int(hotel.get('room_count', 0) or 0),
                                    management_company=hotel.get('management_company', ''),
                                    developer=hotel.get('developer', ''),
                                    owner=hotel.get('owner', ''),
                                    contact_name=hotel.get('contact_name', ''),
                                    contact_title=hotel.get('contact_title', ''),
                                    contact_email=hotel.get('contact_email', ''),
                                    contact_phone=hotel.get('contact_phone', ''),
                                    key_insights=hotel.get('key_insights', ''),
                                    amenities=hotel.get('amenities', ''),
                                    investment_amount=hotel.get('investment_amount', ''),
                                    source_url=url,
                                    source_name=source_name,
                                    source_urls=[url],
                                    source_names=[source_name] if source_name else [],
                                    extracted_at=datetime.now().isoformat(),
                                )
                                leads.append(lead)
                        
                        self._stats['leads'] += len(leads)
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
# STAGE 4: LEAD QUALIFIER (Uses scorer.py + Priority + Contact)
# =============================================================================

class LeadQualifier:
    """
    Stage 4: Score and filter leads.
    - Uses scorer.py for qualification score
    - Uses LeadPriorityCalculator for timing
    - Uses ContactRelevanceClassifier for contact quality
    """
    
    def __init__(self):
        self.priority_calculator = LeadPriorityCalculator()
    
    def qualify(self, lead: ExtractedLead) -> ExtractedLead:
        """Score a lead"""
        
        # 1. QUALIFICATION SCORE (from scorer.py)
        result = calculate_lead_score(
            hotel_name=lead.hotel_name,
            city=lead.city,
            state=lead.state,
            country=lead.country,
            opening_date=lead.opening_date,
            room_count=lead.room_count,
            contact_name=lead.contact_name,
            contact_email=lead.contact_email,
            contact_phone=lead.contact_phone,
            brand=lead.brand,
        )
        
        lead.qualification_score = result['total_score']
        lead.brand_tier = result.get('brand_tier', '')
        lead.location_type = result.get('location_type', '')
        lead.opening_year = result.get('opening_year')
        
        if not result['should_save']:
            lead.skip_reason = result.get('skip_reason', '')
            lead.qualification_score = 0
            return lead
        
        # 2. LEAD PRIORITY (timing)
        priority, reason, months, window = self.priority_calculator.calculate_priority(
            lead.opening_date
        )
        lead.lead_priority = priority.value
        lead.lead_priority_reason = reason
        lead.months_to_opening = months
        lead.uniform_decision_window = window
        
        # 3. CONTACT RELEVANCE
        if lead.contact_title:
            relevance, _ = ContactRelevanceClassifier.classify(lead.contact_title)
            lead.contact_relevance = relevance.value
        
        # 4. REVENUE ESTIMATE
        room_count = lead.room_count or 150
        staff_ratio = {
            'tier1_ultra_luxury': 2.5, 'tier2_luxury': 1.8,
            'tier3_upper_upscale': 1.3, 'tier4_upscale': 1.0
        }.get(lead.brand_tier, 1.2)
        uniform_cost = {
            'tier1_ultra_luxury': 500, 'tier2_luxury': 400,
            'tier3_upper_upscale': 275, 'tier4_upscale': 200
        }.get(lead.brand_tier, 250)
        
        lead.estimated_staff = int(room_count * staff_ratio)
        lead.estimated_revenue = lead.estimated_staff * uniform_cost
        
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
            logger.info(f"   🚫 Filtered out {skipped} leads (budget/international/old)")
        
        return qualified


# =============================================================================
# MAIN PIPELINE
# =============================================================================

class IntelligentPipeline:
    """
    The main intelligent extraction pipeline.
    
    Usage:
        pipeline = IntelligentPipeline()
        result = await pipeline.process_pages(pages)
    """
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        
        self.quick_reject = QuickRejectFilter()
        self.classifier = ContentClassifier(self.config)
        self.extractor = LeadExtractor(self.config)
        self.qualifier = LeadQualifier()
        
        self._stats = {'runs': 0, 'pages': 0, 'leads': 0}
    
    async def process_pages(
        self,
        pages: List[Dict[str, str]],
        source_name: str = ""
    ) -> PipelineResult:
        """
        Process pages through the full pipeline.
        
        Args:
            pages: List of {'url': ..., 'content': ..., 'source': ...}
            source_name: Name of the source
            
        Returns:
            PipelineResult with all leads and stats
        """
        start_time = time.time()
        self._stats['runs'] += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🧠 INTELLIGENT PIPELINE - {len(pages)} pages")
        logger.info(f"{'='*60}")
        
        # STAGE 2: CLASSIFICATION
        logger.info(f"\n📊 STAGE 2: Classifying {len(pages)} pages...")
        classification_start = time.time()
        
        relevant_pages = []
        not_relevant = 0
        
        for page in pages:
            url = page.get('url', '')
            content = page.get('content', '') or page.get('text', '')
            
            if not content or len(content) < 100:
                continue
            
            result = await self.classifier.classify(url, content)
            
            if result.should_extract:
                relevant_pages.append({
                    'url': url,
                    'content': content,
                    'source': page.get('source', source_name),
                })
                logger.info(f"✅ RELEVANT: {result.summary[:60]}...")
            else:
                not_relevant += 1
        
        classification_time = int((time.time() - classification_start) * 1000)
        logger.info(f"✅ {len(relevant_pages)} relevant, {not_relevant} skipped")
        
        # STAGE 3: EXTRACTION
        logger.info(f"\n🔍 STAGE 3: Extracting from {len(relevant_pages)} pages...")
        extraction_start = time.time()
        
        all_leads = []
        for page in relevant_pages:
            leads = await self.extractor.extract(
                page['url'], 
                page['content'],
                page.get('source', source_name)
            )
            if leads:
                all_leads.extend(leads)
                for lead in leads:
                    logger.info(f"   📝 {lead.hotel_name} ({lead.city}, {lead.state})")
        
        extraction_time = int((time.time() - extraction_start) * 1000)
        logger.info(f"✅ Extracted {len(all_leads)} leads")
        
        # STAGE 4: QUALIFICATION
        logger.info(f"\n⭐ STAGE 4: Qualifying {len(all_leads)} leads...")
        
        qualified_leads = self.qualifier.qualify_batch(all_leads)
        
        # Filter by threshold
        final_leads = [l for l in qualified_leads 
                       if l.qualification_score >= self.config.qualification_threshold]
        
        # Categorize
        high_quality = len([l for l in final_leads if l.qualification_score >= 70])
        medium_quality = len([l for l in final_leads if 40 <= l.qualification_score < 70])
        low_quality = len([l for l in final_leads if l.qualification_score < 40])
        
        # Count by priority
        hot_leads = len([l for l in final_leads if 'HOT' in l.lead_priority])
        warm_leads = len([l for l in final_leads if 'WARM' in l.lead_priority])
        
        total_time = time.time() - start_time
        
        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 PIPELINE COMPLETE")
        logger.info(f"{'='*60}")
        logger.info(f"📊 Classified: {len(relevant_pages)} relevant / {len(pages)} total")
        logger.info(f"📝 Extracted: {len(all_leads)} leads")
        logger.info(f"✅ Qualified: {len(final_leads)} leads (score >= {self.config.qualification_threshold})")
        logger.info(f"   🔴 High (70+): {high_quality}")
        logger.info(f"   🟠 Medium (40-69): {medium_quality}")
        logger.info(f"   🔵 Low (<40): {low_quality}")
        logger.info(f"⏰ Priority: 🔴 {hot_leads} HOT | 🟠 {warm_leads} WARM")
        logger.info(f"⏱️ Time: {total_time:.1f}s")
        
        self._stats['pages'] += len(pages)
        self._stats['leads'] += len(final_leads)
        
        return PipelineResult(
            source_name=source_name,
            pages_scraped=len(pages),
            pages_classified=len(pages),
            pages_relevant=len(relevant_pages),
            pages_not_relevant=not_relevant,
            leads_extracted=len(all_leads),
            leads_qualified=len(final_leads),
            leads_high_quality=high_quality,
            leads_medium_quality=medium_quality,
            leads_low_quality=low_quality,
            final_leads=final_leads,
            total_time_seconds=total_time,
            classification_time_ms=classification_time,
            extraction_time_ms=extraction_time
        )
    
    async def extract(
        self,
        content: str,
        source_url: str = "",
        source_name: str = ""
    ) -> ExtractionResult:
        """
        Direct extraction (skips classification).
        For compatibility with old code.
        """
        result = ExtractionResult(source_url=source_url, source_name=source_name)
        
        if not content or len(content) < 100:
            result.error = "Content too short"
            return result
        
        try:
            leads = await self.extractor.extract(source_url, content, source_name)
            qualified = self.qualifier.qualify_batch(leads)
            
            result.leads = qualified
            result.success = True
            
        except Exception as e:
            result.error = str(e)
        
        return result
    
    def get_stats(self) -> Dict:
        """Get pipeline statistics"""
        return {
            'pipeline': self._stats,
            'quick_reject': self.quick_reject.get_stats(),
            'classifier': self.classifier.get_stats(),
            'extractor': self.extractor.get_stats()
        }


# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================

class LeadExtractionPipeline(IntelligentPipeline):
    """Alias for backward compatibility with old imports"""
    
    def __init__(self, gemini_api_key: str = None, use_ollama: bool = True):
        config = PipelineConfig(gemini_api_key=gemini_api_key or "")
        super().__init__(config)


class LeadDeduplicator:
    """Simple deduplicator for backward compatibility"""
    
    @staticmethod
    def normalize_name(name: str) -> str:
        if not name:
            return ""
        name = name.lower()
        name = re.sub(r'[^\w\s]', '', name)
        stopwords = ['the', 'hotel', 'resort', 'spa', 'and', 'at', 'by']
        words = [w for w in name.split() if w not in stopwords]
        return ' '.join(sorted(words))
    
    @staticmethod
    def deduplicate(leads: List[ExtractedLead]) -> List[ExtractedLead]:
        """Remove duplicates based on normalized name"""
        seen = set()
        unique = []
        
        for lead in leads:
            key = LeadDeduplicator.normalize_name(lead.hotel_name)
            if key and key not in seen:
                seen.add(key)
                unique.append(lead)
        
        return unique


def create_pipeline(gemini_api_key: str = None) -> IntelligentPipeline:
    """Create a pipeline instance"""
    config = PipelineConfig(gemini_api_key=gemini_api_key or "")
    return IntelligentPipeline(config)


# =============================================================================
# CLI TEST
# =============================================================================

async def main():
    """Test the pipeline"""
    print("=" * 60)
    print("INTELLIGENT PIPELINE TEST")
    print("=" * 60)
    
    config = PipelineConfig()
    
    if config.gemini_api_key:
        print(f"✅ Gemini API key found")
    else:
        print("❌ No GEMINI_API_KEY in environment")
        return
    
    pipeline = IntelligentPipeline(config)
    
    # Test content
    test_content = """
    Ritz-Carlton Announces New Luxury Hotel in Miami Beach
    
    The Ritz-Carlton Hotel Company announced plans for a new 200-room luxury 
    resort in Miami Beach, Florida, expected to open in Q2 2026.
    
    The property will feature a full-service spa, three restaurants, and 
    15,000 square feet of meeting space. The hotel will hire approximately 
    400 staff members.
    
    "We're excited to expand our presence in South Florida," said John Smith,
    General Manager.
    """
    
    result = await pipeline.extract(test_content, "https://test.com", "Test")
    
    if result.success and result.leads:
        lead = result.leads[0]
        print(f"\n✅ Extracted: {lead.hotel_name}")
        print(f"   Location: {lead.city}, {lead.state}")
        print(f"   Opening: {lead.opening_date}")
        print(f"   Score: {lead.qualification_score}")
        print(f"   Priority: {lead.lead_priority}")
        print(f"   Contact Relevance: {lead.contact_relevance}")
        print(f"   Est. Revenue: ${lead.estimated_revenue:,}")
        print(f"   Key Insights: {lead.key_insights[:100]}...")
    else:
        print(f"\n❌ Failed: {result.error}")


if __name__ == "__main__":
    asyncio.run(main())