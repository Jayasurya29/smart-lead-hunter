"""
SMART LEAD HUNTER - UNIFIED INTELLIGENT PIPELINE
=================================================
Single pipeline for all AI-powered lead extraction.


STAGES:
1. Quick Reject (FREE) - Filter junk URLs
2. Classification (CHEAP) - Is this about a new hotel opening?
3. Extraction (FULL) - Extract all hotel details
4. Qualification (FREE) - Score using scorer.py
5. Priority & Contact Analysis - Timing and contact relevance

AI PROVIDER: Google Gemini
-Classifier: gemini-2.5-flash-lite (4,000 RPM / Unlimited RPD)
-Extractor: gemini-3-flash (1,000 RPM / 10,000 RPD)


Usage:
    from app.services.intelligent_pipeline import IntelligentPipeline

    pipeline = IntelligentPipeline()
    result = await pipeline.process_pages(pages)

    for lead in result.final_leads:
        print(lead.hotel_name, lead.qualification_score, lead.lead_priority)
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

import httpx

# Import scorer for qualification (no more duplicate brand lists!)
from app.services.scorer import (
    calculate_lead_score,
)


def _safe_int(value, default: int = 0) -> int:
    """Safely parse an integer from various formats (Audit Fix #5).

    Handles: 200, "200", "approximately 200", "200-300", "200+", None
    """
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        # Extract first number from string
        match = re.search(r"\d+", str(value))
        return int(match.group()) if match else default


logger = logging.getLogger(__name__)


# =============================================================================
# GEMINI CIRCUIT BREAKER
# =============================================================================
# Prevents wasted API calls when Gemini is down or rate-limited.
# After FAILURE_THRESHOLD consecutive failures, the breaker "opens" and
# rejects calls immediately for RECOVERY_TIMEOUT seconds.
# After timeout, allows ONE test call ("half-open"). If it succeeds,
# the breaker closes and normal operation resumes.


class GeminiCircuitBreaker:
    """Circuit breaker for Gemini API calls."""

    FAILURE_THRESHOLD = 5  # consecutive failures to trip
    RECOVERY_TIMEOUT = 300  # seconds before retry (5 min)

    # States
    CLOSED = "closed"  # normal operation
    OPEN = "open"  # rejecting calls
    HALF_OPEN = "half_open"  # testing with one call

    def __init__(self):
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self._lock = None  # lazy init for async

    def can_call(self) -> bool:
        """Check if a Gemini call should be attempted."""
        if self.state == self.CLOSED:
            return True
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time >= self.RECOVERY_TIMEOUT:
                self.state = self.HALF_OPEN
                logger.info("Circuit breaker HALF-OPEN: testing Gemini...")
                return True
            return False
        # HALF_OPEN: allow one test call
        return True

    def record_success(self):
        """Record a successful Gemini call."""
        if self.state == self.HALF_OPEN:
            logger.info("Circuit breaker CLOSED: Gemini recovered")
        self.state = self.CLOSED
        self.failure_count = 0

    def record_failure(self):
        """Record a failed Gemini call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.FAILURE_THRESHOLD:
            self.state = self.OPEN
            logger.warning(
                f"Circuit breaker OPEN: {self.failure_count} consecutive Gemini failures. "
                f"Rejecting calls for {self.RECOVERY_TIMEOUT}s."
            )
        elif self.state == self.HALF_OPEN:
            self.state = self.OPEN
            logger.warning("Circuit breaker re-OPENED: test call failed")

    @property
    def is_open(self) -> bool:
        return self.state == self.OPEN


# Module-level singleton shared by classifier and extractor
_gemini_breaker = GeminiCircuitBreaker()


# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass
class PipelineConfig:
    """Pipeline configuration"""

    # API Key (from env if not provided)
    gemini_api_key: str = ""

    # Models — Gemini 3/2.5 configuration (no RPM bottlenecks)
    # Classifier: Flash Lite — 4,000 RPM / Unlimited RPD (binary yes/no)
    # Extractor: 3 Flash — 1,000 RPM / 10,000 RPD (structured extraction)
    classifier_model: str = "gemini-2.5-flash-lite"
    extractor_model: str = "gemini-3-flash"

    # Thresholds
    classification_confidence: float = 0.5
    qualification_threshold: int = 30  # Min score to keep lead

    # Rate limiting — Flash models have generous limits
    min_delay_seconds: float = 0.3

    # Concurrency
    max_concurrent_requests: int = 15

    # Content limits
    classifier_content_limit: int = 5000  # Chars for classification
    extractor_content_limit: int = 15000  # Chars for extraction

    # Redis extraction cache (skip re-extraction for same content)
    redis_cache_enabled: bool = True
    redis_cache_ttl_hours: int = 72
    redis_url: str = ""

    # TODO: Gemini Batch API (50% cost reduction when GA)
    use_batch_api: bool = False

    def __post_init__(self):
        if not self.gemini_api_key:
            self.gemini_api_key = os.getenv("GEMINI_API_KEY", "")
        if not self.redis_url:
            self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")


# =============================================================================
# ENUMS
# =============================================================================


class LeadPriority(Enum):
    """Lead priority based on opening timeline"""

    HOT = "🔴 HOT"  # 0-9 months - ACT NOW!
    WARM = "🟠 WARM"  # 9-18 months - Build relationship
    DEVELOPING = "🟡 DEVELOPING"  # 18-24 months - Monitor
    COLD = "🔵 COLD"  # 24+ months - Track only
    MISSED = "⚫ MISSED"  # Already opened
    UNKNOWN = "⚪ UNKNOWN"  # No opening date


class ContactRelevance(Enum):
    """Contact relevance for uniform sales"""

    HIGH = "HIGH"  # GM, Exec Housekeeper, Purchasing Director
    MEDIUM = "MEDIUM"  # Director of Rooms, HR Director
    LOW = "LOW"  # PR, Marketing, Communications
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
            "hotel_name": self.hotel_name,
            "brand": self.brand,
            "property_type": self.property_type,
            "hotel_type": self.property_type,  # Alias for compatibility
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "opening_date": self.opening_date,
            "opening_status": self.opening_status,
            "room_count": self.room_count,
            "management_company": self.management_company,
            "developer": self.developer,
            "owner": self.owner,
            "contact_name": self.contact_name,
            "contact_title": self.contact_title,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "contact_relevance": self.contact_relevance,
            "key_insights": self.key_insights,
            "amenities": self.amenities,
            "investment_amount": self.investment_amount,
            "source_url": self.source_url,
            "source_name": self.source_name,
            "source_urls": " | ".join(self.source_urls)
            if self.source_urls
            else self.source_url,
            "source_names": " | ".join(self.source_names)
            if self.source_names
            else self.source_name,
            "merged_from_count": self.merged_from_count,
            "extracted_at": self.extracted_at,
            "confidence_score": self.confidence_score,
            "qualification_score": self.qualification_score,
            "brand_tier": self.brand_tier,
            "location_type": self.location_type,
            "opening_year": self.opening_year,
            "lead_priority": self.lead_priority,
            "lead_priority_reason": self.lead_priority_reason,
            "months_to_opening": self.months_to_opening,
            "uniform_decision_window": self.uniform_decision_window,
            "estimated_revenue": self.estimated_revenue,
            "estimated_staff": self.estimated_staff,
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
    pages_rejected: int = 0  # pages rejected by QuickRejectFilter
    leads_extracted: int = 0
    leads_validated: int = 0  # leads that passed validation
    leads_qualified: int = 0
    leads_high_quality: int = 0
    leads_medium_quality: int = 0
    leads_low_quality: int = 0
    final_leads: List[ExtractedLead] = field(default_factory=list)
    relevant_urls: List[str] = field(default_factory=list)
    total_time_seconds: float = 0.0
    classification_time_ms: int = 0
    extraction_time_ms: int = 0

    # Quality metrics
    avg_classification_confidence: float = 0.0
    avg_lead_score: float = 0.0
    cache_hits: int = 0
    validation_rejects: int = 0
    source_type_detected: str = ""

    # Per-source breakdown for monitoring
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_metrics_dict(self) -> Dict[str, Any]:
        """Export metrics for logging / dashboard tracking."""
        return {
            "source": self.source_name,
            "source_type": self.source_type_detected,
            "timestamp": datetime.now().isoformat(),
            "pages_scraped": self.pages_scraped,
            "pages_relevant": self.pages_relevant,
            "pages_rejected": self.pages_rejected,
            "relevance_rate": round(
                self.pages_relevant / max(self.pages_classified, 1), 3
            ),
            "leads_extracted": self.leads_extracted,
            "leads_validated": self.leads_validated,
            "leads_qualified": self.leads_qualified,
            "extraction_rate": round(
                self.leads_extracted / max(self.pages_relevant, 1), 2
            ),
            "qualification_rate": round(
                self.leads_qualified / max(self.leads_extracted, 1), 3
            ),
            "avg_confidence": round(self.avg_classification_confidence, 3),
            "avg_lead_score": round(self.avg_lead_score, 1),
            "cache_hits": self.cache_hits,
            "validation_rejects": self.validation_rejects,
            "high_quality": self.leads_high_quality,
            "medium_quality": self.leads_medium_quality,
            "low_quality": self.leads_low_quality,
            "time_seconds": round(self.total_time_seconds, 1),
        }


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
        "january": 1,
        "jan": 1,
        "february": 2,
        "feb": 2,
        "march": 3,
        "mar": 3,
        "april": 4,
        "apr": 4,
        "may": 5,
        "june": 6,
        "jun": 6,
        "july": 7,
        "jul": 7,
        "august": 8,
        "aug": 8,
        "september": 9,
        "sep": 9,
        "sept": 9,
        "october": 10,
        "oct": 10,
        "november": 11,
        "nov": 11,
        "december": 12,
        "dec": 12,
    }

    QUARTER_MAPPING = {
        "q1": 2,
        "q2": 5,
        "q3": 8,
        "q4": 11,
        "first quarter": 2,
        "second quarter": 5,
        "third quarter": 8,
        "fourth quarter": 11,
    }

    SEASON_MAPPING = {
        "spring": 4,
        "summer": 7,
        "fall": 10,
        "autumn": 10,
        "winter": 1,
        "early": 3,
        "mid": 6,
        "late": 10,
    }

    def parse_opening_date(self, date_str: str) -> Optional[Tuple[int, int]]:
        """Parse opening date to (year, month)"""
        if not date_str:
            return None

        date_lower = date_str.lower().strip()

        # Already opened?
        if any(
            word in date_lower for word in ["opened", "open now", "recently opened"]
        ):
            return (datetime.now().year, datetime.now().month)

        # Extract year
        year_match = re.search(r"20\d{2}", date_str)
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

    def calculate_priority(
        self, date_str: str
    ) -> Tuple[LeadPriority, str, Optional[int], str]:
        """
        Calculate lead priority.
        Returns: (priority, reason, months_to_opening, decision_window)
        """
        months = self.calculate_months_to_opening(date_str)

        if months is None:
            return (LeadPriority.UNKNOWN, "No opening date", None, "UNKNOWN")

        if months < 0:
            return (
                LeadPriority.MISSED,
                f"Opened {abs(months)} months ago",
                months,
                "MISSED",
            )

        if months < 3:
            return (
                LeadPriority.MISSED,
                f"Opens in {months} months - too late",
                months,
                "MISSED",
            )

        if months <= 9:
            return (
                LeadPriority.HOT,
                f"Opens in {months} months - DECIDE NOW!",
                months,
                "NOW",
            )

        if months <= 18:
            return (
                LeadPriority.WARM,
                f"Opens in {months} months - perfect timing",
                months,
                "SOON",
            )

        if months <= 24:
            return (
                LeadPriority.DEVELOPING,
                f"Opens in {months} months - build relationship",
                months,
                "LATER",
            )

        return (
            LeadPriority.COLD,
            f"Opens in {months} months - too early",
            months,
            "LATER",
        )


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
        "general manager",
        "gm",
        "hotel manager",
        "resident manager",
        "executive housekeeper",
        "director of housekeeping",
        "director of purchasing",
        "purchasing manager",
        "procurement",
        "director of operations",
        "operations manager",
        "pre-opening director",
        "pre-opening manager",
        "director of rooms",
        "rooms division",
    ]

    MEDIUM_TITLES = [
        "hr director",
        "human resources",
        "director of hr",
        "food and beverage director",
        "f&b director",
        "f & b",
        "front office manager",
        "front desk manager",
        "director of finance",
        "controller",
        "cfo",
        "chief engineer",
        "director of engineering",
        "regional manager",
        "area manager",
        "regional director",
        "assistant general manager",
        "agm",
    ]

    LOW_TITLES = [
        "vp communications",
        "communications director",
        "communications manager",
        "pr manager",
        "public relations",
        "media relations",
        "marketing director",
        "marketing manager",
        "brand manager",
        "social media",
        "digital marketing",
        "investor relations",
        "svp",
        "senior vice president",
        "ceo",
        "president",
        "chairman",  # Too high level for uniform purchasing
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
        r"/login",
        r"/signin",
        r"/signup",
        r"/register",
        r"/logout",
        r"/account",
        r"/profile",
        r"/settings",
        r"/password",
        # Site infrastructure
        r"/contact",
        r"/about-us",
        r"/about$",
        r"/privacy",
        r"/terms",
        r"/cookie",
        r"/sitemap",
        r"/robots",
        r"/feed",
        r"/rss",
        r"/wp-admin",
        r"/wp-login",
        r"/admin",
        r"/_next/",
        r"/_nuxt/",
        # Media
        r"/video-gallery",
        r"/photo-gallery",
        r"/gallery",
        r"/podcast",
        r"\.pdf$",
        r"\.jpg$",
        r"\.png$",
        r"\.mp4$",
        # Social
        r"facebook\.com",
        r"twitter\.com",
        r"instagram\.com",
        r"linkedin\.com",
        r"youtube\.com",
        r"mailto:",
        r"tel:",
        r"javascript:",
        # E-commerce
        r"/cart",
        r"/checkout",
        r"/shop",
        r"/store",
        r"/subscribe",
        # Navigation
        r"/search\?",
        r"/search$",
        r"/tag/",
        r"/tags/",
        r"/author/",
        r"/page/\d+$",
        r"#",
    ]

    def __init__(self):
        self._patterns = [re.compile(p, re.IGNORECASE) for p in self.JUNK_PATTERNS]
        self._stats = {"checked": 0, "rejected": 0, "passed": 0}

    def should_reject(self, url: str) -> Tuple[bool, str]:
        """Check if URL should be rejected"""
        self._stats["checked"] += 1

        for pattern in self._patterns:
            if pattern.search(url):
                self._stats["rejected"] += 1
                return True, f"Junk pattern: {pattern.pattern}"

        self._stats["passed"] += 1
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

    FIX A: Improved prompt with few-shot examples for higher accuracy.
    Reduces false positives (executive appointments, reviews) and
    false negatives (renovation-to-new-brand conversions).
    """

    @staticmethod
    def _build_prompt(content: str) -> str:
        """Build classifier prompt with dynamic year and few-shot examples."""
        current_year = datetime.now().year
        return f"""You are a hotel industry analyst classifying web content.

TASK: Does this content announce a NEW HOTEL OPENING or MAJOR HOTEL DEVELOPMENT in {current_year} or later?

TARGET LOCATIONS (we ONLY care about these):
- Florida (Miami, Fort Lauderdale, Orlando, Tampa, Naples, etc.)
- Caribbean (Bahamas, Jamaica, Puerto Rico, Turks & Caicos, etc.)
- Other USA states (New York, California, Texas, etc.)

WHAT COUNTS AS RELEVANT:
✅ A NAMED hotel opening, groundbreaking, or construction start in {current_year}+
✅ Hotel brand conversion/renovation reopening as a new brand (e.g., Hilton → Four Seasons)
✅ New resort or hotel development announced for USA/Caribbean
✅ Hotel under construction with projected opening date
✅ Mixed-use development that includes a hotel component

WHAT IS NOT RELEVANT:
❌ Hotels that already opened ({current_year - 1} or earlier) — unless announcing a new phase
❌ International hotels (Europe, Asia, Middle East, Africa) with no US/Caribbean connection
❌ Executive appointments, promotions, or leadership changes (UNLESS tied to a new property)
❌ Hotel reviews, travel guides, or "best hotels" lists
❌ Airlines, cruises, restaurants (unless inside a new hotel)
❌ Renovations of existing hotels keeping the same brand

EXAMPLES:

Example 1: "Marriott announces new 350-room Courtyard in downtown Miami, opening Q3 {current_year}"
→ {{"is_new_hotel_opening": true, "confidence": 0.95, "reasoning": "Named hotel, specific location in Florida, future opening date"}}

Example 2: "John Smith named VP of Operations at Hilton Hotels"
→ {{"is_new_hotel_opening": false, "confidence": 0.95, "reasoning": "Executive appointment, no new hotel announced"}}

Example 3: "The historic Grand Hotel in Venice completes €50M restoration"
→ {{"is_new_hotel_opening": false, "confidence": 0.90, "reasoning": "International location (Italy), renovation not new build"}}

Example 4: "Developer breaks ground on $200M mixed-use tower in Fort Lauderdale featuring a 200-room luxury hotel"
→ {{"is_new_hotel_opening": true, "confidence": 0.90, "reasoning": "New construction in Florida, includes hotel component"}}

Example 5: "Top 10 luxury hotels opening in {current_year}"
→ {{"is_new_hotel_opening": true, "confidence": 0.70, "reasoning": "Listicle about new openings — likely contains multiple relevant hotels"}}

Now classify THIS content:
---
{content}
---

Respond in JSON:
{{
    "summary": "One sentence describing this page",
    "is_new_hotel_opening": true or false,
    "confidence": 0.0 to 1.0,
    "reasoning": "Brief explanation of your decision"
}}"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.api_key = config.gemini_api_key
        self._last_call = 0.0
        self._stats = {"classified": 0, "relevant": 0, "errors": 0}
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    async def classify(self, url: str, content: str) -> ClassificationResult:
        """Classify content relevance with retry logic and circuit breaker."""
        if not _gemini_breaker.can_call():
            logger.warning(f"Circuit breaker OPEN — skipping classify for {url}")
            return ClassificationResult(
                is_relevant=False,
                confidence=0.0,
                reason="Circuit breaker open",
                category="skipped",
            )
        start = time.time()

        # Truncate for classification (use config limit)
        truncated = (
            content[: self.config.classifier_content_limit]
            if len(content) > self.config.classifier_content_limit
            else content
        )
        logger.info(
            f"   📄 Classifying {url[:60]}... ({len(content)} chars, first 100: {content[:100].strip()!r})"
        )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Rate limiting
                elapsed = time.time() - self._last_call
                if elapsed < self.config.min_delay_seconds:
                    await asyncio.sleep(self.config.min_delay_seconds - elapsed)

                response = await self._client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self.config.classifier_model}:generateContent",
                    headers={"x-goog-api-key": self.api_key},
                    json={
                        "contents": [
                            {"parts": [{"text": self._build_prompt(truncated)}]}
                        ],
                        "generationConfig": {
                            "temperature": 0.1,
                            "maxOutputTokens": 200,
                            "responseMimeType": "application/json",
                            "thinkingConfig": {"thinkingBudget": 0},
                        },
                    },
                )
                self._last_call = time.time()

                # Retryable errors
                if response.status_code in (429, 503):
                    wait = (2**attempt) * 2
                    logger.warning(
                        f"Classifier {response.status_code}, retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status_code == 200:
                    result = response.json()
                    text = result["candidates"][0]["content"]["parts"][0]["text"]

                    # With responseMimeType, parse directly; fallback to regex
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        text = text.replace("```json", "").replace("```", "").strip()
                        # Balanced brace matching (handles nested braces in reasoning text)
                        data = None
                        brace_start = text.find(
                            "{"
                        )  # Audit Fix #4: was `start` — shadowed timing var
                        if brace_start != -1:
                            depth = 0
                            for i in range(brace_start, len(text)):
                                if text[i] == "{":
                                    depth += 1
                                elif text[i] == "}":
                                    depth -= 1
                                    if depth == 0:
                                        try:
                                            data = json.loads(text[brace_start : i + 1])
                                        except json.JSONDecodeError:
                                            pass
                                        break
                        if data is None:
                            self._stats["errors"] += 1
                            _gemini_breaker.record_success()
                            _gemini_breaker.record_success()
                            return ClassificationResult(
                                url=url,
                                summary="JSON parse failed",
                                is_relevant=False,
                                confidence=0.0,
                                reasoning="Could not parse classifier response",
                                processing_time_ms=int((time.time() - start) * 1000),
                            )

                    logger.info(f"   🤖 Gemini response for {url[:50]}: {data}")
                    is_relevant = data.get("is_new_hotel_opening", False)
                    self._stats["classified"] += 1
                    if is_relevant:
                        self._stats["relevant"] += 1

                    return ClassificationResult(
                        url=url,
                        summary=data.get("summary", "Unknown"),
                        is_relevant=is_relevant,
                        confidence=float(data.get("confidence", 0.5)),
                        reasoning=data.get("reasoning", ""),
                        processing_time_ms=int((time.time() - start) * 1000),
                    )

                # Non-retryable error
                _gemini_breaker.record_failure()
                _gemini_breaker.record_failure()
                self._stats["errors"] += 1
                return ClassificationResult(
                    url=url,
                    summary="Classification failed",
                    is_relevant=False,
                    confidence=0.0,
                    reasoning=f"API error: {response.status_code}",
                    processing_time_ms=int((time.time() - start) * 1000),
                )

            except httpx.TimeoutException:
                wait = (2**attempt) * 2
                logger.warning(
                    f"Classifier timeout, retry {attempt + 1}/{max_retries} in {wait}s"
                )
                await asyncio.sleep(wait)
                continue
            except Exception as e:
                self._stats["errors"] += 1
                return ClassificationResult(
                    url=url,
                    summary="Error",
                    is_relevant=False,
                    confidence=0.0,
                    reasoning=str(e),
                    processing_time_ms=int((time.time() - start) * 1000),
                )

        # All retries exhausted
        self._stats["errors"] += 1
        return ClassificationResult(
            url=url,
            summary="Retries exhausted",
            is_relevant=False,
            confidence=0.0,
            reasoning=f"All {max_retries} retries failed",
            processing_time_ms=int((time.time() - start) * 1000),
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

    # Source-type aware extraction hints — tells Gemini what to focus on
    # based on the kind of source being scraped
    SOURCE_HINTS = {
        "chain_newsroom": (
            "\nSOURCE TYPE: Official brand/chain press release — expect precise opening dates, "
            "exact room counts, brand standards, management details. These are authoritative; "
            "extract every detail including GM names and contact info."
        ),
        "aggregator": (
            "\nSOURCE TYPE: Hotel news aggregator — may list MULTIPLE hotels per page. "
            "Extract ALL hotels mentioned, not just the headline one. Data may be summarized; "
            "capture what's available and flag fields as approximate if needed."
        ),
        "industry": (
            "\nSOURCE TYPE: Industry/trade publication — focus on business details: investment "
            "amounts, developer/owner names, financing, construction timelines, management "
            "contracts. These sources often have insider details not in consumer press."
        ),
        "florida": (
            "\nSOURCE TYPE: Florida local news/real estate — focus on local developer names, "
            "zoning/permit details, exact street addresses, county, construction start dates, "
            "and any community impact or job creation numbers."
        ),
        "caribbean": (
            "\nSOURCE TYPE: Caribbean hospitality source — focus on all-inclusive vs. "
            "standalone resort, island location, staff hiring (seasonal workers = uniform "
            "orders), water sports/beach operations, and any renovation vs. new build details."
        ),
        "permit": (
            "\nSOURCE TYPE: Building permits/planning records — extract exact addresses, "
            "permit numbers, square footage, contractor names, estimated construction cost, "
            "and project timeline from official records."
        ),
        "business": (
            "\nSOURCE TYPE: Business journal — look for investment amounts, developer names, "
            "financing sources, construction timelines, job creation numbers, and any "
            "public incentives or tax breaks mentioned."
        ),
    }

    # URL/name patterns mapped to source types
    SOURCE_PATTERNS = {
        "chain_newsroom": [
            "marriott.com/news",
            "hilton.com/news",
            "hyatt.com/news",
            "ihg.com/news",
            "fourseasons.com/press",
            "rosewoodhotels.com/press",
            "press-release",
        ],
        "aggregator": [
            "hotelnewsresource",
            "hotelmanagement.net",
            "tophotelprojects",
            "hotel-online.com",
            "hospitalitynet",
        ],
        "industry": [
            "costar",
            "lodgingmagazine",
            "hoteldive",
            "hotelbusiness",
            "htrends",
        ],
        "florida": [
            "bizjournals.com/southflorida",
            "bizjournals.com/orlando",
            "bizjournals.com/tampa",
            "floridatrend",
            "therealdeal.com/miami",
            "southflorida",
            "sun-sentinel",
        ],
        "caribbean": [
            "caribjournal",
            "caribbeanhotelandtourism",
            "travelweekly.com/caribbean",
        ],
        "permit": ["permit", "building.co", "planning", "construction-ede"],
        "business": ["bizjournals", "commercialobserver", "therealdeal"],
    }

    @classmethod
    def _detect_source_type(cls, url: str, source_name: str = "") -> str:
        """Detect source type from URL and name patterns."""
        combined = f"{url.lower()} {(source_name or '').lower()}"
        for source_type, patterns in cls.SOURCE_PATTERNS.items():
            if any(p in combined for p in patterns):
                return source_type
        return ""

    @classmethod
    def _build_prompt(cls, content: str, source_url: str, source_name: str = "") -> str:
        """Build extraction prompt with dynamic year and source-type hints."""
        current_year = datetime.now().year

        # Detect source type and get appropriate hint
        source_type = cls._detect_source_type(source_url, source_name)
        source_hint = cls.SOURCE_HINTS.get(source_type, "")

        return f"""You are a hotel data extraction specialist.

Extract information about NEW HOTEL OPENINGS from this article.
{source_hint}

RULES:
1. Only extract NEW hotels being announced (not existing hotels)
2. Leave fields empty if not clearly stated
3. For opening_date use format "Month YYYY" or "Q1 {current_year}" or "{current_year}"
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
        self._stats = {"extracted": 0, "leads": 0, "errors": 0, "cache_hits": 0}
        self._client = httpx.AsyncClient(timeout=120.0)

        # Redis extraction cache — skip re-extraction for identical content
        self._redis = None
        self._redis_ready = False
        if config.redis_cache_enabled:
            try:
                import redis.asyncio as aioredis

                self._redis = aioredis.from_url(
                    config.redis_url,
                    socket_timeout=2,
                    socket_connect_timeout=2,
                    decode_responses=True,
                )
                self._redis_ready = True
                logger.info("Extraction cache: async Redis configured")
            except Exception as e:
                logger.warning(
                    f"Extraction cache: Redis unavailable ({e}), caching disabled"
                )
                self._redis = None

    @staticmethod
    def _content_hash(content: str, url: str) -> str:
        """Generate a stable hash for content + URL."""
        return hashlib.sha256(f"{url}:{content[:15000]}".encode()).hexdigest()[:16]

    async def _cache_get(self, cache_key: str) -> Optional[List[dict]]:
        """Try to get cached extraction results (async)."""
        if not self._redis or not self._redis_ready:
            return None
        try:
            cached = await self._redis.get(f"extract:{cache_key}")
            if cached:
                return json.loads(cached)
        except Exception:
            pass
        return None

    async def _cache_set(self, cache_key: str, leads_data: List[dict]):
        """Cache extraction results (async)."""
        if not self._redis or not self._redis_ready:
            return
        try:
            ttl = self.config.redis_cache_ttl_hours * 3600
            await self._redis.setex(
                f"extract:{cache_key}",
                ttl,
                json.dumps(leads_data),
            )
        except Exception:
            pass

    async def extract(
        self, url: str, content: str, source_name: str = ""
    ) -> List[ExtractedLead]:
        """Extract leads from content with retry logic and Redis caching."""
        self._stats["extracted"] += 1
        model = self.config.extractor_model

        # Truncate content (use config limit — Pro handles more context)
        truncated = (
            content[: self.config.extractor_content_limit]
            if len(content) > self.config.extractor_content_limit
            else content
        )

        # ── CHECK CACHE FIRST ──
        cache_key = self._content_hash(truncated, url)
        cached_data = await self._cache_get(cache_key)
        if cached_data is not None:
            self._stats["cache_hits"] += 1
            logger.debug(f"Cache hit for {url[:60]}")
            leads = []
            for hotel in cached_data:
                if hotel.get("hotel_name"):
                    lead = ExtractedLead(
                        hotel_name=hotel.get("hotel_name", ""),
                        brand=hotel.get("brand", ""),
                        property_type=hotel.get("property_type", ""),
                        city=hotel.get("city", ""),
                        state=hotel.get("state", ""),
                        country=hotel.get("country", "USA"),
                        opening_date=hotel.get("opening_date", ""),
                        opening_status=hotel.get("opening_status", ""),
                        room_count=_safe_int(hotel.get("room_count")),  # Audit Fix #5,
                        management_company=hotel.get("management_company", ""),
                        developer=hotel.get("developer", ""),
                        owner=hotel.get("owner", ""),
                        contact_name=hotel.get("contact_name", ""),
                        contact_title=hotel.get("contact_title", ""),
                        contact_email=hotel.get("contact_email", ""),
                        contact_phone=hotel.get("contact_phone", ""),
                        key_insights=hotel.get("key_insights", ""),
                        amenities=hotel.get("amenities", ""),
                        investment_amount=hotel.get("investment_amount", ""),
                        source_url=url,
                        source_name=source_name,
                        source_urls=[url],
                        source_names=[source_name] if source_name else [],
                        extracted_at=datetime.now().isoformat(),
                    )
                    leads.append(lead)
            self._stats["leads"] += len(leads)
            return leads

        # ── CALL GEMINI (with retry) ──
        # Fix 5: Retry with exponential backoff (3 attempts)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Rate limiting
                elapsed = time.time() - self._last_call
                if elapsed < self.config.min_delay_seconds:
                    await asyncio.sleep(self.config.min_delay_seconds - elapsed)

                response = await self._client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                    headers={"x-goog-api-key": self.api_key},
                    json={
                        "contents": [
                            {
                                "parts": [
                                    {
                                        "text": self._build_prompt(
                                            truncated, url, source_name
                                        )
                                    }
                                ]
                            }
                        ],
                        "generationConfig": {
                            "temperature": 0.15,
                            "maxOutputTokens": 16000,
                            "responseMimeType": "application/json",
                            "responseSchema": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "hotel_name": {"type": "string"},
                                        "brand": {"type": "string"},
                                        "property_type": {"type": "string"},
                                        "city": {"type": "string"},
                                        "state": {"type": "string"},
                                        "country": {"type": "string"},
                                        "opening_date": {"type": "string"},
                                        "room_count": {"type": "string"},
                                        "key_insights": {"type": "string"},
                                        "source_url": {"type": "string"},
                                        "investment": {"type": "string"},
                                        "developer": {"type": "string"},
                                        "management_company": {"type": "string"},
                                    },
                                    "required": ["hotel_name", "city", "state"],
                                },
                            },
                        },
                    },
                )
                self._last_call = time.time()

                # Retryable status codes
                if response.status_code == 429:
                    wait = (2**attempt) * 2  # 2s, 4s, 8s
                    logger.warning(
                        f"Rate limited (429), retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue
                elif response.status_code == 503:
                    wait = (2**attempt) * 3  # 3s, 6s, 12s
                    logger.warning(
                        f"Service unavailable (503), retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status_code == 200:
                    result = response.json()
                    text = result["candidates"][0]["content"]["parts"][0]["text"]

                    # With responseMimeType, Gemini returns clean JSON
                    # but fall back to robust extraction just in case
                    try:
                        hotels = json.loads(text)
                    except json.JSONDecodeError:
                        # Try to find JSON array with balanced bracket matching
                        # (greedy r'\[.*\]' would capture garbage between multiple arrays)
                        hotels = None
                        bracket_start = text.find(
                            "["
                        )  # Audit Fix #4: was `start` — shadowed timing var
                        if bracket_start != -1:
                            depth = 0
                            for i in range(bracket_start, len(text)):
                                if text[i] == "[":
                                    depth += 1
                                elif text[i] == "]":
                                    depth -= 1
                                    if depth == 0:
                                        try:
                                            hotels = json.loads(
                                                text[bracket_start : i + 1]
                                            )
                                        except json.JSONDecodeError:
                                            pass
                                        break
                        if hotels is None:
                            logger.warning(f"No JSON array found in response for {url}")
                            self._stats["errors"] += 1
                            return []

                    # Ensure it's a list
                    if isinstance(hotels, dict):
                        hotels = [hotels]

                    leads = []
                    for hotel in hotels:
                        if hotel.get("hotel_name"):
                            lead = ExtractedLead(
                                hotel_name=hotel.get("hotel_name", ""),
                                brand=hotel.get("brand", ""),
                                property_type=hotel.get("property_type", ""),
                                city=hotel.get("city", ""),
                                state=hotel.get("state", ""),
                                country=hotel.get("country", "USA"),
                                opening_date=hotel.get("opening_date", ""),
                                opening_status=hotel.get("opening_status", ""),
                                room_count=_safe_int(
                                    hotel.get("room_count")
                                ),  # Audit Fix #5,
                                management_company=hotel.get("management_company", ""),
                                developer=hotel.get("developer", ""),
                                owner=hotel.get("owner", ""),
                                contact_name=hotel.get("contact_name", ""),
                                contact_title=hotel.get("contact_title", ""),
                                contact_email=hotel.get("contact_email", ""),
                                contact_phone=hotel.get("contact_phone", ""),
                                key_insights=hotel.get("key_insights", ""),
                                amenities=hotel.get("amenities", ""),
                                investment_amount=hotel.get("investment_amount", ""),
                                source_url=url,
                                source_name=source_name,
                                source_urls=[url],
                                source_names=[source_name] if source_name else [],
                                extracted_at=datetime.now().isoformat(),
                            )
                            leads.append(lead)

                    self._stats["leads"] += len(leads)

                    # Cache the raw hotel dicts for future runs
                    if leads and isinstance(hotels, list):
                        await self._cache_set(cache_key, hotels)

                    return leads

                # Non-retryable error
                _gemini_breaker.record_failure()  # (400, 401, 403, etc.)
                logger.error(
                    f"Extraction failed: HTTP {response.status_code} for {url}"
                )
                self._stats["errors"] += 1
                return []

            except httpx.TimeoutException:
                wait = (2**attempt) * 2
                # Fall back to faster model on 2nd retry
                if attempt >= 1:
                    model = "gemini-2.5-flash"
                    logger.warning(
                        f"Timeout on {url}, falling back to {model}, retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                else:
                    logger.warning(
                        f"Timeout on {url}, retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                await asyncio.sleep(wait)

            except Exception as e:
                logger.error(f"Extraction error: {e}")
                self._stats["errors"] += 1
                return []

        # All retries exhausted
        logger.error(f"All {max_retries} retries exhausted for {url}")
        self._stats["errors"] += 1
        return []

    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()

    async def close(self):
        await self._client.aclose()
        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass


# =============================================================================
# STAGE 4: LEAD QUALIFIER (Uses scorer.py + Priority + Contact)
# =============================================================================


class LeadValidator:
    """
    Stage 3.5: Validate extracted leads before scoring.

    Catches garbage output from Gemini — short names, missing locations,
    past opening dates, placeholder text, etc. Runs FREE (no API calls).
    """

    # Placeholder / junk patterns that Gemini sometimes returns
    JUNK_NAMES = {
        "new hotel",
        "hotel name",
        "tbd",
        "tba",
        "unnamed",
        "n/a",
        "unknown",
        "untitled",
        "placeholder",
        "test hotel",
    }

    def __init__(self):
        self._stats = {"validated": 0, "rejected": 0, "reasons": {}}

    @staticmethod
    def _extract_year(date_str: str) -> Optional[int]:
        """Pull a 4-digit year from an opening date string."""
        if not date_str:
            return None
        match = re.search(r"20\d{2}", str(date_str))
        return int(match.group()) if match else None

    def validate(self, lead: ExtractedLead) -> Tuple[bool, str]:
        """
        Validate a single extracted lead.

        Returns: (is_valid, rejection_reason)
        """
        self._stats["validated"] += 1

        name = (lead.hotel_name or "").strip()

        # Rule 1: Name must be meaningful (> 5 chars, not placeholder)
        if len(name) < 5:
            return self._reject("name_too_short", f"Name too short: '{name}'")

        if name.lower() in self.JUNK_NAMES:
            return self._reject("junk_name", f"Placeholder name: '{name}'")

        # Rule 2: Must have city OR state (otherwise we can't score location)
        if not (lead.city or "").strip() and not (lead.state or "").strip():
            return self._reject("no_location", f"No city or state: '{name}'")

        # Rule 3: Opening date should be current year or later (not ancient)
        current_year = datetime.now().year
        opening_year = self._extract_year(lead.opening_date)
        if opening_year and opening_year < current_year - 1:
            return self._reject(
                "past_opening", f"Old opening ({opening_year}): '{name}'"
            )

        # Rule 4: Room count sanity (if provided, must be realistic)
        if lead.room_count and (lead.room_count < 3 or lead.room_count > 5000):
            return self._reject(
                "bad_room_count", f"Unrealistic rooms ({lead.room_count}): '{name}'"
            )

        # Rule 5: Name shouldn't be a duplicate of city/brand alone
        name_lower = name.lower()
        brand_lower = (lead.brand or "").lower()
        city_lower = (lead.city or "").lower()
        if name_lower == brand_lower or name_lower == city_lower:
            return self._reject(
                "name_is_brand_or_city", f"Name is just brand/city: '{name}'"
            )

        return (True, "")

    def validate_batch(self, leads: List[ExtractedLead]) -> List[ExtractedLead]:
        """Validate a list of leads, returning only valid ones."""
        valid = []
        for lead in leads:
            is_valid, reason = self.validate(lead)
            if is_valid:
                valid.append(lead)
            else:
                logger.debug(f"   ❌ Validation reject: {reason}")

        rejected = len(leads) - len(valid)
        if rejected > 0:
            logger.info(
                f"   🔍 Validation: {rejected} leads rejected, {len(valid)} passed"
            )

        return valid

    def _reject(self, code: str, message: str) -> Tuple[bool, str]:
        """Record a rejection."""
        self._stats["rejected"] += 1
        self._stats["reasons"][code] = self._stats["reasons"].get(code, 0) + 1
        return (False, message)

    def get_stats(self) -> Dict:
        return self._stats.copy()


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

        lead.qualification_score = result["total_score"]
        lead.brand_tier = result.get("brand_tier", "")
        lead.location_type = result.get("location_type", "")
        lead.opening_year = result.get("opening_year")

        if not result["should_save"]:
            lead.skip_reason = result.get("skip_reason", "")
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
            "tier1_ultra_luxury": 2.5,
            "tier2_luxury": 1.8,
            "tier3_upper_upscale": 1.3,
            "tier4_upscale": 1.0,
        }.get(lead.brand_tier, 1.2)
        uniform_cost = {
            "tier1_ultra_luxury": 500,
            "tier2_luxury": 400,
            "tier3_upper_upscale": 275,
            "tier4_upscale": 200,
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
                logger.debug(
                    f"⏭️ Skipped: {lead.hotel_name} - {qualified_lead.skip_reason}"
                )

        if skipped > 0:
            logger.info(
                f"   🚫 Filtered out {skipped} leads (budget/international/old)"
            )

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
        self.validator = LeadValidator()
        self.qualifier = LeadQualifier()

        self._stats = {"runs": 0, "pages": 0, "leads": 0}

    async def process_pages(
        self, pages: List[Dict[str, str]], source_name: str = ""
    ) -> PipelineResult:
        """
        Process pages through the full pipeline.

        FIX: Now uses parallel processing with semaphore for
        classification and extraction stages (~5x faster).

        Args:
            pages: List of {'url': ..., 'content': ..., 'source': ...}
            source_name: Name of the source

        Returns:
            PipelineResult with all leads and stats
        """
        start_time = time.time()
        self._stats["runs"] += 1

        # Concurrency limit from config (default 10)
        sem = asyncio.Semaphore(self.config.max_concurrent_requests)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"🧠 INTELLIGENT PIPELINE - {len(pages)} pages")
        logger.info(f"{'=' * 60}")

        # =====================================================================
        # STAGE 1: QUICK REJECT (FREE - no AI cost)
        # =====================================================================
        logger.info(f"\n🚫 STAGE 1: Quick reject filtering {len(pages)} URLs...")

        pages_after_reject = []
        rejected_count = 0

        for page in pages:
            url = page.get("url", "")

            should_reject, reason = self.quick_reject.should_reject(url)
            if should_reject:
                rejected_count += 1
                logger.debug(f"   ❌ Rejected: {url[:60]}... ({reason})")
            else:
                pages_after_reject.append(page)

        if rejected_count > 0:
            logger.info(
                f"   🚫 Rejected {rejected_count} junk URLs (saved {rejected_count} API calls)"
            )
        logger.info(f"   ✅ {len(pages_after_reject)} pages passed to classification")

        # =====================================================================
        # STAGE 2: CLASSIFICATION (PARALLEL)
        # =====================================================================
        logger.info(
            f"\n📊 STAGE 2: Classifying {len(pages_after_reject)} pages (parallel)..."
        )
        classification_start = time.time()

        async def classify_one(page):
            async with sem:
                url = page.get("url", "")
                content = page.get("content", "") or page.get("text", "")
                if not content or len(content) < 100:
                    return None
                result = await self.classifier.classify(url, content)
                return (page, result)

        classify_tasks = [classify_one(p) for p in pages_after_reject]
        classify_results = await asyncio.gather(*classify_tasks, return_exceptions=True)

        relevant_pages = []
        not_relevant = 0
        classification_confidences = []

        for result in classify_results:
            if result is None or isinstance(result, Exception):
                continue
            page, classification = result
            classification_confidences.append(classification.confidence)
            if classification.should_extract:
                relevant_pages.append(
                    {
                        "url": page.get("url", ""),
                        "content": page.get("content", "") or page.get("text", ""),
                        "source": page.get("source", source_name),
                    }
                )
                logger.info(f"✅ RELEVANT: {classification.summary[:60]}...")
            else:
                not_relevant += 1

        classification_time = int((time.time() - classification_start) * 1000)
        logger.info(f"✅ {len(relevant_pages)} relevant, {not_relevant} skipped")

        # =====================================================================
        # STAGE 3: EXTRACTION (PARALLEL)
        # =====================================================================
        logger.info(
            f"\n🔍 STAGE 3: Extracting from {len(relevant_pages)} pages (parallel)..."
        )
        extraction_start = time.time()

        async def extract_one(page):
            async with sem:
                return await self.extractor.extract(
                    page["url"], page["content"], page.get("source", source_name)
                )

        extract_tasks = [extract_one(p) for p in relevant_pages]
        extract_results = await asyncio.gather(*extract_tasks, return_exceptions=True)

        all_leads = []
        for result in extract_results:
            if isinstance(result, Exception):
                logger.error(f"Extraction task failed: {result}")
                continue
            if result:
                all_leads.extend(result)
                for lead in result:
                    logger.info(f"   📝 {lead.hotel_name} ({lead.city}, {lead.state})")

        extraction_time = int((time.time() - extraction_start) * 1000)
        logger.info(f"✅ Extracted {len(all_leads)} leads")

        # =====================================================================
        # STAGE 3.5: VALIDATION (FREE - no AI cost)
        # =====================================================================
        logger.info(f"\n🔍 STAGE 3.5: Validating {len(all_leads)} leads...")

        validated_leads = self.validator.validate_batch(all_leads)

        logger.info(f"✅ {len(validated_leads)} leads passed validation")

        # =====================================================================
        # STAGE 4: QUALIFICATION
        # =====================================================================
        logger.info(f"\n⭐ STAGE 4: Qualifying {len(validated_leads)} leads...")

        qualified_leads = self.qualifier.qualify_batch(validated_leads)

        # Filter by threshold
        final_leads = [
            lead
            for lead in qualified_leads
            if lead.qualification_score >= self.config.qualification_threshold
        ]

        # Categorize
        high_quality = len(
            [lead for lead in final_leads if lead.qualification_score >= 70]
        )
        medium_quality = len(
            [lead for lead in final_leads if 40 <= lead.qualification_score < 70]
        )
        low_quality = len(
            [lead for lead in final_leads if lead.qualification_score < 40]
        )

        # Count by priority
        hot_leads = len([lead for lead in final_leads if "HOT" in lead.lead_priority])
        warm_leads = len([lead for lead in final_leads if "WARM" in lead.lead_priority])

        total_time = time.time() - start_time

        # Quality metrics
        avg_confidence = sum(classification_confidences) / max(
            len(classification_confidences), 1
        )
        avg_score = sum(lead.qualification_score for lead in final_leads) / max(
            len(final_leads), 1
        )
        cache_hits = self.extractor._stats.get("cache_hits", 0)
        validation_rejects = len(all_leads) - len(validated_leads)
        source_type = LeadExtractor._detect_source_type(
            pages[0].get("url", "") if pages else "", source_name
        )

        # Summary
        logger.info(f"\n{'=' * 60}")
        logger.info("🎯 PIPELINE COMPLETE")
        logger.info(f"{'=' * 60}")
        logger.info(f"🚫 Quick Reject: {rejected_count} junk URLs filtered (FREE)")
        logger.info(
            f"📊 Classified: {len(relevant_pages)} relevant / {len(pages_after_reject)} checked (avg confidence: {avg_confidence:.2f})"
        )
        logger.info(f"📝 Extracted: {len(all_leads)} leads (cache hits: {cache_hits})")
        logger.info(
            f"🔍 Validated: {len(validated_leads)} passed, {validation_rejects} rejected"
        )
        logger.info(
            f"✅ Qualified: {len(final_leads)} leads (score >= {self.config.qualification_threshold}, avg: {avg_score:.0f})"
        )
        logger.info(f"   🔴 High (70+): {high_quality}")
        logger.info(f"   🟠 Medium (40-69): {medium_quality}")
        logger.info(f"   🔵 Low (<40): {low_quality}")
        logger.info(f"⏰ Priority: 🔴 {hot_leads} HOT | 🟠 {warm_leads} WARM")
        logger.info(f"⏱️ Time: {total_time:.1f}s")

        self._stats["pages"] += len(pages)
        self._stats["leads"] += len(final_leads)

        pipeline_result = PipelineResult(
            source_name=source_name,
            pages_scraped=len(pages),
            pages_classified=len(pages_after_reject),
            pages_relevant=len(relevant_pages),
            pages_not_relevant=not_relevant,
            pages_rejected=rejected_count,
            leads_extracted=len(all_leads),
            leads_validated=len(validated_leads),
            leads_qualified=len(final_leads),
            leads_high_quality=high_quality,
            leads_medium_quality=medium_quality,
            leads_low_quality=low_quality,
            final_leads=final_leads,
            relevant_urls=[p["url"] for p in relevant_pages],
            total_time_seconds=total_time,
            classification_time_ms=classification_time,
            extraction_time_ms=extraction_time,
            avg_classification_confidence=avg_confidence,
            avg_lead_score=avg_score,
            cache_hits=cache_hits,
            validation_rejects=validation_rejects,
            source_type_detected=source_type,
        )

        # Log structured metrics for monitoring
        logger.info(f"📊 Metrics: {json.dumps(pipeline_result.to_metrics_dict())}")

        return pipeline_result

    async def extract(
        self, content: str, source_url: str = "", source_name: str = ""
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
            "pipeline": self._stats,
            "quick_reject": self.quick_reject.get_stats(),
            "classifier": self.classifier.get_stats(),
            "extractor": self.extractor.get_stats(),
        }

    async def close(self):
        """Close shared HTTP clients."""
        await self.classifier.close()
        await self.extractor.close()


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
        name = re.sub(r"[^\w\s]", "", name)
        stopwords = ["the", "hotel", "resort", "spa", "and", "at", "by"]
        words = [w for w in name.split() if w not in stopwords]
        return " ".join(sorted(words))

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
        print("✅ Gemini API key found")
    else:
        print("❌ No GEMINI_API_KEY in environment")
        return

    pipeline = IntelligentPipeline(config)

    # Test content

    test_content = (
        "Ritz-Carlton Announces New Luxury Hotel in Miami Beach\n\n"
        "The Ritz-Carlton Hotel Company announced plans for a new 200-room luxury "
        "resort in Miami Beach, Florida, expected to open in Q2 2026.\n\n"
        "The property will feature a full-service spa, three restaurants, and "
        "15,000 square feet of meeting space. The hotel will hire approximately "
        "400 staff members.\n\n"
        '"We\'re excited to expand our presence in South Florida," said John Smith, '
        "General Manager."
    )

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
