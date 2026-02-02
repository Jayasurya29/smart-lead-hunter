"""
SMART LEAD HUNTER - SOURCE INTELLIGENCE SERVICE
================================================
Self-healing, self-learning URL management system.

BUILDS ON TOP OF:
- source_learning.py (pattern learning)
- intelligent_pipeline.py (content classification)

ADDS:
- Multi-entry point URLs with fallback
- Automatic URL discovery when sources fail
- Health monitoring and self-healing
- Smart link scoring for finding content

Author: Smart Lead Hunter
Version: 1.0
"""

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path
from urllib.parse import urlparse, urljoin
from enum import Enum

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# =============================================================================
# HEALTH STATUS
# =============================================================================

class HealthStatus(str, Enum):
    """Source health status levels"""
    HEALTHY = "healthy"       # Working fine, finding leads
    DEGRADED = "degraded"     # Some issues, needs attention
    FAILING = "failing"       # Serious problems, trying to heal
    DEAD = "dead"             # All recovery attempts failed
    NEW = "new"               # Just added, no history yet


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class EntryPoint:
    """A single entry point URL for a source"""
    url: str
    priority: int = 1                    # 1 = primary, 2 = fallback, 3 = discovered
    last_success: Optional[str] = None   # ISO timestamp
    last_failure: Optional[str] = None   # ISO timestamp
    consecutive_failures: int = 0
    total_successes: int = 0
    total_failures: int = 0
    leads_found: int = 0
    discovery_method: str = "manual"     # manual, crawl, google, backlink
    notes: str = ""
    
    @property
    def success_rate(self) -> float:
        total = self.total_successes + self.total_failures
        return self.total_successes / total if total > 0 else 0.0
    
    @property
    def is_healthy(self) -> bool:
        return self.consecutive_failures < 3


@dataclass
class SourceIntelligence:
    """Intelligence data for a single source"""
    name: str
    domain: str
    
    # Entry points (multiple URLs per source)
    entry_points: List[EntryPoint] = field(default_factory=list)
    
    # Health tracking
    health_status: HealthStatus = HealthStatus.NEW
    last_health_check: Optional[str] = None
    consecutive_empty_scrapes: int = 0      # Scrapes with 0 leads
    days_since_last_lead: int = 0
    
    # Learned patterns (from source_learning.py)
    gold_url_patterns: List[str] = field(default_factory=list)
    junk_url_patterns: List[str] = field(default_factory=list)
    
    # Link discovery signals
    content_keywords: List[str] = field(default_factory=lambda: [
        'opening', 'opens', 'debut', 'announce', 'new hotel',
        'coming soon', 'now open', 'grand opening', 'development'
    ])
    
    # Recovery history
    healing_attempts: int = 0
    last_healing_attempt: Optional[str] = None
    healing_success: bool = False
    
    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    @property
    def primary_url(self) -> Optional[str]:
        """Get the best URL to try first"""
        healthy_entries = [e for e in self.entry_points if e.is_healthy]
        if healthy_entries:
            # Sort by priority, then by success rate
            healthy_entries.sort(key=lambda e: (e.priority, -e.success_rate))
            return healthy_entries[0].url
        # All entries unhealthy - return the one with fewest failures
        if self.entry_points:
            return min(self.entry_points, key=lambda e: e.consecutive_failures).url
        return None


# =============================================================================
# LINK SCORER - Smart URL Discovery
# =============================================================================

class LinkScorer:
    """
    Scores links on a page to find where content likely lives.
    
    Used when primary URL fails - crawls homepage to find new content location.
    """
    
    # URL patterns that signal relevant content
    URL_SIGNALS = {
        # High signal - very likely to have hotel openings
        'news': 4, 'press': 4, 'media': 3, 'releases': 4,
        'opening': 6, 'openings': 6, 'new-hotel': 6, 'new-hotels': 6,
        'announcement': 5, 'announcements': 5,
        'development': 4, 'developments': 4, 'pipeline': 4,
        'coming-soon': 5, 'now-open': 6,
        
        # Medium signal - might have content
        'stories': 2, 'articles': 2, 'blog': 1,
        'properties': 2, 'hotels': 2, 'resorts': 2,
        'brands': 2, 'portfolio': 2,
        
        # Low signal - category pages
        'category': 1, 'archive': 1, 'tag': 1,
    }
    
    # Link text patterns
    TEXT_SIGNALS = {
        'new hotel': 6, 'now open': 6, 'coming soon': 5,
        'opening': 5, 'grand opening': 6, 'debut': 5,
        'announce': 4, 'announcement': 4,
        'press release': 4, 'news release': 4,
        'development': 3, 'pipeline': 3,
        'latest': 2, 'recent': 2, 'new': 2,
    }
    
    # Negative signals - URLs to avoid
    NEGATIVE_SIGNALS = [
        'login', 'signin', 'signup', 'register', 'account', 'profile',
        'cart', 'checkout', 'shop', 'store', 'booking', 'reservation',
        'careers', 'jobs', 'employment', 'work-with-us',
        'contact', 'about-us', 'about', 'privacy', 'terms', 'legal',
        'sitemap', 'search', 'subscribe', 'newsletter',
        'facebook', 'twitter', 'instagram', 'linkedin', 'youtube',
        '.pdf', '.jpg', '.png', '.mp4', 'mailto:', 'tel:',
    ]
    
    def score_link(self, url: str, link_text: str, base_domain: str) -> int:
        """
        Score a link's likelihood of containing hotel opening content.
        
        Args:
            url: The link URL
            link_text: The anchor text
            base_domain: The source's domain (to filter external links)
        
        Returns:
            Score (higher = more likely to have content)
        """
        score = 0
        url_lower = url.lower()
        text_lower = (link_text or '').lower()
        
        # Check if external link (skip)
        try:
            link_domain = urlparse(url).netloc.replace('www.', '')
            if base_domain not in link_domain and link_domain not in base_domain:
                return -100  # External link
        except:
            pass
        
        # Check negative signals first
        for neg in self.NEGATIVE_SIGNALS:
            if neg in url_lower:
                return -50
        
        # Score URL patterns
        for pattern, weight in self.URL_SIGNALS.items():
            if pattern in url_lower:
                score += weight
        
        # Score link text
        for pattern, weight in self.TEXT_SIGNALS.items():
            if pattern in text_lower:
                score += weight
        
        # Bonus for short, focused URLs (likely main sections)
        path = urlparse(url).path
        if path.count('/') <= 2:
            score += 2
        
        return score
    
    def find_content_links(
        self, 
        html: str, 
        base_url: str, 
        min_score: int = 4,
        max_links: int = 10
    ) -> List[Tuple[str, int]]:
        """
        Find the most promising links on a page.
        
        Args:
            html: Page HTML content
            base_url: The page URL (for resolving relative links)
            min_score: Minimum score to include
            max_links: Maximum links to return
        
        Returns:
            List of (url, score) tuples, sorted by score descending
        """
        soup = BeautifulSoup(html, 'html.parser')
        base_domain = urlparse(base_url).netloc.replace('www.', '')
        
        scored_links = []
        seen_urls = set()
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if not href or href.startswith('#'):
                continue
            
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            
            # Skip duplicates
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)
            
            # Get link text
            link_text = a_tag.get_text(strip=True)
            
            # Score the link
            score = self.score_link(full_url, link_text, base_domain)
            
            if score >= min_score:
                scored_links.append((full_url, score))
        
        # Sort by score descending
        scored_links.sort(key=lambda x: x[1], reverse=True)
        
        return scored_links[:max_links]


# =============================================================================
# HEALTH MONITOR
# =============================================================================

class HealthMonitor:
    """
    Monitors source health and determines when healing is needed.
    """
    
    # Thresholds
    DEGRADED_EMPTY_SCRAPES = 3      # Empty scrapes before degraded
    FAILING_EMPTY_SCRAPES = 7       # Empty scrapes before failing
    DEAD_EMPTY_SCRAPES = 14         # Empty scrapes before dead
    
    DEGRADED_DAYS_NO_LEAD = 7       # Days without lead before degraded
    FAILING_DAYS_NO_LEAD = 14       # Days without lead before failing
    DEAD_DAYS_NO_LEAD = 30          # Days without lead before dead
    
    MAX_HEALING_ATTEMPTS = 5        # Max healing attempts before giving up
    
    def evaluate_health(self, source: SourceIntelligence) -> HealthStatus:
        """
        Evaluate current health status of a source.
        
        Returns:
            HealthStatus enum value
        """
        # Check if new
        if not source.entry_points or all(e.total_successes == 0 for e in source.entry_points):
            return HealthStatus.NEW
        
        # Check entry point health
        healthy_entries = sum(1 for e in source.entry_points if e.is_healthy)
        total_entries = len(source.entry_points)
        
        # All entry points failing = serious problem
        if healthy_entries == 0:
            return HealthStatus.DEAD if source.healing_attempts >= self.MAX_HEALING_ATTEMPTS else HealthStatus.FAILING
        
        # Check consecutive empty scrapes
        if source.consecutive_empty_scrapes >= self.DEAD_EMPTY_SCRAPES:
            return HealthStatus.DEAD
        if source.consecutive_empty_scrapes >= self.FAILING_EMPTY_SCRAPES:
            return HealthStatus.FAILING
        if source.consecutive_empty_scrapes >= self.DEGRADED_EMPTY_SCRAPES:
            return HealthStatus.DEGRADED
        
        # Check days since last lead
        if source.days_since_last_lead >= self.DEAD_DAYS_NO_LEAD:
            return HealthStatus.DEAD
        if source.days_since_last_lead >= self.FAILING_DAYS_NO_LEAD:
            return HealthStatus.FAILING
        if source.days_since_last_lead >= self.DEGRADED_DAYS_NO_LEAD:
            return HealthStatus.DEGRADED
        
        # Check entry point ratio
        if healthy_entries < total_entries * 0.5:
            return HealthStatus.DEGRADED
        
        return HealthStatus.HEALTHY
    
    def needs_healing(self, source: SourceIntelligence) -> bool:
        """Check if source needs healing intervention"""
        return source.health_status in [HealthStatus.FAILING, HealthStatus.DEAD]
    
    def needs_attention(self, source: SourceIntelligence) -> bool:
        """Check if source needs human attention"""
        return (
            source.health_status == HealthStatus.DEAD or
            source.healing_attempts >= self.MAX_HEALING_ATTEMPTS
        )


# =============================================================================
# SOURCE INTELLIGENCE SERVICE
# =============================================================================

class SourceIntelligenceService:
    """
    The main intelligence service that orchestrates smart URL management.
    
    Features:
    - Multi-entry point URL management
    - Automatic fallback when URLs fail
    - Smart link discovery
    - Health monitoring
    - Self-healing capabilities
    
    Usage:
        intel = SourceIntelligenceService()
        
        # Get best URL to scrape
        url = intel.get_best_url("Four Seasons Press")
        
        # Record result
        intel.record_scrape_result("Four Seasons Press", url, success=True, leads_found=3)
        
        # Check if healing needed
        if intel.needs_healing("Four Seasons Press"):
            await intel.heal_source("Four Seasons Press")
    """
    
    def __init__(self, data_dir: str = "data/intelligence"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.data_file = self.data_dir / "source_intelligence.json"
        
        # Components
        self.link_scorer = LinkScorer()
        self.health_monitor = HealthMonitor()
        
        # In-memory data
        self.sources: Dict[str, SourceIntelligence] = {}
        
        # HTTP client for discovery
        self._client: Optional[httpx.AsyncClient] = None
        
        # Load existing data
        self._load()
    
    def _load(self):
        """Load existing intelligence data"""
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    for name, source_data in data.items():
                        # Convert entry points
                        entry_points = [
                            EntryPoint(**ep) for ep in source_data.pop('entry_points', [])
                        ]
                        # Convert health status
                        source_data['health_status'] = HealthStatus(
                            source_data.get('health_status', 'new')
                        )
                        source_data['entry_points'] = entry_points
                        self.sources[name] = SourceIntelligence(**source_data)
                logger.info(f"✅ Loaded intelligence for {len(self.sources)} sources")
            except Exception as e:
                logger.warning(f"Could not load intelligence data: {e}")
    
    def save(self):
        """Save intelligence data to disk"""
        try:
            data = {}
            for name, source in self.sources.items():
                source_dict = asdict(source)
                source_dict['health_status'] = source.health_status.value
                data[name] = source_dict
            
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.debug(f"Saved intelligence for {len(self.sources)} sources")
        except Exception as e:
            logger.error(f"Could not save intelligence data: {e}")
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                follow_redirects=True,
                headers={
                    'User-Agent': 'SmartLeadHunter/1.0 (Hotel Research; contact@company.com)'
                }
            )
        return self._client
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    # =========================================================================
    # SOURCE MANAGEMENT
    # =========================================================================
    
    def add_source(
        self, 
        name: str, 
        primary_url: str,
        fallback_urls: List[str] = None,
        domain: str = None
    ) -> SourceIntelligence:
        """
        Add a new source with entry points.
        
        Args:
            name: Source name (e.g., "Four Seasons Press")
            primary_url: Main URL to scrape
            fallback_urls: Backup URLs if primary fails
            domain: Domain (auto-detected if not provided)
        
        Returns:
            SourceIntelligence object
        """
        if domain is None:
            domain = urlparse(primary_url).netloc.replace('www.', '')
        
        entry_points = [
            EntryPoint(url=primary_url, priority=1, discovery_method="manual")
        ]
        
        if fallback_urls:
            for i, url in enumerate(fallback_urls):
                entry_points.append(
                    EntryPoint(url=url, priority=i+2, discovery_method="manual")
                )
        
        source = SourceIntelligence(
            name=name,
            domain=domain,
            entry_points=entry_points
        )
        
        self.sources[name] = source
        self.save()
        
        logger.info(f"✅ Added source: {name} with {len(entry_points)} entry points")
        return source
    
    def get_source(self, name: str) -> Optional[SourceIntelligence]:
        """Get intelligence for a source"""
        return self.sources.get(name)
    
    def add_entry_point(
        self,
        source_name: str,
        url: str,
        priority: int = 2,
        discovery_method: str = "discovered"
    ):
        """Add a new entry point to an existing source"""
        source = self.sources.get(source_name)
        if not source:
            logger.warning(f"Source not found: {source_name}")
            return
        
        # Check if URL already exists
        existing_urls = {e.url for e in source.entry_points}
        if url in existing_urls:
            logger.debug(f"URL already exists for {source_name}: {url}")
            return
        
        entry = EntryPoint(
            url=url,
            priority=priority,
            discovery_method=discovery_method
        )
        source.entry_points.append(entry)
        source.updated_at = datetime.now().isoformat()
        
        self.save()
        logger.info(f"✅ Added entry point for {source_name}: {url}")
    
    # =========================================================================
    # URL SELECTION
    # =========================================================================
    
    def get_best_url(self, source_name: str) -> Optional[str]:
        """
        Get the best URL to scrape for a source.
        
        Prioritizes:
        1. Healthy entry points
        2. Higher priority (lower number)
        3. Higher success rate
        
        Returns:
            Best URL to try, or None if source not found
        """
        source = self.sources.get(source_name)
        if not source:
            return None
        return source.primary_url
    
    def get_all_urls(self, source_name: str) -> List[str]:
        """
        Get all entry point URLs for a source, sorted by priority.
        
        Returns:
            List of URLs in priority order
        """
        source = self.sources.get(source_name)
        if not source:
            return []
        
        # Sort by priority, then by success rate
        sorted_entries = sorted(
            source.entry_points,
            key=lambda e: (e.priority, -e.success_rate, e.consecutive_failures)
        )
        return [e.url for e in sorted_entries]
    
    def get_fallback_url(self, source_name: str, failed_url: str) -> Optional[str]:
        """
        Get next URL to try after one fails.
        
        Args:
            source_name: Name of the source
            failed_url: URL that just failed
        
        Returns:
            Next URL to try, or None if no more options
        """
        all_urls = self.get_all_urls(source_name)
        
        try:
            current_index = all_urls.index(failed_url)
            if current_index + 1 < len(all_urls):
                return all_urls[current_index + 1]
        except ValueError:
            pass
        
        return None
    
    # =========================================================================
    # RESULT RECORDING
    # =========================================================================
    
    def record_scrape_result(
        self,
        source_name: str,
        url: str,
        success: bool,
        leads_found: int = 0,
        error_message: str = None
    ):
        """
        Record the result of a scrape attempt.
        
        Args:
            source_name: Name of the source
            url: URL that was scraped
            success: Whether the scrape succeeded (HTTP 200 + valid content)
            leads_found: Number of leads extracted
            error_message: Error message if failed
        """
        source = self.sources.get(source_name)
        if not source:
            logger.warning(f"Source not found: {source_name}")
            return
        
        # Find the entry point
        entry = next((e for e in source.entry_points if e.url == url), None)
        if not entry:
            # URL not in entry points - add it
            entry = EntryPoint(url=url, priority=3, discovery_method="scraped")
            source.entry_points.append(entry)
        
        now = datetime.now().isoformat()
        
        if success:
            entry.last_success = now
            entry.total_successes += 1
            entry.consecutive_failures = 0
            entry.leads_found += leads_found
            
            if leads_found > 0:
                source.consecutive_empty_scrapes = 0
                source.days_since_last_lead = 0
            else:
                source.consecutive_empty_scrapes += 1
        else:
            entry.last_failure = now
            entry.total_failures += 1
            entry.consecutive_failures += 1
            entry.notes = error_message or ""
            source.consecutive_empty_scrapes += 1
        
        # Update health status
        source.health_status = self.health_monitor.evaluate_health(source)
        source.last_health_check = now
        source.updated_at = now
        
        self.save()
        
        status_emoji = "✅" if success else "❌"
        logger.info(f"{status_emoji} Recorded result for {source_name}: success={success}, leads={leads_found}")
    
    # =========================================================================
    # SMART DISCOVERY
    # =========================================================================
    
    async def discover_content_urls(
        self,
        source_name: str,
        base_url: str = None,
        max_links: int = 5
    ) -> List[str]:
        """
        Discover URLs that might contain hotel opening content.
        
        Crawls the base URL (or homepage) and scores links to find
        the most promising pages.
        
        Args:
            source_name: Name of the source
            base_url: URL to crawl (defaults to source's domain homepage)
            max_links: Maximum links to return
        
        Returns:
            List of discovered URLs
        """
        source = self.sources.get(source_name)
        if not source:
            logger.warning(f"Source not found: {source_name}")
            return []
        
        # Use provided URL or construct homepage
        if base_url is None:
            base_url = f"https://{source.domain}/"
        
        try:
            client = await self._get_client()
            response = await client.get(base_url)
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {base_url}: {response.status_code}")
                return []
            
            # Find promising links
            scored_links = self.link_scorer.find_content_links(
                response.text,
                base_url,
                min_score=4,
                max_links=max_links
            )
            
            discovered_urls = [url for url, score in scored_links]
            
            logger.info(f"🔍 Discovered {len(discovered_urls)} potential URLs for {source_name}")
            for url, score in scored_links:
                logger.debug(f"   Score {score}: {url}")
            
            return discovered_urls
            
        except Exception as e:
            logger.error(f"Error discovering URLs for {source_name}: {e}")
            return []
    
    # =========================================================================
    # SELF-HEALING
    # =========================================================================
    
    def needs_healing(self, source_name: str) -> bool:
        """Check if a source needs healing"""
        source = self.sources.get(source_name)
        if not source:
            return False
        return self.health_monitor.needs_healing(source)
    
    def needs_attention(self, source_name: str) -> bool:
        """Check if a source needs human attention"""
        source = self.sources.get(source_name)
        if not source:
            return False
        return self.health_monitor.needs_attention(source)
    
    async def heal_source(self, source_name: str) -> bool:
        """
        Attempt to heal a failing source.
        
        Healing steps:
        1. Try all existing entry points
        2. Discover new URLs from homepage
        3. Test discovered URLs
        4. Add working URLs as new entry points
        
        Args:
            source_name: Name of the source to heal
        
        Returns:
            True if healing succeeded, False otherwise
        """
        source = self.sources.get(source_name)
        if not source:
            logger.warning(f"Source not found: {source_name}")
            return False
        
        logger.info(f"🏥 Starting healing for {source_name}...")
        source.healing_attempts += 1
        source.last_healing_attempt = datetime.now().isoformat()
        
        client = await self._get_client()
        
        # Step 1: Test all existing entry points
        logger.info("   Step 1: Testing existing entry points...")
        for entry in source.entry_points:
            try:
                response = await client.get(entry.url)
                if response.status_code == 200 and len(response.text) > 1000:
                    logger.info(f"   ✅ Working: {entry.url}")
                    entry.consecutive_failures = 0
                    source.healing_success = True
                    self.save()
                    return True
                else:
                    logger.debug(f"   ❌ Failed: {entry.url} ({response.status_code})")
            except Exception as e:
                logger.debug(f"   ❌ Error: {entry.url} ({e})")
        
        # Step 2: Discover new URLs
        logger.info("   Step 2: Discovering new URLs...")
        discovered = await self.discover_content_urls(source_name, max_links=10)
        
        # Step 3: Test discovered URLs
        logger.info(f"   Step 3: Testing {len(discovered)} discovered URLs...")
        for url in discovered:
            try:
                response = await client.get(url)
                if response.status_code == 200 and len(response.text) > 1000:
                    # Check if it has relevant content
                    text_lower = response.text.lower()
                    has_content = any(kw in text_lower for kw in source.content_keywords)
                    
                    if has_content:
                        logger.info(f"   ✅ Found working URL: {url}")
                        self.add_entry_point(source_name, url, priority=2, discovery_method="healed")
                        source.healing_success = True
                        self.save()
                        return True
            except Exception as e:
                logger.debug(f"   ❌ Error testing {url}: {e}")
        
        # Step 4: Try Google search (if implemented)
        # TODO: Add Google search discovery
        
        # Healing failed
        logger.warning(f"   ❌ Healing failed for {source_name}")
        source.healing_success = False
        
        # Update health status
        source.health_status = self.health_monitor.evaluate_health(source)
        self.save()
        
        return False
    
    # =========================================================================
    # SMART SCRAPING WRAPPER
    # =========================================================================
    
    async def scrape_with_fallback(
        self,
        source_name: str,
        scrape_func,  # async function(url) -> (success, leads, error)
        max_attempts: int = 3
    ) -> Tuple[bool, List, str]:
        """
        Scrape a source with automatic fallback and healing.
        
        Args:
            source_name: Name of the source
            scrape_func: Async function that takes URL and returns (success, leads, error)
            max_attempts: Maximum URLs to try
        
        Returns:
            (success, leads, last_error)
        """
        source = self.sources.get(source_name)
        if not source:
            return False, [], f"Source not found: {source_name}"
        
        urls = self.get_all_urls(source_name)
        last_error = ""
        
        for i, url in enumerate(urls[:max_attempts]):
            logger.info(f"📥 Trying URL {i+1}/{min(len(urls), max_attempts)}: {url}")
            
            try:
                success, leads, error = await scrape_func(url)
                
                # Record result
                self.record_scrape_result(
                    source_name,
                    url,
                    success=success,
                    leads_found=len(leads) if leads else 0,
                    error_message=error
                )
                
                if success and leads:
                    return True, leads, ""
                
                last_error = error or "No leads found"
                
            except Exception as e:
                last_error = str(e)
                self.record_scrape_result(
                    source_name,
                    url,
                    success=False,
                    error_message=last_error
                )
        
        # All URLs failed - try healing if not done recently
        if self.needs_healing(source_name):
            logger.info(f"🏥 All URLs failed, attempting to heal {source_name}...")
            healed = await self.heal_source(source_name)
            
            if healed:
                # Try again with new URLs
                new_url = self.get_best_url(source_name)
                if new_url and new_url not in urls[:max_attempts]:
                    success, leads, error = await scrape_func(new_url)
                    self.record_scrape_result(
                        source_name,
                        new_url,
                        success=success,
                        leads_found=len(leads) if leads else 0
                    )
                    if success:
                        return True, leads, ""
        
        return False, [], last_error
    
    # =========================================================================
    # REPORTING
    # =========================================================================
    
    def get_health_report(self) -> str:
        """Generate a health report for all sources"""
        lines = []
        lines.append("=" * 70)
        lines.append("🏥 SOURCE HEALTH REPORT")
        lines.append("=" * 70)
        
        # Group by health status
        by_status = {}
        for name, source in self.sources.items():
            status = source.health_status
            if status not in by_status:
                by_status[status] = []
            by_status[status].append((name, source))
        
        # Print summary
        lines.append("\n📊 SUMMARY:")
        for status in HealthStatus:
            count = len(by_status.get(status, []))
            emoji = {"healthy": "🟢", "degraded": "🟡", "failing": "🟠", "dead": "🔴", "new": "⚪"}
            lines.append(f"   {emoji.get(status.value, '⚪')} {status.value.upper()}: {count}")
        
        # Print details for non-healthy sources
        for status in [HealthStatus.DEAD, HealthStatus.FAILING, HealthStatus.DEGRADED]:
            sources = by_status.get(status, [])
            if sources:
                lines.append(f"\n{status.value.upper()} SOURCES:")
                for name, source in sources:
                    lines.append(f"   • {name}")
                    lines.append(f"     Domain: {source.domain}")
                    lines.append(f"     Empty scrapes: {source.consecutive_empty_scrapes}")
                    lines.append(f"     Days since lead: {source.days_since_last_lead}")
                    lines.append(f"     Healing attempts: {source.healing_attempts}")
                    if source.entry_points:
                        healthy = sum(1 for e in source.entry_points if e.is_healthy)
                        lines.append(f"     Entry points: {healthy}/{len(source.entry_points)} healthy")
        
        lines.append("\n" + "=" * 70)
        return "\n".join(lines)
    
    def get_source_report(self, source_name: str) -> str:
        """Generate detailed report for a single source"""
        source = self.sources.get(source_name)
        if not source:
            return f"❌ Source not found: {source_name}"
        
        lines = []
        lines.append("=" * 60)
        lines.append(f"📊 SOURCE REPORT: {source_name}")
        lines.append("=" * 60)
        
        status_emoji = {
            "healthy": "🟢", "degraded": "🟡", 
            "failing": "🟠", "dead": "🔴", "new": "⚪"
        }
        
        lines.append(f"\n🌐 Domain: {source.domain}")
        lines.append(f"🏥 Health: {status_emoji.get(source.health_status.value, '⚪')} {source.health_status.value.upper()}")
        lines.append(f"📅 Last check: {source.last_health_check or 'Never'}")
        
        lines.append(f"\n📈 METRICS:")
        lines.append(f"   Empty scrapes: {source.consecutive_empty_scrapes}")
        lines.append(f"   Days since lead: {source.days_since_last_lead}")
        lines.append(f"   Healing attempts: {source.healing_attempts}")
        
        lines.append(f"\n🔗 ENTRY POINTS ({len(source.entry_points)}):")
        for entry in sorted(source.entry_points, key=lambda e: e.priority):
            health = "✅" if entry.is_healthy else "❌"
            lines.append(f"   {health} [P{entry.priority}] {entry.url}")
            lines.append(f"      Success rate: {entry.success_rate:.0%} ({entry.total_successes}/{entry.total_successes + entry.total_failures})")
            lines.append(f"      Leads found: {entry.leads_found}")
            lines.append(f"      Discovery: {entry.discovery_method}")
        
        if source.gold_url_patterns:
            lines.append(f"\n🥇 GOLD PATTERNS:")
            for pattern in source.gold_url_patterns[:5]:
                lines.append(f"   ✅ {pattern}")
        
        if source.junk_url_patterns:
            lines.append(f"\n🗑️ JUNK PATTERNS:")
            for pattern in source.junk_url_patterns[:5]:
                lines.append(f"   ❌ {pattern}")
        
        lines.append("\n" + "=" * 60)
        return "\n".join(lines)


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """CLI interface for testing"""
    import sys
    
    intel = SourceIntelligenceService()
    
    if len(sys.argv) < 2:
        print("""
SOURCE INTELLIGENCE SERVICE
===========================

Commands:
    python source_intelligence.py add <name> <url> [fallback_url]
    python source_intelligence.py report <name>
    python source_intelligence.py health
    python source_intelligence.py discover <name>
    python source_intelligence.py heal <name>

Examples:
    python source_intelligence.py add "Four Seasons" "https://press.fourseasons.com/news-releases/"
    python source_intelligence.py report "Four Seasons"
    python source_intelligence.py health
    python source_intelligence.py discover "Four Seasons"
    python source_intelligence.py heal "Four Seasons"
        """)
        return
    
    command = sys.argv[1]
    
    try:
        if command == "add" and len(sys.argv) >= 4:
            name = sys.argv[2]
            url = sys.argv[3]
            fallbacks = sys.argv[4:] if len(sys.argv) > 4 else None
            intel.add_source(name, url, fallbacks)
            print(f"✅ Added source: {name}")
            
        elif command == "report" and len(sys.argv) >= 3:
            name = sys.argv[2]
            print(intel.get_source_report(name))
            
        elif command == "health":
            print(intel.get_health_report())
            
        elif command == "discover" and len(sys.argv) >= 3:
            name = sys.argv[2]
            urls = await intel.discover_content_urls(name)
            print(f"\n🔍 Discovered URLs for {name}:")
            for url in urls:
                print(f"   • {url}")
            
        elif command == "heal" and len(sys.argv) >= 3:
            name = sys.argv[2]
            success = await intel.heal_source(name)
            if success:
                print(f"✅ Successfully healed {name}")
            else:
                print(f"❌ Could not heal {name}")
        
        else:
            print("Unknown command. Run without arguments for help.")
    
    finally:
        await intel.close()


if __name__ == "__main__":
    asyncio.run(main())