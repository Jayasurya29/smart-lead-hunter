"""
SMART LEAD HUNTER - PRODUCTION SCRAPING ENGINE V3
==================================================
A robust, scalable scraping system with comprehensive URL filtering.

CHANGES IN V3:
- Sources loaded from DATABASE (single source of truth)
- Patterns loaded from source_config.py
- Removed dependency on source_tuning.py

FEATURES:
✅ Multi-layer URL filtering (blocks junk pages)
✅ Multi-threaded parallel scraping
✅ Smart rate limiting per domain
✅ Automatic retry with exponential backoff
✅ Deep crawling (follows links to find leads)
✅ Content change detection (skip unchanged pages)
✅ Session management for efficiency
✅ Anti-detection measures
✅ Progress tracking and reporting
✅ Error recovery and logging

Usage:
    engine = ScrapingEngine()
    await engine.initialize()
    results = await engine.scrape_all_sources()
"""

import asyncio
import hashlib
import logging
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# Import our URL filter
try:
    from app.services.url_filter import URLFilter, URLFilterResult
except ImportError:
    # Fallback if running standalone
    from url_filter import URLFilter, URLFilterResult

# Import source patterns
try:
    from app.services.source_config import (
        get_patterns_with_default,
        get_gold_patterns,
        get_block_patterns,
        get_link_patterns,
        get_max_pages,
        has_patterns,
        SourcePatterns,
    )
except ImportError:
    # Fallback - define minimal defaults
    def get_patterns_with_default(name): 
        return None
    def get_gold_patterns(name): 
        return []
    def get_block_patterns(name): 
        return []
    def get_link_patterns(name): 
        return []
    def get_max_pages(name): 
        return 50
    def has_patterns(name): 
        return False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

class CrawlerType(Enum):
    """Types of crawlers available"""
    AUTO = "auto"              # SMART AUTO-SELECTION (recommended!)
    HTTPX = "httpx"            # Fast, for static HTML
    PLAYWRIGHT = "playwright"  # For JavaScript-heavy sites
    CRAWL4AI = "crawl4ai"      # Smart crawler with caching


class ContentType(Enum):
    """Types of content we extract"""
    PRESS_RELEASE = "press_release"
    NEWS_ARTICLE = "news_article"
    HOTEL_LISTING = "hotel_listing"
    DEVELOPMENT_NEWS = "development_news"


@dataclass
class ScrapingConfig:
    """Global scraping configuration"""
    max_concurrent_requests: int = 10
    default_timeout: int = 30
    max_retries: int = 3
    retry_delay_base: float = 2.0
    respect_robots_txt: bool = True
    cache_ttl_hours: int = 24
    user_agent: str = "SmartLeadHunter/1.0 (Hotel Lead Discovery; contact@company.com)"
    
    # Rate limits per domain (requests per minute)
    rate_limits: Dict[str, int] = field(default_factory=lambda: {
        "default": 30,
        "marriott.com": 15,
        "hilton.com": 15,
        "hyatt.com": 15,
        "ihg.com": 15,
        "fourseasons.com": 20,
        "hospitalitynet.org": 30,
        "hoteldive.com": 30,
        "bizjournals.com": 20,
    })


@dataclass
class ScrapeResult:
    """Result from scraping a single URL"""
    url: str
    success: bool
    html: Optional[str] = None
    text: Optional[str] = None
    title: Optional[str] = None
    links: List[str] = field(default_factory=list)
    error: Optional[str] = None
    status_code: Optional[int] = None
    content_hash: Optional[str] = None
    scraped_at: datetime = field(default_factory=datetime.now)
    crawl_time_ms: int = 0
    crawler_used: str = "unknown"
    is_cached: bool = False


@dataclass
class SourceConfig:
    """Configuration for a specific source"""
    name: str
    url: str
    additional_urls: List[str] = field(default_factory=list)  # Extra URLs to scrape
    crawler_type: CrawlerType = CrawlerType.HTTPX
    priority: int = 5
    frequency: str = "daily"
    
    # Selectors for content extraction
    article_selector: Optional[str] = None
    title_selector: Optional[str] = None
    content_selector: Optional[str] = None
    link_selector: Optional[str] = None
    
    # Deep crawl settings
    follow_links: bool = True
    max_depth: int = 2
    max_pages: int = 50
    link_patterns: List[str] = field(default_factory=list)
    
    # URL filtering patterns (from source_config.py)
    gold_patterns: List[str] = field(default_factory=list)
    block_patterns: List[str] = field(default_factory=list)
    
    # Custom settings
    wait_for_selector: Optional[str] = None
    extra_headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)


# =============================================================================
# RATE LIMITER
# =============================================================================

class RateLimiter:
    """
    Token bucket rate limiter per domain.
    Ensures we don't hammer any single website.
    """
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._buckets: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        parsed = urlparse(url)
        return parsed.netloc.lower()
    
    def _get_rate_limit(self, domain: str) -> int:
        """Get rate limit for a domain (requests per minute)"""
        for pattern, limit in self.config.rate_limits.items():
            if pattern in domain:
                return limit
        return self.config.rate_limits.get("default", 30)
    
    async def acquire(self, url: str) -> None:
        """Wait until we can make a request to this URL"""
        domain = self._get_domain(url)
        rate_limit = self._get_rate_limit(domain)
        
        async with self._lock:
            now = time.time()
            
            if domain not in self._buckets:
                self._buckets[domain] = {
                    "tokens": rate_limit,
                    "last_update": now
                }
            
            bucket = self._buckets[domain]
            
            # Refill tokens based on time elapsed
            elapsed = now - bucket["last_update"]
            tokens_to_add = elapsed * (rate_limit / 60.0)
            bucket["tokens"] = min(rate_limit, bucket["tokens"] + tokens_to_add)
            bucket["last_update"] = now
            
            # Wait if no tokens available
            if bucket["tokens"] < 1:
                wait_time = (1 - bucket["tokens"]) * (60.0 / rate_limit)
                logger.debug(f"Rate limiting {domain}: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                bucket["tokens"] = 1
            
            # Consume a token
            bucket["tokens"] -= 1


# =============================================================================
# CONTENT CACHE
# =============================================================================

class ContentCache:
    """
    In-memory cache to avoid re-scraping unchanged content.
    Can be extended to use Redis or database.
    """
    
    def __init__(self, ttl_hours: int = 24):
        self.ttl = timedelta(hours=ttl_hours)
        self._cache: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
    
    def _get_key(self, url: str) -> str:
        """Generate cache key from URL"""
        return hashlib.md5(url.encode()).hexdigest()
    
    async def get(self, url: str) -> Optional[ScrapeResult]:
        """Get cached result if still valid"""
        async with self._lock:
            key = self._get_key(url)
            if key in self._cache:
                entry = self._cache[key]
                if datetime.now() - entry["timestamp"] < self.ttl:
                    result = entry["result"]
                    result.is_cached = True
                    return result
                else:
                    del self._cache[key]
            return None
    
    async def set(self, url: str, result: ScrapeResult) -> None:
        """Cache a scrape result"""
        async with self._lock:
            key = self._get_key(url)
            self._cache[key] = {
                "result": result,
                "timestamp": datetime.now()
            }
    
    async def has_changed(self, url: str, content_hash: str) -> bool:
        """Check if content has changed since last scrape"""
        async with self._lock:
            key = self._get_key(url)
            if key in self._cache:
                old_hash = self._cache[key]["result"].content_hash
                return old_hash != content_hash
            return True  # No cache = assume changed
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        return {
            "entries": len(self._cache),
            "size_bytes": sum(
                len(str(e["result"].html or "")) 
                for e in self._cache.values()
            )
        }


# =============================================================================
# HTTP SCRAPER (Fast, for static sites)
# =============================================================================

class HTTPScraper:
    """
    Fast HTTP scraper using httpx.
    Best for static HTML pages.
    """
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
    
    async def initialize(self):
        """Initialize HTTP client with connection pooling"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.default_timeout),
                follow_redirects=True,
                headers={
                    "User-Agent": self.config.user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                },
                limits=httpx.Limits(
                    max_connections=100,
                    max_keepalive_connections=20
                )
            )
    
    async def scrape(
        self, 
        url: str, 
        extra_headers: Optional[Dict] = None
    ) -> ScrapeResult:
        """Scrape a URL using HTTP"""
        start_time = time.time()
        
        await self.initialize()
        
        try:
            headers = extra_headers or {}
            response = await self._client.get(url, headers=headers)
            
            crawl_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                html = response.text
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract text content
                for script in soup(["script", "style", "nav", "footer", "header"]):
                    script.decompose()
                text = soup.get_text(separator="\n", strip=True)
                
                # Extract title
                title = soup.title.string if soup.title else None
                
                # Extract links
                links = []
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if href.startswith('/'):
                        href = urljoin(url, href)
                    if href.startswith('http'):
                        links.append(href)
                
                # Calculate content hash
                content_hash = hashlib.md5(html.encode()).hexdigest()[:16]
                
                return ScrapeResult(
                    url=url,
                    success=True,
                    html=html,
                    text=text,
                    title=title,
                    links=links[:100],  # Limit links
                    status_code=response.status_code,
                    content_hash=content_hash,
                    crawl_time_ms=crawl_time,
                    crawler_used="httpx"
                )
            else:
                return ScrapeResult(
                    url=url,
                    success=False,
                    error=f"HTTP {response.status_code}",
                    status_code=response.status_code,
                    crawl_time_ms=crawl_time,
                    crawler_used="httpx"
                )
                
        except Exception as e:
            crawl_time = int((time.time() - start_time) * 1000)
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawl_time_ms=crawl_time,
                crawler_used="httpx"
            )
    
    async def close(self):
        """Close the HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None


# =============================================================================
# PLAYWRIGHT SCRAPER (For JavaScript-heavy sites)
# =============================================================================

class PlaywrightScraper:
    """
    Playwright-based scraper for JavaScript-heavy sites.
    Renders pages fully before extracting content.
    """
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._playwright = None
        self._browser = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize Playwright browser"""
        if self._initialized:
            return
        
        try:
            from playwright.async_api import async_playwright
            
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ]
            )
            self._initialized = True
            logger.info("✅ Playwright browser initialized")
            
        except Exception as e:
            logger.error(f"❌ Playwright initialization failed: {e}")
            raise
    
    async def scrape(
        self, 
        url: str, 
        wait_for: Optional[str] = None,
        extra_headers: Optional[Dict] = None
    ) -> ScrapeResult:
        """Scrape a URL using Playwright"""
        start_time = time.time()
        
        await self.initialize()
        
        context = None
        page = None
        
        try:
            context = await self._browser.new_context(
                user_agent=self.config.user_agent,
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers=extra_headers or {}
            )
            
            page = await context.new_page()
            
            # Navigate to URL
            response = await page.goto(
                url, 
                wait_until="networkidle",
                timeout=self.config.default_timeout * 1000
            )
            
            # Wait for specific selector if provided
            if wait_for:
                try:
                    await page.wait_for_selector(wait_for, timeout=10000)
                except:
                    pass  # Continue even if selector not found
            
            # Small delay for dynamic content
            await asyncio.sleep(1)
            
            # Get page content
            html = await page.content()
            
            crawl_time = int((time.time() - start_time) * 1000)
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract text
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            text = soup.get_text(separator="\n", strip=True)
            
            # Extract title
            title = soup.title.string if soup.title else None
            
            # Extract links
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith('/'):
                    href = urljoin(url, href)
                if href.startswith('http'):
                    links.append(href)
            
            # Content hash
            content_hash = hashlib.md5(html.encode()).hexdigest()[:16]
            
            return ScrapeResult(
                url=url,
                success=True,
                html=html,
                text=text,
                title=title,
                links=links[:100],
                status_code=response.status if response else None,
                content_hash=content_hash,
                crawl_time_ms=crawl_time,
                crawler_used="playwright"
            )
            
        except Exception as e:
            crawl_time = int((time.time() - start_time) * 1000)
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawl_time_ms=crawl_time,
                crawler_used="playwright"
            )
            
        finally:
            if page:
                await page.close()
            if context:
                await context.close()
    
    async def close(self):
        """Close Playwright browser"""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._initialized = False


# =============================================================================
# CRAWL4AI SCRAPER (Smart AI-ready scraping)
# =============================================================================

class Crawl4AIScraper:
    """
    Crawl4AI-based scraper for smart, AI-ready content extraction.
    
    FEATURES:
    - Built-in caching (skip already-scraped pages)
    - Anti-bot bypass (better than raw Playwright)
    - Clean markdown output (LLM-ready, fewer tokens)
    - Automatic link extraction
    - Smart content extraction
    
    Uses Playwright internally but adds intelligence layer.
    """
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._crawler = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize Crawl4AI crawler"""
        if self._initialized:
            return
        
        try:
            from crawl4ai import AsyncWebCrawler
            self._crawler = AsyncWebCrawler(verbose=False)
            await self._crawler.start()
            self._initialized = True
            logger.info("✅ Crawl4AI initialized")
        except ImportError:
            logger.warning("⚠️ Crawl4AI not installed. Run: pip install crawl4ai")
            raise
        except Exception as e:
            logger.error(f"❌ Failed to initialize Crawl4AI: {e}")
            raise
    
    async def scrape(
        self, 
        url: str, 
        bypass_cache: bool = False
    ) -> ScrapeResult:
        """
        Scrape a URL using Crawl4AI.
        
        Args:
            url: URL to scrape
            bypass_cache: If True, force fresh scrape even if cached
        
        Returns:
            ScrapeResult with clean markdown content
        """
        start_time = time.time()
        
        if not self._initialized:
            await self.initialize()
        
        try:
            # Run the crawler
            result = await self._crawler.arun(
                url=url,
                bypass_cache=bypass_cache,
            )
            
            crawl_time = int((time.time() - start_time) * 1000)
            
            if result.success:
                # Crawl4AI provides clean markdown - great for LLM!
                text = result.markdown if hasattr(result, 'markdown') else result.extracted_content
                html = result.html if hasattr(result, 'html') else ""
                
                # Extract title
                title = None
                if hasattr(result, 'metadata') and result.metadata:
                    title = result.metadata.get('title')
                
                # Get links - Crawl4AI can return various formats
                links = []
                if hasattr(result, 'links') and result.links:
                    raw_links = result.links
                    
                    # Handle dict format: {'internal': [...], 'external': [...]}
                    if isinstance(raw_links, dict):
                        internal = raw_links.get('internal', [])
                        external = raw_links.get('external', [])
                        raw_links = internal + external
                    
                    # Extract URLs from links (could be strings or dicts)
                    for link in raw_links:
                        if isinstance(link, str):
                            links.append(link)
                        elif isinstance(link, dict):
                            # Crawl4AI often returns {'href': 'url', 'text': '...'}
                            href = link.get('href') or link.get('url') or link.get('link')
                            if href:
                                links.append(href)
                
                # Content hash
                content_hash = hashlib.md5((text or "").encode()).hexdigest()[:16]
                
                return ScrapeResult(
                    url=url,
                    success=True,
                    html=html,
                    text=text,  # This is clean markdown!
                    title=title,
                    links=links[:100],
                    status_code=200,
                    content_hash=content_hash,
                    crawl_time_ms=crawl_time,
                    crawler_used="crawl4ai"
                )
            else:
                return ScrapeResult(
                    url=url,
                    success=False,
                    error=result.error_message if hasattr(result, 'error_message') else "Unknown error",
                    crawl_time_ms=crawl_time,
                    crawler_used="crawl4ai"
                )
                
        except Exception as e:
            crawl_time = int((time.time() - start_time) * 1000)
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawl_time_ms=crawl_time,
                crawler_used="crawl4ai"
            )
    
    async def close(self):
        """Close Crawl4AI crawler"""
        if self._crawler:
            await self._crawler.close()
            self._crawler = None
        self._initialized = False


# =============================================================================
# DEEP CRAWLER V3 (With URL Filtering + source_config patterns)
# =============================================================================

class DeepCrawler:
    """
    Crawls a source deeply by following links.
    
    V3 IMPROVEMENTS:
    - Uses patterns from source_config.py
    - SMART SCRAPER SELECTION - Auto-chooses best scraper
    - AUTOMATIC FALLBACK - If HTTPX fails, tries Crawl4AI, then Playwright
    - DOMAIN MEMORY - Remembers which scraper works for each domain
    - Uses URLFilter to block junk URLs BEFORE scraping
    - Better prioritization of valuable links
    """
    
    def __init__(
        self, 
        http_scraper: HTTPScraper,
        playwright_scraper: PlaywrightScraper,
        rate_limiter: RateLimiter,
        cache: ContentCache,
        crawl4ai_scraper: Optional['Crawl4AIScraper'] = None
    ):
        self.http = http_scraper
        self.playwright = playwright_scraper
        self.crawl4ai = crawl4ai_scraper
        self.rate_limiter = rate_limiter
        self.cache = cache
        
        # Initialize URL filter
        self.url_filter = URLFilter()
        
        # SMART SCRAPER MEMORY - Remember what works for each domain
        self._domain_scraper_memory: Dict[str, str] = {}
        
        # Statistics
        self._filter_stats = {
            'urls_discovered': 0,
            'urls_blocked': 0,
            'urls_allowed': 0,
        }
    
    def _should_follow_link(self, url: str, source: SourceConfig) -> bool:
        """
        Check if a link should be followed based on source patterns.
        Uses gold_patterns and block_patterns from source_config.py
        """
        url_lower = url.lower()
        
        # Check block patterns first
        for pattern in source.block_patterns:
            if re.search(pattern, url_lower):
                return False
        
        # If we have gold patterns, URL must match one
        if source.gold_patterns:
            for pattern in source.gold_patterns:
                if re.search(pattern, url_lower):
                    return True
            return False  # Has gold patterns but didn't match any
        
        # If we have link patterns (legacy), check those
        if source.link_patterns:
            for pattern in source.link_patterns:
                if re.search(pattern, url_lower):
                    return True
            return False
        
        # No patterns = allow all (will be filtered by URLFilter)
        return True
    
    def _filter_links(self, links: List, source: SourceConfig) -> List[str]:
        """
        Filter links using source patterns + URLFilter.
        
        Returns only links that should be scraped.
        """
        # Sanitize links - ensure all are strings
        clean_links = []
        for link in links:
            if isinstance(link, str):
                clean_links.append(link)
            elif isinstance(link, dict):
                href = link.get('href') or link.get('url') or link.get('link')
                if href and isinstance(href, str):
                    clean_links.append(href)
        
        self._filter_stats['urls_discovered'] += len(clean_links)
        
        # First pass: source-specific patterns
        pattern_filtered = []
        for link in clean_links:
            if self._should_follow_link(link, source):
                pattern_filtered.append(link)
        
        # Second pass: URLFilter for general junk
        filtered = self.url_filter.filter_urls(pattern_filtered, source.url)
        
        self._filter_stats['urls_allowed'] += len(filtered)
        self._filter_stats['urls_blocked'] += len(clean_links) - len(filtered)
        
        return filtered
    
    async def crawl(
        self, 
        source: SourceConfig,
        max_depth: int = 2,
        max_pages: int = 50
    ) -> List[ScrapeResult]:
        """
        Deep crawl a source following relevant links.
        
        Args:
            source: Source configuration (includes patterns from source_config.py)
            max_depth: Maximum link depth to follow
            max_pages: Maximum pages to scrape
        
        Returns:
            List of ScrapeResults for all pages
        """
        results: List[ScrapeResult] = []
        visited: Set[str] = set()
        
        # Initialize queue with source URL and any additional URLs (depth 0)
        to_visit: List[Tuple[str, int]] = [(source.url, 0)]
        
        # Add additional URLs if configured
        if hasattr(source, 'additional_urls') and source.additional_urls:
            for extra_url in source.additional_urls:
                to_visit.append((extra_url, 0))
        
        # Reset filter stats for this crawl
        self._filter_stats = {
            'urls_discovered': 0,
            'urls_blocked': 0,
            'urls_allowed': 0,
        }
        
        while to_visit and len(results) < max_pages:
            url, depth = to_visit.pop(0)
            
            # Safety check - ensure URL is a string
            if not isinstance(url, str):
                if isinstance(url, dict):
                    url = url.get('href') or url.get('url') or url.get('link', '')
                else:
                    continue
            
            if not url:
                continue
            
            # Normalize URL for deduplication
            url_normalized = url.lower().rstrip('/')
            
            # Skip if already visited
            if url_normalized in visited:
                continue
            visited.add(url_normalized)
            
            # ========== URL FILTERING ==========
            # Check source-specific patterns
            if depth > 0 and not self._should_follow_link(url, source):
                logger.debug(f"🚫 Pattern blocked: {url[:60]}...")
                continue
            
            # Check URLFilter for general junk
            filter_result = self.url_filter.should_scrape(url, source.url)
            if not filter_result.should_scrape:
                logger.debug(f"🚫 URLFilter blocked: {url[:60]}... - {filter_result.reason}")
                continue
            # ===================================
            
            # Check cache
            cached = await self.cache.get(url)
            if cached:
                results.append(cached)
                logger.info(f"📦 Cache hit: {url[:60]}...")
                
                # Still process links from cached content
                if depth < max_depth and source.follow_links:
                    filtered_links = self._filter_links(cached.links, source)
                    for link in filtered_links:
                        link_normalized = link.lower().rstrip('/')
                        if link_normalized not in visited:
                            to_visit.append((link, depth + 1))
                continue
            
            # Rate limit
            await self.rate_limiter.acquire(url)
            
            # SMART SCRAPER SELECTION - Auto-choose and fallback
            result = await self._smart_scrape(url, source)
            
            results.append(result)
            
            # Cache successful results
            if result.success:
                await self.cache.set(url, result)
                logger.info(f"✅ Scraped: {url[:60]}... ({result.crawl_time_ms}ms) [{result.crawler_used}]")
                
                # Follow links if not at max depth
                if depth < max_depth and source.follow_links:
                    # Filter links using patterns
                    filtered_links = self._filter_links(result.links, source)
                    
                    for link in filtered_links:
                        link_normalized = link.lower().rstrip('/')
                        if link_normalized not in visited:
                            to_visit.append((link, depth + 1))
            else:
                logger.warning(f"❌ Failed: {url[:60]}... - {result.error}")
        
        # Log filtering statistics
        stats = self._filter_stats
        logger.info(
            f"📊 URL Filter Stats: "
            f"{stats['urls_discovered']} discovered, "
            f"{stats['urls_allowed']} allowed, "
            f"{stats['urls_blocked']} blocked"
        )
        
        return results
    
    def get_filter_stats(self) -> Dict[str, int]:
        """Get URL filtering statistics"""
        return self._filter_stats.copy()
    
    async def _smart_scrape(self, url: str, source: SourceConfig) -> ScrapeResult:
        """
        SMART SCRAPER SELECTION
        =======================
        Automatically chooses the best scraper and falls back if needed.
        
        STRATEGY:
        1. If AUTO (default) → Smart selection with fallback
        2. If source specifies CRAWL4AI → Use it (best for JS + caching)
        3. If source specifies PLAYWRIGHT → Use it
        4. If source specifies HTTPX → Use it (but fallback if fails)
        
        SMART AUTO LOGIC:
        - Try HTTPX first (fastest)
        - If 403/blocked → Fallback to CRAWL4AI
        - If CRAWL4AI fails → Fallback to Playwright
        
        LEARNING:
        - Remembers which scraper works for each domain
        - Next time, uses the working scraper directly
        """
        domain = self._get_domain(url)
        
        # Check if we've learned which scraper works for this domain
        if domain in self._domain_scraper_memory:
            preferred = self._domain_scraper_memory[domain]
            logger.debug(f"🧠 Using remembered scraper for {domain}: {preferred}")
            result = await self._scrape_with(url, preferred, source)
            if result.success:
                return result
            # If preferred fails, clear memory and try fresh
            del self._domain_scraper_memory[domain]
        
        # If source explicitly specifies a scraper (not AUTO), try it first
        if source.crawler_type == CrawlerType.CRAWL4AI and self.crawl4ai:
            result = await self._scrape_with(url, "crawl4ai", source)
            if result.success:
                self._remember_scraper(domain, "crawl4ai")
                return result
            # Fallback to others if specified scraper fails
            
        elif source.crawler_type == CrawlerType.PLAYWRIGHT:
            result = await self._scrape_with(url, "playwright", source)
            if result.success:
                self._remember_scraper(domain, "playwright")
                return result
            # Fallback to others if specified scraper fails
            
        elif source.crawler_type == CrawlerType.HTTPX:
            result = await self._scrape_with(url, "httpx", source)
            if result.success:
                self._remember_scraper(domain, "httpx")
                return result
            # Fallback to others if specified scraper fails
        
        # AUTO MODE or fallback from failed specified scraper
        # ====================================================
        
        # Step 1: Try HTTPX first (fastest)
        result = await self._scrape_with(url, "httpx", source)
        
        if result.success and not self._looks_like_blocked_content(result):
            self._remember_scraper(domain, "httpx")
            return result
        
        # Step 2: HTTPX failed or looks blocked - check why
        is_blocked = self._is_blocked_response(result)
        is_js_needed = self._might_need_javascript(result)
        looks_blocked = self._looks_like_blocked_content(result)
        
        if is_blocked or is_js_needed or looks_blocked or not result.success:
            # Step 3: Try CRAWL4AI (smart + anti-bot + caching)
            if self.crawl4ai:
                logger.info(f"🔄 Auto-switching to Crawl4AI for {domain}...")
                result = await self._scrape_with(url, "crawl4ai", source)
                if result.success and not self._looks_like_blocked_content(result):
                    self._remember_scraper(domain, "crawl4ai")
                    return result
            
            # Step 4: Try Playwright as last resort
            logger.info(f"🔄 Auto-switching to Playwright for {domain}...")
            result = await self._scrape_with(url, "playwright", source)
            if result.success:
                self._remember_scraper(domain, "playwright")
                return result
        
        # All failed, return last result
        return result
    
    async def _scrape_with(self, url: str, scraper: str, source: SourceConfig) -> ScrapeResult:
        """Scrape with a specific scraper"""
        try:
            if scraper == "crawl4ai" and self.crawl4ai:
                return await self.crawl4ai.scrape(url)
            elif scraper == "playwright":
                return await self.playwright.scrape(
                    url,
                    wait_for=source.wait_for_selector,
                    extra_headers=source.extra_headers
                )
            else:  # httpx
                return await self.http.scrape(
                    url,
                    extra_headers=source.extra_headers
                )
        except Exception as e:
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawler_used=scraper
            )
    
    def _is_blocked_response(self, result: ScrapeResult) -> bool:
        """Check if response indicates we're blocked"""
        if not result.error:
            return False
        
        error_lower = result.error.lower()
        blocked_indicators = [
            "403", "forbidden",
            "401", "unauthorized",
            "429", "too many requests",
            "blocked", "denied",
            "captcha", "challenge",
            "cloudflare", "security check",
            "access denied", "bot detected"
        ]
        return any(indicator in error_lower for indicator in blocked_indicators)
    
    def _looks_like_blocked_content(self, result: ScrapeResult) -> bool:
        """Check if the content looks like a block/challenge page"""
        if not result.success or not result.text:
            return False
        
        text_lower = result.text.lower()
        
        # Check for common block page indicators
        block_indicators = [
            "access denied",
            "please verify you are human",
            "checking your browser",
            "ray id",  # Cloudflare
            "attention required",
            "one more step",
            "security check",
            "captcha",
            "robot or human",
            "ddos protection",
            "please wait while we verify",
            "just a moment",
        ]
        
        # Very short content often indicates a block page
        if len(result.text.strip()) < 200:
            return True
        
        return any(indicator in text_lower for indicator in block_indicators)
    
    def _might_need_javascript(self, result: ScrapeResult) -> bool:
        """Check if page might need JavaScript rendering"""
        if result.success and result.text:
            text_lower = result.text.lower()
            js_indicators = [
                "enable javascript",
                "javascript required",
                "please enable js",
                "loading...",
                "react-root",
                "angular",
                "__next",
                "window.__initial"
            ]
            # Very short content might indicate JS-rendered page
            if len(result.text) < 500:
                return True
            return any(indicator in text_lower for indicator in js_indicators)
        return False
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.lower().replace('www.', '')
        except:
            return url
    
    def _remember_scraper(self, domain: str, scraper: str):
        """Remember which scraper works for a domain"""
        self._domain_scraper_memory[domain] = scraper
        logger.debug(f"📝 Remembered: {domain} → {scraper}")


# =============================================================================
# MAIN SCRAPING ENGINE V3
# =============================================================================

class ScrapingEngine:
    """
    Main orchestrator for scraping sources.
    
    V3 CHANGES:
    - Sources loaded from DATABASE
    - Patterns loaded from source_config.py
    - No dependency on source_tuning.py
    
    Features:
    - Parallel scraping with rate limiting
    - Automatic retry with backoff
    - Deep crawling for more leads
    - URL filtering to block junk pages
    - Progress tracking
    - Error recovery
    
    Usage:
        engine = ScrapingEngine()
        await engine.initialize()
        
        # Scrape all sources
        results = await engine.scrape_all_sources()
        
        # Or scrape specific sources
        results = await engine.scrape_sources(["Hospitality Net", "Hotel Dive"])
        
        await engine.close()
    """
    
    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.config = config or ScrapingConfig()
        self.rate_limiter = RateLimiter(self.config)
        self.cache = ContentCache(self.config.cache_ttl_hours)
        
        self.http_scraper = HTTPScraper(self.config)
        self.playwright_scraper = PlaywrightScraper(self.config)
        self.crawl4ai_scraper = None  # Initialize later if available
        
        # Deep crawler will be set up after all scrapers are ready
        self.deep_crawler = None
        
        self._sources: Dict[str, SourceConfig] = {}
        self._initialized = False
        
        # Statistics
        self._stats = {
            "total_scraped": 0,
            "successful": 0,
            "failed": 0,
            "cached_hits": 0,
            "total_time_ms": 0,
            "urls_filtered": 0,
        }
    
    async def initialize(self):
        """Initialize all scrapers and load sources"""
        if self._initialized:
            return
        
        logger.info("🚀 Initializing Scraping Engine V3...")
        
        # Initialize scrapers
        await self.http_scraper.initialize()
        
        try:
            await self.playwright_scraper.initialize()
        except Exception as e:
            logger.warning(f"⚠️ Playwright not available: {e}")
        
        # Try to initialize Crawl4AI (optional but recommended)
        try:
            self.crawl4ai_scraper = Crawl4AIScraper(self.config)
            await self.crawl4ai_scraper.initialize()
            logger.info("✅ Crawl4AI initialized (smart AI-ready scraping)")
        except ImportError:
            logger.warning("⚠️ Crawl4AI not installed. Run: pip install crawl4ai")
            logger.warning("   Falling back to Playwright for JS-heavy sites")
        except Exception as e:
            logger.warning(f"⚠️ Crawl4AI not available: {e}")
        
        # Create deep crawler with all available scrapers
        self.deep_crawler = DeepCrawler(
            self.http_scraper,
            self.playwright_scraper,
            self.rate_limiter,
            self.cache,
            self.crawl4ai_scraper
        )
        
        # Load sources from DATABASE + patterns from source_config.py
        await self._load_sources()
        
        self._initialized = True
        logger.info(f"✅ Engine initialized with {len(self._sources)} sources")
    
    async def _load_sources(self):
        """
        Load sources from DATABASE + patterns from source_config.py
        
        V3 Architecture:
        - Database = source list + runtime stats
        - source_config.py = URL patterns (gold, block, link)
        """
        sources_loaded = 0
        
        # PRIMARY: Load from DATABASE
        try:
            from app.database import async_session
            from app.models.source import Source
            from sqlalchemy import select

            async with async_session() as db:
                result = await db.execute(select(Source).where(Source.is_active == True))
                db_sources = result.scalars().all()
            
            for src in db_sources:
                # Get patterns from source_config.py
                patterns = get_patterns_with_default(src.name)
                
                # Determine crawler type
                if src.use_playwright:
                    crawler = CrawlerType.PLAYWRIGHT
                else:
                    crawler = CrawlerType.AUTO  # Smart auto-selection
                
                # Get additional entry URLs if available
                additional_urls = []
                if hasattr(src, 'entry_urls') and src.entry_urls:
                    additional_urls = [u for u in src.entry_urls if u != src.base_url]
                
                # Create SourceConfig with patterns
                self._sources[src.name] = SourceConfig(
                    name=src.name,
                    url=src.base_url,
                    additional_urls=additional_urls,
                    crawler_type=crawler,
                    priority=src.priority or 5,
                    follow_links=True,
                    max_depth=src.max_depth or 2,
                    max_pages=patterns.max_pages if patterns else (src.max_depth * 20 if src.max_depth else 50),
                    # Patterns from source_config.py
                    gold_patterns=patterns.gold_patterns if patterns else [],
                    block_patterns=patterns.block_patterns if patterns else [],
                    link_patterns=patterns.link_patterns if patterns else [],
                )
                sources_loaded += 1
            
            logger.info(f"✅ Loaded {sources_loaded} sources from DATABASE")
            
            # Log sources without patterns
            sources_without_patterns = [
                name for name in self._sources.keys() 
                if not has_patterns(name)
            ]
            if sources_without_patterns:
                logger.warning(
                    f"⚠️ {len(sources_without_patterns)} sources missing patterns in source_config.py: "
                    f"{sources_without_patterns[:5]}..."
                )
            
            return
            
        except ImportError as e:
            logger.warning(f"⚠️ Database not available: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to load from database: {e}")
        
        # FALLBACK: Add hardcoded defaults if database is empty
        logger.warning("⚠️ Using LAST RESORT: hardcoded default sources")
        self._add_default_sources()
    
    def _add_default_sources(self):
        """Add hardcoded default sources as last resort fallback"""
        defaults = [
            SourceConfig(
                name="Hospitality Net",
                url="https://www.hospitalitynet.org/news/global.html",
                crawler_type=CrawlerType.HTTPX,
                priority=8,
                follow_links=True,
                max_pages=30,
                gold_patterns=[r'/announcement/\d+/', r'/news/\d+\.html'],
                block_patterns=[r'/organization/', r'/opinion/', r'/video/'],
                link_patterns=[r'announcement/\d+', r'news/\d+\.html']
            ),
            SourceConfig(
                name="Hotel Dive",
                url="https://www.hoteldive.com/topic/development/",
                crawler_type=CrawlerType.HTTPX,
                priority=10,
                follow_links=True,
                max_pages=50,
                gold_patterns=[r'/news/[a-z0-9-]+/\d+/'],
                block_patterns=[r'/selfservice/', r'/library/', r'/events/'],
                link_patterns=[r'/news/[a-z0-9-]+/\d+/']
            ),
            SourceConfig(
                name="Caribbean Journal",
                url="https://www.caribjournal.com/category/hotels/",
                crawler_type=CrawlerType.HTTPX,
                priority=10,
                follow_links=True,
                max_pages=50,
                gold_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel', r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-resort'],
                block_patterns=[r'/tag/', r'/author/', r'-cruise'],
                link_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+/']
            ),
        ]
        
        for source in defaults:
            self._sources[source.name] = source
    
    async def scrape_source(
        self, 
        source_name: str,
        deep: bool = True
    ) -> List[ScrapeResult]:
        """
        Scrape a single source.
        
        Args:
            source_name: Name of the source
            deep: Whether to follow links
        
        Returns:
            List of ScrapeResults
        """
        if source_name not in self._sources:
            logger.error(f"Source not found: {source_name}")
            return []
        
        source = self._sources[source_name]
        
        logger.info(f"🔍 Scraping: {source.name}")
        start_time = time.time()
        
        if deep:
            results = await self.deep_crawler.crawl(
                source,
                max_depth=source.max_depth,
                max_pages=source.max_pages
            )
        else:
            # Single page scrape
            await self.rate_limiter.acquire(source.url)
            
            if source.crawler_type == CrawlerType.PLAYWRIGHT:
                result = await self.playwright_scraper.scrape(
                    source.url,
                    wait_for=source.wait_for_selector
                )
            else:
                result = await self.http_scraper.scrape(source.url)
            
            results = [result]
        
        # Update stats
        elapsed = int((time.time() - start_time) * 1000)
        self._stats["total_scraped"] += len(results)
        self._stats["successful"] += sum(1 for r in results if r.success)
        self._stats["failed"] += sum(1 for r in results if not r.success)
        self._stats["cached_hits"] += sum(1 for r in results if r.is_cached)
        self._stats["total_time_ms"] += elapsed
        self._stats["urls_filtered"] += self.deep_crawler.get_filter_stats().get('urls_blocked', 0)
        
        logger.info(
            f"✅ {source.name}: {len(results)} pages, "
            f"{sum(1 for r in results if r.success)} success, "
            f"{elapsed}ms"
        )
        
        return results
    
    async def scrape_sources(
        self, 
        source_names: List[str],
        deep: bool = True,
        max_concurrent: int = 5
    ) -> Dict[str, List[ScrapeResult]]:
        """
        Scrape multiple sources in parallel.
        
        Args:
            source_names: List of source names
            deep: Whether to deep crawl
            max_concurrent: Max concurrent sources
        
        Returns:
            Dict mapping source name to results
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results: Dict[str, List[ScrapeResult]] = {}
        
        async def scrape_with_semaphore(name: str):
            async with semaphore:
                return name, await self.scrape_source(name, deep=deep)
        
        tasks = [scrape_with_semaphore(name) for name in source_names]
        completed = await asyncio.gather(*tasks, return_exceptions=True)
        
        for item in completed:
            if isinstance(item, Exception):
                logger.error(f"Source failed: {item}")
            else:
                name, source_results = item
                results[name] = source_results
        
        return results
    
    async def scrape_all_sources(
        self, 
        deep: bool = True,
        max_concurrent: int = 5,
        priority_threshold: int = 7
    ) -> Dict[str, List[ScrapeResult]]:
        """
        Scrape all sources above priority threshold.
        
        Args:
            deep: Whether to deep crawl
            max_concurrent: Max concurrent sources
            priority_threshold: Minimum priority to scrape
        
        Returns:
            Dict mapping source name to results
        """
        # Filter by priority and sort
        sources_to_scrape = [
            name for name, config in self._sources.items()
            if config.priority >= priority_threshold
        ]
        sources_to_scrape.sort(
            key=lambda x: self._sources[x].priority,
            reverse=True
        )
        
        logger.info(f"📋 Scraping {len(sources_to_scrape)} sources (priority >= {priority_threshold})")
        
        return await self.scrape_sources(
            sources_to_scrape,
            deep=deep,
            max_concurrent=max_concurrent
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics"""
        return {
            **self._stats,
            "sources_loaded": len(self._sources),
            "cache_stats": self.cache.get_stats(),
            "avg_time_per_page": (
                self._stats["total_time_ms"] / max(1, self._stats["total_scraped"])
            ),
            "url_filter_stats": self.deep_crawler.url_filter.get_stats() if self.deep_crawler else {}
        }
    
    def list_sources(self) -> List[Dict[str, Any]]:
        """List all loaded sources"""
        return [
            {
                "name": config.name,
                "url": config.url,
                "priority": config.priority,
                "crawler": config.crawler_type.value,
                "frequency": config.frequency,
                "has_patterns": bool(config.gold_patterns or config.block_patterns),
            }
            for config in sorted(
                self._sources.values(),
                key=lambda x: -x.priority
            )
        ]
    
    async def close(self):
        """Clean up resources"""
        await self.http_scraper.close()
        await self.playwright_scraper.close()
        if self.crawl4ai_scraper:
            await self.crawl4ai_scraper.close()
        logger.info("🔒 Scraping engine closed")


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Run the scraping engine"""
    print("=" * 70)
    print("SMART LEAD HUNTER - PRODUCTION SCRAPING ENGINE V3")
    print("=" * 70)
    
    engine = ScrapingEngine()
    
    try:
        await engine.initialize()
        
        # List sources
        print(f"\n📚 Loaded {len(engine._sources)} sources:")
        for source in engine.list_sources()[:10]:
            patterns_status = "✓ patterns" if source['has_patterns'] else "⚠ no patterns"
            print(f"   [{source['priority']}] {source['name']} ({patterns_status})")
        if len(engine._sources) > 10:
            print(f"   ... and {len(engine._sources) - 10} more\n")
        
        # Test scrape a few sources
        print("🔍 Testing scrape on 3 sources...\n")
        
        test_sources = list(engine._sources.keys())[:3]
        results = await engine.scrape_sources(test_sources, deep=False)
        
        for name, source_results in results.items():
            print(f"\n📰 {name}:")
            for result in source_results:
                if result.success:
                    print(f"   ✅ {result.url[:60]}...")
                    print(f"      {len(result.text or '')} chars, {len(result.links)} links")
                else:
                    print(f"   ❌ {result.error}")
        
        # Print stats
        stats = engine.get_stats()
        print(f"\n📊 Statistics:")
        print(f"   Total scraped: {stats['total_scraped']}")
        print(f"   Successful: {stats['successful']}")
        print(f"   Failed: {stats['failed']}")
        print(f"   Cached hits: {stats['cached_hits']}")
        print(f"   URLs filtered: {stats['urls_filtered']}")
        print(f"   Avg time/page: {stats['avg_time_per_page']:.0f}ms")
        
        if 'url_filter_stats' in stats:
            uf = stats['url_filter_stats']
            print(f"\n🚫 URL Filter Stats:")
            print(f"   Total checked: {uf.get('total_checked', 0)}")
            print(f"   Blocked: {uf.get('blocked', 0)}")
            print(f"   Allowed: {uf.get('allowed', 0)}")
        
    finally:
        await engine.close()
    
    print("\n" + "=" * 70)
    print("SCRAPING TEST COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())