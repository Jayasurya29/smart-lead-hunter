"""
SMART LEAD HUNTER - PRODUCTION SCRAPING ENGINE V3
==================================================
H-05 FIX: Added asyncio.Lock to PlaywrightScraper and Crawl4AIScraper
initialization to prevent race conditions during concurrent scraping.

Uses double-check locking pattern: check _initialized before and after
acquiring the lock so the fast path (already initialized) doesn't block.
"""

import asyncio
import hashlib
import logging
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from app.services.utils import local_now

try:
    from app.services.url_filter import URLFilter
except ImportError:
    from url_filter import URLFilter

try:
    from app.services.source_config import (
        get_patterns_with_default,
        get_gold_patterns,
        get_block_patterns,
        get_link_patterns,
        get_max_pages,
        has_patterns,
    )
except ImportError:

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


# logging configured by app startup
logger = logging.getLogger(__name__)


class CrawlerType(Enum):
    AUTO = "auto"
    HTTPX = "httpx"
    PLAYWRIGHT = "playwright"
    CRAWL4AI = "crawl4ai"


class ContentType(Enum):
    PRESS_RELEASE = "press_release"
    NEWS_ARTICLE = "news_article"
    HOTEL_LISTING = "hotel_listing"
    DEVELOPMENT_NEWS = "development_news"


@dataclass
class ScrapingConfig:
    max_concurrent_requests: int = 10
    default_timeout: int = 30
    max_retries: int = 3
    retry_delay_base: float = 2.0
    respect_robots_txt: bool = True
    cache_ttl_hours: int = 24
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    rate_limits: Dict[str, int] = field(
        default_factory=lambda: {
            "default": 30,
            "marriott.com": 15,
            "hilton.com": 15,
            "hyatt.com": 15,
            "ihg.com": 15,
            "fourseasons.com": 20,
            "hospitalitynet.org": 30,
            "hoteldive.com": 30,
            "bizjournals.com": 20,
        }
    )


@dataclass
class ScrapeResult:
    url: str
    success: bool
    html: Optional[str] = None
    text: Optional[str] = None
    title: Optional[str] = None
    links: List[str] = field(default_factory=list)
    error: Optional[str] = None
    status_code: Optional[int] = None
    content_hash: Optional[str] = None
    scraped_at: datetime = field(default_factory=lambda: local_now())
    crawl_time_ms: int = 0
    crawler_used: str = "unknown"
    is_cached: bool = False


@dataclass
class SourceConfig:
    name: str
    url: str
    additional_urls: List[str] = field(default_factory=list)
    crawler_type: CrawlerType = CrawlerType.HTTPX
    priority: int = 5
    frequency: str = "daily"
    article_selector: Optional[str] = None
    title_selector: Optional[str] = None
    content_selector: Optional[str] = None
    link_selector: Optional[str] = None
    follow_links: bool = True
    max_depth: int = 2
    max_pages: int = 50
    link_patterns: List[str] = field(default_factory=list)
    gold_patterns: List[str] = field(default_factory=list)
    block_patterns: List[str] = field(default_factory=list)
    wait_for_selector: Optional[str] = None
    extra_headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)


class RateLimiter:
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._buckets: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def _get_rate_limit(self, domain: str) -> int:
        for pattern, limit in self.config.rate_limits.items():
            if pattern in domain:
                return limit
        return self.config.rate_limits.get("default", 30)

    async def acquire(self, url: str) -> None:
        domain = self._get_domain(url)
        rate_limit = self._get_rate_limit(domain)

        # M-11 FIX: Calculate wait time inside the lock, but sleep OUTSIDE
        # it. Previously the sleep happened inside `async with self._lock`,
        # meaning a 2-second rate-limit delay on marriott.com would block
        # ALL domains (including hospitalitynet.org) from proceeding.
        wait_time = 0.0
        async with self._lock:
            now = time.time()
            if domain not in self._buckets:
                self._buckets[domain] = {"tokens": rate_limit, "last_update": now}
            bucket = self._buckets[domain]
            elapsed = now - bucket["last_update"]
            bucket["tokens"] = min(
                rate_limit, bucket["tokens"] + elapsed * (rate_limit / 60.0)
            )
            bucket["last_update"] = now
            if bucket["tokens"] < 1:
                wait_time = (1 - bucket["tokens"]) * (60.0 / rate_limit)
                bucket["tokens"] = 0  # Will be refilled after sleep
            bucket["tokens"] -= 1

        # Sleep outside the lock so other domains aren't blocked
        if wait_time > 0:
            logger.debug(f"Rate limiting {domain}: waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)


class ContentCache:
    def __init__(self, ttl_hours: int = 24, max_entries: int = 1000):
        self.ttl = timedelta(hours=ttl_hours)
        self.max_entries = (
            max_entries  # M-06: Cap cache to prevent unbounded memory growth
        )
        self._cache: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    def _get_key(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    async def get(self, url: str) -> Optional[ScrapeResult]:
        async with self._lock:
            key = self._get_key(url)
            if key in self._cache:
                entry = self._cache[key]
                if local_now() - entry["timestamp"] < self.ttl:
                    # M-06: Update access time for LRU tracking
                    entry["last_access"] = local_now()
                    result = entry["result"]
                    result.is_cached = True
                    return result
                else:
                    del self._cache[key]
            return None

    async def set(self, url: str, result: ScrapeResult) -> None:
        async with self._lock:
            # M-06: Evict oldest entries if at capacity
            if len(self._cache) >= self.max_entries:
                self._evict_lru()
            now = local_now()
            self._cache[self._get_key(url)] = {
                "result": result,
                "timestamp": now,
                "last_access": now,
            }

    def _evict_lru(self) -> None:
        """M-06: Remove least-recently-used entries to stay under max_entries.
        Evicts 10% of entries at a time to avoid evicting on every insert."""
        evict_count = max(1, self.max_entries // 10)
        # Sort by last_access (oldest first), then remove
        sorted_keys = sorted(
            self._cache.keys(),
            key=lambda k: self._cache[k].get(
                "last_access", self._cache[k]["timestamp"]
            ),
        )
        for key in sorted_keys[:evict_count]:
            del self._cache[key]

    async def has_changed(self, url: str, content_hash: str) -> bool:
        async with self._lock:
            key = self._get_key(url)
            if key in self._cache:
                return self._cache[key]["result"].content_hash != content_hash
            return True

    def get_stats(self) -> Dict[str, int]:
        return {
            "entries": len(self._cache),
            "size_bytes": sum(
                len(str(e["result"].html or "")) for e in self._cache.values()
            ),
        }


class HTTPScraper:
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def initialize(self):
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.default_timeout),
                follow_redirects=True,
                headers={
                    "User-Agent": self.config.user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "Cache-Control": "max-age=0",
                },
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )

    async def scrape(
        self, url: str, extra_headers: Optional[Dict] = None
    ) -> ScrapeResult:
        start_time = time.time()
        await self.initialize()
        try:
            response = await self._client.get(url, headers=extra_headers or {})
            crawl_time = int((time.time() - start_time) * 1000)
            if response.status_code == 200:
                html = response.text
                soup = BeautifulSoup(html, "lxml")
                for s in soup(["script", "style", "nav", "footer", "header"]):
                    s.decompose()
                text = soup.get_text(separator="\n", strip=True)
                title = soup.title.string if soup.title else None
                links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("/"):
                        href = urljoin(url, href)
                    if href.startswith("http"):
                        links.append(href)
                return ScrapeResult(
                    url=url,
                    success=True,
                    html=html,
                    text=text,
                    title=title,
                    links=links[:100],
                    status_code=200,
                    content_hash=hashlib.md5(html.encode()).hexdigest()[:16],
                    crawl_time_ms=crawl_time,
                    crawler_used="httpx",
                )
            return ScrapeResult(
                url=url,
                success=False,
                error=f"HTTP {response.status_code}",
                status_code=response.status_code,
                crawl_time_ms=crawl_time,
                crawler_used="httpx",
            )
        except Exception as e:
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawl_time_ms=int((time.time() - start_time) * 1000),
                crawler_used="httpx",
            )

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None


class PlaywrightScraper:
    """Playwright scraper using sync API in a dedicated background thread.

    On Windows, uvicorn's event loop (SelectorEventLoop) cannot spawn
    subprocesses. The async Playwright API fails with NotImplementedError.
    Solution: use playwright.sync_api in a dedicated thread.

    IMPORTANT: Playwright's sync API uses greenlets pinned to one thread.
    All calls MUST run on the same thread. We use a single-thread
    ThreadPoolExecutor for this.
    """

    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._playwright = None
        self._browser = None
        self._initialized = False
        self._disabled = False
        self._init_lock = asyncio.Lock()
        # Single-thread executor — all Playwright ops run here
        from concurrent.futures import ThreadPoolExecutor

        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="playwright"
        )

    @property
    def available(self) -> bool:
        return self._initialized and not self._disabled

    def _run_in_pw_thread(self, fn, *args):
        """Run a function in the dedicated Playwright thread."""
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(self._executor, fn, *args)

    def _sync_initialize(self):
        """Initialize Playwright (runs in dedicated thread)."""
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

    async def initialize(self):
        if self._initialized or self._disabled:
            return
        async with self._init_lock:
            if self._initialized or self._disabled:
                return
            try:
                await self._run_in_pw_thread(self._sync_initialize)
                self._initialized = True
                logger.info("✅ Playwright browser initialized")
            except BaseException as e:
                self._disabled = True
                logger.warning(f"⚠️ Playwright initialization failed: {e}")
                try:
                    if self._playwright:
                        self._playwright.stop()
                except BaseException:
                    pass
                self._playwright = None
                self._browser = None

    def _sync_scrape(
        self,
        url: str,
        wait_for: Optional[str] = None,
        extra_headers: Optional[Dict] = None,
    ) -> ScrapeResult:
        """Run the actual Playwright scrape (runs in dedicated thread)."""
        start_time = time.time()
        context = page = None
        try:
            context = self._browser.new_context(
                user_agent=self.config.user_agent,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                extra_http_headers=extra_headers or {},
            )
            # Stealth: hide automation flags from anti-bot detection
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            """)
            page = context.new_page()
            try:
                response = page.goto(
                    url,
                    wait_until="networkidle",
                    timeout=self.config.default_timeout * 1000,
                )
            except Exception:
                response = page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.config.default_timeout * 1000,
                )
                page.wait_for_timeout(5000)
            # Auto-dismiss cookie consent
            self._sync_dismiss_cookies(page)
            if wait_for:
                try:
                    page.wait_for_selector(wait_for, timeout=10000)
                except Exception:
                    pass
            import time as _time

            _time.sleep(1)
            html = page.content()
            crawl_time = int((time.time() - start_time) * 1000)
            text = page.inner_text("body")
            soup = BeautifulSoup(html, "lxml")
            title = soup.title.string if soup.title else None
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/"):
                    href = urljoin(url, href)
                if href.startswith("http"):
                    links.append(href)
            return ScrapeResult(
                url=url,
                success=True,
                html=html,
                text=text,
                title=title,
                links=links[:100],
                status_code=response.status if response else None,
                content_hash=hashlib.md5(html.encode()).hexdigest()[:16],
                crawl_time_ms=crawl_time,
                crawler_used="playwright",
            )
        except Exception as e:
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawl_time_ms=int((time.time() - start_time) * 1000),
                crawler_used="playwright",
            )
        finally:
            if page:
                page.close()
            if context:
                context.close()

    def _sync_dismiss_cookies(self, page):
        """Try to dismiss cookie consent banners."""
        COOKIE_SELECTORS = [
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "#CybotCookiebotDialogBodyButtonAccept",
            "#onetrust-accept-btn-handler",
            "button:has-text('Allow all')",
            "button:has-text('Accept all')",
            "button:has-text('Accept All')",
            "button:has-text('Accept cookies')",
            "button:has-text('Accept Cookies')",
            "button:has-text('I agree')",
            "button:has-text('I Accept')",
            "button:has-text('Got it')",
            "button:has-text('Agree')",
            "button:has-text('Allow All')",
            "button:has-text('Consent')",
            "[id*='cookie'] button:has-text('Accept')",
            "[id*='consent'] button:has-text('Accept')",
            "[class*='cookie'] button:has-text('Accept')",
            "[class*='consent'] button:has-text('Accept')",
            ".cookie-consent-accept",
            "[data-action='accept']",
            "[aria-label*='accept' i]",
        ]
        try:
            for selector in COOKIE_SELECTORS:
                try:
                    btn = page.locator(selector).first
                    if btn.is_visible(timeout=2000):
                        btn.click()
                        logger.info(f"\U0001f36a Cookie dismissed via: {selector}")
                        page.wait_for_timeout(2000)
                        return
                except Exception:
                    continue
        except Exception:
            pass

    async def scrape(
        self,
        url: str,
        wait_for: Optional[str] = None,
        extra_headers: Optional[Dict] = None,
    ) -> ScrapeResult:
        if self._disabled:
            return ScrapeResult(
                url=url,
                success=False,
                error="Playwright not available on this platform",
                crawler_used="playwright",
            )
        await self.initialize()
        return await self._run_in_pw_thread(
            self._sync_scrape, url, wait_for, extra_headers
        )

    async def close(self):
        def _sync_close():
            if self._browser:
                try:
                    self._browser.close()
                except Exception:
                    pass
            if self._playwright:
                try:
                    self._playwright.stop()
                except Exception:
                    pass

        try:
            await self._run_in_pw_thread(_sync_close)
        except BaseException:
            pass
        self._initialized = False
        self._executor.shutdown(wait=False)


class Crawl4AIScraper:
    """Crawl4AI scraper in a dedicated background thread.

    Crawl4AI uses AsyncWebCrawler which internally launches Playwright.
    On Windows/uvicorn, subprocess_exec fails. Solution: run Crawl4AI
    in a dedicated thread with its own ProactorEventLoop.
    """

    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._crawler = None
        self._initialized = False
        self._disabled = False
        self._init_lock = asyncio.Lock()
        self._loop = None  # Dedicated event loop in the thread
        from concurrent.futures import ThreadPoolExecutor

        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="crawl4ai"
        )

    @property
    def available(self) -> bool:
        return self._initialized and not self._disabled

    def _run_in_thread(self, fn, *args):
        """Run a function in the dedicated Crawl4AI thread."""
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(self._executor, fn, *args)

    def _sync_initialize(self):
        """Initialize Crawl4AI with its own event loop in a dedicated thread."""
        import asyncio as _aio

        if sys.platform == "win32":
            _aio.set_event_loop_policy(_aio.WindowsProactorEventLoopPolicy())
        self._loop = _aio.new_event_loop()
        _aio.set_event_loop(self._loop)
        from crawl4ai import AsyncWebCrawler

        self._crawler = AsyncWebCrawler(verbose=False)
        self._loop.run_until_complete(self._crawler.start())

    async def initialize(self):
        if self._initialized or self._disabled:
            return
        async with self._init_lock:
            if self._initialized or self._disabled:
                return
            try:
                await self._run_in_thread(self._sync_initialize)
                self._initialized = True
                logger.info("✅ Crawl4AI initialized")
            except ImportError:
                self._disabled = True
                logger.warning("⚠️ Crawl4AI not installed. Run: pip install crawl4ai")
            except BaseException as e:
                self._disabled = True
                logger.warning(f"⚠️ Failed to initialize Crawl4AI: {e}")

    def _sync_scrape(self, url: str, bypass_cache: bool = False) -> ScrapeResult:
        """Run Crawl4AI scrape in the dedicated thread."""
        start_time = time.time()
        try:
            result = self._loop.run_until_complete(
                self._crawler.arun(url=url, bypass_cache=bypass_cache)
            )
            crawl_time = int((time.time() - start_time) * 1000)
            if result.success:
                text = (
                    result.markdown
                    if hasattr(result, "markdown")
                    else result.extracted_content
                )
                html = result.html if hasattr(result, "html") else ""
                title = None
                if hasattr(result, "metadata") and result.metadata:
                    title = result.metadata.get("title")
                links = []
                if hasattr(result, "links") and result.links:
                    raw_links = result.links
                    if isinstance(raw_links, dict):
                        raw_links = raw_links.get("internal", []) + raw_links.get(
                            "external", []
                        )
                    for link in raw_links:
                        if isinstance(link, str):
                            links.append(link)
                        elif isinstance(link, dict):
                            href = (
                                link.get("href") or link.get("url") or link.get("link")
                            )
                            if href:
                                links.append(href)
                return ScrapeResult(
                    url=url,
                    success=True,
                    html=html,
                    text=text,
                    title=title,
                    links=links[:100],
                    status_code=200,
                    content_hash=hashlib.md5((text or "").encode()).hexdigest()[:16],
                    crawl_time_ms=crawl_time,
                    crawler_used="crawl4ai",
                )
            return ScrapeResult(
                url=url,
                success=False,
                error=result.error_message
                if hasattr(result, "error_message")
                else "Unknown error",
                crawl_time_ms=crawl_time,
                crawler_used="crawl4ai",
            )
        except Exception as e:
            return ScrapeResult(
                url=url,
                success=False,
                error=str(e),
                crawl_time_ms=int((time.time() - start_time) * 1000),
                crawler_used="crawl4ai",
            )

    async def scrape(self, url: str, bypass_cache: bool = False) -> ScrapeResult:
        if self._disabled:
            return ScrapeResult(
                url=url,
                success=False,
                error="Crawl4AI not available on this platform",
                crawler_used="crawl4ai",
            )
        if not self._initialized:
            await self.initialize()
        return await self._run_in_thread(self._sync_scrape, url, bypass_cache)

    async def close(self):
        def _sync_close():
            if self._crawler and self._loop:
                try:
                    self._loop.run_until_complete(self._crawler.close())
                except Exception:
                    pass
            if self._loop:
                try:
                    self._loop.close()
                except Exception:
                    pass

        try:
            await self._run_in_thread(_sync_close)
        except BaseException:
            pass
        self._crawler = None
        self._initialized = False
        self._executor.shutdown(wait=False)


class DeepCrawler:
    def __init__(
        self,
        http_scraper,
        playwright_scraper,
        rate_limiter,
        cache,
        crawl4ai_scraper=None,
    ):
        self.http = http_scraper
        self.playwright = playwright_scraper
        self.crawl4ai = crawl4ai_scraper
        self.rate_limiter = rate_limiter
        self.cache = cache
        self.url_filter = URLFilter()
        self._domain_scraper_memory: Dict[str, str] = {}
        self._filter_stats = {
            "urls_discovered": 0,
            "urls_blocked": 0,
            "urls_allowed": 0,
        }

    def _should_follow_link(self, url: str, source: SourceConfig) -> bool:
        url_lower = url.lower()
        for pattern in source.block_patterns:
            if re.search(pattern, url_lower):
                return False
        if source.gold_patterns:
            return any(re.search(p, url_lower) for p in source.gold_patterns)
        if source.link_patterns:
            return any(re.search(p, url_lower) for p in source.link_patterns)
        return True

    def _filter_links(self, links: List, source: SourceConfig) -> List[str]:
        clean = []
        for link in links:
            if isinstance(link, str):
                clean.append(link)
            elif isinstance(link, dict):
                href = link.get("href") or link.get("url") or link.get("link")
                if href and isinstance(href, str):
                    clean.append(href)
        self._filter_stats["urls_discovered"] += len(clean)
        pattern_filtered = [
            lnk for lnk in clean if self._should_follow_link(lnk, source)
        ]
        filtered = self.url_filter.filter_urls(pattern_filtered, source.url)
        self._filter_stats["urls_allowed"] += len(filtered)
        self._filter_stats["urls_blocked"] += len(clean) - len(filtered)
        return filtered

    async def crawl(
        self, source: SourceConfig, max_depth: int = 2, max_pages: int = 50
    ) -> List[ScrapeResult]:
        results: List[ScrapeResult] = []
        visited: Set[str] = set()
        to_visit: deque = deque([(source.url, 0)])
        if hasattr(source, "additional_urls") and source.additional_urls:
            for u in source.additional_urls:
                to_visit.append((u, 0))
        self._filter_stats = {
            "urls_discovered": 0,
            "urls_blocked": 0,
            "urls_allowed": 0,
        }

        while to_visit and len(results) < max_pages:
            url, depth = to_visit.popleft()
            if not isinstance(url, str):
                url = (
                    url.get("href") or url.get("url") or url.get("link", "")
                    if isinstance(url, dict)
                    else ""
                )
            if not url:
                continue
            url_norm = url.lower().rstrip("/")
            if url_norm in visited:
                continue
            visited.add(url_norm)
            if depth > 0 and not self._should_follow_link(url, source):
                continue
            fr = self.url_filter.should_scrape(url, source.url)
            if not fr.should_scrape:
                continue

            cached = await self.cache.get(url)
            if cached:
                results.append(cached)
                if depth < max_depth and source.follow_links:
                    for link in self._filter_links(cached.links, source):
                        ln = link.lower().rstrip("/")
                        if ln not in visited:
                            to_visit.append((link, depth + 1))
                continue

            await self.rate_limiter.acquire(url)
            result = await self._smart_scrape(url, source)
            results.append(result)
            if result.success:
                await self.cache.set(url, result)
                logger.info(
                    f"✅ Scraped: {url[:60]}... ({result.crawl_time_ms}ms) [{result.crawler_used}]"
                )
                if depth < max_depth and source.follow_links:
                    for link in self._filter_links(result.links, source):
                        ln = link.lower().rstrip("/")
                        if ln not in visited:
                            to_visit.append((link, depth + 1))
            else:
                logger.warning(f"❌ Failed: {url[:60]}... - {result.error}")

        s = self._filter_stats
        logger.info(
            f"📊 URL Filter: {s['urls_discovered']} discovered, {s['urls_allowed']} allowed, {s['urls_blocked']} blocked"
        )
        return results

    def get_filter_stats(self) -> Dict[str, int]:
        return self._filter_stats.copy()

    def _playwright_available(self) -> bool:
        return hasattr(self.playwright, "available") and self.playwright.available

    def _crawl4ai_available(self) -> bool:
        return (
            self.crawl4ai is not None
            and hasattr(self.crawl4ai, "available")
            and self.crawl4ai.available
        )

    async def _smart_scrape(self, url: str, source: SourceConfig) -> ScrapeResult:
        domain = self._get_domain(url)
        if domain in self._domain_scraper_memory:
            remembered = self._domain_scraper_memory[domain]
            if remembered == "playwright" and not self._playwright_available():
                del self._domain_scraper_memory[domain]
            elif remembered == "crawl4ai" and not self._crawl4ai_available():
                del self._domain_scraper_memory[domain]
            else:
                result = await self._scrape_with(url, remembered, source)
                if result.success:
                    return result
                del self._domain_scraper_memory[domain]

        if source.crawler_type == CrawlerType.CRAWL4AI and self._crawl4ai_available():
            result = await self._scrape_with(url, "crawl4ai", source)
            if result.success:
                self._domain_scraper_memory[domain] = "crawl4ai"
                return result
        elif (
            source.crawler_type == CrawlerType.PLAYWRIGHT
            and self._playwright_available()
        ):
            result = await self._scrape_with(url, "playwright", source)
            if result.success:
                self._domain_scraper_memory[domain] = "playwright"
                return result
        elif source.crawler_type == CrawlerType.HTTPX:
            result = await self._scrape_with(url, "httpx", source)
            if result.success:
                self._domain_scraper_memory[domain] = "httpx"
                return result

        result = await self._scrape_with(url, "httpx", source)
        if result.success and not self._looks_like_blocked(result):
            self._domain_scraper_memory[domain] = "httpx"
            return result

        if (
            self._is_blocked(result)
            or self._needs_js(result)
            or self._looks_like_blocked(result)
            or not result.success
        ):
            if self._crawl4ai_available():
                result = await self._scrape_with(url, "crawl4ai", source)
                if result.success and not self._looks_like_blocked(result):
                    self._domain_scraper_memory[domain] = "crawl4ai"
                    return result
            if self._playwright_available():
                result = await self._scrape_with(url, "playwright", source)
                if result.success:
                    self._domain_scraper_memory[domain] = "playwright"
                    return result
        return result

    async def _scrape_with(
        self, url: str, scraper: str, source: SourceConfig
    ) -> ScrapeResult:
        try:
            if scraper == "crawl4ai" and self._crawl4ai_available():
                return await self.crawl4ai.scrape(url)
            elif scraper == "playwright" and self._playwright_available():
                return await self.playwright.scrape(
                    url,
                    wait_for=source.wait_for_selector,
                    extra_headers=source.extra_headers,
                )
            elif scraper in ("crawl4ai", "playwright"):
                return ScrapeResult(
                    url=url,
                    success=False,
                    error=f"{scraper} not available",
                    crawler_used=scraper,
                )
            else:
                return await self.http.scrape(url, extra_headers=source.extra_headers)
        except Exception as e:
            return ScrapeResult(
                url=url, success=False, error=str(e), crawler_used=scraper
            )

    def _is_blocked(self, r: ScrapeResult) -> bool:
        if not r.error:
            return False
        el = r.error.lower()
        return any(
            x in el
            for x in [
                "403",
                "forbidden",
                "401",
                "429",
                "blocked",
                "denied",
                "captcha",
                "cloudflare",
                "bot detected",
            ]
        )

    def _looks_like_blocked(self, r: ScrapeResult) -> bool:
        if not r.success or not r.text:
            return False
        if len(r.text.strip()) < 200:
            return True
        tl = r.text.lower()
        return any(
            x in tl
            for x in [
                "access denied",
                "verify you are human",
                "checking your browser",
                "ray id",
                "captcha",
                "ddos protection",
            ]
        )

    def _needs_js(self, r: ScrapeResult) -> bool:
        if r.success and r.text:
            if len(r.text) < 500:
                return True
            tl = r.text.lower()
            return any(
                x in tl
                for x in [
                    "enable javascript",
                    "javascript required",
                    "loading...",
                    "react-root",
                    "__next",
                ]
            )
        return False

    def _get_domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return url


class ScrapingEngine:
    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.config = config or ScrapingConfig()
        self.rate_limiter = RateLimiter(self.config)
        self.cache = ContentCache(self.config.cache_ttl_hours)
        self.http_scraper = HTTPScraper(self.config)
        self.playwright_scraper = PlaywrightScraper(self.config)
        self.crawl4ai_scraper = None
        self.deep_crawler = None
        self._sources: Dict[str, SourceConfig] = {}
        self._initialized = False
        self._stats = {
            "total_scraped": 0,
            "successful": 0,
            "failed": 0,
            "cached_hits": 0,
            "total_time_ms": 0,
            "urls_filtered": 0,
        }

    async def initialize(self):
        if self._initialized:
            return
        logger.info("🚀 Initializing Scraping Engine V3...")
        await self.http_scraper.initialize()

        # Playwright now uses sync API in a background thread (asyncio.to_thread)
        # so it works on all platforms including Windows/uvicorn.
        try:
            await self.playwright_scraper.initialize()
        except BaseException as e:
            logger.warning(f"⚠️ Playwright not available: {e}")

        # Crawl4AI now runs in a dedicated thread with its own ProactorEventLoop
        try:
            self.crawl4ai_scraper = Crawl4AIScraper(self.config)
            await self.crawl4ai_scraper.initialize()
        except ImportError:
            self.crawl4ai_scraper = None
            logger.warning("⚠️ Crawl4AI not installed")
        except BaseException as e:
            self.crawl4ai_scraper = None
            logger.warning(f"⚠️ Crawl4AI not available: {e}")

        available_scrapers = ["httpx"]
        if self.playwright_scraper.available:
            available_scrapers.append("playwright")
        if self.crawl4ai_scraper and self.crawl4ai_scraper.available:
            available_scrapers.append("crawl4ai")
        else:
            self.crawl4ai_scraper = None

        self.deep_crawler = DeepCrawler(
            self.http_scraper,
            self.playwright_scraper,
            self.rate_limiter,
            self.cache,
            self.crawl4ai_scraper,
        )
        await self._load_sources()
        self._initialized = True
        logger.info(
            f"✅ Engine initialized with {len(self._sources)} sources "
            f"(scrapers: {', '.join(available_scrapers)})"
        )

    async def _load_sources(self):
        sources_loaded = 0
        try:
            from app.database import async_session
            from app.models.source import Source
            from sqlalchemy import select

            async with async_session() as db:
                result = await db.execute(select(Source).where(Source.is_active))
                db_sources = result.scalars().all()
            for src in db_sources:
                patterns = get_patterns_with_default(src.name)
                crawler = (
                    CrawlerType.PLAYWRIGHT if src.use_playwright else CrawlerType.AUTO
                )
                additional_urls = []
                if hasattr(src, "entry_urls") and src.entry_urls:
                    additional_urls = [u for u in src.entry_urls if u != src.base_url]
                self._sources[src.name] = SourceConfig(
                    name=src.name,
                    url=src.base_url,
                    additional_urls=additional_urls,
                    crawler_type=crawler,
                    priority=src.priority or 5,
                    follow_links=True,
                    max_depth=src.max_depth or 2,
                    max_pages=patterns.max_pages
                    if patterns
                    else (src.max_depth * 20 if src.max_depth else 50),
                    gold_patterns=patterns.gold_patterns if patterns else [],
                    block_patterns=patterns.block_patterns if patterns else [],
                    link_patterns=patterns.link_patterns if patterns else [],
                )
                sources_loaded += 1
            logger.info(f"✅ Loaded {sources_loaded} sources from DATABASE")
            no_patterns = [n for n in self._sources if not has_patterns(n)]
            if no_patterns:
                logger.warning(
                    f"⚠️ {len(no_patterns)} sources missing patterns: {no_patterns[:5]}..."
                )
            return
        except ImportError as e:
            logger.warning(f"⚠️ Database not available: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to load from database: {e}")
        self._add_default_sources()

    def _add_default_sources(self):
        for s in [
            SourceConfig(
                name="Hospitality Net",
                url="https://www.hospitalitynet.org/news/global.html",
                crawler_type=CrawlerType.HTTPX,
                priority=8,
                max_pages=30,
                gold_patterns=[r"/announcement/\d+/", r"/news/\d+\.html"],
                block_patterns=[r"/organization/", r"/opinion/", r"/video/"],
            ),
            SourceConfig(
                name="Hotel Dive",
                url="https://www.hoteldive.com/topic/development/",
                crawler_type=CrawlerType.HTTPX,
                priority=10,
                max_pages=50,
                gold_patterns=[r"/news/[a-z0-9-]+/\d+/"],
                block_patterns=[r"/selfservice/", r"/library/", r"/events/"],
            ),
            SourceConfig(
                name="Caribbean Journal",
                url="https://www.caribjournal.com/category/hotels/",
                crawler_type=CrawlerType.HTTPX,
                priority=10,
                max_pages=50,
                gold_patterns=[
                    r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel",
                    r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-resort",
                ],
                block_patterns=[r"/tag/", r"/author/", r"-cruise"],
            ),
        ]:
            self._sources[s.name] = s

    async def scrape_source(
        self, source_name: str, deep: bool = True
    ) -> List[ScrapeResult]:
        if source_name not in self._sources:
            logger.error(f"Source not found: {source_name}")
            return []
        source = self._sources[source_name]
        logger.info(f"🔍 Scraping: {source.name}")
        t0 = time.time()
        if deep:
            results = await self.deep_crawler.crawl(
                source, max_depth=source.max_depth, max_pages=source.max_pages
            )
        else:
            await self.rate_limiter.acquire(source.url)
            results = [
                await (
                    self.playwright_scraper.scrape(
                        source.url, wait_for=source.wait_for_selector
                    )
                    if source.crawler_type == CrawlerType.PLAYWRIGHT
                    else self.http_scraper.scrape(source.url)
                )
            ]
        elapsed = int((time.time() - t0) * 1000)
        self._stats["total_scraped"] += len(results)
        self._stats["successful"] += sum(1 for r in results if r.success)
        self._stats["failed"] += sum(1 for r in results if not r.success)
        self._stats["cached_hits"] += sum(1 for r in results if r.is_cached)
        self._stats["total_time_ms"] += elapsed
        self._stats["urls_filtered"] += self.deep_crawler.get_filter_stats().get(
            "urls_blocked", 0
        )
        logger.info(
            f"✅ {source.name}: {len(results)} pages, {sum(1 for r in results if r.success)} success, {elapsed}ms"
        )
        return results

    async def scrape_sources(
        self, source_names: List[str], deep: bool = True, max_concurrent: int = 5
    ) -> Dict[str, List[ScrapeResult]]:
        sem = asyncio.Semaphore(max_concurrent)
        results: Dict[str, List[ScrapeResult]] = {}

        async def _go(name):
            async with sem:
                return name, await self.scrape_source(name, deep=deep)

        for item in await asyncio.gather(
            *[_go(n) for n in source_names], return_exceptions=True
        ):
            if isinstance(item, Exception):
                logger.error(f"Source failed: {item}")
            else:
                results[item[0]] = item[1]
        return results

    async def scrape_all_sources(
        self, deep=True, max_concurrent=5, priority_threshold=7
    ):
        names = sorted(
            [n for n, c in self._sources.items() if c.priority >= priority_threshold],
            key=lambda x: self._sources[x].priority,
            reverse=True,
        )
        logger.info(
            f"📋 Scraping {len(names)} sources (priority >= {priority_threshold})"
        )
        return await self.scrape_sources(
            names, deep=deep, max_concurrent=max_concurrent
        )

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "sources_loaded": len(self._sources),
            "cache_stats": self.cache.get_stats(),
            "avg_time_per_page": self._stats["total_time_ms"]
            / max(1, self._stats["total_scraped"]),
            "url_filter_stats": self.deep_crawler.url_filter.get_stats()
            if self.deep_crawler
            else {},
        }

    def list_sources(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": c.name,
                "url": c.url,
                "priority": c.priority,
                "crawler": c.crawler_type.value,
                "frequency": c.frequency,
                "has_patterns": bool(c.gold_patterns or c.block_patterns),
            }
            for c in sorted(self._sources.values(), key=lambda x: -x.priority)
        ]

    async def close(self):
        await self.http_scraper.close()
        await self.playwright_scraper.close()
        if self.crawl4ai_scraper:
            await self.crawl4ai_scraper.close()
        logger.info("🔒 Scraping engine closed")


async def main():
    engine = ScrapingEngine()
    try:
        await engine.initialize()
        test_sources = list(engine._sources.keys())[:3]
        results = await engine.scrape_sources(test_sources, deep=False)
        for name, sr in results.items():
            for r in sr:
                if r.success:
                    print(f"✅ {name}: {r.url[:60]}... ({len(r.text or '')} chars)")
                else:
                    print(f"❌ {name}: {r.error}")
        print(f"\nStats: {engine.get_stats()}")
    finally:
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
