"""
🌐 WEB DISCOVERY ENGINE v5.1
============================
Discovers new hotel news sources AND extracts leads from discovered articles.

Uses the SAME IntelligentPipeline as the scraper for AI classification —
no separate Gemini validator, no duplicate AI calls, proven accuracy.

Flow:
  Phase 1:  Search (DuckDuckGo + Google News RSS)
  Phase 2:  Filter known sources + blacklisted + failed domains
  Phase 3:  Signal-test candidates (regex patterns, no AI cost)
  Phase 3b: Fetch actual article pages from qualified domains
  Phase 4:  Pipeline classification + extraction (Gemini Flash + Pro)
  Phase 5:  Save recurring sources + extracted leads

Usage:
    python discover_sources.py --dry-run              # Preview sources + leads
    python discover_sources.py --dry-run --queries 5  # Quick test
    python discover_sources.py                        # Full run, add to DB
    python discover_sources.py --min-quality 60       # Higher quality bar
    python discover_sources.py --sources-only         # Skip lead extraction
    python discover_sources.py --skip-queries 10      # Resume from query 11
"""

import asyncio
import argparse
import base64
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, quote_plus, urljoin

import httpx
from sqlalchemy import select

from app.database import async_session
from app.models import Source
from app.models.failed_domain import FailedDomain
from duckduckgo_search import DDGS

logger = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH QUERIES
# ═══════════════════════════════════════════════════════════════════════════════

SEARCH_QUERIES = [
    # ── Core (wide net) ──
    "new hotel opening 2026 United States",
    "new luxury hotel opening 2026 USA",
    "new hotel opening 2027 United States",
    "new resort opening 2026 2027",
    # ── Brand-Specific (luxury + upper-upscale) ──
    "Hilton new hotel opening 2026 2027",
    "Marriott new hotel resort opening 2026",
    "Hyatt new hotel opening 2026 2027",
    "Four Seasons new hotel 2026 2027",
    "IHG new hotel opening USA 2026",
    "Ritz-Carlton Waldorf Astoria new hotel 2026",
    "Rosewood Montage Peninsula hotel opening 2026",
    "Accor Fairmont Sofitel new hotel Americas 2026",
    # ── Major US Markets (by region) ──
    # Southeast
    "new hotel Miami Fort Lauderdale 2026",
    "new hotel Orlando resort opening 2026",
    "new hotel Palm Beach Naples Florida 2026",
    "new hotel Atlanta Nashville Charlotte 2026",
    # Northeast
    "new hotel opening New York City 2026",
    "new hotel Boston Washington DC Philadelphia 2026",
    # West
    "new hotel opening Los Angeles San Diego 2026",
    "new hotel San Francisco Seattle Portland 2026",
    "new hotel Las Vegas Phoenix Scottsdale 2026",
    "new hotel resort Hawaii 2026",
    # South / Central
    "new hotel Dallas Houston Austin Texas 2026",
    "new hotel Chicago Denver 2026",
    "new hotel New Orleans Charleston Savannah 2026",
    # ── Caribbean & Latin America ──
    "new resort Dominican Republic Punta Cana 2026",
    "new hotel Bahamas opening 2026 2027",
    "new hotel Aruba Cayman Islands 2026",
    "new resort Jamaica Turks Caicos 2026",
    "new hotel Puerto Rico Costa Rica 2026",
    # ── Renovations & Conversions (new uniforms!) ──
    "hotel renovation rebranding conversion 2026",
    "hotel flag change conversion USA 2026",
    # ── Industry Pipeline ──
    "hotel groundbreaking construction announcement 2026",
    "all inclusive resort opening 2026 2027",
    "upper upscale hotel opening announcement 2026",
]


# ═══════════════════════════════════════════════════════════════════════════════
# BLACKLIST
# ═══════════════════════════════════════════════════════════════════════════════

BLACKLISTED_DOMAINS = {
    # Search engines
    "google.com",
    "google.co",
    "google.co.uk",
    "bing.com",
    "yahoo.com",
    "duckduckgo.com",
    "googleapis.com",
    "gstatic.com",
    "maps.google.com",
    "news.google.com",
    # Social media
    "facebook.com",
    "twitter.com",
    "x.com",
    "instagram.com",
    "threads.net",
    "linkedin.com",
    "reddit.com",
    "youtube.com",
    "tiktok.com",
    "pinterest.com",
    # Booking & OTAs
    "booking.com",
    "expedia.com",
    "hotels.com",
    "tripadvisor.com",
    "kayak.com",
    "trivago.com",
    "agoda.com",
    "priceline.com",
    "orbitz.com",
    # Reference
    "wikipedia.org",
    "wikimedia.org",
    "wikidata.org",
    "britannica.com",
    # Aggregators
    "apple.news",
    "msn.com",
    "news.yahoo.com",
    "flipboard.com",
    # E-commerce
    "amazon.com",
    "ebay.com",
    "walmart.com",
    "target.com",
    # Job boards
    "indeed.com",
    "glassdoor.com",
    "ziprecruiter.com",
    # Real estate
    "zillow.com",
    "realtor.com",
    # File sharing
    "docs.google.com",
    "drive.google.com",
    "dropbox.com",
    "scribd.com",
    "slideshare.net",
    "academia.edu",
    # General news (too broad, rarely produce actionable hotel leads)
    "cnn.com",
    "foxnews.com",
    "nytimes.com",
    "washingtonpost.com",
    "usatoday.com",
    "reuters.com",
    "apnews.com",
    "nbcnews.com",
    "abcnews.go.com",
    "cbsnews.com",
    "huffpost.com",
    # Government
    "sec.gov",
    "fda.gov",
    "irs.gov",
    "state.gov",
    # Fashion / lifestyle noise
    "vogue.com",
    "elledecor.com",
    "veranda.com",
    "harpersbazaar.com",
    "cosmopolitan.com",
    "elle.com",
    "wmagazine.com",
    "instyle.com",
    "nationalgeographic.com",
    # Theme parks / entertainment
    "disney.com",
    "universalstudios.com",
    "insidethemagic.net",
    "wdw-magazine.com",
    # Airlines
    "united.com",
    "delta.com",
    "aa.com",
    "southwest.com",
    "cathaypacific.com",
    # Low-value local news
    "wesh.com",
    "wcjb.com",
    "visitorlando.com",
    "orlandodatenightguide.com",
    "dayton247now.com",
    "theledger.com",
    # Theme park blogs (not hotel sources)
    "disneytouristblog.com",
    "blogmickey.com",
    "wdwnt.com",
    "dapsmagic.com",
}

BLACKLIST_SUBSTRINGS = [".gov", ".edu", ".mil"]

HIGH_VALUE_KEYWORDS = [
    "hotel",
    "hospitality",
    "lodging",
    "resort",
    "hotelier",
    "hotelmanagement",
    "hotelbusiness",
    "hospitalitynet",
    "hospitalitydesign",
    "costar",
    "htrends",
    "hotelnews",
]


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE TYPE CLASSIFICATION RULES
# ═══════════════════════════════════════════════════════════════════════════════

SOURCE_TYPE_RULES = {
    "chain_newsroom": {
        "domain_kw": [
            "hilton",
            "marriott",
            "hyatt",
            "ihg",
            "accor",
            "wyndham",
            "choicehotels",
        ],
        "content_kw": ["press release", "newsroom", "media center"],
    },
    "luxury_independent": {
        "domain_kw": [
            "fourseasons",
            "ritzcarlton",
            "waldorf",
            "stregis",
            "rosewood",
            "aman",
            "peninsula",
        ],
        "content_kw": ["luxury", "ultra luxury", "boutique hotel"],
    },
    "florida": {
        "domain_kw": [
            "florida",
            "miami",
            "orlando",
            "tampa",
            "southflorida",
            "palmbeach",
        ],
        "content_kw": [
            "florida hotel",
            "miami hotel",
            "orlando resort",
            "fort lauderdale",
        ],
    },
    "caribbean": {
        "domain_kw": ["caribbean", "bahamas", "jamaica", "aruba", "cayman", "bermuda"],
        "content_kw": ["caribbean hotel", "island resort", "caribbean tourism"],
    },
    "industry": {
        "domain_kw": ["construction", "development", "pipeline"],
        "content_kw": ["construction", "development pipeline", "groundbreaking"],
    },
    "aggregator": {
        "domain_kw": ["hotelnews", "hospitality", "lodging", "hotelier", "costar"],
        "content_kw": ["hotel news", "hospitality news", "hotel industry"],
    },
    "travel_pub": {
        "domain_kw": ["travel", "tourism", "leisure", "cntraveler", "travelpulse"],
        "content_kw": ["travel news", "new hotel", "hotel review"],
    },
    "pr_wire": {
        "domain_kw": ["prnewswire", "businesswire", "globenewswire"],
        "content_kw": ["press release", "for immediate release"],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# HTML CLEANING UTILITY
# ═══════════════════════════════════════════════════════════════════════════════


def clean_html_to_text(html: str) -> str:
    """Strip HTML to clean text. Used by both DomainTester and pipeline wrapper."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(
        [
            "script",
            "style",
            "nav",
            "footer",
            "header",
            "noscript",
            "svg",
            "iframe",
            "aside",
        ]
    ):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH BACKENDS
# ═══════════════════════════════════════════════════════════════════════════════


class SearchBackend:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=20,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    async def search(self, query: str) -> list[dict]:
        raise NotImplementedError

    @staticmethod
    def clean_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    async def close(self):
        await self.client.aclose()


class DuckDuckGoSearch(SearchBackend):
    """DuckDuckGo via duckduckgo-search library (handles anti-bot internally)."""

    async def search(self, query: str) -> list[dict]:
        results = []
        seen = set()
        try:
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=15):
                    url = r.get("href", "")
                    if not url:
                        continue
                    domain = self.clean_domain(url)
                    if not domain or domain in seen:
                        continue
                    seen.add(domain)
                    results.append(
                        {"url": url, "domain": domain, "engine": "duckduckgo"}
                    )
        except Exception as e:
            logger.warning(f"DuckDuckGo error: {e}")
        return results[:15]


class GoogleNewsRSS(SearchBackend):
    """Google News RSS — parse XML feed."""

    @staticmethod
    def _decode_gnews_url(gnews_url: str) -> str:
        if "/rss/articles/" not in gnews_url:
            return ""
        try:
            encoded = gnews_url.split("/rss/articles/")[-1].split("?")[0]
            padded = encoded + "=" * (4 - len(encoded) % 4)
            decoded = base64.urlsafe_b64decode(padded)
            decoded_str = decoded.decode("utf-8", errors="ignore")
            match = re.search(r'https?://[^\s\x00-\x1f"]+', decoded_str)
            if match:
                url = match.group(0).rstrip("/")
                if urlparse(url).path.strip("/"):
                    return url
        except Exception:
            pass
        return ""

    async def search(self, query: str) -> list[dict]:
        results = []
        seen = set()
        try:
            rss_url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
            resp = await self.client.get(rss_url)
            if resp.status_code != 200:
                return results

            root = ET.fromstring(resp.text)
            for item in root.findall(".//item")[:25]:
                title = item.findtext("title", "")
                gnews_link = item.findtext("link", "")
                source_el = item.find("source")
                source_url = source_el.get("url", "") if source_el is not None else ""
                source_name = source_el.text if source_el is not None else ""

                article_url = self._decode_gnews_url(gnews_link)
                if not article_url and source_url.startswith("http"):
                    article_url = source_url

                if not article_url:
                    continue

                domain = self.clean_domain(article_url)
                if domain in seen:
                    continue
                seen.add(domain)

                results.append(
                    {
                        "url": article_url,
                        "domain": domain,
                        "engine": "google_news",
                        "title": title,
                        "source_name": source_name,
                        "gnews_link": gnews_link,
                    }
                )

        except ET.ParseError:
            logger.warning("Google News RSS parse error")
        except Exception as e:
            logger.warning(f"Google News RSS error: {e}")

        return results


# ═══════════════════════════════════════════════════════════════════════════════
# DOMAIN TESTER — signal detection, recurring detection, classification
# ═══════════════════════════════════════════════════════════════════════════════


class DomainTester:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=15,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            },
        )
        self._crawler = None
        self._crawl4ai_available = None
        self._crawl4ai_loop = None  # Dedicated event loop for Crawl4AI thread
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="discovery-crawl4ai")

    def _run_in_thread(self, fn, *args):
        """Run a function in the dedicated Crawl4AI thread."""
        import asyncio as _aio
        loop = _aio.get_event_loop()
        return loop.run_in_executor(self._executor, fn, *args)

    def _sync_init_crawl4ai(self):
        """Initialize Crawl4AI in dedicated thread with own ProactorEventLoop."""
        import asyncio as _aio
        import sys
        if sys.platform == "win32":
            _aio.set_event_loop_policy(_aio.WindowsProactorEventLoopPolicy())
        self._crawl4ai_loop = _aio.new_event_loop()
        _aio.set_event_loop(self._crawl4ai_loop)
        from crawl4ai import AsyncWebCrawler
        self._crawler = AsyncWebCrawler(verbose=False)
        self._crawl4ai_loop.run_until_complete(self._crawler.start())

    async def _init_crawl4ai(self):
        if self._crawl4ai_available is not None:
            return self._crawl4ai_available
        try:
            await self._run_in_thread(self._sync_init_crawl4ai)
            self._crawl4ai_available = True
            logger.info("  🌐 Crawl4AI ready for JS-heavy sites")
        except ImportError:
            logger.warning("  ⚠️ Crawl4AI not installed")
            self._crawl4ai_available = False
        except Exception as e:
            logger.debug(f"Crawl4AI not available: {e}")
            self._crawl4ai_available = False
        return self._crawl4ai_available

    def _sync_fetch_crawl4ai(self, url: str) -> Optional[str]:
        """Fetch page with Crawl4AI in dedicated thread."""
        try:
            result = self._crawl4ai_loop.run_until_complete(
                self._crawler.arun(url=url)
            )
            if result and result.markdown:
                return result.markdown
        except Exception as e:
            logger.debug(f"Crawl4AI failed for {url}: {e}")
        return None

    async def _fetch_with_crawl4ai(self, url: str) -> Optional[str]:
        if not self._crawl4ai_available:
            if not await self._init_crawl4ai():
                return None
        return await self._run_in_thread(self._sync_fetch_crawl4ai, url)

    async def test(self, url: str, domain: str) -> dict:
        result = {
            "url": url,
            "domain": domain,
            "reachable": False,
            "has_hotel_content": False,
            "signals": [],
            "signal_count": 0,
            "article_links": 0,
            "hotel_articles": 0,
            "has_recent_content": False,
            "quality_score": 0,
            "suggested_type": "aggregator",
            "suggested_priority": 5,
            "sample_articles": [],
            "homepage_url": "",
            "page_html": "",
            "page_text": "",
            "is_recurring": False,
            "recurring_reason": "",
            "unique_hotel_articles": 0,
        }

        # Step 1: Fetch URL
        page_content = ""
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                return result
            result["reachable"] = True
            page_content = resp.text
        except Exception as e:
            logger.debug(f"Cannot reach {domain}: {e}")
            return result

        result["page_html"] = page_content

        # Step 2: Clean text
        text_content = clean_html_to_text(page_content)

        # Crawl4AI fallback for JS-rendered sites
        if len(text_content) < 500:
            crawl_text = await self._fetch_with_crawl4ai(url)
            if crawl_text and len(crawl_text) > len(text_content):
                text_content = crawl_text

        page_lower = text_content.lower()
        result["page_text"] = text_content[:50000]

        # Step 3: Regex-based signal detection
        result["signals"] = self._detect_signals(page_lower)
        result["signal_count"] = len(result["signals"])
        result["has_hotel_content"] = result["signal_count"] >= 2

        # Step 4: Extract article links — prioritize opening-specific articles
        from bs4 import BeautifulSoup

        soup2 = BeautifulSoup(page_content, "lxml")
        all_links = set()

        opening_kw = [
            "opening",
            "open",
            "debut",
            "new hotel",
            "new resort",
            "announce",
            "groundbreaking",
            "breaking ground",
            "development",
            "pipeline",
            "2026",
            "2027",
            "2028",
            "upcoming",
            "planned",
            "construction",
        ]
        hotel_kw = [
            "hotel",
            "resort",
            "hospitality",
            "lodging",
            "opening",
            "luxury",
            "boutique",
        ]

        opening_article_links = []
        general_hotel_links = []

        for a in soup2.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                href = urljoin(url, href)
            if domain in href:
                all_links.add(href)
                link_text = (a.get_text() + " " + href).lower()
                if any(kw in link_text for kw in hotel_kw):
                    if any(ok in link_text for ok in opening_kw):
                        opening_article_links.append(href)
                    else:
                        general_hotel_links.append(href)

        # Prefer opening articles, fall back to general hotel links
        hotel_article_links = (
            opening_article_links if opening_article_links else general_hotel_links
        )

        # Deduplicate while preserving order
        seen_urls = set()
        deduped = []
        for link in hotel_article_links:
            if link not in seen_urls:
                seen_urls.add(link)
                deduped.append(link)
        hotel_article_links = deduped

        result["article_links"] = len(all_links)
        result["hotel_articles"] = len(hotel_article_links)
        result["sample_articles"] = hotel_article_links[:8]

        # Step 5: Recent content check
        year_matches = re.findall(r"\b(2025|2026|2027|2028)\b", text_content)
        result["has_recent_content"] = len(year_matches) >= 2

        # Step 6: Homepage + recurring detection
        parsed = urlparse(url)
        result["homepage_url"] = f"{parsed.scheme}://{parsed.netloc}"
        result["is_recurring"], result["recurring_reason"] = self._detect_recurring(
            domain, hotel_article_links, page_lower
        )
        result["unique_hotel_articles"] = len(set(hotel_article_links))

        # Step 7: Classify source type
        result["suggested_type"] = self._classify(page_lower, domain)

        # Step 8: Quality score
        score = 0
        score += min(result["signal_count"] * 8, 30)
        score += min(result["hotel_articles"] * 5, 25)
        score += 10 if result["has_recent_content"] else 0
        score += 10 if result["article_links"] > 10 else 0
        if any(kw in domain for kw in HIGH_VALUE_KEYWORDS):
            score += 15
        if any(
            loc in page_lower
            for loc in ["florida", "miami", "caribbean", "aruba", "bahamas"]
        ):
            score += 10
        result["quality_score"] = min(score, 100)

        # Step 9: Priority
        if result["quality_score"] >= 75:
            result["suggested_priority"] = 9
        elif result["quality_score"] >= 60:
            result["suggested_priority"] = 7
        elif result["quality_score"] >= 45:
            result["suggested_priority"] = 5
        else:
            result["suggested_priority"] = 3

        return result

    @staticmethod
    def _detect_signals(content: str) -> list[str]:
        signals = []
        checks = {
            "hotel_opening": r"\b(?:hotel|resort)\b.*\b(?:open|opening|debut|launch|unveil)\b",
            "new_property": r"\bnew\b.*\b(?:hotel|resort|property|destination)\b",
            "year_2026_plus": r"\b(?:2026|2027|2028)\b",
            "brand_major": r"\b(?:hilton|marriott|hyatt|ihg|four\s*seasons|ritz|waldorf|st\.?\s*regis)\b",
            "brand_luxury": r"\b(?:rosewood|aman|oetker|peninsula|mandarin\s*oriental|park\s*hyatt|conrad)\b",
            "room_count": r"\b\d{2,4}\s*[-–]?\s*(?:room|key|suite|guest\s*room)\b",
            "location_florida": r"\b(?:florida|miami|orlando|tampa|fort\s*lauderdale|palm\s*beach|naples)\b",
            "location_caribbean": r"\b(?:caribbean|bahamas|aruba|jamaica|cayman|bermuda|turks|barbados)\b",
            "location_usa": r"\b(?:new\s*york|los\s*angeles|chicago|texas|california|las\s*vegas|atlanta)\b",
            "development": r"\b(?:development|construction|groundbreaking|renovation|conversion)\b",
            "hospitality_news": r"\b(?:hospitality|lodging|hotelier|hotel\s*industry|hotel\s*news)\b",
            "pipeline": r"\b(?:pipeline|planned|announced|upcoming|breaking\s*ground|topping\s*off)\b",
            "investment": r"\b(?:investment|acquisition|franchise|management\s*agreement)\b",
        }
        for name, pattern in checks.items():
            if re.search(pattern, content, re.IGNORECASE):
                signals.append(name)
        return signals

    @staticmethod
    def _detect_recurring(
        domain: str, hotel_links: list, content: str
    ) -> tuple[bool, str]:
        if any(
            kw in domain
            for kw in ["hotel", "hospitality", "lodging", "resort", "travel"]
        ):
            return True, "Domain name indicates hotel/travel industry site"

        unique_links = set(hotel_links)
        if len(unique_links) >= 5:
            return True, f"Links to {len(unique_links)} hotel articles"
        if len(unique_links) >= 3:
            return True, f"{len(unique_links)} hotel article links found"

        news_indicators = [
            "latest news",
            "recent posts",
            "more stories",
            "related articles",
            "trending",
            "subscribe",
            "newsletter",
            "daily",
            "weekly",
        ]
        news_count = sum(1 for ni in news_indicators if ni in content)
        if news_count >= 3:
            return True, f"Site has {news_count} news indicators"

        if any(
            kw in content
            for kw in [
                "category",
                "archive",
                "tag/hotel",
                "topic/hotel",
                "/hotels/",
                "/hospitality/",
            ]
        ):
            return True, "Has hotel category/archive section"

        date_urls = [link for link in hotel_links if re.search(r"/20\d{2}/", link)]
        if len(date_urls) >= 2:
            return True, f"Has {len(date_urls)} date-based article URLs"

        return False, "One-off article"

    @staticmethod
    def _classify(content: str, domain: str) -> str:
        scores = {}
        for stype, rules in SOURCE_TYPE_RULES.items():
            score = sum(3 for kw in rules["domain_kw"] if kw in domain)
            score += sum(1 for kw in rules["content_kw"] if kw in content)
            scores[stype] = score
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] > 0:
                return best
        return "aggregator"

    async def close(self):
        await self.client.aclose()
        if self._crawler and self._crawl4ai_loop:
            def _sync_close():
                try:
                    self._crawl4ai_loop.run_until_complete(self._crawler.close())
                except Exception:
                    pass
                try:
                    self._crawl4ai_loop.close()
                except Exception:
                    pass
            try:
                await self._run_in_thread(_sync_close)
            except Exception:
                pass
        self._executor.shutdown(wait=False)


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE WRAPPER — uses IntelligentPipeline for classification + extraction
# ═══════════════════════════════════════════════════════════════════════════════


class DiscoveryLeadExtractor:
    """Uses the existing IntelligentPipeline for BOTH classification and extraction.

    CRITICAL: Sends CLEANED TEXT to pipeline, not raw HTML.
    The pipeline classifier truncates to 5000 chars — if that's raw HTML,
    Gemini only sees <head> boilerplate and rejects everything.
    """

    def __init__(self):
        self.pipeline = None
        self.domain_to_source_name: dict[str, str] = {}

    async def initialize(self):
        try:
            from app.services.intelligent_pipeline import IntelligentPipeline

            self.pipeline = IntelligentPipeline()
            logger.info("✅ Pipeline ready (classification + extraction)")
        except Exception as e:
            logger.warning(f"Pipeline init failed: {e} — AI validation disabled")

    async def close(self):
        if self.pipeline:
            await self.pipeline.close()

    async def classify_and_extract(self, pages: list[dict]) -> dict:
        """Run pipeline on discovered pages. Returns leads + classification results."""
        result = {
            "leads": [],
            "relevant_urls": set(),
            "rejected_urls": set(),
            "pages_checked": 0,
            "leads_per_domain": defaultdict(int),
        }

        if not self.pipeline:
            return result

        pipeline_pages = []
        url_to_domain = {}
        seen_urls = set()

        for page in pages:
            url = page.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            # Use pre-cleaned text if available, otherwise clean from HTML
            content = page.get("page_text", "")
            if not content and page.get("page_html"):
                content = clean_html_to_text(page["page_html"])

            if not content or len(content) < 200:
                continue

            pipeline_pages.append(
                {
                    "url": url,
                    "content": content,
                    "source": self.domain_to_source_name.get(
                        page["domain"], f"discovery:{page['domain']}"
                    ),
                }
            )
            url_to_domain[url] = page["domain"]

        if not pipeline_pages:
            return result

        try:
            pipeline_result = await self.pipeline.process_pages(
                pipeline_pages, source_name="Web Discovery"
            )

            result["pages_checked"] = len(pipeline_pages)

            # Get leads
            if hasattr(pipeline_result, "final_leads"):
                result["leads"] = pipeline_result.final_leads or []
            elif hasattr(pipeline_result, "leads"):
                result["leads"] = pipeline_result.leads or []
            elif isinstance(pipeline_result, list):
                result["leads"] = pipeline_result

            # Get relevant URLs from pipeline
            if (
                hasattr(pipeline_result, "relevant_urls")
                and pipeline_result.relevant_urls
            ):
                result["relevant_urls"] = set(pipeline_result.relevant_urls)
            else:
                for lead in result["leads"]:
                    url = getattr(lead, "source_url", None) or (
                        lead.get("source_url") if isinstance(lead, dict) else None
                    )
                    if url and url in url_to_domain:
                        result["relevant_urls"].add(url)

            result["rejected_urls"] = (
                set(url_to_domain.keys()) - result["relevant_urls"]
            )

            for lead in result["leads"]:
                url = getattr(lead, "source_url", None) or (
                    lead.get("source_url") if isinstance(lead, dict) else None
                )
                if url and url in url_to_domain:
                    result["leads_per_domain"][url_to_domain[url]] += 1

        except Exception as e:
            logger.error(f"Pipeline error: {e}")

        return result


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DISCOVERY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════


class WebDiscoveryEngine:
    def __init__(self, dry_run=False, min_quality=35, sources_only=False):
        self.dry_run = dry_run
        self.min_quality = min_quality
        self.sources_only = sources_only

        self.engines = [
            ("DuckDuckGo", DuckDuckGoSearch()),
            ("Google News", GoogleNewsRSS()),
        ]
        self.tester = DomainTester()
        self.pipeline = DiscoveryLeadExtractor()

        self.known_domains: set[str] = set()
        self.domain_to_source_name: dict[str, str] = {}
        self.failed_domains: dict = {}
        self.discovered: list[dict] = []
        self.extracted_leads: list = []
        self.stats = defaultdict(int)

    async def initialize(self):
        async with async_session() as session:
            result = await session.execute(
                select(Source.base_url, Source.name).where(Source.is_active.is_(True))
            )
            for row in result.all():
                domain = urlparse(row.base_url).netloc.lower().replace("www.", "")
                self.known_domains.add(domain)
                self.domain_to_source_name[domain] = row.name
                parts = domain.split(".")
                if len(parts) > 2:
                    short = ".".join(parts[-2:])
                    self.known_domains.add(short)
                    if short not in self.domain_to_source_name:
                        self.domain_to_source_name[short] = row.name

            try:
                fd_result = await session.execute(select(FailedDomain))
                for fd in fd_result.scalars().all():
                    self.failed_domains[fd.domain] = fd
            except Exception:
                pass

        await self.pipeline.initialize()
        # Share domain→source name mapping with pipeline extractor
        self.pipeline.domain_to_source_name = self.domain_to_source_name

        skippable = sum(1 for fd in self.failed_domains.values() if fd.should_skip())
        print(f"  📂 Loaded {len(self.known_domains)} known domains from database")
        if self.failed_domains:
            print(
                f"  🚫 Loaded {len(self.failed_domains)} failed domains ({skippable} will skip)"
            )

    def _is_blacklisted(self, domain: str) -> bool:
        if domain in BLACKLISTED_DOMAINS:
            return True
        return any(bl in domain for bl in BLACKLIST_SUBSTRINGS)

    def _is_known(self, domain: str) -> bool:
        if domain in self.known_domains:
            return True
        parts = domain.split(".")
        if len(parts) > 2 and ".".join(parts[-2:]) in self.known_domains:
            return True
        return False

    async def run(self, max_queries: int = None, skip_queries: int = 0):
        queries = SEARCH_QUERIES[skip_queries:]
        if max_queries:
            queries = queries[:max_queries]

        print("═" * 70)
        print("  🌐  W E B   D I S C O V E R Y   E N G I N E   v5.1")
        print("═" * 70)
        print(f"  Search queries : {len(queries)}")
        print(f"  Known sources  : {len(self.known_domains)}")
        print(f"  Search engines : {', '.join(name for name, _ in self.engines)}")
        print(f"  Min quality    : {self.min_quality}")
        print(
            f"  Pipeline       : {'ON' if self.pipeline.pipeline else 'OFF'} (classify + extract)"
        )
        print(f"  Sources only   : {'YES' if self.sources_only else 'NO'}")
        print(f"  Mode           : {'🔍 DRY RUN' if self.dry_run else '🚀 LIVE'}")
        print("═" * 70)

        # ── Phase 1: Search ──
        print(
            f"\n📡 Phase 1: Searching ({len(queries)} queries × {len(self.engines)} engines)..."
        )
        all_results = {}
        for i, query in enumerate(queries, 1):
            print(f"  [{i:2d}/{len(queries)}] {query}")
            for engine_name, engine in self.engines:
                try:
                    results = await engine.search(query)
                    for r in results:
                        domain = r["domain"]
                        if not domain:
                            continue
                        existing = all_results.get(domain)
                        if existing is None:
                            all_results[domain] = r
                        else:
                            new_path = urlparse(r["url"]).path.strip("/")
                            old_path = urlparse(existing["url"]).path.strip("/")
                            if len(new_path) > len(old_path):
                                title = existing.get("title") or r.get("title", "")
                                all_results[domain] = r
                                if title:
                                    all_results[domain]["title"] = title
                    await asyncio.sleep(2.5)  # Increased delay to avoid DDG rate limits
                except Exception as e:
                    logger.debug(f"{engine_name} error on query '{query}': {e}")

        self.stats["search_results"] = len(all_results)
        print(f"  → {len(all_results)} unique domains found across all engines")

        # ── Phase 2: Filter ──
        print("\n🔍 Phase 2: Filtering known & blacklisted domains...")
        candidates = {}
        known_count = 0
        blacklisted_count = 0
        failed_skip_count = 0

        for domain, data in all_results.items():
            if self._is_known(domain):
                known_count += 1
            elif self._is_blacklisted(domain):
                blacklisted_count += 1
            elif (
                domain in self.failed_domains
                and self.failed_domains[domain].should_skip()
            ):
                failed_skip_count += 1
            else:
                candidates[domain] = data

        self.stats["already_known"] = known_count
        self.stats["blacklisted"] = blacklisted_count
        self.stats["failed_skipped"] = failed_skip_count

        print(f"  → {len(candidates)} candidates to test")
        print(f"  → {known_count} already known")
        print(f"  → {blacklisted_count} blacklisted")
        if failed_skip_count:
            print(f"  → {failed_skip_count} skipped (failed before)")

        # ── Phase 3: Signal test ──
        print(f"\n🧪 Phase 3: Signal-testing {len(candidates)} domains...")
        signal_passed = []

        for i, (domain, data) in enumerate(candidates.items(), 1):
            url = data["url"]
            print(f"  [{i:2d}/{len(candidates)}] {domain}...", end=" ", flush=True)

            test_result = await self.tester.test(url, domain)

            if not test_result["reachable"]:
                print("❌ Unreachable")
                self.stats["unreachable"] += 1
                await self._record_domain_failure(domain, "unreachable")
                continue

            await self._clear_domain_failure(domain)

            if test_result["signal_count"] == 0:
                print("⚪ No signals")
                self.stats["low_signals"] += 1
                continue

            score = test_result["quality_score"]
            signals = test_result["signal_count"]
            articles = len(test_result.get("sample_articles", []))
            rec = " 🔄" if test_result["is_recurring"] else ""
            print(
                f"📋 Score:{score} Signals:{signals} Articles:{articles}{rec} → queued"
            )
            signal_passed.append({**data, **test_result})

            await asyncio.sleep(0.5)

        print(f"  → {len(signal_passed)} pages passed signal test")

        # ── Phase 3b: Fetch actual article pages for pipeline ──
        if signal_passed:
            print(
                f"\n📄 Phase 3b: Fetching article pages from {len(signal_passed)} domains..."
            )
            article_pages = []
            fetch_client = httpx.AsyncClient(
                timeout=15,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                },
            )

            for src in signal_passed:
                domain = src["domain"]
                articles = src.get("sample_articles", [])[:3]

                if not articles:
                    # No article links — send homepage with cleaned text
                    if src.get("page_text") or src.get("page_html"):
                        article_pages.append(src)
                        print(f"  📄 {domain}: no article links, using homepage")
                    continue

                fetched = 0
                for article_url in articles:
                    try:
                        resp = await fetch_client.get(article_url)
                        if resp.status_code == 200 and len(resp.text) > 1000:
                            # Clean the HTML to text immediately
                            cleaned_text = clean_html_to_text(resp.text)
                            if len(cleaned_text) > 300:
                                article_entry = {
                                    **src,
                                    "url": article_url,
                                    "page_html": resp.text,
                                    "page_text": cleaned_text[:50000],
                                    "is_article": True,
                                }
                                article_pages.append(article_entry)
                                fetched += 1
                    except Exception as e:
                        logger.debug(f"Failed to fetch {article_url[:60]}: {e}")
                    await asyncio.sleep(0.3)

                if fetched > 0:
                    print(f"  ✅ {domain}: fetched {fetched}/{len(articles)} articles")
                else:
                    if src.get("page_text") or src.get("page_html"):
                        article_pages.append(src)
                        print(f"  ⚠️  {domain}: articles failed, using homepage")

            await fetch_client.aclose()
            print(f"  → {len(article_pages)} pages ready for pipeline")
        else:
            article_pages = []

        # ── Phase 4: Pipeline classification + extraction ──
        qualified_sources = []
        qualified_articles = []

        if article_pages and self.pipeline.pipeline:
            print(
                f"\n🤖 Phase 4: Pipeline classification + extraction ({len(article_pages)} pages)..."
            )

            pipeline_result = await self.pipeline.classify_and_extract(article_pages)
            relevant_urls = pipeline_result["relevant_urls"]
            rejected_urls = pipeline_result["rejected_urls"]
            leads = pipeline_result["leads"]
            leads_per_domain = pipeline_result.get("leads_per_domain", {})

            print(
                f"  → Pipeline: {len(relevant_urls)} relevant, {len(rejected_urls)} rejected"
            )
            if leads:
                print(f"  → Extracted {len(leads)} leads!")
                self.extracted_leads = leads

            # Build per-domain best result for scoring
            domain_best = {}
            for page in article_pages:
                d = page["domain"]
                if d not in domain_best:
                    domain_best[d] = page
                elif page.get("is_article"):
                    domain_best[d] = page

            for src in domain_best.values():
                url = src["url"]
                domain = src["domain"]
                is_relevant = url in relevant_urls
                lead_count = leads_per_domain.get(domain, 0)

                if is_relevant or lead_count > 0:
                    src["quality_score"] += 25
                    if lead_count > 0:
                        src["quality_score"] += 10 * min(lead_count, 5)
                    src["pipeline_relevant"] = True
                    src["pipeline_leads"] = lead_count
                    tag = (
                        f"Pipeline: ✅ ({lead_count} leads)"
                        if lead_count
                        else "Pipeline: ✅"
                    )
                else:
                    src["quality_score"] -= 40
                    src["pipeline_relevant"] = False
                    src["pipeline_leads"] = 0
                    tag = "Pipeline: ❌"

                score = src["quality_score"]
                if score >= self.min_quality:
                    rec_tag = ""
                    if src.get("is_recurring"):
                        qualified_sources.append(src)
                        rec_tag = " | 🔄 Recurring"
                        self.stats["recurring"] += 1
                    else:
                        qualified_articles.append(src)
                        rec_tag = " | 📄 One-off"
                        self.stats["one_off"] += 1
                    self.discovered.append(src)
                    print(f"  ✅ {domain}: Score:{score} {tag}{rec_tag}")
                else:
                    print(f"  ⚠️  {domain}: Score:{score} < {self.min_quality} {tag}")
                    self.stats["low_quality"] += 1

        elif article_pages:
            print("\n⚠️  Pipeline unavailable — using signal scores only")
            for src in article_pages:
                if src["quality_score"] >= self.min_quality:
                    if src.get("is_recurring"):
                        qualified_sources.append(src)
                    else:
                        qualified_articles.append(src)
                    self.discovered.append(src)
                else:
                    self.stats["low_quality"] += 1

        self.stats["qualified"] = len(qualified_sources) + len(qualified_articles)

        # ── Phase 5: Save ──
        if not self.dry_run:
            if self.extracted_leads and not self.sources_only:
                saved = await self._save_leads(self.extracted_leads)
                print(
                    f"  → Leads saved: {saved.get('saved', 0)} new, {saved.get('duplicates', 0)} duplicates"
                )

            if qualified_sources:
                print(
                    f"\n💾 Phase 5: Saving {len(qualified_sources)} recurring sources to database..."
                )
                added = await self._save_sources(qualified_sources)
                print(f"  → {added} sources added")
        else:
            print(
                f"\n📋 Phase 5: DRY RUN — would add {len(qualified_sources)} recurring sources"
            )
            if qualified_articles:
                print(f"  → {len(qualified_articles)} one-off articles (leads only)")

        self._print_report(qualified_sources, qualified_articles)
        self._save_log(qualified_sources, qualified_articles)

    # ─── Domain failure tracking ─────────────────────────────────────────────

    async def _record_domain_failure(self, domain: str, reason: str = "unreachable"):
        if self.dry_run:
            return
        try:
            async with async_session() as session:
                result = await session.execute(
                    select(FailedDomain).where(FailedDomain.domain == domain)
                )
                fd = result.scalar_one_or_none()
                if fd:
                    fd.record_failure(reason=reason)
                else:
                    now = datetime.now(timezone.utc)
                    fd = FailedDomain(
                        domain=domain,
                        reason=reason,
                        fail_count=1,
                        first_failed=now,
                        last_failed=now,
                        retry_after=now + timedelta(days=7),
                    )
                    session.add(fd)
                await session.commit()
                self.failed_domains[domain] = fd
        except Exception as e:
            logger.debug(f"Failed to record domain failure for {domain}: {e}")

    async def _clear_domain_failure(self, domain: str):
        if domain not in self.failed_domains:
            return
        if self.dry_run:
            return
        try:
            async with async_session() as session:
                result = await session.execute(
                    select(FailedDomain).where(FailedDomain.domain == domain)
                )
                fd = result.scalar_one_or_none()
                if fd:
                    await session.delete(fd)
                    await session.commit()
                del self.failed_domains[domain]
        except Exception as e:
            logger.debug(f"Failed to clear domain failure for {domain}: {e}")

    # ─── Save sources ────────────────────────────────────────────────────────

    async def _save_sources(self, qualified: list[dict]) -> int:
        added = 0
        async with async_session() as session:
            for src in qualified:
                existing = await session.execute(
                    select(Source).where(Source.base_url.ilike(f"%{src['domain']}%"))
                )
                if existing.scalars().first():
                    continue

                homepage = src.get("homepage_url", f"https://{src['domain']}")
                name = self._domain_to_name(src["domain"])

                notes = (
                    f"Auto-discovered {datetime.now(timezone.utc).strftime('%Y-%m-%d')} | "
                    f"Score: {src.get('quality_score', 0)} | "
                    f"Type: {src.get('suggested_type', '?')} | "
                    f"Recurring: {src.get('recurring_reason', 'yes')}"
                )

                source = Source(
                    name=name,
                    base_url=homepage,
                    source_type=src.get("suggested_type", "aggregator"),
                    priority=src.get("suggested_priority", 5),
                    scrape_frequency="weekly",
                    is_active=True,
                    health_status="new",
                    notes=notes,
                )

                if src.get("sample_articles"):
                    gold = {}
                    for url in src["sample_articles"][:3]:
                        gold[url] = {
                            "leads_found": 0,
                            "first_found": datetime.now(timezone.utc).isoformat(),
                            "miss_streak": 0,
                            "total_checks": 0,
                        }
                    source.gold_urls = gold

                session.add(source)
                added += 1
                print(
                    f"    ✅ {name} (priority={src.get('suggested_priority')}, type={src.get('suggested_type')})"
                )

            if added:
                await session.commit()
        return added

    # ─── Save leads ──────────────────────────────────────────────────────────

    async def _save_leads(self, leads: list) -> dict:
        result = {"saved": 0, "duplicates": 0}
        try:
            from app.services.orchestrator import LeadHunterOrchestrator

            orchestrator = LeadHunterOrchestrator(
                gemini_api_key="vertex-ai",
                save_to_database=True,
            )
            await orchestrator.initialize()

            lead_dicts = []
            for lead in leads:
                if hasattr(lead, "to_dict"):
                    lead_dicts.append(lead.to_dict())
                elif isinstance(lead, dict):
                    lead_dicts.append(lead)

            if lead_dicts:
                db_result = await orchestrator.save_leads_to_database(lead_dicts)
                result["saved"] = db_result.get("saved", 0)
                result["duplicates"] = db_result.get("duplicates", 0)

            await orchestrator.close()
        except Exception as e:
            logger.error(f"Failed to save discovery leads: {e}")
        return result

    # ─── Report ──────────────────────────────────────────────────────────────

    def _print_report(self, recurring: list, one_off: list):
        print("\n" + "═" * 70)
        print("  📊  D I S C O V E R Y   R E S U L T S")
        print("═" * 70)
        print(f"  Queries run      : {self.stats.get('queries', len(SEARCH_QUERIES))}")
        print(f"  Search results   : {self.stats['search_results']}")
        print(f"  Already known    : {self.stats['already_known']}")
        print(f"  Blacklisted      : {self.stats['blacklisted']}")
        if self.stats["failed_skipped"]:
            print(f"  Failed (skipped) : {self.stats['failed_skipped']}")
        tested = (
            self.stats["search_results"]
            - self.stats["already_known"]
            - self.stats["blacklisted"]
            - self.stats.get("failed_skipped", 0)
        )
        print(f"  Tested           : {tested}")
        print(f"  Unreachable      : {self.stats['unreachable']}")
        print(f"  Low signals      : {self.stats['low_signals']}")
        print(f"  Low quality      : {self.stats['low_quality']}")
        print("  ───────────────────────────────────")
        print(f"  ✅ QUALIFIED       : {self.stats['qualified']}")
        print(f"     🔄 Recurring   : {len(recurring)} (add to sources DB)")
        print(f"     📄 One-off     : {len(one_off)} (leads only)")

        if recurring:
            print("\n  🔄 RECURRING SOURCES:")
            for src in sorted(
                recurring, key=lambda x: x["quality_score"], reverse=True
            ):
                leads = src.get("pipeline_leads", 0)
                tag = f" | {leads} leads" if leads else ""
                print(f"    🌐 {src['domain']}")
                print(
                    f"       Score: {src['quality_score']} | Type: {src.get('suggested_type', '?')}{tag}"
                )
                if src.get("recurring_reason"):
                    print(f"       Why: {src['recurring_reason']}")

        if one_off:
            print("\n  📄 ONE-OFF ARTICLES:")
            for src in sorted(one_off, key=lambda x: x["quality_score"], reverse=True)[
                :5
            ]:
                print(
                    f"    [{src['quality_score']:3d}] {src['domain']} — {src.get('recurring_reason', '')}"
                )

        if self.extracted_leads:
            print(f"\n  🏨 EXTRACTED LEADS ({len(self.extracted_leads)}):")
            for lead in self.extracted_leads[:15]:
                if hasattr(lead, "hotel_name"):
                    name = lead.hotel_name
                    city = getattr(lead, "city", "")
                    state = getattr(lead, "state", "")
                elif isinstance(lead, dict):
                    name = lead.get("hotel_name", "Unknown")
                    city = lead.get("city", "")
                    state = lead.get("state", "")
                else:
                    continue
                loc = f" — {city}, {state}" if city else ""
                print(f"    🏨 {name}{loc}")

    # ─── Discovery log ───────────────────────────────────────────────────────

    def _save_log(self, recurring: list, one_off: list):
        log_path = Path("data/learnings/discovery_log.json")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        history = []
        if log_path.exists():
            try:
                history = json.loads(log_path.read_text())
            except Exception:
                pass
        history.append(
            {
                "run_at": datetime.now(timezone.utc).isoformat(),
                "dry_run": self.dry_run,
                "stats": dict(self.stats),
                "recurring_sources": [
                    {
                        "domain": d["domain"],
                        "score": d["quality_score"],
                        "type": d["suggested_type"],
                    }
                    for d in recurring
                ],
                "one_off_articles": [
                    {"domain": d["domain"], "score": d["quality_score"]}
                    for d in one_off
                ],
                "leads_count": len(self.extracted_leads),
            }
        )
        history = history[-50:]
        log_path.write_text(json.dumps(history, indent=2))

    # ─── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _domain_to_name(domain: str) -> str:
        name = domain.replace("www.", "")
        name = re.sub(r"\.(com|org|net|io|co|us|uk|info|news|ca)$", "", name)
        name = name.replace("-", " ").replace(".", " ").title()
        return f"{name} - Hotels"

    async def close(self):
        for _, engine in self.engines:
            await engine.close()
        await self.tester.close()
        if hasattr(self, "pipeline") and self.pipeline:
            await self.pipeline.close()


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════


async def main():
    parser = argparse.ArgumentParser(description="🌐 Web Discovery Engine v5.1")
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview only, don't add to DB"
    )
    parser.add_argument(
        "--queries", type=int, default=None, help="Limit number of search queries"
    )
    parser.add_argument(
        "--skip-queries", type=int, default=0, help="Skip first N queries"
    )
    parser.add_argument(
        "--min-quality", type=int, default=35, help="Minimum quality score to qualify"
    )
    parser.add_argument(
        "--sources-only", action="store_true", help="Skip lead extraction/saving"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    engine = WebDiscoveryEngine(
        dry_run=args.dry_run,
        min_quality=args.min_quality,
        sources_only=args.sources_only,
    )

    try:
        await engine.initialize()
        await engine.run(max_queries=args.queries, skip_queries=args.skip_queries)

        if engine.discovered:
            print(f"\n✨ Found {len(engine.discovered)} qualified source(s).")
            if engine.extracted_leads:
                print(f"🏨 Extracted {len(engine.extracted_leads)} leads!")
            if args.dry_run:
                print("   Run without --dry-run to save to database.")
        else:
            print("\n📭 No new sources or leads this run.")
    except KeyboardInterrupt:
        print("\n\n⚠️  Cancelled")
    finally:
        await engine.close()
        from app.database import engine as db_engine

        await db_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
