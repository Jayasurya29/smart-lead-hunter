# -*- coding: utf-8 -*-
"""
SMART SCRAPER SERVICE
======================
Unified scraping logic used by BOTH the dashboard SSE pipeline AND Celery tasks.
One function, one brain, no duplication.

Handles:
- Gold mode (fast: hit listing pages + follow links)
- Discovery mode (deep crawl for new sources)
- Rediscovery mode (overdue gold → full crawl)
- Intelligence integration (adaptive delays, junk filtering, rate limit detection)
- Response tracking for learning

Usage:
    from app.services.smart_scraper import scrape_source_smart

    # From SSE pipeline or Celery task:
    result = await scrape_source_smart(source, orchestrator, on_progress=callback)
"""

import re
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.services.utils import local_now
from app.config.intelligence_config import SKIP_URL_PATTERNS

logger = logging.getLogger(__name__)

# URLs to never follow — imported from single config
SKIP_PATTERNS = SKIP_URL_PATTERNS


@dataclass
class ScrapeSourceResult:
    """Result of scraping a single source."""

    source_name: str = ""
    source_id: int = 0
    mode: str = "discovery"  # gold, rediscovery, discovery
    pages_scraped: int = 0
    pages: List = field(default_factory=list)  # List of ScrapeResult objects
    all_pages_data: List[Dict] = field(default_factory=list)  # For pipeline input
    rate_limited: bool = False
    error: str = ""
    skipped: bool = False
    skip_reason: str = ""


async def scrape_source_smart(
    source,
    orchestrator,
    source_intel_map: Optional[Dict] = None,
    on_progress: Optional[Callable] = None,
) -> ScrapeSourceResult:
    """
    Scrape a single source using the full intelligence stack.

    This is THE scraping function. Both SSE dashboard and Celery tasks call this.

    Args:
        source: Source model instance
        orchestrator: LeadHunterOrchestrator (initialized)
        source_intel_map: Optional dict to store SourceIntelligence instances
        on_progress: Optional callback(message: str) for progress updates

    Returns:
        ScrapeSourceResult with all scraped pages
    """
    from app.services.source_intelligence import SourceIntelligence

    result = ScrapeSourceResult(
        source_name=source.name,
        source_id=source.id,
    )

    if source_intel_map is None:
        source_intel_map = {}

    def _progress(msg: str):
        if on_progress:
            on_progress(msg)
        logger.info(msg)

    # ─── LOAD INTELLIGENCE ────────────────────────────────────
    scrape_settings = None
    try:
        intel = SourceIntelligence(source)
        source_intel_map[source.id] = intel
        scrape_settings = intel.get_scrape_settings()

        if scrape_settings.should_skip:
            result.skipped = True
            result.skip_reason = scrape_settings.skip_reason
            _progress(f"Skipping {source.name}: {scrape_settings.skip_reason}")
            return result
    except Exception as intel_err:
        logger.warning(f"Intelligence load failed for {source.name}: {intel_err}")

    # ─── DETERMINE MODE ──────────────────────────────────────
    gold_urls_dict = source.gold_urls or {}
    active_gold = [
        url for url, meta in gold_urls_dict.items() if meta.get("miss_streak", 0) < 3
    ]

    use_gold = len(active_gold) > 0
    needs_rediscovery = False

    if use_gold and source.last_discovery_at:
        discovery_interval = source.discovery_interval_days or 7
        days_since = (local_now() - source.last_discovery_at).total_seconds() / 86400
        if days_since >= discovery_interval:
            needs_rediscovery = True
            use_gold = False
    elif use_gold and not source.last_discovery_at:
        needs_rediscovery = True
        use_gold = False

    if needs_rediscovery:
        result.mode = "rediscovery"
        mode_label = f"Rediscovery (overdue, {len(active_gold)} gold exist)"
    elif use_gold:
        result.mode = "gold"
        mode_label = f"GOLD ({len(active_gold)} URLs)"
    else:
        result.mode = "discovery"
        mode_label = "First Discovery"

    _progress(f"{source.name}: {mode_label}")

    # ─── SCRAPE ───────────────────────────────────────────────
    scrape_results = {source.name: []}

    try:
        if use_gold:
            # ═══ GOLD MODE: Hit gold URLs + follow links ═══
            visited = set()
            for gold_url in active_gold:
                try:
                    # Adaptive delay
                    if scrape_settings and scrape_settings.delay_seconds > 1.0:
                        await asyncio.sleep(scrape_settings.delay_seconds)

                    await orchestrator.scraping_engine.rate_limiter.acquire(gold_url)

                    page_result = (
                        await orchestrator.scraping_engine.http_scraper.scrape(gold_url)
                    )

                    # Record to intelligence
                    if source.id in source_intel_map:
                        src_intel = source_intel_map[source.id]
                        if page_result.status_code in (429, 403):
                            src_intel.record_rate_limit(page_result.status_code)
                            result.rate_limited = True
                            logger.warning(
                                f"Rate limit {page_result.status_code} from {source.name}"
                            )
                            break  # Stop scraping this source
                        if page_result.crawl_time_ms:
                            src_intel.record_url_result(
                                url=gold_url,
                                produced_lead=False,
                                response_time_ms=page_result.crawl_time_ms,
                            )

                    if page_result.success:
                        scrape_results[source.name].append(page_result)
                        visited.add(gold_url)

                        # Follow links from listing page
                        links = _extract_links(
                            html=page_result.html or "",
                            base_url=gold_url,
                            visited=visited,
                            junk_patterns=(
                                scrape_settings.junk_patterns if scrape_settings else []
                            ),
                        )

                        # Fetch linked pages (capped by intelligence)
                        max_follow = (
                            scrape_settings.max_pages if scrape_settings else 15
                        )
                        for link_url in list(links)[:max_follow]:
                            try:
                                if (
                                    scrape_settings
                                    and scrape_settings.delay_seconds > 1.0
                                ):
                                    await asyncio.sleep(scrape_settings.delay_seconds)

                                await orchestrator.scraping_engine.rate_limiter.acquire(
                                    link_url
                                )
                                link_result = await orchestrator.scraping_engine.http_scraper.scrape(
                                    link_url
                                )

                                # Rate limit check on followed links
                                if link_result.status_code in (429, 403):
                                    if source.id in source_intel_map:
                                        source_intel_map[source.id].record_rate_limit(
                                            link_result.status_code
                                        )
                                    result.rate_limited = True
                                    break

                                if link_result.success:
                                    scrape_results[source.name].append(link_result)
                                    visited.add(link_url)
                            except Exception:
                                pass

                except Exception as e:
                    logger.warning(f"Gold URL failed {gold_url[:50]}: {e}")

            logger.info(
                f"Gold mode: {source.name} -> "
                f"{len(scrape_results[source.name])} pages "
                f"from {len(active_gold)} gold URLs"
            )

        else:
            # ═══ DISCOVERY MODE: Deep crawl ═══
            scrape_results = await orchestrator.scraping_engine.scrape_sources(
                [source.name], deep=True, max_concurrent=3
            )

    except Exception as e:
        result.error = str(e)
        logger.error(f"Scrape failed for {source.name}: {e}")
        return result

    # ─── BUILD OUTPUT ─────────────────────────────────────────
    for sname, pages in scrape_results.items():
        successful = [r for r in pages if r.success]
        result.pages_scraped = len(successful)
        result.pages = successful

        for r in successful:
            result.all_pages_data.append(
                {
                    "source_name": sname,
                    "url": r.url,
                    "content": r.text or "",
                    "html": r.html or "",
                }
            )

    # Log intelligence summary
    if source.id in source_intel_map:
        si = source_intel_map[source.id]
        junk_count = len(si.patterns.get("junk", []))
        gold_count = len(si.patterns.get("gold", []))
        logger.info(
            f"Intelligence: {source.name} | "
            f"score={si.efficiency_score} | "
            f"delay={scrape_settings.delay_seconds if scrape_settings else 1.0}s | "
            f"{gold_count} gold, {junk_count} junk patterns"
        )

    _progress(
        f"{source.name}: {result.pages_scraped} pages scraped ({result.mode} mode)"
    )

    return result


def _extract_links(
    html: str,
    base_url: str,
    visited: set,
    junk_patterns: List[str] = None,
) -> set:
    """
    Extract followable links from a page.
    Filters out junk URLs, visited URLs, and off-domain links.
    """
    soup = BeautifulSoup(html, "lxml")
    links = set()
    domain = urlparse(base_url).netloc

    for a in soup.find_all("a", href=True):
        full_url = urljoin(base_url, a["href"])

        # Same domain only
        if urlparse(full_url).netloc != domain:
            continue

        # Not already visited
        if full_url in visited:
            continue

        # Not a skip pattern
        if any(skip in full_url.lower() for skip in SKIP_PATTERNS):
            continue

        # Intelligence junk filter
        if junk_patterns:
            is_junk = False
            for jp in junk_patterns:
                try:
                    if re.search(jp, full_url):
                        is_junk = True
                        break
                except re.error:
                    pass
            if is_junk:
                continue

        links.add(full_url)

    return links
