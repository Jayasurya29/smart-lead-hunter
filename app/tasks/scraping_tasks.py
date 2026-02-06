"""
SMART LEAD HUNTER - CELERY SCRAPING TASKS
==========================================
Background tasks for automated lead scraping

H-08 FIX: run_full_scrape() now uses Celery group() to fan out source scrapes
in parallel instead of calling .apply().get() in a blocking loop. With 79
sources this reduces daily scrape time from hours to minutes.

H-09 FIX: Replaced run_async() helper that created a new event loop per task
invocation with a shared asyncio.Runner (Python 3.11+) that persists across
task calls within a worker. This enables connection reuse for DB sessions
and HTTP clients.

Usage:
    celery -A app.tasks.celery_app worker --loglevel=info
    celery -A app.tasks.celery_app beat --loglevel=info
    
    from app.tasks.scraping_tasks import run_full_scrape
    run_full_scrape.delay()
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from celery import shared_task, group
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.tasks.celery_app import celery_app, BaseTask
from app.database import async_session
from app.models import PotentialLead, Source, ScrapeLog
from app.services.intelligent_pipeline import LeadExtractionPipeline 
from app.services.scorer import score_lead, LeadScorer
from app.services.utils import normalize_hotel_name

logger = logging.getLogger(__name__)


# =============================================================================
# H-09 FIX: SHARED EVENT LOOP HELPER
# =============================================================================
# 
# BEFORE (broken): Every task created and destroyed its own event loop via
# asyncio.new_event_loop(). This prevented connection reuse for DB sessions
# and HTTP clients, creating a new connection pool per task invocation.
#
# AFTER: Use asyncio.Runner (Python 3.11+) which maintains a single event
# loop across calls within the same worker process. Falls back to the old
# pattern on Python < 3.11.

import sys

if sys.version_info >= (3, 11):
    # Python 3.11+: Use asyncio.Runner for connection reuse
    import threading
    
    _runner_local = threading.local()
    
    def run_async(coro):
        """Run async coroutine in Celery sync context with shared event loop.
        
        Uses asyncio.Runner (Python 3.11+) which keeps the event loop alive
        across calls, enabling connection pooling for DB and HTTP clients.
        One Runner per worker thread.
        """
        if not hasattr(_runner_local, 'runner') or _runner_local.runner is None:
            _runner_local.runner = asyncio.Runner()
        try:
            return _runner_local.runner.run(coro)
        except RuntimeError:
            # Runner was closed or loop is dead — create a new one
            _runner_local.runner = asyncio.Runner()
            return _runner_local.runner.run(coro)
else:
    # Python < 3.11: Fallback to loop-per-task (original behavior)
    def run_async(coro):
        """Run async function in sync context (for Celery) — legacy fallback."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def get_active_sources() -> List[Dict]:
    """Get all active sources from database"""
    async with async_session() as session:
        result = await session.execute(
            select(Source).where(Source.is_active == True).order_by(Source.priority.desc())
        )
        sources = result.scalars().all()
        return [s.to_dict() for s in sources]


async def get_source_by_id(source_id: int) -> Optional[Dict]:
    """Get a single source by ID"""
    async with async_session() as session:
        result = await session.execute(
            select(Source).where(Source.id == source_id)
        )
        source = result.scalar_one_or_none()
        return source.to_dict() if source else None


async def save_lead_to_db(
    hotel: Dict,
    session=None,
) -> Optional[int]:
    """Save extracted hotel as potential lead.
    
    M-13 FIX: Accepts an optional session parameter. When called in a loop,
    the caller passes a shared session to avoid opening 20+ connections for
    20+ leads. If no session is provided, creates its own (backward compat).
    """
    owns_session = session is None
    if owns_session:
        session = async_session()
    
    try:
        if owns_session:
            # If we own the session, use context manager
            async with session:
                return await _save_lead_impl(hotel, session, commit=True)
        else:
            # Caller owns the session — don't commit (caller batches)
            return await _save_lead_impl(hotel, session, commit=False)
    except Exception as e:
        logger.error(f"Failed to save lead: {e}")
        if owns_session:
            await session.rollback()
        return None


async def _save_lead_impl(
    hotel: Dict,
    session,
    commit: bool = True,
) -> Optional[int]:
    """Internal implementation for save_lead_to_db."""
    # Use shared normalization (matches orchestrator's logic)
    normalized_name = normalize_hotel_name(hotel.get("hotel_name") or "")
    
    result = await session.execute(
        select(PotentialLead).where(
            PotentialLead.hotel_name_normalized == normalized_name
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        logger.info(f"Duplicate found: {hotel.get('hotel_name')}")
        return None
    
    lead = PotentialLead(
        hotel_name=hotel.get("hotel_name"),
        hotel_name_normalized=normalized_name,
        brand=hotel.get("brand"),
        hotel_type=hotel.get("hotel_type"),
        hotel_website=hotel.get("hotel_website"),
        city=hotel.get("city"),
        state=hotel.get("state"),
        country=hotel.get("country", "USA"),
        contact_name=hotel.get("contact_name"),
        contact_title=hotel.get("contact_title"),
        contact_email=hotel.get("contact_email"),
        contact_phone=hotel.get("contact_phone"),
        opening_date=hotel.get("opening_date"),
        room_count=hotel.get("room_count"),
        description=hotel.get("description"),
        key_insights=hotel.get("key_insights"),
        management_company=hotel.get("management_company"),
        developer=hotel.get("developer"),
        owner=hotel.get("owner"),
        lead_score=hotel.get("lead_score"),
        score_breakdown=hotel.get("score_breakdown"),
        source_url=hotel.get("source_url"),
        source_site=hotel.get("source_site"),
        status="new",
        raw_data=hotel
    )
    
    session.add(lead)
    
    if commit:
        await session.commit()
        await session.refresh(lead)
    else:
        # Flush to get the ID without committing
        await session.flush()
    
    logger.info(f"✅ Saved lead: {lead.hotel_name} (ID: {lead.id})")
    return lead.id


async def create_scrape_log(source_id: int) -> int:
    """Create a new scrape log entry"""
    async with async_session() as session:
        log = ScrapeLog(
            source_id=source_id,
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        session.add(log)
        await session.commit()
        await session.refresh(log)
        return log.id


async def update_scrape_log(
    log_id: int,
    status: str,
    urls_scraped: int = 0,
    leads_found: int = 0,
    leads_new: int = 0,
    errors: List[str] = None
):
    """Update scrape log with results"""
    async with async_session() as session:
        await session.execute(
            update(ScrapeLog)
            .where(ScrapeLog.id == log_id)
            .values(
                completed_at=datetime.now(timezone.utc),
                status=status,
                urls_scraped=urls_scraped,
                leads_found=leads_found,
                leads_new=leads_new,
                errors=errors
            )
        )
        await session.commit()


async def update_source_stats(source_id: int, leads_found: int):
    """Update source statistics after scraping"""
    async with async_session() as session:
        await session.execute(
            update(Source)
            .where(Source.id == source_id)
            .values(
                last_scraped_at=datetime.now(timezone.utc),
                leads_found=Source.leads_found + leads_found
            )
        )
        await session.commit()


# =============================================================================
# SCRAPING LOGIC
# =============================================================================

# Shared Playwright browser instance — avoids 1-3s launch overhead per URL.
# Created once per worker, reused across all scrape_url_async calls.
_playwright_instance = None
_playwright_browser = None
_playwright_lock = asyncio.Lock()


async def _get_shared_browser():
    """Get or create a shared Playwright browser instance."""
    global _playwright_instance, _playwright_browser
    async with _playwright_lock:
        if _playwright_browser is None or not _playwright_browser.is_connected():
            try:
                from playwright.async_api import async_playwright
                _playwright_instance = await async_playwright().start()
                _playwright_browser = await _playwright_instance.chromium.launch(headless=True)
                logger.info("Shared Playwright browser launched")
            except ImportError:
                logger.warning("Playwright not available")
                return None
    return _playwright_browser


async def _close_shared_browser():
    """Close shared browser on worker shutdown."""
    global _playwright_instance, _playwright_browser
    async with _playwright_lock:
        if _playwright_browser:
            try:
                await _playwright_browser.close()
            except Exception:
                pass
            _playwright_browser = None
        if _playwright_instance:
            try:
                await _playwright_instance.stop()
            except Exception:
                pass
            _playwright_instance = None


async def scrape_url_async(url: str, use_playwright: bool = False) -> Optional[str]:
    """
    Scrape a single URL and return text content.
    
    ⚠️ M-14 DEPRECATION NOTE: This function reimplements basic HTTP+BeautifulSoup
    scraping that scraping_engine.py already handles with 3-engine fallback
    (HTTPX → Crawl4AI → Playwright), caching, rate limiting, and anti-detection.
    
    TODO: Replace calls to this function with ScrapingEngine.scrape() to get:
    - Automatic engine selection based on domain learning
    - Content caching (avoids re-scraping same URL within TTL)
    - Rate limiting per domain
    - Anti-detection headers and browser fingerprinting
    - Retry logic with engine fallback
    
    Keeping for now to avoid breaking changes mid-sprint. Target removal: Phase 4.
    
    H-07 FIX: Uses shared browser instance instead of launching a new browser per URL.
    For 20 URLs, saves 20-60 seconds of browser startup overhead.
    """
    import httpx
    from bs4 import BeautifulSoup
    
    try:
        if use_playwright:
            browser = await _get_shared_browser()
            if browser:
                page = await browser.new_page()
                try:
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(2)
                    content = await page.content()
                    
                    soup = BeautifulSoup(content, "lxml")
                    for tag in soup(['script', 'style', 'nav', 'footer']):
                        tag.decompose()
                    return soup.get_text(' ', strip=True)
                finally:
                    await page.close()  # Close page, keep browser alive
        
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        ) as client:
            response = await client.get(url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "lxml")
                for tag in soup(['script', 'style', 'nav', 'footer']):
                    tag.decompose()
                return soup.get_text(' ', strip=True)
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                return None
                
    except Exception as e:
        logger.error(f"Scrape failed for {url}: {e}")
        return None


async def process_scraped_content(
    text: str,
    source_url: str,
    source_site: str
) -> Dict[str, Any]:
    """Process scraped content: extract, score, and save leads.
    
    M-13 FIX: Uses a single shared DB session for all leads from one page
    instead of opening a new connection per lead.
    """
    stats = {
        "leads_found": 0,
        "leads_saved": 0,
        "leads_skipped": 0,
        "errors": []
    }
    
    try:
        pipeline = LeadExtractionPipeline()
        result = await pipeline.extract(text, source_url=source_url, source_name=source_site)
        
        if not result.success or not result.leads:
            logger.info(f"No hotels found in {source_url}")
            return stats
        
        stats["leads_found"] = len(result.leads)
        
        # M-13: Single session for all leads from this page
        async with async_session() as session:
            for lead in result.leads:
                try:
                    hotel = lead.to_dict() if hasattr(lead, 'to_dict') else lead.__dict__
                    hotel["source_url"] = source_url
                    hotel["source_site"] = source_site
                    
                    scorer = LeadScorer()
                    breakdown = scorer.score_with_breakdown(hotel)
                    
                    if scorer.is_budget_brand(hotel):
                        logger.info(f"Skipping budget brand: {hotel.get('hotel_name')}")
                        stats["leads_skipped"] += 1
                        continue
                    
                    if breakdown.total < 20:
                        logger.info(f"Skipping low score: {hotel.get('hotel_name')} ({breakdown.total})")
                        stats["leads_skipped"] += 1
                        continue
                    
                    hotel["lead_score"] = breakdown.total
                    hotel["score_breakdown"] = {
                        "location": breakdown.location,
                        "brand": breakdown.brand,
                        "timing": breakdown.timing,
                        "room_count": breakdown.room_count,
                        "contact": breakdown.contact
                    }
                    
                    lead_id = await save_lead_to_db(hotel, session=session)
                    if lead_id:
                        stats["leads_saved"] += 1
                    else:
                        stats["leads_skipped"] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing hotel: {e}")
                    stats["errors"].append(str(e))
            
            # M-13: Single commit for all leads from this page
            await session.commit()
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        stats["errors"].append(str(e))
    
    return stats


# =============================================================================
# CELERY TASKS
# =============================================================================

@celery_app.task(bind=True, base=BaseTask, name="scrape_single_url")
def scrape_single_url(self, url: str, source_site: str = "manual") -> Dict[str, Any]:
    """Scrape a single URL"""
    logger.info(f"📥 Scraping: {url}")
    
    async def _scrape():
        text = await scrape_url_async(url)
        if not text:
            return {"success": False, "error": "Failed to fetch URL"}
        stats = await process_scraped_content(text, url, source_site)
        stats["success"] = True
        stats["url"] = url
        return stats
    
    return run_async(_scrape())


@celery_app.task(bind=True, base=BaseTask, name="scrape_source")
def scrape_source(self, source_id: int) -> Dict[str, Any]:
    """Scrape all URLs from a source"""
    async def _scrape():
        source = await get_source_by_id(source_id)
        if not source:
            return {"success": False, "error": f"Source {source_id} not found"}
        
        logger.info(f"🌐 Scraping source: {source['name']}")
        log_id = await create_scrape_log(source_id)
        
        total_stats = {
            "source_id": source_id,
            "source_name": source["name"],
            "urls_scraped": 0,
            "leads_found": 0,
            "leads_saved": 0,
            "errors": []
        }
        
        urls = source.get("entry_urls") or [source.get("base_url") or source.get("url")]
        urls = [u for u in urls if u]
        
        for url in urls:
            try:
                text = await scrape_url_async(url, source.get("use_playwright", False))
                if text:
                    total_stats["urls_scraped"] += 1
                    stats = await process_scraped_content(text, url, source["name"])
                    total_stats["leads_found"] += stats["leads_found"]
                    total_stats["leads_saved"] += stats["leads_saved"]
                    total_stats["errors"].extend(stats.get("errors", []))
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
                total_stats["errors"].append(str(e))
        
        status = "completed" if not total_stats["errors"] else "completed_with_errors"
        await update_scrape_log(
            log_id, status=status,
            urls_scraped=total_stats["urls_scraped"],
            leads_found=total_stats["leads_found"],
            leads_new=total_stats["leads_saved"],
            errors=total_stats["errors"] if total_stats["errors"] else None
        )
        await update_source_stats(source_id, total_stats["leads_saved"])
        total_stats["success"] = True
        return total_stats
    
    return run_async(_scrape())


@celery_app.task(bind=True, base=BaseTask, name="run_full_scrape")
def run_full_scrape(self) -> Dict[str, Any]:
    """
    Run full scrape of all active sources.
    Scheduled daily at 6 AM.
    
    H-08 FIX: Uses Celery group() to fan out source scrapes in parallel
    instead of the old .apply().get() loop that ran them synchronously
    in the current process. With 79 sources this reduces scrape time
    from hours to minutes.
    """
    logger.info("=" * 60)
    logger.info("🚀 STARTING FULL SCRAPE")
    logger.info("=" * 60)
    
    async def _get_sources():
        return await get_active_sources()
    
    sources = run_async(_get_sources())
    
    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "sources_total": len(sources),
        "sources_scraped": 0,
        "total_leads_found": 0,
        "total_leads_saved": 0,
        "errors": []
    }
    
    if not sources:
        results["success"] = True
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results
    
    # H-08: Fan out all source scrapes in parallel using Celery group()
    # Instead of: for source in sources: scrape_source.apply(args=[id]).get()
    # This dispatches all tasks to workers and waits for all to complete.
    job = group(
        scrape_source.s(source['id']) for source in sources
    )
    
    # Apply the group and wait for all results (with a generous timeout)
    # timeout = 30 min per source max, but they run in parallel
    timeout_seconds = max(len(sources) * 60, 1800)  # At least 30 min
    group_result = job.apply_async()
    
    try:
        source_results = group_result.get(
            timeout=timeout_seconds,
            propagate=False  # Don't raise on individual task failures
        )
    except Exception as e:
        logger.error(f"Group execution error: {e}")
        results["errors"].append(f"Group error: {str(e)}")
        source_results = []
    
    # Aggregate results from all sources
    for source_result in source_results:
        if isinstance(source_result, Exception):
            results["errors"].append(str(source_result))
            continue
        if isinstance(source_result, dict):
            results["sources_scraped"] += 1
            results["total_leads_found"] += source_result.get("leads_found", 0)
            results["total_leads_saved"] += source_result.get("leads_saved", 0)
            if source_result.get("errors"):
                results["errors"].extend(source_result["errors"])
    
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    results["success"] = True
    
    logger.info("=" * 60)
    logger.info(f"✅ FULL SCRAPE COMPLETE")
    logger.info(f"   Sources: {results['sources_scraped']}/{results['sources_total']}")
    logger.info(f"   Leads found: {results['total_leads_found']}")
    logger.info(f"   Leads saved: {results['total_leads_saved']}")
    logger.info("=" * 60)
    
    return results


@celery_app.task(bind=True, base=BaseTask, name="scrape_high_priority_sources")
def scrape_high_priority_sources(self) -> Dict[str, Any]:
    """
    Scrape only high-priority sources (priority >= 8).
    Runs more frequently than full scrape.
    
    H-08: Also uses group() for parallel execution.
    """
    logger.info("🔥 Scraping high-priority sources")
    
    async def _get_priority_sources():
        async with async_session() as session:
            result = await session.execute(
                select(Source)
                .where(Source.is_active == True)
                .where(Source.priority >= 8)
                .order_by(Source.priority.desc())
            )
            sources = result.scalars().all()
            return [{"id": s.id, "name": s.name} for s in sources]
    
    sources = run_async(_get_priority_sources())
    
    results = {
        "sources_scraped": 0,
        "leads_saved": 0,
        "errors": []
    }
    
    if not sources:
        results["success"] = True
        return results
    
    # H-08: Parallel execution with group()
    job = group(scrape_source.s(s['id']) for s in sources)
    group_result = job.apply_async()
    
    try:
        source_results = group_result.get(timeout=1800, propagate=False)
    except Exception as e:
        results["errors"].append(str(e))
        source_results = []
    
    for sr in source_results:
        if isinstance(sr, dict):
            results["sources_scraped"] += 1
            results["leads_saved"] += sr.get("leads_saved", 0)
        elif isinstance(sr, Exception):
            results["errors"].append(str(sr))
    
    results["success"] = True
    return results


# =============================================================================
# MAINTENANCE TASKS
# =============================================================================

@celery_app.task(name="update_all_embeddings")
def update_all_embeddings() -> Dict[str, Any]:
    """Update embeddings for leads that don't have them"""
    logger.info("🔄 Updating embeddings...")
    # TODO: Implement embedding updates using sentence-transformers
    return {"status": "not_implemented_yet"}


@celery_app.task(name="check_duplicates")
def check_duplicates() -> Dict[str, Any]:
    """Check for and merge duplicate leads"""
    logger.info("🔍 Checking for duplicates...")
    # TODO: Implement duplicate detection using pgvector
    return {"status": "not_implemented_yet"}


# =============================================================================
# CRM SYNC TASKS
# =============================================================================

@celery_app.task(name="sync_approved_to_insightly")
def sync_approved_to_insightly() -> Dict[str, Any]:
    """Sync approved leads to Insightly CRM"""
    logger.info("📤 Syncing to Insightly...")
    
    async def _sync():
        from app.services.insightly import insightly_client
        
        if not insightly_client.enabled:
            return {"success": False, "error": "Insightly not configured"}
        
        async with async_session() as session:
            result = await session.execute(
                select(PotentialLead)
                .where(PotentialLead.status == "approved")
                .where(PotentialLead.insightly_id.is_(None))
                .limit(50)
            )
            leads = result.scalars().all()
        
        synced = 0
        errors = []
        
        for lead in leads:
            try:
                result = await insightly_client.create_potential_lead({
                    "hotel_name": lead.hotel_name,
                    "contact_email": lead.contact_email,
                    "contact_phone": lead.contact_phone,
                    "city": lead.city,
                    "state": lead.state,
                    "country": lead.country,
                    "opening_date": lead.opening_date,
                    "room_count": lead.room_count,
                    "lead_score": lead.lead_score,
                    "source_url": lead.source_url,
                })
                
                if result:
                    async with async_session() as session:
                        await session.execute(
                            update(PotentialLead)
                            .where(PotentialLead.id == lead.id)
                            .values(
                                insightly_id=result.get("RECORD_ID"),
                                synced_at=datetime.now(timezone.utc)
                            )
                        )
                        await session.commit()
                    synced += 1
                    
            except Exception as e:
                errors.append(f"{lead.hotel_name}: {str(e)}")
        
        return {
            "success": True,
            "synced": synced,
            "total": len(leads),
            "errors": errors
        }
    
    return run_async(_sync())


@celery_app.task(name="convert_lead_to_insightly")
def convert_lead_to_insightly(lead_id: int) -> Dict[str, Any]:
    """Convert a single approved lead to Insightly standard Lead"""
    logger.info(f"📤 Converting lead {lead_id} to Insightly")
    
    async def _convert():
        from app.services.insightly import insightly_client
        
        async with async_session() as session:
            result = await session.execute(
                select(PotentialLead).where(PotentialLead.id == lead_id)
            )
            lead = result.scalar_one_or_none()
        
        if not lead:
            return {"success": False, "error": "Lead not found"}
        
        if not lead.insightly_id:
            return {"success": False, "error": "Lead not synced to Insightly yet"}
        
        result = await insightly_client.convert_to_lead(lead.insightly_id)
        
        if result:
            return {"success": True, "insightly_lead_id": result.get("LEAD_ID")}
        else:
            return {"success": False, "error": "Conversion failed"}
    
    return run_async(_convert())


# =============================================================================
# UTILITY TASKS
# =============================================================================

@celery_app.task(name="health_check")
def health_check() -> Dict[str, Any]:
    """Simple health check task"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "worker": "celery"
    }


__all__ = [
    "scrape_single_url",
    "scrape_source",
    "run_full_scrape",
    "scrape_high_priority_sources",
    "update_all_embeddings",
    "check_duplicates",
    "sync_approved_to_insightly",
    "convert_lead_to_insightly",
    "health_check",
]