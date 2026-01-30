"""
SMART LEAD HUNTER - CELERY SCRAPING TASKS
==========================================
Background tasks for automated lead scraping

Tasks:
- scrape_single_url: Scrape one URL
- scrape_source: Scrape all URLs from a source
- run_full_scrape: Daily scrape of all active sources
- scrape_high_priority_sources: Scrape priority sources more frequently

Usage:
    # Start Celery worker
    celery -A app.tasks.celery_app worker --loglevel=info
    
    # Start beat scheduler
    celery -A app.tasks.celery_app beat --loglevel=info
    
    # Trigger manually
    from app.tasks.scraping_tasks import run_full_scrape
    run_full_scrape.delay()
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from celery import shared_task
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.tasks.celery_app import celery_app, BaseTask
from app.database import async_session
from app.models import PotentialLead, Source, ScrapeLog
from app.services.lead_extraction_pipeline import LeadExtractionPipeline
from app.services.scorer import score_lead, LeadScorer

logger = logging.getLogger(__name__)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def run_async(coro):
    """Run async function in sync context (for Celery)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


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


async def save_lead_to_db(hotel: Dict) -> Optional[int]:
    """Save extracted hotel as potential lead"""
    async with async_session() as session:
        try:
            # Check for duplicate by normalized name
            normalized_name = (hotel.get("hotel_name") or "").lower().strip()
            
            result = await session.execute(
                select(PotentialLead).where(
                    PotentialLead.hotel_name_normalized == normalized_name
                )
            )
            existing = result.scalar_one_or_none()
            
            if existing:
                logger.info(f"Duplicate found: {hotel.get('hotel_name')}")
                return None
            
            # Create new lead
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
                lead_score=hotel.get("lead_score"),
                score_breakdown=hotel.get("score_breakdown"),
                source_url=hotel.get("source_url"),
                source_site=hotel.get("source_site"),
                status="new",
                raw_data=hotel
            )
            
            session.add(lead)
            await session.commit()
            await session.refresh(lead)
            
            logger.info(f"✅ Saved lead: {lead.hotel_name} (ID: {lead.id})")
            return lead.id
            
        except Exception as e:
            logger.error(f"Failed to save lead: {e}")
            await session.rollback()
            return None


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

async def scrape_url_async(url: str, use_playwright: bool = False) -> Optional[str]:
    """
    Scrape a single URL and return text content
    
    Args:
        url: URL to scrape
        use_playwright: Whether to use Playwright for JS rendering
    
    Returns:
        Text content or None if failed
    """
    import httpx
    from bs4 import BeautifulSoup
    
    try:
        if use_playwright:
            # Use Playwright for JS-heavy sites
            try:
                from playwright.async_api import async_playwright
                
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(2)  # Wait for dynamic content
                    content = await page.content()
                    await browser.close()
                    
                    soup = BeautifulSoup(content, "lxml")
                    for tag in soup(['script', 'style', 'nav', 'footer']):
                        tag.decompose()
                    return soup.get_text(' ', strip=True)
                    
            except ImportError:
                logger.warning("Playwright not available, falling back to httpx")
        
        # Use httpx for static sites
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
    """
    Process scraped content: extract, score, and save leads
    
    Returns:
        Stats about processing
    """
    stats = {
        "leads_found": 0,
        "leads_saved": 0,
        "leads_skipped": 0,
        "errors": []
    }
    
    try:
        # Extract hotel data using AI
        pipeline = LeadExtractionPipeline()
        result = await pipeline.extract(text, source_url=source_url, source_name=source_site)
        
        if not result.success or not result.leads:
            logger.info(f"No hotels found in {source_url}")
            return stats
        
        stats["leads_found"] = len(result.leads)
        
        for lead in result.leads:
            try:
                # Convert to dict for scoring
                hotel = lead.to_dict() if hasattr(lead, 'to_dict') else lead.__dict__
                
                # Add source info
                hotel["source_url"] = source_url
                hotel["source_site"] = source_site
                
                # Calculate score using new scorer
                scorer = LeadScorer()
                breakdown = scorer.score_with_breakdown(hotel)
                
                # Check if should save (skip budget brands and low scores)
                if scorer.is_budget_brand(hotel):
                    logger.info(f"Skipping budget brand: {hotel.get('hotel_name')}")
                    stats["leads_skipped"] += 1
                    continue
                
                if breakdown.total < 20:
                    logger.info(f"Skipping low score: {hotel.get('hotel_name')} ({breakdown.total})")
                    stats["leads_skipped"] += 1
                    continue
                
                # Add score to hotel data
                hotel["lead_score"] = breakdown.total
                hotel["score_breakdown"] = {
                    "location": breakdown.location,
                    "brand": breakdown.brand,
                    "timing": breakdown.timing,
                    "room_count": breakdown.room_count,
                    "contact": breakdown.contact
                }
                
                # Save to database
                lead_id = await save_lead_to_db(hotel)
                if lead_id:
                    stats["leads_saved"] += 1
                else:
                    stats["leads_skipped"] += 1
                    
            except Exception as e:
                logger.error(f"Error processing hotel: {e}")
                stats["errors"].append(str(e))
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        stats["errors"].append(str(e))
    
    return stats


# =============================================================================
# CELERY TASKS
# =============================================================================

@celery_app.task(bind=True, base=BaseTask, name="scrape_single_url")
def scrape_single_url(self, url: str, source_site: str = "manual") -> Dict[str, Any]:
    """
    Scrape a single URL
    
    Args:
        url: URL to scrape
        source_site: Name of the source site
    
    Returns:
        Stats about the scrape
    """
    logger.info(f"📥 Scraping: {url}")
    
    async def _scrape():
        # Scrape the URL
        text = await scrape_url_async(url)
        
        if not text:
            return {"success": False, "error": "Failed to fetch URL"}
        
        # Process content
        stats = await process_scraped_content(text, url, source_site)
        stats["success"] = True
        stats["url"] = url
        
        return stats
    
    return run_async(_scrape())


@celery_app.task(bind=True, base=BaseTask, name="scrape_source")
def scrape_source(self, source_id: int) -> Dict[str, Any]:
    """
    Scrape all URLs from a source
    
    Args:
        source_id: ID of the source to scrape
    
    Returns:
        Stats about the scrape
    """
    async def _scrape():
        source = await get_source_by_id(source_id)
        
        if not source:
            return {"success": False, "error": f"Source {source_id} not found"}
        
        logger.info(f"🌐 Scraping source: {source['name']}")
        
        # Create scrape log
        log_id = await create_scrape_log(source_id)
        
        total_stats = {
            "source_id": source_id,
            "source_name": source["name"],
            "urls_scraped": 0,
            "leads_found": 0,
            "leads_saved": 0,
            "errors": []
        }
        
        # Get URLs to scrape
        urls = source.get("entry_urls") or [source.get("base_url") or source.get("url")]
        urls = [u for u in urls if u]  # Filter None
        
        for url in urls:
            try:
                # Scrape URL
                text = await scrape_url_async(url, source.get("use_playwright", False))
                
                if text:
                    total_stats["urls_scraped"] += 1
                    
                    # Process content
                    stats = await process_scraped_content(
                        text, url, source["name"]
                    )
                    
                    total_stats["leads_found"] += stats["leads_found"]
                    total_stats["leads_saved"] += stats["leads_saved"]
                    total_stats["errors"].extend(stats.get("errors", []))
                
                # Rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
                total_stats["errors"].append(str(e))
        
        # Update scrape log
        status = "completed" if not total_stats["errors"] else "completed_with_errors"
        await update_scrape_log(
            log_id,
            status=status,
            urls_scraped=total_stats["urls_scraped"],
            leads_found=total_stats["leads_found"],
            leads_new=total_stats["leads_saved"],
            errors=total_stats["errors"] if total_stats["errors"] else None
        )
        
        # Update source stats
        await update_source_stats(source_id, total_stats["leads_saved"])
        
        total_stats["success"] = True
        return total_stats
    
    return run_async(_scrape())


@celery_app.task(bind=True, base=BaseTask, name="run_full_scrape")
def run_full_scrape(self) -> Dict[str, Any]:
    """
    Run full scrape of all active sources
    
    This is scheduled to run daily at 6 AM
    """
    logger.info("=" * 60)
    logger.info("🚀 STARTING FULL SCRAPE")
    logger.info("=" * 60)
    
    async def _full_scrape():
        sources = await get_active_sources()
        
        results = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "sources_total": len(sources),
            "sources_scraped": 0,
            "total_leads_found": 0,
            "total_leads_saved": 0,
            "errors": []
        }
        
        for source in sources:
            try:
                logger.info(f"\n📍 Processing: {source['name']}")
                
                # Scrape this source (synchronously within the task)
                source_result = scrape_source.apply(args=[source['id']]).get()
                
                results["sources_scraped"] += 1
                results["total_leads_found"] += source_result.get("leads_found", 0)
                results["total_leads_saved"] += source_result.get("leads_saved", 0)
                
                if source_result.get("errors"):
                    results["errors"].extend(source_result["errors"])
                
            except Exception as e:
                logger.error(f"Failed to scrape {source['name']}: {e}")
                results["errors"].append(f"{source['name']}: {str(e)}")
        
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        results["success"] = True
        
        logger.info("=" * 60)
        logger.info(f"✅ FULL SCRAPE COMPLETE")
        logger.info(f"   Sources: {results['sources_scraped']}/{results['sources_total']}")
        logger.info(f"   Leads found: {results['total_leads_found']}")
        logger.info(f"   Leads saved: {results['total_leads_saved']}")
        logger.info("=" * 60)
        
        return results
    
    return run_async(_full_scrape())


@celery_app.task(bind=True, base=BaseTask, name="scrape_high_priority_sources")
def scrape_high_priority_sources(self) -> Dict[str, Any]:
    """
    Scrape only high-priority sources (priority >= 8)
    
    This runs more frequently than full scrape
    """
    logger.info("🔥 Scraping high-priority sources")
    
    async def _priority_scrape():
        async with async_session() as session:
            result = await session.execute(
                select(Source)
                .where(Source.is_active == True)
                .where(Source.priority >= 8)
                .order_by(Source.priority.desc())
            )
            sources = result.scalars().all()
        
        results = {
            "sources_scraped": 0,
            "leads_saved": 0,
            "errors": []
        }
        
        for source in sources:
            try:
                source_result = scrape_source.apply(args=[source.id]).get()
                results["sources_scraped"] += 1
                results["leads_saved"] += source_result.get("leads_saved", 0)
            except Exception as e:
                results["errors"].append(str(e))
        
        results["success"] = True
        return results
    
    return run_async(_priority_scrape())


# =============================================================================
# MAINTENANCE TASKS
# =============================================================================

@celery_app.task(name="update_all_embeddings")
def update_all_embeddings() -> Dict[str, Any]:
    """Update embeddings for leads that don't have them"""
    logger.info("🔄 Updating embeddings...")
    
    # TODO: Implement embedding updates using sentence-transformers
    # This is for deduplication
    
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
            # Get approved leads not yet synced
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
                    # Update lead with Insightly ID
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


# =============================================================================
# EXPORTS
# =============================================================================

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