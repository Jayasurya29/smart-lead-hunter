"""
Scraping Tasks
--------------
Celery tasks for background web scraping and lead processing

Tasks:
- scrape_single_url: Scrape one URL
- scrape_source: Scrape all URLs from a source
- run_full_scrape: Scrape all active sources
- process_scraped_data: Extract and score leads from HTML
- sync_approved_to_insightly: Push approved leads to CRM

Usage:
    from app.tasks.scraping_tasks import run_full_scrape
    
    # Trigger async
    run_full_scrape.delay()
    
    # Trigger with countdown (delay 60 seconds)
    run_full_scrape.apply_async(countdown=60)
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from celery import chain, group, chord
from sqlalchemy.orm import Session

from .celery_app import celery_app, BaseTask
from ..database import SessionLocal
from ..models import PotentialLead, Source, ScrapeLog
from ..services.scraper import WebScraper, scrape_url
from ..services.extractor import DataExtractor, extract_leads_from_html
from ..services.scorer import LeadScorer, calculate_lead_score
from ..services.deduplicator import Deduplicator, is_duplicate
from ..services.insightly import insightly_client

logger = logging.getLogger(__name__)


def get_db() -> Session:
    """Get database session for tasks"""
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.close()
        raise


# -----------------------------------------------------------------------------
# Individual URL Scraping
# -----------------------------------------------------------------------------

@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.scrape_single_url")
def scrape_single_url(
    self,
    url: str,
    source_id: Optional[int] = None,
    depth: int = 0,
    max_depth: int = 2
) -> Dict[str, Any]:
    """
    Scrape a single URL and extract leads
    
    Args:
        url: URL to scrape
        source_id: ID of the source (for logging)
        depth: Current crawl depth
        max_depth: Maximum crawl depth
    
    Returns:
        Dict with scrape results
    """
    logger.info(f"Scraping URL: {url} (depth: {depth})")
    
    result = {
        "url": url,
        "success": False,
        "leads_found": 0,
        "child_urls": [],
        "error": None
    }
    
    db = get_db()
    
    try:
        # Scrape the URL
        scraper = WebScraper()
        html_content, child_urls = scraper.scrape(url)
        
        if not html_content:
            result["error"] = "No content returned"
            return result
        
        # Extract leads from HTML
        extractor = DataExtractor()
        raw_leads = extractor.extract(html_content, url)
        
        # Process each extracted lead
        scorer = LeadScorer()
        dedup = Deduplicator(db)
        leads_saved = 0
        
        for lead_data in raw_leads:
            # Check for duplicates
            is_dup, matches = dedup.check_duplicate(
                hotel_name=lead_data.get("hotel_name", ""),
                city=lead_data.get("city"),
                state=lead_data.get("state")
            )
            
            if is_dup:
                logger.debug(f"Skipping duplicate: {lead_data.get('hotel_name')}")
                continue
            
            # Calculate lead score
            score, breakdown = scorer.calculate_score(lead_data)
            lead_data["lead_score"] = score
            lead_data["score_breakdown"] = breakdown
            
            # Save to database
            lead = PotentialLead(
                hotel_name=lead_data.get("hotel_name"),
                hotel_name_normalized=dedup._normalize_text(lead_data.get("hotel_name", "")),
                contact_email=lead_data.get("contact_email"),
                contact_phone=lead_data.get("contact_phone"),
                contact_name=lead_data.get("contact_name"),
                city=lead_data.get("city"),
                state=lead_data.get("state"),
                country=lead_data.get("country", "USA"),
                opening_date=lead_data.get("opening_date"),
                room_count=lead_data.get("room_count"),
                hotel_type=lead_data.get("hotel_type"),
                brand=lead_data.get("brand"),
                lead_score=score,
                score_breakdown=breakdown,
                source_url=url,
                source_id=source_id,
                status="new",
                raw_data=lead_data
            )
            
            db.add(lead)
            leads_saved += 1
        
        db.commit()
        
        # Update embedding for new leads
        for lead in db.query(PotentialLead).filter(
            PotentialLead.source_url == url,
            PotentialLead.embedding.is_(None)
        ).all():
            dedup.update_embedding(lead.id)
        
        result["success"] = True
        result["leads_found"] = leads_saved
        result["child_urls"] = child_urls[:10] if depth < max_depth else []
        
        logger.info(f"Scraped {url}: {leads_saved} leads saved")
        
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        result["error"] = str(e)
        db.rollback()
        raise self.retry(exc=e)
        
    finally:
        db.close()
    
    return result


# -----------------------------------------------------------------------------
# Source-Level Scraping
# -----------------------------------------------------------------------------

@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.scrape_source")
def scrape_source(self, source_id: int) -> Dict[str, Any]:
    """
    Scrape all entry URLs for a source
    
    Args:
        source_id: Database ID of the source
    
    Returns:
        Summary of scraping results
    """
    db = get_db()
    
    try:
        # Get source configuration
        source = db.query(Source).filter(Source.id == source_id).first()
        
        if not source:
            return {"error": f"Source {source_id} not found"}
        
        if not source.is_active:
            return {"skipped": True, "reason": "Source is inactive"}
        
        logger.info(f"Starting scrape for source: {source.name}")
        
        # Create scrape log entry
        scrape_log = ScrapeLog(
            source_id=source_id,
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        db.add(scrape_log)
        db.commit()
        
        # Get entry URLs
        entry_urls = source.entry_urls or [source.base_url]
        
        results = {
            "source_id": source_id,
            "source_name": source.name,
            "urls_scraped": 0,
            "leads_found": 0,
            "errors": []
        }
        
        # Scrape each entry URL
        for url in entry_urls:
            try:
                url_result = scrape_single_url(
                    url=url,
                    source_id=source_id,
                    depth=0,
                    max_depth=source.max_depth or 2
                )
                
                results["urls_scraped"] += 1
                results["leads_found"] += url_result.get("leads_found", 0)
                
                # Queue child URLs for scraping
                for child_url in url_result.get("child_urls", []):
                    scrape_single_url.delay(
                        url=child_url,
                        source_id=source_id,
                        depth=1,
                        max_depth=source.max_depth or 2
                    )
                    
            except Exception as e:
                results["errors"].append({"url": url, "error": str(e)})
        
        # Update scrape log
        scrape_log.completed_at = datetime.now(timezone.utc)
        scrape_log.status = "completed" if not results["errors"] else "completed_with_errors"
        scrape_log.urls_scraped = results["urls_scraped"]
        scrape_log.leads_found = results["leads_found"]
        scrape_log.errors = results["errors"]
        
        # Update source last_scraped
        source.last_scraped = datetime.now(timezone.utc)
        
        db.commit()
        
        logger.info(
            f"Completed scrape for {source.name}: "
            f"{results['urls_scraped']} URLs, {results['leads_found']} leads"
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Error scraping source {source_id}: {e}")
        raise self.retry(exc=e)
        
    finally:
        db.close()


# -----------------------------------------------------------------------------
# Full System Scrape
# -----------------------------------------------------------------------------

@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.run_full_scrape")
def run_full_scrape(self) -> Dict[str, Any]:
    """
    Run a full scrape of all active sources
    
    This is the main daily scraping task.
    
    Returns:
        Summary of all scraping results
    """
    logger.info("Starting full system scrape")
    
    db = get_db()
    
    try:
        # Get all active sources
        sources = db.query(Source).filter(Source.is_active == True).all()
        
        if not sources:
            return {"error": "No active sources found"}
        
        # Queue scrape tasks for each source
        # Using group() to run in parallel (with rate limiting)
        tasks = group(
            scrape_source.s(source.id) for source in sources
        )
        
        # Execute and wait for results
        result = tasks.apply_async()
        
        return {
            "status": "started",
            "sources_queued": len(sources),
            "task_group_id": result.id
        }
        
    finally:
        db.close()


@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.scrape_high_priority_sources")
def scrape_high_priority_sources(self) -> Dict[str, Any]:
    """
    Scrape only high-priority sources (luxury chains, aggregators)
    
    Runs more frequently than full scrape.
    """
    logger.info("Starting high-priority source scrape")
    
    db = get_db()
    
    try:
        # Get high-priority sources (priority >= 8)
        sources = db.query(Source).filter(
            Source.is_active == True,
            Source.priority >= 8
        ).all()
        
        if not sources:
            return {"message": "No high-priority sources found"}
        
        # Queue scrape tasks
        for source in sources:
            scrape_source.delay(source.id)
        
        return {
            "status": "started",
            "sources_queued": len(sources)
        }
        
    finally:
        db.close()


# -----------------------------------------------------------------------------
# Maintenance Tasks
# -----------------------------------------------------------------------------

@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.update_all_embeddings")
def update_all_embeddings(self, batch_size: int = 500) -> Dict[str, Any]:
    """
    Update embeddings for all leads that don't have one
    
    Args:
        batch_size: Number of leads to process per batch
    """
    logger.info("Starting embedding update task")
    
    db = get_db()
    
    try:
        dedup = Deduplicator(db)
        stats = dedup.bulk_update_embeddings(batch_size=batch_size)
        
        return stats
        
    finally:
        db.close()


@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.check_duplicates")
def check_duplicates(self) -> Dict[str, Any]:
    """
    Find and flag duplicate leads
    
    Checks recent leads against the full database.
    """
    logger.info("Starting duplicate check task")
    
    db = get_db()
    
    try:
        # Get leads from last 24 hours
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        
        recent_leads = db.query(PotentialLead).filter(
            PotentialLead.created_at >= cutoff,
            PotentialLead.status == "new"
        ).all()
        
        dedup = Deduplicator(db)
        duplicates_found = 0
        
        for lead in recent_leads:
            is_dup, matches = dedup.check_duplicate(
                hotel_name=lead.hotel_name,
                city=lead.city,
                state=lead.state
            )
            
            if is_dup and matches:
                # Check if any match is older (this lead is the duplicate)
                for match in matches:
                    if match.lead_id != lead.id:
                        # Mark as potential duplicate
                        lead.status = "duplicate"
                        lead.duplicate_of_id = match.lead_id
                        duplicates_found += 1
                        break
        
        db.commit()
        
        return {
            "leads_checked": len(recent_leads),
            "duplicates_found": duplicates_found
        }
        
    finally:
        db.close()


# -----------------------------------------------------------------------------
# CRM Sync Tasks
# -----------------------------------------------------------------------------

@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.sync_approved_to_insightly")
def sync_approved_to_insightly(self) -> Dict[str, Any]:
    """
    Sync approved leads to Insightly CRM
    
    Finds leads with status "approved" that haven't been synced yet.
    """
    logger.info("Starting Insightly sync task")
    
    db = get_db()
    
    try:
        # Get approved leads not yet synced
        approved_leads = db.query(PotentialLead).filter(
            PotentialLead.status == "approved",
            PotentialLead.insightly_id.is_(None)
        ).limit(50).all()
        
        if not approved_leads:
            return {"message": "No approved leads to sync"}
        
        results = {
            "synced": 0,
            "failed": 0,
            "errors": []
        }
        
        for lead in approved_leads:
            try:
                # Push to Insightly
                lead_data = {
                    "hotel_name": lead.hotel_name,
                    "contact_email": lead.contact_email,
                    "contact_phone": lead.contact_phone,
                    "contact_first_name": lead.contact_name.split()[0] if lead.contact_name else "",
                    "contact_last_name": " ".join(lead.contact_name.split()[1:]) if lead.contact_name else "",
                    "city": lead.city,
                    "state": lead.state,
                    "country": lead.country,
                    "opening_date": lead.opening_date.isoformat() if lead.opening_date else None,
                    "room_count": lead.room_count,
                    "hotel_type": lead.hotel_type,
                    "brand": lead.brand,
                    "lead_score": lead.lead_score,
                    "source_url": lead.source_url,
                    "slh_id": str(lead.id),
                    "status": "New"
                }
                
                # Create in Insightly
                import asyncio
                result = asyncio.run(
                    insightly_client.create_potential_lead(lead_data)
                )
                
                if result:
                    lead.insightly_id = result.get("RECORD_ID")
                    lead.synced_at = datetime.now(timezone.utc)
                    results["synced"] += 1
                else:
                    results["failed"] += 1
                    results["errors"].append({
                        "lead_id": lead.id,
                        "error": "API returned no result"
                    })
                    
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({
                    "lead_id": lead.id,
                    "error": str(e)
                })
        
        db.commit()
        
        logger.info(
            f"Insightly sync complete: {results['synced']} synced, "
            f"{results['failed']} failed"
        )
        
        return results
        
    finally:
        db.close()


@celery_app.task(bind=True, base=BaseTask, name="app.tasks.scraping_tasks.convert_lead_to_insightly")
def convert_lead_to_insightly(self, lead_id: int) -> Dict[str, Any]:
    """
    Convert a single approved lead to Insightly standard Lead
    
    Called when a potential lead is manually approved and ready
    to become a real Lead in Insightly.
    
    Args:
        lead_id: Database ID of the potential lead
    """
    logger.info(f"Converting lead {lead_id} to Insightly Lead")
    
    db = get_db()
    
    try:
        lead = db.query(PotentialLead).filter(
            PotentialLead.id == lead_id
        ).first()
        
        if not lead:
            return {"error": f"Lead {lead_id} not found"}
        
        if not lead.insightly_id:
            return {"error": "Lead not synced to Insightly yet"}
        
        # Convert to standard Lead
        import asyncio
        result = asyncio.run(
            insightly_client.convert_to_lead(lead.insightly_id)
        )
        
        if result:
            lead.status = "converted"
            lead.converted_at = datetime.now(timezone.utc)
            lead.converted_lead_id = result.get("LEAD_ID")
            db.commit()
            
            return {
                "success": True,
                "lead_id": result.get("LEAD_ID")
            }
        else:
            return {"error": "Conversion failed"}
            
    finally:
        db.close()


# -----------------------------------------------------------------------------
# Utility Tasks
# -----------------------------------------------------------------------------

@celery_app.task(name="app.tasks.scraping_tasks.health_check")
def health_check() -> Dict[str, Any]:
    """Simple health check task"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }