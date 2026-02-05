"""
SMART LEAD HUNTER - MASTER ORCHESTRATOR
========================================
Coordinates all components:
1. SCRAPING ENGINE - Fetches content
2. INTELLIGENT PIPELINE - Classifies + Extracts leads
3. SMART DEDUPLICATOR - Removes duplicates
4. SCORER - Ranks leads (via pipeline)
5. DATABASE - Saves leads

Usage:
    python -m app.services.orchestrator

    Or in code:
        orchestrator = LeadHunterOrchestrator()
        await orchestrator.run()
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import select

# Database
from app.database import async_session
from app.models import PotentialLead
from app.services.scorer import calculate_lead_score

logger = logging.getLogger(__name__)

# Try imports
try:
    from app.services.source_learning import SourceLearningSystem
    LEARNING_AVAILABLE = True
except ImportError:
    LEARNING_AVAILABLE = False

try:
    from app.services.intelligent_pipeline import (
        IntelligentPipeline,
        PipelineConfig,
        PipelineResult,
        ExtractedLead,
    )
    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False
    logger.warning("Intelligent pipeline not available")

try:
    from app.services.smart_deduplicator import SmartDeduplicator, MergedLead
    SMART_DEDUP_AVAILABLE = True
except ImportError:
    SMART_DEDUP_AVAILABLE = False


@dataclass
class PipelineStats:
    """Statistics from a pipeline run"""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    sources_attempted: int = 0
    sources_successful: int = 0
    pages_scraped: int = 0
    pages_processed: int = 0
    leads_extracted: int = 0
    leads_after_dedup: int = 0
    leads_saved: int = 0
    leads_skipped_duplicates: int = 0
    
    high_quality_leads: int = 0
    medium_quality_leads: int = 0
    low_quality_leads: int = 0
    
    leads_with_email: int = 0
    leads_with_phone: int = 0
    leads_with_contact_name: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.end_time else None,
            "sources_attempted": self.sources_attempted,
            "sources_successful": self.sources_successful,
            "pages_scraped": self.pages_scraped,
            "pages_processed": self.pages_processed,
            "leads_extracted": self.leads_extracted,
            "leads_after_dedup": self.leads_after_dedup,
            "leads_saved": self.leads_saved,
            "leads_skipped_duplicates": self.leads_skipped_duplicates,
            "high_quality_leads": self.high_quality_leads,
            "medium_quality_leads": self.medium_quality_leads,
            "low_quality_leads": self.low_quality_leads,
        }


class LeadHunterOrchestrator:
    """
    Master orchestrator for Smart Lead Hunter.
    
    Usage:
        orchestrator = LeadHunterOrchestrator()
        await orchestrator.initialize()
        leads = await orchestrator.run()
    """
    
    def __init__(
        self,
        gemini_api_key: Optional[str] = None,
        use_ollama: bool = True,
        output_dir: str = "./output",
        max_concurrent_scrapes: int = 5,
        save_to_database: bool = True,
    ):
        self.gemini_api_key = gemini_api_key
        self.use_ollama = use_ollama
        self.output_dir = Path(output_dir)
        self.max_concurrent_scrapes = max_concurrent_scrapes
        self.save_to_database = save_to_database
        
        self.scraping_engine = None
        self.pipeline = None
        self.deduplicator = None
        self.learning_system = None
        
        self.stats = PipelineStats()
        self._initialized = False
    
    async def initialize(self):
        """Initialize all components"""
        if self._initialized:
            return
        
        logger.info("🚀 Initializing Smart Lead Hunter...")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Scraping engine
        try:
            from app.services.scraping_engine import ScrapingEngine
            self.scraping_engine = ScrapingEngine()
            await self.scraping_engine.initialize()
            logger.info("✅ Scraping engine initialized")
        except Exception as e:
            logger.error(f"❌ Scraping engine failed: {e}")
            raise
        
        # Unified pipeline
        if PIPELINE_AVAILABLE:
            config = PipelineConfig(gemini_api_key=self.gemini_api_key or "")
            self.pipeline = IntelligentPipeline(config)
            logger.info("✅ Intelligent pipeline initialized")
        else:
            raise RuntimeError("Intelligent pipeline not available")
        
        # Deduplicator
        if SMART_DEDUP_AVAILABLE:
            self.deduplicator = SmartDeduplicator(threshold=0.75)
            logger.info("✅ Smart deduplicator initialized")
        else:
            logger.warning("⚠️ Smart deduplicator not available")
        
        # Learning system
        if LEARNING_AVAILABLE:
            try:
                self.learning_system = SourceLearningSystem()
                logger.info("✅ Learning system initialized")
            except Exception as e:
                logger.warning(f"⚠️ Learning system not available: {e}")
        else:
            self.learning_system = None
        
        self._initialized = True
        logger.info("✅ Smart Lead Hunter ready!")
    
    async def run(
        self,
        source_names: Optional[List[str]] = None,
        priority_threshold: int = 8,
        deep_crawl: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Run the full pipeline.
        
        Args:
            source_names: Specific sources (None = all high priority)
            priority_threshold: Minimum priority (default 8)
            deep_crawl: Follow links
            
        Returns:
            List of lead dictionaries
        """
        if not self._initialized:
            await self.initialize()
        
        self.stats = PipelineStats()
        logger.info("=" * 60)
        logger.info("STARTING LEAD DISCOVERY PIPELINE")
        logger.info("=" * 60)
        
        # PHASE 1: SCRAPING
        logger.info("\n📡 PHASE 1: SCRAPING...")
        
        if source_names:
            scrape_results = await self.scraping_engine.scrape_sources(
                source_names, deep=deep_crawl, max_concurrent=self.max_concurrent_scrapes
            )
        else:
            scrape_results = await self.scraping_engine.scrape_all_sources(
                deep=deep_crawl, max_concurrent=self.max_concurrent_scrapes,
                priority_threshold=priority_threshold
            )
        
        # Collect pages
        all_pages = []
        for source_name, results in scrape_results.items():
            self.stats.sources_attempted += 1
            successful = [r for r in results if r.success]
            if successful:
                self.stats.sources_successful += 1
            self.stats.pages_scraped += len(successful)
            
            for result in successful:
                all_pages.append({
                    "source_name": source_name,
                    "url": result.url,
                    "content": result.text or result.html or "",
                })
        
        logger.info(f"✅ Scraped {self.stats.pages_scraped} pages from {self.stats.sources_successful} sources")
        
        # PHASE 2: EXTRACTION (using unified pipeline)
        logger.info("\n🧠 PHASE 2: INTELLIGENT EXTRACTION...")
        
        pages_for_pipeline = [
            {'url': p['url'], 'content': p['content'], 'source': p['source_name']}
            for p in all_pages
        ]
        
        pipeline_result = await self.pipeline.process_pages(
            pages_for_pipeline,
            source_name=source_names[0] if source_names and len(source_names) == 1 else "Multiple"
        )
        
        self.stats.pages_processed = pipeline_result.pages_classified
        self.stats.leads_extracted = pipeline_result.leads_extracted
        
        # PHASE 3: DEDUPLICATION
        logger.info("\n🔄 PHASE 3: DEDUPLICATION...")
        
        if self.deduplicator and SMART_DEDUP_AVAILABLE:
            leads_for_dedup = [lead.to_dict() for lead in pipeline_result.final_leads]
            merged_leads = self.deduplicator.deduplicate(leads_for_dedup)
            
            # Convert back to ExtractedLead
            unique_leads = []
            for lead in merged_leads:
                extracted = ExtractedLead(
                    hotel_name=lead.hotel_name,
                    brand=lead.brand,
                    property_type=lead.property_type,
                    city=lead.city,
                    state=lead.state,
                    country=lead.country,
                    opening_date=lead.opening_date,
                    opening_status=getattr(lead, 'opening_status', ''),
                    room_count=lead.room_count,
                    management_company=lead.management_company,
                    developer=lead.developer,
                    owner=getattr(lead, 'owner', ''),
                    contact_name=lead.contact_name,
                    contact_title=lead.contact_title,
                    contact_email=lead.contact_email,
                    contact_phone=lead.contact_phone,
                    key_insights=getattr(lead, 'key_insights', ''),
                    source_url=lead.source_urls[0] if lead.source_urls else '',
                    source_name=lead.source_names[0] if lead.source_names else '',
                    source_urls=lead.source_urls,
                    source_names=lead.source_names,
                    merged_from_count=lead.merged_from_count,
                    confidence_score=lead.confidence_score,
                    qualification_score=getattr(lead, 'qualification_score', 0),
                )
                
                # Add merge note to insights
                if lead.merged_from_count > 1:
                    merge_note = f"\n\n📎 Merged from {lead.merged_from_count} sources"
                    extracted.key_insights = (extracted.key_insights or '') + merge_note
                
                unique_leads.append(extracted)
            
            dedup_stats = self.deduplicator.get_stats()
            logger.info(f"   ✅ {dedup_stats['duplicates_found']} duplicates merged")
            logger.info(f"   📊 {len(unique_leads)} unique leads")
        else:
            unique_leads = pipeline_result.final_leads
        
        self.stats.leads_after_dedup = len(unique_leads)
        
        # Count quality levels
        for lead in unique_leads:
            score = lead.qualification_score
            if score >= 70:
                self.stats.high_quality_leads += 1
            elif score >= 40:
                self.stats.medium_quality_leads += 1
            else:
                self.stats.low_quality_leads += 1
            
            if lead.contact_email:
                self.stats.leads_with_email += 1
            if lead.contact_phone:
                self.stats.leads_with_phone += 1
            if lead.contact_name:
                self.stats.leads_with_contact_name += 1
        
        self.stats.end_time = datetime.now()
        
        # Record learnings
        if LEARNING_AVAILABLE:
            self._record_learnings(scrape_results, unique_leads)
        
        # Convert to dicts
        lead_dicts = [lead.to_dict() for lead in unique_leads]
        lead_dicts.sort(key=lambda x: -x.get("qualification_score", 0))
        
        # PHASE 4: SAVE TO DATABASE
        if self.save_to_database:
            db_result = await self.save_leads_to_database(lead_dicts)
            self.stats.leads_saved = db_result['saved']
            self.stats.leads_skipped_duplicates = db_result['duplicates']
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        self._print_summary()
        
        return lead_dicts
    
    async def save_leads_to_database(self, leads: list) -> dict:
        """Save leads to database"""
        logger.info(f"\n💾 SAVING {len(leads)} LEADS TO DATABASE...")
        
        saved = 0
        duplicates = 0
        errors = 0
        
        def normalize_name(name: str) -> str:
            if not name:
                return ""
            return re.sub(r'[^a-z0-9\s]', '', name.lower()).strip()
        
        def extract_year(opening_date: str) -> int:
            if not opening_date:
                return None
            match = re.search(r'20\d{2}', str(opening_date))
            return int(match.group()) if match else None
        
        async with async_session() as db:
            for lead_dict in leads:
                try:
                    hotel_name = (lead_dict.get('hotel_name') or '').strip()
                    if not hotel_name:
                        errors += 1
                        continue
                    
                    normalized = normalize_name(hotel_name)
                    
                    # Check for existing
                    result = await db.execute(
                        select(PotentialLead).where(
                            PotentialLead.hotel_name_normalized == normalized
                        )
                    )
                    if result.scalars().first():
                        duplicates += 1
                        continue
                    
                    # Calculate score
                    score_result = calculate_lead_score(
                        hotel_name=hotel_name,
                        city=lead_dict.get('city'),
                        state=lead_dict.get('state'),
                        country=lead_dict.get('country', 'USA'),
                        opening_date=lead_dict.get('opening_date'),
                        room_count=lead_dict.get('room_count'),
                        contact_name=lead_dict.get('contact_name'),
                        contact_email=lead_dict.get('contact_email'),
                        contact_phone=lead_dict.get('contact_phone'),
                        brand=lead_dict.get('brand'),
                    )
                    
                    if not score_result['should_save']:
                        logger.info(f"   ⏭️ Skipped: {hotel_name} - {score_result['skip_reason']}")
                        duplicates += 1
                        continue
                    
                    # Create lead
                    room_count = None
                    try:
                        room_count = int(float(lead_dict.get('room_count', 0) or 0))
                        if room_count == 0:
                            room_count = None
                    except:
                        pass
                    
                    lead = PotentialLead(
                        hotel_name=hotel_name,
                        hotel_name_normalized=normalized,
                        brand=lead_dict.get('brand') or None,
                        brand_tier=score_result.get('brand_tier'),
                        hotel_type=lead_dict.get('property_type') or lead_dict.get('hotel_type'),
                        city=lead_dict.get('city'),
                        state=lead_dict.get('state'),
                        country=lead_dict.get('country', 'USA'),
                        location_type=score_result.get('location_type'),
                        opening_date=lead_dict.get('opening_date'),
                        opening_year=score_result.get('opening_year') or extract_year(lead_dict.get('opening_date')),
                        room_count=room_count,
                        contact_name=lead_dict.get('contact_name'),
                        contact_title=lead_dict.get('contact_title'),
                        contact_email=lead_dict.get('contact_email'),
                        contact_phone=lead_dict.get('contact_phone'),
                        description=lead_dict.get('key_insights'),
                        source_url=lead_dict.get('source_url'),
                        source_site=lead_dict.get('source_name'),
                        lead_score=score_result['total_score'],
                        score_breakdown=score_result['breakdown'],
                        status='new',
                        scraped_at=datetime.now(timezone.utc),
                        created_at=datetime.now(timezone.utc),
                    )
                    
                    db.add(lead)
                    saved += 1
                    
                    quality = "🔴 HOT" if lead.lead_score >= 70 else "🟠 WARM" if lead.lead_score >= 50 else "🔵 COOL"
                    logger.info(f"   {quality} [{lead.lead_score}] {hotel_name}")
                    
                except Exception as e:
                    logger.error(f"   ❌ Error: {lead_dict.get('hotel_name', 'unknown')}: {e}")
                    errors += 1
            
            await db.commit()
        
        logger.info(f"\n✅ SAVED: {saved} | Duplicates: {duplicates} | Errors: {errors}")
        return {'saved': saved, 'duplicates': duplicates, 'errors': errors}
    
    def _print_summary(self):
        """Print summary"""
        s = self.stats
        duration = (s.end_time - s.start_time).total_seconds() if s.end_time else 0
        
        print(f"""
📊 PIPELINE SUMMARY
────────────────────────────────────────
⏱️  Duration: {duration:.1f}s

📡 SCRAPING: {s.pages_scraped} pages from {s.sources_successful}/{s.sources_attempted} sources
🔍 EXTRACTION: {s.leads_extracted} leads → {s.leads_after_dedup} unique
💾 DATABASE: {s.leads_saved} saved, {s.leads_skipped_duplicates} skipped

⭐ QUALITY: 🔴 {s.high_quality_leads} HOT | 🟠 {s.medium_quality_leads} WARM | 🔵 {s.low_quality_leads} COOL
📧 CONTACTS: {s.leads_with_email} email | {s.leads_with_phone} phone | {s.leads_with_contact_name} name
────────────────────────────────────────
🎉 Leads available in dashboard!
""")
    
    def _record_learnings(self, scrape_results: Dict, leads: List):
        """Record learnings about which URLs produced leads."""
        if not LEARNING_AVAILABLE or not self.learning_system:
            return
        
        try:
            # Build map of URL -> leads
            url_to_leads = {}
            for lead in leads:
                source_url = lead.source_url if hasattr(lead, 'source_url') else lead.get('source_url', '')
                if source_url:
                    if source_url not in url_to_leads:
                        url_to_leads[source_url] = []
                    url_to_leads[source_url].append(lead)
            
            # Record each URL's result
            for source_name, results in scrape_results.items():
                for result in results:
                    if not result.success:
                        continue
                    
                    url = result.url
                    leads_from_url = url_to_leads.get(url, [])
                    produced_lead = len(leads_from_url) > 0
                    
                    # Calculate lead quality
                    lead_quality = None
                    lead_location = None
                    
                    if leads_from_url:
                        # Get average confidence
                        qualities = []
                        for lead in leads_from_url:
                            if hasattr(lead, 'confidence_score'):
                                qualities.append(lead.confidence_score)
                            elif isinstance(lead, dict) and lead.get('confidence_score'):
                                qualities.append(lead['confidence_score'])
                        if qualities:
                            lead_quality = sum(qualities) / len(qualities)
                        
                        # Get location type
                        for lead in leads_from_url:
                            if hasattr(lead, 'location_type'):
                                lead_location = lead.location_type
                            elif isinstance(lead, dict):
                                lead_location = lead.get('location_type')
                            if lead_location:
                                break
                    
                    # Record to learning system
                    self.learning_system.record_result(
                        source_name=source_name,
                        url=url,
                        produced_lead=produced_lead,
                        lead_quality=lead_quality,
                        lead_location=lead_location,
                        response_time_ms=getattr(result, 'crawl_time_ms', 0)
                    )
            
            self.learning_system.save()
            logger.info("📚 Learnings recorded")
            
        except Exception as e:
            logger.warning(f"Could not record learnings: {e}")
    
    async def export_leads(self, leads: List[Dict], filename: str = "leads.json", format: str = "json") -> str:
        """Export leads to file"""
        output_path = self.output_dir / filename
        
        if format == "json":
            with open(output_path, 'w') as f:
                json.dump({"exported_at": datetime.now().isoformat(), "leads": leads}, f, indent=2)
        elif format == "csv":
            import csv
            if leads:
                with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=leads[0].keys())
                    writer.writeheader()
                    writer.writerows(leads)
        
        logger.info(f"✅ Exported {len(leads)} leads to {output_path}")
        return str(output_path)
    
    async def close(self):
        """Clean up"""
        if self.scraping_engine:
            await self.scraping_engine.close()
        logger.info("🔒 Orchestrator closed")


# =============================================================================
# CLI
# =============================================================================

async def main():
    """Main entry point"""
    import argparse
    import os
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    parser = argparse.ArgumentParser(description="Smart Lead Hunter")
    parser.add_argument("--sources", nargs="+", help="Specific sources")
    parser.add_argument("--priority", type=int, default=8, help="Min priority")
    parser.add_argument("--output", default="./output", help="Output dir")
    parser.add_argument("--no-deep", action="store_true", help="No deep crawl")
    parser.add_argument("--no-save", action="store_true", help="Don't save to DB")
    parser.add_argument("--test", action="store_true", help="Test mode (3 sources)")
    
    args = parser.parse_args()
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║   🏨 SMART LEAD HUNTER                                          ║
║   Intelligent Hotel Lead Discovery                               ║
╚══════════════════════════════════════════════════════════════════╝
""")
    
    orchestrator = LeadHunterOrchestrator(
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        output_dir=args.output,
        save_to_database=not args.no_save,
    )
    
    try:
        await orchestrator.initialize()
        
        if args.test:
            sources = list(orchestrator.scraping_engine._sources.keys())[:3]
            leads = await orchestrator.run(source_names=sources, deep_crawl=False)
        else:
            leads = await orchestrator.run(
                source_names=args.sources,
                priority_threshold=args.priority,
                deep_crawl=not args.no_deep
            )
        
        if leads:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            await orchestrator.export_leads(leads, f"leads_{timestamp}.json")
            await orchestrator.export_leads(leads, f"leads_{timestamp}.csv", format="csv")
    
    finally:
        await orchestrator.close()


if __name__ == "__main__":
    asyncio.run(main())