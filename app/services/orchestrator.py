"""
SMART LEAD HUNTER - MASTER ORCHESTRATOR
========================================
The main entry point that coordinates all components:

1. SCRAPING ENGINE - Fetches content from 73+ sources
2. EXTRACTION PIPELINE - Converts text to structured leads
3. DEDUPLICATION - Removes duplicates
4. SCORING - Ranks leads by quality
5. LEARNING SYSTEM - Learns which URLs produce leads
6. EXPORT - Saves to database/files

Usage:
    python -m app.services.orchestrator

    Or in code:
        orchestrator = LeadHunterOrchestrator()
        await orchestrator.run()
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import learning system
try:
    from app.services.source_learning import SourceLearningSystem
    LEARNING_AVAILABLE = True
except ImportError:
    LEARNING_AVAILABLE = False
    logger.debug("Learning system not available")

# Try to import intelligent pipeline
try:
    from app.services.intelligent_pipeline import (
        IntelligentPipeline, 
        PipelineConfig,
        PipelineResult
    )
    INTELLIGENT_PIPELINE_AVAILABLE = True
except ImportError:
    INTELLIGENT_PIPELINE_AVAILABLE = False
    logger.debug("Intelligent pipeline not available")

# Try to import smart deduplicator
try:
    from app.services.smart_deduplicator import SmartDeduplicator, MergedLead
    SMART_DEDUP_AVAILABLE = True
except ImportError:
    SMART_DEDUP_AVAILABLE = False
    logger.debug("Smart deduplicator not available")


@dataclass
class PipelineStats:
    """Statistics from a pipeline run"""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    # Scraping stats
    sources_attempted: int = 0
    sources_successful: int = 0
    pages_scraped: int = 0
    
    # Extraction stats
    pages_processed: int = 0
    leads_extracted: int = 0
    leads_after_dedup: int = 0
    
    # Quality stats
    high_quality_leads: int = 0  # Score >= 0.7
    medium_quality_leads: int = 0  # Score 0.4-0.7
    low_quality_leads: int = 0  # Score < 0.4
    
    # Contact stats
    leads_with_email: int = 0
    leads_with_phone: int = 0
    leads_with_contact_name: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (
                (self.end_time - self.start_time).total_seconds()
                if self.end_time else None
            ),
            "sources_attempted": self.sources_attempted,
            "sources_successful": self.sources_successful,
            "pages_scraped": self.pages_scraped,
            "pages_processed": self.pages_processed,
            "leads_extracted": self.leads_extracted,
            "leads_after_dedup": self.leads_after_dedup,
            "high_quality_leads": self.high_quality_leads,
            "medium_quality_leads": self.medium_quality_leads,
            "low_quality_leads": self.low_quality_leads,
            "leads_with_email": self.leads_with_email,
            "leads_with_phone": self.leads_with_phone,
            "leads_with_contact_name": self.leads_with_contact_name,
        }


class LeadHunterOrchestrator:
    """
    Master orchestrator for the Smart Lead Hunter system.
    
    Coordinates:
    - Source management
    - Web scraping
    - AI extraction
    - Lead processing
    - Data export
    
    Usage:
        orchestrator = LeadHunterOrchestrator()
        await orchestrator.initialize()
        leads = await orchestrator.run()
        await orchestrator.export_leads(leads, "leads.json")
    """
    
    def __init__(
        self,
        gemini_api_key: Optional[str] = None,
        use_ollama: bool = True,
        output_dir: str = "./output",
        max_concurrent_scrapes: int = 5,
        max_concurrent_extractions: int = 3,
        use_intelligent_pipeline: bool = True,  # NEW: Use smart classification
    ):
        self.gemini_api_key = gemini_api_key
        self.use_ollama = use_ollama
        self.output_dir = Path(output_dir)
        self.max_concurrent_scrapes = max_concurrent_scrapes
        self.max_concurrent_extractions = max_concurrent_extractions
        self.use_intelligent_pipeline = use_intelligent_pipeline and INTELLIGENT_PIPELINE_AVAILABLE
        
        self.scraping_engine = None
        self.extraction_pipeline = None
        self.intelligent_pipeline = None  # NEW
        self.deduplicator = None
        
        self.stats = PipelineStats()
        self._initialized = False
    
    async def initialize(self):
        """Initialize all components"""
        if self._initialized:
            return
        
        logger.info("🚀 Initializing Smart Lead Hunter...")
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize scraping engine
        try:
            from app.services.scraping_engine import ScrapingEngine
            self.scraping_engine = ScrapingEngine()
            await self.scraping_engine.initialize()
            logger.info("✅ Scraping engine initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize scraping engine: {e}")
            raise
        
        # Initialize extraction pipeline
        try:
            from app.services.lead_extraction_pipeline import (
                LeadExtractionPipeline, LeadDeduplicator
            )
            self.extraction_pipeline = LeadExtractionPipeline(
                gemini_api_key=self.gemini_api_key,
                use_ollama=self.use_ollama
            )
            
            # Use SmartDeduplicator if available, otherwise fall back to LeadDeduplicator
            if SMART_DEDUP_AVAILABLE:
                self.deduplicator = SmartDeduplicator(name_threshold=0.75)
                self.smart_dedup = True
                logger.info("✅ Smart deduplicator initialized (fuzzy matching enabled)")
            else:
                self.deduplicator = LeadDeduplicator()
                self.smart_dedup = False
                logger.info("✅ Legacy deduplicator initialized")
            
            logger.info("✅ Extraction pipeline initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize extraction pipeline: {e}")
            raise
        
        # Initialize intelligent pipeline (if enabled)
        if self.use_intelligent_pipeline:
            try:
                config = PipelineConfig(
                    gemini_api_key=self.gemini_api_key or "",
                )
                self.intelligent_pipeline = IntelligentPipeline(config)
                logger.info("✅ Intelligent pipeline initialized (smart classification enabled)")
            except Exception as e:
                logger.warning(f"⚠️ Intelligent pipeline not available: {e}")
                self.use_intelligent_pipeline = False
        
        self._initialized = True
        logger.info("✅ Smart Lead Hunter ready!")
    
    async def run(
        self,
        source_names: Optional[List[str]] = None,
        priority_threshold: int = 8,
        deep_crawl: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Run the full lead discovery pipeline.
        
        Args:
            source_names: Specific sources to scrape (None = all high priority)
            priority_threshold: Minimum priority for sources (default 8)
            deep_crawl: Whether to follow links
        
        Returns:
            List of extracted leads as dictionaries
        """
        if not self._initialized:
            await self.initialize()
        
        self.stats = PipelineStats()
        logger.info("=" * 60)
        logger.info("STARTING LEAD DISCOVERY PIPELINE")
        logger.info("=" * 60)
        
        # PHASE 1: SCRAPING
        logger.info("\n📡 PHASE 1: SCRAPING SOURCES...")
        
        if source_names:
            scrape_results = await self.scraping_engine.scrape_sources(
                source_names,
                deep=deep_crawl,
                max_concurrent=self.max_concurrent_scrapes
            )
        else:
            scrape_results = await self.scraping_engine.scrape_all_sources(
                deep=deep_crawl,
                max_concurrent=self.max_concurrent_scrapes,
                priority_threshold=priority_threshold
            )
        
        # Collect all scraped pages
        all_pages = []
        for source_name, results in scrape_results.items():
            self.stats.sources_attempted += 1
            successful_pages = [r for r in results if r.success]
            if successful_pages:
                self.stats.sources_successful += 1
            self.stats.pages_scraped += len(successful_pages)
            
            for result in successful_pages:
                all_pages.append({
                    "source_name": source_name,
                    "url": result.url,
                    "text": result.text,
                    "html": result.html,
                })
        
        logger.info(
            f"✅ Scraped {self.stats.pages_scraped} pages "
            f"from {self.stats.sources_successful}/{self.stats.sources_attempted} sources"
        )
        
        # PHASE 2 & 3: INTELLIGENT EXTRACTION or LEGACY EXTRACTION
        if self.use_intelligent_pipeline and self.intelligent_pipeline:
            # === NEW INTELLIGENT PIPELINE ===
            logger.info("\n🧠 PHASE 2: INTELLIGENT CLASSIFICATION & EXTRACTION...")
            
            # Prepare pages for intelligent pipeline
            pages_for_pipeline = [
                {
                    'url': p['url'],
                    'content': p['text'] or p['html'] or '',
                    'source': p['source_name']
                }
                for p in all_pages
            ]
            
            # Run intelligent pipeline
            pipeline_result = await self.intelligent_pipeline.process_pages(
                pages_for_pipeline,
                source_name=source_names[0] if source_names and len(source_names) == 1 else "Multiple Sources"
            )
            
            # Update stats
            self.stats.pages_processed = pipeline_result.pages_classified
            self.stats.leads_extracted = pipeline_result.leads_extracted
            
            # Apply Smart Deduplication to the extracted leads
            if self.smart_dedup and SMART_DEDUP_AVAILABLE:
                logger.info(f"\n🔄 SMART DEDUPLICATION: Processing {len(pipeline_result.final_leads)} leads...")
                
                # Convert intelligent pipeline leads to dicts for deduplication
                leads_for_dedup = []
                for lead in pipeline_result.final_leads:
                    leads_for_dedup.append({
                        'hotel_name': lead.hotel_name,
                        'brand': lead.brand,
                        'property_type': lead.property_type,
                        'city': lead.city,
                        'state': lead.state,
                        'country': lead.country,
                        'opening_date': lead.opening_date,
                        'opening_status': lead.opening_status,
                        'room_count': lead.room_count,
                        'management_company': lead.management_company,
                        'developer': lead.developer,
                        'contact_name': lead.contact_name,
                        'contact_title': lead.contact_title,
                        'contact_email': lead.contact_email,
                        'contact_phone': lead.contact_phone,
                        'source_url': lead.source_url,
                        'source_name': lead.source_name,
                        'confidence_score': lead.qualification_score / 100.0,
                        'qualification_score': lead.qualification_score,
                    })
                
                # Run smart deduplication
                merged_leads = self.deduplicator.deduplicate(leads_for_dedup)
                
                # Convert MergedLead to our format
                from app.services.lead_extraction_pipeline import ExtractedLead as LegacyLead
                unique_leads = []
                for lead in merged_leads:
                    legacy_lead = LegacyLead(
                        hotel_name=lead.hotel_name,
                        brand=lead.brand,
                        hotel_type=lead.property_type,
                        city=lead.city,
                        state=lead.state,
                        country=lead.country,
                        opening_date=lead.opening_date,
                        room_count=lead.room_count,
                        management_company=lead.management_company,
                        developer=lead.developer,
                        contact_name=lead.contact_name,
                        contact_title=lead.contact_title,
                        contact_email=lead.contact_email,
                        contact_phone=lead.contact_phone,
                        source_url=lead.source_urls[0] if lead.source_urls else '',
                        source_name=lead.source_names[0] if lead.source_names else '',
                        # NEW: Store ALL source URLs for merged leads
                        source_urls=' | '.join(lead.source_urls) if lead.source_urls else '',
                        source_names=' | '.join(lead.source_names) if lead.source_names else '',
                        merged_from_count=lead.merged_from_count,
                        confidence_score=lead.confidence_score,
                    )
                    # Store merged info in key_insights
                    if lead.merged_from_count > 1:
                        legacy_lead.key_insights = f"Merged from {lead.merged_from_count} sources"
                    
                    unique_leads.append(legacy_lead)
                    
                    # Contact stats
                    if lead.contact_email:
                        self.stats.leads_with_email += 1
                    if lead.contact_phone:
                        self.stats.leads_with_phone += 1
                    if lead.contact_name:
                        self.stats.leads_with_contact_name += 1
                
                # Update dedup stats
                self.stats.leads_after_dedup = len(unique_leads)
                dedup_stats = self.deduplicator.get_stats()
                logger.info(f"   ✅ {dedup_stats['duplicates_found']} duplicates merged")
                logger.info(f"   📊 {len(unique_leads)} unique leads after smart dedup")
                
                # Recalculate quality stats based on merged leads
                self.stats.high_quality_leads = len([l for l in merged_leads if l.qualification_score >= 70])
                self.stats.medium_quality_leads = len([l for l in merged_leads if 40 <= l.qualification_score < 70])
                self.stats.low_quality_leads = len([l for l in merged_leads if l.qualification_score < 40])
                
            else:
                # Fallback: no smart dedup, just use qualified leads from pipeline
                self.stats.leads_after_dedup = pipeline_result.leads_qualified
                self.stats.high_quality_leads = pipeline_result.leads_high_quality
                self.stats.medium_quality_leads = pipeline_result.leads_medium_quality
                self.stats.low_quality_leads = pipeline_result.leads_low_quality
                
                # Convert intelligent pipeline leads to our format
                from app.services.lead_extraction_pipeline import ExtractedLead as LegacyLead
                unique_leads = []
                for lead in pipeline_result.final_leads:
                    legacy_lead = LegacyLead(
                        hotel_name=lead.hotel_name,
                        brand=lead.brand,
                        hotel_type=lead.property_type,
                        city=lead.city,
                        state=lead.state,
                        country=lead.country,
                        opening_date=lead.opening_date,
                        room_count=lead.room_count,
                        management_company=lead.management_company,
                        developer=lead.developer,
                        contact_name=lead.contact_name,
                        contact_title=lead.contact_title,
                        contact_email=lead.contact_email,
                        contact_phone=lead.contact_phone,
                        source_url=lead.source_url,
                        source_name=lead.source_name,
                        confidence_score=lead.qualification_score / 100.0,
                    )
                    unique_leads.append(legacy_lead)
                    
                    # Contact stats
                    if lead.contact_email:
                        self.stats.leads_with_email += 1
                    if lead.contact_phone:
                        self.stats.leads_with_phone += 1
                    if lead.contact_name:
                        self.stats.leads_with_contact_name += 1
            
            logger.info(f"\n✅ Intelligent pipeline complete:")
            logger.info(f"   📊 Classified: {pipeline_result.pages_relevant} relevant / {pipeline_result.pages_classified} total")
            logger.info(f"   📝 Extracted: {pipeline_result.leads_extracted} leads")
            logger.info(f"   ✅ Qualified: {pipeline_result.leads_qualified} leads")
            
        else:
            # === LEGACY PIPELINE ===
            logger.info("\n🔍 PHASE 2: EXTRACTING LEADS (Legacy Mode)...")
            
            all_leads = []
            semaphore = asyncio.Semaphore(self.max_concurrent_extractions)
            
            async def extract_page(page):
                async with semaphore:
                    result = await self.extraction_pipeline.extract(
                        page["text"] or page["html"] or "",
                        source_url=page["url"],
                        source_name=page["source_name"]
                    )
                    return result
            
            tasks = [extract_page(page) for page in all_pages]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.warning(f"Extraction error: {result}")
                    continue
                
                self.stats.pages_processed += 1
                if result.success:
                    all_leads.extend(result.leads)
                    self.stats.leads_extracted += len(result.leads)
            
            logger.info(
                f"✅ Extracted {self.stats.leads_extracted} leads "
                f"from {self.stats.pages_processed} pages"
            )
            
            # PHASE 3: SMART DEDUPLICATION (Legacy)
            logger.info("\n🔄 PHASE 3: SMART DEDUPLICATING...")
            
            if self.smart_dedup and SMART_DEDUP_AVAILABLE:
                # Convert leads to dicts for smart deduplication
                leads_for_dedup = [lead.to_dict() if hasattr(lead, 'to_dict') else lead.__dict__ for lead in all_leads]
                merged_leads = self.deduplicator.deduplicate(leads_for_dedup)
                
                # Convert back to ExtractedLead format
                from app.services.lead_extraction_pipeline import ExtractedLead as LegacyLead
                unique_leads = []
                for lead in merged_leads:
                    legacy_lead = LegacyLead(
                        hotel_name=lead.hotel_name,
                        brand=lead.brand,
                        hotel_type=lead.property_type,
                        city=lead.city,
                        state=lead.state,
                        country=lead.country,
                        opening_date=lead.opening_date,
                        room_count=lead.room_count,
                        management_company=lead.management_company,
                        developer=lead.developer,
                        contact_name=lead.contact_name,
                        contact_title=lead.contact_title,
                        contact_email=lead.contact_email,
                        contact_phone=lead.contact_phone,
                        source_url=lead.source_urls[0] if lead.source_urls else '',
                        source_name=lead.source_names[0] if lead.source_names else '',
                        # NEW: Store ALL source URLs for merged leads
                        source_urls=' | '.join(lead.source_urls) if lead.source_urls else '',
                        source_names=' | '.join(lead.source_names) if lead.source_names else '',
                        merged_from_count=lead.merged_from_count,
                        confidence_score=lead.confidence_score,
                    )
                    if lead.merged_from_count > 1:
                        legacy_lead.key_insights = f"Merged from {lead.merged_from_count} sources"
                    unique_leads.append(legacy_lead)
                
                dedup_stats = self.deduplicator.get_stats()
                logger.info(f"   ✅ {dedup_stats['duplicates_found']} duplicates merged")
            else:
                # Fallback to simple dedup
                unique_leads = self.deduplicator.deduplicate(all_leads)
            
            self.stats.leads_after_dedup = len(unique_leads)
            
            logger.info(
                f"✅ {self.stats.leads_after_dedup} unique leads "
                f"(removed {self.stats.leads_extracted - self.stats.leads_after_dedup} duplicates)"
            )
            
            # Update stats for legacy mode
            for lead in unique_leads:
                if lead.confidence_score >= 0.7:
                    self.stats.high_quality_leads += 1
                elif lead.confidence_score >= 0.4:
                    self.stats.medium_quality_leads += 1
                else:
                    self.stats.low_quality_leads += 1
                
                if lead.contact_email:
                    self.stats.leads_with_email += 1
                if lead.contact_phone:
                    self.stats.leads_with_phone += 1
                if lead.contact_name:
                    self.stats.leads_with_contact_name += 1
        
        # PHASE 4: FINAL STATS
        logger.info("\n📊 PHASE 4: ANALYZING LEADS...")
        
        self.stats.end_time = datetime.now()
        
        # PHASE 5: LEARNING (if available)
        if LEARNING_AVAILABLE:
            try:
                self._record_learnings(scrape_results, unique_leads)
            except Exception as e:
                logger.warning(f"Could not record learnings: {e}")
        
        # Convert to dicts and sort by score
        lead_dicts = [lead.to_dict() for lead in unique_leads]
        lead_dicts.sort(key=lambda x: -x["confidence_score"])
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        self._print_summary()
        
        return lead_dicts
    
    def _print_summary(self):
        """Print pipeline summary"""
        s = self.stats
        duration = (s.end_time - s.start_time).total_seconds() if s.end_time else 0
        
        print(f"""
📊 PIPELINE SUMMARY
──────────────────────────────────────────
⏱️  Duration: {duration:.1f} seconds

📡 SCRAPING:
   Sources attempted: {s.sources_attempted}
   Sources successful: {s.sources_successful}
   Pages scraped: {s.pages_scraped}

🔍 EXTRACTION:
   Pages processed: {s.pages_processed}
   Leads extracted: {s.leads_extracted}
   After smart dedup: {s.leads_after_dedup}

⭐ QUALITY:
   High quality (70+): {s.high_quality_leads}
   Medium quality (40-69): {s.medium_quality_leads}
   Low quality (<40): {s.low_quality_leads}

📧 CONTACTS:
   With email: {s.leads_with_email}
   With phone: {s.leads_with_phone}
   With name: {s.leads_with_contact_name}
──────────────────────────────────────────
""")
    
    def _record_learnings(self, scrape_results: Dict, leads: List):
        """
        Record learnings about which URLs produced leads.
        This helps the system learn and improve over time.
        """
        if not LEARNING_AVAILABLE:
            return
        
        learning_system = SourceLearningSystem()
        
        # Build URL -> lead mapping
        url_to_leads = {}
        for lead in leads:
            source_url = lead.source_url if hasattr(lead, 'source_url') else ''
            if source_url:
                if source_url not in url_to_leads:
                    url_to_leads[source_url] = []
                url_to_leads[source_url].append(lead)
        
        # Record results for each scraped URL
        for source_name, results in scrape_results.items():
            for result in results:
                if not result.success:
                    continue
                
                url = result.url
                leads_from_url = url_to_leads.get(url, [])
                produced_lead = len(leads_from_url) > 0
                
                # Get lead details
                lead_quality = None
                lead_location = None
                
                if leads_from_url:
                    # Average quality of leads from this URL
                    qualities = [l.confidence_score for l in leads_from_url 
                                if hasattr(l, 'confidence_score') and l.confidence_score]
                    if qualities:
                        lead_quality = sum(qualities) / len(qualities)
                    
                    # Determine location category
                    for lead in leads_from_url:
                        country = lead.country if hasattr(lead, 'country') else ''
                        state = lead.state if hasattr(lead, 'state') else ''
                        
                        if country in ('USA', 'United States', 'US'):
                            if state and 'Florida' in state:
                                lead_location = 'Florida'
                            else:
                                lead_location = 'USA'
                            break
                        elif country in ('Aruba', 'Bahamas', 'Jamaica', 'Puerto Rico', 
                                        'Turks and Caicos', 'Cayman Islands', 'Barbados',
                                        'St. Lucia', 'Antigua', 'Bermuda', 'Virgin Islands'):
                            lead_location = 'Caribbean'
                            break
                        else:
                            lead_location = 'International'
                
                # Record
                learning_system.record_result(
                    source_name=source_name,
                    url=url,
                    produced_lead=produced_lead,
                    lead_quality=lead_quality,
                    lead_location=lead_location,
                    response_time_ms=result.crawl_time_ms if hasattr(result, 'crawl_time_ms') else 0
                )
        
        # Save learnings
        learning_system.save()
        logger.info("📚 Learnings recorded")
    
    async def export_leads(
        self,
        leads: List[Dict[str, Any]],
        filename: str = "leads.json",
        format: str = "json"
    ) -> str:
        """
        Export leads to file.
        
        Args:
            leads: List of lead dictionaries
            filename: Output filename
            format: "json" or "csv"
        
        Returns:
            Path to output file
        """
        output_path = self.output_dir / filename
        
        if format == "json":
            with open(output_path, 'w') as f:
                json.dump({
                    "exported_at": datetime.now().isoformat(),
                    "total_leads": len(leads),
                    "pipeline_stats": self.stats.to_dict(),
                    "leads": leads
                }, f, indent=2)
        
        elif format == "csv":
            import csv
            
            if not leads:
                return str(output_path)
            
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=leads[0].keys())
                writer.writeheader()
                writer.writerows(leads)
        
        logger.info(f"✅ Exported {len(leads)} leads to {output_path}")
        return str(output_path)
    
    async def run_single_source(self, source_name: str) -> List[Dict[str, Any]]:
        """Quick run on a single source for testing"""
        return await self.run(source_names=[source_name], deep_crawl=False)
    
    async def close(self):
        """Clean up resources"""
        if self.scraping_engine:
            await self.scraping_engine.close()
        logger.info("🔒 Orchestrator closed")


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Smart Lead Hunter - Hotel Lead Discovery System"
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        help="Specific sources to scrape"
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=8,
        help="Minimum source priority (default: 8)"
    )
    parser.add_argument(
        "--output",
        default="./output",
        help="Output directory (default: ./output)"
    )
    parser.add_argument(
        "--no-deep",
        action="store_true",
        help="Disable deep crawling"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run quick test on 3 sources"
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    import os
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║   🏨 SMART LEAD HUNTER                                          ║
║   Intelligent Hotel Lead Discovery System                        ║
║                                                                  ║
║   Finding 4-star+ hotel openings across USA & Caribbean          ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
""")
    
    # Show AI status
    if gemini_api_key:
        print(f"🤖 AI: Gemini (PRIMARY) + Ollama (BACKUP)")
    else:
        print(f"🤖 AI: Ollama only (add GEMINI_API_KEY to .env for faster extraction)")
    print()
    
    orchestrator = LeadHunterOrchestrator(
        gemini_api_key=gemini_api_key,
        output_dir=args.output,
        use_ollama=True
    )
    
    try:
        await orchestrator.initialize()
        
        if args.test:
            # Quick test mode
            logger.info("🧪 Running in TEST MODE (3 sources, no deep crawl)")
            sources = list(orchestrator.scraping_engine._sources.keys())[:3]
            leads = await orchestrator.run(
                source_names=sources,
                deep_crawl=False
            )
        else:
            leads = await orchestrator.run(
                source_names=args.sources,
                priority_threshold=args.priority,
                deep_crawl=not args.no_deep
            )
        
        # Export results
        if leads:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            json_path = await orchestrator.export_leads(
                leads,
                f"leads_{timestamp}.json",
                format="json"
            )
            
            csv_path = await orchestrator.export_leads(
                leads,
                f"leads_{timestamp}.csv",
                format="csv"
            )
            
            print(f"\n📁 Files saved to {args.output}/")
            print(f"   • {json_path}")
            print(f"   • {csv_path}")
        
    finally:
        await orchestrator.close()


if __name__ == "__main__":
    asyncio.run(main())