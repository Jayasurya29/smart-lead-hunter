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
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


# Database
from app.database import async_session
from app.services.utils import local_now
from app.config.intelligence_config import SCORE_HOT_THRESHOLD, SCORE_MIN_ATTENTION

logger = logging.getLogger(__name__)

LEARNING_AVAILABLE = False

try:
    from app.services.intelligent_pipeline import IntelligentPipeline, PipelineConfig

    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False
    logger.warning("Intelligent pipeline not available")

try:
    from app.services.smart_deduplicator import SmartDeduplicator

    SMART_DEDUP_AVAILABLE = True
except ImportError:
    SMART_DEDUP_AVAILABLE = False


@dataclass
class PipelineStats:
    """Statistics from a pipeline run"""

    start_time: datetime = field(default_factory=lambda: local_now())
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
            "duration_seconds": (self.end_time - self.start_time).total_seconds()
            if self.end_time
            else None,
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
        except BaseException as e:
            # BaseException catches asyncio.CancelledError (Python 3.9+)
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
                self.learning_system = None
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
                source_names,
                deep=deep_crawl,
                max_concurrent=self.max_concurrent_scrapes,
            )
        else:
            scrape_results = await self.scraping_engine.scrape_all_sources(
                deep=deep_crawl,
                max_concurrent=self.max_concurrent_scrapes,
                priority_threshold=priority_threshold,
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
                all_pages.append(
                    {
                        "source_name": source_name,
                        "url": result.url,
                        "content": result.text or result.html or "",
                    }
                )

        logger.info(
            f"✅ Scraped {self.stats.pages_scraped} pages from {self.stats.sources_successful} sources"
        )

        # PHASE 2: EXTRACTION (using unified pipeline)
        logger.info("\n🧠 PHASE 2: INTELLIGENT EXTRACTION...")

        pages_for_pipeline = [
            {"url": p["url"], "content": p["content"], "source": p["source_name"]}
            for p in all_pages
        ]

        pipeline_result = await self.pipeline.process_pages(
            pages_for_pipeline,
            source_name=source_names[0]
            if source_names and len(source_names) == 1
            else "Multiple",
        )

        self.stats.pages_processed = pipeline_result.pages_classified
        self.stats.leads_extracted = pipeline_result.leads_extracted

        # PHASE 3: DEDUPLICATION
        logger.info("\n🔄 PHASE 3: DEDUPLICATION...")

        if self.deduplicator and SMART_DEDUP_AVAILABLE:
            leads_for_dedup = [lead.to_dict() for lead in pipeline_result.final_leads]
            merged_leads = self.deduplicator.deduplicate(leads_for_dedup)

            # M-05 FIX: Work with dicts directly instead of converting
            # MergedLead back to ExtractedLead. The old conversion lost fields
            # like priority, revenue_estimates, contact_relevance, and
            # estimated_value that exist on MergedLead but not ExtractedLead.
            unique_lead_dicts = []
            for lead in merged_leads:
                lead_dict = lead.to_dict() if hasattr(lead, "to_dict") else vars(lead)

                # Ensure source_url/source_name are set from merged lists
                if not lead_dict.get("source_url") and lead.source_urls:
                    lead_dict["source_url"] = lead.source_urls[0]
                if not lead_dict.get("source_name") and lead.source_names:
                    lead_dict["source_name"] = lead.source_names[0]

                # Add merge note to insights
                if lead.merged_from_count > 1:
                    merge_note = f"\n\n📎 Merged from {lead.merged_from_count} sources"
                    lead_dict["key_insights"] = (
                        lead_dict.get("key_insights") or ""
                    ) + merge_note

                unique_lead_dicts.append(lead_dict)

            dedup_stats = self.deduplicator.get_stats()
            logger.info(f"   ✅ {dedup_stats['duplicates_found']} duplicates merged")
            logger.info(f"   📊 {len(unique_lead_dicts)} unique leads")
        else:
            unique_lead_dicts = [lead.to_dict() for lead in pipeline_result.final_leads]

        self.stats.leads_after_dedup = len(unique_lead_dicts)

        # Count quality levels
        for ld in unique_lead_dicts:
            score = ld.get("qualification_score", 0)
            if score >= SCORE_HOT_THRESHOLD:
                self.stats.high_quality_leads += 1
            elif score >= SCORE_MIN_ATTENTION:
                self.stats.medium_quality_leads += 1
            else:
                self.stats.low_quality_leads += 1

            if ld.get("contact_email"):
                self.stats.leads_with_email += 1
            if ld.get("contact_phone"):
                self.stats.leads_with_phone += 1
            if ld.get("contact_name"):
                self.stats.leads_with_contact_name += 1

        self.stats.end_time = datetime.now()

        # Record learnings
        if LEARNING_AVAILABLE:
            self._record_learnings(scrape_results, unique_lead_dicts)

        # Sort by score (already dicts — no redundant conversion)
        lead_dicts = unique_lead_dicts
        lead_dicts.sort(key=lambda x: -x.get("qualification_score", 0))

        # PHASE 4: SAVE TO DATABASE
        if self.save_to_database:
            db_result = await self.save_leads_to_database(lead_dicts)
            self.stats.leads_saved = db_result["saved"]
            self.stats.leads_skipped_duplicates = db_result["duplicates"]

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        self._print_summary()

        return lead_dicts

    async def save_leads_to_database(self, leads: list) -> dict:
        """Save leads to database using shared lead factory."""
        logger.info(f"\n💾 SAVING {len(leads)} LEADS TO DATABASE...")

        from app.services.lead_factory import save_leads_batch

        async with async_session() as db:
            result = await save_leads_batch(leads, db)

        return result

    def _print_summary(self):
        """Print summary"""
        s = self.stats
        if s.end_time and s.start_time:
            # Ensure both are timezone-aware or both naive
            from datetime import timezone

            end = (
                s.end_time
                if s.end_time.tzinfo
                else s.end_time.replace(tzinfo=timezone.utc)
            )
            start = (
                s.start_time
                if s.start_time.tzinfo
                else s.start_time.replace(tzinfo=timezone.utc)
            )
            duration = (end - start).total_seconds()
        else:
            duration = 0

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
        # Learning is now handled by source_intelligence.py (DB-backed).
        # Called automatically by main.py Phase 5 and autonomous_tasks.py.
        return

    async def export_leads(
        self, leads: List[Dict], filename: str = "leads.json", format: str = "json"
    ) -> str:
        """Export leads to file"""
        output_path = self.output_dir / filename

        if format == "json":
            with open(output_path, "w") as f:
                json.dump(
                    {"exported_at": datetime.now().isoformat(), "leads": leads},
                    f,
                    indent=2,
                )
        elif format == "csv":
            import csv

            if leads:
                with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
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
                deep_crawl=not args.no_deep,
            )

        if leads:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            await orchestrator.export_leads(leads, f"leads_{timestamp}.json")
            await orchestrator.export_leads(
                leads, f"leads_{timestamp}.csv", format="csv"
            )

    finally:
        await orchestrator.close()


if __name__ == "__main__":
    asyncio.run(main())
