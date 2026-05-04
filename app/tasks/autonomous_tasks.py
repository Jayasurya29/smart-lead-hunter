# -*- coding: utf-8 -*-
"""
SMART LEAD HUNTER â€” AUTONOMOUS TASKS v2
=========================================
Intelligence-driven Celery tasks using the unified smart_scraper service.
Same logic as dashboard â€” no duplication.
"""

import asyncio
import logging
import sys
from datetime import timedelta
from typing import Dict, Any

from sqlalchemy import select, func, and_, or_

from app.tasks.celery_app import celery_app, BaseTask
from app.database import async_session
from app.models.source import Source
from app.models.potential_lead import PotentialLead
from app.models.lead_contact import LeadContact
from app.services.utils import local_now
from app.config.intelligence_config import (
    MIN_RUNS_TO_GRADUATE,
    PRODUCER_YIELD_THRESHOLD,
    MODERATE_YIELD_THRESHOLD,
    PRODUCER_FREQ_MULTIPLIER,
    MODERATE_FREQ_MULTIPLIER,
    MIN_PRODUCER_INTERVAL_HOURS,
    MIN_MODERATE_INTERVAL_HOURS,
    LEARNING_INTERVAL_HOURS,
    LOW_YIELD_INTERVAL_HOURS,
    ZERO_YIELD_INTERVAL_HOURS,
    MAX_CONSECUTIVE_FAILURES,
    MIN_EFFICIENCY_SCORE,
    MIN_RUNS_FOR_DEACTIVATION,
    SCORE_MIN_ENRICH,
)


logger = logging.getLogger(__name__)

if sys.version_info >= (3, 11):
    import threading

    _runner_local = threading.local()

    def run_async(coro):
        if not hasattr(_runner_local, "runner"):
            _runner_local.runner = asyncio.Runner()
        return _runner_local.runner.run(coro)
else:

    def run_async(coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TASK 1: SMART SCRAPE
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


@celery_app.task(bind=True, base=BaseTask, name="smart_scrape")
def smart_scrape(self) -> Dict[str, Any]:
    """Brain picks which sources are due, scrapes with full intelligence."""
    logger.info("brain Smart Scrape: deciding which sources to scrape...")

    async def _decide_and_scrape():
        from app.services.source_intelligence import SourceIntelligence
        from app.services.orchestrator import LeadHunterOrchestrator
        from app.services.smart_scraper import scrape_source_smart
        from app.services.lead_factory import save_leads_batch

        results = {
            "sources_checked": 0,
            "sources_scraped": 0,
            "sources_skipped": 0,
            "leads_extracted": 0,
            "leads_saved": 0,
            "details": [],
        }

        now = local_now()

        async with async_session() as session:
            db_result = await session.execute(
                select(Source).where(Source.is_active.is_(True))
            )
            sources = db_result.scalars().all()
            results["sources_checked"] = len(sources)

            # Brain decides which sources are due
            sources_to_scrape = []
            for source in sources:
                intel = SourceIntelligence(source)
                score = intel.efficiency_score
                last_scraped = source.last_scraped_at

                # Interval based on LEARNED behavior, not just score
                yield_rate = intel.performance.get("lead_yield_rate", 0)
                runs = len(intel.history)
                pub_freq = intel.behavior.get("publish_frequency_days", 7)

                if (
                    runs >= MIN_RUNS_TO_GRADUATE
                    and yield_rate > PRODUCER_YIELD_THRESHOLD
                ):
                    interval_hours = max(
                        pub_freq * PRODUCER_FREQ_MULTIPLIER, MIN_PRODUCER_INTERVAL_HOURS
                    )
                elif runs >= 2 and yield_rate > MODERATE_YIELD_THRESHOLD:
                    interval_hours = max(
                        pub_freq * MODERATE_FREQ_MULTIPLIER, MIN_MODERATE_INTERVAL_HOURS
                    )
                elif runs < MIN_RUNS_TO_GRADUATE:
                    interval_hours = LEARNING_INTERVAL_HOURS
                elif runs >= MIN_RUNS_TO_GRADUATE and yield_rate > 0:
                    interval_hours = LOW_YIELD_INTERVAL_HOURS
                else:
                    interval_hours = ZERO_YIELD_INTERVAL_HOURS

                if last_scraped:
                    hours_since = (now - last_scraped).total_seconds() / 3600
                    if hours_since < interval_hours:
                        results["sources_skipped"] += 1
                        continue

                sources_to_scrape.append((source, score))

            if not sources_to_scrape:
                logger.info("brain No sources due. All caught up!")
                results["success"] = True
                return results

            # Sort by score (best first)
            sources_to_scrape.sort(key=lambda x: x[1], reverse=True)
            logger.info(f"brain {len(sources_to_scrape)} sources due for scraping")

        # Scrape each source using shared smart_scraper
        orchestrator = LeadHunterOrchestrator(save_to_database=True)
        await orchestrator.initialize()

        source_intel_map = {}

        for source, score in sources_to_scrape:
            try:
                scrape_result = await scrape_source_smart(
                    source=source,
                    orchestrator=orchestrator,
                    source_intel_map=source_intel_map,
                )

                if scrape_result.skipped:
                    results["sources_skipped"] += 1
                    continue

                if scrape_result.pages_scraped == 0:
                    continue

                results["sources_scraped"] += 1
                # Per-source counters (NOT cumulative)
                lead_dicts = []
                saved = 0
                # Run through extraction pipeline
                if scrape_result.all_pages_data:
                    pipeline_result = await orchestrator.pipeline.process_pages(
                        [
                            {
                                "url": p["url"],
                                "content": p["content"],
                                "source": p["source_name"],
                            }
                            for p in scrape_result.all_pages_data
                        ],
                        source_name=source.name,
                    )

                    # Get leads from pipeline
                    leads = []
                    if hasattr(pipeline_result, "final_leads"):
                        leads = pipeline_result.final_leads or []
                    elif hasattr(pipeline_result, "leads"):
                        leads = pipeline_result.leads or []

                    if leads:
                        # Dedup
                        if orchestrator.deduplicator:
                            leads = orchestrator.deduplicator.deduplicate(leads)

                        # Convert to dicts
                        lead_dicts = []
                        for lead in leads:
                            if hasattr(lead, "to_dict"):
                                lead_dicts.append(lead.to_dict())
                            elif isinstance(lead, dict):
                                lead_dicts.append(lead)

                        # Save
                        if lead_dicts:
                            async with async_session() as save_session:
                                db_result = await save_leads_batch(
                                    lead_dicts, save_session
                                )
                                saved = db_result.get("saved", 0)
                                # Track enriched separately. A source that
                                # consistently re-finds leads we already
                                # have IS productive — it's giving us fresh
                                # extractions on existing rows. Without
                                # counting these, the brain marks the
                                # source dead. Bug found 2026-05-04.
                                enriched = db_result.get("enriched", 0)
                                productive = saved + enriched
                                results["leads_saved"] += saved
                                results["leads_extracted"] += len(lead_dicts)

                        results["details"].append(
                            {
                                "source": source.name,
                                "score": score,
                                "mode": scrape_result.mode,
                                "pages": scrape_result.pages_scraped,
                                "leads": len(lead_dicts) if lead_dicts else 0,
                                "saved": saved if lead_dicts else 0,
                            }
                        )

                # Update source intelligence (per-source, NOT cumulative)
                if source.id in source_intel_map:
                    try:
                        src_intel = source_intel_map[source.id]
                        # Pull pipeline funnel metrics if available — distinguishes
                        # "source has no hotel content" from "extractor failed
                        # on hotel content". Defaults to 0 if pipeline didn't run.
                        _pages_classified = 0
                        _pages_relevant = 0
                        _leads_extracted = 0
                        try:
                            _pr = locals().get("pipeline_result")
                            if _pr is not None:
                                _pages_classified = (
                                    getattr(_pr, "pages_classified", 0) or 0
                                )
                                _pages_relevant = getattr(_pr, "pages_relevant", 0) or 0
                                _leads_extracted = (
                                    getattr(_pr, "leads_extracted", 0) or 0
                                )
                        except Exception:
                            pass
                        src_intel.record_scrape_run(
                            pages_scraped=scrape_result.pages_scraped,
                            leads_found=len(lead_dicts),
                            leads_saved=saved,
                            duration_seconds=0,
                            mode=scrape_result.mode,
                            pages_classified=_pages_classified,
                            pages_relevant=_pages_relevant,
                            leads_extracted=_leads_extracted,
                        )
                        src_intel.save()
                        async with async_session() as intel_session:
                            src_obj = (
                                await intel_session.execute(
                                    select(Source).where(Source.id == source.id)
                                )
                            ).scalar_one_or_none()
                            if src_obj:
                                src_obj.source_intelligence = dict(src_intel._data)
                                # Use 'productive' (saved + enriched), not
                                # raw lead_dicts count. A source extracting
                                # 6 leads where all 6 already exist is still
                                # productive — record_success keeps it from
                                # being demoted to LOW_YIELD by the brain.
                                # See save block above for full reasoning.
                                productive_leads = locals().get("productive", 0)
                                if productive_leads > 0:
                                    src_obj.record_success(productive_leads)
                                else:
                                    src_obj.total_scrapes = (
                                        src_obj.total_scrapes or 0
                                    ) + 1
                                    src_obj.last_scraped_at = local_now()
                                # Sync gold URL count from intelligence
                                intel_data = src_intel._data or {}
                                gold_count = len(intel_data.get("gold_patterns", []))
                                if gold_count > 0 and not src_obj.gold_urls:
                                    src_obj.gold_urls = {
                                        p: {"source": "intelligence"}
                                        for p in intel_data.get("gold_patterns", [])
                                    }
                                await intel_session.commit()
                    except Exception as ie:
                        logger.warning(f"Intel save failed: {ie}")

                logger.info(
                    f"  Done: {source.name} ({scrape_result.mode}) - "
                    f"{scrape_result.pages_scraped} pages"
                )

            except Exception as e:
                logger.error(f"  Failed: {source.name}: {e}")

        await orchestrator.close()
        results["success"] = True

        logger.info(
            f"brain Smart Scrape complete: "
            f"{results['sources_scraped']} scraped, "
            f"{results['leads_saved']} leads saved"
        )
        return results

    return run_async(_decide_and_scrape())


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TASK 2: AUTO-ENRICH
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


@celery_app.task(bind=True, base=BaseTask, name="auto_enrich")
def auto_enrich(self) -> Dict[str, Any]:
    """Automatically enrich the most promising unenriched leads."""
    logger.info("Auto-Enrich: Finding leads that need contacts...")

    async def _enrich():
        results = {
            "checked": 0,
            "enriched": 0,
            "skipped": 0,
            "errors": 0,
            "leads_enriched": [],
        }

        async with async_session() as session:
            leads_with_contacts = (
                select(LeadContact.lead_id)
                .group_by(LeadContact.lead_id)
                .having(func.count(LeadContact.id) > 0)
            )

            unenriched = await session.execute(
                select(PotentialLead)
                .where(
                    and_(
                        PotentialLead.status == "new",
                        PotentialLead.lead_score >= SCORE_MIN_ENRICH,
                        ~PotentialLead.id.in_(leads_with_contacts),
                    )
                )
                .order_by(PotentialLead.lead_score.desc())
                .limit(10)
            )
            candidates = unenriched.scalars().all()
            results["checked"] = len(candidates)

            if not candidates:
                logger.info("Auto-Enrich: No unenriched leads above threshold")
                results["success"] = True
                return results

            to_enrich = []
            for lead in candidates:
                from app.services.utils import months_to_opening

                months = months_to_opening(lead.opening_date or "")
                tier = lead.brand_tier or ""

                is_hot = months is not None and 6 <= months <= 12
                is_urgent = months is not None and 3 <= months <= 6
                is_warm_luxury = (
                    months is not None
                    and 12 <= months <= 18
                    and tier in ("tier1_ultra_luxury", "tier2_luxury")
                )

                if is_hot or is_urgent or is_warm_luxury:
                    to_enrich.append(lead)
                    if len(to_enrich) >= 5:
                        break

            logger.info(f"Auto-Enrich: {len(to_enrich)} leads selected")

            for lead in to_enrich:
                try:
                    from app.services.contact_enrichment import (
                        enrich_lead_contacts,
                        persist_enrichment_contacts,
                    )
                    from app.services.rescore import rescore_lead

                    logger.info(
                        f"  Enriching: {lead.hotel_name} (score={lead.lead_score})"
                    )

                    enrich_result = await enrich_lead_contacts(
                        lead_id=lead.id,
                        hotel_name=lead.hotel_name,
                        brand=lead.brand,
                        city=lead.city,
                        state=lead.state,
                        country=lead.country,
                        management_company=lead.management_company,
                        opening_date=lead.opening_date,
                        timeline_label=lead.timeline_label,
                        description=lead.description,
                        project_type_str=lead.hotel_type,
                    )

                    # Phase B: if researcher flagged as residences_only etc, reject the lead
                    if enrich_result and enrich_result.should_reject:
                        lead.status = "rejected"
                        lead.rejection_reason = (
                            enrich_result.rejection_reason or "auto_reject"
                        )[:100]  # VARCHAR(100) guard
                        results["rejected"] = results.get("rejected", 0) + 1
                        logger.info(
                            f"  {lead.hotel_name}: auto-rejected "
                            f"({enrich_result.rejection_reason})"
                        )
                    elif enrich_result and enrich_result.contacts:
                        # FIX: Persist to lead_contacts table + flat lead fields.
                        # Previously this task called enrich_lead_contacts() but
                        # never wrote contacts to the DB, so the dashboard's
                        # contacts panel stayed empty for auto-enriched leads.
                        persist_summary = await persist_enrichment_contacts(
                            lead.id, enrich_result, session
                        )
                        results["enriched"] += 1
                        results["leads_enriched"].append(
                            {
                                "name": lead.hotel_name,
                                "contacts": len(enrich_result.contacts),
                                "added": persist_summary["contacts_added"],
                                "updated": persist_summary["contacts_updated"],
                            }
                        )
                        await rescore_lead(lead.id, session)
                        logger.info(
                            f"  {lead.hotel_name}: "
                            f"{len(enrich_result.contacts)} contacts found "
                            f"({persist_summary['contacts_added']} new, "
                            f"{persist_summary['contacts_updated']} updated)"
                        )
                    else:
                        results["skipped"] += 1

                except Exception as e:
                    results["errors"] += 1
                    logger.error(f"  {lead.hotel_name}: {e}")

            await session.commit()

        results["success"] = True
        return results

    return run_async(_enrich())


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TASK 3: WEEKLY DISCOVERY
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


@celery_app.task(bind=True, base=BaseTask, name="weekly_discovery")
def weekly_discovery(self) -> Dict[str, Any]:
    """Run web discovery engine for new sources and leads."""
    logger.info("Weekly Discovery: Searching for new sources...")

    async def _discover():
        results = {"sources_found": 0, "leads_found": 0, "queries_run": 0}
        try:
            # Defensive sys.path injection — Celery worker's CWD isn't
            # always the project root, which made `from scripts.discover_sources`
            # fail with "No module named 'scripts'". Adding the project
            # root explicitly fixes this regardless of how the worker
            # was launched. (Bug observed 2026-05-04.)
            import sys as _sys
            from pathlib import Path as _Path

            _project_root = _Path(__file__).resolve().parent.parent.parent
            if str(_project_root) not in _sys.path:
                _sys.path.insert(0, str(_project_root))

            from scripts.discover_sources import WebDiscoveryEngine

            engine = WebDiscoveryEngine(
                dry_run=False, min_quality=35, sources_only=False
            )
            await engine.initialize()
            await engine.run(max_queries=None)

            results["sources_found"] = len(engine.discovered)
            results["leads_found"] = len(engine.extracted_leads)
            results["queries_run"] = engine.stats.get("search_results", 0)
            await engine.close()

            logger.info(
                f"Discovery: {results['sources_found']} sources, "
                f"{results['leads_found']} leads"
            )
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            results["error"] = str(e)

        results["success"] = True
        return results

    return run_async(_discover())


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TASK 4: DAILY HEALTH CHECK
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


@celery_app.task(bind=True, base=BaseTask, name="daily_health_check")
def daily_health_check(self) -> Dict[str, Any]:
    """Maintenance: cleanup, deactivate dead sources, rescore stale leads."""
    logger.info("Health Check: Running maintenance...")

    async def _health_check():
        from app.services.source_intelligence import SourceIntelligence

        results = {
            "sources_checked": 0,
            "sources_deactivated": 0,
            "gold_urls_cleaned": 0,
            "leads_rescored": 0,
        }

        async with async_session() as session:
            db_result = await session.execute(
                select(Source).where(Source.is_active.is_(True))
            )
            sources = db_result.scalars().all()
            results["sources_checked"] = len(sources)

            for source in sources:
                intel = SourceIntelligence(source)

                if (source.consecutive_failures or 0) >= MAX_CONSECUTIVE_FAILURES:
                    source.is_active = False
                    source.health_status = "dead"
                    results["sources_deactivated"] += 1
                    logger.info(f"  Deactivated: {source.name} (10+ failures)")
                    continue

                if (
                    intel.efficiency_score < MIN_EFFICIENCY_SCORE
                    and len(intel.history) >= MIN_RUNS_FOR_DEACTIVATION
                ):
                    source.is_active = False
                    source.health_status = "dead"
                    results["sources_deactivated"] += 1
                    logger.info(
                        f"  Deactivated: {source.name} (score={intel.efficiency_score})"
                    )
                    continue

                gold = dict(source.gold_urls or {})
                cleaned = {
                    url: meta
                    for url, meta in gold.items()
                    if meta.get("miss_streak", 0) < 3
                }
                removed = len(gold) - len(cleaned)
                if removed > 0:
                    source.gold_urls = cleaned
                    results["gold_urls_cleaned"] += removed

                intel.save()
                source.source_intelligence = source.source_intelligence

            # Recalculate timeline labels & auto-expire LATE leads
            from app.services.utils import get_timeline_label

            active_leads = await session.execute(
                select(PotentialLead).where(PotentialLead.status == "new")
            )
            timeline_updated = 0
            for lead in active_leads.scalars().all():
                new_label = get_timeline_label(lead.opening_date)
                if new_label != lead.timeline_label:
                    lead.timeline_label = new_label
                    timeline_updated += 1
            results["timeline_updated"] = timeline_updated
            # Note: status='expired' transitions + auto-transfer to
            # existing_hotels are handled by the dedicated
            # recompute_timeline_labels task. We ONLY refresh the label
            # here; the daily auto-transfer is its own task with its
            # own per-lead transaction handling.
            if timeline_updated:
                logger.info(f"  Timeline: {timeline_updated} label(s) updated")

            # Rescore stale leads
            week_ago = local_now() - timedelta(days=7)
            stale = await session.execute(
                select(PotentialLead.id)
                .where(
                    and_(
                        PotentialLead.status.notin_(["deleted", "expired", "rejected"]),
                        or_(
                            PotentialLead.updated_at < week_ago,
                            PotentialLead.updated_at.is_(None),
                        ),
                    )
                )
                .limit(50)
            )
            stale_ids = [row[0] for row in stale.all()]

            if stale_ids:
                from app.services.rescore import rescore_lead

                for lid in stale_ids:
                    try:
                        await rescore_lead(lid, session)
                        results["leads_rescored"] += 1
                    except Exception:
                        pass

            await session.commit()

        # FIX M-09: Cleanup expired pending registrations (24h+ old)
        try:
            from app.models.user import PendingRegistration
            from datetime import datetime, timezone

            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            async with async_session() as session:
                expired = await session.execute(
                    select(PendingRegistration).where(
                        PendingRegistration.otp_expires_at < cutoff
                    )
                )
                expired_rows = expired.scalars().all()
                cleaned_pending = len(expired_rows)
                for row in expired_rows:
                    await session.delete(row)
                await session.commit()
                if cleaned_pending:
                    logger.info(
                        f"  Cleaned {cleaned_pending} expired pending registrations"
                    )
                results["pending_registrations_cleaned"] = cleaned_pending
        except Exception as e:
            logger.warning(f"  Pending registration cleanup failed: {e}")
            results["pending_registrations_cleaned"] = 0

        logger.info(
            f"Health Check: {results['sources_checked']} checked, "
            f"{results['sources_deactivated']} deactivated, "
            f"{results['gold_urls_cleaned']} gold cleaned, "
            f"{results['leads_rescored']} rescored"
        )
        results["success"] = True
        return results

    return run_async(_health_check())


# =============================================================================
# TASK 4.5: AUTO SMART FILL  (added 2026-05-01)
# =============================================================================


@celery_app.task(bind=True, base=BaseTask, name="auto_smart_fill")
def auto_smart_fill(self) -> Dict[str, Any]:
    """Backfill empty metadata on top-scoring new leads.

    Targets `status='new'` leads missing any of:
      - opening_date     (required for HOT/URGENT bucketing)
      - brand_tier       (required for prioritization)
      - room_count       (required for revenue projection)
      - management_company (required for centralized procurement targeting)

    Picks top 10 by lead_score, runs Gemini-powered enrichment, persists
    the filled fields, rescores, and (if backfilled opening_date is now
    EXPIRED) auto-transfers to existing_hotels.

    Why this task exists
    --------------------
    Leads from smart_scrape arrive with whatever metadata the source page
    had - often empty. Without this backfill:
      - auto_enrich SKIPS them (filters by HOT/URGENT, which requires a
        valid opening_date) - so they never get contact research
      - revenue projections come back $0 (no room_count)
      - Sandals/Beaches leads do not get HPI procurement targeting
        (no management_company)
      - The leads sit at the bottom of the pipeline forever

    Scheduled at 10:30 AM Mon-Fri so it runs BEFORE the noon auto_enrich,
    giving the enrich task fresh, fully-filled leads to work on.

    Cost
    ----
    ~25 Gemini calls + ~40 Serper calls per run (10 leads x 2-3 Gemini,
    3-5 Serper each). Modest. Spaced 60 min before smart_scrape and
    150 min before auto_enrich so quotas stay healthy.
    """
    logger.info("Auto Smart Fill: backfilling empty lead metadata...")

    async def _smart_fill():
        from app.services.lead_data_enrichment import batch_smart_fill

        result = await batch_smart_fill(limit=10, mode="smart")
        logger.info(
            f"Auto Smart Fill: {result.get('checked', 0)} checked, "
            f"{result.get('enriched', 0)} enriched, "
            f"{result.get('transferred', 0)} graduated to existing_hotels"
        )
        result["success"] = True
        return result

    return run_async(_smart_fill())


# =============================================================================
# TASK 4.6: AUTO FULL REFRESH  (added 2026-05-04)
# =============================================================================


@celery_app.task(bind=True, base=BaseTask, name="auto_full_refresh")
def auto_full_refresh(self) -> Dict[str, Any]:
    """Refresh stale lead metadata using grounding to catch new info.

    Targets `status='new'` leads where `updated_at` is older than 14 days
    (or NULL). Picks the 5 staleest by lead_score, runs full grounded
    enrichment via Gemini googleSearch, persists improved values, and
    auto-transfers to existing_hotels if the refreshed opening_date
    lands in EXPIRED.

    Why this task exists
    --------------------
    Hotel info changes constantly: opening dates slip, brands rebrand,
    room counts adjust, mgmt companies change hands. A lead with all
    fields filled 6 weeks ago is probably stale. Without periodic
    refresh, leads degrade silently and Atlas-style cases happen
    (DB says "Fall 2026", reality is "spring 2026 - already opened").

    Self-throttling: refreshed leads bump their `updated_at`, so they
    sink to the bottom of the stale queue. The next day's run picks up
    different leads. Over ~30-60 days the entire pipeline cycles
    through. Pre-opening leads (which change most) bubble back up first
    because they're more likely to have actual updates.

    Cost
    ----
    ~5 grounded calls + ~25 Serper calls per run = ~$0.20/day.
    With grounding via us-central1 producing real source citations,
    this is the cheapest reliable way to keep the pipeline current.

    Scheduled at 11:00 AM Mon-Fri - between auto_smart_fill (10:30 AM)
    and the first auto_enrich (1:00 PM). Light load, doesn't compete
    for Gemini quota with the heavy enrichment tasks.
    """
    logger.info("Auto Full Refresh: re-checking stale lead metadata...")

    async def _full_refresh():
        from app.services.lead_data_enrichment import batch_full_refresh

        result = await batch_full_refresh(limit=5, stale_days=14)
        logger.info(
            f"Auto Full Refresh: {result.get('checked', 0)} checked, "
            f"{result.get('enriched', 0)} updated, "
            f"{result.get('skipped_no_changes', 0)} no-changes, "
            f"{result.get('transferred', 0)} graduated to existing_hotels"
        )
        result["success"] = True
        return result

    return run_async(_full_refresh())


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TASK 5: RESCORE ALL
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


@celery_app.task(bind=True, base=BaseTask, name="rescore_all_leads")
def rescore_all_leads_task(self) -> Dict[str, Any]:
    """On-demand: rescore all active leads."""
    logger.info("Rescoring all leads...")

    async def _rescore():
        from app.services.rescore import rescore_all_leads

        async with async_session() as session:
            return await rescore_all_leads(session)

    return run_async(_rescore())


@celery_app.task(bind=True, base=BaseTask, name="recompute_timeline_labels")
def recompute_timeline_labels(self) -> Dict[str, Any]:
    """Recompute timeline_label on every active lead based on today's date.

    Runs daily at 9:30 AM (see beat_schedule in celery_app.py).

    Behavior (updated 2026-04-29):
      - For leads still active (URGENT/HOT/WARM/COOL): just update
        timeline_label.
      - For leads that crossed into EXPIRED today: AUTO-TRANSFER them to
        existing_hotels via lead_transfer.transfer_lead(). The lead is
        hard-deleted, contacts re-parent, and a new (or merged) row
        appears on the Existing Hotels Pipeline tab. No operator review
        needed — the lead is now an operating hotel and lives in the
        right table.
      - RESURRECTION case (opening_date pushed back so label is no
        longer EXPIRED): only matters for leads still in
        potential_leads. Leads already auto-transferred won't come back.
    """
    from app.services.utils import get_timeline_label
    from app.services.lead_transfer import transfer_lead

    logger.info("Recompute Timeline Labels: starting...")

    async def _recompute():
        results = {
            "leads_checked": 0,
            "labels_updated": 0,
            "auto_transferred": 0,
            "auto_merged": 0,
            "transfer_errors": 0,
            "resurrected": 0,
            "by_label": {},
        }

        # Pass 1: walk all leads, decide which need label-only update vs
        # full auto-transfer. We collect the transfer-bound IDs so we
        # can run transfer_lead() in its own transaction (it commits
        # internally and we don't want partial writes if one fails).
        ids_to_transfer: list[int] = []

        async with async_session() as session:
            leads_q = await session.execute(
                select(PotentialLead).where(
                    PotentialLead.status.notin_(["deleted", "rejected"])
                )
            )
            for lead in leads_q.scalars().all():
                results["leads_checked"] += 1
                new_label = get_timeline_label(lead.opening_date)
                old_label = lead.timeline_label

                if new_label != old_label:
                    lead.timeline_label = new_label
                    results["labels_updated"] += 1
                    key = f"{old_label or 'NONE'}->{new_label}"
                    results["by_label"][key] = results["by_label"].get(key, 0) + 1

                # Queue for auto-transfer if EXPIRED
                if new_label == "EXPIRED":
                    ids_to_transfer.append(lead.id)
                    continue  # don't set status='expired' — it's about to be deleted

                # RESURRECTION: opening_date moved forward, no longer expired.
                # Reset status if it was stuck at 'expired' from a previous run.
                if (
                    new_label in ("URGENT", "HOT", "WARM", "COOL")
                    and lead.status == "expired"
                ):
                    lead.status = "new"
                    results["resurrected"] += 1
                    logger.info(
                        f"[RESURRECTION] Lead {lead.id} "
                        f"{lead.hotel_name!r}: "
                        f"timeline {old_label or 'NONE'} -> {new_label}, "
                        f"status 'expired' -> 'new' "
                        f"(opening={lead.opening_date})"
                    )

            await session.commit()

        # Pass 2: auto-transfer expired leads. transfer_lead handles
        # cross-table dedup against existing_hotels (merge if match,
        # create if not), contact re-parenting, scoring under Option B,
        # revenue calculation, and hard-deletes the source lead.
        if ids_to_transfer:
            logger.info(
                f"Recompute Timeline Labels: auto-transferring "
                f"{len(ids_to_transfer)} expired lead(s) to existing_hotels..."
            )
            for lid in ids_to_transfer:
                try:
                    async with async_session() as transfer_session:
                        r = await transfer_lead(lid, transfer_session, commit=True)
                    status = r.get("status")
                    eh_id = r.get("existing_hotel_id")
                    if status == "transferred":
                        results["auto_transferred"] += 1
                        logger.info(
                            f"   ✓ Auto-transferred lead #{lid} → "
                            f"existing_hotel #{eh_id}"
                        )
                    elif status == "merged":
                        results["auto_merged"] += 1
                        logger.info(
                            f"   ⇄ Auto-merged lead #{lid} → "
                            f"existing_hotel #{eh_id}"
                        )
                    elif status == "not_found":
                        # Lead disappeared between pass 1 and pass 2 —
                        # could be concurrent edit. Not an error.
                        pass
                    else:
                        results["transfer_errors"] += 1
                        logger.warning(
                            f"   ✗ Auto-transfer #{lid}: unexpected status {status!r}"
                        )
                except Exception as e:
                    results["transfer_errors"] += 1
                    logger.exception(f"   ✗ Auto-transfer failed for lead #{lid}: {e}")

        logger.info(
            f"Recompute Timeline Labels: checked={results['leads_checked']}, "
            f"labels_updated={results['labels_updated']}, "
            f"auto_transferred={results['auto_transferred']}, "
            f"auto_merged={results['auto_merged']}, "
            f"transfer_errors={results['transfer_errors']}, "
            f"resurrected={results['resurrected']}"
        )
        if results["by_label"]:
            for transition, count in sorted(
                results["by_label"].items(), key=lambda x: -x[1]
            ):
                logger.info(f"    {transition}: {count}")
        return results

    return run_async(_recompute())
