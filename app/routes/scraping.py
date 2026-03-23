"""Scrape, Extract URL, and Discovery SSE streaming endpoints."""

import asyncio
import json
import logging
import os
import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from app.database import async_session
from app.models import Source
from app.services.utils import local_now
from app.config.intelligence_config import SKIP_URL_PATTERNS
from app.services.orchestrator import LeadHunterOrchestrator

from app.shared import (
    active_scrapes,
    scrape_cancellations,
    _scrape_lock,
    _pending_configs,
    _pending_extract_urls,
    _pending_discovery_configs,
    store_pending,
    pop_pending,
    cleanup_stale_scrapes,
    safe_error,
    require_ajax,
    checked_json,
    merged_lead_to_dict,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# Pre-initialized orchestrators keyed by scrape_id / extract_id.
# POST creates + inits them; GET pops and streams immediately.
# This avoids the ~1s init silence that killed SSE connections.
_pending_orchestrators: dict = {}  # id -> orchestrator


async def _init_orchestrator() -> LeadHunterOrchestrator:
    """Create and initialize an orchestrator instance."""
    orch = LeadHunterOrchestrator(
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        save_to_database=True,
    )
    await orch.initialize()
    return orch


@router.post("/api/dashboard/scrape", tags=["Dashboard"])
async def dashboard_trigger_scrape(request: Request, _csrf=Depends(require_ajax)):
    try:
        body = await checked_json(request)

        mode = body.get("mode", "full")
        source_ids = body.get("source_ids", [])

        scrape_id = str(uuid.uuid4())

        # Initialize orchestrator HERE in the POST (normal JSON endpoint).
        # The browser waits patiently for JSON — no SSE timeout issues.
        # Previously this ran inside the SSE GET handler, where ~1s of
        # silence killed the EventSource connection on Windows/uvicorn.
        try:
            orchestrator = await _init_orchestrator()
            _pending_orchestrators[scrape_id] = orchestrator
        except BaseException as e:
            logger.error(f"Dashboard: Pipeline init failed: {e}")
            return {
                "status": "error",
                "message": f"Pipeline init failed: {safe_error(e)}",
            }

        store_pending(
            _pending_configs,
            scrape_id,
            {
                "mode": mode,
                "source_ids": source_ids,
            },
        )

        logger.info(
            f"Dashboard: Scrape triggered (mode={mode}, sources={len(source_ids) if source_ids else 'all'})"
        )

        return {
            "status": "started",
            "message": f"Scrape job started ({mode} mode)",
            "scrape_id": scrape_id,
            "mode": mode,
            "source_count": len(source_ids) if source_ids else "all",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger scrape: {e}")
        return {
            "status": "error",
            "message": f"Failed to start scrape: {safe_error(e)}",
        }


# -----------------------------------------------------------------------------
# SSE Scrape Endpoint - Uses the REAL Orchestrator Pipeline
# -----------------------------------------------------------------------------
# This is the UNIFIED scrape path. Both the dashboard "Run Scrape" button
# and any future triggers use the same orchestrator that the CLI uses.
# No duplicate scraping/extraction/scoring/dedup logic.
# -----------------------------------------------------------------------------


@router.get("/api/dashboard/scrape/stream", tags=["Dashboard"])
async def scrape_with_progress(request: Request):
    """SSE endpoint for real-time scrape progress using the orchestrator pipeline"""

    # Get scrape config by ID from query param (Audit Fix #3 — race-safe)
    scrape_id = request.query_params.get("scrape_id", "")
    if not scrape_id or scrape_id not in _pending_configs:

        async def no_config():
            err = {
                "type": "error",
                "message": "No scrape config found. Please trigger scrape again.",
            }
            yield "data: " + json.dumps(err) + "\n\n"

        return StreamingResponse(no_config(), media_type="text/event-stream")

    scrape_config = pop_pending(_pending_configs, scrape_id, {})
    config_source_ids = scrape_config.get("source_ids", [])

    async with _scrape_lock:
        active_scrapes[scrape_id] = {"status": "starting", "_started": time.monotonic()}
    # Periodic cleanup of stale entries (M-05)
    await cleanup_stale_scrapes()

    # ── Retrieve pre-initialized orchestrator from POST handler ──
    orchestrator = _pending_orchestrators.pop(scrape_id, None)
    if not orchestrator:

        async def no_orch():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Pipeline not ready. Please trigger scrape again.'})}\n\n"

        return StreamingResponse(no_orch(), media_type="text/event-stream")

    scrapers = ["httpx"]
    if orchestrator.scraping_engine.playwright_scraper.available:
        scrapers.append("playwright")
    if (
        orchestrator.scraping_engine.crawl4ai_scraper
        and orchestrator.scraping_engine.crawl4ai_scraper.available
    ):
        scrapers.append("crawl4ai")
    scraper_list = ", ".join(scrapers)

    async def event_generator():
        try:
            # Send initial event with scrape ID
            yield f"data: {json.dumps({'type': 'started', 'scrape_id': scrape_id})}\n\n"
            yield f"data: {json.dumps({'type': 'info', 'message': 'Pipeline ready (scrapers: ' + scraper_list + '). Loading sources...'})}\n\n"

            # --- Get active sources from DB ---
            async with async_session() as session:
                result = await session.execute(
                    select(Source)
                    .where(Source.is_active.is_(True))
                    .order_by(Source.priority.desc())
                )
                sources = result.scalars().all()

            # Filter if specific sources requested
            if config_source_ids:
                sources = [s for s in sources if s.id in config_source_ids]

            total_sources = len(sources)
            # source_names = [s.name for s in sources]

            yield f"data: {json.dumps({'type': 'info', 'message': f'Found {total_sources} active sources to scrape'})}\n\n"

            start_time = local_now()

            # Load Source Intelligence for adaptive scraping
            from app.services.source_intelligence import SourceIntelligence

            source_intel_map = {}  # source_id -> SourceIntelligence

            # --- PHASE 1: SCRAPE all sources via the engine ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 1: Scraping sources...'})}\n\n"

            # Scrape one source at a time so we can send progress events
            all_pages = []
            sources_successful = 0

            for idx, source in enumerate(sources, 1):
                # Check for cancellation (async-safe)
                async with _scrape_lock:
                    cancelled = scrape_id in scrape_cancellations
                    if cancelled:
                        scrape_cancellations.discard(scrape_id)
                if cancelled:
                    yield f"data: {json.dumps({'type': 'cancelled', 'message': 'Scrape cancelled by user'})}\n\n"
                    break

                # Check if client disconnected (stop wasting resources)
                if await request.is_disconnected():
                    logger.info(
                        f"Client disconnected during scrape {scrape_id}, stopping pipeline"
                    )
                    break

                source_name = source.name

                # Load Source Intelligence (adaptive settings)
                scrape_settings = None
                try:
                    intel = SourceIntelligence(source)
                    source_intel_map[source.id] = intel
                    scrape_settings = intel.get_scrape_settings()
                    if scrape_settings.should_skip:
                        skip_msg = (
                            f"Skipping {source_name}: {scrape_settings.skip_reason}"
                        )
                        yield f"data: {json.dumps({'type': 'info', 'message': skip_msg})}\n\n"
                        continue
                except Exception as intel_err:
                    logger.warning(
                        f"Intelligence load failed for {source_name}: {intel_err}"
                    )

                # Check for gold URLs (fast scrape mode)
                gold_urls_dict = source.gold_urls or {}
                active_gold = [
                    url
                    for url, meta in gold_urls_dict.items()
                    if meta.get("miss_streak", 0) < 3
                ]

                # Decide: gold mode vs rediscovery
                use_gold = len(active_gold) > 0
                needs_rediscovery = False

                if use_gold and source.last_discovery_at:
                    discovery_interval = source.discovery_interval_days or 7
                    days_since_discovery = (
                        local_now() - source.last_discovery_at
                    ).total_seconds() / 86400
                    if days_since_discovery >= discovery_interval:
                        needs_rediscovery = True
                        use_gold = False  # Force deep crawl to find new gold
                elif use_gold and not source.last_discovery_at:
                    # Has gold URLs but never formally discovered — do a full crawl
                    needs_rediscovery = True
                    use_gold = False

                if needs_rediscovery:
                    mode_label = (
                        f"🔄 Rediscovery (overdue, {len(active_gold)} gold exist)"
                    )
                elif use_gold:
                    mode_label = f"⚡ GOLD ({len(active_gold)} URLs)"
                else:
                    mode_label = "🔍 First Discovery"
                yield f"data: {json.dumps({'type': 'source_start', 'source': source_name, 'current': idx, 'total': total_sources, 'mode': 'gold' if use_gold else 'discover'})}\n\n"
                yield f"data: {json.dumps({'type': 'info', 'message': f'{source_name}: {mode_label}'})}\n\n"

                try:
                    if use_gold:
                        # FAST MODE: Hit gold URLs + follow their links (depth 1)
                        scrape_results = {source_name: []}
                        visited = set()
                        for gold_url in active_gold:
                            try:
                                # 1. Fetch the listing/hub page (adaptive delay)
                                if (
                                    scrape_settings
                                    and scrape_settings.delay_seconds > 1.0
                                ):
                                    import asyncio as _aio

                                    await _aio.sleep(scrape_settings.delay_seconds)
                                await orchestrator.scraping_engine.rate_limiter.acquire(
                                    gold_url
                                )
                                # Disconnect check before scrape call (Audit Fix C-05)
                                if await request.is_disconnected():
                                    return

                                result = await orchestrator.scraping_engine.http_scraper.scrape(
                                    gold_url
                                )
                                # Record response to intelligence
                                if scrape_settings and source.id in source_intel_map:
                                    src_intel = source_intel_map[source.id]
                                    if result.status_code in (429, 403):
                                        src_intel.record_rate_limit(result.status_code)
                                        logger.warning(
                                            f"Rate limit {result.status_code} from {source_name}"
                                        )
                                    if result.crawl_time_ms:
                                        src_intel.record_url_result(
                                            url=gold_url,
                                            produced_lead=False,
                                            response_time_ms=result.crawl_time_ms,
                                        )

                                if result.success:
                                    scrape_results[source_name].append(result)
                                    visited.add(gold_url)

                                    # 2. Extract links and follow depth-1 (new articles)
                                    from bs4 import BeautifulSoup
                                    from urllib.parse import urljoin

                                    soup = BeautifulSoup(result.html or "", "lxml")
                                    links = set()
                                    # M-10: Filter out junk URLs before following
                                    _skip_patterns = SKIP_URL_PATTERNS
                                    from urllib.parse import urlparse

                                    gold_domain = urlparse(gold_url).netloc
                                    for a in soup.find_all("a", href=True):
                                        full_url = urljoin(gold_url, a["href"])
                                        if (
                                            full_url not in visited
                                            and urlparse(full_url).netloc == gold_domain
                                            and not any(
                                                skip in full_url.lower()
                                                for skip in _skip_patterns
                                            )
                                        ):
                                            # Intelligence junk filter
                                            import re as _re

                                            is_junk = False
                                            if (
                                                scrape_settings
                                                and scrape_settings.junk_patterns
                                            ):
                                                for jp in scrape_settings.junk_patterns:
                                                    try:
                                                        if _re.search(jp, full_url):
                                                            is_junk = True
                                                            break
                                                    except _re.error:
                                                        pass
                                            if not is_junk:
                                                links.add(full_url)

                                    # Fetch linked pages (capped by intelligence)
                                    max_follow = (
                                        scrape_settings.max_pages
                                        if scrape_settings
                                        else 15
                                    )
                                    for link_url in list(links)[:max_follow]:
                                        try:
                                            # Adaptive delay from intelligence
                                            if (
                                                scrape_settings
                                                and scrape_settings.delay_seconds > 1.0
                                            ):
                                                import asyncio as _aio

                                                await _aio.sleep(
                                                    scrape_settings.delay_seconds
                                                )
                                            await orchestrator.scraping_engine.rate_limiter.acquire(
                                                link_url
                                            )
                                            link_result = await orchestrator.scraping_engine.http_scraper.scrape(
                                                link_url
                                            )
                                            # Track rate limits on followed links
                                            if (
                                                link_result.status_code in (429, 403)
                                                and source.id in source_intel_map
                                            ):
                                                source_intel_map[
                                                    source.id
                                                ].record_rate_limit(
                                                    link_result.status_code
                                                )
                                                break  # Stop following links if rate limited

                                            if link_result.success:
                                                scrape_results[source_name].append(
                                                    link_result
                                                )
                                                visited.add(link_url)
                                        except Exception:
                                            pass
                            except Exception as e:
                                logger.warning(f"Gold URL failed {gold_url[:50]}: {e}")
                        logger.info(
                            f"⚡ Gold mode: {source_name} → {len(scrape_results[source_name])} pages from {len(active_gold)} gold URLs"
                        )
                    else:
                        # DISCOVERY MODE: Deep crawl to find new gold URLs
                        scrape_results = (
                            await orchestrator.scraping_engine.scrape_sources(
                                [source_name], deep=True, max_concurrent=3
                            )
                        )
                    source_pages = 0
                    # Log intelligence summary
                    if source.id in source_intel_map:
                        _si = source_intel_map[source.id]
                        _junk_count = len(_si.patterns.get("junk", []))
                        _gold_count = len(_si.patterns.get("gold", []))
                        logger.info(
                            f"Intelligence: {source_name} | "
                            f"score={_si.efficiency_score} | "
                            f"delay={scrape_settings.delay_seconds if scrape_settings else 1.0}s | "
                            f"{_gold_count} gold, {_junk_count} junk patterns"
                        )

                    for sname, results in scrape_results.items():
                        successful = [r for r in results if r.success]
                        source_pages += len(successful)
                        for r in successful:
                            all_pages.append(
                                {
                                    "source_name": sname,
                                    "url": r.url,
                                    "content": r.text or r.html or "",
                                }
                            )

                    if source_pages > 0:
                        sources_successful += 1
                        yield f"data: {json.dumps({'type': 'source_complete', 'source': source_name, 'current': idx, 'total': total_sources, 'pages': source_pages})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'url_error', 'url': source.base_url[:60], 'error': 'No content returned'})}\n\n"

                    # Update source last_scraped_at
                    async with async_session() as session:
                        source_obj = (
                            await session.execute(
                                select(Source).where(Source.id == source.id)
                            )
                        ).scalar_one_or_none()
                        if source_obj:
                            if source_pages > 0:
                                source_obj.record_success(
                                    0
                                )  # lead count updated after extraction
                            else:
                                source_obj.record_failure()
                            await session.commit()

                except Exception as e:
                    logger.error(f"Source {source_name} failed: {e}")
                    yield f"data: {json.dumps({'type': 'url_error', 'url': source.base_url[:60], 'error': safe_error(e)})}\n\n"

                # Rate limiting between sources
                await asyncio.sleep(1)

            yield f"data: {json.dumps({'type': 'info', 'message': f'Scraping complete: {len(all_pages)} pages from {sources_successful} sources'})}\n\n"

            if not all_pages:
                yield f"data: {json.dumps({'type': 'complete', 'stats': {'sources_scraped': 0, 'leads_found': 0, 'leads_saved': 0}, 'duration_seconds': 0})}\n\n"
                return

            # --- PHASE 2: EXTRACTION via intelligent pipeline ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 2: AI extraction (Gemini)...'})}\n\n"

            pages_for_pipeline = [
                {"url": p["url"], "content": p["content"], "source": p["source_name"]}
                for p in all_pages
            ]

            # Disconnect check before Gemini processing (Audit Fix C-05)
            if await request.is_disconnected():
                return

            pipeline_result = await orchestrator.pipeline.process_pages(
                pages_for_pipeline, source_name="Dashboard Scrape"
            )

            leads_extracted = pipeline_result.leads_extracted
            yield f"data: {json.dumps({'type': 'info', 'message': f'Extracted {leads_extracted} leads from {pipeline_result.pages_classified} pages'})}\n\n"

            # --- PHASE 3: DEDUPLICATION via smart deduplicator ---
            if orchestrator.deduplicator and pipeline_result.final_leads:
                yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 3: Deduplication...'})}\n\n"

                leads_for_dedup = [
                    lead.to_dict() for lead in pipeline_result.final_leads
                ]
                merged_leads = orchestrator.deduplicator.deduplicate(leads_for_dedup)
                dedup_stats = orchestrator.deduplicator.get_stats()

                dupes_found = dedup_stats.get("duplicates_found", 0)
                unique_count = len(merged_leads)
                yield f"data: {json.dumps({'type': 'info', 'message': f'Dedup: {dupes_found} duplicates merged, {unique_count} unique leads'})}\n\n"

                # Convert MergedLead objects to dicts for save_leads_to_database
                lead_dicts = [merged_lead_to_dict(ml) for ml in merged_leads]
            else:
                # No deduplicator or no leads
                lead_dicts = [
                    lead.to_dict() for lead in (pipeline_result.final_leads or [])
                ]

            # --- PHASE 4: SAVE TO DATABASE via orchestrator ---
            if lead_dicts:
                yield f"data: {json.dumps({'type': 'info', 'message': f'Phase 4: Saving {len(lead_dicts)} leads to database...'})}\n\n"

                db_result = await orchestrator.save_leads_to_database(lead_dicts)
                leads_saved = db_result["saved"]
                leads_dupes = db_result["duplicates"]

                if leads_saved > 0:
                    yield f"data: {json.dumps({'type': 'leads_found', 'url': 'pipeline', 'found': len(lead_dicts), 'saved': leads_saved, 'total_saved': leads_saved})}\n\n"

                yield f"data: {json.dumps({'type': 'info', 'message': f'Saved {leads_saved} new leads, {leads_dupes} already existed'})}\n\n"
            else:
                leads_saved = 0
                leads_dupes = 0

            # --- PHASE 5: GOLD URL TRACKING & SOURCE STATS ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Updating source intelligence...'})}\n\n"

            try:
                async with async_session() as stats_session:
                    source_id_map = {src.name: src.id for src in sources}

                    # Build map: source_id -> {url: lead_count}
                    url_lead_map = {}

                    if lead_dicts:
                        for lead in lead_dicts:
                            src_url = lead.get("source_url", "")
                            src_name = lead.get("source_name", "") or lead.get(
                                "source", ""
                            )

                            source_id = None
                            for sname, sid in source_id_map.items():
                                if (
                                    sname.lower() in (src_name or "").lower()
                                    or (src_name or "").lower() in sname.lower()
                                ):
                                    source_id = sid
                                    break

                            if source_id and src_url:
                                if source_id not in url_lead_map:
                                    url_lead_map[source_id] = {}
                                url_lead_map[source_id][src_url] = (
                                    url_lead_map[source_id].get(src_url, 0) + 1
                                )

                    # Update each source
                    for src in sources:
                        source_obj = (
                            await stats_session.execute(
                                select(Source).where(Source.id == src.id)
                            )
                        ).scalar_one_or_none()

                        if not source_obj:
                            continue

                        source_obj.total_scrapes = (source_obj.total_scrapes or 0) + 1
                        source_obj.last_scraped_at = local_now()

                        source_leads = (
                            sum(url_lead_map.get(src.id, {}).values())
                            if src.id in url_lead_map
                            else 0
                        )

                        if source_leads > 0:
                            source_obj.leads_found = (
                                source_obj.leads_found or 0
                            ) + source_leads
                            source_obj.last_success_at = local_now()
                            source_obj.consecutive_failures = 0
                            source_obj.health_status = "healthy"

                        scrapes = source_obj.total_scrapes or 1
                        old_avg = float(source_obj.avg_lead_yield or 0)
                        source_obj.avg_lead_yield = (
                            (old_avg * (scrapes - 1)) + source_leads
                        ) / scrapes

                        # Update gold URLs
                        gold = dict(source_obj.gold_urls or {})
                        now_str = local_now().isoformat()

                        if src.id in url_lead_map:
                            for url, count in url_lead_map[src.id].items():
                                # Only record as gold if 2+ leads from same page
                                # (listing/hub pages have multiple leads, individual articles don't)
                                if count < 2 and url not in gold:
                                    continue  # Skip individual article pages

                                if url in gold:
                                    gold[url]["leads_found"] = (
                                        gold[url].get("leads_found", 0) + count
                                    )
                                    gold[url]["last_hit"] = now_str
                                    gold[url]["miss_streak"] = 0
                                    gold[url]["total_checks"] = (
                                        gold[url].get("total_checks", 0) + 1
                                    )
                                else:
                                    gold[url] = {
                                        "leads_found": count,
                                        "last_hit": now_str,
                                        "first_found": now_str,
                                        "miss_streak": 0,
                                        "total_checks": 1,
                                    }

                        # Track misses on existing gold URLs
                        scraped_urls_for_source = [
                            p["url"]
                            for p in all_pages
                            if p.get("source_name") == src.name
                        ]
                        for url in scraped_urls_for_source:
                            if url in gold and (
                                src.id not in url_lead_map
                                or url not in url_lead_map.get(src.id, {})
                            ):
                                gold[url]["miss_streak"] = (
                                    gold[url].get("miss_streak", 0) + 1
                                )
                                gold[url]["total_checks"] = (
                                    gold[url].get("total_checks", 0) + 1
                                )
                                if gold[url]["miss_streak"] >= 3:
                                    logger.info(
                                        f"Gold URL demoted (3 misses): {url[:60]}"
                                    )

                        source_obj.gold_urls = gold
                        source_obj.last_discovery_at = local_now()

                        # --- SOURCE INTELLIGENCE: Record & Learn ---
                        if source_obj.id in source_intel_map:
                            try:
                                src_intel = source_intel_map[source_obj.id]
                                src_pages = [
                                    p
                                    for p in all_pages
                                    if p.get("source_name") == src.name
                                ]
                                for pg in src_pages:
                                    pg_url = pg.get("url", "")
                                    pg_leads = url_lead_map.get(src.id, {}).get(
                                        pg_url, 0
                                    )
                                    src_intel.record_url_result(
                                        url=pg_url,
                                        produced_lead=pg_leads > 0,
                                        lead_count=pg_leads,
                                    )
                                src_intel.record_scrape_run(
                                    pages_scraped=len(src_pages),
                                    leads_found=source_leads,
                                    leads_saved=source_leads,
                                    duration_seconds=0,
                                    mode="gold"
                                    if source_obj.gold_urls
                                    else "discovery",
                                )
                                src_intel.save()
                                source_obj.source_intelligence = dict(src_intel._data)
                                logger.info(
                                    f"Brain updated: {src.name} (score={src_intel.efficiency_score})"
                                )
                            except Exception as intel_err:
                                logger.warning(
                                    f"Intelligence record failed for {src.name}: {intel_err}"
                                )

                    await stats_session.commit()

                total_gold = sum(len(urls) for urls in url_lead_map.values())
                if total_gold > 0:
                    yield f"data: {json.dumps({'type': 'info', 'message': f'Recorded {total_gold} gold URLs across {len(url_lead_map)} sources'})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'info', 'message': 'Source stats updated (no new gold URLs this run)'})}\n\n"

            except Exception as gold_err:
                logger.error(f"Gold URL tracking error: {gold_err}")
                yield f"data: {json.dumps({'type': 'info', 'message': f'Warning: Source stats update failed: {str(gold_err)[:50]}'})}\n\n"

            # --- COMPLETE ---
            end_time = local_now()
            duration = (end_time - start_time).total_seconds()

            final_stats = {
                "sources_scraped": sources_successful,
                "urls_scraped": len(all_pages),
                "leads_found": leads_extracted,
                "leads_saved": leads_saved,
                "leads_skipped": leads_dupes,
                "errors": [],
            }

            yield f"data: {json.dumps({'type': 'complete', 'stats': final_stats, 'duration_seconds': duration})}\n\n"

        except BaseException as e:
            # BaseException catches asyncio.CancelledError (Python 3.9+)
            # which Playwright raises on Windows via subprocess_exec failure.
            logger.error(f"Scrape stream error: {e}")
            try:
                yield f"data: {json.dumps({'type': 'error', 'message': safe_error(e)})}\n\n"
            except BaseException:
                pass  # Client already disconnected
        finally:
            async with _scrape_lock:
                active_scrapes.pop(scrape_id, None)
                scrape_cancellations.discard(scrape_id)
            # Clean up orchestrator
            if orchestrator:
                try:
                    await orchestrator.close()
                except Exception:
                    pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# =============================================================================
# URL EXTRACT FEATURE
# =============================================================================


@router.post("/api/dashboard/extract-url", tags=["Dashboard"])
async def dashboard_extract_url(request: Request, _csrf=Depends(require_ajax)):
    """Accept a URL for direct lead extraction"""
    try:
        body = await checked_json(request)
        url = (body.get("url") or "").strip()

        if not url:
            return {"status": "error", "message": "No URL provided"}

        if not url.startswith("http"):
            url = "https://" + url

        extract_id = str(uuid.uuid4())

        # Initialize orchestrator in POST (same pattern as scrape)
        try:
            orchestrator = await _init_orchestrator()
            _pending_orchestrators[extract_id] = orchestrator
        except BaseException as e:
            logger.error(f"Dashboard: Extract URL init failed: {e}")
            return {
                "status": "error",
                "message": f"Pipeline init failed: {safe_error(e)}",
            }

        store_pending(_pending_extract_urls, extract_id, url)

        logger.info(f"Dashboard: URL extract triggered for {url}")

        return {
            "status": "started",
            "message": "Extracting leads from URL",
            "url": url,
            "extract_id": extract_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger URL extract: {e}")
        return {"status": "error", "message": f"Failed: {safe_error(e)}"}


# --- ENDPOINT 2: SSE stream for URL extraction progress ---


@router.get("/api/dashboard/extract-url/stream", tags=["Dashboard"])
async def extract_url_stream(request: Request):
    """SSE endpoint for real-time URL extraction progress"""

    extract_id = request.query_params.get("extract_id", "")
    target_url = (
        pop_pending(_pending_extract_urls, extract_id, "") if extract_id else ""
    )

    if not target_url:

        async def empty():
            yield f"data: {json.dumps({'type': 'error', 'message': 'No URL pending. Please click Extract again.'})}\n\n"

        return StreamingResponse(empty(), media_type="text/event-stream")

    # ── Retrieve pre-initialized orchestrator from POST handler ──
    orchestrator = _pending_orchestrators.pop(extract_id, None)
    if not orchestrator:

        async def no_orch():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Pipeline not ready. Please try again.'})}\n\n"

        return StreamingResponse(no_orch(), media_type="text/event-stream")

    async def event_generator():
        try:
            yield f"data: {json.dumps({'type': 'started', 'scrape_id': 'url-extract'})}\n\n"
            yield f"data: {json.dumps({'type': 'info', 'message': f'Target: {target_url}'})}\n\n"
            yield f"data: {json.dumps({'type': 'info', 'message': 'Pipeline ready.'})}\n\n"

            start_time = local_now()

            # --- PHASE 1: SCRAPE the URL ---
            yield f"data: {json.dumps({'type': 'source_start', 'source': 'URL Extract', 'current': 1, 'total': 1, 'mode': 'direct'})}\n\n"
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 1: Fetching page content...'})}\n\n"

            # Try scraping with the engine's HTTP scraper
            scrape_result = None
            page_content = ""

            try:
                scrape_result = await orchestrator.scraping_engine.http_scraper.scrape(
                    target_url
                )
                if scrape_result and scrape_result.success:
                    page_content = scrape_result.text or scrape_result.html or ""
                    # Safety: if we got raw HTML instead of clean text, strip it
                    if (
                        page_content.strip().startswith("<")
                        and len(page_content) > 30000
                    ):
                        from app.services.utils import clean_html_to_text

                        page_content = clean_html_to_text(page_content)
                    yield f"data: {json.dumps({'type': 'source_complete', 'source': 'URL Extract', 'current': 1, 'total': 1, 'pages': 1})}\n\n"
                    yield f"data: {json.dumps({'type': 'info', 'message': f'Page fetched: {len(page_content):,} chars'})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'info', 'message': 'HTTP scraper failed, trying fallback...'})}\n\n"
            except Exception as e:
                _err_msg = f"HTTP scraper error: {safe_error(e)}, trying fallback..."
                yield f"data: {json.dumps({'type': 'info', 'message': _err_msg})}\n\n"

            # Fallback: try with httpx directly
            if not page_content:
                try:
                    import httpx

                    async with httpx.AsyncClient(
                        timeout=30,
                        follow_redirects=True,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                        },
                    ) as client:
                        resp = await client.get(target_url)
                        if resp.status_code == 200:
                            # Strip HTML to clean text (same as scraping engine)

                            from app.services.utils import clean_html_to_text

                            page_content = clean_html_to_text(resp.text)
                            yield f"data: {json.dumps({'type': 'source_complete', 'source': 'URL Extract', 'current': 1, 'total': 1, 'pages': 1})}\n\n"
                            yield f"data: {json.dumps({'type': 'info', 'message': f'Page fetched (fallback): {len(page_content):,} chars'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'type': 'error', 'message': f'Failed to fetch URL: HTTP {resp.status_code}'})}\n\n"
                            return
                except Exception as e2:
                    _err = f"All fetch methods failed: {safe_error(e2)}"
                    yield f"data: {json.dumps({'type': 'error', 'message': _err})}\n\n"
                    return

            if not page_content:
                yield f"data: {json.dumps({'type': 'error', 'message': 'No content retrieved from URL'})}\n\n"
                return

            # Check if client disconnected
            if await request.is_disconnected():
                return

            # --- PHASE 2: AI EXTRACTION ---
            yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 2: AI extraction (Gemini)...'})}\n\n"

            # Extract domain for source name
            from urllib.parse import urlparse

            domain = urlparse(target_url).netloc.replace("www.", "")
            source_label = f"URL Extract ({domain})"

            pages_for_pipeline = [
                {
                    "url": target_url,
                    "content": page_content,
                    "source": source_label,
                }
            ]

            # Disconnect check before Gemini processing (Audit Fix C-05)
            if await request.is_disconnected():
                return

            pipeline_result = await orchestrator.pipeline.process_pages(
                pages_for_pipeline,
                source_name=source_label,
            )

            leads_extracted = pipeline_result.leads_extracted
            yield f"data: {json.dumps({'type': 'info', 'message': f'Extracted {leads_extracted} leads from page'})}\n\n"

            if leads_extracted == 0:
                yield f"data: {json.dumps({'type': 'info', 'message': 'No hotel leads found on this page. Try a different URL with hotel opening announcements.'})}\n\n"
                end_time = local_now()
                duration = (end_time - start_time).total_seconds()
                yield f"data: {json.dumps({'type': 'complete', 'stats': {'sources_scraped': 1, 'urls_scraped': 1, 'leads_found': 0, 'leads_saved': 0, 'leads_skipped': 0}, 'duration_seconds': duration})}\n\n"
                return

            # --- PHASE 3: DEDUPLICATION ---
            if orchestrator.deduplicator and pipeline_result.final_leads:
                yield f"data: {json.dumps({'type': 'info', 'message': 'Phase 3: Deduplication...'})}\n\n"

                leads_for_dedup = [
                    lead.to_dict() for lead in pipeline_result.final_leads
                ]
                merged_leads = orchestrator.deduplicator.deduplicate(leads_for_dedup)
                dedup_stats = orchestrator.deduplicator.get_stats()

                dupes_found = dedup_stats.get("duplicates_found", 0)
                unique_count = len(merged_leads)
                yield f"data: {json.dumps({'type': 'info', 'message': f'Dedup: {dupes_found} duplicates merged, {unique_count} unique leads'})}\n\n"

                # Convert to dicts
                lead_dicts = [
                    merged_lead_to_dict(
                        ml, fallback_url=target_url, fallback_source=source_label
                    )
                    for ml in merged_leads
                ]
            else:
                lead_dicts = [
                    lead.to_dict() for lead in (pipeline_result.final_leads or [])
                ]
                # Ensure source_url is set
                for d in lead_dicts:
                    if not d.get("source_url"):
                        d["source_url"] = target_url
                    if not d.get("source_name"):
                        d["source_name"] = source_label

            # --- PHASE 4: SAVE ---
            leads_saved = 0
            leads_dupes = 0
            if lead_dicts:
                yield f"data: {json.dumps({'type': 'info', 'message': f'Phase 4: Saving {len(lead_dicts)} leads to database...'})}\n\n"

                db_result = await orchestrator.save_leads_to_database(lead_dicts)
                leads_saved = db_result["saved"]
                leads_dupes = db_result["duplicates"]

                if leads_saved > 0:
                    yield f"data: {json.dumps({'type': 'leads_found', 'url': target_url, 'found': len(lead_dicts), 'saved': leads_saved, 'total_saved': leads_saved})}\n\n"

                yield f"data: {json.dumps({'type': 'info', 'message': f'Saved {leads_saved} new leads, {leads_dupes} already existed'})}\n\n"

            # --- COMPLETE ---
            end_time = local_now()
            duration = (end_time - start_time).total_seconds()

            final_stats = {
                "sources_scraped": 1,
                "urls_scraped": 1,
                "leads_found": leads_extracted,
                "leads_saved": leads_saved,
                "leads_skipped": leads_dupes,
            }

            yield f"data: {json.dumps({'type': 'complete', 'stats': final_stats, 'duration_seconds': duration})}\n\n"

        except BaseException as e:
            logger.error(f"URL extract stream error: {e}", exc_info=True)
            try:
                yield f"data: {json.dumps({'type': 'error', 'message': safe_error(e)})}\n\n"
            except BaseException:
                pass
        finally:
            if orchestrator:
                try:
                    await orchestrator.close()
                except Exception:
                    pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/api/dashboard/scrape/cancel/{scrape_id}", tags=["Dashboard"])
async def cancel_scrape(scrape_id: str, _csrf=Depends(require_ajax)):
    """Cancel an active scrape job"""
    async with _scrape_lock:
        if scrape_id in active_scrapes:
            scrape_cancellations.add(scrape_id)
            return {"status": "cancelling", "message": "Cancellation requested"}
    return {"status": "not_found", "message": "Scrape job not found"}


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Start Discovery (stores config, returns immediately)
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/api/dashboard/discovery/start", tags=["Dashboard"])
async def discovery_start(request: Request, _csrf=Depends(require_ajax)):
    """Trigger a web discovery run from the dashboard"""
    try:
        body = await checked_json(request)
        mode = body.get("mode", "full")
        extract_leads = body.get("extract_leads", True)
        dry_run = body.get("dry_run", False)

        discovery_id = str(uuid.uuid4())
        store_pending(
            _pending_discovery_configs,
            discovery_id,
            {
                "mode": mode,
                "extract_leads": extract_leads,
                "dry_run": dry_run,
            },
        )

        logger.info(
            f"Dashboard: Discovery triggered (mode={mode}, leads={extract_leads}, dry_run={dry_run})"
        )

        return {
            "status": "started",
            "message": f"Discovery started ({mode} mode)",
            "mode": mode,
            "discovery_id": discovery_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard: Failed to trigger discovery: {e}")
        return {"status": "error", "message": f"Failed: {safe_error(e)}"}


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Discovery SSE Stream (asyncio-based, matches v5 engine)
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/dashboard/discovery/stream", tags=["Dashboard"])
async def discovery_stream(request: Request):
    """SSE endpoint for real-time web discovery progress."""

    # Get config by discovery_id query param (Audit Fix #3 — race-safe)
    discovery_id = request.query_params.get("discovery_id", "")
    config = (
        pop_pending(_pending_discovery_configs, discovery_id, {})
        if discovery_id
        else {}
    )

    mode = config.get("mode", "full")
    extract_leads = config.get("extract_leads", True)
    dry_run = config.get("dry_run", False)

    async def event_generator():
        try:
            import sys
            import os

            sys.path.insert(0, os.getcwd())

            yield f"data: {json.dumps({'type': 'phase', 'message': '🌐 Initializing Web Discovery Engine v5...'})}\n\n"

            # v5 constructor: no use_ai param, uses IntelligentPipeline automatically
            max_queries = 5 if mode == "quick" else None
            start_time = local_now()

            progress_queue = asyncio.Queue()

            async def run_discovery():
                try:
                    from scripts.discover_sources import WebDiscoveryEngine

                    eng = WebDiscoveryEngine(
                        dry_run=dry_run,
                        min_quality=35,
                        sources_only=not extract_leads,
                    )
                    await eng.initialize()

                    # Audit Fix M-09: Use contextlib.redirect_stdout instead of
                    # monkey-patching builtins.print (global mutation).
                    import io
                    import contextlib

                    def _classify_msg_type(msg):
                        if any(
                            s in msg
                            for s in ["\u2705", "\u2728", "Found", "Added", "QUALIFIED"]
                        ):
                            return "success"
                        if any(s in msg for s in ["\u274c", "Error", "Failed"]):
                            return "error"
                        if any(
                            s in msg
                            for s in ["\u26a0\ufe0f", "Warning", "Skip", "\u26aa"]
                        ):
                            return "warning"
                        if any(
                            s in msg
                            for s in [
                                "\U0001f4e1",
                                "\U0001f50d",
                                "\U0001f9ea",
                                "\U0001f916",
                                "\U0001f4be",
                                "Phase",
                                "\u2550\u2550\u2550",
                            ]
                        ):
                            return "phase"
                        return "info"

                    class _ProgressWriter(io.TextIOBase):
                        """Captures print() output and routes to SSE queue."""

                        def write(self, text):
                            msg = text.strip()
                            if not msg:
                                return len(text)
                            progress_queue.put_nowait(
                                {
                                    "type": _classify_msg_type(msg),
                                    "message": msg,
                                }
                            )
                            try:
                                progress_queue.put_nowait(
                                    {
                                        "type": "stats",
                                        "queries": eng.stats.get("search_results", 0),
                                        "domains": (
                                            eng.stats.get("search_results", 0)
                                            - eng.stats.get("already_known", 0)
                                            - eng.stats.get("blacklisted", 0)
                                        ),
                                        "sources": len(eng.discovered),
                                        "leads": len(eng.extracted_leads),
                                    }
                                )
                            except Exception:
                                pass
                            return len(text)

                    try:
                        with contextlib.redirect_stdout(_ProgressWriter()):
                            await eng.run(max_queries=max_queries)
                    finally:
                        await eng.close()

                    # Final completion event with stats
                    elapsed = (local_now() - start_time).total_seconds()
                    progress_queue.put_nowait(
                        {
                            "type": "complete",
                            "message": f"✅ Discovery complete in {elapsed:.0f}s",
                            "stats": {
                                "queries": eng.stats.get("search_results", 0),
                                "domains": (
                                    eng.stats.get("search_results", 0)
                                    - eng.stats.get("already_known", 0)
                                    - eng.stats.get("blacklisted", 0)
                                ),
                                "sources": len(eng.discovered),
                                "leads": len(eng.extracted_leads),
                            },
                        }
                    )

                except Exception as e:
                    logger.error(f"Discovery error: {e}", exc_info=True)
                    progress_queue.put_nowait(
                        {
                            "type": "complete",
                            "message": f"❌ Discovery failed: {safe_error(e)}",
                            "stats": {},
                        }
                    )

            # Run on the MAIN event loop (no threading — avoids AsyncEngine conflicts)
            task = asyncio.create_task(run_discovery())

            # Stream progress messages to frontend
            while True:
                if await request.is_disconnected():
                    task.cancel()
                    break

                try:
                    msg = progress_queue.get_nowait()
                    yield f"data: {json.dumps(msg)}\n\n"

                    if msg.get("type") == "complete":
                        break
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.3)

        except BaseException as e:
            logger.error(f"Discovery stream error: {e}")
            try:
                yield f"data: {json.dumps({'type': 'complete', 'message': f'❌ Stream error: {safe_error(e)}', 'stats': {}})}\n\n"
            except BaseException:
                pass

    from starlette.responses import StreamingResponse

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Contact Enrichment (Enrich button on lead detail panel)
# ─────────────────────────────────────────────────────────────────────────────


# Prevent duplicate enrichment runs for the same lead (Audit Fix M-09)
_enrichment_locks: dict[int, asyncio.Lock] = {}
