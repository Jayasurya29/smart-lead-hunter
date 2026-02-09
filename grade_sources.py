"""
Source Grader — Trial & Grade All Sources
==========================================

Runs each source through the pipeline once, grades them:

  S-TIER (Gold Mine)  → Consistently produces qualified leads
  A-TIER (Reliable)   → Produces leads, worth keeping on schedule  
  B-TIER (Occasional) → Sometimes has leads, keep on weekly schedule
  C-TIER (Marginal)   → Rarely produces, reduce to monthly checks
  F-TIER (Dead)       → Never produces anything useful → DEACTIVATE

Grading criteria:
1. Can we scrape it? (pages returned > 0)
2. Are there relevant articles? (classification hit rate)
3. Can we extract leads? (extraction success)
4. Are leads in our market? (US/Caribbean filter)

Usage:
    python grade_sources.py                    # Grade all sources
    python grade_sources.py --type chain_newsroom  # Grade one category
    python grade_sources.py --source "Hotel Dive"  # Grade one source
    python grade_sources.py --apply            # Apply grades (deactivate F-tier)
    python grade_sources.py --report           # Just show current grades
"""

import asyncio
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import select, update
from app.database import async_session
from app.models.source import Source


# ─── Grade thresholds ────────────────────────────────────────
GRADES = {
    "S": {"label": "⭐ Gold Mine",  "color": "🟡", "min_leads": 3, "min_relevant_pct": 40, "frequency": "daily"},
    "A": {"label": "✅ Reliable",   "color": "🟢", "min_leads": 1, "min_relevant_pct": 20, "frequency": "every_3_days"},
    "B": {"label": "🔵 Occasional", "color": "🔵", "min_leads": 0, "min_relevant_pct": 10, "frequency": "weekly"},
    "C": {"label": "⚪ Marginal",   "color": "⚪", "min_leads": 0, "min_relevant_pct": 0,  "frequency": "weekly"},
    "F": {"label": "❌ Dead",       "color": "🔴", "min_leads": 0, "min_relevant_pct": 0,  "frequency": "disabled"},
}


async def grade_source(source: Source, orchestrator) -> Dict:
    """
    Trial a single source through the full pipeline.
    Returns grading data.
    """
    result = {
        "source_id": source.id,
        "source_name": source.name,
        "source_type": source.source_type,
        "priority": source.priority,
        
        # Scraping
        "scrape_success": False,
        "pages_scraped": 0,
        "total_chars": 0,
        
        # Classification
        "pages_relevant": 0,
        "pages_irrelevant": 0,
        "relevance_pct": 0.0,
        
        # Extraction
        "leads_extracted": 0,
        "leads_us_caribbean": 0,
        "leads_international": 0,
        
        # Timing
        "scrape_time_s": 0,
        "extract_time_s": 0,
        "total_time_s": 0,
        
        # Grade
        "grade": "F",
        "grade_reason": "",
        "recommended_frequency": "disabled",
        "gold_urls": [],
    }
    
    total_start = time.time()
    
    # ── Phase 1: Scrape ──
    try:
        scrape_start = time.time()
        scrape_results = await orchestrator.scraping_engine.scrape_sources(
            [source.name], deep=True, max_concurrent=3
        )
        result["scrape_time_s"] = round(time.time() - scrape_start, 1)
        
        pages = []
        for sname, results_list in scrape_results.items():
            successful = [r for r in results_list if r.success]
            for r in successful:
                content = r.text or r.html or ""
                if len(content) > 200:  # Skip empty/tiny pages
                    pages.append({
                        "url": r.url,
                        "content": content,
                        "source": sname,
                    })
        
        result["pages_scraped"] = len(pages)
        result["total_chars"] = sum(len(p["content"]) for p in pages)
        result["scrape_success"] = len(pages) > 0
        
        if not pages:
            result["grade"] = "F"
            result["grade_reason"] = "No pages scraped (site may be down or blocking)"
            result["total_time_s"] = round(time.time() - total_start, 1)
            return result
            
    except Exception as e:
        result["grade"] = "F"
        result["grade_reason"] = f"Scrape failed: {str(e)[:100]}"
        result["total_time_s"] = round(time.time() - total_start, 1)
        return result
    
    # ── Phase 2: Classification + Extraction ──
    try:
        extract_start = time.time()
        pipeline_result = await orchestrator.pipeline.process_pages(pages)
        result["extract_time_s"] = round(time.time() - extract_start, 1)
        
        result["pages_relevant"] = pipeline_result.pages_relevant
        result["pages_irrelevant"] = pipeline_result.pages_classified - pipeline_result.pages_relevant
        
        if pipeline_result.pages_classified > 0:
            result["relevance_pct"] = round(
                (pipeline_result.pages_relevant / pipeline_result.pages_classified) * 100, 1
            )
        
        # Count leads
        leads = pipeline_result.final_leads or []
        result["leads_extracted"] = len(leads)
        
        # Categorize leads by market
        us_caribbean = []
        international = []
        
        for lead in leads:
            lead_dict = lead.to_dict() if hasattr(lead, 'to_dict') else lead
            country = (lead_dict.get("country") or "").lower()
            state = (lead_dict.get("state") or "").lower()
            
            is_us = country in ("us", "usa", "united states", "u.s.", "u.s.a.")
            is_caribbean = country in (
                "caribbean", "bahamas", "jamaica", "puerto rico", "usvi",
                "us virgin islands", "turks and caicos", "cayman islands",
                "barbados", "aruba", "bermuda", "antigua", "st. lucia",
                "dominican republic", "trinidad"
            ) or state in ("caribbean",)
            
            if is_us or is_caribbean:
                us_caribbean.append(lead_dict)
                # Track which URL this came from for gold URL detection
                url = lead_dict.get("source_url", "")
                if url:
                    result["gold_urls"].append(url)
            else:
                international.append(lead_dict)
        
        result["leads_us_caribbean"] = len(us_caribbean)
        result["leads_international"] = len(international)
        
        # Deduplicate gold URLs
        result["gold_urls"] = list(set(result["gold_urls"]))
        
    except Exception as e:
        result["grade"] = "C"
        result["grade_reason"] = f"Extraction failed: {str(e)[:100]}"
        result["total_time_s"] = round(time.time() - total_start, 1)
        return result
    
    # ── Grading Logic ──
    leads = result["leads_us_caribbean"]
    relevant_pct = result["relevance_pct"]
    pages = result["pages_scraped"]
    
    if leads >= 3 and relevant_pct >= 30:
        result["grade"] = "S"
        result["grade_reason"] = f"{leads} US/Caribbean leads, {relevant_pct}% relevant content"
        result["recommended_frequency"] = "daily"
    elif leads >= 1 and relevant_pct >= 15:
        result["grade"] = "A"
        result["grade_reason"] = f"{leads} leads found, good relevance ({relevant_pct}%)"
        result["recommended_frequency"] = "every_3_days"
    elif relevant_pct >= 10 or leads > 0:
        result["grade"] = "B"
        result["grade_reason"] = f"Some relevant content ({relevant_pct}%), {leads} leads"
        result["recommended_frequency"] = "weekly"
    elif pages > 0 and relevant_pct > 0:
        result["grade"] = "C"
        result["grade_reason"] = f"Low relevance ({relevant_pct}%), may improve over time"
        result["recommended_frequency"] = "weekly"
    else:
        result["grade"] = "F"
        result["grade_reason"] = f"No relevant content found in {pages} pages"
        result["recommended_frequency"] = "disabled"
    
    result["total_time_s"] = round(time.time() - total_start, 1)
    return result


async def run_grading(
    source_type: Optional[str] = None,
    source_name: Optional[str] = None,
    apply: bool = False,
    report_only: bool = False,
    limit: Optional[int] = None,
):
    """Run the grading process."""
    
    async with async_session() as session:
        # Build query
        query = select(Source).where(Source.is_active == True).order_by(Source.priority.desc())
        
        if source_type:
            query = query.where(Source.source_type == source_type)
        if source_name:
            query = query.where(Source.name.ilike(f"%{source_name}%"))
        
        result = await session.execute(query)
        sources = result.scalars().all()
        
        if limit:
            sources = sources[:limit]
        
        print(f"\n{'='*80}")
        print(f"  SOURCE GRADER — {'Report' if report_only else 'Trial'}")
        print(f"  {len(sources)} sources to {'review' if report_only else 'evaluate'}")
        print(f"{'='*80}\n")
        
        if report_only:
            # Just show existing grades based on historical data
            await show_report(sources)
            return
        
        # Initialize orchestrator for grading
        print("🔧 Initializing pipeline...")
        from app.services.orchestrator import LeadHunterOrchestrator
        orchestrator = LeadHunterOrchestrator(
            gemini_api_key=os.getenv("GEMINI_API_KEY"),
            save_to_database=False,  # Don't save during grading
        )
        await orchestrator.initialize()
        print("✅ Pipeline ready\n")
        
        # Grade each source
        all_grades = []
        grade_counts = {"S": 0, "A": 0, "B": 0, "C": 0, "F": 0}
        
        for idx, source in enumerate(sources, 1):
            print(f"[{idx}/{len(sources)}] Grading: {source.name}")
            print(f"         Type: {source.source_type} | Priority: {source.priority} | URL: {source.base_url[:60]}")
            
            grade_data = await grade_source(source, orchestrator)
            all_grades.append(grade_data)
            grade_counts[grade_data["grade"]] += 1
            
            # Print result
            grade_info = GRADES[grade_data["grade"]]
            print(f"         📊 Scraped: {grade_data['pages_scraped']} pages ({grade_data['total_chars']:,} chars) in {grade_data['scrape_time_s']}s")
            print(f"         🎯 Relevant: {grade_data['pages_relevant']}/{grade_data['pages_scraped']} ({grade_data['relevance_pct']}%)")
            print(f"         🏨 Leads: {grade_data['leads_us_caribbean']} US/Caribbean, {grade_data['leads_international']} international")
            print(f"         📝 Grade: {grade_info['color']} {grade_data['grade']}-TIER ({grade_info['label']})")
            print(f"         💡 {grade_data['grade_reason']}")
            
            if grade_data["gold_urls"]:
                print(f"         ⭐ Gold URLs: {len(grade_data['gold_urls'])}")
                for url in grade_data["gold_urls"][:3]:
                    print(f"            → {url[:70]}")
            
            print()
            
            # Small delay between sources to be nice to APIs
            if idx < len(sources):
                await asyncio.sleep(2)
        
        # ── Summary ──
        print(f"\n{'='*80}")
        print(f"  GRADING SUMMARY")
        print(f"{'='*80}")
        print(f"\n  Grade Distribution:")
        print(f"  {'⭐ S-Tier (Gold Mine):':<30} {grade_counts['S']:>3} sources")
        print(f"  {'✅ A-Tier (Reliable):':<30} {grade_counts['A']:>3} sources")
        print(f"  {'🔵 B-Tier (Occasional):':<30} {grade_counts['B']:>3} sources")
        print(f"  {'⚪ C-Tier (Marginal):':<30} {grade_counts['C']:>3} sources")
        print(f"  {'❌ F-Tier (Dead):':<30} {grade_counts['F']:>3} sources")
        
        total_leads = sum(g["leads_us_caribbean"] for g in all_grades)
        total_time = sum(g["total_time_s"] for g in all_grades)
        print(f"\n  Total leads found: {total_leads}")
        print(f"  Total time: {total_time:.0f}s ({total_time/60:.1f}min)")
        
        # Show F-tier sources to deactivate
        f_tier = [g for g in all_grades if g["grade"] == "F"]
        if f_tier:
            print(f"\n  🗑️  F-TIER (candidates for deactivation):")
            for g in f_tier:
                print(f"     • {g['source_name']:<40} — {g['grade_reason']}")
        
        # Show S-tier gold mines
        s_tier = [g for g in all_grades if g["grade"] == "S"]
        if s_tier:
            print(f"\n  🏆 S-TIER (gold mines to prioritize):")
            for g in s_tier:
                print(f"     • {g['source_name']:<40} — {g['leads_us_caribbean']} leads, {g['relevance_pct']}% relevant")
        
        # ── Apply grades ──
        if apply:
            print(f"\n{'='*80}")
            print(f"  APPLYING GRADES")
            print(f"{'='*80}")
            
            applied = 0
            deactivated = 0
            
            for grade_data in all_grades:
                source_obj = await session.execute(
                    select(Source).where(Source.id == grade_data["source_id"])
                )
                source_obj = source_obj.scalar_one_or_none()
                if not source_obj:
                    continue
                
                grade = grade_data["grade"]
                
                # Update frequency based on grade
                if grade == "F":
                    source_obj.is_active = False
                    source_obj.health_status = "dead"
                    source_obj.notes = (source_obj.notes or "") + f"\n[{datetime.now().strftime('%Y-%m-%d')}] Graded F-TIER: {grade_data['grade_reason']}. Deactivated."
                    deactivated += 1
                    print(f"  ❌ DEACTIVATED: {source_obj.name}")
                else:
                    new_freq = GRADES[grade]["frequency"]
                    old_freq = source_obj.scrape_frequency
                    source_obj.scrape_frequency = new_freq
                    source_obj.notes = (source_obj.notes or "") + f"\n[{datetime.now().strftime('%Y-%m-%d')}] Graded {grade}-TIER: {grade_data['grade_reason']}. Freq: {old_freq}→{new_freq}"
                    
                    # Update gold URLs if found
                    if grade_data["gold_urls"]:
                        gold = dict(source_obj.gold_urls or {})
                        now_str = datetime.now(timezone.utc).isoformat()
                        for url in grade_data["gold_urls"]:
                            if url not in gold:
                                gold[url] = {
                                    "leads_found": 1,
                                    "last_hit": now_str,
                                    "first_found": now_str,
                                    "miss_streak": 0,
                                    "total_checks": 1,
                                }
                            else:
                                gold[url]["leads_found"] += 1
                                gold[url]["last_hit"] = now_str
                        source_obj.gold_urls = gold
                    
                    if old_freq != new_freq:
                        print(f"  🔄 {source_obj.name}: {old_freq} → {new_freq} (Grade {grade})")
                    else:
                        print(f"  ✅ {source_obj.name}: kept {new_freq} (Grade {grade})")
                
                applied += 1
            
            await session.commit()
            print(f"\n  Applied: {applied} sources updated, {deactivated} deactivated")
        
        # Save full report
        report_path = f"grading_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, "w") as f:
            json.dump(all_grades, f, indent=2, default=str)
        print(f"\n  📄 Full report saved: {report_path}")
        
        # Cleanup
        if hasattr(orchestrator, 'cleanup'):
            await orchestrator.cleanup()


async def show_report(sources: List[Source]):
    """Show a report based on existing source data (no scraping)."""
    
    print(f"{'Source':<40} {'Type':<20} {'P':<3} {'Freq':<14} {'Leads':<7} {'Health':<10} {'Gold':<5}")
    print("─" * 110)
    
    for src in sources:
        gold_count = 0
        if hasattr(src, 'gold_urls') and src.gold_urls:
            gold_count = sum(1 for m in src.gold_urls.values() if m.get("miss_streak", 0) < 3)
        
        health_icon = {
            "healthy": "🟢",
            "degraded": "🟡", 
            "failing": "🔴",
            "dead": "💀",
            "new": "⚪",
        }.get(src.health_status, "❓")
        
        print(f"  {src.name[:38]:<38} {src.source_type:<20} {src.priority:<3} {src.scrape_frequency:<14} {src.leads_found or 0:<7} {health_icon} {src.health_status:<8} {gold_count}")
    
    # Stats
    total = len(sources)
    with_leads = sum(1 for s in sources if (s.leads_found or 0) > 0)
    healthy = sum(1 for s in sources if s.health_status == "healthy")
    
    print(f"\n  Total: {total} | With leads: {with_leads} | Healthy: {healthy}")
    print(f"  Sources that NEVER produced leads: {total - with_leads}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Grade source quality")
    parser.add_argument("--type", help="Filter by source type (e.g. chain_newsroom)")
    parser.add_argument("--source", help="Filter by source name (partial match)")
    parser.add_argument("--apply", action="store_true", help="Apply grades (update frequencies, deactivate F-tier)")
    parser.add_argument("--report", action="store_true", help="Show report without scraping")
    parser.add_argument("--limit", type=int, help="Limit number of sources to grade")
    
    args = parser.parse_args()
    
    asyncio.run(run_grading(
        source_type=args.type,
        source_name=args.source,
        apply=args.apply,
        report_only=args.report,
        limit=args.limit,
    ))