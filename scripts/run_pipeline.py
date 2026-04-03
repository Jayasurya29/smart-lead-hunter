#!/usr/bin/env python3
"""
SMART LEAD HUNTER — Quick-Start Runner
========================================
CLI tool for testing and running the extraction pipeline.

Usage:
    # Check system status (Redis, Postgres, Gemini, Ollama)
    python run_pipeline.py --status

    # Test on a single URL
    python run_pipeline.py --url "https://hoteldive.com/news/..."

    # Test mode (3 sources, no deep crawl)
    python run_pipeline.py --test

    # Full pipeline (all sources)
    python run_pipeline.py

    # Full pipeline, specific sources only
    python run_pipeline.py --sources "Hotel Dive,CoStar"

    # Show scoring for a hotel name
    python run_pipeline.py --score "Four Seasons Fort Lauderdale"
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent))

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ─────────────────────────────────────────────────────────
# STATUS CHECK
# ─────────────────────────────────────────────────────────

async def check_status():
    """Check all system dependencies."""
    import httpx

    print("=" * 60)
    print("🔍 SMART LEAD HUNTER — System Status")
    print("=" * 60)

    checks = {}

    # 1. Gemini API (Vertex AI)
    try:
        from app.services.ai_client import get_ai_url, get_ai_headers, is_vertex_ai
        if is_vertex_ai():
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    get_ai_url(),
                    headers=get_ai_headers(),
                    json={"contents": [{"role": "user", "parts": [{"text": "Say OK"}]}],
                          "generationConfig": {"maxOutputTokens": 5, "thinkingConfig": {"thinkingBudget": 0}}},
                )
                if resp.status_code == 200:
                    checks["Gemini API"] = ("✅", "Connected (Vertex AI)")
                else:
                    checks["Gemini API"] = ("❌", f"HTTP {resp.status_code}")
        else:
            checks["Gemini API"] = ("❌", "Vertex AI not configured")
    except Exception as e:
        checks["Gemini API"] = ("❌", str(e)[:60])

    # 2. Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        import redis
        r = redis.from_url(redis_url, socket_timeout=3)
        r.ping()
        checks["Redis"] = ("✅", redis_url.split("@")[-1] if "@" in redis_url else redis_url)
    except ImportError:
        checks["Redis"] = ("⚠️", "redis package not installed")
    except Exception as e:
        checks["Redis"] = ("❌", str(e)[:60])

    # 3. PostgreSQL
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        try:
            from sqlalchemy import create_engine, text
            sync_url = db_url.replace("+asyncpg", "").replace("postgresql+aiosqlite", "sqlite")
            engine = create_engine(sync_url.replace("asyncpg", "psycopg2") if "asyncpg" in sync_url else sync_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            checks["PostgreSQL"] = ("✅", db_url.split("@")[-1].split("?")[0] if "@" in db_url else "connected")
            engine.dispose()
        except ImportError:
            checks["PostgreSQL"] = ("⚠️", "sqlalchemy not installed")
        except Exception as e:
            checks["PostgreSQL"] = ("❌", str(e)[:60])
    else:
        checks["PostgreSQL"] = ("❌", "DATABASE_URL not set in .env")

    # 4. Ollama (backup LLM)
    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{ollama_url}/api/tags")
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                has_llama = any("llama" in m for m in models)
                status = f"{'llama3.2 ready' if has_llama else 'no llama model'} ({len(models)} models)"
                checks["Ollama"] = ("✅" if has_llama else "⚠️", status)
            else:
                checks["Ollama"] = ("❌", f"HTTP {resp.status_code}")
    except Exception:
        checks["Ollama"] = ("⚠️", "Not running (optional backup)")

    # 5. Insightly CRM
    crm_key = os.getenv("INSIGHTLY_API_KEY", "")
    if crm_key:
        try:
            import base64
            async with httpx.AsyncClient(timeout=10) as client:
                auth = base64.b64encode(f"{crm_key}:".encode()).decode()
                resp = await client.get(
                    "https://api.insightly.com/v3.1/Users/Me",
                    headers={"Authorization": f"Basic {auth}"},
                )
                if resp.status_code == 200:
                    checks["Insightly CRM"] = ("✅", "Connected")
                else:
                    checks["Insightly CRM"] = ("❌", f"HTTP {resp.status_code}")
        except Exception as e:
            checks["Insightly CRM"] = ("❌", str(e)[:60])
    else:
        checks["Insightly CRM"] = ("⚠️", "INSIGHTLY_API_KEY not set")

    # Print results
    print()
    for name, (icon, detail) in checks.items():
        print(f"  {icon} {name:20s} {detail}")

    # Summary
    ok_count = sum(1 for _, (icon, _) in checks.items() if icon == "✅")
    total = len(checks)
    print(f"\n  {ok_count}/{total} systems healthy")

    if ok_count < 2:
        print("\n  ⚠️  At minimum, Gemini API + Redis must be running.")
        print("     Check your .env file and Docker containers.")

    print("=" * 60)
    return ok_count >= 2


# ─────────────────────────────────────────────────────────
# SINGLE URL TEST
# ─────────────────────────────────────────────────────────

async def test_url(url: str):
    """Scrape and extract leads from a single URL."""
    import httpx
    from app.services.intelligent_pipeline import IntelligentPipeline, PipelineConfig

    print("=" * 60)
    print(f"🔗 Testing URL: {url[:80]}")
    print("=" * 60)

    # Step 1: Fetch the page
    print("\n📥 Fetching page...")
    start = time.time()
    try:
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "SmartLeadHunter/1.0"}
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                print(f"  ❌ HTTP {resp.status_code}")
                return
            content = resp.text
    except Exception as e:
        print(f"  ❌ Fetch failed: {e}")
        return

    fetch_time = time.time() - start
    print(f"  ✅ Fetched {len(content):,} chars in {fetch_time:.1f}s")

    # Step 2: Run pipeline
    print("\n🧠 Running extraction pipeline...")
    config = PipelineConfig()
    pipeline = IntelligentPipeline(config)

    pages = [{"url": url, "content": content, "source": "CLI Test"}]
    result = await pipeline.process_pages(pages, source_name="CLI Test")

    # Step 3: Show results
    print(f"\n{'─' * 60}")
    print(f"📊 RESULTS")
    print(f"{'─' * 60}")
    print(f"  Pages classified:  {result.pages_classified}")
    print(f"  Pages relevant:    {result.pages_relevant}")
    print(f"  Leads extracted:   {result.leads_extracted}")
    print(f"  Leads qualified:   {result.leads_qualified}")
    print(f"  Time:              {result.total_time_seconds:.1f}s")

    if result.final_leads:
        print(f"\n{'─' * 60}")
        for lead in sorted(result.final_leads, key=lambda l: l.qualification_score, reverse=True):
            icon = "🔥" if lead.qualification_score >= 70 else "🟡" if lead.qualification_score >= 50 else "⚪"
            print(f"  {icon} {lead.qualification_score:3d} pts │ {lead.hotel_name}")
            details = []
            if lead.brand:
                details.append(f"Brand: {lead.brand}")
            if lead.city:
                details.append(f"{lead.city}")
            if lead.state:
                details.append(f"{lead.state}")
            if lead.country and lead.country != "USA":
                details.append(f"{lead.country}")
            if lead.room_count:
                details.append(f"{lead.room_count} rooms")
            if lead.opening_date:
                details.append(f"Opening: {lead.opening_date}")
            if lead.lead_priority:
                details.append(f"Priority: {lead.lead_priority}")
            print(f"         │ {' · '.join(details)}")
            if lead.key_insights:
                print(f"         │ 💡 {lead.key_insights[:100]}")
    else:
        print("\n  No leads extracted from this URL.")
        print("  This could mean:")
        print("    - Content isn't about new hotel openings")
        print("    - Hotels are international (filtered by location scoring)")
        print("    - Content was too short or couldn't be parsed")

    print("=" * 60)


# ─────────────────────────────────────────────────────────
# SCORE CHECK
# ─────────────────────────────────────────────────────────

def check_score(hotel_name: str):
    """Show scoring breakdown for a hotel name."""
    from app.services.scorer import calculate_lead_score, format_score_breakdown

    print("=" * 60)
    print(f"📊 Score Check: {hotel_name}")
    print("=" * 60)

    # Try to infer basic details from the name
    result = calculate_lead_score(
        hotel_name=hotel_name,
        opening_date=str(datetime.now().year),
    )

    print(format_score_breakdown(result))
    print("=" * 60)


# ─────────────────────────────────────────────────────────
# TEST MODE
# ─────────────────────────────────────────────────────────

async def run_test():
    """Quick test: scrape 3 high-value sources, no deep crawl."""
    from app.services.intelligent_pipeline import IntelligentPipeline, PipelineConfig
    import httpx

    test_urls = [
        ("Hotel Dive", "https://www.hoteldive.com/"),
        ("Hospitality Net", "https://www.hospitalitynet.org/news/hotel-openings.html"),
        ("CoStar", "https://www.costar.com/article/latest/hospitality"),
    ]

    print("=" * 60)
    print("🧪 TEST MODE — 3 sources, surface scrape only")
    print("=" * 60)

    config = PipelineConfig()
    pipeline = IntelligentPipeline(config)

    all_leads = []

    for source_name, url in test_urls:
        print(f"\n📡 Scraping: {source_name} ({url})")
        try:
            async with httpx.AsyncClient(
                timeout=30.0,
                follow_redirects=True,
                headers={"User-Agent": "SmartLeadHunter/1.0"}
            ) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    print(f"  ❌ HTTP {resp.status_code}")
                    continue
                content = resp.text
                print(f"  ✅ {len(content):,} chars")
        except Exception as e:
            print(f"  ❌ {e}")
            continue

        pages = [{"url": url, "content": content, "source": source_name}]
        result = await pipeline.process_pages(pages, source_name=source_name)

        if result.final_leads:
            all_leads.extend(result.final_leads)
            for lead in result.final_leads:
                print(f"  📝 {lead.qualification_score}pts — {lead.hotel_name}")

    print(f"\n{'=' * 60}")
    print(f"🧪 TEST COMPLETE")
    print(f"  Sources scraped: {len(test_urls)}")
    print(f"  Total leads:     {len(all_leads)}")

    if all_leads:
        avg_score = sum(l.qualification_score for l in all_leads) / len(all_leads)
        print(f"  Avg score:       {avg_score:.0f}")
        hot = sum(1 for l in all_leads if l.qualification_score >= 70)
        print(f"  Hot leads (70+): {hot}")

    print("=" * 60)


# ─────────────────────────────────────────────────────────
# FULL PIPELINE
# ─────────────────────────────────────────────────────────

async def run_full(source_filter: str = None):
    """Run the full orchestrator pipeline."""
    from app.services.orchestrator import Orchestrator

    print("=" * 60)
    print("🚀 FULL PIPELINE RUN")
    if source_filter:
        print(f"   Filtered to: {source_filter}")
    print("=" * 60)

    try:
        orchestrator = Orchestrator()
        # The orchestrator handles everything: scrape → extract → score → dedup → save
        stats = await orchestrator.run(source_filter=source_filter)

        print(f"\n{'=' * 60}")
        print("✅ PIPELINE COMPLETE")
        print(f"{'=' * 60}")
        if isinstance(stats, dict):
            for key, value in stats.items():
                print(f"  {key}: {value}")
        else:
            print(f"  Result: {stats}")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

    print("=" * 60)


# ─────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Smart Lead Hunter — Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --status                          Check system health
  python run_pipeline.py --url "https://example.com/..."   Test single URL
  python run_pipeline.py --test                            Quick test (3 sources)
  python run_pipeline.py                                   Full pipeline
  python run_pipeline.py --sources "Hotel Dive,CoStar"     Specific sources
  python run_pipeline.py --score "Four Seasons Miami"      Check scoring
        """,
    )

    parser.add_argument("--status", action="store_true", help="Check system dependencies")
    parser.add_argument("--url", type=str, help="Test extraction on a single URL")
    parser.add_argument("--test", action="store_true", help="Quick test mode (3 sources)")
    parser.add_argument("--sources", type=str, help="Comma-separated source names for full run")
    parser.add_argument("--score", type=str, help="Show scoring breakdown for a hotel name")

    args = parser.parse_args()

    if args.status:
        asyncio.run(check_status())
    elif args.url:
        asyncio.run(test_url(args.url))
    elif args.score:
        check_score(args.score)
    elif args.test:
        asyncio.run(run_test())
    else:
        asyncio.run(run_full(source_filter=args.sources))


if __name__ == "__main__":
    main()
