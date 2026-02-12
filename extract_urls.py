#!/usr/bin/env python3
"""
One-Time URL Extractor — Smart Lead Hunter
============================================
Scrapes given URLs once, extracts hotel leads, deduplicates against DB,
and saves new leads to the dashboard.

Usage:
    # Extract and save to dashboard
    python extract_urls.py "https://example.com/2026-hotel-openings"

    # Multiple URLs
    python extract_urls.py "https://url1.com/article" "https://url2.com/article"

    # From a file (one URL per line)
    python extract_urls.py --file urls.txt

    # Dry run (extract only, don't save to DB)
    python extract_urls.py --dry-run "https://example.com/article"

    # Force Playwright for JS-heavy sites
    python extract_urls.py --playwright "https://js-heavy-site.com/page"

    # Show international leads too
    python extract_urls.py --raw "https://example.com/article"
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Suppress noisy loggers
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)


async def scrape_url(url: str, use_playwright: bool = False) -> dict:
    """Scrape a URL and return content."""
    import httpx

    if not use_playwright:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
                r = await client.get(url, headers=headers)
                if r.status_code == 200:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(r.text, 'html.parser')
                    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                        tag.decompose()
                    text = soup.get_text(separator='\n', strip=True)
                    if len(text) > 500:
                        return {'url': url, 'content': text, 'chars': len(text), 'method': 'httpx'}
                    else:
                        logger.info(f"   ⚠️ httpx got too little content ({len(text)} chars), trying Playwright...")
                        use_playwright = True
                elif r.status_code in (403, 406, 429):
                    logger.info(f"   ⚠️ httpx blocked ({r.status_code}), trying Playwright...")
                    use_playwright = True
                else:
                    return {'url': url, 'content': '', 'chars': 0, 'method': 'httpx', 'error': f'HTTP {r.status_code}'}
        except Exception as e:
            logger.info(f"   ⚠️ httpx failed ({e}), trying Playwright...")
            use_playwright = True

    if use_playwright:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                try:
                    await page.goto(url, wait_until='domcontentloaded', timeout=15000)
                    await page.wait_for_timeout(5000)
                    text = await page.inner_text('body')
                    return {'url': url, 'content': text, 'chars': len(text), 'method': 'playwright'}
                except Exception as e:
                    return {'url': url, 'content': '', 'chars': 0, 'method': 'playwright', 'error': str(e)}
                finally:
                    await page.close()
                    await browser.close()
        except ImportError:
            return {'url': url, 'content': '', 'chars': 0, 'method': 'none', 'error': 'Playwright not installed'}

    return {'url': url, 'content': '', 'chars': 0, 'method': 'none', 'error': 'Unknown error'}


async def extract_leads(url: str, content: str, pipeline) -> list:
    """Run content through the AI extractor and qualifier."""
    leads = await pipeline.extractor.extract(url, content, source_name="one-time-extract")
    if not leads:
        return []
    qualified = pipeline.qualifier.qualify_batch(leads)
    return qualified


async def save_leads_to_db(lead_dicts: list, dry_run: bool = False) -> dict:
    """
    Save leads to the database using the same logic as the orchestrator.
    Deduplicates against existing leads by normalized hotel name.
    """
    if dry_run:
        return {'saved': 0, 'enriched': 0, 'skipped': 0, 'errors': 0, 'dry_run': True}

    from app.database import async_session
    from app.models import PotentialLead
    from app.services.scorer import calculate_lead_score
    from app.services.utils import normalize_hotel_name
    from sqlalchemy import select

    saved = 0
    enriched = 0
    skipped = 0
    errors = 0

    def extract_year(opening_date: str) -> int:
        if not opening_date:
            return None
        match = re.search(r'20\d{2}', str(opening_date))
        return int(match.group()) if match else None

    async with async_session() as db:
        for lead_dict in lead_dicts:
            async with db.begin_nested():
                try:
                    hotel_name = (lead_dict.get('hotel_name') or '').strip()
                    if not hotel_name:
                        errors += 1
                        continue

                    normalized = normalize_hotel_name(hotel_name)

                    # Check for existing lead
                    result = await db.execute(
                        select(PotentialLead).where(
                            PotentialLead.hotel_name_normalized == normalized
                        )
                    )
                    existing = result.scalars().first()

                    if existing:
                        # ENRICHMENT: Update existing lead with new info
                        enrichment_happened = False
                        enrichment_fields = {
                            'brand': lead_dict.get('brand'),
                            'city': lead_dict.get('city'),
                            'state': lead_dict.get('state'),
                            'country': lead_dict.get('country'),
                            'opening_date': lead_dict.get('opening_date'),
                            'room_count': lead_dict.get('room_count'),
                            'contact_name': lead_dict.get('contact_name'),
                            'contact_title': lead_dict.get('contact_title'),
                            'contact_email': lead_dict.get('contact_email'),
                            'contact_phone': lead_dict.get('contact_phone'),
                            'description': lead_dict.get('key_insights'),
                            'hotel_type': lead_dict.get('property_type') or lead_dict.get('hotel_type'),
                        }

                        for field, new_val in enrichment_fields.items():
                            if not new_val:
                                continue
                            old_val = getattr(existing, field, None)
                            if not old_val and new_val:
                                setattr(existing, field, new_val)
                                enrichment_happened = True
                            elif field == 'description' and old_val and new_val and len(str(new_val)) > len(str(old_val)):
                                setattr(existing, field, new_val)
                                enrichment_happened = True

                        if enrichment_happened:
                            existing.updated_at = datetime.now(timezone.utc)
                            enriched += 1
                            print(f"      🔄 Enriched existing: {hotel_name}")
                        else:
                            skipped += 1
                            print(f"      ⏭️ Already exists: {hotel_name}")
                        continue

                    # Score the lead
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
                        print(f"      ⚠️ Skipped: {hotel_name} — {score_result['skip_reason']}")
                        skipped += 1
                        continue

                    pipeline_score = lead_dict.get('qualification_score')
                    final_score = pipeline_score if pipeline_score else score_result['total_score']

                    room_count = None
                    try:
                        room_count = int(float(lead_dict.get('room_count', 0) or 0))
                        if room_count == 0:
                            room_count = None
                    except (ValueError, TypeError):
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
                        source_site='one-time-extract',
                        lead_score=final_score,
                        score_breakdown=score_result['breakdown'],
                        status='new',
                        scraped_at=datetime.now(timezone.utc),
                        created_at=datetime.now(timezone.utc),
                    )
                    db.add(lead)
                    saved += 1

                    quality = "🔴 HOT" if final_score >= 70 else "🟠 WARM" if final_score >= 50 else "🔵 COOL"
                    print(f"      ✅ {quality} [{final_score}] {hotel_name}")

                except Exception as e:
                    print(f"      ❌ Error saving {lead_dict.get('hotel_name', 'unknown')}: {e}")
                    errors += 1

        await db.commit()

    return {'saved': saved, 'enriched': enriched, 'skipped': skipped, 'errors': errors}


async def main():
    parser = argparse.ArgumentParser(
        description='One-Time URL Extractor — Extract hotel leads and save to dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_urls.py "https://example.com/2026-hotels"
  python extract_urls.py --file urls.txt
  python extract_urls.py --dry-run "https://example.com/article"
  python extract_urls.py --playwright "https://js-heavy-site.com"
        """
    )
    parser.add_argument('urls', nargs='*', help='URLs to extract leads from')
    parser.add_argument('--file', '-f', help='File containing URLs (one per line)')
    parser.add_argument('--playwright', '-p', action='store_true', help='Force Playwright for JS-heavy sites')
    parser.add_argument('--output', '-o', help='Save results to JSON file')
    parser.add_argument('--raw', action='store_true', help='Show all leads including international/filtered')
    parser.add_argument('--dry-run', action='store_true', help='Extract only, do NOT save to database')
    args = parser.parse_args()

    urls = list(args.urls) if args.urls else []
    if args.file:
        try:
            with open(args.file, 'r') as f:
                urls.extend([line.strip() for line in f if line.strip() and not line.startswith('#')])
        except FileNotFoundError:
            logger.error(f"File not found: {args.file}")
            sys.exit(1)

    if not urls:
        parser.print_help()
        sys.exit(1)

    urls = list(dict.fromkeys(urls))

    mode = "DRY RUN (no DB save)" if args.dry_run else "LIVE → saves to dashboard"
    print(f"""
{'='*70}
  ONE-TIME URL EXTRACTOR — Smart Lead Hunter
  {len(urls)} URL(s) to process | Mode: {mode}
{'='*70}
""")

    print("🔧 Initializing pipeline...")
    from app.services.intelligent_pipeline import IntelligentPipeline
    pipeline = IntelligentPipeline()
    print("✅ Pipeline ready\n")

    all_results = []
    all_qualified_dicts = []
    total_leads = 0
    total_qualified = 0
    start_time = time.time()

    for i, url in enumerate(urls, 1):
        print(f"{'─'*70}")
        print(f"  [{i}/{len(urls)}] {url[:70]}{'...' if len(url) > 70 else ''}")
        print(f"{'─'*70}")

        # Step 1: Scrape
        print(f"   📥 Scraping...", end=' ')
        scrape_start = time.time()
        page_data = await scrape_url(url, use_playwright=args.playwright)
        scrape_time = time.time() - scrape_start

        if page_data.get('error') or not page_data['content']:
            error = page_data.get('error', 'Empty content')
            print(f"❌ FAILED — {error}")
            all_results.append({'url': url, 'status': 'failed', 'error': error, 'leads': [], 'qualified': 0})
            continue

        print(f"✅ {page_data['chars']:,} chars via {page_data['method']} ({scrape_time:.1f}s)")

        # Step 2: Extract
        print(f"   🤖 Extracting leads...", end=' ')
        extract_start = time.time()
        try:
            leads = await extract_leads(url, page_data['content'], pipeline)
        except Exception as e:
            print(f"❌ FAILED — {e}")
            all_results.append({'url': url, 'status': 'extract_failed', 'error': str(e), 'leads': [], 'qualified': 0})
            continue
        extract_time = time.time() - extract_start

        us_caribbean = []
        international = []
        filtered = []

        for lead in leads:
            lead_dict = {
                'hotel_name': lead.hotel_name,
                'brand': lead.brand,
                'city': lead.city,
                'state': lead.state,
                'country': lead.country,
                'opening_date': lead.opening_date,
                'room_count': lead.room_count,
                'property_type': lead.property_type,
                'qualification_score': lead.qualification_score,
                'source_url': url,
                'source_name': 'one-time-extract',
                'key_insights': getattr(lead, 'key_insights', None),
                'contact_name': getattr(lead, 'contact_name', None),
                'contact_title': getattr(lead, 'contact_title', None),
                'contact_email': getattr(lead, 'contact_email', None),
                'contact_phone': getattr(lead, 'contact_phone', None),
            }
            if lead.qualification_score >= 30:
                us_caribbean.append(lead_dict)
            elif hasattr(lead, 'country') and lead.country and lead.country.upper() not in ('USA', 'US', 'UNITED STATES'):
                international.append(lead_dict)
            else:
                filtered.append(lead_dict)

        total_leads += len(leads)
        total_qualified += len(us_caribbean)
        all_qualified_dicts.extend(us_caribbean)

        print(f"✅ {len(leads)} leads ({extract_time:.1f}s)")
        print(f"   📊 Results: {len(us_caribbean)} qualified | {len(international)} international | {len(filtered)} filtered")

        if us_caribbean:
            print(f"\n   🏨 QUALIFIED LEADS:")
            for lead in sorted(us_caribbean, key=lambda x: x['qualification_score'], reverse=True):
                score = lead['qualification_score']
                score_icon = '🔴' if score >= 70 else '🟠' if score >= 40 else '🔵'
                location = f"{lead['city']}, {lead['state']}" if lead['state'] else lead['city']
                print(f"      {score_icon} [{score}] {lead['hotel_name']} — {location}")
                if lead.get('opening_date'):
                    print(f"            Opening: {lead['opening_date']}")

        if args.raw and international:
            print(f"\n   🌍 INTERNATIONAL (filtered):")
            for lead in international[:10]:
                print(f"      ⚪ {lead['hotel_name']} — {lead['city']}, {lead.get('country', '?')}")

        all_results.append({
            'url': url, 'status': 'success', 'chars': page_data['chars'],
            'method': page_data['method'], 'scrape_time': round(scrape_time, 1),
            'extract_time': round(extract_time, 1), 'leads_total': len(leads),
            'leads_qualified': len(us_caribbean), 'leads_international': len(international),
            'leads_filtered': len(filtered), 'qualified_leads': us_caribbean,
        })
        print()

    # ═══════════════════════════════════════════════════════════
    # SAVE TO DATABASE
    # ═══════════════════════════════════════════════════════════
    if all_qualified_dicts:
        seen = set()
        unique_dicts = []
        for ld in all_qualified_dicts:
            key = (ld['hotel_name'] or '').lower().strip()
            if key not in seen:
                seen.add(key)
                unique_dicts.append(ld)

        print(f"{'='*70}")
        if args.dry_run:
            print(f"  🔍 DRY RUN — {len(unique_dicts)} unique leads found (not saved)")
        else:
            print(f"  💾 SAVING {len(unique_dicts)} unique leads to database...")
        print(f"{'='*70}")

        db_result = await save_leads_to_db(unique_dicts, dry_run=args.dry_run)

        if not args.dry_run:
            print(f"\n   ✅ New leads saved:     {db_result['saved']}")
            print(f"   🔄 Existing enriched:  {db_result['enriched']}")
            print(f"   ⏭️ Already in DB:      {db_result['skipped']}")
            if db_result['errors']:
                print(f"   ❌ Errors:             {db_result['errors']}")

    # Summary
    elapsed = time.time() - start_time
    print(f"""
{'='*70}
  EXTRACTION COMPLETE
{'='*70}
  URLs processed:    {len(urls)}
  Successful:        {sum(1 for r in all_results if r['status'] == 'success')}
  Failed:            {sum(1 for r in all_results if r['status'] != 'success')}
  Total leads:       {total_leads}
  Qualified (US/CB): {total_qualified}
  Time:              {elapsed:.1f}s ({elapsed/60:.1f}min)
{'='*70}
""")

    if total_qualified > 0:
        seen = set()
        unique_leads = []
        for r in all_results:
            for lead in r.get('qualified_leads', []):
                key = lead['hotel_name'].lower().strip()
                if key not in seen:
                    seen.add(key)
                    unique_leads.append(lead)

        print(f"  📋 UNIQUE QUALIFIED LEADS ({len(unique_leads)}):")
        print(f"  {'─'*66}")
        for lead in sorted(unique_leads, key=lambda x: x['qualification_score'], reverse=True):
            score = lead['qualification_score']
            score_icon = '🔴' if score >= 70 else '🟠' if score >= 40 else '🔵'
            location = f"{lead['city']}, {lead['state']}" if lead['state'] else lead['city']
            print(f"    {score_icon} [{score:>3}] {lead['hotel_name'][:40]:<40} {location}")
        print()

    filename = args.output or f"onetime_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'urls_processed': len(urls),
        'total_qualified': total_qualified,
        'mode': 'dry_run' if args.dry_run else 'live',
        'results': all_results,
    }
    with open(filename, 'w') as f:
        json.dump(output_data, f, indent=2)
    print(f"  💾 JSON log saved to: {filename}")

    if not args.dry_run and all_qualified_dicts:
        print(f"\n  🎉 Leads are now in your dashboard!")


if __name__ == '__main__':
    asyncio.run(main())