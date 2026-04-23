"""
Test Grounding with Google Search on Vertex AI
================================================
Tests whether grounding gives us better hotel research than the current
Serper→Gemini pipeline. Uses existing Vertex AI credentials (no new setup).

Usage:
    python scripts/test_grounding.py
    python scripts/test_grounding.py --hotel "Nickelodeon Hotels & Resorts Orlando" --location "Kissimmee, Florida"
    python scripts/test_grounding.py --hotel "Sandals Montego Bay" --location "Montego Bay, Jamaica"

What it tests:
    1. Whether grounding returns current info (2028 for Nickelodeon, not 2026)
    2. Whether grounding correctly identifies operator/owner/developer
    3. Source URLs it cites
    4. Rough latency (how long the call takes)

Cost per run: ~$0.04 (one grounded prompt on Gemini 2.5 Flash).
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Bootstrap sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import httpx


def _print_header(text: str) -> None:
    print()
    print("=" * 80)
    print(f"  {text}")
    print("=" * 80)


async def test_grounding(hotel_name: str, location: str) -> None:
    from app.services.gemini_client import get_gemini_url, get_gemini_headers

    _print_header(f"GROUNDING TEST — {hotel_name}")
    print(f"Location: {location}")
    print(f"Model: gemini-2.5-flash with google_search tool enabled")
    print()

    prompt = f"""You are researching a specific hotel for a uniform sales pipeline.

HOTEL: {hotel_name}
LOCATION: {location}

Use Google Search to find the MOST CURRENT information about this hotel.
Look for announcement delays, rescheduled opening dates, and the full
ownership/operator chain. Prefer 2025-2026 sources over older ones.

Return a JSON object with these fields:
{{
  "opening_date": "most recent announced opening or reopening date (e.g. 'December 2028', 'Q4 2026')",
  "project_type": "new_opening | renovation | rebrand | reopening | conversion",
  "room_count": 400,
  "brand": "the hotel brand/flag",
  "brand_tier": "tier1_ultra_luxury | tier2_luxury | tier3_upper_upscale | tier4_upscale",
  "management_company": "day-to-day hotel OPERATOR (NOT IP licensor like Paramount, Disney)",
  "owner": "PROPERTY OWNER / real estate holding entity",
  "developer": "entity BUILDING the property",
  "key_executives": ["names of CEO/principal at owner/developer for sales outreach"],
  "confidence": "high | medium | low",
  "notes": "any important recent news (delays, rebrands, ownership changes)"
}}

Return ONLY the JSON object, no other text."""

    url = get_gemini_url("gemini-2.5-flash")
    headers = get_gemini_headers()

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],  # ← enables grounding
        "generationConfig": {
            "temperature": 1.0,  # Grounding works best at temp 1.0
            "maxOutputTokens": 4096,
        },
    }

    _print_header("REQUEST")
    print(f"Prompt length: {len(prompt)} chars")
    print(f"Tools: [googleSearch]")

    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(url, headers=headers, json=payload)
    except Exception as ex:
        print(f"\n❌ Request failed: {ex}")
        return

    elapsed = time.time() - start

    if resp.status_code != 200:
        print(f"\n❌ HTTP {resp.status_code}: {resp.text[:500]}")
        return

    data = resp.json()

    _print_header("TIMING")
    print(f"Total round-trip: {elapsed:.1f}s")

    # Extract the text response
    try:
        candidate = data["candidates"][0]
        content = candidate["content"]["parts"][0]["text"]
    except (KeyError, IndexError) as ex:
        print(f"\n❌ Couldn't parse response: {ex}")
        print(json.dumps(data, indent=2)[:2000])
        return

    _print_header("GROUNDED RESPONSE (Gemini's answer)")
    print(content)

    # Try to parse the JSON inside
    try:
        # Strip markdown fences if present
        clean = content.strip()
        if clean.startswith("```"):
            clean = clean.split("```", 2)[1]
            if clean.startswith("json"):
                clean = clean[4:].strip()
            clean = clean.rsplit("```", 1)[0].strip()
        parsed = json.loads(clean)
        _print_header("PARSED JSON")
        for k, v in parsed.items():
            print(f"  {k:<22} = {v}")
    except (json.JSONDecodeError, ValueError):
        print("\n⚠️ Could not parse JSON from response")

    # Show grounding metadata — which URLs did Gemini actually read
    grounding_meta = candidate.get("groundingMetadata", {})
    web_queries = grounding_meta.get("webSearchQueries", [])
    grounding_chunks = grounding_meta.get("groundingChunks", [])

    _print_header("GROUNDING METADATA — what Gemini actually searched")
    print(f"Search queries Gemini decided to run ({len(web_queries)}):")
    for i, q in enumerate(web_queries, 1):
        print(f"  {i}. {q}")

    print(f"\nSource URLs Gemini read ({len(grounding_chunks)}):")
    for i, chunk in enumerate(grounding_chunks[:10], 1):
        web = chunk.get("web", {})
        title = web.get("title", "(no title)")
        uri = web.get("uri", "(no uri)")
        # URIs are vertexaisearch.cloud.google.com redirects — the title is
        # the actual domain (aljazeera.com, etc.)
        print(f"  {i}. {title}")
        print(f"     {uri[:120]}")

    _print_header("COST ESTIMATE")
    # Gemini 2.5 Flash grounding = $35/1000 prompts = $0.035/prompt
    # Plus token costs (negligible for 1 call)
    print(f"  Grounded prompt:     $0.035")
    print(f"  Gemini tokens:       ~$0.005 (2.5 Flash)")
    print(f"  TOTAL this request:  ~$0.04")

    _print_header("DONE")


def main():
    parser = argparse.ArgumentParser(description="Test Vertex AI Grounding")
    parser.add_argument(
        "--hotel",
        default="Nickelodeon Hotels & Resorts Orlando",
        help="Hotel name to research (default: Nickelodeon Orlando)",
    )
    parser.add_argument(
        "--location",
        default="Kissimmee, Florida",
        help="Hotel location (default: Kissimmee, Florida)",
    )
    args = parser.parse_args()
    asyncio.run(test_grounding(args.hotel, args.location))


if __name__ == "__main__":
    main()
