"""
Quick test script for grounding prompts.
Run from smart-lead-hunter root:

    python scripts/test_grounding_prompt.py "Blue Chip Casino Hotel Spa" "Michigan City" "IN"
    python scripts/test_grounding_prompt.py "AC Hotel Cincinnati at The Banks" "Cincinnati" "OH"
    python scripts/test_grounding_prompt.py "Omni Orlando Resort at ChampionsGate" "Orlando" "FL"

Or with a custom prompt:
    python scripts/test_grounding_prompt.py --prompt "Blue Chip Casino Hotel Spa leadership team"

Tests the grounding API directly — no enrichment pipeline, no DB, no scoring.
Shows exactly what Gemini returns for a given prompt.
"""

import asyncio
import json
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()


async def test_grounding(prompt: str):
    """Call the grounding API and print full response."""
    import httpx
    from app.services.ai_client import _ensure_init, _get_config
    from app.services.gemini_client import get_gemini_headers

    _ensure_init()
    config = _get_config()
    project = config["vertex_project_id"]
    model = config["model"]
    location = "us-central1"

    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{location}/"
        f"publishers/google/models/{model}:generateContent"
    )

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {
            "temperature": 1.0,
            "maxOutputTokens": 8192,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    print(f"\n{'='*60}")
    print(f"PROMPT: {prompt}")
    print(f"{'='*60}\n")

    start = time.monotonic()
    async with httpx.AsyncClient(timeout=45) as client:
        resp = await client.post(url, headers=get_gemini_headers(), json=payload)

    elapsed = time.monotonic() - start
    data = resp.json()

    # Extract text
    try:
        candidate = data["candidates"][0]
        parts = (candidate.get("content") or {}).get("parts") or []
        text = (parts[0].get("text") if parts else "") or ""
    except (KeyError, IndexError):
        text = ""
        print(f"ERROR: Bad response shape. Keys: {list(data.keys())}")
        print(json.dumps(data, indent=2)[:2000])
        return

    # Extract citations
    grounding_meta = candidate.get("groundingMetadata", {}) or {}
    sources = []
    for chunk in (grounding_meta.get("groundingChunks", []) or []):
        web = chunk.get("web", {}) or {}
        title = web.get("title", "")
        uri = web.get("uri", "")
        if title:
            sources.append(f"  {title}: {uri[:80]}")

    # Extract the search queries Gemini actually ran
    search_queries = grounding_meta.get("webSearchQueries", []) or []

    finish_reason = candidate.get("finishReason", "?")

    print(f"TIME: {elapsed:.1f}s")
    print(f"FINISH REASON: {finish_reason}")
    print(f"TEXT LENGTH: {len(text)} chars")
    print(f"CITATIONS: {len(sources)}")
    print(f"SEARCH QUERIES RUN: {len(search_queries)}")
    print()
    if search_queries:
        print("QUERIES GEMINI GENERATED:")
        for i, q in enumerate(search_queries, 1):
            print(f"  {i}. {q}")
        print()
    print("FULL TEXT:")
    print("-" * 40)
    print(text)
    print("-" * 40)
    print()
    print("FULL TEXT:")
    print("-" * 40)
    print(text)
    print("-" * 40)
    print()
    if sources:
        print("SOURCES:")
        for s in sources:
            print(s)
    else:
        print("⚠ ZERO CITATIONS — PHANTOM RESPONSE (answered from training data)")
    print()


async def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print('  python scripts/test_grounding_prompt.py "Hotel Name" "City" "State"')
        print('  python scripts/test_grounding_prompt.py --prompt "your custom prompt"')
        sys.exit(1)

    if sys.argv[1] == "--prompt":
        prompt = " ".join(sys.argv[2:])
        await test_grounding(prompt)
    else:
        hotel = sys.argv[1]
        city = sys.argv[2] if len(sys.argv) > 2 else ""
        state = sys.argv[3] if len(sys.argv) > 3 else ""

        # Test multiple prompt styles
        prompts = [
            # Style 1: Keywords (like Google search)
            f"{hotel} in {city}, {state} current leadership team and department heads 2026",
            # Style 2: Simple question
            f"Who are the current leadership and management team at {hotel} in {city}, {state}?",
            # Style 3: AI Overview style
            f"{hotel} leadership team",
            # Style 4: Specific roles (old approach)
            (
                f"Who are the current General Manager, Director of Housekeeping, "
                f"Director of Operations, Director of HR, Director of Purchasing, "
                f"and Director of Food & Beverage at {hotel} in {city}, {state}? "
                f"List each person's full name, exact job title, and company."
            ),
        ]

        for i, p in enumerate(prompts, 1):
            print(f"\n{'#'*60}")
            print(f"TEST {i} of {len(prompts)}")
            print(f"{'#'*60}")
            await test_grounding(p)
            # Small delay between calls
            if i < len(prompts):
                await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
