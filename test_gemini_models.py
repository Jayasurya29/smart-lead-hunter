"""Quick test — pings each Gemini model in our intended fallback chain
to verify which ones actually work on this GCP project.

Usage:
    cd C:\\Users\\it2\\smart-lead-hunter
    python test_gemini_models.py

What it does:
    For each candidate model, send a tiny test prompt and report:
      - SUCCESS (with response text + latency)
      - FAILED (with HTTP status / error)

This tells us BEFORE deploying the fallback chain whether the fallback
models are actually accessible. If gemini-3-flash-preview returns a
404, we know the project doesn't have access and we should remove it
from the chain.
"""

import asyncio
import time
import sys

# Same imports as contact_enrichment.py — uses the existing auth machinery
from app.services.ai_client import (
    get_ai_url,
    get_ai_headers,
    is_vertex_ai,
)
import httpx


CANDIDATE_MODELS = [
    "gemini-2.5-flash",         # current primary — should always work
    "gemini-2.5-flash-lite",    # current fallback for some helpers
    "gemini-3-flash-preview",   # NEW — preview, may or may not be accessible
    "gemini-3.1-flash-lite-preview",  # NEW — preview, even newer
    "gemini-2.0-flash-001",     # OLD — may be access-frozen
]


async def test_one_model(model_name: str) -> None:
    """Ping a model with a minimal prompt. Reports result."""
    url = get_ai_url(model_name)
    headers = get_ai_headers()

    payload = {
        "contents": [{"role": "user", "parts": [{"text": "Reply with the single word: OK"}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 10,
        },
    }

    print(f"\n  Testing {model_name}...")
    print(f"    URL: {url[:90]}...")

    started = time.time()
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
        elapsed = time.time() - started

        if resp.status_code == 200:
            data = resp.json()
            text = ""
            try:
                text = data["candidates"][0]["content"]["parts"][0]["text"]
            except (KeyError, IndexError, TypeError):
                text = "(no text in response)"
            print(f"    ✓ SUCCESS ({elapsed:.2f}s) — model returned: {text!r}")
            return True
        elif resp.status_code == 404:
            print(
                f"    ✗ NOT FOUND (404) — this model is NOT accessible on this project"
            )
            print(f"      Body: {resp.text[:300]}")
            return False
        elif resp.status_code == 429:
            print(
                f"    ⚠ RATE LIMITED (429) — model exists but DSQ pool is busy right now. "
                f"This DOES count as 'works' — when pool clears, calls will succeed."
            )
            print(f"      Retry-After: {resp.headers.get('Retry-After', '<not set>')}")
            return True  # 429 means model is reachable
        elif resp.status_code == 403:
            print(
                f"    ✗ FORBIDDEN (403) — project lacks permission to use this model"
            )
            print(f"      Body: {resp.text[:300]}")
            return False
        else:
            print(f"    ✗ HTTP {resp.status_code}")
            print(f"      Body: {resp.text[:300]}")
            return False
    except httpx.TimeoutException:
        print(f"    ✗ TIMEOUT after {time.time() - started:.1f}s")
        return False
    except Exception as e:
        print(f"    ✗ EXCEPTION: {type(e).__name__}: {e}")
        return False


async def main():
    print("=" * 70)
    print("Gemini Model Availability Test")
    print("=" * 70)
    print(f"Provider: {'Vertex AI' if is_vertex_ai() else 'Gemini Developer API'}")

    accessible = []
    not_accessible = []
    for model in CANDIDATE_MODELS:
        ok = await test_one_model(model)
        if ok:
            accessible.append(model)
        else:
            not_accessible.append(model)
        await asyncio.sleep(1)  # tiny delay so we don't trigger our own rate limit

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n✓ Accessible models ({len(accessible)}):")
    for m in accessible:
        print(f"    {m}")
    print(f"\n✗ NOT accessible ({len(not_accessible)}):")
    for m in not_accessible:
        print(f"    {m}")

    print("\nRecommendation: build fallback chain using ONLY the accessible models.")


if __name__ == "__main__":
    asyncio.run(main())
