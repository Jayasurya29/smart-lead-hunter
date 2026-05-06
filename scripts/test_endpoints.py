"""
Quick test: verify which Gemini models respond on which endpoints.
Tests all 9 combinations (3 models × 3 endpoints).
Run from smart-lead-hunter root:
  python scripts/test_endpoints.py
"""
import asyncio
import os
import time
import sys

# ── Load env ──
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MODELS = [
    "gemini-2.5-flash",
    "gemini-3-flash-preview",
    "gemini-2.5-flash-lite",
]

ENDPOINTS = [
    "global",
    "us-central1",
    "us-east4",
]

PROMPT = 'Reply with exactly this JSON and nothing else: {"ok": true}'


async def test_one(model: str, endpoint: str, project: str, headers: dict) -> dict:
    import httpx

    if endpoint == "global":
        host = "aiplatform.googleapis.com"
    else:
        host = f"{endpoint}-aiplatform.googleapis.com"

    url = (
        f"https://{host}/v1/"
        f"projects/{project}/locations/{endpoint}/"
        f"publishers/google/models/{model}:generateContent"
    )

    payload = {
        "contents": [{"role": "user", "parts": [{"text": PROMPT}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 50,
        },
    }

    # Disable thinking for speed
    if "gemini-3" in model:
        payload["generationConfig"]["thinking_config"] = {"thinking_level": "minimal"}
    elif "gemini-2.5" in model:
        payload["generationConfig"]["thinking_config"] = {"thinking_budget": 0}

    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
        elapsed = time.monotonic() - start

        if resp.status_code == 200:
            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            return {"status": "✅ OK", "elapsed": f"{elapsed:.1f}s", "response": text[:60]}
        else:
            body = resp.text[:120]
            return {"status": f"❌ HTTP {resp.status_code}", "elapsed": f"{elapsed:.1f}s", "response": body}
    except Exception as e:
        elapsed = time.monotonic() - start
        return {"status": f"❌ {type(e).__name__}", "elapsed": f"{elapsed:.1f}s", "response": str(e)[:80]}


async def main():
    # Get credentials
    try:
        sys.path.insert(0, os.getcwd())
        from app.services.ai_client import _ensure_init, _get_config, _creds
        _ensure_init()
        config = _get_config()
        project = config["vertex_project_id"]

        import google.auth.transport.requests
        from app.services.ai_client import _creds as creds
        if creds and creds.expired:
            creds.refresh(google.auth.transport.requests.Request())
        token = creds.token if creds else None
        if not token:
            print("ERROR: No credentials found. Run from smart-lead-hunter root.")
            return

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        print(f"Project: {project}")
        print(f"Testing {len(MODELS)} models × {len(ENDPOINTS)} endpoints = {len(MODELS)*len(ENDPOINTS)} calls\n")
    except Exception as e:
        print(f"ERROR loading credentials: {e}")
        return

    results = []
    for model in MODELS:
        for endpoint in ENDPOINTS:
            print(f"Testing {model} @ {endpoint}...", end=" ", flush=True)
            result = await test_one(model, endpoint, project, headers)
            print(f"{result['status']} ({result['elapsed']})")
            results.append((model, endpoint, result))
            await asyncio.sleep(1)  # small gap between calls

    print("\n" + "═"*70)
    print(f"{'MODEL':<30} {'ENDPOINT':<15} {'STATUS':<20} {'TIME'}")
    print("═"*70)
    for model, endpoint, r in results:
        print(f"{model:<30} {endpoint:<15} {r['status']:<20} {r['elapsed']}")
    print("═"*70)

    # Summary — which combinations are safe to use
    working = [(m, e) for m, e, r in results if "✅" in r["status"]]
    print(f"\n✅ Working combinations ({len(working)}/{len(results)}):")
    for m, e in working:
        print(f"   {m} @ {e}")

asyncio.run(main())
