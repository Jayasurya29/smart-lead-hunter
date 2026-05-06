"""Quick test: us-west1 endpoint for all 3 models."""
import asyncio, os, sys, time

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
ENDPOINTS = ["us-west1"]
PROMPT = 'Reply with exactly this JSON and nothing else: {"ok": true}'

async def test_one(model, endpoint, project, headers):
    import httpx
    host = f"{endpoint}-aiplatform.googleapis.com"
    url = (
        f"https://{host}/v1/projects/{project}/locations/{endpoint}/"
        f"publishers/google/models/{model}:generateContent"
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": PROMPT}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 50},
    }
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
            text = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            return {"status": "✅ OK", "elapsed": f"{elapsed:.1f}s", "response": text[:60]}
        else:
            return {"status": f"❌ HTTP {resp.status_code}", "elapsed": f"{elapsed:.1f}s", "response": resp.text[:80]}
    except Exception as e:
        return {"status": f"❌ {type(e).__name__}", "elapsed": f"{time.monotonic()-start:.1f}s", "response": str(e)[:80]}

async def main():
    sys.path.insert(0, os.getcwd())
    from app.services.ai_client import _ensure_init, _get_config, _creds
    _ensure_init()
    config = _get_config()
    project = config["vertex_project_id"]
    import google.auth.transport.requests
    from app.services.ai_client import _creds as creds
    if creds and creds.expired:
        creds.refresh(google.auth.transport.requests.Request())
    headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}

    print(f"Testing us-west1 endpoint for {len(MODELS)} models:\n")
    for model in MODELS:
        for endpoint in ENDPOINTS:
            print(f"  {model} @ {endpoint}...", end=" ", flush=True)
            r = await test_one(model, endpoint, project, headers)
            print(f"{r['status']} ({r['elapsed']})")
            await asyncio.sleep(1)

asyncio.run(main())
