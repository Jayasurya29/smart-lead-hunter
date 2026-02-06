from dotenv import load_dotenv
load_dotenv()
import asyncio, httpx, os, json
from app.services.intelligent_pipeline import ContentClassifier

async def test():
    key = os.getenv("GEMINI_API_KEY")
    prompt = ContentClassifier._build_prompt("Hard Rock International plans a new 400-room hotel and casino in Puerto Rico, opening 2027. The development is valued at 850 million dollars.")
    
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
            headers={"x-goog-api-key": key},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 200,
                    "responseMimeType": "application/json",
                    "thinkingConfig": {"thinkingBudget": 0}
                }
            }
        )
        print("Status:", r.status_code)
        data = r.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        print("FULL RAW:", repr(text))
        parsed = json.loads(text)
        print("Parsed:", parsed)
        print("is_new_hotel_opening:", parsed.get("is_new_hotel_opening"))

asyncio.run(test())
