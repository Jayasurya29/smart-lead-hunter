import asyncio, httpx
from app.config import settings
from app.services.ai_client import ai_generate
async def main():
    print("FULL:", settings.gemini_model, "| LITE:", settings.gemini_model_lite)
    async with httpx.AsyncClient() as c:
        for m in (settings.gemini_model, settings.gemini_model_lite):
            r = await ai_generate(c, "Reply with the word OK", model=m, max_tokens=20)
            print(m, "->", repr(r))
asyncio.run(main())
