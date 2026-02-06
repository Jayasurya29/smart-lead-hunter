from dotenv import load_dotenv
load_dotenv()
import asyncio
from app.services.intelligent_pipeline import IntelligentPipeline, PipelineConfig
import httpx
from bs4 import BeautifulSoup

async def test():
    url = "https://www.sleepermagazine.com/stories/originals/sleeper-presents-top-new-hotel-openings-of-2026/"
    
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        print(f"Status: {r.status_code}, Length: {len(r.text)}")
    
    # Extract clean text from HTML
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    print(f"Clean text: {len(text)} chars")
    print(f"First 200: {text[:200]}")
    
    config = PipelineConfig()
    pipeline = IntelligentPipeline(config)
    
    pages = [{"url": url, "content": text, "source": "Sleeper Magazine"}]
    result = await pipeline.process_pages(pages, source_name="Sleeper Magazine")
    
    print(f"\nFound {len(result.final_leads)} leads:")
    for lead in result.final_leads:
        print(f"  - {lead.hotel_name} ({lead.city}, {lead.state}) Score: {lead.score}")

asyncio.run(test())
