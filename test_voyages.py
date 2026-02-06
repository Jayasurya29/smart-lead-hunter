from dotenv import load_dotenv
load_dotenv()
import asyncio, logging
logging.basicConfig(level=logging.INFO)
from app.services.intelligent_pipeline import IntelligentPipeline, PipelineConfig
import httpx
from bs4 import BeautifulSoup

async def test():
    url = "https://www.voyages-d-affaires.com/en/hotel-openings-ameriques-2026"
    
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        print(f"Status: {r.status_code}, Length: {len(r.text)}")
    
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    print(f"Clean text: {len(text)} chars")
    print(f"First 200: {text[:200]}")
    
    config = PipelineConfig()
    pipeline = IntelligentPipeline(config)
    
    pages = [{"url": url, "content": text, "source": "Voyages d'Affaires"}]
    result = await pipeline.process_pages(pages, source_name="Voyages d'Affaires")
    
    print(f"\nFound {len(result.final_leads)} leads:")
    for lead in result.final_leads:
        print(f"  - {lead.hotel_name} ({lead.city}, {lead.state})")

asyncio.run(test())
