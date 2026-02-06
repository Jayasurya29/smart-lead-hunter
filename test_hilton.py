from dotenv import load_dotenv
load_dotenv()
import asyncio, logging
logging.basicConfig(level=logging.INFO)
from app.services.intelligent_pipeline import IntelligentPipeline, PipelineConfig
import httpx
from bs4 import BeautifulSoup

async def test():
    url = "https://stories.hilton.com/releases/new-hilton-openings-in-2026"
    
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(url)
        print(f"Status: {r.status_code}, Length: {len(r.text)}")
    
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    print(f"Clean text: {len(text)} chars")
    print(f"First 300: {text[:300]}")
    
    config = PipelineConfig()
    pipeline = IntelligentPipeline(config)
    
    pages = [{"url": url, "content": text, "source": "Hilton Newsroom"}]
    result = await pipeline.process_pages(pages, source_name="Hilton Newsroom")
    
    print(f"\nFound {len(result.final_leads)} leads:")
    for lead in result.final_leads:
        print(f"  - {lead.hotel_name} ({lead.city}, {lead.state})")

asyncio.run(test())
