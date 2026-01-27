#!/usr/bin/env python3
"""
SMART LEAD HUNTER v7 - ULTIMATE SCRAPER WITH FULL TECH STACK
=============================================================
Complete hotel lead scraping system with:

SCRAPING:
- Playwright for JavaScript-heavy sites (chain newsrooms)
- BeautifulSoup + httpx for static HTML (faster)
- Auto-detection of which method to use

AI EXTRACTION:
- Ollama (Llama 3.2) for intelligent data extraction
- spaCy for NLP entity recognition

SCORING:
- 100-point scoring system
- Skip filters (budget brands, international)

DATABASE:
- PostgreSQL with pgvector for deduplication
- FastAPI backend integration

Usage:
    python smart_scraper_v7.py
    python smart_scraper_v7.py --source orange_studio
    python smart_scraper_v7.py --discover
"""

import asyncio
import httpx
from bs4 import BeautifulSoup
import json
import re
import sys
import argparse
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime
from urllib.parse import quote_plus, urljoin, urlparse
from dataclasses import dataclass
from enum import Enum

# Rich for beautiful output
try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.panel import Panel
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

# Playwright for JS-heavy sites
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️  Playwright not installed. Run: pip install playwright && playwright install chromium")

# spaCy for NLP
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False

# Import scoring system
from app.services.scorer import (
    calculate_lead_score,
    should_skip_brand,
    should_skip_location,
    format_score_breakdown
)


# =============================================================================
# CONFIG
# =============================================================================

API_BASE_URL = "http://localhost:8000"
OLLAMA_URL = "http://localhost:11434"
MIN_YEAR = 2026

# Site classification - which sites need JavaScript rendering
JS_REQUIRED_SITES = [
    "newsroom.marriott.com",
    "stories.hilton.com",
    "newsroom.hilton.com",
    "news.marriott.com",
    "press.fourseasons.com",
    "newsroom.hyatt.com",
    "news.hyatt.com",
    "ihgplc.com",
    "development.marriott.com",
]

# Sites that work with simple HTTP requests
STATIC_SITES = [
    "theorangestudio.com",
    "hospitalitynet.org",
    "hotelnewsresource.com",
    "hotelmanagement.net",
    "costar.com",
]


class ScrapeMethod(Enum):
    """Scraping method to use"""
    HTTPX = "httpx"           # Fast, for static sites
    PLAYWRIGHT = "playwright"  # Slower, for JS sites
    AUTO = "auto"             # Auto-detect


@dataclass
class ScrapeResult:
    """Result from scraping a page"""
    url: str
    html: Optional[str]
    text: Optional[str]
    success: bool
    method: ScrapeMethod
    error: Optional[str] = None
    load_time: float = 0.0


# Location lists for display
FLORIDA_CITIES = [
    "miami", "palm beach", "naples", "orlando", "tampa", "jacksonville",
    "fort lauderdale", "clearwater", "sarasota", "key west", "boca raton",
    "key largo", "delray", "west palm", "st. petersburg", "destin", "pensacola"
]

CARIBBEAN_COUNTRIES = [
    "bahamas", "jamaica", "puerto rico", "dominican republic", "aruba",
    "barbados", "cayman", "turks", "bermuda", "anguilla", "antigua",
    "st. lucia", "grenada", "st. kitts", "virgin islands", "curacao"
]

# Discovery queries
DISCOVERY_QUERIES = [
    "new luxury hotel opening 2026 Florida",
    "new hotel opening 2027 Miami",
    "Four Seasons new hotel opening Americas",
    "Ritz Carlton new opening United States",
    "luxury resort opening Caribbean 2026",
]

# Skip these domains when discovering
SKIP_DOMAINS = [
    "booking.com", "expedia.com", "tripadvisor.com", "kayak.com",
    "hotels.com", "trivago.com", "facebook.com", "twitter.com",
    "instagram.com", "youtube.com", "linkedin.com", "pinterest.com",
    "wikipedia.org", "amazon.com"
]


# =============================================================================
# SCRAPING ENGINE
# =============================================================================

class ScrapingEngine:
    """
    Unified scraping engine with multiple backends:
    - Playwright for JavaScript sites
    - httpx for static sites
    - Auto-detection based on site
    """
    
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.http_client: Optional[httpx.AsyncClient] = None
        self.nlp = None  # spaCy model
        
    async def initialize(self):
        """Initialize scraping engines"""
        # Initialize httpx client
        self.http_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        
        # Initialize Playwright if available
        if PLAYWRIGHT_AVAILABLE:
            try:
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                print("   ✅ Playwright browser initialized")
            except Exception as e:
                print(f"   ⚠️  Playwright init failed: {e}")
                self.browser = None
        
        # Initialize spaCy if available
        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load("en_core_web_sm")
                print("   ✅ spaCy NLP model loaded")
            except:
                print("   ⚠️  spaCy model not found. Run: python -m spacy download en_core_web_sm")
                self.nlp = None
    
    async def close(self):
        """Clean up resources"""
        if self.http_client:
            await self.http_client.aclose()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    def _detect_method(self, url: str) -> ScrapeMethod:
        """Auto-detect best scraping method for URL"""
        domain = urlparse(url).netloc.lower()
        
        # Check if JS is required
        for js_site in JS_REQUIRED_SITES:
            if js_site in domain:
                return ScrapeMethod.PLAYWRIGHT
        
        # Check if known static site
        for static_site in STATIC_SITES:
            if static_site in domain:
                return ScrapeMethod.HTTPX
        
        # Default to httpx (faster), fall back to Playwright if needed
        return ScrapeMethod.HTTPX
    
    async def scrape(self, url: str, method: ScrapeMethod = ScrapeMethod.AUTO) -> ScrapeResult:
        """
        Scrape a URL using the appropriate method.
        
        Args:
            url: URL to scrape
            method: Scraping method (AUTO, HTTPX, or PLAYWRIGHT)
        
        Returns:
            ScrapeResult with HTML content and metadata
        """
        start_time = datetime.now()
        
        # Auto-detect method if needed
        if method == ScrapeMethod.AUTO:
            method = self._detect_method(url)
        
        # Try primary method
        if method == ScrapeMethod.PLAYWRIGHT and self.browser:
            result = await self._scrape_playwright(url)
        else:
            result = await self._scrape_httpx(url)
        
        # If httpx failed, try Playwright as fallback
        if not result.success and method == ScrapeMethod.HTTPX and self.browser:
            print(f"      ↪️  Retrying with Playwright...")
            result = await self._scrape_playwright(url)
        
        result.load_time = (datetime.now() - start_time).total_seconds()
        return result
    
    async def _scrape_httpx(self, url: str) -> ScrapeResult:
        """Scrape using httpx (fast, for static sites)"""
        try:
            response = await self.http_client.get(url)
            
            if response.status_code == 200:
                html = response.text
                soup = BeautifulSoup(html, 'lxml')
                
                # Remove script/style tags
                for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                    tag.decompose()
                
                text = soup.get_text(' ', strip=True)
                
                return ScrapeResult(
                    url=url,
                    html=html,
                    text=text,
                    success=True,
                    method=ScrapeMethod.HTTPX
                )
            else:
                return ScrapeResult(
                    url=url, html=None, text=None, success=False,
                    method=ScrapeMethod.HTTPX,
                    error=f"HTTP {response.status_code}"
                )
        except Exception as e:
            return ScrapeResult(
                url=url, html=None, text=None, success=False,
                method=ScrapeMethod.HTTPX,
                error=str(e)
            )
    
    async def _scrape_playwright(self, url: str) -> ScrapeResult:
        """Scrape using Playwright (for JavaScript sites)"""
        if not self.browser:
            return ScrapeResult(
                url=url, html=None, text=None, success=False,
                method=ScrapeMethod.PLAYWRIGHT,
                error="Playwright not available"
            )
        
        page = None
        try:
            page = await self.browser.new_page()
            
            # Set viewport
            await page.set_viewport_size({"width": 1920, "height": 1080})
            
            # Navigate with wait for network idle
            await page.goto(url, wait_until="networkidle", timeout=30000)
            
            # Wait a bit for dynamic content
            await asyncio.sleep(2)
            
            # Scroll to load lazy content
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            
            # Get HTML
            html = await page.content()
            
            # Extract text
            text = await page.evaluate("""
                () => {
                    // Remove unwanted elements
                    const unwanted = document.querySelectorAll('script, style, nav, footer, header, aside, [role="navigation"]');
                    unwanted.forEach(el => el.remove());
                    return document.body.innerText;
                }
            """)
            
            return ScrapeResult(
                url=url,
                html=html,
                text=text,
                success=True,
                method=ScrapeMethod.PLAYWRIGHT
            )
        except Exception as e:
            return ScrapeResult(
                url=url, html=None, text=None, success=False,
                method=ScrapeMethod.PLAYWRIGHT,
                error=str(e)
            )
        finally:
            if page:
                await page.close()
    
    def extract_with_spacy(self, text: str) -> Dict:
        """Extract entities using spaCy NLP"""
        if not self.nlp:
            return {}
        
        doc = self.nlp(text[:100000])  # Limit text size
        
        entities = {
            "persons": [],
            "organizations": [],
            "locations": [],
            "dates": [],
        }
        
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                entities["persons"].append(ent.text)
            elif ent.label_ == "ORG":
                entities["organizations"].append(ent.text)
            elif ent.label_ in ["GPE", "LOC"]:
                entities["locations"].append(ent.text)
            elif ent.label_ == "DATE":
                entities["dates"].append(ent.text)
        
        # Deduplicate
        for key in entities:
            entities[key] = list(set(entities[key]))[:10]
        
        return entities


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def extract_phones(text: str) -> List[str]:
    """Extract phone numbers from text"""
    patterns = [
        r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',
        r'\([0-9]{3}\)\s*[0-9]{3}[-.\s]?[0-9]{4}',
    ]
    phones = []
    for pattern in patterns:
        for match in re.findall(pattern, text):
            clean = re.sub(r'[^\d]', '', match)
            if len(clean) >= 10:
                formatted = f"({clean[-10:-7]}) {clean[-7:-4]}-{clean[-4:]}"
                if formatted not in phones:
                    phones.append(formatted)
    return phones[:2]


def extract_emails(text: str) -> List[str]:
    """Extract email addresses"""
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = list(set(re.findall(pattern, text.lower())))
    skip = ["info@", "contact@", "hello@", "noreply", "support@", "admin@", "webmaster@"]
    return [e for e in emails if not any(s in e for s in skip)][:3]


def detect_brand(name: str) -> Optional[str]:
    """Detect hotel brand from name"""
    name_lower = name.lower()
    
    brands = [
        ("four seasons", "Four Seasons"), ("ritz-carlton", "Ritz-Carlton"),
        ("st. regis", "St. Regis"), ("park hyatt", "Park Hyatt"),
        ("mandarin oriental", "Mandarin Oriental"), ("aman", "Aman"),
        ("rosewood", "Rosewood"), ("bulgari", "Bulgari"), ("one&only", "One&Only"),
        ("waldorf astoria", "Waldorf Astoria"), ("conrad", "Conrad"),
        ("jw marriott", "JW Marriott"), ("edition", "EDITION"),
        ("montage", "Montage"), ("fairmont", "Fairmont"), ("auberge", "Auberge"),
        ("delano", "Delano"), ("nobu", "Nobu"), ("thompson", "Thompson"),
        ("andaz", "Andaz"), ("1 hotel", "1 Hotel"), ("kimpton", "Kimpton"),
        ("w hotel", "W"), ("hilton", "Hilton"), ("marriott", "Marriott"),
        ("hyatt", "Hyatt"), ("omni", "Omni"), ("loews", "Loews"),
        ("peninsula", "Peninsula"), ("graduate", "Graduate Hotels"),
        ("signia", "Signia"), ("canopy", "Canopy"), ("curio", "Curio"),
        ("lxr", "LXR"), ("tempo", "Tempo"),
    ]
    
    for key, brand_name in brands:
        if key in name_lower:
            return brand_name
    return None


def print_header(text: str):
    """Print a formatted header"""
    if RICH_AVAILABLE:
        console.print(Panel(text, style="bold blue"))
    else:
        print("\n" + "=" * 70)
        print(f"   {text}")
        print("=" * 70)


def print_success(text: str):
    """Print success message"""
    if RICH_AVAILABLE:
        console.print(f"   ✅ {text}", style="green")
    else:
        print(f"   ✅ {text}")


def print_error(text: str):
    """Print error message"""
    if RICH_AVAILABLE:
        console.print(f"   ❌ {text}", style="red")
    else:
        print(f"   ❌ {text}")


def print_warning(text: str):
    """Print warning message"""
    if RICH_AVAILABLE:
        console.print(f"   ⚠️  {text}", style="yellow")
    else:
        print(f"   ⚠️  {text}")


def print_info(text: str):
    """Print info message"""
    if RICH_AVAILABLE:
        console.print(f"   ℹ️  {text}", style="cyan")
    else:
        print(f"   ℹ️  {text}")
# =============================================================================
# PART 2: EXTRACTORS AND MAIN LOGIC
# =============================================================================
# This continues from smart_scraper_v7_part1.py
# =============================================================================


# =============================================================================
# ORANGE STUDIO EXTRACTOR
# =============================================================================

async def extract_orange_studio(engine: ScrapingEngine, url: str) -> List[Dict]:
    """Extract hotels from Orange Studio using grid divs"""
    print("   🎯 Orange Studio extraction...")
    
    result = await engine.scrape(url, ScrapeMethod.HTTPX)
    
    if not result.success:
        print_error(f"Failed to fetch: {result.error}")
        return []
    
    print_success(f"Fetched {len(result.html):,} chars ({result.method.value}, {result.load_time:.1f}s)")
    
    soup = BeautifulSoup(result.html, "lxml")
    hotels = []
    
    us_pattern = re.compile(r'^(.+?)\s+United States\s+(20\d{2})\s*(Beach Resort|City Hotel|Resort|Hotel|Waterfront Hotel|Ski Resort)?')
    
    for div in soup.find_all("div", class_="grid"):
        text = div.get_text(" ", strip=True)
        
        if len(text) < 10 or len(text) > 200:
            continue
        
        # US pattern
        match = us_pattern.match(text)
        if match:
            name = match.group(1).strip()
            year = int(match.group(2))
            hotel_type = match.group(3) or "Hotel"
            
            if year >= MIN_YEAR:
                name_lower = name.lower()
                city, state = None, None
                
                for fl_city in FLORIDA_CITIES:
                    if fl_city in name_lower:
                        city = fl_city.title()
                        state = "Florida"
                        break
                
                hotels.append({
                    "hotel_name": name,
                    "brand": detect_brand(name),
                    "city": city,
                    "state": state,
                    "country": "USA",
                    "opening_date": str(year),
                    "hotel_type": hotel_type,
                    "source_url": url,
                    "source_site": "theorangestudio.com"
                })
            continue
        
        # Caribbean pattern
        for country in CARIBBEAN_COUNTRIES:
            pattern = rf'^(.+?)\s+{country}\s+(20\d{{2}})'
            match = re.match(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                year = int(match.group(2))
                
                if year >= MIN_YEAR:
                    hotels.append({
                        "hotel_name": name,
                        "brand": detect_brand(name),
                        "city": country.title(),
                        "state": None,
                        "country": country.title(),
                        "opening_date": str(year),
                        "source_url": url,
                        "source_site": "theorangestudio.com"
                    })
                break
    
    return hotels


# =============================================================================
# AI EXTRACTION WITH OLLAMA
# =============================================================================

async def ai_extract_hotels(text: str, source_url: str, spacy_entities: Dict = None) -> List[Dict]:
    """Use Ollama to extract hotels from text"""
    
    # Add spaCy hints if available
    hints = ""
    if spacy_entities:
        if spacy_entities.get("organizations"):
            hints += f"\nOrganizations found: {', '.join(spacy_entities['organizations'][:5])}"
        if spacy_entities.get("locations"):
            hints += f"\nLocations found: {', '.join(spacy_entities['locations'][:5])}"
    
    prompt = f"""Extract ALL hotels opening in 2026 or later from this text.
ONLY include hotels in: USA (especially Florida) OR Caribbean.
SKIP hotels in Europe, Asia, Middle East, Africa.

For EACH hotel, extract:
- hotel_name: Full hotel name
- brand: Hotel brand (Four Seasons, Hilton, etc.) or null
- city: City name or null
- state: US state (e.g. "Florida") or null
- country: "USA" or Caribbean country name
- opening_date: Year like "2026" or "Q2 2027"
- room_count: Number of rooms or null
- contact_name: Any person mentioned (GM, Director, etc.) or null
- contact_title: Their title or null
{hints}

TEXT:
{text[:8000]}

Return ONLY a JSON array. If no hotels found, return []
JSON:"""

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": "llama3.2",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1}
                },
                timeout=120.0
            )
            
            if response.status_code == 200:
                result = response.json().get("response", "")
                json_match = re.search(r'\[[\s\S]*?\]', result)
                
                if json_match:
                    hotels = json.loads(json_match.group(0))
                    
                    valid = []
                    for h in hotels:
                        if not isinstance(h, dict):
                            continue
                        
                        name = h.get("hotel_name") or h.get("name")
                        if not name:
                            continue
                        
                        # Check year
                        year_match = re.search(r'20(\d{2})', str(h.get("opening_date", "")))
                        if year_match and int("20" + year_match.group(1)) < MIN_YEAR:
                            continue
                        
                        h["hotel_name"] = name
                        h["brand"] = h.get("brand") or detect_brand(name)
                        h["source_url"] = source_url
                        h["source_site"] = urlparse(source_url).netloc
                        valid.append(h)
                    
                    return valid
    except Exception as e:
        print_warning(f"AI extraction error: {e}")
    
    return []


# =============================================================================
# API CLIENT
# =============================================================================

async def check_services() -> Tuple[bool, bool]:
    """Check if required services are running"""
    api_ok = False
    ollama_ok = False
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                api = await client.get(f"{API_BASE_URL}/health")
                api_ok = api.status_code == 200
            except:
                pass
            
            try:
                ollama = await client.get(f"{OLLAMA_URL}/api/tags")
                ollama_ok = ollama.status_code == 200
            except:
                pass
    except:
        pass
    
    return api_ok, ollama_ok


async def get_existing_leads() -> Set[str]:
    """Get normalized names of existing leads"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{API_BASE_URL}/leads")
            if r.status_code == 200:
                data = r.json()
                leads = data.get("leads", []) if isinstance(data, dict) else data
                return {re.sub(r'[^a-z0-9]', '', (l.get("hotel_name") or "").lower()) for l in leads}
    except:
        pass
    return set()


async def get_seeded_sources() -> List[Dict]:
    """Get sources from database"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{API_BASE_URL}/sources")
            if r.status_code == 200:
                return r.json()
    except:
        pass
    return []


async def save_lead(hotel: Dict) -> Optional[int]:
    """Save a lead to database"""
    
    score_info = f"Score: {hotel.get('lead_score', 0)}/100 {hotel.get('score_tier', '')}"
    notes = hotel.get('notes', '')
    
    payload = {
        "hotel_name": hotel.get("hotel_name"),
        "brand": hotel.get("brand"),
        "city": hotel.get("city"),
        "state": hotel.get("state"),
        "country": hotel.get("country", "USA"),
        "opening_date": hotel.get("opening_date"),
        "room_count": hotel.get("room_count"),
        "hotel_type": hotel.get("hotel_type"),
        "contact_name": hotel.get("contact_name"),
        "contact_title": hotel.get("contact_title"),
        "contact_email": hotel.get("contact_email"),
        "contact_phone": hotel.get("contact_phone"),
        "source_url": hotel.get("source_url"),
        "source_site": hotel.get("source_site"),
        "lead_score": hotel.get("lead_score", 0),
        "notes": f"{score_info} | {notes}" if notes else score_info
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{API_BASE_URL}/leads", json=payload)
            if r.status_code == 200:
                return r.json().get("id")
    except:
        pass
    return None


# =============================================================================
# SCORING AND FILTERING
# =============================================================================

def score_and_filter_lead(hotel: Dict) -> Dict:
    """Calculate lead score and check skip filters"""
    result = calculate_lead_score(
        hotel_name=hotel.get("hotel_name", ""),
        city=hotel.get("city"),
        state=hotel.get("state"),
        country=hotel.get("country"),
        opening_date=hotel.get("opening_date"),
        room_count=hotel.get("room_count"),
        contact_name=hotel.get("contact_name"),
        contact_email=hotel.get("contact_email"),
        contact_phone=hotel.get("contact_phone"),
        project_type=hotel.get("project_type"),
        description=hotel.get("notes"),
        brand=hotel.get("brand"),
    )
    
    hotel["lead_score"] = result["total_score"]
    hotel["should_save"] = result["should_save"]
    hotel["skip_reason"] = result.get("skip_reason")
    hotel["score_tier"] = result.get("score_tier")
    hotel["score_breakdown"] = result.get("breakdown")
    
    return hotel


# =============================================================================
# DISCOVERY ENGINE
# =============================================================================

async def discover_new_sources(engine: ScrapingEngine) -> List[Dict]:
    """Discover new hotel news sources from the web"""
    print_header("DISCOVERING NEW SOURCES")
    
    discovered_urls = set()
    new_sources = []
    
    for query in DISCOVERY_QUERIES[:3]:
        print(f"\n   🔍 Searching: {query[:50]}...")
        
        # Use DuckDuckGo search
        search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        result = await engine.scrape(search_url, ScrapeMethod.HTTPX)
        
        if not result.success:
            continue
        
        soup = BeautifulSoup(result.html, "lxml")
        
        for link in soup.select(".result__a")[:10]:
            url = link.get("href", "")
            
            if not url or url in discovered_urls:
                continue
            
            # Skip bad domains
            domain = urlparse(url).netloc.lower()
            if any(skip in domain for skip in SKIP_DOMAINS):
                continue
            
            discovered_urls.add(url)
            new_sources.append({
                "url": url,
                "title": link.get_text(strip=True),
                "domain": domain
            })
        
        await asyncio.sleep(1)
    
    print_success(f"Discovered {len(new_sources)} potential sources")
    return new_sources[:15]


# =============================================================================
# MAIN SCRAPER
# =============================================================================

async def main():
    """Main scraping routine"""
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Smart Lead Hunter Scraper")
    parser.add_argument("--source", help="Scrape specific source (orange_studio, etc.)")
    parser.add_argument("--discover", action="store_true", help="Discover new sources")
    parser.add_argument("--js", action="store_true", help="Force Playwright for all sites")
    args = parser.parse_args()
    
    # Header
    print_header("SMART LEAD HUNTER v7 - FULL TECH STACK")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # Check services
    print("\n🔍 Checking services...")
    api_ok, ollama_ok = await check_services()
    
    if api_ok:
        print_success("API running")
    else:
        print_error("API not running! Start with: uvicorn app.main:app --reload")
        return
    
    if ollama_ok:
        print_success("Ollama running")
    else:
        print_warning("Ollama not running - AI extraction disabled")
    
    # Initialize scraping engine
    print("\n🔧 Initializing scraping engines...")
    engine = ScrapingEngine()
    await engine.initialize()
    
    if engine.browser:
        print_success("Playwright ready (for JS-heavy sites)")
    else:
        print_warning("Playwright not available - using httpx only")
    
    if engine.nlp:
        print_success("spaCy NLP ready")
    
    try:
        # Get existing leads
        existing = await get_existing_leads()
        print_info(f"Existing leads in database: {len(existing)}")
        
        # Get seeded sources
        sources = await get_seeded_sources()
        print_info(f"Seeded sources: {len(sources)}")
        
        all_hotels = []
        
        # ======================================================================
        # PHASE 1: ORANGE STUDIO
        # ======================================================================
        print_header("PHASE 1: ORANGE STUDIO")
        
        orange_url = "https://www.theorangestudio.com/hotel-openings"
        hotels = await extract_orange_studio(engine, orange_url)
        print_info(f"Found {len(hotels)} hotels")
        all_hotels.extend(hotels)
        
        # ======================================================================
        # PHASE 2: CHAIN NEWSROOMS (JavaScript sites)
        # ======================================================================
        print_header("PHASE 2: CHAIN NEWSROOMS")
        
        # Priority JS sites
        js_sources = [s for s in sources if any(js in s.get("url", "").lower() for js in JS_REQUIRED_SITES)]
        
        for source in js_sources[:5]:
            url = source.get("url")
            name = source.get("name", url)
            
            print(f"\n   🌐 [{name}]")
            print(f"      URL: {url}")
            
            # Use Playwright for JS sites
            result = await engine.scrape(url, ScrapeMethod.PLAYWRIGHT if engine.browser else ScrapeMethod.HTTPX)
            
            if not result.success:
                print_error(f"Failed: {result.error}")
                continue
            
            print_success(f"Got {len(result.text):,} chars ({result.method.value}, {result.load_time:.1f}s)")
            
            # NLP extraction
            spacy_entities = None
            if engine.nlp:
                spacy_entities = engine.extract_with_spacy(result.text[:50000])
            
            # AI extraction
            if ollama_ok:
                print("      🤖 AI extracting...")
                hotels = await ai_extract_hotels(result.text[:15000], url, spacy_entities)
                print_info(f"Found {len(hotels)} hotels")
                all_hotels.extend(hotels)
            
            await asyncio.sleep(1)
        
        # ======================================================================
        # PHASE 3: STATIC SOURCES
        # ======================================================================
        print_header("PHASE 3: STATIC SOURCES")
        
        static_sources = [s for s in sources if s not in js_sources and "orange" not in s.get("name", "").lower()]
        
        for source in static_sources[:5]:
            url = source.get("url")
            name = source.get("name", url)
            
            print(f"\n   🌐 [{name}]")
            
            result = await engine.scrape(url, ScrapeMethod.HTTPX)
            
            if not result.success:
                print_error(f"Failed: {result.error}")
                continue
            
            print_success(f"Got {len(result.text):,} chars ({result.load_time:.1f}s)")
            
            if ollama_ok:
                print("      🤖 AI extracting...")
                hotels = await ai_extract_hotels(result.text[:12000], url)
                print_info(f"Found {len(hotels)} hotels")
                all_hotels.extend(hotels)
            
            await asyncio.sleep(1)
        
        # ======================================================================
        # PHASE 4: DISCOVER NEW SOURCES (optional)
        # ======================================================================
        if args.discover:
            new_sources = await discover_new_sources(engine)
            
            for source in new_sources[:3]:
                url = source.get("url")
                print(f"\n   🆕 {url[:60]}...")
                
                result = await engine.scrape(url)
                if result.success and ollama_ok:
                    hotels = await ai_extract_hotels(result.text[:10000], url)
                    all_hotels.extend(hotels)
                
                await asyncio.sleep(1)
        
        # ======================================================================
        # PHASE 5: PROCESS & SCORE
        # ======================================================================
        print_header("PHASE 5: SCORING & FILTERING")
        
        print(f"\n   Raw hotels found: {len(all_hotels)}")
        
        # Dedupe
        seen = {}
        for h in all_hotels:
            key = re.sub(r'[^a-z0-9]', '', (h.get("hotel_name") or "").lower())
            if key and key not in seen:
                seen[key] = h
        
        unique = list(seen.values())
        print(f"   After dedupe: {len(unique)}")
        
        # Score and filter
        scored_hotels = []
        skipped_budget = 0
        skipped_international = 0
        skipped_existing = 0
        
        for h in unique:
            key = re.sub(r'[^a-z0-9]', '', (h.get("hotel_name") or "").lower())
            
            if key in existing:
                skipped_existing += 1
                continue
            
            h = score_and_filter_lead(h)
            
            if not h["should_save"]:
                reason = h.get("skip_reason", "")
                if "Budget" in reason or "brand" in reason.lower():
                    skipped_budget += 1
                    print(f"   ❌ SKIP (Budget): {h.get('hotel_name')}")
                elif "International" in reason:
                    skipped_international += 1
                    print(f"   ❌ SKIP (Intl): {h.get('hotel_name')}")
                continue
            
            scored_hotels.append(h)
        
        print(f"\n   📊 Filtering Results:")
        print(f"      Skipped (existing): {skipped_existing}")
        print(f"      Skipped (budget): {skipped_budget}")
        print(f"      Skipped (international): {skipped_international}")
        print_success(f"Leads to save: {len(scored_hotels)}")
        
        if not scored_hotels:
            print_warning("No new qualified leads found")
            return
        
        # Sort by score
        scored_hotels.sort(key=lambda x: x.get("lead_score", 0), reverse=True)
        
        # ======================================================================
        # PHASE 6: SAVE LEADS
        # ======================================================================
        print_header("PHASE 6: SAVING LEADS")
        
        saved = 0
        
        for i, h in enumerate(scored_hotels, 1):
            score = h.get("lead_score", 0)
            tier = h.get("score_tier", "")
            
            # Location flag
            state = (h.get("state") or "").lower()
            country = (h.get("country") or "").lower()
            
            if "florida" in state:
                flag = "🌴FL"
            elif any(c in country for c in CARIBBEAN_COUNTRIES):
                flag = "🏝️CB"
            else:
                flag = "🇺🇸US"
            
            print(f"\n   {i}. {tier}[{score}] {flag} {h.get('hotel_name')}")
            
            breakdown = h.get("score_breakdown", {})
            if breakdown:
                brand_pts = breakdown.get("brand", {}).get("points", 0)
                loc_pts = breakdown.get("location", {}).get("points", 0)
                time_pts = breakdown.get("timing", {}).get("points", 0)
                print(f"      📊 Brand:{brand_pts} Loc:{loc_pts} Time:{time_pts}")
            
            loc = ", ".join(filter(None, [h.get("city"), h.get("state"), h.get("country")]))
            if loc:
                print(f"      📍 {loc}")
            
            if h.get("brand"):
                print(f"      🏷️  {h['brand']}")
            
            # Save
            lead_id = await save_lead(h)
            if lead_id:
                print_success(f"Saved (ID: {lead_id})")
                saved += 1
            else:
                print_error("Failed to save")
        
        # ======================================================================
        # SUMMARY
        # ======================================================================
        fl_count = len([h for h in scored_hotels if "florida" in (h.get("state") or "").lower()])
        cb_count = len([h for h in scored_hotels if any(c in (h.get("country") or "").lower() for c in CARIBBEAN_COUNTRIES)])
        hot_count = len([h for h in scored_hotels if h.get("lead_score", 0) >= 70])
        warm_count = len([h for h in scored_hotels if 50 <= h.get("lead_score", 0) < 70])
        
        print_header("SUMMARY")
        
        if RICH_AVAILABLE:
            table = Table(title="Scraping Results")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Hotels Found", str(len(all_hotels)))
            table.add_row("Skipped (Budget)", str(skipped_budget))
            table.add_row("Skipped (International)", str(skipped_international))
            table.add_row("New Leads", str(len(scored_hotels)))
            table.add_row("Saved", str(saved))
            table.add_row("🔥 Hot (70+)", str(hot_count))
            table.add_row("🌡️ Warm (50-69)", str(warm_count))
            table.add_row("🌴 Florida", str(fl_count))
            table.add_row("🏝️ Caribbean", str(cb_count))
            
            console.print(table)
        else:
            print(f"""
   Hotels found: {len(all_hotels)}
   Skipped (budget): {skipped_budget}
   Skipped (international): {skipped_international}
   New leads: {len(scored_hotels)}
   Saved: {saved}
   
   🔥 Hot (70+): {hot_count}
   🌡️ Warm (50-69): {warm_count}
   🌴 Florida: {fl_count}
   🏝️ Caribbean: {cb_count}
""")
    
    finally:
        await engine.close()
        print("\n✅ Scraping complete!")


if __name__ == "__main__":
    asyncio.run(main())