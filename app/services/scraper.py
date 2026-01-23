"""
Scraper Service - crawls websites for hotel opening news
"""
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import httpx
from typing import Optional
from app.config import settings


async def scrape_with_playwright(url: str) -> Optional[str]:
    """
    Scrape JavaScript-heavy websites using Playwright
    
    Args:
        url: URL to scrape
        
    Returns:
        HTML content or None if failed
    """
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()
            
            # Navigate and wait for content
            await page.goto(url, wait_until="networkidle", timeout=30000)
            
            # Wait a bit for dynamic content
            await asyncio.sleep(settings.scrape_delay)
            
            # Get HTML
            html = await page.content()
            
            await browser.close()
            return html
            
    except Exception as e:
        print(f"Playwright error for {url}: {e}")
        return None


async def scrape_with_httpx(url: str) -> Optional[str]:
    """
    Scrape simple static websites using httpx
    
    Args:
        url: URL to scrape
        
    Returns:
        HTML content or None if failed
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
                timeout=30.0,
                follow_redirects=True
            )
            
            if response.status_code == 200:
                return response.text
            else:
                print(f"HTTP {response.status_code} for {url}")
                return None
                
    except Exception as e:
        print(f"HTTPX error for {url}: {e}")
        return None


def parse_html(html: str) -> dict:
    """
    Parse HTML and extract text content
    
    Args:
        html: Raw HTML string
        
    Returns:
        Dictionary with parsed content
    """
    soup = BeautifulSoup(html, "lxml")
    
    # Remove script and style elements
    for element in soup(["script", "style", "nav", "footer", "header"]):
        element.decompose()
    
    # Get title
    title = soup.title.string if soup.title else ""
    
    # Get main content text
    text = soup.get_text(separator=" ", strip=True)
    
    # Get all links
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        link_text = a.get_text(strip=True)
        if href and not href.startswith("#"):
            links.append({"url": href, "text": link_text})
    
    # Get meta description
    meta_desc = ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if meta_tag:
        meta_desc = meta_tag.get("content", "")
    
    return {
        "title": title,
        "text": text,
        "links": links,
        "meta_description": meta_desc
    }


def find_article_links(parsed_content: dict, base_url: str) -> list:
    """
    Find links that likely lead to hotel opening articles
    
    Args:
        parsed_content: Dictionary from parse_html
        base_url: Base URL for resolving relative links
        
    Returns:
        List of article URLs to follow
    """
    keywords = [
        "hotel", "resort", "opening", "opens", "new", "announce",
        "luxury", "boutique", "grand", "debut", "launch"
    ]
    
    article_links = []
    
    for link in parsed_content.get("links", []):
        url = link["url"]
        text = link["text"].lower()
        
        # Check if link text contains relevant keywords
        if any(kw in text for kw in keywords):
            # Resolve relative URLs
            if url.startswith("/"):
                url = base_url.rstrip("/") + url
            elif not url.startswith("http"):
                url = base_url.rstrip("/") + "/" + url
            
            article_links.append(url)
    
    return list(set(article_links))  # Remove duplicates


async def scrape_url(url: str, use_playwright: bool = False) -> Optional[dict]:
    """
    Main scraping function
    
    Args:
        url: URL to scrape
        use_playwright: Whether to use Playwright (for JS-heavy sites)
        
    Returns:
        Dictionary with scraped and parsed content
    """
    # Scrape the page
    if use_playwright:
        html = await scrape_with_playwright(url)
    else:
        html = await scrape_with_httpx(url)
    
    if not html:
        return None
    
    # Parse the HTML
    parsed = parse_html(html)
    parsed["url"] = url
    
    return parsed


async def deep_scrape(url: str, max_depth: int = 2, use_playwright: bool = False) -> list:
    """
    Recursively scrape a site following article links
    
    Args:
        url: Starting URL
        max_depth: How many levels deep to follow links
        use_playwright: Whether to use Playwright
        
    Returns:
        List of all scraped content
    """
    visited = set()
    results = []
    
    async def crawl(current_url: str, depth: int):
        if depth > max_depth or current_url in visited:
            return
        
        visited.add(current_url)
        
        # Respect rate limiting
        await asyncio.sleep(settings.scrape_delay)
        
        # Scrape the page
        content = await scrape_url(current_url, use_playwright)
        
        if content:
            results.append(content)
            
            # Find and follow article links
            if depth < max_depth:
                base_url = "/".join(current_url.split("/")[:3])
                article_links = find_article_links(content, base_url)
                
                for link in article_links[:5]:  # Limit to 5 links per page
                    await crawl(link, depth + 1)
    
    await crawl(url, 0)
    return results