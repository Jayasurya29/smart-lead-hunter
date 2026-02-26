"""
SMART LEAD HUNTER — Contact Enrichment Service v4.1
=====================================================
Multi-layer contact discovery with SAP-trained intelligence.

Layer 0: Google Search via Serper.dev (finds LinkedIn posts, press releases)
Layer 1: Web Scrape + Gemini AI Extract (scrape articles from search results)
Layer 2: LinkedIn Snippet Extraction (names from search snippets)
Layer 3: Apollo Fallback (chain-level contacts, uses credits)
Fallback: DuckDuckGo (free, unlimited) when Serper unavailable

KEY v4.1 CHANGES:
- Google search via Serper.dev (finds Kara DePool that DDG misses)
- DDG as free fallback when SERPER_API_KEY not set
- SAP-trained title classifier (780 titles → 7 buyer tiers)
- Contact validator with name-collision detection (Nora Hotel fix)
- Smart query builder with parent company fallback
- Auto-retry when all contacts are false positives
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, date
from typing import Optional

import httpx
from dotenv import load_dotenv

from app.config.enrichment_config import (
    BRAND_TO_PARENT,
    CONTACT_SEARCH_PRIORITIES,
    ENRICHMENT_SETTINGS,
    HOSPITALITY_NEWS_DOMAINS,
)
from app.config.sap_title_classifier import title_classifier, BuyerTier
from app.services.contact_validator import contact_validator, query_builder

load_dotenv()

logger = logging.getLogger(__name__)

MAX_CONTACTS_TO_SAVE = 5


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT RESULT
# ═══════════════════════════════════════════════════════════════


class EnrichmentResult:
    """Holds all data found during enrichment."""

    def __init__(self):
        self.contacts: list[dict] = []
        self.management_company: Optional[str] = None
        self.developer: Optional[str] = None
        self.opening_update: Optional[str] = None
        self.additional_details: Optional[str] = None
        self.sources_used: list[str] = []
        self.layers_tried: list[str] = []
        self.errors: list[str] = []

    @property
    def best_contact(self) -> Optional[dict]:
        """Return highest-priority contact: hotel_specific first, then by confidence."""
        if not self.contacts:
            return None

        scope_rank = {
            "hotel_specific": 0,
            "chain_area": 1,
            "chain_corporate": 2,
            "unknown": 3,
        }
        confidence_rank = {"high": 0, "medium": 1, "low": 2}

        def sort_key(c):
            return (
                scope_rank.get(c.get("scope", "unknown"), 3),
                confidence_rank.get(c.get("confidence", "low"), 2),
            )

        sorted_contacts = sorted(self.contacts, key=sort_key)
        return sorted_contacts[0]

    def to_dict(self) -> dict:
        return {
            "contacts": self.contacts,
            "management_company": self.management_company,
            "developer": self.developer,
            "opening_update": self.opening_update,
            "additional_details": self.additional_details,
            "sources_used": self.sources_used,
            "layers_tried": self.layers_tried,
            "errors": self.errors,
        }


# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════


def _get_search_mode(opening_date: Optional[str]) -> str:
    """Determine if hotel is 'pre_opening' or 'opening_soon' based on opening date."""
    if not opening_date:
        return "pre_opening"

    today = date.today()
    odate = opening_date.lower().strip()

    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
        "q1": 2,
        "q2": 5,
        "q3": 8,
        "q4": 11,
        "early": 3,
        "mid": 6,
        "late": 10,
        "spring": 4,
        "summer": 7,
        "fall": 10,
        "winter": 1,
    }

    year_match = re.search(r"20(\d{2})", odate)
    if not year_match:
        return "pre_opening"

    year = 2000 + int(year_match.group(1))
    month = 6

    for keyword, m in month_map.items():
        if keyword in odate:
            month = m
            break

    try:
        opening = date(year, month, 15)
        months_until = (opening.year - today.year) * 12 + (opening.month - today.month)
        return "opening_soon" if months_until <= 6 else "pre_opening"
    except ValueError:
        return "pre_opening"


def _get_priority_titles(mode: str) -> list[str]:
    """Get flat list of titles in priority order for the given mode."""
    priorities = CONTACT_SEARCH_PRIORITIES.get(
        mode, CONTACT_SEARCH_PRIORITIES["pre_opening"]
    )
    titles = []
    for group in priorities:
        titles.extend(group["titles"])
    return titles


def _resolve_parent_brand(
    brand: Optional[str], hotel_name: Optional[str], mgmt_company: Optional[str]
) -> tuple[str, str]:
    """Resolve brand for Apollo search. Returns (specific_brand, parent_company)."""
    specific = brand or ""
    parent = ""

    if brand:
        key = brand.lower().strip()
        if key in BRAND_TO_PARENT:
            parent = BRAND_TO_PARENT[key]
            specific = brand
        else:
            for k, v in BRAND_TO_PARENT.items():
                if k in key or key in k:
                    parent = v
                    specific = brand
                    break

    if not specific and hotel_name:
        name_lower = hotel_name.lower()
        for k, v in BRAND_TO_PARENT.items():
            if k in name_lower:
                parent = v
                specific = k.title()
                break

    if not specific and mgmt_company:
        specific = mgmt_company
        parent = mgmt_company

    return (specific or parent, parent or specific)


def _build_location_string(
    city: Optional[str], state: Optional[str], country: Optional[str]
) -> str:
    """Build location string for Apollo search."""
    parts = []
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    if country and country.upper() not in ("USA", "US", "UNITED STATES"):
        parts.append(country)
    elif not state:
        parts.append("United States")
    return ", ".join(parts) if parts else "United States"


def _build_region_string(state: Optional[str], country: Optional[str]) -> str:
    """Build broader region string for Apollo fallback."""
    if state:
        return f"{state}, United States"
    if country:
        return country
    return "United States"


def _clean_title(raw_title: str) -> str:
    """Clean up a messy title extracted from LinkedIn snippets."""
    if not raw_title:
        return ""

    raw_title = re.sub(r"\s*\|?\s*LinkedIn.*$", "", raw_title, flags=re.IGNORECASE)
    raw_title = re.sub(r"\s*#\w+.*$", "", raw_title)
    raw_title = re.sub(
        r"\s*\|\s*\d+\s*comments?.*$", "", raw_title, flags=re.IGNORECASE
    )
    raw_title = re.sub(r"\s*-\s*LinkedIn.*$", "", raw_title, flags=re.IGNORECASE)

    if len(raw_title) > 60:
        for sep in [" | ", " - ", " at ", " ... "]:
            idx = raw_title.find(sep)
            if 5 < idx < 60:
                raw_title = raw_title[:idx]
                break

    role_words = [
        "director",
        "manager",
        "chef",
        "coordinator",
        "supervisor",
        "housekeeper",
        "housekeeping",
        "purchasing",
        "procurement",
        "operations",
        "sales",
        "f&b",
        "food",
        "beverage",
        "spa",
        "general manager",
        "assistant",
        "executive",
        "buyer",
        "uniform",
        "wardrobe",
        "laundry",
        "steward",
        "rooms",
        "front office",
        "resort",
        "property",
    ]
    title_lower = raw_title.lower()
    has_role = any(w in title_lower for w in role_words)
    if not has_role and len(raw_title) > 20:
        return ""

    if len(raw_title) > 80:
        raw_title = raw_title[:80].rsplit(" ", 1)[0]

    return raw_title.strip()


def _is_hotel_relevant_title(title: str) -> bool:
    """Check if a title is relevant to hotel uniform sales using SAP classifier."""
    if not title:
        return False

    classification = title_classifier.classify(title)
    # Anything Tier 1-5 is relevant; Tier 6 (Finance) and Tier 7 (Irrelevant) are not
    return classification.tier.value <= BuyerTier.TIER5_HR.value


def _is_irrelevant_org(org: str) -> bool:
    """Filter out contacts from non-hotel organizations."""
    if not org:
        return False

    org_lower = org.lower()
    irrelevant = [
        "cbre",
        "jll",
        "cushman",
        "colliers",
        "real estate",
        "law firm",
        "attorney",
        "legal",
        "government",
        "chamber of commerce",
        "office of the prime",
        "architecture",
        "architect",
        "construction",
        "investment",
        "capital",
        "equity",
        "fund",
        "consulting",
        "advisory",
        "advisor",
        "news",
        "media",
        "journal",
        "magazine",
    ]

    for term in irrelevant:
        if term in org_lower:
            return True

    return False


def _is_corporate_title(title: str) -> bool:
    """Filter out corporate/executive/investor titles — not property-level contacts."""
    if not title:
        return False

    classification = title_classifier.classify(title)
    # The SAP classifier already handles corporate detection
    # Check if it got tagged as corporate (score=5, priority=7)
    if classification.search_priority >= 7:
        return True

    title_lower = title.lower()
    corporate_keywords = [
        "president",
        "ceo",
        "cfo",
        "coo",
        "cto",
        "chief executive",
        "chief operating",
        "chief financial",
        "vice president",
        "svp",
        "evp",
        "senior vice",
        "executive vice",
        "area vice president",
        "global head",
        "head of region",
    ]
    # Investor/developer/owner — they don't buy uniforms
    investor_keywords = [
        "co-founder",
        "cofounder",
        "founder",
        "partner",
        "managing partner",
        "owner",
        "co-owner",
        "investor",
        "principal",
        "developer",
        "real estate",
    ]
    for kw in corporate_keywords + investor_keywords:
        if kw in title_lower:
            return True
    return False


# ═══════════════════════════════════════════════════════════════
# GEMINI AI EXTRACTION PROMPT v3 — Stricter hotel verification
# ═══════════════════════════════════════════════════════════════

CONTACT_EXTRACTION_PROMPT_V3 = """You are extracting hotel staff contact information from a news article.

TARGET HOTEL: {hotel_name}
LOCATION: {location}

CRITICAL RULES:
1. ONLY extract people who work at hotel/hospitality operations roles (GM, Directors, Managers)
2. DO NOT extract: journalists, government officials, real estate brokers, architects, investors, developers, lawyers
3. If this article is about a DIFFERENT hotel than "{hotel_name}", mark those contacts scope as "wrong_hotel"
4. Maximum 5 contacts — only the most relevant ones

For each contact, determine SCOPE:
- "hotel_specific" = CONFIRMED to work at {hotel_name} specifically (article names them WITH this hotel)
- "chain_area" = Works for parent brand in same area, NOT confirmed at this specific property
- "chain_corporate" = Corporate/HQ level role at parent company
- "wrong_hotel" = Works at a DIFFERENT hotel mentioned in the same article
- "irrelevant" = Not a hotel operations person (broker, journalist, politician, developer)

CONFIDENCE:
- "high" = Article explicitly says "[Name] is the [Title] of/at {hotel_name}"
- "medium" = Strong indication (LinkedIn title matches hotel name)
- "low" = Weak connection, might be different property or role

Return JSON with max 5 contacts:
- name: Full name
- title: Job title only (short, no company name)
- email: Email or null
- phone: Phone or null
- linkedin: LinkedIn URL or null
- organization: Hotel or company name
- scope: hotel_specific | chain_area | chain_corporate | wrong_hotel | irrelevant
- confidence: high | medium | low
- confidence_note: One sentence why

Also extract:
- management_company: or null
- developer: or null
- opening_update: or null
- additional_details: or null

Return ONLY valid JSON:
{{
    "contacts": [],
    "management_company": null,
    "developer": null,
    "opening_update": null,
    "additional_details": null
}}

Article text:
{article_text}
"""


# ═══════════════════════════════════════════════════════════════
# LAYER 1: WEB SEARCH + SCRAPE + AI EXTRACT
# ═══════════════════════════════════════════════════════════════


async def _search_serper(query: str, max_results: int = 5) -> list[dict]:
    """Search Google via Serper.dev API. Returns same format as DDG for compatibility."""
    api_key = os.getenv("SERPER_API_KEY")
    if not api_key:
        logger.debug("SERPER_API_KEY not set, skipping Google search")
        return []

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                json={"q": query, "num": max_results},
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
            )
            if resp.status_code != 200:
                logger.warning(f"Serper API error: {resp.status_code}")
                return []

            data = resp.json()
            results = []

            # Organic results
            for r in data.get("organic", [])[:max_results]:
                results.append(
                    {
                        "title": r.get("title", ""),
                        "url": r.get("link", ""),
                        "snippet": r.get("snippet", ""),
                    }
                )

            # Knowledge graph — often contains the GM name directly
            kg = data.get("knowledgeGraph", {})
            if kg and kg.get("description"):
                logger.info(
                    f"Serper Knowledge Graph: {kg.get('title', '')} — {kg.get('description', '')[:100]}"
                )

            return results
    except Exception as e:
        logger.warning(f"Serper search failed: {e}")
        return []


async def _search_duckduckgo(query: str, max_results: int = 3) -> list[dict]:
    """Search DuckDuckGo using the ddgs package. Free fallback when Serper unavailable."""
    try:
        from ddgs import DDGS

        results = DDGS().text(query, max_results=max_results)
        return [
            {
                "title": r.get("title", ""),
                "url": r.get("href", ""),
                "snippet": r.get("body", ""),
            }
            for r in results
        ]
    except Exception as e:
        logger.warning(f"DuckDuckGo search failed: {e}")
        return []


async def _search_web(query: str, max_results: int = 5) -> list[dict]:
    """Unified search: runs BOTH Serper (Google) and DDG, merges + deduplicates."""
    all_results = []
    seen_urls = set()

    # ── Serper (Google) first — better LinkedIn coverage ──
    serper_results = await _search_serper(query, max_results=max_results)
    if serper_results:
        logger.info(f"Google (Serper): {len(serper_results)} results for: {query[:60]}")
        for r in serper_results:
            url_key = r["url"].rstrip("/").lower()
            if url_key not in seen_urls:
                seen_urls.add(url_key)
                all_results.append(r)

    # ── DDG second — catches different results (Rocky, Dale, etc.) ──
    ddg_results = await _search_duckduckgo(query, max_results=max_results)
    if ddg_results:
        new_count = 0
        for r in ddg_results:
            url_key = r["url"].rstrip("/").lower()
            if url_key not in seen_urls:
                seen_urls.add(url_key)
                all_results.append(r)
                new_count += 1
        if new_count:
            logger.info(f"DDG added {new_count} unique results for: {query[:60]}")

    if not all_results:
        logger.info(f"No search results for: {query[:60]}")

    return all_results


async def _scrape_url(url: str) -> Optional[str]:
    """Scrape article text - tries httpx first, falls back to Crawl4AI for blocked sites."""
    timeout = ENRICHMENT_SETTINGS["crawl_timeout_seconds"]
    skip_domains = [
        "linkedin.com",
        "indeed.com",
        "ziprecruiter.com",
        "careers.",
        "jobs.",
        "wikipedia.org",
    ]
    url_lower = url.lower()
    for skip in skip_domains:
        if skip in url_lower:
            logger.info(f"Skipping: {url} (non-article site)")
            return None

    text = ""
    httpx_failed = False

    # Try httpx first (fast)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                },
            )
            if resp.status_code == 200:
                text = re.sub(
                    r"<script[^>]*>.*?</script>", "", resp.text, flags=re.DOTALL
                )
                text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 200:
                    return text[: ENRICHMENT_SETTINGS["max_article_chars"]]
            # 403 or empty = fall through to Crawl4AI
            if resp.status_code == 403:
                logger.info(f"httpx blocked (403), trying Crawl4AI: {url}")
                httpx_failed = True
            elif resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
    except Exception as e:
        logger.info(f"httpx failed ({e}), trying Crawl4AI: {url}")
        httpx_failed = True

    if not httpx_failed:
        return None

    # Fallback: Crawl4AI with browser rendering
    try:
        import os

        os.environ["PYTHONIOENCODING"] = "utf-8"
        from crawl4ai import AsyncWebCrawler

        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(url=url)
            if result and result.markdown:
                crawled = result.markdown.strip()
                if len(crawled) > 200:
                    # Strip markdown formatting for cleaner Gemini extraction
                    crawled = re.sub(r"!\[.*?\]\(.*?\)", "", crawled)
                    crawled = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", crawled)
                    crawled = re.sub(r"#{1,6}\s*", "", crawled)
                    crawled = re.sub(r"\*{1,3}([^*]+)\*{1,3}", r"\1", crawled)
                    crawled = re.sub(r"\n{3,}", "\n\n", crawled)
                    crawled = crawled.strip()
                    crawled = crawled.strip()
                    logger.info(f"Crawl4AI succeeded: {url} ({len(crawled)} chars)")
                    return crawled[: ENRICHMENT_SETTINGS["max_article_chars"]]
            logger.warning(f"Crawl4AI returned no content for {url}")
            return None
    except Exception as e:
        logger.warning(f"Crawl4AI failed for {url}: {e}")
        return None


async def _extract_contacts_with_gemini(
    article_text: str, hotel_name: str, location: str
) -> Optional[dict]:
    """Use Gemini to extract contacts with scope tagging."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set")
        return None

    model = ENRICHMENT_SETTINGS["gemini_model"]
    prompt = CONTACT_EXTRACTION_PROMPT_V3.format(
        hotel_name=hotel_name,
        location=location,
        article_text=article_text[: ENRICHMENT_SETTINGS["max_article_chars"]],
    )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.1},
                },
            )

            if resp.status_code != 200:
                logger.error(f"Gemini API error: {resp.status_code}")
                return None

            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text.strip())
    except Exception as e:
        logger.error(f"Gemini extraction failed: {e}")
        return None


async def _layer_web_search(
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    opening_date: Optional[str],
    result: EnrichmentResult,
    retry_attempt: int = 0,
) -> bool:
    """Layer 1: Search web using smart queries, scrape articles, extract contacts."""
    result.layers_tried.append(f"web_search_attempt_{retry_attempt}")

    location = _build_location_string(city, state, country)

    # ── SMART QUERY BUILDER — replaces hardcoded queries ──
    queries = query_builder.build_queries(
        hotel_name=hotel_name,
        brand=brand,
        management_company=management_company or result.management_company,
        city=city,
        state=state,
        country=country,
        mode=_get_search_mode(opening_date),
        retry_attempt=retry_attempt,
    )

    all_urls = []
    for query in queries:
        logger.info(f"Web search (attempt {retry_attempt}): {query}")
        search_results = await _search_web(query, max_results=5)
        for sr in search_results:
            if sr["url"] not in [u["url"] for u in all_urls]:
                all_urls.append(sr)
        has_serper = bool(os.getenv("SERPER_API_KEY"))
        delay = (
            ENRICHMENT_SETTINGS["serper_delay_seconds"]
            if has_serper
            else ENRICHMENT_SETTINGS["ddg_delay_seconds"]
        )
        await asyncio.sleep(delay)

    if not all_urls:
        logger.info(f"No web results for {hotel_name} (attempt {retry_attempt})")
        return False

    # Prioritize hospitality news sources
    def _source_priority(item):
        url_lower = item["url"].lower()
        for i, domain in enumerate(HOSPITALITY_NEWS_DOMAINS):
            if domain in url_lower:
                return i
        return 100

    all_urls.sort(key=_source_priority)

    found_contacts = False
    for item in all_urls[: ENRICHMENT_SETTINGS["max_articles_to_scrape"]]:
        url = item["url"]
        logger.info(f"Scraping: {url}")

        article_text = await _scrape_url(url)
        if not article_text or len(article_text) < 100:
            continue

        extracted = await _extract_contacts_with_gemini(
            article_text, hotel_name, location
        )
        if not extracted:
            continue

        result.sources_used.append(url)

        for contact in extracted.get("contacts", []):
            name = contact.get("name", "")
            scope = contact.get("scope", "unknown")

            if scope in ("wrong_hotel", "irrelevant"):
                logger.info(f"Filtered out: {name} [{scope}]")
                continue

            if not name or len(name) < 3:
                continue

            if _is_corporate_title(contact.get("title", "")):
                logger.info(
                    f"Filtered out: {name} (corporate title: {contact.get('title')})"
                )
                continue

            if _is_irrelevant_org(contact.get("organization", "")):
                logger.info(
                    f"Filtered out: {name} (irrelevant org: {contact.get('organization')})"
                )
                continue

            contact["source"] = url
            contact["source_type"] = "press_release"
            contact["title"] = _clean_title(contact.get("title", ""))

            if "scope" not in contact:
                contact["scope"] = "unknown"
            if "confidence" not in contact:
                contact["confidence"] = "medium"
            if "confidence_note" not in contact:
                contact["confidence_note"] = "Extracted from web article"

            result.contacts.append(contact)
            found_contacts = True

        if extracted.get("management_company") and not result.management_company:
            result.management_company = extracted["management_company"]
        if extracted.get("developer") and not result.developer:
            result.developer = extracted["developer"]
        if extracted.get("opening_update"):
            result.opening_update = extracted["opening_update"]
        if extracted.get("additional_details"):
            result.additional_details = extracted["additional_details"]

    return found_contacts


# ═══════════════════════════════════════════════════════════════
# LAYER 2: LINKEDIN SNIPPET EXTRACTION
# ═══════════════════════════════════════════════════════════════


async def _layer_linkedin_snippets(
    hotel_name: str,
    brand: Optional[str],
    management_company: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    result: EnrichmentResult,
) -> bool:
    """Layer 2: Extract contact names/titles from LinkedIn search snippets."""
    result.layers_tried.append("linkedin_snippets")

    # ── SMART QUERIES for LinkedIn ──
    location_str = ", ".join(
        filter(
            None,
            [
                city,
                state,
                country if country and country.upper() not in ("US", "USA") else None,
            ],
        )
    )
    queries = [
        f"{hotel_name} General Manager OR Director site:linkedin.com",
        f"{hotel_name} Purchasing OR Housekeeping OR Operations site:linkedin.com",
    ]
    # Targeted title-specific queries (finds Dale Dcruz, Jessica Farley, etc.)
    targeted_titles = [
        "Director of Food and Beverage",
        "Assistant Director of Food and Beverage",
        "Restaurants General Manager",
        "Director of Housekeeping",
        "Executive Housekeeper",
        "Director of Rooms",
        "Purchasing Manager",
        "Front Office Manager",
    ]
    for tt in targeted_titles:
        queries.append(f"{hotel_name} {location_str} {tt}")
    # Add parent company query to catch contacts like Kara DePool
    parent = management_company or brand
    if parent:
        queries.append(f"{parent} {hotel_name} site:linkedin.com")

    found = False
    for query in queries:
        logger.info(f"LinkedIn snippet search: {query}")
        search_results = await _search_web(query, max_results=5)
        has_serper = bool(os.getenv("SERPER_API_KEY"))
        delay = (
            ENRICHMENT_SETTINGS["serper_delay_seconds"]
            if has_serper
            else ENRICHMENT_SETTINGS["ddg_delay_seconds"]
        )
        await asyncio.sleep(delay)

        for sr in search_results:
            url = sr.get("url", "")
            title = sr.get("title", "")
            snippet = sr.get("snippet", "")

            is_profile = "linkedin.com/in/" in url
            is_post = "linkedin.com/posts/" in url

            if not is_profile and not is_post:
                continue

            name = None
            extracted_title = None
            org = ""
            linkedin_url = url  # For posts, we'll try to build profile URL

            if is_profile:
                # ── PROFILE URL: "Kara dePool - General Manager at The Nora Hotel | LinkedIn" ──
                # Also handles: "Steven Andre - Grand Hyatt Grand Cayman Resort & Spa - LinkedIn"
                m = re.match(
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})\s*[-\u2013\u2014]\s*(.+)",
                    title,
                )
                if m:
                    name = m.group(1).strip()
                    raw_rest = m.group(2).strip()

                    for sep in [" at ", " | ", " - "]:
                        if sep in raw_rest:
                            parts = raw_rest.split(sep, 1)
                            extracted_title = parts[0].strip()
                            remainder = parts[1].strip() if len(parts) > 1 else ""
                            org = re.sub(
                                r"\s*\|?\s*LinkedIn.*$",
                                "",
                                remainder,
                                flags=re.IGNORECASE,
                            ).strip()
                            break
                    else:
                        extracted_title = raw_rest

                    # ── FIX: Detect when "title" is actually an org/hotel name ──
                    # e.g. "Grand Hyatt Grand Cayman Resort & Spa" is NOT a job title
                    if extracted_title:
                        title_lower = extracted_title.lower()
                        org_indicators = [
                            "hotel",
                            "resort",
                            "hyatt",
                            "hilton",
                            "marriott",
                            "ihg",
                            "accor",
                            "four seasons",
                            "fairmont",
                            "westin",
                            "sheraton",
                            "waldorf",
                            "conrad",
                            "intercontinental",
                            "kimpton",
                            "rosewood",
                            "mandarin",
                            "peninsula",
                            "hospitality",
                            "group",
                            "collection",
                            "nora",
                        ]
                        is_org_not_title = any(
                            kw in title_lower for kw in org_indicators
                        )

                        # Also check: real titles have role words
                        role_words = [
                            "director",
                            "manager",
                            "chef",
                            "coordinator",
                            "supervisor",
                            "housekeeper",
                            "purchasing",
                            "operations",
                            "general manager",
                            "assistant",
                            "executive",
                            "buyer",
                            "vp",
                            "head of",
                            "ceo",
                            "coo",
                            "cfo",
                            "president",
                            "chairman",
                            "investor",
                            "founder",
                            "partner",
                            "board member",
                        ]
                        has_role_word = any(rw in title_lower for rw in role_words)

                        if is_org_not_title and not has_role_word:
                            # It's an org name, not a title — swap
                            org = extracted_title
                            extracted_title = ""

                            # Try to find actual title from snippet
                            snippet_lower = snippet.lower()
                            for rw in [
                                "chief executive officer",
                                "chief operating officer",
                                "chief financial officer",
                                "investor",
                                "board member",
                                "chairman",
                                "president",
                                "senior vice president",
                                "executive vice president",
                                "vice president",
                                "regional director",
                                "area director",
                                "svp",
                                "evp",
                                "ceo",
                                "coo",
                                "cfo",
                                "director of operations",
                                "director of food and beverage",
                                "director of food & beverage",
                                "director of housekeeping",
                                "director of procurement",
                                "director of purchasing",
                                "director of rooms",
                                "director of front office",
                                "director of banquets",
                                "director of catering",
                                "director of f&b",
                                "director of sales",
                                "assistant general manager",
                                "rooms division manager",
                                "general manager",
                                "resort manager",
                                "operations manager",
                                "executive housekeeper",
                                "purchasing manager",
                                "housekeeping manager",
                                "f&b director",
                                "front office manager",
                                "hotel manager",
                                "property manager",
                                "restaurant general manager",
                                "restaurants general manager",
                            ]:
                                if rw in snippet_lower:
                                    extracted_title = rw.title()
                                    break
                            if extracted_title:
                                logger.info(
                                    f"Title recovered from snippet: {name} -> {extracted_title}"
                                )

            elif is_post:
                # ── POST URL: "Kara dePool's Post - LinkedIn" ──
                # Name from title: "Kara dePool's Post"
                m = re.match(
                    r"^([A-Z][a-zA-Z]+(?:\s+[a-zA-Z][a-zA-Z]+){1,3})(?:'s)?\s+Post",
                    title,
                )
                if m:
                    name = m.group(1).strip()

                # Title from snippet: look for role keywords
                snippet_lower = snippet.lower()
                if is_post:
                    # For POSTS: titles in text refer to OTHER people, not the poster
                    # e.g. "recently-named General Manager Brett Orlando" -> Brett = GM
                    mention_patterns = [
                        r"(?:recently[- ]?named|appointed|named|hired|announcing)\s+(?:our\s+)?(?:new\s+)?((?:general manager|director of \w+|executive housekeeper|resort manager|hotel manager|purchasing manager|operations manager|front office manager))\s+([A-Z][a-z]+\s+[A-Z][a-zA-Z]+)",
                        r"(?:our\s+)?(?:new\s+)?(general manager|director of \w+|executive housekeeper|resort manager|hotel manager|purchasing manager|operations manager)\s+([A-Z][a-z]+\s+[A-Z][a-zA-Z]+)",
                    ]
                    for mp in mention_patterns:
                        mm = re.search(mp, sr.get("snippet", ""), re.IGNORECASE)
                        if mm:
                            mentioned_title = mm.group(1).strip().title()
                            mentioned_name = mm.group(2).strip()
                            # Validate: real name must be 2+ capitalized words (not "for the", "at our", etc.)
                            name_words = mentioned_name.split()
                            is_real_name = (
                                len(mentioned_name) > 4
                                and len(name_words) >= 2
                                and all(w[0].isupper() for w in name_words)
                                and not any(
                                    w.lower()
                                    in (
                                        "the",
                                        "our",
                                        "for",
                                        "at",
                                        "as",
                                        "and",
                                        "or",
                                        "in",
                                        "of",
                                        "a",
                                        "an",
                                    )
                                    for w in name_words
                                )
                            )
                            if is_real_name:
                                existing = [
                                    c.get("name", "").lower() for c in result.contacts
                                ]
                                if mentioned_name.lower() not in existing:
                                    mentioned_contact = {
                                        "name": mentioned_name,
                                        "title": mentioned_title,
                                        "email": None,
                                        "phone": None,
                                        "linkedin": None,
                                        "organization": hotel_name,
                                        "scope": "hotel_specific",
                                        "confidence": "medium",
                                        "confidence_note": f"Mentioned in LinkedIn post by {name}",
                                        "source": url,
                                        "source_type": "linkedin_post_mention",
                                        "_raw_snippet": sr.get("snippet", ""),
                                        "_raw_title": title,
                                    }
                                    result.contacts.append(mentioned_contact)
                                    logger.info(
                                        f"Post mention extracted: {mentioned_name} - {mentioned_title}"
                                    )
                    # Poster gets NO title from post body
                    extracted_title = None
                else:
                    role_patterns = [
                        r"role of\s+([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                        r"(?:appointed|named|hired|joined)\s+(?:as\s+)?(?:the\s+)?([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                        r"(?:i am|i\'m|i\'ve)\s+(?:the\s+)?(?:new\s+)?([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                        r"stepped into the role of\s+([\w\s]+?)(?:\s+(?:for|at|of)\s+)",
                    ]
                    for pattern in role_patterns:
                        role_match = re.search(pattern, snippet_lower)
                        if role_match:
                            extracted_title = role_match.group(1).strip().title()
                            break

                # If no role found in snippet, try to extract from post URL slug
                # e.g. linkedin.com/posts/kara-depool-a69a85151_...
                if not extracted_title:
                    # Check snippet for common title keywords
                    for kw in [
                        "chief executive officer",
                        "investor",
                        "board member",
                        "chairman",
                        "president",
                        "senior vice president",
                        "vice president",
                        "ceo",
                        "coo",
                        "svp",
                        "general manager",
                        "director of",
                        "executive housekeeper",
                        "purchasing manager",
                        "operations manager",
                        "resort manager",
                    ]:
                        if kw in snippet_lower:
                            extracted_title = kw.title()
                            break

                # Try to build profile URL from post URL slug
                post_slug = re.search(r"linkedin\.com/posts/([a-z0-9-]+?)_", url)
                if post_slug:
                    linkedin_url = f"https://www.linkedin.com/in/{post_slug.group(1)}"

                logger.info(
                    f"LinkedIn post parsed: {name} - {extracted_title} (from post snippet)"
                )

            if not name or len(name) < 4:
                continue

            # ── Filter out non-person names (e.g. "UPDATE GROUP", "LinkedIn News") ──
            name_words = name.split()
            if len(name_words) < 2:
                continue  # Need at least first + last name
            if name.isupper():
                continue  # All caps = not a person name
            if any(
                w.lower()
                in {
                    "group",
                    "hotel",
                    "hotels",
                    "news",
                    "update",
                    "the",
                    "resort",
                    "company",
                    "district",
                    "post",
                    "linkedin",
                }
                for w in name_words
            ):
                continue  # Contains non-person words

            extracted_title = _clean_title(extracted_title or "")

            # Determine scope based on hotel name match
            hotel_lower = hotel_name.lower()
            combined_text = f"{title} {sr.get('snippet', '')}".lower()

            hotel_words = [w for w in hotel_lower.split() if len(w) > 3]
            matches = sum(1 for w in hotel_words if w in combined_text)
            match_ratio = matches / len(hotel_words) if hotel_words else 0

            # ── Check for name collision BEFORE assigning hotel_specific ──
            name_lower = name.lower()
            name_parts = set(name_lower.split())
            hotel_word_set = set(w.lower() for w in hotel_words)
            has_name_collision = bool(name_parts & hotel_word_set)

            # ── Also check: is the person's NAME the reason for the match? ──
            # e.g. "Nora Cunningham" matches "Nora Hotel" because of "nora" in her name
            # Remove name words from the match count to get true hotel relevance
            true_matches = 0
            for hw in hotel_words:
                if hw in combined_text:
                    # Check if match is ONLY because of the person's name
                    text_without_name = combined_text.replace(name_lower, "")
                    if hw in text_without_name:
                        true_matches += 1
            true_match_ratio = true_matches / len(hotel_words) if hotel_words else 0

            if true_match_ratio >= 0.6 and not has_name_collision:
                scope = "hotel_specific"
                confidence = "medium"
                confidence_note = f"LinkedIn profile mentions {hotel_name}"
                if not org:
                    org = hotel_name
            elif match_ratio >= 0.6:
                # Match exists but might be from name collision — lower confidence
                scope = "hotel_specific" if not has_name_collision else "unknown"
                confidence = "medium" if not has_name_collision else "low"
                confidence_note = f"LinkedIn profile mentions {hotel_name}"
            else:
                scope = "chain_area"
                confidence = "low"
                confidence_note = (
                    "Found in LinkedIn search but hotel name not confirmed"
                )

            existing_names = [c.get("name", "").lower() for c in result.contacts]
            if name.lower() in existing_names:
                # If existing contact has no title but this one does, UPDATE it
                if extracted_title:
                    for existing_c in result.contacts:
                        if existing_c.get("name", "").lower() == name.lower():
                            if not existing_c.get("title"):
                                existing_c["title"] = extracted_title
                                existing_c["_raw_snippet"] = sr.get("snippet", "")
                                existing_c["_raw_title"] = title
                                if scope == "hotel_specific":
                                    existing_c["scope"] = scope
                                logger.info(
                                    f"Title updated for {name}: {extracted_title}"
                                )
                            break
                continue

            contact = {
                "name": name,
                "title": extracted_title or "",
                "email": None,
                "phone": None,
                "linkedin": linkedin_url,
                "organization": org,
                "scope": scope,
                "confidence": confidence,
                "confidence_note": confidence_note,
                "source": url,
                "source_type": "linkedin_snippet",
                "_raw_snippet": sr.get("snippet", ""),
                "_raw_title": title,
            }

            result.contacts.append(contact)
            result.sources_used.append(f"LinkedIn: {name}")
            found = True
            logger.info(f"LinkedIn: {name} - {extracted_title} [{scope}]")

    return found


# ═══════════════════════════════════════════════════════════════
# LAYER 3: APOLLO SEARCH + REVEAL
# ═══════════════════════════════════════════════════════════════


async def _apollo_search(
    org_name: str, location: str, titles: list[str], max_results: int = 5
) -> list[dict]:
    """Search Apollo for people by org + location + titles."""
    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        logger.warning("APOLLO_API_KEY not set, skipping Apollo")
        return []

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.apollo.io/api/v1/mixed_people/api_search",
                headers={"Content-Type": "application/json", "X-Api-Key": api_key},
                json={
                    "q_organization_name": org_name,
                    "person_locations": [location],
                    "person_titles": titles,
                    "page": 1,
                    "per_page": max_results,
                },
            )
            if resp.status_code != 200:
                logger.warning(f"Apollo search failed: {resp.status_code}")
                return []
            return resp.json().get("people", [])
    except Exception as e:
        logger.error(f"Apollo search error: {e}")
        return []


async def _apollo_reveal(person_id: str) -> Optional[dict]:
    """Reveal contact's full details via Apollo."""
    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        return None

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.apollo.io/api/v1/people/match",
                headers={"Content-Type": "application/json", "X-Api-Key": api_key},
                json={"id": person_id},
            )
            if resp.status_code == 200 and "person" in resp.json():
                return resp.json()["person"]
    except Exception as e:
        logger.error(f"Apollo reveal error: {e}")
    return None


async def _layer_apollo(
    hotel_name: str,
    brand: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    management_company: Optional[str],
    opening_date: Optional[str],
    result: EnrichmentResult,
    broad: bool = False,
) -> bool:
    """Layer 3: Apollo search — always tagged as chain_area."""
    layer_name = "apollo_broad" if broad else "apollo_specific"
    result.layers_tried.append(layer_name)

    specific_brand, parent_company = _resolve_parent_brand(
        brand, hotel_name, management_company or result.management_company
    )
    parent_brand = specific_brand if not broad else parent_company
    if not parent_brand:
        logger.info(f"Cannot resolve parent brand for {hotel_name}")
        return False

    location = (
        _build_region_string(state, country)
        if broad
        else _build_location_string(city, state, country)
    )

    mode = _get_search_mode(opening_date)
    titles = _get_priority_titles(mode)[:6]

    logger.info(f"Apollo {layer_name}: {parent_brand} in {location}")

    people = await _apollo_search(parent_brand, location, titles)
    await asyncio.sleep(ENRICHMENT_SETTINGS["apollo_delay_seconds"])

    if not people:
        return False

    max_reveals = ENRICHMENT_SETTINGS["max_apollo_reveals_per_lead"]
    revealed_count = 0

    for person in people[: max_reveals + 1]:
        if revealed_count >= max_reveals:
            break

        person_id = person.get("id")
        if not person_id:
            continue

        revealed = await _apollo_reveal(person_id)
        await asyncio.sleep(ENRICHMENT_SETTINGS["apollo_delay_seconds"])

        if not revealed:
            continue

        first = revealed.get("first_name", "")
        last = revealed.get("last_name", "")
        if not first or not last:
            continue

        full_name = f"{first} {last}".strip()

        existing_names = [c.get("name", "").lower() for c in result.contacts]
        if full_name.lower() in existing_names:
            continue

        org_name = (revealed.get("organization") or {}).get("name", "")
        person_title = revealed.get("title", "")

        contact = {
            "name": full_name,
            "title": _clean_title(person_title),
            "email": revealed.get("email"),
            "phone": None,
            "linkedin": revealed.get("linkedin_url"),
            "organization": org_name,
            "scope": "chain_area",
            "confidence": "low",
            "confidence_note": (
                f"Apollo: {parent_brand} in {location}. "
                f"Not confirmed at {hotel_name} specifically."
            ),
            "source": "apollo",
            "source_type": "apollo_reveal",
        }

        phones = revealed.get("phone_numbers") or []
        if phones:
            contact["phone"] = phones[0].get("sanitized_number", "")

        result.contacts.append(contact)
        result.sources_used.append(f"Apollo: {full_name}")
        revealed_count += 1

    return revealed_count > 0


# ═══════════════════════════════════════════════════════════════
# GEMINI CONTACT VERIFICATION — AI reads context to fix false positives
# ═══════════════════════════════════════════════════════════════


CONTACT_VERIFICATION_PROMPT = """You are a hotel staffing verification expert for JA Uniforms, a hotel uniform supplier.

TARGET HOTEL: {hotel_name}
LOCATION: {location}
BRAND: {brand}
MANAGEMENT COMPANY: {management_company}

Below are contacts discovered during lead research. For EACH contact, determine:
1) Their ACTUAL job title (not someone else mentioned in a post)
2) Their ACTUAL employer/organization
3) Whether they are OPERATIONAL HOTEL STAFF at the target hotel

OPERATIONAL HOTEL STAFF includes: General Manager, Director of Housekeeping, Executive Housekeeper,
Purchasing Manager, Director of Operations, Director of Rooms, Front Office Manager, F&B Director,
Assistant GM, Resort Manager, Property Manager, Hotel Manager, Operations Manager, Housekeeping Manager,
Uniform Manager, Wardrobe Manager, Laundry Manager, Supply Chain Manager, Procurement Manager, Executive Chef.

NOT operational hotel staff (REJECT these):
- C-suite executives: CEO, COO, CFO, Chairman, Board Member, Investor, Founder, President
- Regional/corporate roles: Regional Director, VP of Development, SVP, Area Manager
- Construction contractors, architects, project managers for building projects
- Sales, marketing, revenue management, catering sales roles
- People at other hotels, not the target hotel
- People mentioned in a LinkedIn post who are NOT the post author

CRITICAL RULE FOR LINKEDIN POSTS: If source_url contains /posts/, the contact name is the POST AUTHOR.
Titles mentioned in the snippet may refer to SOMEONE ELSE discussed in the post, NOT the author.
Read the snippet carefully to determine the post author actual role vs who they are writing about.

IMPORTANT RULES FOR KEEPING vs REJECTING:
ALWAYS REJECT these even if they appear connected to the hotel:
- C-suite/corporate: CEO, COO, CFO, Chairman, Investor, Founder, President, Board Member
- Regional/area roles: Regional Director, VP of Development, SVP, Area Manager
- Construction: contractors, architects, project managers for building projects
- Sales/marketing: Director of Sales, National Accounts, Revenue Management, Catering Sales
- People confirmed to work at a DIFFERENT hotel than the target
- People with NO title whose snippet context mentions construction, building site, onsite progress, or groundbreaking
  (these are typically contractors visiting the construction site, NOT hotel operational staff)
- People whose ORGANIZATION name contains: Construction, Capital, Development, Holdings, Investment, Architecture,
  Contracting, Consulting, Engineering (these are vendors/developers, NOT hotel operational staff)
  Exception: only keep them if they hold a clear operational hotel title like General Manager or Director of Housekeeping

CRITICAL: A contact MUST be confirmed at the EXACT target hotel to be kept as hotel_specific.
Same city is NOT enough. "Director of Housekeeping in Miami" does NOT mean they work at the target hotel.
Check raw_snippet and organization carefully - the target hotel name must appear in their profile/snippet.
If you cannot confirm the SPECIFIC hotel, set corrected_scope to "chain_area" not "hotel_specific".

ALWAYS KEEP these:
- Contacts whose ACTUAL ROLE (not a role mentioned in someone else's post) is operational hotel staff
- Resort Manager, Director of Operations, Director of Rooms, Director of F&B, Executive Housekeeper,
  Purchasing Manager, Housekeeping Manager, Front Office Manager, Hotel Manager, Property Manager
- Contacts with no extractable title but whose LinkedIn profile URL or org matches the target hotel

WHEN IN DOUBT: If you cannot determine their role but they appear connected to the target hotel, KEEP them.
But if you CAN determine they are CEO, Investor, Sales, or Construction - ALWAYS REJECT.

REMINDER: For LinkedIn POSTS (source_url contains /posts/), the poster OWN title is NOT in the post text.
The post text describes OTHER people. The poster is typically a corporate executive sharing company news.

EXAMPLE OF A FALSE POSITIVE YOU MUST CATCH:
- name: "Alinio Azevedo"
- source_url: linkedin.com/posts/alinioazevedo_exciting-day-onsite...
- raw_snippet: "Exciting day onsite at our Westin Cocoa Beach Resort. Construction is progressing well and our recently-named General Manager Brett Orlando..."
- extracted_title: "General Manager"
WRONG: Keeping Alinio as General Manager. "General Manager" refers to Brett Orlando, not Alinio.
RIGHT: Alinio is the poster (CEO/Investor). He should be REJECTED as corporate. Brett should be a separate contact.

Apply this same logic to ALL posts: the poster is sharing news about someone else getting a role.
If raw_search_title says "Person Name Post - LinkedIn", that person is the POSTER not the role holder.
Cross-reference the snippet text carefully - who is ACTUALLY being named/appointed/hired?

IMPORTANT: If a contact has NO title in the snippet but their LinkedIn profile URL or context confirms they work at the
target hotel, do NOT reject them. Set corrected_scope to "hotel_specific" and leave verified_title empty.
Only reject contacts you can CONFIRM are non-operational (sales, construction, corporate, etc).
When in doubt, KEEP the contact — our scoring system will handle ranking.

IMPORTANT: If a contact has NO title in the snippet but their LinkedIn profile URL or context confirms they work at the
target hotel, do NOT reject them. Set corrected_scope to "hotel_specific" and leave verified_title empty.
Only reject contacts you can CONFIRM are non-operational (sales, construction, corporate, etc).
When in doubt, KEEP the contact — our scoring system will handle ranking.

CONTACTS TO VERIFY:
{contacts_json}

Respond with ONLY a JSON array. For each contact:
{{"name": "original name", "verified_title": "actual title or empty", "verified_org": "actual employer", "is_hotel_ops": true/false, "is_at_target_hotel": true/false, "rejection_reason": "why rejected or null", "corrected_scope": "hotel_specific|chain_area|chain_corporate|rejected"}}
"""


async def _verify_contacts_with_gemini(
    contacts: list[dict],
    hotel_name: str,
    brand: Optional[str] = None,
    management_company: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
) -> list[dict]:
    """
    AI verification layer: Gemini reads raw snippets to determine each contact
    real title, org, and relevance. Fixes false positives from regex parsing.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set, skipping contact verification")
        return contacts

    # Build verification payload with raw snippet context
    contacts_for_verification = []
    for i, c in enumerate(contacts):
        entry = {
            "index": i,
            "name": c.get("name", ""),
            "extracted_title": c.get("title", ""),
            "organization": c.get("organization", ""),
            "source_url": c.get("source", ""),
            "source_type": c.get("source_type", ""),
            "current_scope": c.get("scope", ""),
            "raw_snippet": c.get("_raw_snippet", ""),
            "raw_search_title": c.get("_raw_title", ""),
        }
        contacts_for_verification.append(entry)

    if not contacts_for_verification:
        return contacts

    location = _build_location_string(city, state, country)

    prompt = CONTACT_VERIFICATION_PROMPT.format(
        hotel_name=hotel_name,
        location=location,
        brand=brand or "Unknown",
        management_company=management_company or "Unknown",
        contacts_json=json.dumps(contacts_for_verification, indent=2),
    )

    model = ENRICHMENT_SETTINGS["gemini_model"]
    api_url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}"
        f":generateContent?key={api_key}"
    )

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                api_url,
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.1},
                },
            )

            if resp.status_code != 200:
                logger.error(f"Gemini verification API error: {resp.status_code}")
                return contacts

            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            verifications = json.loads(text.strip())

    except Exception as e:
        import traceback

        logger.error(f"Gemini contact verification failed: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        return contacts

    # Apply verification results
    verified_contacts = []
    for v in verifications:
        vname = v.get("name", "").lower().strip()
        # Find matching original contact
        match = None
        for c in contacts:
            if c.get("name", "").lower().strip() == vname:
                match = c
                break
        if not match:
            continue

        corrected_scope = v.get("corrected_scope", "")
        rejection_reason = v.get("rejection_reason")

        if corrected_scope == "rejected":
            logger.info(f"Gemini REJECTED: {v.get('name')} -- {rejection_reason}")
            match["_gemini_rejected"] = True
            match["_gemini_rejection_reason"] = rejection_reason
            continue

        # Update with verified info
        verified_title = v.get("verified_title", "")
        verified_org = v.get("verified_org", "")

        if verified_title:
            old_title = match.get("title", "")
            if old_title.lower() != verified_title.lower():
                logger.info(
                    f"Gemini title fix: {v.get('name')}: "
                    f"'{old_title}' -> '{verified_title}'"
                )
            match["title"] = verified_title

        if verified_org:
            match["organization"] = verified_org

        if corrected_scope in ("hotel_specific", "chain_area", "chain_corporate"):
            old_scope = match.get("scope", "")
            if old_scope != corrected_scope:
                logger.info(
                    f"Gemini scope fix: {v.get('name')}: "
                    f"'{old_scope}' -> '{corrected_scope}'"
                )
            match["scope"] = corrected_scope

        match["_gemini_verified"] = True
        match["_gemini_is_hotel_ops"] = v.get("is_hotel_ops", False)
        match["_gemini_is_at_target"] = v.get("is_at_target_hotel", False)
        verified_contacts.append(match)

    # Keep contacts Gemini did not mention (do not drop silently)
    verified_names = {v.get("name", "").lower().strip() for v in verifications}
    for c in contacts:
        if c.get("name", "").lower().strip() not in verified_names:
            logger.warning(f"Gemini skipped contact: {c.get('name')} -- keeping as-is")
            verified_contacts.append(c)

    # Deterministic backstop: reject contacts from non-hotel orgs with no operational title
    NON_HOTEL_ORG_KEYWORDS = [
        "construction",
        "capital",
        "development",
        "holdings",
        "investment",
        "architecture",
        "contracting",
        "consulting",
        "engineering",
        "ventures",
        "equity",
        "realty",
        "real estate",
        "contractors",
        "builders",
    ]
    OPERATIONAL_TITLES = [
        "general manager",
        "director of",
        "executive housekeeper",
        "purchasing manager",
        "housekeeping manager",
        "front office manager",
        "hotel manager",
        "resort manager",
        "property manager",
        "operations manager",
        "assistant general manager",
        "rooms division",
        "uniform manager",
        "wardrobe manager",
        "laundry manager",
    ]
    final_contacts = []
    for c in verified_contacts:
        org = (c.get("organization") or "").lower()
        title = (c.get("title") or "").lower()
        has_non_hotel_org = any(kw in org for kw in NON_HOTEL_ORG_KEYWORDS)
        has_operational_title = any(kw in title for kw in OPERATIONAL_TITLES)
        if has_non_hotel_org and not has_operational_title:
            logger.info(
                f"Org-filter REJECTED: {c.get('name')} -- org='{c.get('organization')}' "
                f"title='{c.get('title', '')}' (non-hotel org, no operational title)"
            )
            continue
        final_contacts.append(c)

    rejected_count = len(contacts) - len(final_contacts)
    logger.info(
        f"Gemini verification: {len(contacts)} in -> "
        f"{len(final_contacts)} out ({rejected_count} rejected)"
    )
    return final_contacts


# ═══════════════════════════════════════════════════════════════
# MAIN ENRICHMENT ORCHESTRATOR — v4 with validation + auto-retry
# ═══════════════════════════════════════════════════════════════


async def enrich_lead_contacts(
    lead_id: int,
    hotel_name: str,
    brand: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    management_company: Optional[str] = None,
    opening_date: Optional[str] = None,
) -> EnrichmentResult:
    """
    Main enrichment function v4. Runs multi-layer search with
    SAP-trained validation and auto-retry on false positives.
    """
    result = EnrichmentResult()
    logger.info(f"Starting enrichment v4 for lead {lead_id}: {hotel_name}")

    # ── Layer 1: Web search + scrape + AI extract ──
    try:
        found = await _layer_web_search(
            hotel_name,
            brand,
            management_company,
            city,
            state,
            country,
            opening_date,
            result,
            retry_attempt=0,
        )
        if found:
            hotel_specific = [
                c for c in result.contacts if c.get("scope") == "hotel_specific"
            ]
            logger.info(
                f"Layer 1: {len(result.contacts)} contacts "
                f"({len(hotel_specific)} hotel-specific)"
            )
    except Exception as e:
        result.errors.append(f"Web search failed: {str(e)}")
        logger.error(f"Layer 1 error: {e}")

    # ── Layer 2: LinkedIn snippet extraction ──
    try:
        found = await _layer_linkedin_snippets(
            hotel_name,
            brand,
            management_company,
            city,
            state,
            country,
            result,
        )
        if found:
            logger.info("Layer 2: LinkedIn snippets found contacts")
    except Exception as e:
        result.errors.append(f"LinkedIn snippets failed: {str(e)}")
        logger.error(f"Layer 2 error: {e}")

    # ═══════════════════════════════════════════════════════════
    # GEMINI AI VERIFICATION — fix false positives before scoring
    # ═══════════════════════════════════════════════════════════

    if result.contacts:
        try:
            result.contacts = await _verify_contacts_with_gemini(
                contacts=result.contacts,
                hotel_name=hotel_name,
                brand=brand,
                management_company=management_company or result.management_company,
                city=city,
                state=state,
                country=country,
            )
            # Remove rejected contacts
            result.contacts = [
                c for c in result.contacts if not c.get("_gemini_rejected", False)
            ]
        except Exception as e:
            result.errors.append(f"Gemini verification failed: {str(e)}")
            logger.error(f"Gemini verification error: {e}")

    # ══════════════════════════════════════════════════════════
    # CONTACT VALIDATION — SAP-trained scoring + false positive filter
    # ══════════════════════════════════════════════════════════

    if result.contacts:
        scored_contacts = contact_validator.validate_and_score(
            contacts=result.contacts,
            hotel_name=hotel_name,
            brand=brand,
            management_company=management_company or result.management_company,
            city=city,
            state=state,
            country=country,
        )

        # Check if we should retry (all name collisions / no decision makers)
        should_retry, retry_reason = contact_validator.should_retry_search(
            scored_contacts
        )
        if should_retry:
            logger.info(f"Validation says retry: {retry_reason}")

            # ── AUTO-RETRY with different queries ──
            retry_result_contacts_before = len(result.contacts)
            try:
                await _layer_web_search(
                    hotel_name,
                    brand,
                    management_company,
                    city,
                    state,
                    country,
                    opening_date,
                    result,
                    retry_attempt=1,
                )
                # Re-validate with new contacts included
                if len(result.contacts) > retry_result_contacts_before:
                    scored_contacts = contact_validator.validate_and_score(
                        contacts=result.contacts,
                        hotel_name=hotel_name,
                        brand=brand,
                        management_company=management_company
                        or result.management_company,
                        city=city,
                        state=state,
                        country=country,
                    )
            except Exception as e:
                logger.warning(f"Retry search failed: {e}")

        # Filter and rank — keep only good contacts
        good_contacts = contact_validator.filter_and_rank(
            scored_contacts,
            min_score=5,
            max_contacts=MAX_CONTACTS_TO_SAVE,
        )

        # Replace raw contacts with validated ones, preserving extra metadata
        validated_contacts = []
        for sc in good_contacts:
            c = sc.contact.copy()
            c["_validation_score"] = sc.total_score
            c["_buyer_tier"] = sc.title_tier.name if sc.title_tier else "UNKNOWN"
            c["_validation_confidence"] = sc.confidence
            c["_validation_scope"] = sc.scope_tag
            c["_validation_reason"] = sc.reason
            validated_contacts.append(c)

        result.contacts = validated_contacts
        logger.info(
            f"Validation: {len(validated_contacts)} contacts passed "
            f"(from {len(scored_contacts)} raw)"
        )

    # ── Layer 3: Apollo (always run as supplement to fill gaps) ──
    if True:  # Always run Apollo to supplement web/LinkedIn contacts
        try:
            pre_apollo_count = len(result.contacts)
            await _layer_apollo(
                hotel_name,
                brand,
                city,
                state,
                country,
                management_company,
                opening_date,
                result,
                broad=False,
            )
            # Validate any new Apollo contacts too
            if len(result.contacts) > pre_apollo_count:
                new_apollo = result.contacts[pre_apollo_count:]
                scored_apollo = contact_validator.validate_and_score(
                    contacts=new_apollo,
                    hotel_name=hotel_name,
                    brand=brand,
                    management_company=management_company or result.management_company,
                    city=city,
                    state=state,
                    country=country,
                )
                good_apollo = contact_validator.filter_and_rank(
                    scored_apollo, min_score=0
                )
                # Replace apollo contacts with validated ones
                result.contacts = result.contacts[:pre_apollo_count]
                for sc in good_apollo:
                    c = sc.contact.copy()
                    c["_validation_score"] = sc.total_score
                    c["_buyer_tier"] = (
                        sc.title_tier.name if sc.title_tier else "UNKNOWN"
                    )
                    c["_validation_confidence"] = sc.confidence
                    c["_validation_scope"] = sc.scope_tag
                    result.contacts.append(c)
        except Exception as e:
            result.errors.append(f"Apollo specific failed: {str(e)}")

    # ── Layer 4: Apollo broad (only if zero contacts) ──
    if not result.contacts:
        try:
            await _layer_apollo(
                hotel_name,
                brand,
                city,
                state,
                country,
                management_company,
                opening_date,
                result,
                broad=True,
            )
        except Exception as e:
            result.errors.append(f"Apollo broad failed: {str(e)}")

    # ── Final deduplicate by name ──
    seen = set()
    unique = []
    for c in result.contacts:
        key = c.get("name", "").lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(c)
    result.contacts = unique

    # ── Sort: hotel_specific > chain_area > unknown, then by validation score ──
    scope_rank = {
        "hotel_specific": 0,
        "chain_area": 1,
        "chain_corporate": 2,
        "unknown": 3,
    }
    result.contacts.sort(
        key=lambda c: (
            scope_rank.get(c.get("scope", c.get("_validation_scope", "unknown")), 3),
            -(c.get("_validation_score", 0)),
        )
    )

    # ── TOP 5 ONLY ──
    result.contacts = result.contacts[:MAX_CONTACTS_TO_SAVE]

    logger.info(
        f"Enrichment v4 complete for {hotel_name}: "
        f"{len(result.contacts)} contacts, "
        f"layers: {result.layers_tried}"
    )
    return result


# ═══════════════════════════════════════════════════════════════
# SAVE TO DATABASE — v4 with tier scoring in notes
# ═══════════════════════════════════════════════════════════════


async def save_enrichment_to_lead(lead_id: int, result: EnrichmentResult) -> dict:
    """Save enrichment results. REPLACES old enrichment notes (no duplicates)."""
    from app.database import async_session
    from sqlalchemy import select
    from app.models.potential_lead import PotentialLead

    best = result.best_contact
    if not best and not result.management_company and not result.developer:
        return {"status": "no_data", "message": "No contacts or details found"}

    async with async_session() as session:
        db_result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = db_result.scalar_one_or_none()
        if not lead:
            return {"status": "error", "message": f"Lead {lead_id} not found"}

        updated_fields = []

        if best:
            lead.contact_name = best.get("name")
            updated_fields.append("contact_name")

            lead.contact_title = best.get("title")
            updated_fields.append("contact_title")

            if best.get("email"):
                lead.contact_email = best["email"]
                updated_fields.append("contact_email")

            if best.get("linkedin"):
                lead.contact_linkedin = best["linkedin"]
                updated_fields.append("contact_linkedin")

            if best.get("phone"):
                lead.contact_phone = best["phone"]
                updated_fields.append("contact_phone")

        if result.management_company:
            lead.management_company = result.management_company
            updated_fields.append("management_company")
        if result.developer:
            lead.developer = result.developer
            updated_fields.append("developer")

        # ── Build notes: REPLACE old enrichment section with tier-scored format ──
        if result.contacts:
            existing_notes = lead.notes or ""
            enrichment_marker = "--- Enrichment ("
            if enrichment_marker in existing_notes:
                idx = existing_notes.index(enrichment_marker)
                existing_notes = existing_notes[:idx].rstrip()

            lines = []
            if existing_notes:
                lines.append(existing_notes)

            lines.append(
                f"\n--- Enrichment ({datetime.now().strftime('%b %d, %Y')}) "
                f"— Top {len(result.contacts)} contacts ---"
            )

            for i, c in enumerate(result.contacts, 1):
                # Tier emoji from SAP classifier
                tier_name = c.get("_buyer_tier", "UNKNOWN")
                tier_emoji = {
                    "TIER1_UNIFORM_DIRECT": "\U0001f3c6",  # 🏆
                    "TIER2_PURCHASING": "\U0001f4b0",  # 💰
                    "TIER3_GM_OPS": "\U0001f3e8",  # 🏨
                    "TIER4_FB": "\U0001f37d\ufe0f",  # 🍽️
                    "TIER5_HR": "\U0001f465",  # 👥
                }.get(tier_name, "\u2753")

                v_conf = c.get("_validation_confidence", c.get("confidence", "low"))
                confidence_icon = {
                    "high": "\U0001f7e2",  # 🟢
                    "medium": "\U0001f7e1",  # 🟡
                    "low": "\U0001f534",  # 🔴
                }.get(v_conf, "\U0001f534")

                scope_label = c.get("scope", "unknown").replace("_", " ").title()
                conf_label = v_conf.title()
                v_score = c.get("_validation_score", "?")

                line = f"\n{i}. {tier_emoji} {confidence_icon} {c['name']}"
                line += f"\n   Title: {c.get('title', 'N/A')}"
                line += f"\n   Tier: {tier_name} | Score: {v_score}"
                if c.get("email"):
                    line += f"\n   Email: {c['email']}"
                if c.get("linkedin"):
                    line += f"\n   LinkedIn: {c['linkedin']}"
                if c.get("phone"):
                    line += f"\n   Phone: {c['phone']}"
                if c.get("organization"):
                    line += f"\n   Org: {c['organization']}"
                line += f"\n   [{scope_label} | {conf_label}] {c.get('confidence_note', c.get('_validation_reason', ''))}"

                lines.append(line)

            lead.notes = "\n".join(lines)
            updated_fields.append("notes")

        if result.additional_details and not lead.description:
            lead.description = result.additional_details
            updated_fields.append("description")

        lead.updated_at = datetime.utcnow()
        await session.commit()

        return {
            "status": "success",
            "updated_fields": updated_fields,
            "contacts_found": len(result.contacts),
            "best_contact": best,
        }
