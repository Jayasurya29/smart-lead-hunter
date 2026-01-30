"""
TARGETED CONTACT FINDER
========================
Finds the RIGHT decision makers for NEW hotel openings.

STRATEGY:
1. Extract NAMES from press releases/articles (GM appointed, Director hired, etc.)
2. Use Apollo.io to find their email (targeted - 1 credit per person)
3. For brands without specific names, contact regional/corporate offices
4. Generate LinkedIn search URLs for manual research

PROBLEM WE'RE SOLVING:
- Hunter.io gives us fourseasons.com emails = Corporate contacts, not property-specific
- We need the Executive Housekeeper at THIS specific new hotel
- New hires may not be in databases yet

SOLUTION:
- Extract names mentioned in articles ("John Smith appointed GM of Four Seasons Orlando")
- Do targeted Apollo lookup for that specific person
- Provide LinkedIn search strategies for manual follow-up
"""

import asyncio
import json
import logging
import re
import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any
from datetime import datetime

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class ExtractedPerson:
    """A person mentioned in an article/press release"""
    name: str
    title: str = ""
    hotel: str = ""
    context: str = ""  # The sentence where they were mentioned
    is_decision_maker: bool = False
    relevance_score: int = 0


@dataclass
class EnrichedContact:
    """A contact with email/phone found via API"""
    name: str
    title: str = ""
    email: str = ""
    phone: str = ""
    linkedin_url: str = ""
    hotel: str = ""
    source: str = ""  # "apollo", "hunter", "article", "guessed"
    confidence: int = 0
    is_verified: bool = False


@dataclass
class ContactSearchStrategy:
    """Strategy for finding contacts for a specific hotel"""
    hotel_name: str
    brand: str
    location: str
    opening_date: str
    
    # People extracted from articles
    extracted_people: List[ExtractedPerson] = field(default_factory=list)
    
    # Enriched contacts (with emails)
    enriched_contacts: List[EnrichedContact] = field(default_factory=list)
    
    # Fallback strategies
    corporate_contacts: List[EnrichedContact] = field(default_factory=list)
    linkedin_searches: List[str] = field(default_factory=list)
    email_patterns: List[str] = field(default_factory=list)
    
    # Status
    credits_used: int = 0
    search_complete: bool = False


# =============================================================================
# PERSON EXTRACTION FROM ARTICLES
# =============================================================================

class ArticlePersonExtractor:
    """
    Extract people mentioned in hotel opening articles.
    
    Looks for patterns like:
    - "John Smith has been appointed General Manager"
    - "Led by Executive Director Jane Doe"
    - "Contact: Sarah Johnson, Director of PR"
    """
    
    # Job titles that indicate uniform decision makers
    DECISION_MAKER_TITLES = [
        # Tier 1 - Primary (buy uniforms directly)
        'executive housekeeper', 'director of housekeeping', 'housekeeping director',
        'director of purchasing', 'purchasing director', 'procurement director',
        'purchasing manager', 'procurement manager',
        
        # Tier 2 - Secondary (approve purchases)
        'general manager', 'hotel manager', 'managing director',
        'director of operations', 'operations director',
        'pre-opening director', 'pre-opening manager',
        'task force manager', 'opening team',
        
        # Tier 3 - Influencers
        'director of food', 'f&b director', 'food and beverage director',
        'director of spa', 'spa director',
        'director of rooms', 'rooms division director',
        'hr director', 'director of human resources',
    ]
    
    # Titles to SKIP (not relevant for uniforms)
    SKIP_TITLES = [
        'communications', 'public relations', 'pr director', 'media relations',
        'marketing', 'sales director', 'director of sales', 'revenue',
        'reservations', 'social media', 'digital', 'brand ambassador',
        'analyst', 'accountant', 'finance director', 'cfo', 'legal', 'attorney',
        'architect', 'designer', 'interior', 'landscape',
    ]
    
    # Patterns to find people in text
    PERSON_PATTERNS = [
        # "John Smith has been appointed General Manager"
        (r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:has been|was|is|will be)\s+(?:appointed|named|hired|promoted|serve)\s+(?:as\s+)?(?:the\s+)?(.+?)(?:\s+of\s+the|\s+of\s+Four|\s+at\s+|\s+for\s+|,\s+over|,\s+who|\.)', 'name_first'),
        
        # "Sarah Johnson will serve as Director of Housekeeping"
        (r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+will\s+serve\s+as\s+(.+?)(?:,|\.|\s+over|\s+who)', 'name_first'),
        
        # "General Manager John Smith"
        (r'((?:General Manager|Executive Housekeeper|Director of [A-Za-z]+|Pre-Opening Director|Hotel Manager))\s+([A-Z][a-z]+\s+[A-Z][a-z]+)', 'title_first'),
        
        # "led by John Smith, General Manager"
        (r'(?:led by|headed by|managed by|under|overseen by)\s+([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(.+?)(?:\.|,|who)', 'name_first'),
        
        # "Contact: John Smith, Director of Sales"
        (r'[Cc]ontact:?\s*([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(.+?)(?:\s+at\s+|\s*@|\.)', 'name_first'),
    ]
    
    def extract_people(self, article_text: str, hotel_name: str = "") -> List[ExtractedPerson]:
        """Extract people mentioned in article text"""
        people = []
        seen_names = set()
        
        # Normalize text
        text = article_text.replace('\n', ' ').replace('\r', ' ')
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single
        
        for pattern, pattern_type in self.PERSON_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                groups = match.groups()
                
                if len(groups) >= 2:
                    if pattern_type == 'name_first':
                        name = groups[0].strip()
                        title = groups[1].strip()
                    else:  # title_first
                        title = groups[0].strip()
                        name = groups[1].strip()
                    
                    # Validate name looks like a real name (First Last format)
                    if not re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', name):
                        continue
                    
                    # Clean up title
                    title = re.sub(r'\s+', ' ', title).strip()
                    title = title.rstrip('.,;:')
                    
                    if name and name not in seen_names and len(name) > 4:
                        seen_names.add(name)
                        
                        # Check if decision maker
                        title_lower = title.lower()
                        is_decision_maker = any(t in title_lower for t in self.DECISION_MAKER_TITLES)
                        is_skip = any(t in title_lower for t in self.SKIP_TITLES)
                        
                        if is_skip:
                            continue  # Skip irrelevant people
                        
                        # Calculate relevance
                        relevance = 0
                        if any(t in title_lower for t in ['executive housekeeper', 'director of housekeeping', 'housekeeping']):
                            relevance = 100
                        elif any(t in title_lower for t in ['purchasing', 'procurement']):
                            relevance = 100
                        elif 'general manager' in title_lower:
                            relevance = 90
                        elif 'pre-opening' in title_lower or 'task force' in title_lower:
                            relevance = 95
                        elif 'director' in title_lower:
                            relevance = 70
                        elif 'manager' in title_lower:
                            relevance = 50
                        else:
                            relevance = 30
                        
                        # Mark as decision maker if high relevance
                        is_decision_maker = relevance >= 70
                        
                        # Get context (sentence containing the name)
                        context = self._get_context(text, name)
                        
                        people.append(ExtractedPerson(
                            name=name,
                            title=title,
                            hotel=hotel_name,
                            context=context,
                            is_decision_maker=is_decision_maker,
                            relevance_score=relevance
                        ))
        
        # Sort by relevance
        people.sort(key=lambda p: p.relevance_score, reverse=True)
        
        return people
    
    def _get_context(self, text: str, name: str) -> str:
        """Get the sentence containing the name"""
        # Find sentences containing the name
        sentences = re.split(r'[.!?]', text)
        for sentence in sentences:
            if name in sentence:
                return sentence.strip()[:200]
        return ""


# =============================================================================
# SMART CONTACT ENRICHMENT
# =============================================================================

class TargetedContactFinder:
    """
    Find contacts for specific hotel openings using multiple strategies.
    
    Strategy Order:
    1. Extract names from article → Apollo lookup (most targeted)
    2. Hunter.io domain search (for known brands)
    3. Generate LinkedIn search URLs (manual follow-up)
    4. Email pattern guessing (last resort)
    """
    
    # Brand email domains
    BRAND_DOMAINS = {
        'four seasons': 'fourseasons.com',
        'ritz-carlton': 'ritzcarlton.com',
        'ritz carlton': 'ritzcarlton.com',
        'marriott': 'marriott.com',
        'jw marriott': 'marriott.com',
        'w hotel': 'whotels.com',
        'westin': 'westin.com',
        'sheraton': 'sheraton.com',
        'st regis': 'stregis.com',
        'hilton': 'hilton.com',
        'waldorf astoria': 'waldorfastoria.com',
        'conrad': 'conradhotels.com',
        'hyatt': 'hyatt.com',
        'grand hyatt': 'hyatt.com',
        'park hyatt': 'hyatt.com',
        'andaz': 'andaz.com',
        'intercontinental': 'ihg.com',
        'kimpton': 'kimptonhotels.com',
        'fairmont': 'fairmont.com',
        'sofitel': 'sofitel.com',
        'rosewood': 'rosewoodhotels.com',
        'mandarin oriental': 'mandarinoriental.com',
        'peninsula': 'peninsula.com',
        'aman': 'aman.com',
        'one&only': 'oneandonlyresorts.com',
        'six senses': 'sixsenses.com',
        'montage': 'montagehotels.com',
        'auberge': 'aubergeresorts.com',
        'loews': 'loewshotels.com',
        'omni': 'omnihotels.com',
    }
    
    # Target titles for LinkedIn searches
    TARGET_TITLES_FOR_SEARCH = [
        "Executive Housekeeper",
        "Director of Housekeeping", 
        "Director of Purchasing",
        "General Manager",
        "Pre-Opening Director",
    ]
    
    def __init__(self, hunter_api_key: str = None, apollo_api_key: str = None):
        self.hunter_key = hunter_api_key or os.environ.get("HUNTER_API_KEY")
        self.apollo_key = apollo_api_key or os.environ.get("APOLLO_API_KEY")
        self.person_extractor = ArticlePersonExtractor()
        self.credits_used = {"hunter": 0, "apollo": 0}
    
    async def find_contacts(
        self,
        hotel_name: str,
        brand: str,
        location: str,
        opening_date: str,
        article_text: str = "",
        max_credits: int = 3,
    ) -> ContactSearchStrategy:
        """
        Find contacts for a hotel opening using all available strategies.
        
        Args:
            hotel_name: Name of the new hotel
            brand: Hotel brand (Four Seasons, Hilton, etc.)
            location: City, State
            opening_date: Expected opening date
            article_text: Full text of the article (for name extraction)
            max_credits: Maximum API credits to use
        
        Returns:
            ContactSearchStrategy with all found contacts and fallback options
        """
        strategy = ContactSearchStrategy(
            hotel_name=hotel_name,
            brand=brand,
            location=location,
            opening_date=opening_date,
        )
        
        credits_remaining = max_credits
        
        # STEP 1: Extract names from article
        if article_text:
            logger.info(f"📰 Extracting names from article...")
            strategy.extracted_people = self.person_extractor.extract_people(
                article_text, 
                hotel_name
            )
            logger.info(f"   Found {len(strategy.extracted_people)} people mentioned")
            
            for person in strategy.extracted_people[:3]:  # Show top 3
                logger.info(f"   - {person.name}: {person.title} (score: {person.relevance_score})")
        
        # STEP 2: Apollo lookup for extracted decision makers
        if self.apollo_key and credits_remaining > 0:
            decision_makers = [p for p in strategy.extracted_people if p.relevance_score >= 70]
            
            for person in decision_makers[:credits_remaining]:  # Limit by credits
                logger.info(f"🔍 Apollo lookup: {person.name} @ {hotel_name}")
                contact = await self._apollo_enrich_person(
                    first_name=person.name.split()[0],
                    last_name=" ".join(person.name.split()[1:]),
                    organization=hotel_name,
                    title=person.title,
                )
                
                if contact and contact.email:
                    contact.hotel = hotel_name
                    strategy.enriched_contacts.append(contact)
                    credits_remaining -= 1
                    strategy.credits_used += 1
        
        # STEP 3: Hunter.io for brand domain (if no specific contacts found)
        domain = self._get_domain_for_brand(brand, hotel_name)
        if self.hunter_key and domain and len(strategy.enriched_contacts) < 2 and credits_remaining > 0:
            logger.info(f"🔍 Hunter.io domain search: {domain}")
            corporate_contacts = await self._hunter_domain_search(domain)
            
            # Filter to relevant titles only
            for contact in corporate_contacts:
                if self._is_relevant_title(contact.title):
                    contact.hotel = f"{hotel_name} (via corporate)"
                    strategy.corporate_contacts.append(contact)
            
            credits_remaining -= 1
            strategy.credits_used += 1
        
        # STEP 4: Generate LinkedIn search URLs
        strategy.linkedin_searches = self._generate_linkedin_searches(
            hotel_name, brand, location
        )
        
        # STEP 5: Generate email patterns
        if domain:
            strategy.email_patterns = self._generate_email_patterns(domain)
        
        strategy.search_complete = True
        
        return strategy
    
    async def _apollo_enrich_person(
        self, 
        first_name: str, 
        last_name: str, 
        organization: str,
        title: str = ""
    ) -> Optional[EnrichedContact]:
        """Look up a specific person in Apollo.io"""
        if not self.apollo_key or not HTTPX_AVAILABLE:
            return None
        
        try:
            params = {
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": organization,
                "reveal_personal_emails": "false",
                "reveal_phone_number": "false",
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.apollo.io/api/v1/people/match",
                    params=params,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "no-cache",
                        "x-api-key": self.apollo_key,
                    },
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    person = data.get("person")
                    
                    if person:
                        self.credits_used["apollo"] += 1
                        return EnrichedContact(
                            name=f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                            title=person.get("title", title),
                            email=person.get("email", ""),
                            phone=person.get("phone_numbers", [""])[0] if person.get("phone_numbers") else "",
                            linkedin_url=person.get("linkedin_url", ""),
                            source="apollo",
                            confidence=90 if person.get("email") else 50,
                            is_verified=bool(person.get("email")),
                        )
        
        except Exception as e:
            logger.error(f"Apollo error: {e}")
        
        return None
    
    async def _hunter_domain_search(self, domain: str) -> List[EnrichedContact]:
        """Search Hunter.io for emails at a domain"""
        contacts = []
        
        if not self.hunter_key or not HTTPX_AVAILABLE:
            return contacts
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={
                        "domain": domain,
                        "api_key": self.hunter_key,
                        "limit": 10,
                    },
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    self.credits_used["hunter"] += 1
                    data = response.json()
                    
                    for email_data in data.get("data", {}).get("emails", []):
                        email = email_data.get("value", "")
                        
                        # Skip generic emails
                        if any(g in email.lower() for g in ["info@", "contact@", "hello@", "reservations@", "media@"]):
                            continue
                        
                        contacts.append(EnrichedContact(
                            name=f"{email_data.get('first_name', '')} {email_data.get('last_name', '')}".strip(),
                            title=email_data.get("position", ""),
                            email=email,
                            linkedin_url=email_data.get("linkedin", ""),
                            source="hunter",
                            confidence=email_data.get("confidence", 0),
                            is_verified=True,
                        ))
        
        except Exception as e:
            logger.error(f"Hunter error: {e}")
        
        return contacts
    
    def _get_domain_for_brand(self, brand: str, hotel_name: str) -> Optional[str]:
        """Get email domain for a brand"""
        combined = f"{brand} {hotel_name}".lower()
        
        for brand_key, domain in self.BRAND_DOMAINS.items():
            if brand_key in combined:
                return domain
        
        return None
    
    def _is_relevant_title(self, title: str) -> bool:
        """Check if a title is relevant for uniform sales"""
        if not title:
            return False
        
        title_lower = title.lower()
        
        # Skip irrelevant
        skip_keywords = [
            'communications', 'public relations', 'pr ', 'media',
            'marketing', 'sales', 'revenue', 'reservations',
            'social media', 'digital', 'brand', 'analyst', 'accountant',
            'legal', 'attorney', 'software', 'developer', 'engineer',
        ]
        if any(s in title_lower for s in skip_keywords):
            return False
        
        # Include relevant
        relevant_keywords = [
            'housekeeper', 'housekeeping', 'purchasing', 'procurement',
            'general manager', 'operations', 'pre-opening', 'task force',
            'food', 'beverage', 'f&b', 'spa', 'rooms', 'hr', 'human resources'
        ]
        return any(r in title_lower for r in relevant_keywords)
    
    def _generate_linkedin_searches(self, hotel_name: str, brand: str, location: str) -> List[str]:
        """Generate LinkedIn search URLs for manual research"""
        searches = []
        
        # Clean hotel name for search
        hotel_clean = hotel_name.replace("'", "").replace('"', '')
        
        for title in self.TARGET_TITLES_FOR_SEARCH:
            # LinkedIn search URL format
            search_query = f'"{title}" "{hotel_clean}"'
            encoded_query = search_query.replace(" ", "%20").replace('"', "%22")
            url = f"https://www.linkedin.com/search/results/people/?keywords={encoded_query}"
            searches.append(f"{title}: {url}")
        
        # Also search by brand + location
        if brand:
            search_query = f'"Executive Housekeeper" "{brand}" "{location}"'
            encoded_query = search_query.replace(" ", "%20").replace('"', "%22")
            url = f"https://www.linkedin.com/search/results/people/?keywords={encoded_query}"
            searches.append(f"Brand Search: {url}")
        
        return searches
    
    def _generate_email_patterns(self, domain: str) -> List[str]:
        """Generate common email patterns for a domain"""
        patterns = [
            f"firstname.lastname@{domain}",
            f"firstnamelastname@{domain}",
            f"flastname@{domain}",
            f"firstname_lastname@{domain}",
            f"firstname@{domain}",
        ]
        return patterns


# =============================================================================
# CLI TEST
# =============================================================================

async def test_contact_finder():
    """Test the targeted contact finder"""
    
    # Sample article text (simulating a press release)
    sample_article = """
    Four Seasons Hotels and Resorts Announces New Orlando Property
    
    ORLANDO, FL - Four Seasons Hotels and Resorts today announced plans for a new 
    luxury resort in Orlando, Florida, set to open in Q3 2026.
    
    John Smith has been appointed as General Manager of Four Seasons Resort Orlando. 
    Smith brings over 20 years of luxury hospitality experience, most recently serving 
    as Hotel Manager at Four Seasons Resort Palm Beach.
    
    The pre-opening team will be led by Maria Garcia, Pre-Opening Director, who has 
    successfully launched three Four Seasons properties in the past decade.
    
    Sarah Johnson will serve as Director of Housekeeping, overseeing a team of 150 
    housekeeping staff members.
    
    The 200-room resort will feature multiple dining venues under the direction of 
    Executive Chef Michael Chen.
    
    For media inquiries, contact Jennifer Williams, Director of Public Relations, 
    at jennifer.williams@fourseasons.com.
    
    The resort represents a $500 million investment and is expected to create 
    600 new jobs in the Orlando area.
    """
    
    print("=" * 70)
    print("TARGETED CONTACT FINDER TEST")
    print("=" * 70)
    
    finder = TargetedContactFinder()
    
    # Test without API keys (extraction only)
    strategy = await finder.find_contacts(
        hotel_name="Four Seasons Resort Orlando",
        brand="Four Seasons",
        location="Orlando, FL",
        opening_date="Q3 2026",
        article_text=sample_article,
        max_credits=0,  # No API calls for this test
    )
    
    print("\n📰 EXTRACTED PEOPLE FROM ARTICLE:")
    print("-" * 50)
    for person in strategy.extracted_people:
        status = "✅ DECISION MAKER" if person.is_decision_maker else "📋 Other"
        print(f"  {person.name}")
        print(f"    Title: {person.title}")
        print(f"    Relevance: {person.relevance_score}")
        print(f"    Status: {status}")
        print()
    
    print("\n🔗 LINKEDIN SEARCH URLS:")
    print("-" * 50)
    for search in strategy.linkedin_searches[:3]:
        print(f"  {search[:100]}...")
    
    print("\n📧 EMAIL PATTERNS:")
    print("-" * 50)
    for pattern in strategy.email_patterns:
        print(f"  {pattern}")
    
    print("\n" + "=" * 70)
    print("To run with real API lookups, use:")
    print("  HUNTER_API_KEY=xxx APOLLO_API_KEY=xxx python targeted_contact_finder.py")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(test_contact_finder())