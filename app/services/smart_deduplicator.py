"""
SMART LEAD HUNTER - INTELLIGENT DEDUPLICATION SERVICE
======================================================
Fuzzy matching and smart merging of hotel leads.

PROBLEM:
- Same hotel appears from multiple sources with slightly different names
- "Six Senses Camp Korongo" vs "Six Senses Camp Korongo Utah" = SAME HOTEL
- Need to merge data from multiple sources into ONE complete lead

SOLUTION:
1. Fuzzy name matching (Levenshtein distance + token matching)
2. Location-aware matching (same city/state = likely same hotel)
3. Smart merge - keep best data from each source
4. Database comparison - check against existing leads
5. Source tracking - keep all source URLs

Usage:
    from app.services.smart_deduplicator import SmartDeduplicator
    
    dedup = SmartDeduplicator()
    
    # Deduplicate a list of leads
    unique_leads = dedup.deduplicate(raw_leads)
    
    # Check against database
    new_leads = await dedup.filter_existing(unique_leads)
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple, Any
from datetime import datetime
from difflib import SequenceMatcher
import json

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MergedLead:
    """A lead that may have been merged from multiple sources"""
    # Core identity
    hotel_name: str = ""
    brand: str = ""
    
    # Location
    city: str = ""
    state: str = ""
    country: str = "USA"
    
    # Project details
    opening_date: str = ""
    opening_status: str = ""
    room_count: int = 0
    property_type: str = ""
    
    # Key stakeholders
    management_company: str = ""
    developer: str = ""
    owner: str = ""
    
    # Contact info (best available)
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    
    # Quality metrics
    confidence_score: float = 0.0
    qualification_score: int = 0
    
    # Source tracking
    source_urls: List[str] = field(default_factory=list)
    source_names: List[str] = field(default_factory=list)
    merged_from_count: int = 1
    
    # Timestamps
    first_seen: str = ""
    last_updated: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON/database"""
        return {
            'hotel_name': self.hotel_name,
            'brand': self.brand,
            'city': self.city,
            'state': self.state,
            'country': self.country,
            'opening_date': self.opening_date,
            'opening_status': self.opening_status,
            'room_count': self.room_count,
            'property_type': self.property_type,
            'management_company': self.management_company,
            'developer': self.developer,
            'owner': self.owner,
            'contact_name': self.contact_name,
            'contact_title': self.contact_title,
            'contact_email': self.contact_email,
            'contact_phone': self.contact_phone,
            'confidence_score': self.confidence_score,
            'qualification_score': self.qualification_score,
            'source_urls': self.source_urls,
            'source_names': self.source_names,
            'merged_from_count': self.merged_from_count,
            'first_seen': self.first_seen,
            'last_updated': self.last_updated,
        }


# =============================================================================
# SMART DEDUPLICATOR
# =============================================================================

class SmartDeduplicator:
    """
    Intelligent deduplication service for hotel leads.
    
    Features:
    - Fuzzy name matching with configurable threshold
    - Location-aware matching
    - Smart merge keeping best data from each source
    - Brand normalization
    - Source URL tracking
    """
    
    # Similarity thresholds
    NAME_SIMILARITY_THRESHOLD = 0.75  # 75% similar names = potential match
    LOCATION_BOOST = 0.15  # Add 15% if location matches
    BRAND_BOOST = 0.10  # Add 10% if brand matches
    
    # Brand variations to normalize
    BRAND_ALIASES = {
        'four seasons': ['four seasons', 'fs', '4 seasons'],
        'ritz-carlton': ['ritz-carlton', 'ritz carlton', 'the ritz-carlton', 'ritz'],
        'st. regis': ['st. regis', 'st regis', 'saint regis'],
        'waldorf astoria': ['waldorf astoria', 'waldorf', 'the waldorf'],
        'conrad': ['conrad', 'conrad hotels'],
        'jw marriott': ['jw marriott', 'jw', 'j.w. marriott'],
        'w hotels': ['w hotels', 'w hotel', 'w'],
        'hilton': ['hilton', 'hilton hotels'],
        'marriott': ['marriott', 'marriott hotels'],
        'hyatt': ['hyatt', 'hyatt hotels'],
        'ihg': ['ihg', 'ihg hotels', 'intercontinental hotels group'],
        'six senses': ['six senses', '6 senses'],
        'aman': ['aman', 'amanresorts'],
        'rosewood': ['rosewood', 'rosewood hotels'],
        'mandarin oriental': ['mandarin oriental', 'mandarin'],
        'peninsula': ['peninsula', 'the peninsula'],
        'park hyatt': ['park hyatt'],
        'grand hyatt': ['grand hyatt'],
        'andaz': ['andaz'],
        'thompson': ['thompson', 'thompson hotels'],
        'edition': ['edition', 'the edition'],
        'autograph': ['autograph', 'autograph collection'],
        'curio': ['curio', 'curio collection'],
        'tribute': ['tribute', 'tribute portfolio'],
        'tapestry': ['tapestry', 'tapestry collection'],
        'graduate': ['graduate', 'graduate hotels'],
        'motto': ['motto', 'motto by hilton'],
        'canopy': ['canopy', 'canopy by hilton'],
        'tempo': ['tempo', 'tempo by hilton'],
        'spark': ['spark', 'spark by hilton'],
    }
    
    # State abbreviations
    STATE_ABBREVS = {
        'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
        'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
        'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
        'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
        'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
        'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
        'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
        'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
        'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
        'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
        'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
        'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
        'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
    }
    
    # Reverse mapping
    ABBREV_TO_STATE = {v: k.title() for k, v in STATE_ABBREVS.items()}
    
    def __init__(self, name_threshold: float = 0.75):
        self.name_threshold = name_threshold
        self._stats = {
            'total_input': 0,
            'duplicates_found': 0,
            'merges_performed': 0,
            'unique_output': 0
        }
    
    # =========================================================================
    # MAIN DEDUPLICATION
    # =========================================================================
    
    def deduplicate(self, leads: List[Any]) -> List[MergedLead]:
        """
        Deduplicate a list of leads using fuzzy matching.
        
        Args:
            leads: List of lead objects/dicts
        
        Returns:
            List of unique MergedLead objects
        """
        if not leads:
            return []
        
        self._stats['total_input'] = len(leads)
        
        # Convert all leads to a common format
        normalized_leads = [self._normalize_lead(lead) for lead in leads]
        
        # Group potential duplicates
        groups = self._group_similar_leads(normalized_leads)
        
        # Merge each group into a single lead
        merged_leads = []
        for group in groups:
            if len(group) > 1:
                merged = self._merge_leads(group)
                self._stats['merges_performed'] += 1
                self._stats['duplicates_found'] += len(group) - 1
            else:
                merged = group[0]
            merged_leads.append(merged)
        
        self._stats['unique_output'] = len(merged_leads)
        
        logger.info(f"🔄 Deduplication: {len(leads)} → {len(merged_leads)} unique leads")
        logger.info(f"   Found {self._stats['duplicates_found']} duplicates, performed {self._stats['merges_performed']} merges")
        
        return merged_leads
    
    # =========================================================================
    # NORMALIZATION
    # =========================================================================
    
    def _normalize_lead(self, lead: Any) -> MergedLead:
        """Convert any lead format to MergedLead"""
        # Handle dict
        if isinstance(lead, dict):
            data = lead
        # Handle dataclass/object
        elif hasattr(lead, '__dict__'):
            data = lead.__dict__ if not hasattr(lead, 'to_dict') else lead.to_dict()
        else:
            data = {}
        
        # Normalize state
        state = str(data.get('state', '') or '').strip()
        state = self._normalize_state(state)
        
        # Normalize country
        country = str(data.get('country', '') or '').strip()
        country = self._normalize_country(country)
        
        # Normalize brand
        brand = str(data.get('brand', '') or '').strip()
        brand = self._normalize_brand(brand)
        
        # Get source URL
        source_url = data.get('source_url', '') or ''
        source_name = data.get('source_name', '') or ''
        
        return MergedLead(
            hotel_name=str(data.get('hotel_name', '') or '').strip(),
            brand=brand,
            city=str(data.get('city', '') or '').strip(),
            state=state,
            country=country,
            opening_date=str(data.get('opening_date', '') or '').strip(),
            opening_status=str(data.get('opening_status', '') or '').strip(),
            room_count=int(data.get('room_count', 0) or 0),
            property_type=str(data.get('property_type', data.get('hotel_type', '')) or '').strip(),
            management_company=str(data.get('management_company', '') or '').strip(),
            developer=str(data.get('developer', '') or '').strip(),
            owner=str(data.get('owner', '') or '').strip(),
            contact_name=str(data.get('contact_name', '') or '').strip(),
            contact_title=str(data.get('contact_title', '') or '').strip(),
            contact_email=str(data.get('contact_email', '') or '').strip(),
            contact_phone=str(data.get('contact_phone', '') or '').strip(),
            confidence_score=float(data.get('confidence_score', 0) or 0),
            qualification_score=int(data.get('qualification_score', 0) or 0),
            source_urls=[source_url] if source_url else [],
            source_names=[source_name] if source_name else [],
            merged_from_count=1,
            first_seen=datetime.now().isoformat(),
            last_updated=datetime.now().isoformat(),
        )
    
    def _normalize_state(self, state: str) -> str:
        """Normalize state to full name"""
        state_lower = state.lower().strip()
        
        # If it's an abbreviation, convert to full name
        if state.upper() in self.ABBREV_TO_STATE:
            return self.ABBREV_TO_STATE[state.upper()]
        
        # If it's a full name, title case it
        if state_lower in self.STATE_ABBREVS:
            return state_lower.title()
        
        return state.title() if state else ""
    
    def _normalize_country(self, country: str) -> str:
        """Normalize country name"""
        country_lower = country.lower().strip()
        
        # US variations
        if country_lower in ['us', 'usa', 'united states', 'united states of america', 'america']:
            return 'USA'
        
        return country.title() if country else "USA"
    
    def _normalize_brand(self, brand: str) -> str:
        """Normalize brand name"""
        brand_lower = brand.lower().strip()
        
        for canonical, aliases in self.BRAND_ALIASES.items():
            if brand_lower in aliases:
                return canonical.title()
        
        return brand.title() if brand else ""
    
    # =========================================================================
    # SIMILARITY MATCHING
    # =========================================================================
    
    def _calculate_similarity(self, lead1: MergedLead, lead2: MergedLead) -> float:
        """
        Calculate similarity score between two leads.
        
        Returns:
            Float between 0 and 1 (1 = identical)
        """
        # Start with name similarity
        name_sim = self._name_similarity(lead1.hotel_name, lead2.hotel_name)
        
        # Location boost
        location_match = self._locations_match(lead1, lead2)
        if location_match:
            name_sim += self.LOCATION_BOOST
        
        # Brand boost
        if lead1.brand and lead2.brand and lead1.brand.lower() == lead2.brand.lower():
            name_sim += self.BRAND_BOOST
        
        return min(name_sim, 1.0)  # Cap at 1.0
    
    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate name similarity using multiple methods"""
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        n1 = self._normalize_name(name1)
        n2 = self._normalize_name(name2)
        
        # Exact match after normalization
        if n1 == n2:
            return 1.0
        
        # One contains the other
        if n1 in n2 or n2 in n1:
            return 0.9
        
        # Sequence matching (Levenshtein-like)
        seq_ratio = SequenceMatcher(None, n1, n2).ratio()
        
        # Token matching (word overlap)
        tokens1 = set(n1.split())
        tokens2 = set(n2.split())
        if tokens1 and tokens2:
            token_overlap = len(tokens1 & tokens2) / max(len(tokens1), len(tokens2))
        else:
            token_overlap = 0
        
        # Weighted average
        return (seq_ratio * 0.6) + (token_overlap * 0.4)
    
    def _normalize_name(self, name: str) -> str:
        """Normalize hotel name for comparison"""
        name = name.lower().strip()
        
        # Remove common suffixes
        suffixes = [
            'hotel', 'hotels', 'resort', 'resorts', 'spa', 'suites', 'suite',
            'inn', 'lodge', 'collection', 'by hilton', 'by marriott', 'by hyatt',
            'by ihg', 'a luxury collection', 'autograph collection'
        ]
        for suffix in suffixes:
            name = name.replace(suffix, '').strip()
        
        # Remove punctuation
        name = re.sub(r'[^\w\s]', '', name)
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _locations_match(self, lead1: MergedLead, lead2: MergedLead) -> bool:
        """Check if two leads have matching locations"""
        # State match
        if lead1.state and lead2.state:
            if lead1.state.lower() == lead2.state.lower():
                return True
        
        # City match
        if lead1.city and lead2.city:
            city1 = lead1.city.lower().strip()
            city2 = lead2.city.lower().strip()
            if city1 == city2 or city1 in city2 or city2 in city1:
                return True
        
        return False
    
    # =========================================================================
    # GROUPING
    # =========================================================================
    
    def _group_similar_leads(self, leads: List[MergedLead]) -> List[List[MergedLead]]:
        """Group similar leads together"""
        if not leads:
            return []
        
        # Track which leads have been grouped
        grouped = set()
        groups = []
        
        for i, lead1 in enumerate(leads):
            if i in grouped:
                continue
            
            # Start a new group with this lead
            group = [lead1]
            grouped.add(i)
            
            # Find all similar leads
            for j, lead2 in enumerate(leads):
                if j in grouped or i == j:
                    continue
                
                similarity = self._calculate_similarity(lead1, lead2)
                if similarity >= self.name_threshold:
                    group.append(lead2)
                    grouped.add(j)
                    logger.debug(f"   Match ({similarity:.2f}): '{lead1.hotel_name}' ≈ '{lead2.hotel_name}'")
            
            groups.append(group)
        
        return groups
    
    # =========================================================================
    # MERGING
    # =========================================================================
    
    def _merge_leads(self, leads: List[MergedLead]) -> MergedLead:
        """
        Merge multiple leads into one, keeping best data from each.
        
        Strategy:
        - For text fields: prefer non-empty, longer values
        - For numbers: prefer non-zero, larger values
        - For contacts: prefer complete info
        - For sources: combine all
        """
        if not leads:
            return MergedLead()
        
        if len(leads) == 1:
            return leads[0]
        
        # Sort by confidence (highest first) to prefer better sources
        leads = sorted(leads, key=lambda x: x.confidence_score, reverse=True)
        
        # Start with the highest confidence lead as base
        merged = MergedLead()
        
        # Merge text fields (prefer non-empty, longer)
        merged.hotel_name = self._best_text([l.hotel_name for l in leads])
        merged.brand = self._best_text([l.brand for l in leads])
        merged.city = self._best_text([l.city for l in leads])
        merged.state = self._best_text([l.state for l in leads])
        merged.country = self._best_text([l.country for l in leads]) or "USA"
        merged.opening_date = self._best_text([l.opening_date for l in leads])
        merged.opening_status = self._best_text([l.opening_status for l in leads])
        merged.property_type = self._best_text([l.property_type for l in leads])
        merged.management_company = self._best_text([l.management_company for l in leads])
        merged.developer = self._best_text([l.developer for l in leads])
        merged.owner = self._best_text([l.owner for l in leads])
        
        # Merge numeric fields (prefer non-zero, larger)
        merged.room_count = max([l.room_count for l in leads])
        merged.confidence_score = max([l.confidence_score for l in leads])
        merged.qualification_score = max([l.qualification_score for l in leads])
        
        # Merge contact info (prefer complete)
        merged.contact_name = self._best_text([l.contact_name for l in leads])
        merged.contact_title = self._best_text([l.contact_title for l in leads])
        merged.contact_email = self._best_text([l.contact_email for l in leads])
        merged.contact_phone = self._best_text([l.contact_phone for l in leads])
        
        # Combine all source URLs (unique)
        all_urls = []
        all_names = []
        for lead in leads:
            for url in lead.source_urls:
                if url and url not in all_urls:
                    all_urls.append(url)
            for name in lead.source_names:
                if name and name not in all_names:
                    all_names.append(name)
        
        merged.source_urls = all_urls
        merged.source_names = all_names
        merged.merged_from_count = len(leads)
        
        # Timestamps
        merged.first_seen = min([l.first_seen for l in leads if l.first_seen] or [datetime.now().isoformat()])
        merged.last_updated = datetime.now().isoformat()
        
        logger.info(f"   📎 Merged {len(leads)} leads → '{merged.hotel_name}' (from {len(all_urls)} sources)")
        
        return merged
    
    def _best_text(self, values: List[str]) -> str:
        """Select the best text value (non-empty, longer preferred)"""
        # Filter to non-empty values
        valid = [v for v in values if v and v.strip()]
        
        if not valid:
            return ""
        
        # Prefer longer values (usually more complete)
        return max(valid, key=len)
    
    # =========================================================================
    # DATABASE COMPARISON
    # =========================================================================
    
    async def filter_existing(
        self, 
        leads: List[MergedLead], 
        existing_leads: List[Dict]
    ) -> Tuple[List[MergedLead], List[MergedLead]]:
        """
        Filter out leads that already exist in database.
        
        Args:
            leads: New leads to check
            existing_leads: List of existing leads from database
        
        Returns:
            Tuple of (new_leads, existing_matches)
        """
        if not existing_leads:
            return leads, []
        
        # Normalize existing leads for comparison
        existing_normalized = [self._normalize_lead(e) for e in existing_leads]
        
        new_leads = []
        existing_matches = []
        
        for lead in leads:
            is_existing = False
            
            for existing in existing_normalized:
                similarity = self._calculate_similarity(lead, existing)
                if similarity >= self.name_threshold:
                    logger.info(f"   ⚠️  Already exists: '{lead.hotel_name}' ≈ '{existing.hotel_name}' ({similarity:.2f})")
                    existing_matches.append(lead)
                    is_existing = True
                    break
            
            if not is_existing:
                new_leads.append(lead)
        
        logger.info(f"🔍 Database check: {len(new_leads)} new, {len(existing_matches)} already exist")
        
        return new_leads, existing_matches
    
    def get_stats(self) -> Dict[str, int]:
        """Get deduplication statistics"""
        return self._stats.copy()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def deduplicate_leads(leads: List[Any], threshold: float = 0.75) -> List[MergedLead]:
    """
    Convenience function to deduplicate leads.
    
    Args:
        leads: List of lead objects/dicts
        threshold: Similarity threshold (0-1)
    
    Returns:
        List of unique MergedLead objects
    """
    dedup = SmartDeduplicator(name_threshold=threshold)
    return dedup.deduplicate(leads)


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    # Test with your Six Senses example
    test_leads = [
        {
            "hotel_name": "Six Senses South Carolina Islands",
            "brand": "Six Senses",
            "city": "",
            "state": "South Carolina",
            "country": "USA",
            "opening_date": "2026",
            "developer": "Whitestone Resources",
            "source_url": "https://www.hoteldive.com/news/six-senses-opens-south-carolina-resort/708852/"
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "city": "",
            "state": "Utah",
            "country": "US",
            "opening_date": "2026",
            "source_url": "https://www.hoteldive.com/topic/development/"
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "city": "Kanab",
            "state": "Utah",
            "country": "USA",
            "opening_date": "2029",
            "developer": "Canyon Global Partners",
            "management_company": "IHG Hotels & Resorts",
            "source_url": "https://www.hoteldive.com/news/ihg-six-senses-resort-utah/810467/"
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "city": "",
            "state": "Utah",
            "country": "US",
            "source_url": "https://www.hoteldive.com/topic/brands/"
        },
        {
            "hotel_name": "Six Senses RiverStone Estate",
            "brand": "Six Senses",
            "city": "Foxburg",
            "state": "Pennsylvania",
            "country": "USA",
            "opening_date": "2028",
            "room_count": 77,
            "management_company": "IHG Hotels & Resorts",
            "source_url": "https://www.hoteldive.com/news/six-senses-pennsylvania-expansion/727462/"
        },
        {
            "hotel_name": "Six Senses Telluride",
            "brand": "Six Senses",
            "city": "Telluride",
            "state": "Colorado",
            "country": "USA",
            "opening_date": "2028",
            "room_count": 77,
            "developer": "The Vault Home Collection",
            "source_url": "https://www.hoteldive.com/news/ihgs-six-senses-heads-to-colorado-mountains/725345/"
        },
    ]
    
    print("\n" + "="*70)
    print("SMART DEDUPLICATION TEST")
    print("="*70)
    
    print(f"\n📥 Input: {len(test_leads)} leads")
    for lead in test_leads:
        print(f"   • {lead['hotel_name']} ({lead.get('state', 'Unknown')})")
    
    # Run deduplication
    dedup = SmartDeduplicator()
    unique_leads = dedup.deduplicate(test_leads)
    
    print(f"\n📤 Output: {len(unique_leads)} unique leads")
    print("-"*70)
    
    for lead in unique_leads:
        print(f"\n🏨 {lead.hotel_name}")
        print(f"   Brand: {lead.brand}")
        print(f"   Location: {lead.city}, {lead.state}, {lead.country}")
        print(f"   Opening: {lead.opening_date}")
        print(f"   Rooms: {lead.room_count}")
        print(f"   Developer: {lead.developer}")
        print(f"   Management: {lead.management_company}")
        print(f"   Merged from: {lead.merged_from_count} sources")
        print(f"   Source URLs:")
        for url in lead.source_urls:
            print(f"      • {url}")
    
    print("\n" + "="*70)
    print("STATS:", dedup.get_stats())
    print("="*70)