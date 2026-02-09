"""
SMART LEAD HUNTER - INTELLIGENT DEDUPLICATION SERVICE V2
=========================================================
Fuzzy matching and smart merging of hotel leads.

CHANGES IN V2:
- Uses rapidfuzz (10x faster) with difflib fallback
- Simplified code
- Better key_insights preservation
- Async database comparison
- P-05: Unicode normalization in _clean_name() for cross-source matching

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
    from app.services.smart_deduplicator import SmartDeduplicator, deduplicate_leads
    
    dedup = SmartDeduplicator()
    unique_leads = dedup.deduplicate(raw_leads)
"""

import re
import logging
import unicodedata
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple, Any
from datetime import datetime

# Try rapidfuzz first (10x faster), fall back to difflib
try:
    from rapidfuzz import fuzz
    USING_RAPIDFUZZ = True
except ImportError:
    from difflib import SequenceMatcher
    USING_RAPIDFUZZ = False

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
    
    # Key insights from article - IMPORTANT for sales!
    key_insights: str = ""
    
    # Quality metrics
    confidence_score: float = 0.0
    qualification_score: int = 0
    
    # Source tracking
    source_url: str = ""  # Primary source
    source_name: str = ""  # Primary source name
    source_urls: List[str] = field(default_factory=list)
    source_extractions: Dict[str, Dict] = field(default_factory=dict)  # {url: {extracted_fields}}
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
            'key_insights': self.key_insights,
            'confidence_score': self.confidence_score,
            'qualification_score': self.qualification_score,
            'source_url': self.source_urls[0] if self.source_urls else self.source_url,
            'source_name': self.source_names[0] if self.source_names else self.source_name,
            'source_urls': self.source_urls,
            'source_extractions': self.source_extractions,
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
    
    Uses rapidfuzz for 10x faster fuzzy matching (falls back to difflib).
    """
    
    # Similarity thresholds
    DEFAULT_THRESHOLD = 0.75
    LOCATION_BOOST = 0.15
    BRAND_BOOST = 0.10
    
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
    ABBREV_TO_STATE = {v: k.title() for k, v in STATE_ABBREVS.items()}
    
    def __init__(self, threshold: float = DEFAULT_THRESHOLD):
        self.threshold = threshold
        self._stats = {
            'total_input': 0,
            'duplicates_found': 0,
            'merges_performed': 0,
            'unique_output': 0
        }
        
        if USING_RAPIDFUZZ:
            logger.info("Using rapidfuzz for fast fuzzy matching")
        else:
            logger.info("rapidfuzz not available, using difflib (slower)")
    
    # =========================================================================
    # MAIN API
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
        
        # Normalize all leads
        normalized = [self._normalize_lead(lead) for lead in leads]
        
        # Group similar leads
        groups = self._group_similar(normalized)
        
        # Merge each group
        merged = []
        for group in groups:
            if len(group) > 1:
                result = self._merge_group(group)
                self._stats['merges_performed'] += 1
                self._stats['duplicates_found'] += len(group) - 1
            else:
                result = group[0]
            merged.append(result)
        
        self._stats['unique_output'] = len(merged)
        
        logger.info(f"Deduplication: {len(leads)} -> {len(merged)} unique leads")
        logger.info(f"   Found {self._stats['duplicates_found']} duplicates")
        
        return merged
    
    async def filter_existing(
        self, 
        leads: List[MergedLead], 
        existing_leads: List[Dict]
    ) -> Tuple[List[MergedLead], List[MergedLead]]:
        """
        Filter out leads that already exist in database.
        
        Returns:
            Tuple of (new_leads, existing_matches)
        """
        if not existing_leads:
            return leads, []
        
        existing_normalized = [self._normalize_lead(e) for e in existing_leads]
        
        new_leads = []
        existing_matches = []
        
        for lead in leads:
            is_existing = False
            
            for existing in existing_normalized:
                similarity = self._calculate_similarity(lead, existing)
                if similarity >= self.threshold:
                    logger.info(f"   Already exists: '{lead.hotel_name}' ~ '{existing.hotel_name}' ({similarity:.2f})")
                    existing_matches.append(lead)
                    is_existing = True
                    break
            
            if not is_existing:
                new_leads.append(lead)
        
        logger.info(f"Database check: {len(new_leads)} new, {len(existing_matches)} already exist")
        
        return new_leads, existing_matches
    
    def get_stats(self) -> Dict[str, int]:
        """Get deduplication statistics"""
        return self._stats.copy()
    
    # =========================================================================
    # NORMALIZATION
    # =========================================================================
    
    def _normalize_lead(self, lead: Any) -> MergedLead:
        """Convert any lead format to MergedLead"""
        # Handle dict
        if isinstance(lead, dict):
            data = lead
        elif hasattr(lead, 'to_dict'):
            data = lead.to_dict()
        elif hasattr(lead, '__dict__'):
            data = lead.__dict__
        else:
            data = {}
        
        # Normalize state
        state = str(data.get('state', '') or '').strip()
        if state.upper() in self.ABBREV_TO_STATE:
            state = self.ABBREV_TO_STATE[state.upper()]
        elif state.lower() in self.STATE_ABBREVS:
            state = state.title()
        else:
            state = state.title() if state else ""
        
        # Normalize country
        country = str(data.get('country', '') or '').strip()
        if country.lower() in ['us', 'usa', 'united states', 'america']:
            country = 'USA'
        else:
            country = country.title() if country else "USA"
        
        # Get source info
        source_url = data.get('source_url', '') or ''
        source_name = data.get('source_name', '') or ''
        
        return MergedLead(
            hotel_name=str(data.get('hotel_name', '') or '').strip(),
            brand=str(data.get('brand', '') or '').strip(),
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
            key_insights=str(data.get('key_insights', '') or '').strip(),
            confidence_score=float(data.get('confidence_score', 0) or 0),
            qualification_score=int(data.get('qualification_score', 0) or 0),
            source_url=source_url,
            source_name=source_name,
            source_urls=[source_url] if source_url else [],
            source_extractions={source_url: {
            'hotel_name': data.get('hotel_name', ''),
            'city': data.get('city', ''),
            'state': data.get('state', ''),
            'room_count': data.get('room_count'),
            'opening_date': data.get('opening_date', ''),
            'brand': data.get('brand', ''),
            'key_insights': data.get('key_insights', '') or data.get('description', ''),
            }} if source_url else {},
            source_names=[source_name] if source_name else [],
            merged_from_count=1,
            first_seen=datetime.now().isoformat(),
            last_updated=datetime.now().isoformat(),
        )
    
    # =========================================================================
    # SIMILARITY
    # ========================================================================
        
    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate name similarity (0-1)"""
        if not name1 or not name2:
            return 0.0

        # Normalize names
        n1 = self._clean_name(name1)
        n2 = self._clean_name(name2)

        if n1 == n2:
            return 1.0

        if n1 in n2 or n2 in n1:
            return 0.9

        # Use rapidfuzz if available (returns 0-100)
        if USING_RAPIDFUZZ:
            # Take best of character ratio and token-set ratio
            # token_set_ratio handles reordering and extra words much better
            char_ratio = fuzz.ratio(n1, n2) / 100.0
            token_ratio = fuzz.token_set_ratio(n1, n2) / 100.0
            return max(char_ratio, token_ratio)
        else:
            return SequenceMatcher(None, n1, n2).ratio()
    
    def _clean_name(self, name: str) -> str:
        """Normalize hotel name for comparison.
        
        P-05 FIX: Added NFKD Unicode normalization so visually similar
        characters from different scraped pages compare as equal:
          - "Hotel" vs "Hôtel"  (accented chars)
          - "Resort - Spa" vs "Resort — Spa"  (em dashes)
          - "fi" vs ligature char  (ligatures)
        Without this, the same hotel scraped from different sites with
        different Unicode encodings would be treated as two separate leads.
        """
        name = name.lower().strip()
        
        # P-05: Unicode NFKD normalization - decompose accented chars
        # then strip combining marks (accents, diacritics)
        name = unicodedata.normalize('NFKD', name)
        name = ''.join(c for c in name if not unicodedata.combining(c))
        
        # Remove common suffixes
        for suffix in ['hotel', 'hotels', 'resort', 'resorts', 'spa', 'suites', 
                       'suite', 'inn', 'lodge', 'collection', 'by hilton', 
                       'by marriott', 'by hyatt', 'by ihg']:
            name = name.replace(suffix, '').strip()
        
        # Remove punctuation
        name = re.sub(r'[^\w\s]', '', name)
        
        # Collapse whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _locations_match(self, lead1: MergedLead, lead2: MergedLead) -> bool:
        """Check if locations match"""
        if lead1.state and lead2.state:
            if lead1.state.lower() == lead2.state.lower():
                return True
        
        if lead1.city and lead2.city:
            c1, c2 = lead1.city.lower(), lead2.city.lower()
            if c1 == c2 or c1 in c2 or c2 in c1:
                return True
        
        return False
    
    
    def _locations_different(self, lead1: MergedLead, lead2: MergedLead) -> bool:
        """Check if locations are clearly different"""
        # Different states = definitely different
        if lead1.state and lead2.state:
            if lead1.state.lower() != lead2.state.lower():
                return True

        # Different cities only count as "different" if no state info
        # (same state + different city = could be same resort area)
        if not lead1.state and not lead2.state:
            if lead1.city and lead2.city:
                c1, c2 = lead1.city.lower().strip(), lead2.city.lower().strip()
                if c1 != c2 and c1 not in c2 and c2 not in c1:
                    return True

        return False
    
    
    def _calculate_similarity(self, lead1: MergedLead, lead2: MergedLead) -> float:
        """Calculate overall similarity between two leads"""
        # Start with name similarity
        sim = self._name_similarity(lead1.hotel_name, lead2.hotel_name)

        # Location penalty/boost
        if self._locations_different(lead1, lead2):
            sim *= 0.4  # Heavy penalty for different states
        elif self._locations_match(lead1, lead2):
            sim += self.LOCATION_BOOST
        else:
            # Same state but different city — small boost (resort areas span cities)
            if lead1.state and lead2.state and lead1.state.lower() == lead2.state.lower():
                sim += 0.05

        # Brand boost
        if lead1.brand and lead2.brand:
            if lead1.brand.lower() == lead2.brand.lower():
                sim += self.BRAND_BOOST

        return min(sim, 1.0)
    
    # =========================================================================
    # GROUPING & MERGING
    # =========================================================================
    
    def _group_similar(self, leads: List[MergedLead]) -> List[List[MergedLead]]:
        """Group similar leads together"""
        grouped = set()
        groups = []
        
        for i, lead1 in enumerate(leads):
            if i in grouped:
                continue
            
            group = [lead1]
            grouped.add(i)
            
            for j, lead2 in enumerate(leads):
                if j in grouped or i == j:
                    continue
                
                sim = self._calculate_similarity(lead1, lead2)
                if sim >= self.threshold:
                    group.append(lead2)
                    grouped.add(j)
                    logger.debug(f"   Match ({sim:.2f}): '{lead1.hotel_name}' ~ '{lead2.hotel_name}'")
            
            groups.append(group)
        
        return groups
    
    def _merge_group(self, leads: List[MergedLead]) -> MergedLead:
        """Merge a group of similar leads into one"""
        if len(leads) == 1:
            return leads[0]
        
        # Sort by confidence
        leads = sorted(leads, key=lambda x: x.confidence_score, reverse=True)
        
        def best_text(values: List[str]) -> str:
            """Pick best non-empty value (longer preferred)"""
            valid = [v for v in values if v and v.strip()]
            return max(valid, key=len) if valid else ""
        
        merged = MergedLead()
        
        # Text fields - prefer longer values
        merged.hotel_name = best_text([l.hotel_name for l in leads])
        merged.brand = best_text([l.brand for l in leads])
        merged.city = best_text([l.city for l in leads])
        merged.state = best_text([l.state for l in leads])
        merged.country = best_text([l.country for l in leads]) or "USA"
        merged.opening_date = best_text([l.opening_date for l in leads])
        merged.opening_status = best_text([l.opening_status for l in leads])
        merged.property_type = best_text([l.property_type for l in leads])
        merged.management_company = best_text([l.management_company for l in leads])
        merged.developer = best_text([l.developer for l in leads])
        merged.owner = best_text([l.owner for l in leads])
        merged.contact_name = best_text([l.contact_name for l in leads])
        merged.contact_title = best_text([l.contact_title for l in leads])
        merged.contact_email = best_text([l.contact_email for l in leads])
        merged.contact_phone = best_text([l.contact_phone for l in leads])
        merged.key_insights = best_text([l.key_insights for l in leads])
        
        # Numeric fields - prefer max
        merged.room_count = max(l.room_count for l in leads)
        merged.confidence_score = max(l.confidence_score for l in leads)
        merged.qualification_score = max(l.qualification_score for l in leads)
        
        # Combine sources
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
        # Merge source extractions from all leads
        all_extractions = {}
        for lead in leads:
            for url, extraction in lead.source_extractions.items():
                if url and url not in all_extractions:
                    all_extractions[url] = extraction
        merged.source_extractions = all_extractions
        merged.source_names = all_names
        merged.source_url = all_urls[0] if all_urls else ""
        merged.source_name = all_names[0] if all_names else ""
        merged.merged_from_count = len(leads)
        
        # Timestamps
        merged.first_seen = min(l.first_seen for l in leads if l.first_seen)
        merged.last_updated = datetime.now().isoformat()
        
        logger.info(f"   Merged {len(leads)} -> '{merged.hotel_name}' (from {len(all_urls)} sources)")
        
        return merged


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def deduplicate_leads(leads: List[Any], threshold: float = 0.75) -> List[MergedLead]:
    """Convenience function to deduplicate leads"""
    return SmartDeduplicator(threshold=threshold).deduplicate(leads)


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    # Test with Six Senses example
    test_leads = [
        {
            "hotel_name": "Six Senses South Carolina Islands",
            "brand": "Six Senses",
            "state": "South Carolina",
            "opening_date": "2026",
            "source_url": "https://hoteldive.com/news/1"
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "state": "Utah",
            "opening_date": "2026",
            "source_url": "https://hoteldive.com/news/2"
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "city": "Kanab",
            "state": "Utah",
            "opening_date": "2029",
            "key_insights": "77 rooms planned, IHG management",
            "source_url": "https://hoteldive.com/news/3"
        },
        {
            "hotel_name": "Six Senses Camp Korongo Utah",
            "brand": "Six Senses",
            "state": "Utah",
            "source_url": "https://hoteldive.com/news/4"
        },
    ]
    
    print("\n" + "="*70)
    print("SMART DEDUPLICATION TEST")
    print("="*70)
    print(f"Using: {'rapidfuzz' if USING_RAPIDFUZZ else 'difflib'}")
    
    print(f"\nInput: {len(test_leads)} leads")
    for lead in test_leads:
        print(f"   - {lead['hotel_name']} ({lead.get('state', 'Unknown')})")
    
    # Run deduplication
    unique = deduplicate_leads(test_leads)
    
    print(f"\nOutput: {len(unique)} unique leads")
    print("-"*70)
    
    for lead in unique:
        print(f"\n  {lead.hotel_name}")
        print(f"   Location: {lead.city}, {lead.state}")
        print(f"   Opening: {lead.opening_date}")
        print(f"   Key Insights: {lead.key_insights[:50]}..." if lead.key_insights else "   Key Insights: None")
        print(f"   Merged from: {lead.merged_from_count} sources")
        for url in lead.source_urls:
            print(f"      - {url}")
    
    # P-05: Unicode normalization test
    print("\n" + "="*70)
    print("P-05: UNICODE NORMALIZATION TEST")
    print("="*70)
    
    dedup = SmartDeduplicator()
    
    # These should all clean to the same string
    test_names = [
        "Hotel & Residences Miami Beach",
        "H\u00f4tel & R\u00e9sidences Miami Beach",   # accented
        "Hotel \u0026 Residences Miami Beach",          # & entity
    ]
    print("\n_clean_name() results:")
    for name in test_names:
        cleaned = dedup._clean_name(name)
        print(f"   '{name}' -> '{cleaned}'")
    
    unicode_leads = [
        {
            "hotel_name": "H\u00f4tel & R\u00e9sidences Miami Beach",
            "city": "Miami Beach",
            "state": "FL",
            "source_url": "https://source1.com"
        },
        {
            "hotel_name": "Hotel & Residences Miami Beach",
            "city": "Miami Beach",
            "state": "FL",
            "source_url": "https://source2.com"
        },
    ]
    
    print(f"\nInput: {len(unicode_leads)} leads (same hotel, different Unicode)")
    for lead in unicode_leads:
        print(f"   - {lead['hotel_name']}")
    
    unicode_unique = deduplicate_leads(unicode_leads)
    
    print(f"\nOutput: {len(unicode_unique)} unique leads")
    expected = 1
    status = "PASS" if len(unicode_unique) == expected else "FAIL"
    print(f"   {status} (expected {expected})")
    
    print("\n" + "="*70)