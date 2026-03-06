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

CHANGES IN V2.1:
- Multi-bucket keys: hotels land in multiple buckets to catch name variations
- Location word stripping: "Grand Hyatt Miami Beach" vs "Hilton Miami Beach" no longer match
- Brand mismatch penalty: different brands = different hotels
- State normalization: "Florida" and "FL" match correctly
- Expanded suffix stripping: Autograph Collection, Auberge, etc.
- Smart hotel name merge: picks clean mid-length name, not longest bloated one

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
from typing import List, Dict, Tuple, Any
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
    source_extractions: Dict[str, Dict] = field(
        default_factory=dict
    )  # {url: {extracted_fields}}
    source_names: List[str] = field(default_factory=list)
    merged_from_count: int = 1

    # Timestamps
    first_seen: str = ""
    last_updated: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON/database"""
        return {
            "hotel_name": self.hotel_name,
            "brand": self.brand,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "opening_date": self.opening_date,
            "opening_status": self.opening_status,
            "room_count": self.room_count,
            "property_type": self.property_type,
            "management_company": self.management_company,
            "developer": self.developer,
            "owner": self.owner,
            "contact_name": self.contact_name,
            "contact_title": self.contact_title,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "key_insights": self.key_insights,
            "confidence_score": self.confidence_score,
            "qualification_score": self.qualification_score,
            "source_url": self.source_urls[0] if self.source_urls else self.source_url,
            "source_name": self.source_names[0]
            if self.source_names
            else self.source_name,
            "source_urls": self.source_urls,
            "source_extractions": self.source_extractions,
            "source_names": self.source_names,
            "merged_from_count": self.merged_from_count,
            "first_seen": self.first_seen,
            "last_updated": self.last_updated,
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
        "alabama": "AL",
        "alaska": "AK",
        "arizona": "AZ",
        "arkansas": "AR",
        "california": "CA",
        "colorado": "CO",
        "connecticut": "CT",
        "delaware": "DE",
        "florida": "FL",
        "georgia": "GA",
        "hawaii": "HI",
        "idaho": "ID",
        "illinois": "IL",
        "indiana": "IN",
        "iowa": "IA",
        "kansas": "KS",
        "kentucky": "KY",
        "louisiana": "LA",
        "maine": "ME",
        "maryland": "MD",
        "massachusetts": "MA",
        "michigan": "MI",
        "minnesota": "MN",
        "mississippi": "MS",
        "missouri": "MO",
        "montana": "MT",
        "nebraska": "NE",
        "nevada": "NV",
        "new hampshire": "NH",
        "new jersey": "NJ",
        "new mexico": "NM",
        "new york": "NY",
        "north carolina": "NC",
        "north dakota": "ND",
        "ohio": "OH",
        "oklahoma": "OK",
        "oregon": "OR",
        "pennsylvania": "PA",
        "rhode island": "RI",
        "south carolina": "SC",
        "south dakota": "SD",
        "tennessee": "TN",
        "texas": "TX",
        "utah": "UT",
        "vermont": "VT",
        "virginia": "VA",
        "washington": "WA",
        "west virginia": "WV",
        "wisconsin": "WI",
        "wyoming": "WY",
        "district of columbia": "DC",
    }
    ABBREV_TO_STATE = {v: k.title() for k, v in STATE_ABBREVS.items()}

    def __init__(self, threshold: float = DEFAULT_THRESHOLD):
        self.threshold = threshold
        self._stats = {
            "total_input": 0,
            "duplicates_found": 0,
            "merges_performed": 0,
            "unique_output": 0,
        }

        if USING_RAPIDFUZZ:
            logger.info("Using rapidfuzz for fast fuzzy matching")
        else:
            logger.info("rapidfuzz not available, using difflib (slower)")

    @staticmethod
    def _normalize_state(state: str) -> str:
        s = state.lower().strip()
        abbrev = SmartDeduplicator.STATE_ABBREVS.get(s)
        if abbrev:
            return abbrev.lower()
        return s[:2] if len(s) >= 2 else s

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

        self._stats["total_input"] = len(leads)

        # Normalize all leads
        normalized = [self._normalize_lead(lead) for lead in leads]

        # Group similar leads
        groups = self._group_similar(normalized)

        # Merge each group
        merged = []
        for group in groups:
            if len(group) > 1:
                result = self._merge_group(group)
                self._stats["merges_performed"] += 1
                self._stats["duplicates_found"] += len(group) - 1
            else:
                result = group[0]
            merged.append(result)

        self._stats["unique_output"] = len(merged)

        logger.info(f"Deduplication: {len(leads)} -> {len(merged)} unique leads")
        logger.info(f"   Found {self._stats['duplicates_found']} duplicates")

        return merged

    async def filter_existing(
        self, leads: List[MergedLead], existing_leads: List[Dict]
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
                    logger.info(
                        f"   Already exists: '{lead.hotel_name}' ~ '{existing.hotel_name}' ({similarity:.2f})"
                    )
                    existing_matches.append(lead)
                    is_existing = True
                    break

            if not is_existing:
                new_leads.append(lead)

        logger.info(
            f"Database check: {len(new_leads)} new, {len(existing_matches)} already exist"
        )

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
        elif hasattr(lead, "to_dict"):
            data = lead.to_dict()
        elif hasattr(lead, "__dict__"):
            data = lead.__dict__
        else:
            data = {}

        # Normalize state
        state = str(data.get("state", "") or "").strip()
        if state.upper() in self.ABBREV_TO_STATE:
            state = self.ABBREV_TO_STATE[state.upper()]
        elif state.lower() in self.STATE_ABBREVS:
            state = state.title()
        else:
            state = state.title() if state else ""

        # Normalize country
        country = str(data.get("country", "") or "").strip()
        if country.lower() in ["us", "usa", "united states", "america"]:
            country = "USA"
        else:
            country = country.title() if country else "USA"

        # Get source info
        source_url = data.get("source_url", "") or ""
        source_name = data.get("source_name", "") or ""

        return MergedLead(
            hotel_name=str(data.get("hotel_name", "") or "").strip(),
            brand=str(data.get("brand", "") or "").strip(),
            city=str(data.get("city", "") or "").strip(),
            state=state,
            country=country,
            opening_date=str(data.get("opening_date", "") or "").strip(),
            opening_status=str(data.get("opening_status", "") or "").strip(),
            room_count=int(data.get("room_count", 0) or 0),
            property_type=str(
                data.get("property_type", data.get("hotel_type", "")) or ""
            ).strip(),
            management_company=str(data.get("management_company", "") or "").strip(),
            developer=str(data.get("developer", "") or "").strip(),
            owner=str(data.get("owner", "") or "").strip(),
            contact_name=str(data.get("contact_name", "") or "").strip(),
            contact_title=str(data.get("contact_title", "") or "").strip(),
            contact_email=str(data.get("contact_email", "") or "").strip(),
            contact_phone=str(data.get("contact_phone", "") or "").strip(),
            key_insights=str(data.get("key_insights", "") or "").strip(),
            confidence_score=float(data.get("confidence_score", 0) or 0),
            qualification_score=int(data.get("qualification_score", 0) or 0),
            source_url=source_url,
            source_name=source_name,
            source_urls=[source_url] if source_url else [],
            source_extractions={
                source_url: {
                    "hotel_name": data.get("hotel_name", ""),
                    "city": data.get("city", ""),
                    "state": data.get("state", ""),
                    "room_count": data.get("room_count"),
                    "opening_date": data.get("opening_date", ""),
                    "brand": data.get("brand", ""),
                    "key_insights": data.get("key_insights", "")
                    or data.get("description", ""),
                }
            }
            if source_url
            else {},
            source_names=[source_name] if source_name else [],
            merged_from_count=1,
            first_seen=datetime.now().isoformat(),
            last_updated=datetime.now().isoformat(),
        )

    # =========================================================================
    # SIMILARITY
    # =========================================================================

    def _name_similarity(self, name1: str, name2: str, lead1=None, lead2=None) -> float:
        """Calculate name similarity (0-1), stripping shared location words."""
        if not name1 or not name2:
            return 0.0

        n1 = self._clean_name(name1)
        n2 = self._clean_name(name2)

        # Strip location words that inflate similarity between different hotels
        if lead1 and lead2:
            location_words = set()
            for loc_field in [lead1.city, lead1.state, lead2.city, lead2.state]:
                if loc_field:
                    for word in loc_field.lower().split():
                        if len(word) > 2:
                            location_words.add(word)
            for word in location_words:
                n1 = n1.replace(word, "").strip()
                n2 = n2.replace(word, "").strip()
            n1 = " ".join(n1.split())
            n2 = " ".join(n2.split())

            # For same-brand hotels, also strip brand words to compare distinctive parts
            # "Hilton Miami Beach" vs "Hilton Miami Airport" → "hilton" vs "hilton airport"
            # after location strip → strip brand → "" vs "airport" → low score
            brand1 = (lead1.brand or "").lower().strip()
            brand2 = (lead2.brand or "").lower().strip()
            if brand1 and brand2 and brand1 == brand2:
                brand_clean = re.sub(r"[^\w\s]", "", brand1)
                brand_words = set(brand_clean.split())
                n1_stripped = " ".join(
                    w for w in n1.split() if w not in brand_words
                ).strip()
                n2_stripped = " ".join(
                    w for w in n2.split() if w not in brand_words
                ).strip()

                # Both empty after stripping = names were identical brand-only → same hotel
                if not n1_stripped and not n2_stripped:
                    return 1.0
                # One empty, other has distinctive words → different hotel
                if not n1_stripped or not n2_stripped:
                    return 0.3
                # Both have distinctive words → compare those instead
                n1 = n1_stripped
                n2 = n2_stripped

        if not n1 or not n2:
            return 0.0
        if n1 == n2:
            return 1.0
        if n1 in n2 or n2 in n1:
            return 0.9

        if USING_RAPIDFUZZ:
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
        name = unicodedata.normalize("NFKD", name)
        name = "".join(c for c in name if not unicodedata.combining(c))

        # Normalize & to and before suffix stripping
        name = name.replace("&", "and")

        # Normalize dashes (em dash, en dash → hyphen)
        name = name.replace("\u2013", "-").replace("\u2014", "-")

        # Remove common suffixes — longest first to avoid partial matches
        for suffix in [
            "an autograph collection all-inclusive resort - adults only",
            "an autograph collection all-inclusive resort",
            "a singletread inn",
            "a single thread inn",
            "a viceroy resort",
            "auberge resorts collection",
            "autograph collection",
            "auberge collection",
            "hotel and residences",
            "and residences",
            "jdv by hyatt",
            "by hilton",
            "by marriott",
            "by hyatt",
            "by ihg",
            "residences",
            "all-inclusive",
            "adults only",
            "collection",
            "hotel",
            "hotels",
            "resort",
            "resorts",
            "spa",
            "suites",
            "suite",
            "lodge",
            "club",
            "inn",
        ]:
            name = name.replace(suffix, "").strip()

        # Remove punctuation
        name = re.sub(r"[^\w\s]", "", name)

        # Collapse whitespace
        name = " ".join(name.split())

        return name

    def _locations_match(self, lead1: MergedLead, lead2: MergedLead) -> bool:
        """Check if locations match"""
        if lead1.state and lead2.state:
            if SmartDeduplicator._normalize_state(
                lead1.state
            ) == SmartDeduplicator._normalize_state(lead2.state):
                return True

        if lead1.city and lead2.city:
            c1, c2 = lead1.city.lower(), lead2.city.lower()
            if c1 == c2 or c1 in c2 or c2 in c1:
                return True

        return False

    def _locations_different(self, lead1: MergedLead, lead2: MergedLead) -> bool:
        """Check if locations are clearly different"""
        # Different states = definitely different (only if BOTH have states)
        if lead1.state and lead2.state:
            if SmartDeduplicator._normalize_state(
                lead1.state
            ) != SmartDeduplicator._normalize_state(lead2.state):
                return True

        # If one has no state, check city overlap — don't assume different
        if (lead1.state and not lead2.state) or (lead2.state and not lead1.state):
            if lead1.city and lead2.city:
                c1, c2 = lead1.city.lower().strip(), lead2.city.lower().strip()
                if c1 == c2 or c1 in c2 or c2 in c1:
                    return False  # Same city, one missing state — not different
            return False  # Can't determine — don't penalize

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
        # Start with name similarity (with location stripping)
        sim = self._name_similarity(lead1.hotel_name, lead2.hotel_name, lead1, lead2)

        # Location penalty/boost
        if self._locations_different(lead1, lead2):
            sim *= 0.4  # Heavy penalty for different states
        elif self._locations_match(lead1, lead2):
            sim += self.LOCATION_BOOST
        else:
            # Same state but different city — small boost (resort areas span cities)
            if (
                lead1.state
                and lead2.state
                and SmartDeduplicator._normalize_state(lead1.state)
                == SmartDeduplicator._normalize_state(lead2.state)
            ):
                sim += 0.05

        # Brand boost/penalty
        if lead1.brand and lead2.brand:
            if lead1.brand.lower() == lead2.brand.lower():
                sim += self.BRAND_BOOST
            else:
                sim -= 0.15  # Different brands = likely different hotels

        return min(sim, 1.0)

    # =========================================================================
    # GROUPING & MERGING
    # =========================================================================

    @staticmethod
    def _bucket_keys(lead: "MergedLead") -> list[str]:
        """Generate multiple bucket keys to catch name variations.

        Hotels like "Dolly Parton's SongTeller" vs "SongTeller" need
        to land in overlapping buckets to be compared.
        """
        name = (lead.hotel_name or "").lower().strip()
        state = SmartDeduplicator._normalize_state(lead.state or "unknown")

        keys = set()

        # Generate key from raw name
        variants = [name]
        # Remove common prefixes and generate key from each variant
        for prefix in ("the ", "hotel ", "a "):
            if name.startswith(prefix):
                variants.append(name[len(prefix) :])

        # Also generate keys from each significant word (>3 chars)
        # This catches "Dolly Parton's SongTeller" via "song" key
        # and "SongTeller Hotel" also via "song" key
        words = name.split()
        for word in words:
            clean_word = re.sub(r"[^\w]", "", word)
            if len(clean_word) > 3 and clean_word not in (
                "hotel",
                "hotels",
                "resort",
                "resorts",
                "suite",
                "suites",
                "collection",
            ):
                keys.add(f"{clean_word[:4]}|{state}")

        for v in variants:
            v = v.strip()
            if v:
                keys.add(f"{v[:4].ljust(4, '_')}|{state}")

        # Also bucket by city for cross-state matching (Caribbean, mixed state formats)
        city = (lead.city or "").lower().strip()
        if city:
            city_key = re.sub(r"[^\w]", "", city)[:4]
            if len(city_key) >= 3:
                keys.add(f"{city_key}|city")

        return list(keys)

    def _group_similar(self, leads: List[MergedLead]) -> List[List[MergedLead]]:
        """Group similar leads using multi-key bucketing + union-find.

        Union-find solves the ordering problem with multi-bucket grouping:
        a lead in buckets [A, B] won't get stuck as a singleton in bucket A
        before being compared to its match in bucket B.

        Steps:
        1. Bucket leads (each lead in multiple buckets via _bucket_keys)
        2. Compare all pairs within each bucket, union matching pairs
        3. Build groups from connected components
        """
        from collections import defaultdict

        n = len(leads)
        if n == 0:
            return []

        # Union-Find data structure
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]  # path compression
                x = parent[x]
            return x

        def union(x: int, y: int) -> None:
            rx, ry = find(x), find(y)
            if rx != ry:
                parent[rx] = ry

        # Step 1: Bucket leads — each lead can appear in multiple buckets
        buckets: dict = defaultdict(list)
        for i, lead in enumerate(leads):
            for key in self._bucket_keys(lead):
                buckets[key].append((i, lead))

        # Step 2: Compare all pairs within each bucket (deduplicated)
        compared: set = set()
        for bucket_leads in buckets.values():
            for a_idx, (i, lead1) in enumerate(bucket_leads):
                for b_idx, (j, lead2) in enumerate(bucket_leads):
                    if i >= j:
                        continue
                    pair = (i, j)
                    if pair in compared:
                        continue
                    compared.add(pair)

                    sim = self._calculate_similarity(lead1, lead2)
                    if sim >= self.threshold:
                        union(i, j)
                        logger.debug(
                            f"   Match ({sim:.2f}): '{lead1.hotel_name}' ~ '{lead2.hotel_name}'"
                        )

        # Step 3: Build groups from union-find components
        group_map: dict = defaultdict(list)
        for i in range(n):
            group_map[find(i)].append(leads[i])

        return list(group_map.values())

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

        # Hotel name — prefer mid-length (not too short, not bloated with suffixes)
        name_options = [
            ld.hotel_name for ld in leads if ld.hotel_name and ld.hotel_name.strip()
        ]
        if name_options:
            # Score by length closeness to 40 chars (sweet spot)
            merged.hotel_name = min(name_options, key=lambda n: abs(len(n) - 40))
        else:
            merged.hotel_name = ""

        # Text fields - prefer longer values
        merged.brand = best_text([ld.brand for ld in leads])
        merged.city = best_text([ld.city for ld in leads])
        merged.state = best_text([ld.state for ld in leads])
        merged.country = best_text([ld.country for ld in leads]) or "USA"
        merged.opening_date = best_text([ld.opening_date for ld in leads])
        merged.opening_status = best_text([ld.opening_status for ld in leads])
        merged.property_type = best_text([ld.property_type for ld in leads])
        merged.management_company = best_text([ld.management_company for ld in leads])
        merged.developer = best_text([ld.developer for ld in leads])
        merged.owner = best_text([ld.owner for ld in leads])
        merged.contact_name = best_text([ld.contact_name for ld in leads])
        merged.contact_title = best_text([ld.contact_title for ld in leads])
        merged.contact_email = best_text([ld.contact_email for ld in leads])
        merged.contact_phone = best_text([ld.contact_phone for ld in leads])
        merged.key_insights = best_text([ld.key_insights for ld in leads])

        # Numeric fields - prefer max
        merged.room_count = max(ld.room_count for ld in leads)
        merged.confidence_score = max(ld.confidence_score for ld in leads)
        merged.qualification_score = max(ld.qualification_score for ld in leads)

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
        merged.first_seen = min(ld.first_seen for ld in leads if ld.first_seen)
        merged.last_updated = datetime.now().isoformat()

        logger.info(
            f"   Merged {len(leads)} -> '{merged.hotel_name}' (from {len(all_urls)} sources)"
        )

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
            "source_url": "https://hoteldive.com/news/1",
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "state": "Utah",
            "opening_date": "2026",
            "source_url": "https://hoteldive.com/news/2",
        },
        {
            "hotel_name": "Six Senses Camp Korongo",
            "brand": "Six Senses",
            "city": "Kanab",
            "state": "Utah",
            "opening_date": "2029",
            "key_insights": "77 rooms planned, IHG management",
            "source_url": "https://hoteldive.com/news/3",
        },
        {
            "hotel_name": "Six Senses Camp Korongo Utah",
            "brand": "Six Senses",
            "state": "Utah",
            "source_url": "https://hoteldive.com/news/4",
        },
    ]

    print("\n" + "=" * 70)
    print("SMART DEDUPLICATION TEST")
    print("=" * 70)
    print(f"Using: {'rapidfuzz' if USING_RAPIDFUZZ else 'difflib'}")

    print(f"\nInput: {len(test_leads)} leads")
    for lead in test_leads:
        print(f"   - {lead['hotel_name']} ({lead.get('state', 'Unknown')})")

    # Run deduplication
    unique = deduplicate_leads(test_leads)

    print(f"\nOutput: {len(unique)} unique leads")
    print("-" * 70)

    for lead in unique:
        print(f"\n  {lead.hotel_name}")
        print(f"   Location: {lead.city}, {lead.state}")
        print(f"   Opening: {lead.opening_date}")
        print(
            f"   Key Insights: {lead.key_insights[:50]}..."
            if lead.key_insights
            else "   Key Insights: None"
        )
        print(f"   Merged from: {lead.merged_from_count} sources")
        for url in lead.source_urls:
            print(f"      - {url}")

    # P-05: Unicode normalization test
    print("\n" + "=" * 70)
    print("P-05: UNICODE NORMALIZATION TEST")
    print("=" * 70)

    dedup = SmartDeduplicator()

    # These should all clean to the same string
    test_names = [
        "Hotel Marais Residence",
        "H\u00f4tel Marais R\u00e9sidence",  # accented
        "Hotel \u0026 Residences Miami Beach",  # & entity (different test)
    ]
    print("\n_clean_name() results:")
    for name in test_names:
        cleaned = dedup._clean_name(name)
        print(f"   '{name}' -> '{cleaned}'")

    unicode_leads = [
        {
            "hotel_name": "H\u00f4tel Marais R\u00e9sidence",
            "city": "Paris",
            "state": "",
            "source_url": "https://source1.com",
        },
        {
            "hotel_name": "Hotel Marais Residence",
            "city": "Paris",
            "state": "",
            "source_url": "https://source2.com",
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

    # V2.1: False positive test — different hotels same location
    print("\n" + "=" * 70)
    print("V2.1: FALSE POSITIVE TEST (different hotels, same location)")
    print("=" * 70)

    false_positive_leads = [
        {
            "hotel_name": "Grand Hyatt Miami Beach",
            "brand": "Grand Hyatt",
            "city": "Miami Beach",
            "state": "FL",
            "source_url": "https://source1.com",
        },
        {
            "hotel_name": "Hilton Miami Beach",
            "brand": "Hilton",
            "city": "Miami Beach",
            "state": "FL",
            "source_url": "https://source2.com",
        },
        {
            "hotel_name": "Bulgari Hotel Miami Beach",
            "brand": "Bulgari",
            "city": "Miami Beach",
            "state": "FL",
            "source_url": "https://source3.com",
        },
    ]

    print(f"\nInput: {len(false_positive_leads)} leads (3 DIFFERENT hotels)")
    for lead in false_positive_leads:
        print(f"   - {lead['hotel_name']} ({lead['brand']})")

    fp_unique = deduplicate_leads(false_positive_leads)

    print(f"\nOutput: {len(fp_unique)} unique leads")
    expected = 3
    status = "PASS" if len(fp_unique) == expected else "FAIL"
    print(f"   {status} (expected {expected})")

    # V2.1: Name variation test — same hotel, different names
    print("\n" + "=" * 70)
    print("V2.1: NAME VARIATION TEST (same hotel, different names)")
    print("=" * 70)

    variation_leads = [
        {
            "hotel_name": "Dolly Parton's SongTeller Hotel",
            "city": "Nashville",
            "state": "Tennessee",
            "source_url": "https://source1.com",
        },
        {
            "hotel_name": "SongTeller Hotel",
            "city": "Nashville",
            "state": "TN",
            "source_url": "https://source2.com",
        },
    ]

    print(f"\nInput: {len(variation_leads)} leads (same hotel, different names)")
    for lead in variation_leads:
        print(f"   - {lead['hotel_name']} ({lead.get('state')})")

    var_unique = deduplicate_leads(variation_leads)

    print(f"\nOutput: {len(var_unique)} unique leads")
    expected = 1
    status = "PASS" if len(var_unique) == expected else "FAIL"
    print(f"   {status} (expected {expected})")

    print("\n" + "=" * 70)
