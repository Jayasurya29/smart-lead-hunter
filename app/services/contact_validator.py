"""
SMART LEAD HUNTER — Contact Validator & False Positive Filter
==============================================================
Detects and filters out false positive contacts from enrichment searches.

KEY PROBLEMS SOLVED:
1. Name collision: "The Nora Hotel" → finds "Nora Patten" (wrong person)
2. Corporate vs property: Finds VP at Hilton HQ instead of hotel-level GM
3. Wrong property: Finds GM at a different hotel in same chain
4. Stale contacts: Person no longer at that property

USAGE:
    from app.services.contact_validator import ContactValidator

    validator = ContactValidator()
    contacts = validator.validate_and_score(
        contacts=raw_contacts,
        hotel_name="The Nora Hotel",
        brand="BD Hotels",
        city="New York",
    )
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

from app.config.sap_title_classifier import BuyerTier, title_classifier

logger = logging.getLogger(__name__)


@dataclass
class ContactScore:
    """Detailed scoring breakdown for a contact."""

    contact: dict
    total_score: int = 0
    title_score: int = 0
    title_tier: Optional[BuyerTier] = None
    scope_score: int = 0
    scope_tag: str = "unknown"  # hotel_specific, chain_level, corporate, unknown
    name_collision_penalty: int = 0
    org_match_bonus: int = 0
    confidence: str = "low"  # low, medium, high
    flags: list = field(default_factory=list)
    reason: str = ""


class ContactValidator:
    """
    Validates and scores enrichment contacts using multiple signals.
    """

    def __init__(self):
        self.classifier = title_classifier

    def validate_and_score(
        self,
        contacts: list[dict],
        hotel_name: str,
        brand: Optional[str] = None,
        management_company: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
    ) -> list[ContactScore]:
        """
        Score and rank contacts, filtering out false positives.

        Returns list of ContactScore sorted by total_score (highest first).
        Contacts with total_score <= 0 should be discarded.
        """
        scored = []
        hotel_name_words = self._extract_name_words(hotel_name)

        for contact in contacts:
            score = self._score_contact(
                contact=contact,
                hotel_name=hotel_name,
                hotel_name_words=hotel_name_words,
                brand=brand,
                management_company=management_company,
                city=city,
                state=state,
                country=country,
            )
            scored.append(score)

        # Sort by total score descending
        scored.sort(key=lambda s: s.total_score, reverse=True)

        # Log results
        for s in scored:
            status = "✓ KEEP" if s.total_score > 0 else "✗ DROP"
            logger.info(
                f"  {status} | {s.contact.get('name', '?'):30} | "
                f"score={s.total_score:+3d} | tier={s.title_tier.name if s.title_tier else '?':20} | "
                f"scope={s.scope_tag:15} | conf={s.confidence} | "
                f"flags={s.flags}"
            )

        return scored

    def _score_contact(
        self,
        contact: dict,
        hotel_name: str,
        hotel_name_words: set,
        brand: Optional[str],
        management_company: Optional[str],
        city: Optional[str],
        state: Optional[str],
        country: Optional[str],
    ) -> ContactScore:
        """Score a single contact."""
        score = ContactScore(contact=contact)
        name = contact.get("name", "").strip()
        title = contact.get("title", "").strip()
        org = contact.get("organization", "").strip()

        # ── 1. TITLE CLASSIFICATION ──
        classification = self.classifier.classify(title)
        score.title_score = classification.score
        score.title_tier = classification.tier
        score.total_score += classification.score

        # ── 2. NAME COLLISION DETECTION ──
        collision_penalty = self._check_name_collision(name, hotel_name_words)
        score.name_collision_penalty = collision_penalty
        score.total_score += collision_penalty  # Will be negative
        if collision_penalty < 0:
            score.flags.append("name_collision")

        # ── 3. ORGANIZATION MATCH ──
        # Respect pre-existing scope from Gemini verification if hotel_specific
        pre_scope = contact.get("scope", "")
        gemini_confirmed = pre_scope == "hotel_specific"
        if gemini_confirmed:
            org_bonus = 15
            scope_tag = "hotel_specific"
        else:
            org_bonus, scope_tag = self._check_org_match(
                org=org,
                hotel_name=hotel_name,
                brand=brand,
                management_company=management_company,
            )
        score.org_match_bonus = org_bonus
        score.scope_tag = scope_tag
        score.total_score += org_bonus

        # ── 4. CROSS-REFERENCE TITLE + SCOPE ──
        # Management company contacts: are they property-level or corporate?
        if scope_tag == "management_company":
            scope_tag = self._resolve_mgmt_company_scope(title, classification.tier)
            score.scope_tag = scope_tag

        # Safety net: Even if org matched hotel_specific, corporate titles should be downgraded
        # BUT skip if Gemini already confirmed hotel_specific (Gemini has full context)
        if (
            scope_tag == "hotel_specific"
            and self._is_corporate_title(title)
            and not gemini_confirmed
        ):
            scope_tag = "chain_corporate"
            score.scope_tag = scope_tag
            score.flags.append("corporate_at_property_org")

        # ── 5. SCOPE SCORING ──
        scope_penalty = self._scope_penalty(scope_tag, classification.tier)
        score.scope_score = scope_penalty
        score.total_score += scope_penalty

        # ── 6. LINKEDIN VALIDATION ──
        if score.total_score >= 25:
            score.confidence = "high"
        elif score.total_score >= 10:
            score.confidence = "medium"
        else:
            score.confidence = "low"

        # ── 7. BUILD REASON ──
        score.reason = self._build_reason(score, classification)

        return score

    def _extract_name_words(self, hotel_name: str) -> set:
        """
        Extract significant words from hotel name for collision detection.
        Strips common hotel words like 'Hotel', 'Resort', 'The', etc.
        """
        if not hotel_name:
            return set()

        stop_words = {
            "the",
            "hotel",
            "resort",
            "spa",
            "by",
            "and",
            "&",
            "inn",
            "suites",
            "suite",
            "lodge",
            "residences",
            "collection",
            "beach",
            "grand",
            "royal",
            "plaza",
            "palace",
            "tower",
            "club",
            "house",
            "center",
            "centre",
        }

        words = set()
        for word in re.split(r"[\s\-&/]+", hotel_name.lower()):
            word = word.strip(".,;:'\"()[]")
            if word and len(word) > 2 and word not in stop_words:
                words.add(word)

        return words

    def _check_name_collision(self, contact_name: str, hotel_name_words: set) -> int:
        """
        Detect when a contact's name overlaps with the hotel name.
        E.g., "The Nora Hotel" search finds "Nora Patten" — false positive.

        Returns negative penalty if collision detected.
        """
        if not contact_name or not hotel_name_words:
            return 0

        contact_words = set()
        for word in contact_name.lower().split():
            word = word.strip(".,;:'\"()")
            if word and len(word) > 2:
                contact_words.add(word)

        # Check overlap
        overlap = contact_words & hotel_name_words
        if overlap:
            # Strong collision — contact name contains a distinctive hotel name word
            # This is a strong signal of a false positive search result
            logger.warning(
                f"Name collision detected: contact '{contact_name}' "
                f"overlaps with hotel name words: {overlap}"
            )
            return -30  # Heavy penalty � name collision must not pass

        return 0

    def _check_org_match(
        self,
        org: str,
        hotel_name: str,
        brand: Optional[str],
        management_company: Optional[str],
    ) -> tuple[int, str]:
        """
        Check if contact's organization matches the hotel, brand, or management company.
        Cross-references with title to distinguish property staff from corporate execs.

        Returns: (bonus_score, scope_tag)
        """
        if not org:
            return (0, "unknown")

        org_lower = org.lower().strip()
        hotel_lower = hotel_name.lower().strip()

        # Direct hotel match — best case
        if hotel_lower in org_lower or org_lower in hotel_lower:
            return (15, "hotel_specific")

        # Check if org contains significant hotel name words
        hotel_words = self._extract_name_words(hotel_name)
        org_words = set(org_lower.split())
        if len(hotel_words & org_words) >= 2:
            return (10, "hotel_specific")

        # Brand match
        if brand:
            brand_lower = brand.lower().strip()
            if brand_lower in org_lower or org_lower in brand_lower:
                return (5, "chain_level")

            # Check parent brand mapping
            from app.config.enrichment_config import BRAND_TO_PARENT

            parent = BRAND_TO_PARENT.get(brand_lower, "")
            if parent and parent.lower() in org_lower:
                return (5, "chain_level")

        # Management company match — distinguish property staff from corporate execs
        if management_company:
            mgmt_lower = management_company.lower().strip()
            if mgmt_lower in org_lower or org_lower in mgmt_lower:
                return (
                    8,
                    "management_company",
                )  # New scope tag — resolved later with title

        # No match at all — suspicious
        return (-5, "unrelated")

    def _scope_penalty(self, scope_tag: str, tier: BuyerTier) -> int:
        """
        Apply penalty for corporate/chain-level contacts vs. hotel-specific.
        Corporate contacts are less useful for property-level uniform sales.
        """
        if scope_tag == "hotel_specific":
            if tier.value <= 5:  # BuyerTier.TIER5_HR
                return 5
            return 0
        elif scope_tag == "management_company":
            # Unresolved — shouldn't reach here, but treat as chain_area
            return -3
        elif scope_tag == "chain_level":
            if tier in (BuyerTier.TIER1_UNIFORM_DIRECT, BuyerTier.TIER2_PURCHASING):
                return 0
            return -3
        elif scope_tag == "chain_corporate":
            return -15  # Heavy penalty — C-suite / corporate execs don't buy uniforms
        elif scope_tag == "unrelated":
            return -10
        elif scope_tag == "unknown":
            return -5
        return 0

    def _check_linkedin(
        self, linkedin_url: str, hotel_name: str, brand: Optional[str]
    ) -> int:
        """
        Check if LinkedIn profile URL or snippet mentions the hotel.
        """
        if not linkedin_url:
            return 0

        linkedin_lower = linkedin_url.lower()

        # Check for hotel name words in LinkedIn URL
        for word in self._extract_name_words(hotel_name):
            if word in linkedin_lower:
                return 5

        if brand and brand.lower() in linkedin_lower:
            return 3

        return 0

    def _is_corporate_title(self, title: str) -> bool:
        """Detect corporate/executive titles that are NOT property-level buyers."""
        if not title:
            return False
        title_lower = f" {title.lower().strip()} "

        # Property-level titles are NEVER corporate, even if they contain
        # words like "director" or "managing" that look corporate
        property_keywords = [
            "general manager",
            "hotel manager",
            "resort manager",
            "property manager",
            "operations manager",
            "director of operations",
            "director of housekeeping",
            "director of rooms",
            "director of purchasing",
            "director of procurement",
            "director of front office",
            "director of food",
            "director of f&b",
            "director of banquets",
            "director of catering",
            "executive housekeeper",
            "purchasing manager",
            "housekeeping manager",
            "front office manager",
            "uniform manager",
            "wardrobe manager",
            "laundry manager",
            "supply chain manager",
            "rooms division manager",
            "assistant general manager",
            "assistant director",
            "spa director",
            "director of spa",
        ]
        if any(kw in title_lower for kw in property_keywords):
            return False

        corporate_signals = [
            "ceo",
            "coo",
            "cfo",
            "cto",
            "cmo",
            "chief executive",
            "chief operating",
            "chief financial",
            "chairman",
            "chairwoman",
            "chairperson",
            "president",
            "vice president",
            "vp ",
            " svp ",
            "senior vice president",
            " evp ",
            "executive vice president",
            "investor",
            "board member",
            "board of directors",
            "founder",
            "co-founder",
            "cofounder",
            "owner",
            "partner",
            "managing partner",
            "regional director",
            "regional manager",
            "regional vp",
            "area director",
            "area manager",
            "area vp",
            "divisional",
            "division president",
            "head of development",
            "head of acquisitions",
            "development officer",
            "investment",
        ]
        return any(kw in title_lower for kw in corporate_signals)

    def _resolve_mgmt_company_scope(self, title: str, tier: BuyerTier) -> str:
        """
        Determine if a management company contact is property-level or corporate.

        Property-level roles at mgmt companies (hotel_specific):
          - General Manager, Director of Housekeeping, Purchasing Manager, etc.
          - These people work ON-SITE at the specific hotel

        Corporate roles at mgmt companies (chain_corporate):
          - CEO, SVP, VP of Development, Regional Director, etc.
          - These people work at HQ and oversee multiple properties
        """
        if self._is_corporate_title(title):
            return "chain_corporate"

        # Property-level operational roles — these people are at the hotel
        if tier in (
            BuyerTier.TIER1_UNIFORM_DIRECT,
            BuyerTier.TIER2_PURCHASING,
            BuyerTier.TIER3_GM_OPS,
            BuyerTier.TIER4_FB,
            BuyerTier.TIER5_HR,
        ):
            return "hotel_specific"

        # Unknown title at management company — could be either
        return "chain_area"

    def _build_reason(self, score: ContactScore, classification) -> str:
        """Build human-readable reason string."""
        parts = [classification.reason]

        if score.name_collision_penalty < 0:
            parts.append("⚠️ Name matches hotel name — possible false positive")

        if score.scope_tag == "hotel_specific":
            parts.append("✓ Works at this specific property")
        elif score.scope_tag == "chain_level":
            parts.append("ℹ️ Chain/brand level — may not be at this property")
        elif score.scope_tag == "unrelated":
            parts.append("⚠️ Organization doesn't match hotel or brand")

        if "linkedin_verified" in score.flags:
            parts.append("✓ LinkedIn profile references hotel/brand")

        return " | ".join(parts)

    def filter_and_rank(
        self,
        scored_contacts: list[ContactScore],
        min_score: int = 5,
        max_contacts: int = 5,
    ) -> list[ContactScore]:
        """
        Filter out low-quality contacts and return the best ones.

        Args:
            scored_contacts: Output from validate_and_score()
            min_score: Minimum total score to keep (default: 5)
            max_contacts: Maximum contacts to return (default: 5)

        Returns: Filtered and ranked list of ContactScore
        """
        # Filter by minimum score
        valid = [s for s in scored_contacts if s.total_score >= min_score]

        # Deduplicate by name (keep highest score)
        seen_names = set()
        deduped = []
        for s in valid:
            name_key = s.contact.get("name", "").lower().strip()
            if name_key and name_key not in seen_names:
                seen_names.add(name_key)
                deduped.append(s)

        return deduped[:max_contacts]

    def should_retry_search(
        self, scored_contacts: list[ContactScore]
    ) -> tuple[bool, str]:
        """
        Determine if we should retry the search with different queries.

        Returns: (should_retry, reason)
        """
        if not scored_contacts:
            return (True, "No contacts found at all")

        # Check if all contacts have name collisions
        collision_count = sum(1 for s in scored_contacts if "name_collision" in s.flags)
        if collision_count == len(scored_contacts):
            return (True, "All contacts have name collisions — likely false positives")

        # Check if all contacts are unrelated org
        unrelated_count = sum(1 for s in scored_contacts if s.scope_tag == "unrelated")
        if unrelated_count == len(scored_contacts):
            return (
                True,
                "No contacts matched hotel or brand — try different search terms",
            )

        # Check if no decision makers found
        dm_count = sum(
            1
            for s in scored_contacts
            if s.total_score > 0
            and s.title_tier
            in (
                BuyerTier.TIER1_UNIFORM_DIRECT,
                BuyerTier.TIER2_PURCHASING,
                BuyerTier.TIER3_GM_OPS,
            )
        )
        if dm_count == 0:
            return (True, "No decision makers found — try broader search")

        return (False, "")


# ═══════════════════════════════════════════════════════════════
# SMART SEARCH QUERY BUILDER
# ═══════════════════════════════════════════════════════════════


class SmartQueryBuilder:
    """
    Builds intelligent DuckDuckGo search queries that avoid the
    name-collision problem and leverage parent company knowledge.

    Instead of just: "The Nora Hotel" General Manager
    Now generates:
        1. "The Nora Hotel" "General Manager" OR "Director" site:linkedin.com
        2. "BD Hotels" "The Nora Hotel" staff OR team
        3. "The Nora Hotel" New York appoints OR hires OR names
    """

    def build_queries(
        self,
        hotel_name: str,
        brand: Optional[str] = None,
        management_company: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
        mode: str = "pre_opening",
        retry_attempt: int = 0,
    ) -> list[str]:
        """
        Generate search queries for contact discovery.

        Args:
            retry_attempt: 0=first try, 1+=retry with different strategy

        Returns: List of query strings for DuckDuckGo
        """
        queries = []
        location = self._build_location(city, state, country)

        if retry_attempt == 0:
            # ── FIRST ATTEMPT: Standard search ──

            # Query 1: Hotel name + key titles + LinkedIn
            title_terms = "General Manager OR Director OR Purchasing"
            queries.append(f"{hotel_name} {title_terms} site:linkedin.com")

            # Query 2: Hotel name + appointment news
            queries.append(f"{hotel_name} appoints OR hires OR names OR appointed")

            # Query 3: Brand/parent company + hotel name + staff
            parent = management_company or brand
            if parent:
                queries.append(f"{parent} {hotel_name} team OR staff OR leadership")

            # Query 4: Hotel name + location + GM
            if location:
                queries.append(f"{hotel_name} {location} General Manager OR Director")

            # Query 5-N: Targeted title-specific queries (SAP-proven buyer titles)
            location_str = self._build_location(city, state, country)
            targeted_titles = [
                "Director of Food and Beverage",
                "Assistant Director of Food and Beverage",
                "Restaurants General Manager",
                "Director of Housekeeping",
                "Executive Housekeeper",
                "Director of Rooms",
                "Purchasing Manager",
                "Director of Operations",
                "Resort Manager",
                "Front Office Manager",
            ]
            for tt in targeted_titles:
                queries.append(f"{hotel_name} {location_str} {tt}")

        elif retry_attempt == 1:
            # ── RETRY: Use parent company / management company ──

            parent = management_company or brand
            if parent:
                # Search by parent company + location
                queries.append(f"{parent} {hotel_name} site:linkedin.com")
                queries.append(f"{parent} {location} General Manager hotel")

            # Try hotel name + pre-opening
            queries.append(f"{hotel_name} pre-opening team OR leadership")

            # Try just the hotel name + key role (simpler query)
            queries.append(f"{hotel_name} housekeeping director OR purchasing manager")

        elif retry_attempt >= 2:
            # ── LAST RESORT: Very broad searches ──
            if brand:
                queries.append(f"{brand} {location} hotel opening 2026")
            queries.append(f"{hotel_name} hotel staff")

        return queries

    def _build_location(self, city, state, country) -> str:
        parts = []
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        if country and country.upper() not in ("USA", "US", "UNITED STATES"):
            parts.append(country)
        return ", ".join(parts)


# Module-level instances
contact_validator = ContactValidator()
query_builder = SmartQueryBuilder()
