"""
SMART LEAD HUNTER — SAP-Trained Title Classification System
============================================================
Built from 3,929 real contacts across 1,018 business partners in SAP B1.
780 unique position titles mapped to 5 buyer tiers.

This replaces the old hardcoded title lists with a fuzzy-matching,
keyword-based classification engine trained on JA Uniforms' actual
customer contact data.

USAGE:
    from app.config.sap_title_classifier import TitleClassifier

    classifier = TitleClassifier()
    tier, score, reason = classifier.classify("Resort Manager")
    # → (1, 20, "GM/Operations - primary decision maker")

    # Get all searchable titles for a tier
    titles = classifier.get_search_titles(tier=1)
    # → ["General Manager", "Director of Housekeeping", ...]
"""

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional


class BuyerTier(IntEnum):
    """Contact buyer tier — higher tier = more likely to buy uniforms."""

    TIER1_UNIFORM_DIRECT = 1  # +20 pts — Housekeeping/Uniform/Laundry
    TIER2_PURCHASING = 2  # +15 pts — Purchasing/Procurement
    TIER3_GM_OPS = 3  # +10 pts — GM/Operations/Rooms
    TIER4_FB = 4  # +8 pts  — F&B (separate uniform category)
    TIER5_HR = 5  # +5 pts  — HR/People (sometimes involved)
    TIER6_FINANCE = 6  # +0 pts  — Finance/AP (invoice only)
    TIER7_IRRELEVANT = 7  # -5 pts  — PR/Marketing/Sales/IT/etc.
    UNKNOWN = 99  # +3 pts  — Unknown, assume some relevance


@dataclass
class TitleClassification:
    tier: BuyerTier
    score: int
    reason: str
    search_priority: int  # 1=highest priority for enrichment search
    is_decision_maker: bool


# ═══════════════════════════════════════════════════════════════
# KEYWORD PATTERNS — Derived from 780 SAP position titles
# Uses keyword fragments for fuzzy matching (handles abbreviations
# like "Dir", "Mgr", "Asst", "Coord", "Exec", etc.)
# ═══════════════════════════════════════════════════════════════

# Tier 1: Housekeeping / Uniform / Laundry / Stewarding
# SAP count: 81 contacts — These are the PRIMARY uniform buyers
TIER1_KEYWORDS = [
    "housekeep",  # Matches: housekeeping, housekeeper, housekeep mgr, etc.
    "uniform",  # Matches: uniform manager, uniform room, uniform supervisor, etc.
    "laundry",  # Matches: laundry manager, laundry mgr, laundry/valet
    "wardrobe",  # Matches: wardrobe manager, wardrobe room
    "steward",  # Matches: stewarding, executive steward, stewarding supervisor
    "linen",  # Matches: linen room, linen manager
]

# Tier 2: Purchasing / Procurement / Buyer / Supply Chain
# SAP count: 132 contacts — Controls vendor selection
TIER2_KEYWORDS = [
    "purchas",  # Matches: purchasing, purchaser, purchasing manager, etc.
    "procure",  # Matches: procurement, procurement manager, etc.
    "buyer",  # Matches: buyer, general buyer, international buyer
    "supply chain",  # Matches: supply chain manager, supply chain mgr
    "sourcing",  # Matches: sourcing contract manager, sourcing specialist
]

# Tier 3: GM / Operations / Property / Resort / Rooms
# SAP count: 251 contacts — Approves purchases
TIER3_KEYWORDS = [
    "general manager",
    " gm",  # Space prefix to avoid matching "program" etc. (also matches "asst gm")
    "gm ",  # Space suffix
    "gm/",  # Matches "gm/ap contact", "restaurant gm/the bazaar"
    "hotel manager",
    "resort manager",
    "property manager",
    "resident manager",
    "director of operations",
    "dir of ops",
    "dir. of operations",
    "director operations",
    "operations manager",
    "operations mgr",
    "vp of operations",
    "vp operations",
    "vp hotel operations",
    "director of rooms",
    "dir of rooms",
    "rooms director",
    "rooms division",
    "rooms manager",
    "rooms operations",
    "front office manager",
    "front office mgr",
    "front office gm",
    "director of front office",
    "dir of front office",
    "assistant general manager",
    "asst general manager",
    "asst. general manager",
    "asst gm",
    "asst. gm",
    "agm",
    "managing director",
    "pre-opening",
    "task force",
]

# Tier 4: F&B — Separate uniform category (chef coats, restaurant staff)
TIER4_FB_KEYWORDS = [
    "food & beverage",
    "food and beverage",
    "f&b",
    "f & b",
    "food & bev",
    "executive chef",
    "exec chef",
    "chef de cuisine",
    "banquet",
    "catering",
    "restaurant manager",
    "dir of restaurant",
    "director of restaurant",
]

# Tier 5: HR — Sometimes involved in uniform programs
TIER5_HR_KEYWORDS = [
    "human resource",
    "hr ",
    " hr",
    "hr/",
    "people",
    "talent",
    "training",
]

# Tier 6: Finance/AP — Invoice contact only, NOT a buyer
TIER6_FINANCE_KEYWORDS = [
    "accounts payable",
    "account payable",
    "ap clerk",
    "ap contact",
    "ap manager",
    "controller",
    "accounting",
    "accountant",
    "accounts receivable",
    "finance",
    "cfo",
    "bookkeep",
    "payroll",
    "billing",
    "invoic",
]

# Tier 7: Irrelevant — Not involved in uniform purchasing
TIER7_IRRELEVANT_KEYWORDS = [
    "marketing",
    "public relation",
    "pr manager",
    "communications",
    "social media",
    "digital",
    "investor relations",
    "information technology",
    " it ",
    "it director",
    "it manager",
    "software",
    "developer",  # Note: in context of hotel developer, handled separately
    "security",
    "chief engineer",
    "engineering",
    "maintenance",
    "valet",
    "concierge",
    "bellman",
    "bell captain",
    "spa director",
    "spa manager",
    "fitness",
    "recreation",
    "lifeguard",
    "pool ",
    "golf",
    "sales rep",
    "sales manager",
    "sales director",
    "revenue manager",
    "reservations",
    "night audit",
    "guest service",  # Front desk staff, not decision maker
    "front desk agent",
    "front desk supervisor",
    "parking",
    "loss prevention",
]

# C-Suite / Corporate — Too high level for property-level uniform purchasing
CORPORATE_KEYWORDS = [
    "ceo",
    "president",
    "chairman",
    "chief executive",
    "chief operating officer",
    "coo",
    "svp",
    "senior vice president",
    "evp",
    "executive vice president",
    "regional manager",
    "regional director",
    "area manager",
    "area director",
    "regional vice president",
    "regional vp",
    "divisional",
]


class TitleClassifier:
    """
    Classifies hospitality job titles into buyer tiers based on
    SAP B1 contact data from JA Uniforms' 1,018 business partners.
    """

    # Score awarded per tier (matches SAP analysis recommendation)
    TIER_SCORES = {
        BuyerTier.TIER1_UNIFORM_DIRECT: 20,
        BuyerTier.TIER2_PURCHASING: 15,
        BuyerTier.TIER3_GM_OPS: 10,
        BuyerTier.TIER4_FB: 8,
        BuyerTier.TIER5_HR: 5,
        BuyerTier.TIER6_FINANCE: 0,
        BuyerTier.TIER7_IRRELEVANT: -5,
        BuyerTier.UNKNOWN: 3,
    }

    # Search priority (lower = searched first in enrichment)
    TIER_SEARCH_PRIORITY = {
        BuyerTier.TIER1_UNIFORM_DIRECT: 1,
        BuyerTier.TIER2_PURCHASING: 2,
        BuyerTier.TIER3_GM_OPS: 3,
        BuyerTier.TIER4_FB: 4,
        BuyerTier.TIER5_HR: 5,
        BuyerTier.TIER6_FINANCE: 99,  # Don't search for these
        BuyerTier.TIER7_IRRELEVANT: 99,
        BuyerTier.UNKNOWN: 6,
    }

    TIER_REASONS = {
        BuyerTier.TIER1_UNIFORM_DIRECT: "Housekeeping/Uniform/Laundry — primary uniform buyer",
        BuyerTier.TIER2_PURCHASING: "Purchasing/Procurement — controls vendor selection",
        BuyerTier.TIER3_GM_OPS: "GM/Operations — approves purchases, decision maker",
        BuyerTier.TIER4_FB: "F&B — separate uniform category (chef coats, restaurant staff)",
        BuyerTier.TIER5_HR: "HR — sometimes involved in uniform programs",
        BuyerTier.TIER6_FINANCE: "Finance/AP — invoice contact only, not a buyer",
        BuyerTier.TIER7_IRRELEVANT: "Not involved in uniform purchasing",
        BuyerTier.UNKNOWN: "Unknown role — may have some relevance",
    }

    DECISION_MAKER_TIERS = {
        BuyerTier.TIER1_UNIFORM_DIRECT,
        BuyerTier.TIER2_PURCHASING,
        BuyerTier.TIER3_GM_OPS,
        BuyerTier.TIER4_FB,
    }

    def classify(self, title: str) -> TitleClassification:
        """
        Classify a job title into a buyer tier.

        Returns TitleClassification with tier, score, reason, and priority.
        """
        if not title or not title.strip():
            return TitleClassification(
                tier=BuyerTier.UNKNOWN,
                score=self.TIER_SCORES[BuyerTier.UNKNOWN],
                reason="No title provided",
                search_priority=self.TIER_SEARCH_PRIORITY[BuyerTier.UNKNOWN],
                is_decision_maker=False,
            )

        title_lower = (
            f" {title.lower().strip()} "  # Pad with spaces for word boundary matching
        )

        # Check tiers in priority order (most specific first)
        # Tier 1: Uniform/Housekeeping/Laundry (most specific)
        if self._matches_any(title_lower, TIER1_KEYWORDS):
            return self._make_result(BuyerTier.TIER1_UNIFORM_DIRECT)

        # Tier 2: Purchasing/Procurement
        if self._matches_any(title_lower, TIER2_KEYWORDS):
            return self._make_result(BuyerTier.TIER2_PURCHASING)

        # Tier 7: Irrelevant (check BEFORE GM/Ops to avoid false positives
        # like "Parking Operations Manager" matching "operations manager")
        if self._matches_any(title_lower, TIER7_IRRELEVANT_KEYWORDS):
            return self._make_result(BuyerTier.TIER7_IRRELEVANT)

        # Corporate: Too high level
        if self._matches_any(title_lower, CORPORATE_KEYWORDS):
            return TitleClassification(
                tier=BuyerTier.TIER3_GM_OPS,
                score=5,  # Lower score than property-level GM
                reason="Corporate/Regional — may influence but not direct buyer",
                search_priority=7,
                is_decision_maker=False,
            )

        # Tier 6: Finance/AP
        if self._matches_any(title_lower, TIER6_FINANCE_KEYWORDS):
            return self._make_result(BuyerTier.TIER6_FINANCE)

        # Tier 3: GM/Operations
        if self._matches_any(title_lower, TIER3_KEYWORDS):
            return self._make_result(BuyerTier.TIER3_GM_OPS)

        # Tier 4: F&B
        if self._matches_any(title_lower, TIER4_FB_KEYWORDS):
            return self._make_result(BuyerTier.TIER4_FB)

        # Tier 5: HR
        if self._matches_any(title_lower, TIER5_HR_KEYWORDS):
            return self._make_result(BuyerTier.TIER5_HR)

        # Unknown
        return self._make_result(BuyerTier.UNKNOWN)

    def _matches_any(self, title_lower: str, keywords: list[str]) -> bool:
        """Check if title matches any keyword (fuzzy substring match)."""
        return any(kw in title_lower for kw in keywords)

    def _make_result(self, tier: BuyerTier) -> TitleClassification:
        return TitleClassification(
            tier=tier,
            score=self.TIER_SCORES[tier],
            reason=self.TIER_REASONS[tier],
            search_priority=self.TIER_SEARCH_PRIORITY[tier],
            is_decision_maker=tier in self.DECISION_MAKER_TIERS,
        )

    def is_worth_searching(self, tier: BuyerTier) -> bool:
        """Should we search for contacts with this tier?"""
        return tier in self.DECISION_MAKER_TIERS or tier == BuyerTier.TIER5_HR

    def get_search_titles(self, tier: Optional[BuyerTier] = None) -> list[str]:
        """
        Get human-readable job titles to use in DuckDuckGo/Apollo searches.
        Trained from SAP data — these are the ACTUAL titles your contacts use.

        If tier is None, returns all searchable titles in priority order.
        """
        all_titles = {}

        # Tier 1: Uniform/Housekeeping — 81 SAP contacts
        tier1_titles = [
            "Director of Housekeeping",
            "Executive Housekeeper",
            "Housekeeping Manager",
            "Housekeeping Director",
            "Assistant Director of Housekeeping",
            "Uniform Manager",
            "Uniform Room Manager",
            "Uniform Supervisor",
            "Laundry Manager",
            "Stewarding Supervisor",
            "Executive Steward",
            "Wardrobe Manager",
        ]

        # Tier 2: Purchasing — 132 SAP contacts
        tier2_titles = [
            "Purchasing Manager",
            "Director of Purchasing",
            "Purchasing Director",
            "Purchasing Supervisor",
            "Purchasing Coordinator",
            "Purchasing Agent",
            "Procurement Manager",
            "Head of Procurement",
            "Head of Purchasing",
            "Director of Supply Chain",
            "Supply Chain Manager",
            "Buyer",
            "General Buyer",
            "Sourcing Contract Manager",
            "Procurement Specialist",
            "VP Procurement",
        ]

        # Tier 3: GM/Operations — 251 SAP contacts
        tier3_titles = [
            "General Manager",
            "Hotel Manager",
            "Resort Manager",
            "Property Manager",
            "Resident Manager",
            "Director of Operations",
            "Operations Manager",
            "VP of Operations",
            "Director of Rooms",
            "Rooms Division Manager",
            "Rooms Director",
            "Front Office Manager",
            "Director of Front Office",
            "Assistant General Manager",
            "Pre-Opening General Manager",
            "Task Force General Manager",
            "Managing Director",
        ]

        # Tier 4: F&B — separate uniform category
        tier4_titles = [
            "Director of Food & Beverage",
            "F&B Director",
            "F&B Manager",
            "Executive Chef",
            "Director of Banquets",
            "Director of Catering",
            "Banquet Manager",
            "Restaurant Manager",
        ]

        all_titles = {
            BuyerTier.TIER1_UNIFORM_DIRECT: tier1_titles,
            BuyerTier.TIER2_PURCHASING: tier2_titles,
            BuyerTier.TIER3_GM_OPS: tier3_titles,
            BuyerTier.TIER4_FB: tier4_titles,
        }

        if tier is not None:
            return all_titles.get(tier, [])

        # Return all in priority order
        result = []
        for t in [
            BuyerTier.TIER1_UNIFORM_DIRECT,
            BuyerTier.TIER2_PURCHASING,
            BuyerTier.TIER3_GM_OPS,
            BuyerTier.TIER4_FB,
        ]:
            result.extend(all_titles[t])
        return result

    def get_enrichment_search_titles(self, mode: str = "pre_opening") -> list[str]:
        """
        Get titles optimized for enrichment search queries.
        Mode determines priority order:
        - "pre_opening": GM first (small team, GM picks vendors)
        - "opening_soon": Housekeeping first (they're ordering now)
        """
        if mode == "opening_soon":
            # Operational staff now hired, they're the actual buyers
            order = [
                BuyerTier.TIER1_UNIFORM_DIRECT,
                BuyerTier.TIER2_PURCHASING,
                BuyerTier.TIER3_GM_OPS,
                BuyerTier.TIER4_FB,
            ]
        else:
            # Pre-opening: GM is king, team is tiny
            order = [
                BuyerTier.TIER3_GM_OPS,
                BuyerTier.TIER1_UNIFORM_DIRECT,
                BuyerTier.TIER2_PURCHASING,
                BuyerTier.TIER4_FB,
            ]

        result = []
        for t in order:
            result.extend(self.get_search_titles(t))
        return result


# ═══════════════════════════════════════════════════════════════
# MODULE-LEVEL INSTANCE (import and use directly)
# ═══════════════════════════════════════════════════════════════
title_classifier = TitleClassifier()


if __name__ == "__main__":
    """Test the classifier against SAP title data."""
    classifier = TitleClassifier()

    # Test cases from SAP data + the Himanshu Jethi case
    test_titles = [
        # Tier 1 — Should all be TIER1
        ("RESORT MANAGER", BuyerTier.TIER3_GM_OPS),  # The Himanshu case!
        ("DIRECTOR OF HOUSEKEEPING", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("EXEC. HOUSEKEEPER", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("HOUSEKEEPING MGR", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("UNIFORM ROOM MANAGER", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("WARDROBE MANAGER", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("LAUNDRY MANAGER", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("STEWARDING SUPERVISOR", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("DIR OF HOUSEKEEPING", BuyerTier.TIER1_UNIFORM_DIRECT),
        ("EXECUTIVE HOUSEKEEP MGR", BuyerTier.TIER1_UNIFORM_DIRECT),
        # Tier 2 — Purchasing
        ("PURCHASING MANAGER", BuyerTier.TIER2_PURCHASING),
        ("ASST. PURCHASING MGR", BuyerTier.TIER2_PURCHASING),
        ("BUYER", BuyerTier.TIER2_PURCHASING),
        ("HEAD OF PROCUREMENT", BuyerTier.TIER2_PURCHASING),
        ("SUPPLY CHAIN MANAGER", BuyerTier.TIER2_PURCHASING),
        ("GENERAL BUYER", BuyerTier.TIER2_PURCHASING),
        ("SOURCING CONTRACT MANAGER", BuyerTier.TIER2_PURCHASING),
        ("CENTRAL PURCHASING", BuyerTier.TIER2_PURCHASING),
        ("VP PROCUREMENT & AP", BuyerTier.TIER2_PURCHASING),
        # Tier 3 — GM/Ops
        ("GENERAL MANAGER", BuyerTier.TIER3_GM_OPS),
        ("GM", BuyerTier.TIER3_GM_OPS),
        ("HOTEL MANAGER", BuyerTier.TIER3_GM_OPS),
        ("PROPERTY MANAGER", BuyerTier.TIER3_GM_OPS),
        ("DIRECTOR OF OPERATIONS", BuyerTier.TIER3_GM_OPS),
        ("ROOMS DIVISION MANAGER", BuyerTier.TIER3_GM_OPS),
        ("FRONT OFFICE MANAGER", BuyerTier.TIER3_GM_OPS),
        ("ASST GM", BuyerTier.TIER3_GM_OPS),
        ("DIR OF ROOMS", BuyerTier.TIER3_GM_OPS),
        # Tier 4 — F&B
        ("F&B DIRECTOR", BuyerTier.TIER4_FB),
        ("EXECUTIVE CHEF", BuyerTier.TIER4_FB),
        ("DIRECTOR OF FOOD & BEVERAGE", BuyerTier.TIER4_FB),
        ("BANQUET MANAGER", BuyerTier.TIER4_FB),
        # Tier 5 — HR
        ("HUMAN RESOURCES MANAGER", BuyerTier.TIER5_HR),
        # Tier 6 — Finance (NOT buyers)
        ("ACCOUNTS PAYABLE", BuyerTier.TIER6_FINANCE),
        ("CONTROLLER", BuyerTier.TIER6_FINANCE),
        # Tier 7 — Irrelevant
        ("MARKETING DIRECTOR", BuyerTier.TIER7_IRRELEVANT),
        ("SPA DIRECTOR", BuyerTier.TIER7_IRRELEVANT),
        ("CHIEF ENGINEER", BuyerTier.TIER7_IRRELEVANT),
        ("SECURITY", BuyerTier.TIER7_IRRELEVANT),
        # Corporate
        ("PRESIDENT", None),  # Corporate — special handling
        ("REGIONAL DIRECTOR", None),
    ]

    print("=" * 80)
    print("SAP TITLE CLASSIFIER — VALIDATION TEST")
    print("=" * 80)
    passed = 0
    failed = 0

    for title, expected_tier in test_titles:
        result = classifier.classify(title)
        if expected_tier is None:
            # Just show it
            print(
                f"  INFO  | {title:45} → Tier {result.tier.name:25} (score={result.score:+3d})"
            )
            continue

        ok = result.tier == expected_tier
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(
            f"  {status}  | {title:45} → Tier {result.tier.name:25} (score={result.score:+3d}) "
            f"{'✓' if ok else f'✗ expected {expected_tier.name}'}"
        )

    print(f"\n{'=' * 80}")
    print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")
    print(f"{'=' * 80}")
