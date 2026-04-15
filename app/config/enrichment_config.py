"""
SMART LEAD HUNTER — Enrichment Configuration (SAP-Trained Update)
==================================================================
WHAT CHANGED:
1. Title lists expanded from 23 → 53 unique titles based on 780 SAP positions
2. Added titles your sales team actually contacts: Resort Manager, Property Manager,
   Housekeeping Manager, Uniform Manager, Wardrobe Manager, Supply Chain Manager, etc.
3. Added F&B as a separate tier (they buy restaurant/bar staff uniforms separately)
4. Smart query builder replaces naive hotel-name-only searches
5. Name collision detection prevents false positives
"""


# ═══════════════════════════════════════════════════════════════
# CONTACT SEARCH PRIORITY — SAP-Trained, Ordered by buyer influence
# Source: 3,929 contacts across 1,018 business partners in SAP B1
# ═══════════════════════════════════════════════════════════════

CONTACT_SEARCH_PRIORITIES = {
    # ── PRE-OPENING MODE (6+ months before opening) ──
    # When hotel hasn't opened: no property GM yet. Chain COO/VP Operations
    # at the management company controls ALL pre-opening procurement decisions.
    # GM is typically hired only 6-12 months before opening.
    "pre_opening": [
        {
            "priority": 1,
            "titles": [
                # Chain/management company ops executives — the real decision makers
                # before property-level staff are hired
                "Chief Operating Officer",
                "COO",
                "VP of Hotel Operations",
                "VP of Operations",
                "Vice President of Operations",
                "SVP of Operations",
                "Senior Vice President Operations",
                "VP of Development",
                "Vice President Hotel Operations",
                "Director of Hotel Operations",
                "Director of Operations",  # chain-level
                "Regional Director of Operations",
                "Pre-Opening Manager",
                "Pre-Opening Director",
                "Opening Manager",
            ],
            "reason": "When hotel hasnt opened yet, chain COO/VP Operations controls all pre-opening OS&E procurement — GM may not be hired yet",
        },
        {
            "priority": 2,
            "titles": [
                "General Manager",
                "Pre-Opening General Manager",
                "Task Force General Manager",
                "Opening General Manager",
                "Hotel Manager",  # 7 in SAP
                "Resort Manager",  # 3 in SAP (HIMANSHU CASE)
                "Property Manager",  # 21 in SAP
                "Managing Director",  # 5 in SAP
            ],
            "reason": "If GM already appointed (6-12mo out), they personally select all vendors including uniforms",
        },
        {
            "priority": 3,
            "titles": [
                "Director of Operations",
                "Operations Manager",  # 17 in SAP
                "Director of Rooms",  # 18 in SAP
                "Rooms Division Manager",  # 6 in SAP
                "Rooms Director",  # 3 in SAP
            ],
            "reason": "Property-level ops — oversees procurement once hired",
        },
        {
            "priority": 4,
            "titles": [
                "Purchasing Manager",
                "Director of Procurement",
                "Purchasing Director",
                "Director of Purchasing",
                "Head of Procurement",  # from SAP
                "Head of Purchasing",  # from SAP
                "Supply Chain Manager",  # 3 in SAP
                "Procurement Manager",  # from SAP
                "General Buyer",  # 3 in SAP
                "VP Procurement",  # from SAP
                "VP of Purchasing",  # chain-level purchasing exec
            ],
            "reason": "Chain purchasing exec or property purchasing manager — controls vendor RFPs",
        },
        {
            "priority": 5,
            "titles": [
                "Director of Housekeeping",
                "Executive Housekeeper",
                "Housekeeping Manager",  # 15 in SAP
                "Housekeeping Director",  # 2 in SAP
                "Assistant Director of Housekeeping",
                "Uniform Manager",  # from SAP
                "Wardrobe Manager",  # from SAP
                "Laundry Manager",  # from SAP
            ],
            "reason": "Owns the uniform program once hired — specs fabrics, quantities, styles",
        },
        {
            "priority": 6,
            "titles": [
                "Assistant General Manager",
                "Resident Manager",
                "Front Office Manager",  # 28 in SAP
                "Director of Front Office",  # 10 in SAP
            ],
            "reason": "Day-to-day ops authority, often delegates but can approve vendor decisions",
        },
        {
            "priority": 7,
            "titles": [
                "Director of Food and Beverage",
                "Director of F&B",
                "F&B Director",
                "F&B Manager",  # 17 in SAP
                "Executive Chef",
                "Director of Banquets",  # 3 in SAP
                "Banquet Manager",  # 6 in SAP
            ],
            "reason": "Decides restaurant, bar, banquet staff uniforms separately from rooms division",
        },
        {
            "priority": 8,
            "titles": [
                "Director of Human Resources",
                "HR Director",
                "Director of People and Culture",
                "VP of Human Resources",
                "Human Resources Manager",
            ],
            "reason": "Handles uniform onboarding for all new hires — critical for pre-opening staffing",
        },
    ],
    # ── OPENING SOON MODE (under 6 months) ──
    # Operational staff now hired, they're the actual buyers
    "opening_soon": [
        {
            "priority": 1,
            "titles": [
                "Director of Housekeeping",
                "Executive Housekeeper",
                "Housekeeping Manager",
                "Housekeeping Director",
                "Assistant Director of Housekeeping",
                "Uniform Manager",
                "Wardrobe Manager",
                "Laundry Manager",
            ],
            "reason": "Uniform orders happen NOW — housekeeping director specs and orders",
        },
        {
            "priority": 2,
            "titles": [
                "Purchasing Manager",
                "Director of Procurement",
                "Purchasing Director",
                "Director of Purchasing",
                "Head of Procurement",
                "Head of Purchasing",
                "Supply Chain Manager",
                "Procurement Manager",
                "General Buyer",
                "Purchasing Coordinator",  # 4 in SAP
                "Purchasing Supervisor",  # 7 in SAP
            ],
            "reason": "Processing vendor orders, may already have RFPs out",
        },
        {
            "priority": 3,
            "titles": [
                "Director of Operations",
                "VP of Operations",
                "Operations Manager",
                "Director of Rooms",
                "Rooms Division Manager",
            ],
            "reason": "Budget authority for operational purchases",
        },
        {
            "priority": 4,
            "titles": [
                "General Manager",
                "Hotel Manager",
                "Resort Manager",
                "Property Manager",
            ],
            "reason": "Approves final spend, good intro to purchasing team",
        },
        {
            "priority": 5,
            "titles": [
                "Director of Food and Beverage",
                "Director of F&B",
                "F&B Director",
                "F&B Manager",
                "Executive Chef",
                "Director of Banquets",
                "Banquet Manager",
            ],
            "reason": "Restaurant/bar staff uniforms ordered separately",
        },
        {
            "priority": 6,
            "titles": [
                "Executive Chef",
                "Director of Catering",  # 3 in SAP
            ],
            "reason": "Kitchen whites, chef coats — separate uniform category",
        },
        {
            "priority": 7,
            "titles": [
                "Director of Human Resources",
                "HR Director",
                "Director of People and Culture",
                "VP of Human Resources",
                "Human Resources Manager",
            ],
            "reason": "Handles uniform onboarding and distribution to all new hires",
        },
    ],
}

# ═══════════════════════════════════════════════════════════════
# FLATTENED TITLE LIST (used for web search queries)
# ═══════════════════════════════════════════════════════════════

ALL_UNIFORM_TITLES = list(
    {
        title
        for mode in CONTACT_SEARCH_PRIORITIES.values()
        for group in mode
        for title in group["titles"]
    }
)


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT SEARCH STRATEGY — Updated with smart queries
# ═══════════════════════════════════════════════════════════════

SEARCH_LAYERS = [
    {
        "layer": 1,
        "name": "linkedin_search",
        "method": "web_search",
        "description": "Search LinkedIn for hotel-specific contacts",
        "query_templates": [
            "{hotel_name} General Manager OR Director site:linkedin.com",
            "{hotel_name} Purchasing OR Housekeeping site:linkedin.com",
        ],
        "scrape_results": False,
        "max_results": 5,
    },
    {
        "layer": 2,
        "name": "press_release_search",
        "method": "web_search",
        "description": "Search for hotel appointment press releases",
        "query_templates": [
            "{hotel_name} appoints OR hires OR names OR appointed",
            "{hotel_name} pre-opening team OR leadership",
        ],
        "scrape_results": True,
        "max_results": 3,
    },
    {
        "layer": 3,
        "name": "parent_company_search",
        "method": "web_search",
        "description": "Search by parent company + hotel name (fallback)",
        "query_templates": [
            "{management_company} {hotel_name} team OR staff",
            "{brand} {hotel_name} General Manager OR Director",
        ],
        "scrape_results": True,
        "max_results": 3,
        "requires": ["management_company_or_brand"],
    },
]


# ═══════════════════════════════════════════════════════════════
# CONTACT VALIDATION SETTINGS
# ═══════════════════════════════════════════════════════════════

VALIDATION_SETTINGS = {
    "min_contact_score": 5,
    "max_contacts_per_lead": 5,
    "name_collision_penalty": -15,
    "unrelated_org_penalty": -10,
    "hotel_specific_bonus": 15,
    "chain_level_bonus": 5,
    "linkedin_verified_bonus": 5,
    "auto_retry_on_collision": True,
    "max_retry_attempts": 2,
}


# ═══════════════════════════════════════════════════════════════
# HOSPITALITY NEWS SOURCES — Known sources for GM appointments
# ═══════════════════════════════════════════════════════════════

HOSPITALITY_NEWS_DOMAINS = [
    "hotel-online.com",
    "hotelexecutive.com",
    "hotelmanagement.net",
    "hospitalitynet.org",
    "hotelsmag.com",
    "hotelnewsresource.com",
    "costar.com",
]


# ═══════════════════════════════════════════════════════════════
# BRAND → PARENT COMPANY MAPPING
# Maps brand names to parent companies for web search fallback
# ═══════════════════════════════════════════════════════════════

BRAND_TO_PARENT = {
    # Marriott family
    "westin": "Marriott",
    "w hotels": "Marriott",
    "st. regis": "Marriott",
    "st regis": "Marriott",
    "ritz-carlton": "Marriott",
    "ritz carlton": "Marriott",
    "the ritz-carlton": "Marriott",
    "jw marriott": "Marriott",
    "marriott": "Marriott",
    "sheraton": "Marriott",
    "le meridien": "Marriott",
    "autograph collection": "Marriott",
    "tribute portfolio": "Marriott",
    "luxury collection": "Marriott",
    "the luxury collection": "Marriott",
    "edition": "Marriott",
    "bulgari": "Marriott",
    # Hilton family
    "hilton": "Hilton",
    "waldorf astoria": "Hilton",
    "waldorf-astoria": "Hilton",
    "conrad": "Hilton",
    "signia": "Hilton",
    "signia by hilton": "Hilton",
    "lxr": "Hilton",
    "curio collection": "Hilton",
    "curio": "Hilton",
    "tapestry": "Hilton",
    "canopy": "Hilton",
    "canopy by hilton": "Hilton",
    "embassy suites": "Hilton",
    "doubletree": "Hilton",
    # Hyatt family
    "hyatt": "Hyatt",
    "grand hyatt": "Hyatt",
    "park hyatt": "Hyatt",
    "andaz": "Hyatt",
    "alila": "Hyatt",
    "thompson": "Hyatt",
    "hyatt regency": "Hyatt",
    "hyatt centric": "Hyatt",
    "miraval": "Hyatt",
    "secrets": "Hyatt",
    "dreams": "Hyatt",
    "zoetry": "Hyatt",
    "destination by hyatt": "Hyatt",
    # Accor family
    "fairmont": "Accor",
    "sofitel": "Accor",
    "raffles": "Accor",
    "banyan tree": "Accor",
    "rixos": "Accor",
    "swissotel": "Accor",
    "movenpick": "Accor",
    "pullman": "Accor",
    "mgallery": "Accor",
    "delano": "Accor",
    # IHG family
    "intercontinental": "IHG",
    "kimpton": "IHG",
    "regent": "IHG",
    "six senses": "IHG",
    "vignette collection": "IHG",
    "hotel indigo": "IHG",
    # Independent luxury
    "montage": "Montage Hotels & Resorts",
    "pendry": "Montage Hotels & Resorts",
    "four seasons": "Four Seasons",
    "rosewood": "Rosewood Hotel Group",
    "mandarin oriental": "Mandarin Oriental",
    "aman": "Aman Resorts",
    "one&only": "Kerzner International",
    "one & only": "Kerzner International",
    "atlantis": "Kerzner International",
    "peninsula": "The Peninsula Hotels",
    "capella": "Capella Hotel Group",
    "viceroy": "Viceroy Hotel Group",
    "loews": "Loews Hotels",
    "langham": "Langham Hospitality Group",
    "oberoi": "The Oberoi Group",
    "belmond": "LVMH",
    "cheval blanc": "LVMH",
    "faena": "Faena Group",
    "nobu hotel": "Nobu Hospitality",
    "1 hotel": "SH Hotels & Resorts",
    "1hotel": "SH Hotels & Resorts",
    "baccarat hotel": "SH Hotels & Resorts",
    # Other
    "sandals": "Sandals Resorts",
    "palace resorts": "Palace Resorts",
    "moon palace": "Palace Resorts",
    "hard rock": "Hard Rock Hotels",
    "westgate": "Westgate Resorts",
    "driftwood": "Driftwood Hospitality Management",
    "ace hotel": "Atelier Ace",
    "graduate": "AJ Capital Partners",
    "dream hotel": "Dream Hotel Group",
}


# ═══════════════════════════════════════════════════════════════
# MANAGEMENT COMPANIES (search these if brand search fails)
# ═══════════════════════════════════════════════════════════════

MANAGEMENT_COMPANIES = [
    "Aimbridge Hospitality",
    "Highgate Hotels",
    "Driftwood Hospitality Management",
    "Crescent Hotels & Resorts",
    "Sage Hospitality",
    "White Lodging",
    "Pyramid Hotel Group",
    "Davidson Hospitality",
    "Remington Hospitality",
    "Benchmark Hospitality",
]


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT TIMING / RATE LIMITS
# ═══════════════════════════════════════════════════════════════

ENRICHMENT_SETTINGS = {
    "ddg_delay_seconds": 1.5,
    "serper_delay_seconds": 0.5,  # Serper is fast, minimal delay needed
    "crawl_timeout_seconds": 20,
    "max_articles_to_scrape": 5,
    # M-03: Model name loaded from app settings at runtime (see get_gemini_model())
    "gemini_model": None,  # Resolved lazily below
    "max_article_chars": 12000,
}


def get_enrichment_gemini_model() -> str:
    """Get Gemini model for enrichment, centralized from app settings (M-03)."""
    if ENRICHMENT_SETTINGS["gemini_model"]:
        return ENRICHMENT_SETTINGS["gemini_model"]
    try:
        from app.config import settings

        model = getattr(settings, "gemini_model", "gemini-2.5-flash")
        ENRICHMENT_SETTINGS["gemini_model"] = model
        return model
    except Exception:
        return "gemini-2.5-flash"
