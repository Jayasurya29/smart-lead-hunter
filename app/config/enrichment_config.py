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
    # GM is king because team is tiny, GM picks all vendors personally
    "pre_opening": [
        {
            "priority": 1,
            "titles": [
                "General Manager",
                "Pre-Opening General Manager",
                "Task Force General Manager",
                "Hotel Manager",  # 7 in SAP
                "Resort Manager",  # 3 in SAP (HIMANSHU CASE)
                "Property Manager",  # 21 in SAP
                "Managing Director",  # 5 in SAP
            ],
            "reason": "At pre-opening properties, GM personally selects all vendors including uniforms",
        },
        {
            "priority": 2,
            "titles": [
                "Director of Operations",
                "VP of Operations",
                "Regional Director of Operations",
                "Operations Manager",  # 17 in SAP
                "Director of Rooms",  # 18 in SAP
                "Rooms Division Manager",  # 6 in SAP
                "Rooms Director",  # 3 in SAP
            ],
            "reason": "Oversees all operational procurement, often handles vendor RFPs",
        },
        {
            "priority": 3,
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
            ],
            "reason": "Handles vendor selection, negotiates contracts, manages procurement",
        },
        {
            "priority": 4,
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
            "reason": "Owns the uniform program — specs fabrics, quantities, styles",
        },
        {
            "priority": 5,
            "titles": [
                "Assistant General Manager",
                "Resident Manager",
                "Front Office Manager",  # 28 in SAP
                "Director of Front Office",  # 10 in SAP
            ],
            "reason": "Day-to-day ops authority, often delegates but can approve vendor decisions",
        },
        {
            "priority": 6,
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
    ],
}


# ═══════════════════════════════════════════════════════════════
# FLATTENED TITLE LIST (used for Apollo bulk queries)
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
    {
        "layer": 4,
        "name": "apollo_specific",
        "method": "apollo_search",
        "description": "Apollo search by parent brand + location + priority titles",
        "search_by": "brand_location",
        "reveal": True,
    },
    {
        "layer": 5,
        "name": "apollo_broad",
        "method": "apollo_search",
        "description": "Apollo broader search — brand + state/country level",
        "search_by": "brand_region",
        "reveal": True,
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
# Apollo can't find individual hotels, so we search parent brands
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
    "ddg_delay_seconds": 2.0,
    "serper_delay_seconds": 0.5,  # Serper is fast, minimal delay needed
    "apollo_delay_seconds": 0.5,
    "crawl_timeout_seconds": 15,
    "max_articles_to_scrape": 3,
    "max_apollo_reveals_per_lead": 2,
    "gemini_model": "gemini-2.5-flash",
    "max_article_chars": 8000,
}
