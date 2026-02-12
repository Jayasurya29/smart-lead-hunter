"""
SMART LEAD HUNTER - SOURCE CONFIGURATION - IMPROVED
====================================================
All 79+ database sources with patterns.
Previously: 56 configured, 23 using generic fallback.

IMPROVEMENTS:
- Added 24 missing source patterns
- Added source_type field (feeds extraction hints to Gemini)
- Dynamic year for aggregator sources (no manual yearly update)
- Expanded Florida local coverage (primary market)
- Added wire service patterns (PR Newswire, Business Wire)
- Stricter block patterns to reduce junk pages
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class SourcePatterns:
    gold_patterns: List[str] = field(default_factory=list)
    block_patterns: List[str] = field(default_factory=list)
    link_patterns: List[str] = field(default_factory=list)
    max_pages: int = 30
    # NEW: Source type for extraction hint matching
    # Maps to LeadExtractor.SOURCE_HINTS keys:
    # chain_newsroom, aggregator, industry, florida, caribbean, permit, business
    source_type: str = ""


# ─────────────────────────────────────────────────────
# SHARED BLOCK PATTERNS
# ─────────────────────────────────────────────────────

_SOCIAL = [
    r"facebook\.com",
    r"twitter\.com",
    r"x\.com",
    r"instagram\.com",
    r"linkedin\.com",
    r"youtube\.com",
    r"tiktok\.com",
    r"threads\.net",
]
_FILES = [r"\.pdf$", r"\.jpg$", r"\.png$", r"\.gif$", r"\.mp4$", r"\.zip$"]
_GENERIC = (
    [
        r"/login",
        r"/signin",
        r"/signup",
        r"/subscribe",
        r"/contact",
        r"/about-us",
        r"/privacy",
        r"/terms",
        r"/cart",
        r"/checkout",
        r"/shop",
        r"/careers",
        r"/jobs",
        r"/tag/",
        r"/author/",
        r"/category/$",
        r"/page/\d+$",
        r"/search\?",
        r"/advertise",
        r"/podcast/",
        r"/webinar/",
    ]
    + _SOCIAL
    + _FILES
)

_MARRIOTT = [
    r"/category/",
    r"/tag/",
    r"/author/",
    r"/page/",
    r"/search/",
    r"/subscribe/",
    r"/media-contacts/",
] + _SOCIAL

_HILTON = [
    r"/team-members/",
    r"/corporate/",
    r"/about/",
    r"/responsibility/",
    r"/investors/",
] + _SOCIAL

_BIZJOURNAL = [
    r"/news/banking",
    r"/news/technology",
    r"/news/health-care",
    r"/news/retail",
    r"/bizwomen/",
    r"/events/",
    r"/people/",
    r"/lists/",
    r"/subscribers",
    r"/account/",
] + _SOCIAL

_WIRE = (
    [
        r"/tag/",
        r"/category/",
        r"/author/",
        r"/page/\d+$",
    ]
    + _SOCIAL
    + _FILES
)


# ─────────────────────────────────────────────────────
# DYNAMIC YEAR (for aggregator sources)
# ─────────────────────────────────────────────────────

_CUR_YEAR = str(datetime.now().year)
_NEXT_YEAR = str(datetime.now().year + 1)


# ─────────────────────────────────────────────────────
# SOURCE PATTERNS — ALL 79+ SOURCES
# ─────────────────────────────────────────────────────

SOURCE_PATTERNS: Dict[str, SourcePatterns] = {
    # ═════════════════════════════════════════════════
    # MARRIOTT (9)
    # ═════════════════════════════════════════════════
    "Marriott News - All Brands": SourcePatterns(
        gold_patterns=[
            r"news\.marriott\.com/news/\d{4}/\d{2}/",
            r"news\.marriott\.com/.*-open",
            r"news\.marriott\.com/.*-debut",
            r"news\.marriott\.com/.*-resort",
        ],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=30,
        source_type="chain_newsroom",
    ),
    "Marriott  - Ritz-Carlton": SourcePatterns(
        gold_patterns=[
            r"news\.marriott\.com/brands/the-ritz-carlton/",
            r"news\.marriott\.com/news/\d{4}/\d{2}/.*ritz",
        ],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - St. Regis": SourcePatterns(
        gold_patterns=[
            r"news\.marriott\.com/brands/st-regis/",
            r"news\.marriott\.com/news/\d{4}/\d{2}/.*regis",
        ],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - Luxury Collection": SourcePatterns(
        gold_patterns=[r"news\.marriott\.com/brands/the-luxury-collection/"],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - Autograph Collection": SourcePatterns(
        gold_patterns=[r"news\.marriott\.com/brands/autograph-collection/"],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - EDITION": SourcePatterns(
        gold_patterns=[r"news\.marriott\.com/brands/edition/"],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - W Hotels": SourcePatterns(
        gold_patterns=[r"news\.marriott\.com/brands/w-hotels/"],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - JW Marriott": SourcePatterns(
        gold_patterns=[r"news\.marriott\.com/brands/jw-marriott/"],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Marriott - Tribute Portfolio": SourcePatterns(
        gold_patterns=[r"news\.marriott\.com/news/\d{4}/\d{2}/"],
        block_patterns=_MARRIOTT,
        link_patterns=[r"/news/\d{4}/\d{2}/"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    # ═════════════════════════════════════════════════
    # HILTON (5)
    # ═════════════════════════════════════════════════
    "Hilton - Waldorf Astoria": SourcePatterns(
        gold_patterns=[r"stories\.hilton\.com/releases/[a-z0-9-]+"],
        block_patterns=_HILTON,
        link_patterns=[r"/releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Hilton - Curio Collection": SourcePatterns(
        gold_patterns=[r"stories\.hilton\.com/releases/[a-z0-9-]+"],
        block_patterns=_HILTON,
        link_patterns=[r"/releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Hilton - Conrad": SourcePatterns(
        gold_patterns=[r"stories\.hilton\.com/releases/[a-z0-9-]+"],
        block_patterns=_HILTON,
        link_patterns=[r"/releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Hilton - Tempo": SourcePatterns(
        gold_patterns=[r"stories\.hilton\.com/releases/[a-z0-9-]+"],
        block_patterns=_HILTON,
        link_patterns=[r"/releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Hilton - LXR": SourcePatterns(
        gold_patterns=[r"stories\.hilton\.com/releases/[a-z0-9-]+"],
        block_patterns=_HILTON,
        link_patterns=[r"/releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    # ═════════════════════════════════════════════════
    # OTHER CHAINS (8)
    # ═════════════════════════════════════════════════
    "Hyatt Newsroom - All Brands": SourcePatterns(
        gold_patterns=[r"newsroom\.hyatt\.com/news-releases/[a-z0-9-]+"],
        block_patterns=[r"/media-contacts/", r"/about/", r"/brand-pages/", r"/gallery/"]
        + _SOCIAL,
        link_patterns=[r"/news-releases/[a-z0-9-]+"],
        max_pages=30,
        source_type="chain_newsroom",
    ),
    "Hyatt Newsroom - Homepage": SourcePatterns(
        gold_patterns=[r"newsroom\.hyatt\.com/news-releases/[a-z0-9-]+"],
        block_patterns=[r"/media-contacts/", r"/about/"] + _SOCIAL,
        link_patterns=[r"/news-releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "IHG News & Media": SourcePatterns(
        gold_patterns=[r"ihgplc\.com/.*news.*\d{4}", r"ihgplc\.com/.*-hotel"],
        block_patterns=[r"/investors/", r"/responsibility/", r"/careers/"] + _SOCIAL,
        link_patterns=[r"/news-and-media/.*\d{4}"],
        max_pages=30,
        source_type="chain_newsroom",
    ),
    "Accor News Stories": SourcePatterns(
        gold_patterns=[r"group\.accor\.com/.*news-stories/.*"],
        block_patterns=[r"/investors/", r"/commitments/", r"/careers/"] + _SOCIAL,
        link_patterns=[r"/news-stories/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Accor Press Releases": SourcePatterns(
        gold_patterns=[r"group\.accor\.com/.*press-releases/.*"],
        block_patterns=[r"/investors/", r"/careers/"] + _SOCIAL,
        link_patterns=[r"/press-releases/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Ennismore News": SourcePatterns(
        gold_patterns=[r"ennismore\.com/news/[a-z0-9-]+"],
        block_patterns=[r"/brands/", r"/about/", r"/careers/"] + _SOCIAL,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Wyndham Newsroom": SourcePatterns(
        gold_patterns=[r"wyndham.*news.*", r"wyndham.*press.*"],
        block_patterns=_GENERIC,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    "Best Western Media Center": SourcePatterns(
        gold_patterns=[r"bestwestern.*media.*"],
        block_patterns=_GENERIC,
        link_patterns=[r"/media/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Choice Hotels Newsroom": SourcePatterns(
        gold_patterns=[r"choicehotels.*news.*"],
        block_patterns=_GENERIC,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=20,
        source_type="chain_newsroom",
    ),
    # ═════════════════════════════════════════════════
    # LUXURY (10)
    # ═════════════════════════════════════════════════
    "Four Seasons - New Hotels Page": SourcePatterns(
        gold_patterns=[r"/landing/new-hotels"],
        block_patterns=[
            r"/reservations/",
            r"/checkout/",
            r"/spa/",
            r"/dining/",
            r"/weddings/",
        ]
        + _SOCIAL,
        link_patterns=[],
        max_pages=5,
        source_type="chain_newsroom",
    ),
    "Four Seasons - News Releases": SourcePatterns(
        gold_patterns=[r"press\.fourseasons\.com/news-releases/.*"],
        block_patterns=[r"/media-contacts/", r"/about/"] + _SOCIAL,
        link_patterns=[r"/news-releases/[a-z0-9-]+"],
        max_pages=30,
        source_type="chain_newsroom",
    ),
    "Peninsula Hotels News": SourcePatterns(
        gold_patterns=[r"peninsula\.com/.*newsroom.*"],
        block_patterns=[r"/reservations/", r"/offers/", r"/dining/", r"/spa/"]
        + _SOCIAL,
        link_patterns=[r"/newsroom/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Oetker Collection Press": SourcePatterns(
        gold_patterns=[r"oetkercollection\.com/press/.*"],
        block_patterns=[r"/reservations/", r"/offers/"] + _SOCIAL,
        link_patterns=[r"/press/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Montage Hotels Press": SourcePatterns(
        gold_patterns=[r"montagehotels\.com/press/.*"],
        block_patterns=[r"/reservations/", r"/offers/"] + _SOCIAL,
        link_patterns=[r"/press/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Auberge Resorts Press": SourcePatterns(
        gold_patterns=[r"aubergeresorts\.com/press/.*"],
        block_patterns=[r"/reservations/", r"/offers/"] + _SOCIAL,
        link_patterns=[r"/press/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Rosewood Hotels Press": SourcePatterns(
        gold_patterns=[r"rosewoodhotelgroup\.com/.*press.*"],
        block_patterns=[r"/reservations/"] + _SOCIAL,
        link_patterns=[r"/press/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Loews Hotels Press": SourcePatterns(
        gold_patterns=[r"loewshotels\.com/press-room/.*"],
        block_patterns=[r"/reservations/", r"/destinations/"] + _SOCIAL,
        link_patterns=[r"/press-room/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "SH Hotels News": SourcePatterns(
        gold_patterns=[r"shhotelsandresorts\.com/.*news.*"],
        block_patterns=[r"/reservations/"] + _SOCIAL,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    "Aman Press": SourcePatterns(
        gold_patterns=[r"aman\.com/.*press.*"],
        block_patterns=[r"/reservations/", r"/offers/", r"/wellness/"] + _SOCIAL,
        link_patterns=[r"/press/[a-z0-9-]+"],
        max_pages=15,
        source_type="chain_newsroom",
    ),
    # ═════════════════════════════════════════════════
    # INDUSTRY PUBLICATIONS (10 + 4 NEW)
    # ═════════════════════════════════════════════════
    "Hotel Dive": SourcePatterns(
        gold_patterns=[r"hoteldive\.com/news/[a-z0-9-]+/\d+/"],
        block_patterns=[
            r"/selfservice/",
            r"/library/",
            r"/events/",
            r"/editors/",
            r"/signup/",
            r"cfodive\.com",
            r"constructiondive\.com",
            r"retaildive\.com",
        ],
        link_patterns=[r"/news/[a-z0-9-]+/\d+/"],
        max_pages=50,
        source_type="industry",
    ),
    "Hotel Dive - News": SourcePatterns(
        gold_patterns=[r"hoteldive\.com/news/[a-z0-9-]+/\d+/"],
        block_patterns=[
            r"/selfservice/",
            r"/library/",
            r"/events/",
            r"/signup/",
            r"cfodive\.com",
            r"constructiondive\.com",
        ],
        link_patterns=[r"/news/[a-z0-9-]+/\d+/"],
        max_pages=50,
        source_type="industry",
    ),
    "Hotel Management - News": SourcePatterns(
        gold_patterns=[
            r"hotelmanagement\.net/.*-open",
            r"hotelmanagement\.net/.*-hotel",
            r"hotelmanagement\.net/development/",
        ],
        block_patterns=[r"/white-papers/", r"/events/", r"/subscribe/"] + _SOCIAL,
        link_patterns=[r"/development/[a-z0-9-]+"],
        max_pages=30,
        source_type="industry",
    ),
    "Hospitality Net": SourcePatterns(
        gold_patterns=[r"/announcement/\d+/", r"/news/\d+\.html"],
        block_patterns=[
            r"/organization/",
            r"/opinion/",
            r"/video/",
            r"/event/",
            r"/panel/",
            r"/viewpoint/",
            r"/podcast/",
            r"/supplier/",
            r"/me/",
        ],
        link_patterns=[r"announcement/\d+", r"news/\d+\.html"],
        max_pages=30,
        source_type="aggregator",
    ),
    "Hotel News Resource - Openings": SourcePatterns(
        gold_patterns=[r"hotelnewsresource\.com/article\d+\.html"],
        block_patterns=[r"/directory/", r"/events/", r"/advertise/"] + _SOCIAL,
        link_patterns=[r"/article\d+\.html"],
        max_pages=30,
        source_type="aggregator",
    ),
    "Hotel News Resource - Florida": SourcePatterns(
        gold_patterns=[r"hotelnewsresource\.com/article/.*"],
        block_patterns=[r"/directory/", r"/events/"] + _SOCIAL,
        link_patterns=[r"/article/[a-z0-9-]+"],
        max_pages=30,
        source_type="aggregator",
    ),
    "Bisnow Hotels": SourcePatterns(
        gold_patterns=[r"bisnow\.com/.*hotel.*", r"bisnow\.com/.*/news/[a-z0-9-]+"],
        block_patterns=[r"/events/", r"/jobs/", r"/advertise/", r"/subscribe/"]
        + _SOCIAL,
        link_patterns=[r"/[a-z-]+/news/[a-z0-9-]+"],
        max_pages=30,
        source_type="industry",
    ),
    "CoStar Hotels": SourcePatterns(
        gold_patterns=[r"costar\.com/article/\d+"],
        block_patterns=[r"/login", r"/subscribe", r"/pricing"] + _SOCIAL,
        link_patterns=[r"/article/\d+"],
        max_pages=30,
        source_type="industry",
    ),
    "Skift - Hotels": SourcePatterns(
        gold_patterns=[
            r"skift\.com/\d{4}/\d{2}/\d{2}/.*hotel",
            r"skift\.com/\d{4}/\d{2}/\d{2}/.*resort",
        ],
        block_patterns=[r"/events/", r"/research/", r"/subscribe/", r"/skiftx/"]
        + _SOCIAL,
        link_patterns=[r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+"],
        max_pages=30,
        source_type="industry",
    ),
    "LODGING Magazine": SourcePatterns(
        gold_patterns=[
            r"lodgingmagazine\.com/.*hotel",
            r"lodgingmagazine\.com/.*opening",
        ],
        block_patterns=[r"/tag/", r"/author/", r"/wp-admin/", r"/subscribe/"] + _SOCIAL,
        link_patterns=[r"/\d{4}/\d{2}/[a-z0-9-]+/"],
        max_pages=30,
        source_type="industry",
    ),
    # NEW: Missing industry sources
    "Hotel Business": SourcePatterns(
        gold_patterns=[
            r"hotelbusiness\.com/.*hotel",
            r"hotelbusiness\.com/.*opening",
            r"hotelbusiness\.com/.*development",
        ],
        block_patterns=[r"/subscribe/", r"/events/", r"/webinar/", r"/magazine/"]
        + _SOCIAL,
        link_patterns=[r"/[a-z0-9-]+-hotel", r"/[a-z0-9-]+-opening"],
        max_pages=30,
        source_type="industry",
    ),
    "Hotel Interactive": SourcePatterns(
        gold_patterns=[
            r"hotelinteractive\.com/article/\d+",
            r"hotelinteractive\.com/.*hotel.*open",
        ],
        block_patterns=[r"/directory/", r"/events/", r"/supplier/"] + _SOCIAL,
        link_patterns=[r"/article/\d+"],
        max_pages=25,
        source_type="aggregator",
    ),
    "Hospitality Design": SourcePatterns(
        gold_patterns=[
            r"hospitalitydesign\.com/.*hotel",
            r"hospitalitydesign\.com/.*resort",
            r"hospitalitydesign\.com/.*opening",
        ],
        block_patterns=[r"/events/", r"/awards/", r"/subscribe/", r"/magazine/"]
        + _SOCIAL,
        link_patterns=[r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+"],
        max_pages=20,
        source_type="industry",
    ),
    "Construction Dive - Hotels": SourcePatterns(
        gold_patterns=[
            r"constructiondive\.com/news/.*hotel",
            r"constructiondive\.com/news/.*resort",
        ],
        block_patterns=[r"/selfservice/", r"/library/", r"/events/", r"/signup/"]
        + _SOCIAL,
        link_patterns=[r"/news/[a-z0-9-]+/\d+/"],
        max_pages=20,
        source_type="permit",
    ),
    # ═════════════════════════════════════════════════
    # TRAVEL (4 + 2 NEW)
    # ═════════════════════════════════════════════════
    "Travel + Leisure - Hotels": SourcePatterns(
        gold_patterns=[
            r"travelandleisure\.com/.*new-hotel",
            r"travelandleisure\.com/hotels-resorts/.*",
        ],
        block_patterns=[
            r"/tag/",
            r"/author/",
            r"/newsletter/",
            r"/flights/",
            r"/cruises/",
        ]
        + _SOCIAL,
        link_patterns=[r"/hotels-resorts/[a-z0-9-]+"],
        max_pages=30,
        source_type="",
    ),
    "Travel Pulse - Hotels": SourcePatterns(
        gold_patterns=[r"travelpulse\.com/.*hotel.*", r"travelpulse\.com/.*resort.*"],
        block_patterns=[r"/cruises/", r"/travel-agents/"] + _SOCIAL,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=20,
        source_type="",
    ),
    "Conde Nast Traveler - Hotels": SourcePatterns(
        gold_patterns=[
            r"cntraveler\.com/.*hotel.*open",
            r"cntraveler\.com/.*new.*hotel",
        ],
        block_patterns=[r"/newsletter/", r"/subscribe/", r"/video/", r"/cruises/"]
        + _SOCIAL,
        link_patterns=[r"/story/[a-z0-9-]+"],
        max_pages=20,
        source_type="",
    ),
    "Northstar Meetings - Hotels": SourcePatterns(
        gold_patterns=[
            r"northstarmeetingsgroup\.com/.*hotel",
            r"northstarmeetingsgroup\.com/.*opening",
        ],
        block_patterns=[r"/events/", r"/subscribe/"] + _SOCIAL,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=20,
        source_type="",
    ),
    # NEW: Travel sources
    "Travel Weekly - Hotels": SourcePatterns(
        gold_patterns=[
            r"travelweekly\.com/.*hotel.*open",
            r"travelweekly\.com/.*resort.*new",
        ],
        block_patterns=[r"/cruises/", r"/tours/", r"/subscribe/", r"/events/"]
        + _SOCIAL,
        link_patterns=[r"/hotel-news/[a-z0-9-]+", r"/travel-news/[a-z0-9-]+"],
        max_pages=20,
        source_type="",
    ),
    "Luxury Travel Advisor": SourcePatterns(
        gold_patterns=[
            r"luxurytraveladvisor\.com/.*hotel.*open",
            r"luxurytraveladvisor\.com/.*resort.*new",
        ],
        block_patterns=[r"/subscribe/", r"/events/", r"/awards/"] + _SOCIAL,
        link_patterns=[r"/hotels/[a-z0-9-]+", r"/opening/[a-z0-9-]+"],
        max_pages=20,
        source_type="",
    ),
    # ═════════════════════════════════════════════════
    # AGGREGATORS (3 + 2 NEW)  — Dynamic year!
    # ═════════════════════════════════════════════════
    "The Orange Studio - Hotel Openings": SourcePatterns(
        gold_patterns=[r"/hotel-openings$", r"/hotel-openings\?"],
        block_patterns=[r"/about", r"/contact", r"/privacy"],
        link_patterns=[],
        max_pages=1,
        source_type="aggregator",
    ),
    f"New Hotels {_CUR_YEAR}": SourcePatterns(
        gold_patterns=[rf"/hotel-openings-{_CUR_YEAR}", r"/[a-z0-9-]+-hotel"],
        block_patterns=[r"/about", r"/contact"],
        link_patterns=[r"/[a-z0-9-]+-hotel"],
        max_pages=100,
        source_type="aggregator",
    ),
    f"New Hotels {_NEXT_YEAR}": SourcePatterns(
        gold_patterns=[rf"/hotel-openings-{_NEXT_YEAR}", r"/[a-z0-9-]+-hotel"],
        block_patterns=[r"/about", r"/contact"],
        link_patterns=[r"/[a-z0-9-]+-hotel"],
        max_pages=50,
        source_type="aggregator",
    ),
    # NEW: Additional aggregators
    "Top Hotel Projects": SourcePatterns(
        gold_patterns=[
            r"tophotelprojects\.com/.*hotel",
            r"tophotelprojects\.com/.*resort",
        ],
        block_patterns=[r"/login", r"/subscribe", r"/pricing"] + _SOCIAL,
        link_patterns=[r"/en/[a-z0-9-]+"],
        max_pages=30,
        source_type="aggregator",
    ),
    "Top Hotel News": SourcePatterns(
        gold_patterns=[
            r"tophotelnews\.com/.*hotel.*open",
            r"tophotelnews\.com/.*resort.*new",
        ],
        block_patterns=[r"/subscribe/", r"/advertise/"] + _SOCIAL,
        link_patterns=[r"/[a-z0-9-]+-hotel"],
        max_pages=25,
        source_type="aggregator",
    ),
    # ═════════════════════════════════════════════════
    # FLORIDA (2 + 5 NEW) — PRIMARY MARKET
    # ═════════════════════════════════════════════════
    "South Florida Biz Journal - Hotels": SourcePatterns(
        gold_patterns=[r"/southflorida/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html"],
        block_patterns=[r"bizjournals\.com/(?!southflorida)"] + _BIZJOURNAL,
        link_patterns=[
            r"/southflorida/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html",
            r"/southflorida/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html",
        ],
        max_pages=25,
        source_type="florida",
    ),
    "Orlando Biz Journal - Hotels": SourcePatterns(
        gold_patterns=[r"/orlando/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html"],
        block_patterns=[r"bizjournals\.com/(?!orlando)"] + _BIZJOURNAL,
        link_patterns=[
            r"/orlando/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html",
            r"/orlando/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html",
        ],
        max_pages=25,
        source_type="florida",
    ),
    # NEW: More Florida coverage
    "Tampa Bay Biz Journal - Hotels": SourcePatterns(
        gold_patterns=[r"/tampabay/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html"],
        block_patterns=[r"bizjournals\.com/(?!tampabay)"] + _BIZJOURNAL,
        link_patterns=[
            r"/tampabay/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html",
            r"/tampabay/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html",
        ],
        max_pages=25,
        source_type="florida",
    ),
    "Jacksonville Biz Journal - Hotels": SourcePatterns(
        gold_patterns=[r"/jacksonville/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html"],
        block_patterns=[r"bizjournals\.com/(?!jacksonville)"] + _BIZJOURNAL,
        link_patterns=[r"/jacksonville/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html"],
        max_pages=20,
        source_type="florida",
    ),
    "The Real Deal - South Florida": SourcePatterns(
        gold_patterns=[
            r"therealdeal\.com/miami/\d{4}/\d{2}/\d{2}/.*hotel",
            r"therealdeal\.com/miami/\d{4}/\d{2}/\d{2}/.*resort",
        ],
        block_patterns=[
            r"/new-york/",
            r"/los-angeles/",
            r"/chicago/",
            r"/texas/",
            r"/subscribe/",
        ]
        + _SOCIAL,
        link_patterns=[r"/miami/\d{4}/\d{2}/\d{2}/[a-z0-9-]+"],
        max_pages=25,
        source_type="florida",
    ),
    "Florida Trend - Hotels": SourcePatterns(
        gold_patterns=[
            r"floridatrend\.com/.*hotel",
            r"floridatrend\.com/.*resort",
            r"floridatrend\.com/.*tourism",
        ],
        block_patterns=[r"/subscribe/", r"/events/", r"/best-companies/"] + _SOCIAL,
        link_patterns=[r"/article/\d+/[a-z0-9-]+"],
        max_pages=20,
        source_type="florida",
    ),
    "Commercial Observer - Hotels": SourcePatterns(
        gold_patterns=[
            r"commercialobserver\.com/\d{4}/\d{2}/.*hotel",
            r"commercialobserver\.com/\d{4}/\d{2}/.*resort",
        ],
        block_patterns=[r"/events/", r"/subscribe/", r"/commercial-observer-awards/"]
        + _SOCIAL,
        link_patterns=[r"/\d{4}/\d{2}/[a-z0-9-]+"],
        max_pages=20,
        source_type="business",
    ),
    # ═════════════════════════════════════════════════
    # CARIBBEAN (4 + 2 NEW)
    # ═════════════════════════════════════════════════
    "Caribbean Journal - Hotels": SourcePatterns(
        gold_patterns=[
            r"caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel",
            r"caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-resort",
            r"caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-opening",
        ],
        block_patterns=[
            r"/tag/",
            r"/author/",
            r"/destination/",
            r"-cheap-flights",
            r"-cruise",
        ],
        link_patterns=[r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+"],
        max_pages=30,
        source_type="caribbean",
    ),
    "Caribbean Journal - Homepage": SourcePatterns(
        gold_patterns=[r"caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel"],
        block_patterns=[r"/tag/", r"/author/", r"-cruise"],
        link_patterns=[r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+"],
        max_pages=20,
        source_type="caribbean",
    ),
    "Caribbean Hotel & Tourism Association": SourcePatterns(
        gold_patterns=[
            r"caribbeanhotelandtourism\.com/.*hotel",
            r"caribbeanhotelandtourism\.com/\d{4}/\d{2}/\d{2}/",
        ],
        block_patterns=[r"/tag/", r"/about/", r"/membership/"] + _SOCIAL,
        link_patterns=[r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+/"],
        max_pages=20,
        source_type="caribbean",
    ),
    "Sandals Press": SourcePatterns(
        gold_patterns=[r"/press-releases/[a-z0-9-]+"],
        block_patterns=[r"/resorts/", r"/booking/", r"/deals/", r"/weddings/"]
        + _SOCIAL,
        link_patterns=[r"/press-releases/[a-z0-9-]+"],
        max_pages=15,
        source_type="caribbean",
    ),
    # NEW: Caribbean sources
    "Bahamas Ministry of Tourism": SourcePatterns(
        gold_patterns=[r"bahamas\.com/.*press", r"bahamas\.com/.*news"],
        block_patterns=[r"/deals/", r"/islands/", r"/plan-your-trip/", r"/events/"]
        + _SOCIAL,
        link_patterns=[r"/press/[a-z0-9-]+", r"/news/[a-z0-9-]+"],
        max_pages=15,
        source_type="caribbean",
    ),
    "Travel Weekly Caribbean": SourcePatterns(
        gold_patterns=[
            r"travelweekly\.com/Caribbean-Travel/.*hotel",
            r"travelweekly\.com/Caribbean-Travel/.*resort",
        ],
        block_patterns=[r"/subscribe/", r"/events/"] + _SOCIAL,
        link_patterns=[r"/Caribbean-Travel/[a-z0-9-]+"],
        max_pages=20,
        source_type="caribbean",
    ),
    # ═════════════════════════════════════════════════
    # WIRE SERVICES (NEW - 4)
    # ═════════════════════════════════════════════════
    "PR Newswire - Hotels": SourcePatterns(
        gold_patterns=[
            r"prnewswire\.com/news-releases/.*hotel",
            r"prnewswire\.com/news-releases/.*resort",
            r"prnewswire\.com/news-releases/.*hospitality",
        ],
        block_patterns=_WIRE,
        link_patterns=[r"/news-releases/[a-z0-9-]+"],
        max_pages=30,
        source_type="chain_newsroom",
    ),
    "Business Wire - Hotels": SourcePatterns(
        gold_patterns=[
            r"businesswire\.com/news/.*hotel",
            r"businesswire\.com/news/.*resort",
            r"businesswire\.com/news/.*hospitality",
        ],
        block_patterns=_WIRE,
        link_patterns=[r"/news/home/\d+"],
        max_pages=30,
        source_type="chain_newsroom",
    ),
    "Globe Newswire - Hotels": SourcePatterns(
        gold_patterns=[r"globenewswire\.com/.*hotel", r"globenewswire\.com/.*resort"],
        block_patterns=_WIRE,
        link_patterns=[r"/news-release/\d+"],
        max_pages=25,
        source_type="chain_newsroom",
    ),
    "Google News - Hotel Openings": SourcePatterns(
        gold_patterns=[
            r"news\.google\.com/.*hotel.*open",
            r"news\.google\.com/.*resort.*new",
        ],
        block_patterns=[r"/settings/", r"/saved/"] + _SOCIAL,
        link_patterns=[],
        max_pages=20,
        source_type="aggregator",
    ),
    # ═════════════════════════════════════════════════
    # REAL ESTATE / DEVELOPMENT (NEW - 3)
    # ═════════════════════════════════════════════════
    "The Real Deal - National": SourcePatterns(
        gold_patterns=[
            r"therealdeal\.com/\d{4}/\d{2}/\d{2}/.*hotel",
            r"therealdeal\.com/\d{4}/\d{2}/\d{2}/.*resort",
        ],
        block_patterns=[r"/subscribe/", r"/events/", r"/magazine/"] + _SOCIAL,
        link_patterns=[r"/\d{4}/\d{2}/\d{2}/[a-z0-9-]+"],
        max_pages=25,
        source_type="business",
    ),
    "Hospitality Investor": SourcePatterns(
        gold_patterns=[
            r".*hospitalityinvestor.*hotel",
            r".*hospitalityinvestor.*development",
        ],
        block_patterns=[r"/events/", r"/subscribe/"] + _SOCIAL,
        link_patterns=[r"/news/[a-z0-9-]+"],
        max_pages=20,
        source_type="business",
    ),
    "Hotel News Now (CoStar)": SourcePatterns(
        gold_patterns=[
            r"hotelnewsnow\.com/article/\d+",
            r"hotelnewsnow\.com/.*hotel.*open",
        ],
        block_patterns=[r"/login", r"/subscribe"] + _SOCIAL,
        link_patterns=[r"/article/\d+"],
        max_pages=25,
        source_type="industry",
    ),
}


# ─────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────


def get_patterns(source_name: str) -> Optional[SourcePatterns]:
    """Get patterns for a source, or None if not configured."""
    return SOURCE_PATTERNS.get(source_name)


def get_gold_patterns(source_name: str) -> List[str]:
    patterns = SOURCE_PATTERNS.get(source_name)
    return patterns.gold_patterns if patterns else []


def get_block_patterns(source_name: str) -> List[str]:
    patterns = SOURCE_PATTERNS.get(source_name)
    return patterns.block_patterns if patterns else []


def get_link_patterns(source_name: str) -> List[str]:
    patterns = SOURCE_PATTERNS.get(source_name)
    return patterns.link_patterns if patterns else []


def get_max_pages(source_name: str) -> int:
    patterns = SOURCE_PATTERNS.get(source_name)
    return patterns.max_pages if patterns else 30


def get_source_type(source_name: str) -> str:
    """Get the source type for extraction hint matching."""
    patterns = SOURCE_PATTERNS.get(source_name)
    return patterns.source_type if patterns else ""


def has_patterns(source_name: str) -> bool:
    return source_name in SOURCE_PATTERNS


def list_configured_sources() -> List[str]:
    return list(SOURCE_PATTERNS.keys())


def list_by_type(source_type: str) -> List[str]:
    """List all sources of a given type."""
    return [name for name, p in SOURCE_PATTERNS.items() if p.source_type == source_type]


DEFAULT_PATTERNS = SourcePatterns(
    gold_patterns=[r"/news/", r"/press/", r"/releases/", r"/\d{4}/\d{2}/"],
    block_patterns=_GENERIC,
    link_patterns=[r"/news/", r"/press/", r"/article/"],
    max_pages=20,
    source_type="",
)


def get_patterns_with_default(source_name: str) -> SourcePatterns:
    """Get patterns for a source, falling back to defaults."""
    return SOURCE_PATTERNS.get(source_name, DEFAULT_PATTERNS)


def print_summary():
    """Print a summary of all configured sources."""
    print("=" * 75)
    print("SOURCE PATTERNS CONFIGURATION")
    print("=" * 75)
    print(f"\nTotal sources configured: {len(SOURCE_PATTERNS)}")

    # Group by source_type
    by_type: Dict[str, list] = {}
    for name, patterns in SOURCE_PATTERNS.items():
        st = patterns.source_type or "(generic)"
        by_type.setdefault(st, []).append(name)

    print("\nBy source type:")
    for st, names in sorted(by_type.items()):
        print(f"  {st:20s} {len(names)} sources")

    print(f"\n{'Source':<50} {'Type':<16} {'Gold':<6} {'Block':<7} {'Max':<5}")
    print("-" * 85)
    for name, patterns in SOURCE_PATTERNS.items():
        print(
            f"{name:<50} {patterns.source_type or '-':<16} {len(patterns.gold_patterns):<6} {len(patterns.block_patterns):<7} {patterns.max_pages:<5}"
        )
    print("-" * 85)


if __name__ == "__main__":
    print_summary()
