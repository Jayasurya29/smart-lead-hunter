"""
SMART LEAD HUNTER - SOURCE TUNING CONFIGURATION
================================================
Each website has a unique structure. This file maps WHERE THE GOLD IS
for each source - the specific URL patterns that contain actual hotel leads.

PHILOSOPHY:
- Every source is different
- We must identify the EXACT paths where leads live
- Block everything else to save API calls
- Maximize lead yield per API call

Last Updated: January 2026
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum


class CrawlerType(Enum):
    """Types of crawlers available"""
    AUTO = "auto"              # SMART AUTO-SELECTION (recommended!)
    HTTPX = "httpx"            # Fast, for static HTML
    PLAYWRIGHT = "playwright"  # For JavaScript-heavy sites
    CRAWL4AI = "crawl4ai"      # Smart AI-ready scraping with caching


@dataclass
class SourceTuning:
    """Fine-tuned configuration for a specific source"""
    name: str
    entry_url: str                          # Where to start crawling
    additional_urls: List[str] = field(default_factory=list)  # Extra URLs to scrape
    crawler_type: CrawlerType = CrawlerType.AUTO  # AUTO by default!
    priority: int = 5                       # 1-10, higher = scrape first
    max_pages: int = 50                     # Max pages to scrape
    
    # WHERE THE GOLD IS - URL patterns that contain leads
    gold_patterns: List[str] = field(default_factory=list)
    
    # JUNK TO BLOCK - URL patterns to skip
    block_patterns: List[str] = field(default_factory=list)
    
    # Link patterns to follow from entry page
    link_patterns: List[str] = field(default_factory=list)
    
    # Content indicators - text that suggests page has hotel opening info
    content_indicators: List[str] = field(default_factory=lambda: [
        "opening", "debut", "unveil", "announce", "launch", "new hotel",
        "grand opening", "now open", "set to open", "slated to open",
        "breaking ground", "under construction", "renovation", "rebrand"
    ])
    
    # Notes about this source
    notes: str = ""


# =============================================================================
# TIER 1: INDUSTRY PUBLICATIONS - FINE TUNED
# =============================================================================

TUNED_SOURCES: Dict[str, SourceTuning] = {
    
    # -------------------------------------------------------------------------
    # HOSPITALITY NET - International focus, filter carefully
    # -------------------------------------------------------------------------
    "Hospitality Net": SourceTuning(
        name="Hospitality Net",
        entry_url="https://www.hospitalitynet.org/news/global.html",
        crawler_type=CrawlerType.AUTO,
        priority=8,
        max_pages=30,
        gold_patterns=[
            r'/announcement/\d+/',      # Hotel announcements - THE GOLD
            r'/news/\d+\.html',          # News articles with IDs
        ],
        block_patterns=[
            r'/organization/',           # Company profiles - NO LEADS
            r'/opinion/',                # Opinion pieces
            r'/video/',                  # Videos
            r'/event/',                  # Events/conferences
            r'/panel/',                  # Panel discussions
            r'/viewpoint/',              # Viewpoints
            r'/podcast/',                # Podcasts
            r'/hottopic/',               # Aggregation pages
            r'/360/',                    # 360 section
            r'/supplier/',               # Supplier profiles
            r'/list/',                   # Listing pages
            r'/about\.html',
            r'/contact\.html',
            r'/terms\.html',
            r'/privacy\.html',
            r'/rss\.html',
            r'/search\.html',
            r'/me/',                     # User account
        ],
        link_patterns=[r'announcement/\d+', r'news/\d+\.html'],
        notes="International focus - only ~5% USA/Caribbean content. Gold is in /announcement/"
    ),
    
    # -------------------------------------------------------------------------
    # HOTEL DIVE - Excellent US coverage, well-structured
    # -------------------------------------------------------------------------
    "Hotel Dive": SourceTuning(
        name="Hotel Dive",
        entry_url="https://www.hoteldive.com/topic/development/",
        crawler_type=CrawlerType.AUTO,
        priority=10,
        max_pages=50,
        gold_patterns=[
            r'/news/[a-z0-9-]+/\d+/',    # News articles - THE GOLD
        ],
        block_patterns=[
            r'/selfservice/',            # Article licensing
            r'/what-we-are-reading/',    # External links
            r'/library/',                # Resource library
            r'/events/',                 # Events
            r'/editors/',                # Editor profiles
            r'/signup/',                 # Signup
            r'/topic/[^/]+/page/',       # Pagination
            r'/press-release/',          # Press releases (less useful)
            r'/spons-content/',          # Sponsored content
        ],
        link_patterns=[r'/news/[a-z0-9-]+/\d+/'],
        notes="BEST US SOURCE - 43 leads from 48 pages. Gold is in /news/ articles"
    ),
    
    # -------------------------------------------------------------------------
    # LODGING MAGAZINE - AHLA official, excellent US coverage
    # -------------------------------------------------------------------------
    "LODGING Magazine": SourceTuning(
        name="LODGING Magazine",
        entry_url="https://lodgingmagazine.com/category/industrynews/",
        crawler_type=CrawlerType.AUTO,
        priority=10,
        max_pages=50,
        gold_patterns=[
            r'/\d{4}/\d{2}/[a-z0-9-]+/',  # Articles with date paths
            r'lodgingmagazine\.com/[a-z0-9-]+-hotel',  # Hotel-specific articles
            r'lodgingmagazine\.com/[a-z0-9-]+-opening',
            r'lodgingmagazine\.com/[a-z0-9-]+-debut',
        ],
        block_patterns=[
            r'/tag/',
            r'/category/',
            r'/author/',
            r'/page/\d+',
            r'/wp-admin/',
            r'/wp-content/',
            r'/advertise',
            r'/subscribe',
            r'/contact',
            r'/about',
        ],
        link_patterns=[r'/\d{4}/\d{2}/[a-z0-9-]+/'],
        notes="AHLA official publication - excellent US coverage"
    ),
    
    # -------------------------------------------------------------------------
    # CARIBBEAN JOURNAL - THE source for Caribbean
    # -------------------------------------------------------------------------
    "Caribbean Journal": SourceTuning(
        name="Caribbean Journal",
        # GO DIRECTLY TO THE GOLD - Hotel news category
        entry_url="https://www.caribjournal.com/category/hotels/",
        crawler_type=CrawlerType.AUTO,  # CRAWL4AI - smart scraping with caching
        priority=10,
        max_pages=30,  # Reduced - we're targeting, not exploring
        gold_patterns=[
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel',    # Hotel articles
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-resort',   # Resort articles
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-opening',  # Opening articles
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-marriott', # Marriott articles
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hyatt',    # Hyatt articles
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hilton',   # Hilton articles
        ],
        block_patterns=[
            # Block non-hotel content
            r'/tag/',
            r'/author/',
            r'/destination/',              # Destination pages - low quality
            r'/cj-invest-news/',           # Investment section index
            r'/cj-invest/',                # Investment portal
            r'/cji-',                      # CJI media kit, contact, etc
            r'/cta-',                      # CTA media kit, contact
            r'/caribbean-travel-advisor/', # Travel advisor section
            r'/caribbean/$',               # Caribbean index
            r'/memberful',                 # Login/auth pages
            r'-cheap-flights',             # Flight deals - not hotels
            r'-hiking-',                   # Hiking articles
            r'-wedding-',                  # Wedding articles
            r'-cruise',                    # Cruise articles
            r'lamborghini',                # Car articles
        ],
        # ONLY follow these links - this is the key to efficiency!
        link_patterns=[
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+',  # Article links - THE GOLD
            r'/category/hotels/page/\d+',      # Pagination - to find more articles
        ],
        notes="TARGETED: Entry at /category/hotels/, follows only hotel-related articles"
    ),
    
    # -------------------------------------------------------------------------
    # MARRIOTT NEWS CENTER - Direct from source
    # -------------------------------------------------------------------------
    "Marriott News": SourceTuning(
        name="Marriott News",
        entry_url="https://news.marriott.com/news/",
        crawler_type=CrawlerType.AUTO,  # CRAWL4AI - handles JS + caching
        priority=10,
        max_pages=30,
        gold_patterns=[
            r'/news/\d{4}/\d{2}/',        # News by date
            r'news\.marriott\.com/[a-z0-9-]+-open',
            r'news\.marriott\.com/[a-z0-9-]+-debut',
            r'news\.marriott\.com/[a-z0-9-]+-announce',
        ],
        block_patterns=[
            r'/category/',
            r'/tag/',
            r'/author/',
            r'/page/',
            r'/search/',
            r'/about/',
            r'/contact/',
        ],
        link_patterns=[r'/news/\d{4}/\d{2}/'],
        notes="30+ brands - Ritz-Carlton, St. Regis, W, EDITION, JW Marriott"
    ),
    
    # -------------------------------------------------------------------------
    # HILTON NEWSROOM - Growth & Development section is gold
    # -------------------------------------------------------------------------
    "Hilton Newsroom": SourceTuning(
        name="Hilton Newsroom",
        entry_url="https://stories.hilton.com/releases",
        crawler_type=CrawlerType.AUTO,
        priority=10,
        max_pages=30,
        gold_patterns=[
            r'/releases/[a-z0-9-]+',      # Press releases - THE GOLD
            r'stories\.hilton\.com/[a-z0-9-]+-open',
            r'stories\.hilton\.com/[a-z0-9-]+-debut',
        ],
        block_patterns=[
            r'/brands/',                  # Brand pages (not news)
            r'/team-members/',
            r'/corporate/',
            r'/about/',
            r'/contact/',
            r'/search/',
        ],
        link_patterns=[r'/releases/[a-z0-9-]+'],
        notes="22+ brands - Waldorf, Conrad, LXR, Curio. Gold in /releases/"
    ),
    
    # -------------------------------------------------------------------------
    # HYATT NEWSROOM
    # -------------------------------------------------------------------------
    "Hyatt Newsroom": SourceTuning(
        name="Hyatt Newsroom",
        entry_url="https://newsroom.hyatt.com/news-releases",
        crawler_type=CrawlerType.AUTO,
        priority=10,
        max_pages=30,
        gold_patterns=[
            r'/news-releases/[a-z0-9-]+',
            r'newsroom\.hyatt\.com/[a-z0-9-]+-open',
            r'newsroom\.hyatt\.com/[a-z0-9-]+-debut',
        ],
        block_patterns=[
            r'/media-contacts/',
            r'/about/',
            r'/corporate/',
            r'/search/',
        ],
        link_patterns=[r'/news-releases/[a-z0-9-]+'],
        notes="25+ brands - Park Hyatt, Andaz, Thompson, Alila. 148K room pipeline"
    ),
    
    # -------------------------------------------------------------------------
    # IHG NEWS
    # -------------------------------------------------------------------------
    "IHG News": SourceTuning(
        name="IHG News",
        entry_url="https://www.ihgplc.com/en/news-and-media/news-releases",
        crawler_type=CrawlerType.AUTO,
        priority=9,
        max_pages=30,
        gold_patterns=[
            r'/news-releases/\d{4}/',
            r'ihgplc\.com/[a-z0-9-]+-open',
            r'ihgplc\.com/[a-z0-9-]+-hotel',
        ],
        block_patterns=[
            r'/investors/',
            r'/responsibility/',
            r'/about/',
            r'/careers/',
        ],
        link_patterns=[r'/news-releases/\d{4}/'],
        notes="17+ brands - InterContinental, Kimpton, Six Senses, Regent"
    ),
    
    # -------------------------------------------------------------------------
    # FOUR SEASONS PRESS ROOM
    # -------------------------------------------------------------------------
    "Four Seasons Press": SourceTuning(
        name="Four Seasons Press",
        entry_url="https://press.fourseasons.com/news-releases/",
        crawler_type=CrawlerType.AUTO,
        priority=10,
        max_pages=30,
        gold_patterns=[
            r'/news-releases/[a-z0-9-]+',
            r'press\.fourseasons\.com/[a-z0-9-]+-open',
            r'press\.fourseasons\.com/[a-z0-9-]+-debut',
        ],
        block_patterns=[
            r'/media-contacts/',
            r'/about/',
            r'/search/',
        ],
        link_patterns=[r'/news-releases/[a-z0-9-]+'],
        notes="Ultra-luxury - Puerto Rico Nov 2025, Naples Beach Club Nov 2025"
    ),
    
    # -------------------------------------------------------------------------
    # FOUR SEASONS NEW OPENINGS - Follow "Learn More" links to property pages
    # -------------------------------------------------------------------------
    "Four Seasons New Openings": SourceTuning(
        name="Four Seasons New Openings",
        entry_url="https://www.fourseasons.com/newopenings/",
        crawler_type=CrawlerType.CRAWL4AI,  # JS-heavy carousel page
        priority=10,
        max_pages=15,  # Main page + ~9-12 property pages
        gold_patterns=[
            r'/newopenings/$',                           # Main carousel page
            r'/landing_pages/new-openings/',             # Property detail pages
            r'press\.fourseasons\.com/news-releases/.*new-four-seasons',  # Press releases
        ],
        block_patterns=[
            # Block booking/commerce
            r'/reservations/', r'/checkout/', r'/book/',
            # Block existing properties (not new openings)
            r'/offers/', r'/spa/', r'/dining/', r'/restaurants/',
            r'/meetings/', r'/weddings/', r'/shop/',
            # Block marketing pages
            r'/private-jet/', r'/yachts/', r'/residence-rentals/',
            r'/magazine/', r'/esg/', r'/campaigns/',
            r'/find_a_hotel/', r'/villa-residence/',
            # Block existing resort pages (these are NOT new openings)
            r'fourseasons\.com/[a-z]+/(?!landing_pages)',
        ],
        link_patterns=[
            # ONLY follow "Learn More" links to new opening detail pages
            r'/landing_pages/new-openings/',
            r'press\.fourseasons\.com/news-releases/.*new-four-seasons',
        ],
        notes="Carousel page with 9+ new properties. Follow 'Learn More' links to /landing_pages/new-openings/ for details."
    ),
    
    # -------------------------------------------------------------------------
    # THE ORANGE STUDIO - Best visual aggregator
    # -------------------------------------------------------------------------
    "Orange Studio": SourceTuning(
        name="Orange Studio",
        # Single page with all hotels - qualifier handles filtering
        entry_url="https://www.theorangestudio.com/hotel-openings",
        additional_urls=[],  # No additional URLs - main page has everything
        crawler_type=CrawlerType.CRAWL4AI,  # JS-rendered page
        priority=10,
        max_pages=1,  # Just the main page - it has ALL hotels
        gold_patterns=[
            r'/hotel-openings$',
            r'/hotel-openings\?',  # Also match filtered views
        ],
        block_patterns=[
            r'/about',
            r'/contact',
            r'/privacy',
            r'/terms',
            r'/news/',  # Skip news articles
            r'/hotel/',  # Skip individual hotel pages
        ],
        link_patterns=[],  # Don't crawl links
        notes="BEST AGGREGATOR - single page with 200+ hotels. URL filters are JS-only, use qualifier to filter."
    ),
    
    # -------------------------------------------------------------------------
    # BISNOW - Hotel real estate news
    # -------------------------------------------------------------------------
    "Bisnow Hotels": SourceTuning(
        name="Bisnow Hotels",
        entry_url="https://www.bisnow.com/tags/hotels",
        crawler_type=CrawlerType.AUTO,
        priority=9,
        max_pages=40,
        gold_patterns=[
            r'/[a-z-]+/news/[a-z0-9-]+',   # Regional news articles
            r'bisnow\.com/[a-z0-9-]+-hotel',
            r'bisnow\.com/[a-z0-9-]+-opening',
        ],
        block_patterns=[
            r'/events/',
            r'/jobs/',
            r'/advertise/',
            r'/about/',
            r'/contact/',
            r'/subscribe/',
            r'/page/\d+',
        ],
        link_patterns=[r'/[a-z-]+/news/[a-z0-9-]+'],
        notes="Excellent free CRE coverage - Ian Schrager/Highgate JV Jan 2026"
    ),
    
    # -------------------------------------------------------------------------
    # COSTAR HOTELS - Premium but some free content
    # -------------------------------------------------------------------------
    "CoStar Hotels": SourceTuning(
        name="CoStar Hotels",
        entry_url="https://www.costar.com/article/topic/hotels",
        crawler_type=CrawlerType.AUTO,  # CRAWL4AI - handles JS
        priority=9,
        max_pages=30,
        gold_patterns=[
            r'/article/\d+/',
            r'costar\.com/article/[a-z0-9-]+-hotel',
        ],
        block_patterns=[
            r'/login',
            r'/subscribe',
            r'/pricing',
            r'/about',
        ],
        link_patterns=[r'/article/\d+/'],
        notes="Premium data but some free articles"
    ),
    
    # -------------------------------------------------------------------------
    # TRAVEL + LEISURE - Luxury focus
    # -------------------------------------------------------------------------
    "Travel + Leisure": SourceTuning(
        name="Travel + Leisure",
        entry_url="https://www.travelandleisure.com/hotels-resorts/hotel-openings",
        crawler_type=CrawlerType.AUTO,
        priority=9,
        max_pages=40,
        gold_patterns=[
            r'/hotels-resorts/[a-z0-9-]+-opening',
            r'/hotels-resorts/[a-z0-9-]+-hotel',
            r'/hotels-resorts/new-[a-z0-9-]+',
        ],
        block_patterns=[
            r'/tag/',
            r'/author/',
            r'/page/',
            r'/newsletter/',
            r'/subscribe/',
            r'/advertise/',
        ],
        link_patterns=[r'/hotels-resorts/[a-z0-9-]+'],
        notes="Annual It List - 36 North American hotels 2024"
    ),
    
    # -------------------------------------------------------------------------
    # FLORIDA BUSINESS JOURNALS - YOUR CORE MARKET
    # FIXED: Added strict patterns to block other cities and non-hotel content
    # -------------------------------------------------------------------------
    "South Florida Business Journal": SourceTuning(
        name="South Florida Business Journal",
        entry_url="https://www.bizjournals.com/southflorida/news/industry/hotels",
        crawler_type=CrawlerType.CRAWL4AI,  # Required - BizJournals blocks httpx
        priority=10,
        max_pages=25,
        gold_patterns=[
            # ONLY match southflorida articles with date pattern
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html',
        ],
        block_patterns=[
            # Block ALL other BizJournals cities
            r'bizjournals\.com/albany', r'bizjournals\.com/albuquerque',
            r'bizjournals\.com/atlanta', r'bizjournals\.com/austin',
            r'bizjournals\.com/baltimore', r'bizjournals\.com/birmingham',
            r'bizjournals\.com/boston', r'bizjournals\.com/buffalo',
            r'bizjournals\.com/charlotte', r'bizjournals\.com/chicago',
            r'bizjournals\.com/cincinnati', r'bizjournals\.com/cleveland',
            r'bizjournals\.com/columbus', r'bizjournals\.com/dallas',
            r'bizjournals\.com/dayton', r'bizjournals\.com/denver',
            r'bizjournals\.com/detroit', r'bizjournals\.com/houston',
            r'bizjournals\.com/indianapolis', r'bizjournals\.com/jacksonville',
            r'bizjournals\.com/kansascity', r'bizjournals\.com/losangeles',
            r'bizjournals\.com/louisville', r'bizjournals\.com/memphis',
            r'bizjournals\.com/milwaukee', r'bizjournals\.com/minneapolis',
            r'bizjournals\.com/nashville', r'bizjournals\.com/newyork',
            r'bizjournals\.com/orlando', r'bizjournals\.com/pacific',
            r'bizjournals\.com/philadelphia', r'bizjournals\.com/phoenix',
            r'bizjournals\.com/pittsburgh', r'bizjournals\.com/portland',
            r'bizjournals\.com/raleigh', r'bizjournals\.com/richmond',
            r'bizjournals\.com/sacramento', r'bizjournals\.com/sanantonio',
            r'bizjournals\.com/sanfrancisco', r'bizjournals\.com/sanjose',
            r'bizjournals\.com/seattle', r'bizjournals\.com/stlouis',
            r'bizjournals\.com/tampabay', r'bizjournals\.com/twincities',
            r'bizjournals\.com/washington', r'bizjournals\.com/wichita',
            # Block non-hotel industry categories
            r'/news/banking', r'/news/technology', r'/news/health-care',
            r'/news/retail', r'/news/manufacturing', r'/news/energy',
            r'/news/education', r'/news/government', r'/news/professional',
            r'/news/media', r'/news/philanthropy', r'/news/sports',
            r'/news/transportation', r'/news/food-and-lifestyle',
            r'/news/career', r'/news/residential-real-estate',
            r'/news/commercial-real-estate', r'/news/feature/',
            r'/news/industry/undefined', r'/news/industry/southflorida',
            # Block junk
            r'/undefined', r'/null', r'/bizwomen/', r'/events/',
            r'/people/', r'/lists/', r'/subscribe/', r'/page/',
            r'/about/', r'/contact/', r'/advertise/', r'/help/',
        ],
        link_patterns=[
            # Only follow hotel-related article links
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*hospitality.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*hilton.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*marriott.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*hyatt.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*four.?seasons.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*ritz.*\.html',
            r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*opening.*\.html',
        ],
        notes="YOUR CORE MARKET - Miami/Fort Lauderdale/Palm Beach. FIXED with strict city filtering."
    ),
    
    "Orlando Business Journal": SourceTuning(
        name="Orlando Business Journal",
        entry_url="https://www.bizjournals.com/orlando/news/industry/hotels",
        crawler_type=CrawlerType.CRAWL4AI,  # Required - BizJournals blocks httpx
        priority=10,
        max_pages=25,
        gold_patterns=[
            # ONLY match orlando articles with date pattern
            r'/orlando/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html',
        ],
        block_patterns=[
            # Block ALL other BizJournals cities (including other FL cities)
            r'bizjournals\.com/southflorida', r'bizjournals\.com/tampabay',
            r'bizjournals\.com/jacksonville',
            r'bizjournals\.com/albany', r'bizjournals\.com/albuquerque',
            r'bizjournals\.com/atlanta', r'bizjournals\.com/austin',
            r'bizjournals\.com/baltimore', r'bizjournals\.com/birmingham',
            r'bizjournals\.com/boston', r'bizjournals\.com/buffalo',
            r'bizjournals\.com/charlotte', r'bizjournals\.com/chicago',
            r'bizjournals\.com/cincinnati', r'bizjournals\.com/cleveland',
            r'bizjournals\.com/columbus', r'bizjournals\.com/dallas',
            r'bizjournals\.com/dayton', r'bizjournals\.com/denver',
            r'bizjournals\.com/detroit', r'bizjournals\.com/houston',
            r'bizjournals\.com/indianapolis', r'bizjournals\.com/kansascity',
            r'bizjournals\.com/losangeles', r'bizjournals\.com/louisville',
            r'bizjournals\.com/memphis', r'bizjournals\.com/milwaukee',
            r'bizjournals\.com/minneapolis', r'bizjournals\.com/nashville',
            r'bizjournals\.com/newyork', r'bizjournals\.com/pacific',
            r'bizjournals\.com/philadelphia', r'bizjournals\.com/phoenix',
            r'bizjournals\.com/pittsburgh', r'bizjournals\.com/portland',
            r'bizjournals\.com/raleigh', r'bizjournals\.com/richmond',
            r'bizjournals\.com/sacramento', r'bizjournals\.com/sanantonio',
            r'bizjournals\.com/sanfrancisco', r'bizjournals\.com/sanjose',
            r'bizjournals\.com/seattle', r'bizjournals\.com/stlouis',
            r'bizjournals\.com/twincities', r'bizjournals\.com/washington',
            r'bizjournals\.com/wichita',
            # Block non-hotel categories
            r'/news/banking', r'/news/technology', r'/news/health-care',
            r'/news/retail', r'/news/manufacturing', r'/news/energy',
            r'/news/education', r'/news/government', r'/news/professional',
            r'/news/media', r'/news/philanthropy', r'/news/sports',
            r'/news/transportation', r'/news/food-and-lifestyle',
            r'/news/career', r'/news/residential-real-estate',
            r'/news/commercial-real-estate', r'/news/feature/',
            r'/undefined', r'/null', r'/bizwomen/', r'/events/',
            r'/people/', r'/lists/', r'/subscribe/', r'/page/',
        ],
        link_patterns=[
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html',
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html',
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*disney.*\.html',
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*universal.*\.html',
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*theme.*\.html',
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*hospitality.*\.html',
            r'/orlando/news/\d{4}/\d{2}/\d{2}/.*opening.*\.html',
        ],
        notes="YOUR CORE MARKET - Orlando/Central Florida. FIXED with strict city filtering."
    ),
    
    "Tampa Bay Business Journal": SourceTuning(
        name="Tampa Bay Business Journal",
        entry_url="https://www.bizjournals.com/tampabay/news/industry/hotels",
        crawler_type=CrawlerType.CRAWL4AI,  # Required - BizJournals blocks httpx
        priority=9,
        max_pages=25,
        gold_patterns=[
            # ONLY match tampabay articles with date pattern
            r'/tampabay/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html',
        ],
        block_patterns=[
            # Block ALL other BizJournals cities
            r'bizjournals\.com/southflorida', r'bizjournals\.com/orlando',
            r'bizjournals\.com/jacksonville',
            r'bizjournals\.com/albany', r'bizjournals\.com/albuquerque',
            r'bizjournals\.com/atlanta', r'bizjournals\.com/austin',
            r'bizjournals\.com/baltimore', r'bizjournals\.com/birmingham',
            r'bizjournals\.com/boston', r'bizjournals\.com/buffalo',
            r'bizjournals\.com/charlotte', r'bizjournals\.com/chicago',
            r'bizjournals\.com/cincinnati', r'bizjournals\.com/cleveland',
            r'bizjournals\.com/columbus', r'bizjournals\.com/dallas',
            r'bizjournals\.com/dayton', r'bizjournals\.com/denver',
            r'bizjournals\.com/detroit', r'bizjournals\.com/houston',
            r'bizjournals\.com/indianapolis', r'bizjournals\.com/kansascity',
            r'bizjournals\.com/losangeles', r'bizjournals\.com/louisville',
            r'bizjournals\.com/memphis', r'bizjournals\.com/milwaukee',
            r'bizjournals\.com/minneapolis', r'bizjournals\.com/nashville',
            r'bizjournals\.com/newyork', r'bizjournals\.com/pacific',
            r'bizjournals\.com/philadelphia', r'bizjournals\.com/phoenix',
            r'bizjournals\.com/pittsburgh', r'bizjournals\.com/portland',
            r'bizjournals\.com/raleigh', r'bizjournals\.com/richmond',
            r'bizjournals\.com/sacramento', r'bizjournals\.com/sanantonio',
            r'bizjournals\.com/sanfrancisco', r'bizjournals\.com/sanjose',
            r'bizjournals\.com/seattle', r'bizjournals\.com/stlouis',
            r'bizjournals\.com/twincities', r'bizjournals\.com/washington',
            r'bizjournals\.com/wichita',
            # Block non-hotel categories
            r'/news/banking', r'/news/technology', r'/news/health-care',
            r'/news/retail', r'/news/manufacturing', r'/news/energy',
            r'/news/education', r'/news/government', r'/news/professional',
            r'/news/media', r'/news/philanthropy', r'/news/sports',
            r'/news/transportation', r'/news/food-and-lifestyle',
            r'/news/career', r'/news/residential-real-estate',
            r'/news/commercial-real-estate', r'/news/feature/',
            r'/undefined', r'/null', r'/bizwomen/', r'/events/',
            r'/people/', r'/lists/', r'/subscribe/', r'/page/',
        ],
        link_patterns=[
            r'/tampabay/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html',
            r'/tampabay/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html',
            r'/tampabay/news/\d{4}/\d{2}/\d{2}/.*hospitality.*\.html',
            r'/tampabay/news/\d{4}/\d{2}/\d{2}/.*opening.*\.html',
        ],
        notes="YOUR CORE MARKET - Tampa Bay. FIXED with strict city filtering."
    ),
    
    # -------------------------------------------------------------------------
    # CARIBBEAN SOURCES
    # -------------------------------------------------------------------------
    "Caribbean Hotel & Tourism Association": SourceTuning(
        name="Caribbean Hotel & Tourism Association",
        entry_url="https://caribbeanhotelandtourism.com/category/news/",
        crawler_type=CrawlerType.AUTO,
        priority=9,
        max_pages=30,
        gold_patterns=[
            r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+/',
            r'caribbeanhotelandtourism\.com/[a-z0-9-]+-hotel',
            r'caribbeanhotelandtourism\.com/[a-z0-9-]+-resort',
        ],
        block_patterns=[
            r'/tag/',
            r'/category/$',
            r'/author/',
            r'/page/',
            r'/about/',
            r'/contact/',
            r'/membership/',
        ],
        link_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+/'],
        notes="CHTA - 1,000+ member properties"
    ),
    
    "Sandals Press": SourceTuning(
        name="Sandals Press",
        entry_url="https://www.sandals.com/press-releases/",
        crawler_type=CrawlerType.AUTO,
        priority=8,
        max_pages=20,
        gold_patterns=[
            r'/press-releases/[a-z0-9-]+',
        ],
        block_patterns=[
            r'/resorts/',
            r'/booking/',
            r'/deals/',
            r'/weddings/',
        ],
        link_patterns=[r'/press-releases/[a-z0-9-]+'],
        notes="Major Caribbean all-inclusive - Sandals & Beaches"
    ),
    
    # -------------------------------------------------------------------------
    # NEW HOTELS OPENING - By year
    # -------------------------------------------------------------------------
    "New Hotels 2026": SourceTuning(
        name="New Hotels 2026",
        entry_url="https://www.newhotelsopening.com/hotel-openings-2026",
        crawler_type=CrawlerType.AUTO,
        priority=10,
        max_pages=100,
        gold_patterns=[
            r'/hotel-openings-2026',
            r'/[a-z0-9-]+-hotel',
        ],
        block_patterns=[
            r'/about',
            r'/contact',
            r'/privacy',
        ],
        link_patterns=[r'/[a-z0-9-]+-hotel'],
        notes="2026 openings - HIGH PRIORITY current year"
    ),
    
    "New Hotels 2027": SourceTuning(
        name="New Hotels 2027",
        entry_url="https://www.newhotelsopening.com/hotel-openings-2027",
        crawler_type=CrawlerType.AUTO,
        priority=8,
        max_pages=50,
        gold_patterns=[
            r'/hotel-openings-2027',
            r'/[a-z0-9-]+-hotel',
        ],
        block_patterns=[
            r'/about',
            r'/contact',
            r'/privacy',
        ],
        link_patterns=[r'/[a-z0-9-]+-hotel'],
        notes="2027 openings - future pipeline"
    ),
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_tuned_source(name: str) -> Optional[SourceTuning]:
    """Get tuning config for a source by name"""
    return TUNED_SOURCES.get(name)


def get_all_tuned_sources() -> Dict[str, SourceTuning]:
    """Get all tuned sources"""
    return TUNED_SOURCES


def get_high_priority_tuned_sources(min_priority: int = 8) -> Dict[str, SourceTuning]:
    """Get tuned sources with priority >= min_priority"""
    return {k: v for k, v in TUNED_SOURCES.items() if v.priority >= min_priority}


def get_florida_sources() -> Dict[str, SourceTuning]:
    """Get sources focused on Florida market"""
    florida_keywords = ['florida', 'south florida', 'orlando', 'tampa', 'miami']
    return {k: v for k, v in TUNED_SOURCES.items() 
            if any(kw in v.name.lower() or kw in v.notes.lower() for kw in florida_keywords)}


def get_caribbean_sources() -> Dict[str, SourceTuning]:
    """Get sources focused on Caribbean market"""
    caribbean_keywords = ['caribbean', 'carib', 'sandals', 'jamaica', 'bahamas', 'aruba']
    return {k: v for k, v in TUNED_SOURCES.items() 
            if any(kw in v.name.lower() or kw in v.notes.lower() for kw in caribbean_keywords)}


def print_tuning_summary():
    """Print summary of all tuned sources"""
    print("=" * 70)
    print("SMART LEAD HUNTER - SOURCE TUNING SUMMARY")
    print("=" * 70)
    
    print(f"\n📊 TOTAL TUNED SOURCES: {len(TUNED_SOURCES)}")
    
    # By priority
    high_pri = get_high_priority_tuned_sources(9)
    med_pri = {k: v for k, v in TUNED_SOURCES.items() if 7 <= v.priority < 9}
    
    print(f"\n⭐ HIGH PRIORITY (9-10): {len(high_pri)}")
    for name, config in high_pri.items():
        print(f"   [{config.priority}] {name}")
    
    print(f"\n📌 MEDIUM PRIORITY (7-8): {len(med_pri)}")
    for name, config in med_pri.items():
        print(f"   [{config.priority}] {name}")
    
    # Florida focus
    florida = get_florida_sources()
    print(f"\n🌴 FLORIDA SOURCES: {len(florida)}")
    for name in florida:
        print(f"   • {name}")
    
    # Caribbean focus
    caribbean = get_caribbean_sources()
    print(f"\n🏝️ CARIBBEAN SOURCES: {len(caribbean)}")
    for name in caribbean:
        print(f"   • {name}")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    print_tuning_summary()