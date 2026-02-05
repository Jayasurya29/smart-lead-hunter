"""
SMART LEAD HUNTER - SOURCE CONFIGURATION - UPDATED
===================================================
Now with patterns for ALL 79 database sources.
Previously only 14 had patterns (65 were wasting API calls!)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class SourcePatterns:
    gold_patterns: List[str] = field(default_factory=list)
    block_patterns: List[str] = field(default_factory=list)
    link_patterns: List[str] = field(default_factory=list)
    max_pages: int = 30

_SOCIAL = [r'facebook\.com', r'twitter\.com', r'instagram\.com', r'linkedin\.com', r'youtube\.com']
_FILES = [r'\.pdf$', r'\.jpg$', r'\.png$', r'\.gif$', r'\.mp4$']
_GENERIC = [r'/login', r'/signin', r'/signup', r'/subscribe', r'/contact', r'/about-us', r'/privacy', r'/terms', r'/cart', r'/checkout', r'/shop', r'/careers', r'/jobs', r'/tag/', r'/author/', r'/category/$', r'/page/\d+$', r'/search\?', r'/advertise'] + _SOCIAL + _FILES
_MARRIOTT = [r'/category/', r'/tag/', r'/author/', r'/page/', r'/search/', r'/subscribe/', r'/media-contacts/'] + _SOCIAL
_HILTON = [r'/team-members/', r'/corporate/', r'/about/', r'/responsibility/', r'/investors/'] + _SOCIAL
_BIZJOURNAL = [r'/news/banking', r'/news/technology', r'/news/health-care', r'/news/retail', r'/bizwomen/', r'/events/', r'/people/', r'/lists/', r'/subscribers', r'/account/'] + _SOCIAL

SOURCE_PATTERNS: Dict[str, SourcePatterns] = {
    # MARRIOTT
    "Marriott News - All Brands": SourcePatterns(gold_patterns=[r'news\.marriott\.com/news/\d{4}/\d{2}/', r'news\.marriott\.com/.*-open', r'news\.marriott\.com/.*-debut', r'news\.marriott\.com/.*-resort'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=30),
    "Marriott  - Ritz-Carlton": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/the-ritz-carlton/', r'news\.marriott\.com/news/\d{4}/\d{2}/.*ritz'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - St. Regis": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/st-regis/', r'news\.marriott\.com/news/\d{4}/\d{2}/.*regis'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - Luxury Collection": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/the-luxury-collection/'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - Autograph Collection": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/autograph-collection/'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - EDITION": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/edition/'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - W Hotels": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/w-hotels/'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - JW Marriott": SourcePatterns(gold_patterns=[r'news\.marriott\.com/brands/jw-marriott/'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=20),
    "Marriott - Tribute Portfolio": SourcePatterns(gold_patterns=[r'news\.marriott\.com/news/\d{4}/\d{2}/'], block_patterns=_MARRIOTT, link_patterns=[r'/news/\d{4}/\d{2}/'], max_pages=15),
    # HILTON
    "Hilton - Waldorf Astoria": SourcePatterns(gold_patterns=[r'stories\.hilton\.com/releases/[a-z0-9-]+'], block_patterns=_HILTON, link_patterns=[r'/releases/[a-z0-9-]+'], max_pages=20),
    "Hilton - Curio Collection": SourcePatterns(gold_patterns=[r'stories\.hilton\.com/releases/[a-z0-9-]+'], block_patterns=_HILTON, link_patterns=[r'/releases/[a-z0-9-]+'], max_pages=20),
    "Hilton - Conrad": SourcePatterns(gold_patterns=[r'stories\.hilton\.com/releases/[a-z0-9-]+'], block_patterns=_HILTON, link_patterns=[r'/releases/[a-z0-9-]+'], max_pages=20),
    "Hilton - Tempo": SourcePatterns(gold_patterns=[r'stories\.hilton\.com/releases/[a-z0-9-]+'], block_patterns=_HILTON, link_patterns=[r'/releases/[a-z0-9-]+'], max_pages=20),
    "Hilton - LXR": SourcePatterns(gold_patterns=[r'stories\.hilton\.com/releases/[a-z0-9-]+'], block_patterns=_HILTON, link_patterns=[r'/releases/[a-z0-9-]+'], max_pages=20),
    # OTHER CHAINS
    "Hyatt Newsroom - All Brands": SourcePatterns(gold_patterns=[r'newsroom\.hyatt\.com/news-releases/[a-z0-9-]+'], block_patterns=[r'/media-contacts/', r'/about/', r'/brand-pages/', r'/gallery/'] + _SOCIAL, link_patterns=[r'/news-releases/[a-z0-9-]+'], max_pages=30),
    "Hyatt Newsroom - Homepage": SourcePatterns(gold_patterns=[r'newsroom\.hyatt\.com/news-releases/[a-z0-9-]+'], block_patterns=[r'/media-contacts/', r'/about/'] + _SOCIAL, link_patterns=[r'/news-releases/[a-z0-9-]+'], max_pages=20),
    "IHG News & Media": SourcePatterns(gold_patterns=[r'ihgplc\.com/.*news.*\d{4}', r'ihgplc\.com/.*-hotel'], block_patterns=[r'/investors/', r'/responsibility/', r'/careers/'] + _SOCIAL, link_patterns=[r'/news-and-media/.*\d{4}'], max_pages=30),
    "Accor News Stories": SourcePatterns(gold_patterns=[r'group\.accor\.com/.*news-stories/.*'], block_patterns=[r'/investors/', r'/commitments/', r'/careers/'] + _SOCIAL, link_patterns=[r'/news-stories/[a-z0-9-]+'], max_pages=20),
    "Accor Press Releases": SourcePatterns(gold_patterns=[r'group\.accor\.com/.*press-releases/.*'], block_patterns=[r'/investors/', r'/careers/'] + _SOCIAL, link_patterns=[r'/press-releases/[a-z0-9-]+'], max_pages=20),
    "Ennismore News": SourcePatterns(gold_patterns=[r'ennismore\.com/news/[a-z0-9-]+'], block_patterns=[r'/brands/', r'/about/', r'/careers/'] + _SOCIAL, link_patterns=[r'/news/[a-z0-9-]+'], max_pages=20),
    "Wyndham Newsroom": SourcePatterns(gold_patterns=[r'wyndham.*news.*', r'wyndham.*press.*'], block_patterns=_GENERIC, link_patterns=[r'/news/[a-z0-9-]+'], max_pages=20),
    "Best Western Media Center": SourcePatterns(gold_patterns=[r'bestwestern.*media.*'], block_patterns=_GENERIC, link_patterns=[r'/media/[a-z0-9-]+'], max_pages=15),
    "Choice Hotels Newsroom": SourcePatterns(gold_patterns=[r'choicehotels.*news.*'], block_patterns=_GENERIC, link_patterns=[r'/news/[a-z0-9-]+'], max_pages=20),
    # LUXURY
    "Four Seasons - New Hotels Page": SourcePatterns(gold_patterns=[r'/landing/new-hotels'], block_patterns=[r'/reservations/', r'/checkout/', r'/spa/', r'/dining/', r'/weddings/'] + _SOCIAL, link_patterns=[], max_pages=5),
    "Four Seasons - News Releases": SourcePatterns(gold_patterns=[r'press\.fourseasons\.com/news-releases/.*'], block_patterns=[r'/media-contacts/', r'/about/'] + _SOCIAL, link_patterns=[r'/news-releases/[a-z0-9-]+'], max_pages=30),
    "Peninsula Hotels News": SourcePatterns(gold_patterns=[r'peninsula\.com/.*newsroom.*'], block_patterns=[r'/reservations/', r'/offers/', r'/dining/', r'/spa/'] + _SOCIAL, link_patterns=[r'/newsroom/[a-z0-9-]+'], max_pages=15),
    "Oetker Collection Press": SourcePatterns(gold_patterns=[r'oetkercollection\.com/press/.*'], block_patterns=[r'/reservations/', r'/offers/'] + _SOCIAL, link_patterns=[r'/press/[a-z0-9-]+'], max_pages=15),
    "Montage Hotels Press": SourcePatterns(gold_patterns=[r'montagehotels\.com/press/.*'], block_patterns=[r'/reservations/', r'/offers/'] + _SOCIAL, link_patterns=[r'/press/[a-z0-9-]+'], max_pages=15),
    "Auberge Resorts Press": SourcePatterns(gold_patterns=[r'aubergeresorts\.com/press/.*'], block_patterns=[r'/reservations/', r'/offers/'] + _SOCIAL, link_patterns=[r'/press/[a-z0-9-]+'], max_pages=15),
    "Rosewood Hotels Press": SourcePatterns(gold_patterns=[r'rosewoodhotelgroup\.com/.*press.*'], block_patterns=[r'/reservations/'] + _SOCIAL, link_patterns=[r'/press/[a-z0-9-]+'], max_pages=15),
    "Loews Hotels Press": SourcePatterns(gold_patterns=[r'loewshotels\.com/press-room/.*'], block_patterns=[r'/reservations/', r'/destinations/'] + _SOCIAL, link_patterns=[r'/press-room/[a-z0-9-]+'], max_pages=15),
    "SH Hotels News": SourcePatterns(gold_patterns=[r'shhotelsandresorts\.com/.*news.*'], block_patterns=[r'/reservations/'] + _SOCIAL, link_patterns=[r'/news/[a-z0-9-]+'], max_pages=15),
    "Aman Press": SourcePatterns(gold_patterns=[r'aman\.com/.*press.*'], block_patterns=[r'/reservations/', r'/offers/', r'/wellness/'] + _SOCIAL, link_patterns=[r'/press/[a-z0-9-]+'], max_pages=15),
    # INDUSTRY PUBLICATIONS
    "Hotel Dive": SourcePatterns(gold_patterns=[r'hoteldive\.com/news/[a-z0-9-]+/\d+/'], block_patterns=[r'/selfservice/', r'/library/', r'/events/', r'/editors/', r'/signup/', r'cfodive\.com', r'constructiondive\.com', r'retaildive\.com'], link_patterns=[r'/news/[a-z0-9-]+/\d+/'], max_pages=50),
    "Hotel Dive - News": SourcePatterns(gold_patterns=[r'hoteldive\.com/news/[a-z0-9-]+/\d+/'], block_patterns=[r'/selfservice/', r'/library/', r'/events/', r'/signup/', r'cfodive\.com', r'constructiondive\.com'], link_patterns=[r'/news/[a-z0-9-]+/\d+/'], max_pages=50),
    "Hotel Management - News": SourcePatterns(gold_patterns=[r'hotelmanagement\.net/.*-open', r'hotelmanagement\.net/.*-hotel', r'hotelmanagement\.net/development/'], block_patterns=[r'/white-papers/', r'/events/', r'/subscribe/'] + _SOCIAL, link_patterns=[r'/development/[a-z0-9-]+'], max_pages=30),
    "Hospitality Net": SourcePatterns(gold_patterns=[r'/announcement/\d+/', r'/news/\d+\.html'], block_patterns=[r'/organization/', r'/opinion/', r'/video/', r'/event/', r'/panel/', r'/viewpoint/', r'/podcast/', r'/supplier/', r'/me/'], link_patterns=[r'announcement/\d+', r'news/\d+\.html'], max_pages=30),
    "Hotel News Resource - Openings": SourcePatterns(gold_patterns=[r'hotelnewsresource\.com/article/.*'], block_patterns=[r'/directory/', r'/events/', r'/advertise/'] + _SOCIAL, link_patterns=[r'/article/[a-z0-9-]+'], max_pages=30),
    "Hotel News Resource - Florida": SourcePatterns(gold_patterns=[r'hotelnewsresource\.com/article/.*'], block_patterns=[r'/directory/', r'/events/'] + _SOCIAL, link_patterns=[r'/article/[a-z0-9-]+'], max_pages=30),
    "Bisnow Hotels": SourcePatterns(gold_patterns=[r'bisnow\.com/.*hotel.*', r'bisnow\.com/.*/news/[a-z0-9-]+'], block_patterns=[r'/events/', r'/jobs/', r'/advertise/', r'/subscribe/'] + _SOCIAL, link_patterns=[r'/[a-z-]+/news/[a-z0-9-]+'], max_pages=30),
    "CoStar Hotels": SourcePatterns(gold_patterns=[r'costar\.com/article/\d+'], block_patterns=[r'/login', r'/subscribe', r'/pricing'] + _SOCIAL, link_patterns=[r'/article/\d+'], max_pages=30),
    "Skift - Hotels": SourcePatterns(gold_patterns=[r'skift\.com/\d{4}/\d{2}/\d{2}/.*hotel', r'skift\.com/\d{4}/\d{2}/\d{2}/.*resort'], block_patterns=[r'/events/', r'/research/', r'/subscribe/', r'/skiftx/'] + _SOCIAL, link_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+'], max_pages=30),
    "LODGING Magazine": SourcePatterns(gold_patterns=[r'lodgingmagazine\.com/.*hotel', r'lodgingmagazine\.com/.*opening'], block_patterns=[r'/tag/', r'/category/', r'/author/', r'/wp-admin/', r'/subscribe'] + _SOCIAL, link_patterns=[r'/\d{4}/\d{2}/[a-z0-9-]+/'], max_pages=30),
    # TRAVEL
    "Travel + Leisure - Hotels": SourcePatterns(gold_patterns=[r'travelandleisure\.com/.*new-hotel', r'travelandleisure\.com/hotels-resorts/.*'], block_patterns=[r'/tag/', r'/author/', r'/newsletter/', r'/flights/', r'/cruises/'] + _SOCIAL, link_patterns=[r'/hotels-resorts/[a-z0-9-]+'], max_pages=30),
    "Travel Pulse - Hotels": SourcePatterns(gold_patterns=[r'travelpulse\.com/.*hotel.*', r'travelpulse\.com/.*resort.*'], block_patterns=[r'/cruises/', r'/travel-agents/'] + _SOCIAL, link_patterns=[r'/news/[a-z0-9-]+'], max_pages=20),
    "Conde Nast Traveler - Hotels": SourcePatterns(gold_patterns=[r'cntraveler\.com/.*hotel.*open', r'cntraveler\.com/.*new.*hotel'], block_patterns=[r'/newsletter/', r'/subscribe/', r'/video/', r'/cruises/'] + _SOCIAL, link_patterns=[r'/story/[a-z0-9-]+'], max_pages=20),
    "Northstar Meetings - Hotels": SourcePatterns(gold_patterns=[r'northstarmeetingsgroup\.com/.*hotel', r'northstarmeetingsgroup\.com/.*opening'], block_patterns=[r'/events/', r'/subscribe/'] + _SOCIAL, link_patterns=[r'/news/[a-z0-9-]+'], max_pages=20),
    # AGGREGATORS
    "The Orange Studio - Hotel Openings": SourcePatterns(gold_patterns=[r'/hotel-openings$', r'/hotel-openings\?'], block_patterns=[r'/about', r'/contact', r'/privacy'], link_patterns=[], max_pages=1),
    "New Hotels 2026": SourcePatterns(gold_patterns=[r'/hotel-openings-2026', r'/[a-z0-9-]+-hotel'], block_patterns=[r'/about', r'/contact'], link_patterns=[r'/[a-z0-9-]+-hotel'], max_pages=100),
    "New Hotels 2027": SourcePatterns(gold_patterns=[r'/hotel-openings-2027', r'/[a-z0-9-]+-hotel'], block_patterns=[r'/about', r'/contact'], link_patterns=[r'/[a-z0-9-]+-hotel'], max_pages=50),
    # FLORIDA
    "South Florida Biz Journal - Hotels": SourcePatterns(gold_patterns=[r'/southflorida/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html'], block_patterns=[r'bizjournals\.com/(?!southflorida)'] + _BIZJOURNAL, link_patterns=[r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html', r'/southflorida/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html'], max_pages=25),
    "Orlando Biz Journal - Hotels": SourcePatterns(gold_patterns=[r'/orlando/news/\d{4}/\d{2}/\d{2}/[a-z0-9-]+\.html'], block_patterns=[r'bizjournals\.com/(?!orlando)'] + _BIZJOURNAL, link_patterns=[r'/orlando/news/\d{4}/\d{2}/\d{2}/.*hotel.*\.html', r'/orlando/news/\d{4}/\d{2}/\d{2}/.*resort.*\.html'], max_pages=25),
    # CARIBBEAN
    "Caribbean Journal - Hotels": SourcePatterns(gold_patterns=[r'caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel', r'caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-resort', r'caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-opening'], block_patterns=[r'/tag/', r'/author/', r'/destination/', r'-cheap-flights', r'-cruise'], link_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+'], max_pages=30),
    "Caribbean Journal - Homepage": SourcePatterns(gold_patterns=[r'caribjournal\.com/\d{4}/\d{2}/\d{2}/[a-z0-9-]+-hotel'], block_patterns=[r'/tag/', r'/author/', r'-cruise'], link_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+'], max_pages=20),
    "Caribbean Hotel & Tourism Association": SourcePatterns(gold_patterns=[r'caribbeanhotelandtourism\.com/.*hotel', r'caribbeanhotelandtourism\.com/\d{4}/\d{2}/\d{2}/'], block_patterns=[r'/tag/', r'/about/', r'/membership/'] + _SOCIAL, link_patterns=[r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+/'], max_pages=20),
    "Sandals Press": SourcePatterns(gold_patterns=[r'/press-releases/[a-z0-9-]+'], block_patterns=[r'/resorts/', r'/booking/', r'/deals/', r'/weddings/'] + _SOCIAL, link_patterns=[r'/press-releases/[a-z0-9-]+'], max_pages=15),
}

# HELPER FUNCTIONS
def get_patterns(source_name: str) -> Optional[SourcePatterns]:
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

def has_patterns(source_name: str) -> bool:
    return source_name in SOURCE_PATTERNS

def list_configured_sources() -> List[str]:
    return list(SOURCE_PATTERNS.keys())

DEFAULT_PATTERNS = SourcePatterns(
    gold_patterns=[r'/news/', r'/press/', r'/releases/', r'/\d{4}/\d{2}/'],
    block_patterns=_GENERIC,
    link_patterns=[r'/news/', r'/press/', r'/article/'],
    max_pages=20,
)

def get_patterns_with_default(source_name: str) -> SourcePatterns:
    return SOURCE_PATTERNS.get(source_name, DEFAULT_PATTERNS)

def print_summary():
    print("=" * 65)
    print("SOURCE PATTERNS CONFIGURATION")
    print("=" * 65)
    print(f"\nTotal sources configured: {len(SOURCE_PATTERNS)}")
    print(f"\n{'Source':<45} {'Gold':<6} {'Block':<7} {'Max':<5}")
    print("-" * 65)
    for name, patterns in SOURCE_PATTERNS.items():
        print(f"{name:<45} {len(patterns.gold_patterns):<6} {len(patterns.block_patterns):<7} {patterns.max_pages:<5}")
    print("-" * 65)

if __name__ == "__main__":
    print_summary()