"""
SMART LEAD HUNTER - URL FILTER
==============================
Comprehensive URL filtering to prevent scraping junk pages.

This module provides multi-layer URL validation:
1. BLOCKED patterns - URLs we should NEVER scrape
2. PRIORITY patterns - URLs we should DEFINITELY scrape
3. DOMAIN rules - Per-domain filtering rules
4. Content-type detection - Skip non-HTML resources

Usage:
    from app.services.url_filter import URLFilter

    url_filter = URLFilter()

    # Check if URL should be scraped
    should_scrape, reason = url_filter.should_scrape(url)

    # Filter a list of URLs
    valid_urls = url_filter.filter_urls(urls, base_url)
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


@dataclass
class URLFilterResult:
    """Result of URL filtering"""

    url: str
    should_scrape: bool
    reason: str
    priority: int = 0  # Higher = more important to scrape


class URLFilter:
    """
    Comprehensive URL filter for hotel news scraping.

    Blocks junk URLs like:
    - Licensing/sharing pages
    - Login/signup pages
    - Social media links
    - File downloads
    - Duplicate content (pagination, tags, etc.)

    Prioritizes valuable URLs like:
    - News articles
    - Press releases
    - Hotel opening announcements
    """

    # =========================================================================
    # BLOCKED URL PATTERNS
    # These URLs should NEVER be scraped - they contain no hotel content
    # =========================================================================

    BLOCKED_PATTERNS = [
        # ----- SITE FUNCTIONALITY (not content) -----
        r"/selfservice/",  # Article licensing forms
        r"/article-licensing/",  # Reprint request forms
        r"/submit/",  # Form submission pages
        r"/licensing/",  # Licensing pages
        r"/republish/",  # Republishing forms
        r"/reprint/",  # Reprint request
        r"/permission/",  # Permission request
        # ----- USER AUTHENTICATION -----
        r"/signup/",
        r"/sign-up/",
        r"/signin/",
        r"/sign-in/",
        r"/login/",
        r"/log-in/",
        r"/logout/",
        r"/log-out/",
        r"/register/",
        r"/registration/",
        r"/account/",
        r"/my-account/",
        r"/profile/",
        r"/dashboard/",
        r"/settings/",
        r"/preferences/",
        r"/password/",
        r"/forgot-password/",
        r"/reset-password/",
        # ----- SUBSCRIPTIONS & NEWSLETTERS -----
        r"/subscribe/",
        r"/subscription/",
        r"/newsletter/",
        r"/newsletters/",
        r"/email-signup/",
        r"/alerts/",
        r"/notifications/",
        r"/unsubscribe/",
        # ----- CONTACT & SUPPORT -----
        r"/contact/",
        r"/contact-us/",
        r"/contactus/",
        r"/support/",
        r"/help/",
        r"/faq/",
        r"/faqs/",
        r"/feedback/",
        r"/inquiry/",
        r"/enquiry/",
        # ----- LEGAL PAGES -----
        r"/privacy/",
        r"/privacy-policy/",
        r"/terms/",
        r"/terms-of-use/",
        r"/terms-of-service/",
        r"/tos/",
        r"/legal/",
        r"/disclaimer/",
        r"/cookie/",
        r"/cookie-policy/",
        r"/gdpr/",
        r"/ccpa/",
        r"/accessibility/",
        r"/takedown/",
        # ----- ABOUT/INFO PAGES -----
        r"/about-us/",
        r"/aboutus/",
        r"/about/",
        r"/who-we-are/",
        r"/our-team/",
        r"/team/",
        r"/leadership/",
        r"/board/",
        r"/investors/",
        r"/advertise/",
        r"/advertising/",
        r"/media-kit/",
        r"/mediakit/",
        r"/sponsors/",
        r"/partners/",
        # ----- SOCIAL SHARING & ACTIONS -----
        r"/share/",
        r"/share\?",
        r"/print/",
        r"/print\?",
        r"/email/",
        r"/email\?",
        r"/mailto:",
        r"/bookmark/",
        r"/save/",
        r"/favorite/",
        r"/follow/",
        r"/like/",
        r"/comment/",
        r"/comments/",
        r"/reply/",
        r"/recommend/",
        # ----- SOCIAL MEDIA DOMAINS -----
        r"facebook\.com",
        r"twitter\.com",
        r"x\.com/(?!.*article)",  # X.com but not articles
        r"linkedin\.com/share",
        r"instagram\.com",
        r"youtube\.com",
        r"pinterest\.com",
        r"tiktok\.com",
        r"reddit\.com",
        r"whatsapp\.com",
        r"telegram\.org",
        r"t\.me/",
        # ----- FILE DOWNLOADS (not HTML) -----
        r"\.pdf($|\?)",
        r"\.doc($|\?)",
        r"\.docx($|\?)",
        r"\.xls($|\?)",
        r"\.xlsx($|\?)",
        r"\.ppt($|\?)",
        r"\.pptx($|\?)",
        r"\.zip($|\?)",
        r"\.rar($|\?)",
        r"\.exe($|\?)",
        r"\.dmg($|\?)",
        r"\.mp3($|\?)",
        r"\.mp4($|\?)",
        r"\.wav($|\?)",
        r"\.avi($|\?)",
        r"\.mov($|\?)",
        # ----- IMAGES (already have, don't need page) -----
        r"\.jpg($|\?)",
        r"\.jpeg($|\?)",
        r"\.png($|\?)",
        r"\.gif($|\?)",
        r"\.webp($|\?)",
        r"\.svg($|\?)",
        r"\.ico($|\?)",
        # ----- CAREERS/JOBS (not hotel openings) -----
        r"/careers/",
        r"/career/",
        r"/jobs/",
        r"/job/",
        r"/employment/",
        r"/hiring/",
        r"/work-with-us/",
        r"/join-us/",
        r"/join-our-team/",
        r"/opportunities/",
        r"/vacancies/",
        r"/apply/",
        r"/application/",
        r"workday\.com",
        r"greenhouse\.io",
        r"lever\.co",
        r"indeed\.com",
        r"glassdoor\.com",
        # ----- E-COMMERCE (not news) -----
        r"/cart/",
        r"/checkout/",
        r"/shop/",
        r"/store/",
        r"/buy/",
        r"/purchase/",
        r"/order/",
        r"/payment/",
        r"/booking/",
        r"/reservations/",
        r"/book-now/",
        # ----- ARCHIVES & DUPLICATES -----
        r"/tag/",
        r"/tags/",
        r"/category/",
        r"/categories/",
        r"/topic/[^/]+/page/",  # Pagination within topics
        r"/author/",
        r"/authors/",
        r"/contributor/",
        r"/editor/",
        r"/editors/",
        r"/archive/",
        r"/archives/",
        r"/search/",
        r"/search\?",
        r"\?s=",  # Search query
        r"/page/\d+",  # Pagination
        r"\?page=\d+",  # Pagination query
        r"\?p=\d+",  # Pagination query
        r"\?sort=",  # Sorting
        r"\?order=",  # Ordering
        r"\?filter=",  # Filtering
        r"\?view=",  # View mode
        r"#comment",  # Comment anchors
        r"#respond",  # Reply anchors
        # ----- FEEDS & APIs -----
        r"/feed/",
        r"/feeds/",
        r"/rss/",
        r"/rss\.xml",
        r"/atom/",
        r"/atom\.xml",
        r"/api/",
        r"/rest/",
        r"/graphql/",
        r"/webhook/",
        r"\.json($|\?)",
        r"\.xml($|\?)",
        # ----- MISC JUNK -----
        r"/cdn-cgi/",  # Cloudflare
        r"/wp-admin/",  # WordPress admin
        r"/wp-login/",  # WordPress login
        r"/wp-content/uploads/",  # WordPress uploads
        r"/amp/",  # AMP pages (duplicate)
        r"\?amp=",  # AMP query
        r"/embed/",  # Embedded content
        r"/widget/",  # Widgets
        r"/popup/",  # Popups
        r"/modal/",  # Modals
        r"/ad/",  # Ads
        r"/ads/",  # Ads
        r"/sponsor/",  # Sponsored
        r"/sponsored/",  # Sponsored content
        r"/native-ad/",  # Native ads
        r"doubleclick\.net",  # Ad network
        r"googlesyndication\.com",  # Ad network
        r"googleadservices\.com",  # Ad services
    ]

    # =========================================================================
    # PRIORITY URL PATTERNS
    # These URLs are VALUABLE and should be scraped with high priority
    # =========================================================================

    PRIORITY_PATTERNS = [
        # ----- NEWS & ARTICLES -----
        (r"/news/[^/]+", 10),  # News articles
        (r"/article/[^/]+", 10),  # Articles
        (r"/story/[^/]+", 9),  # Stories
        (r"/post/[^/]+", 8),  # Posts
        # ----- PRESS RELEASES -----
        (r"/press-release/", 10),
        (r"/press-releases/", 9),
        (r"/pressrelease/", 10),
        (r"/press/", 8),
        (r"/media-release/", 9),
        (r"/announcement/", 9),
        (r"/announcements/", 8),
        # ----- HOTEL-SPECIFIC CONTENT -----
        (r"/hotel-opening/", 10),
        (r"/hotel-openings/", 9),
        (r"/new-hotel/", 10),
        (r"/new-hotels/", 9),
        (r"/development/", 8),
        (r"/developments/", 8),
        (r"/pipeline/", 8),
        (r"/construction/", 7),
        (r"/groundbreaking/", 9),
        (r"/grand-opening/", 10),
        (r"/debut/", 9),
        (r"/launch/", 8),
        (r"/expansion/", 7),
        # ----- BRAND NEWSROOMS -----
        (r"news\.marriott\.com/[^/]+", 10),
        (r"stories\.hilton\.com/[^/]+", 10),
        (r"newsroom\.hilton\.com/[^/]+", 10),
        (r"press\.fourseasons\.com/[^/]+", 10),
        (r"ihgplc\.com/.*news", 9),
        (r"hyatt\.com/.*news", 9),
        # ----- INDUSTRY NEWS -----
        (r"hoteldive\.com/news/", 10),
        (r"hospitalitynet\.org/news/", 10),
        (r"hotelmanagement\.net/[^/]+", 9),
        (r"htrends\.com/[^/]+", 8),
    ]

    # =========================================================================
    # DOMAIN-SPECIFIC RULES
    # Some domains need special handling
    # =========================================================================

    DOMAIN_RULES = {
        "hoteldive.com": {
            "block": [
                r"/selfservice/",
                r"/what-we-are-reading/",  # External links roundup
                r"/library/",  # Resource library
                r"/events/",  # Events calendar
                r"/editors/",  # Editor profiles
                r"/signup/",  # Signup pages
                r"/press-release/",  # Press releases (less useful)
                r"/spons-content/",  # Sponsored content
            ],
            # NOTE: No 'require' - allow /topic/ pages as entry points
        },
        "hospitalitynet.org": {
            # STRICT FILTERING - Only scrape actual hotel announcements
            "block": [
                r"/supplier/",  # Supplier profiles
                r"/list/",  # Listing pages
                r"/organization/",  # Company profiles - NO LEADS
                r"/opinion/",  # Opinion pieces - NO LEADS
                r"/video/",  # Videos - NO LEADS
                r"/event/",  # Events/conferences - NO LEADS
                r"/panel/",  # Panel discussions - NO LEADS
                r"/viewpoint/",  # Viewpoints - NO LEADS
                r"/podcast/",  # Podcasts - NO LEADS
                r"/hottopic/",  # Hot topics - aggregation pages
                r"/360/",  # 360 section
                r"/about\.html",
                r"/contact\.html",
                r"/terms\.html",
                r"/privacy\.html",
                r"/rss\.html",
                r"/search\.html",
                r"/me/",  # User account pages
            ],
            "allow_patterns": [
                r"/news/global\.html",  # Entry page
                r"/announcement/",  # Hotel openings - THE GOLD
                r"/news/\d+\.html",  # News articles with IDs
            ],
        },
        # -------------------------------------------------------------------------
        # CHAIN NEWSROOMS - Fine tuned
        # -------------------------------------------------------------------------
        "news.marriott.com": {
            "block": [
                r"/category/",
                r"/tag/",
                r"/author/",
                r"/search/",
                r"/about/",
                r"/contact/",
            ],
        },
        "stories.hilton.com": {
            "block": [
                r"/brands/",  # Brand pages (not news)
                r"/team-members/",
                r"/corporate/",
                r"/about/",
                r"/contact/",
                r"/search/",
            ],
        },
        "newsroom.hyatt.com": {
            "block": [
                r"/media-contacts/",
                r"/about/",
                r"/corporate/",
                r"/search/",
            ],
        },
        "press.fourseasons.com": {
            "block": [
                r"/media-contacts/",
                r"/about/",
                r"/search/",
            ],
        },
        "ihgplc.com": {
            "block": [
                r"/investors/",
                r"/responsibility/",
                r"/about/",
                r"/careers/",
            ]
        },
        # -------------------------------------------------------------------------
        # CARIBBEAN SOURCES
        # -------------------------------------------------------------------------
        "caribjournal.com": {
            # TARGETED APPROACH: Only allow hotel article URLs
            "block": [
                r"/tag/",
                r"/author/",
                r"/destination/",  # Destination pages - low quality
                r"/cj-invest-news/",  # Investment section
                r"/cj-invest/",  # Investment portal
                r"/cji-",  # CJI media kit, contact
                r"/cta-",  # CTA media kit, contact
                r"/caribbean-travel-advisor/",  # Travel advisor
                r"/caribbean/$",  # Caribbean index
                r"/memberful",  # Login/auth
                r"-cheap-flights",  # Flight deals
                r"-hiking-",  # Hiking
                r"-wedding-",  # Wedding
                r"-cruise",  # Cruise
                r"lamborghini",  # Cars
            ],
            # REQUIRE: URLs must match one of these patterns (allowlist approach)
            "require": [
                r"/category/hotels",  # Entry page
                r"/\d{4}/\d{2}/\d{2}/",  # Article URLs (YYYY/MM/DD) - THE GOLD
            ],
            # Allow patterns override global blocks
            "allow_patterns": [
                r"/category/hotels",  # Entry + pagination
                r"/\d{4}/\d{2}/\d{2}/",  # Articles
            ],
        },
        "caribbeanhotelandtourism.com": {
            "block": [
                r"/tag/",
                r"/author/",
                r"/page/",
                r"/about/",
                r"/contact/",
                r"/membership/",
            ],
        },
        "sandals.com": {
            "block": [
                r"/resorts/",
                r"/booking/",
                r"/deals/",
                r"/weddings/",
            ],
        },
        # -------------------------------------------------------------------------
        # BUSINESS JOURNALS - YOUR FLORIDA MARKET
        # -------------------------------------------------------------------------
        "bizjournals.com": {
            "block": [
                # Block ALL other BizJournals cities - only allow southflorida/orlando/tampabay/jacksonville
                r"bizjournals\.com/albany",
                r"bizjournals\.com/albuquerque",
                r"bizjournals\.com/atlanta",
                r"bizjournals\.com/austin",
                r"bizjournals\.com/baltimore",
                r"bizjournals\.com/birmingham",
                r"bizjournals\.com/boston",
                r"bizjournals\.com/buffalo",
                r"bizjournals\.com/charlotte",
                r"bizjournals\.com/chicago",
                r"bizjournals\.com/cincinnati",
                r"bizjournals\.com/cleveland",
                r"bizjournals\.com/columbus",
                r"bizjournals\.com/dallas",
                r"bizjournals\.com/dayton",
                r"bizjournals\.com/denver",
                r"bizjournals\.com/detroit",
                r"bizjournals\.com/houston",
                r"bizjournals\.com/indianapolis",
                r"bizjournals\.com/kansascity",
                r"bizjournals\.com/losangeles",
                r"bizjournals\.com/louisville",
                r"bizjournals\.com/memphis",
                r"bizjournals\.com/milwaukee",
                r"bizjournals\.com/minneapolis",
                r"bizjournals\.com/nashville",
                r"bizjournals\.com/newyork",
                r"bizjournals\.com/pacific",
                r"bizjournals\.com/philadelphia",
                r"bizjournals\.com/phoenix",
                r"bizjournals\.com/pittsburgh",
                r"bizjournals\.com/portland",
                r"bizjournals\.com/raleigh",
                r"bizjournals\.com/richmond",
                r"bizjournals\.com/sacramento",
                r"bizjournals\.com/sanantonio",
                r"bizjournals\.com/sanfrancisco",
                r"bizjournals\.com/sanjose",
                r"bizjournals\.com/seattle",
                r"bizjournals\.com/stlouis",
                r"bizjournals\.com/twincities",
                r"bizjournals\.com/washington",
                r"bizjournals\.com/wichita",
                # Block non-hotel industry categories
                r"/news/banking",
                r"/news/technology",
                r"/news/health-care",
                r"/news/retail",
                r"/news/manufacturing",
                r"/news/energy",
                r"/news/education",
                r"/news/government",
                r"/news/professional",
                r"/news/media",
                r"/news/philanthropy",
                r"/news/sports",
                r"/news/transportation",
                r"/news/food-and-lifestyle",
                r"/news/career",
                r"/news/residential-real-estate",
                r"/news/commercial-real-estate",
                r"/news/feature/",
                # Block junk
                r"/undefined",
                r"/null",
                r"/bizwomen/",
                r"/events/",
                r"/people/",
                r"/lists/",
                r"/subscribe/",
                r"/page/\d+",
                r"/account/",
                r"/apps/",
                r"/about/",
                r"/contact/",
                r"/advertise/",
                r"/help/",
            ],
            # Only allow Florida BizJournals hotel article URLs
            "allow_patterns": [
                r"/southflorida/news/\d{4}/\d{2}/\d{2}/.*\.html",
                r"/orlando/news/\d{4}/\d{2}/\d{2}/.*\.html",
                r"/tampabay/news/\d{4}/\d{2}/\d{2}/.*\.html",
                r"/jacksonville/news/\d{4}/\d{2}/\d{2}/.*\.html",
                r"/southflorida/news/industry/hotels",
                r"/orlando/news/industry/hotels",
                r"/tampabay/news/industry/hotels",
                r"/jacksonville/news/industry/hotels",
            ],
        },
        # -------------------------------------------------------------------------
        # REAL ESTATE / CRE
        # -------------------------------------------------------------------------
        "bisnow.com": {
            "block": [
                r"/events/",
                r"/jobs/",
                r"/advertise/",
                r"/about/",
                r"/contact/",
                r"/subscribe/",
                r"/page/\d+",
            ],
        },
        "costar.com": {
            "block": [
                r"/login",
                r"/subscribe",
                r"/pricing",
                r"/about",
            ]
        },
        # -------------------------------------------------------------------------
        # LUXURY MEDIA
        # -------------------------------------------------------------------------
        "travelandleisure.com": {
            "block": [
                r"/tag/",
                r"/author/",
                r"/page/",
                r"/newsletter/",
                r"/subscribe/",
                r"/advertise/",
            ],
        },
        "cntraveler.com": {
            "block": [
                r"/tag/",
                r"/contributor/",
                r"/newsletter/",
                r"/subscribe/",
            ],
        },
        # -------------------------------------------------------------------------
        # AGGREGATORS
        # -------------------------------------------------------------------------
        "theorangestudio.com": {
            "block": [
                r"/about",
                r"/contact",
                r"/privacy",
                r"/terms",
            ],
        },
        "newhotelsopening.com": {
            "block": [
                r"/about",
                r"/contact",
                r"/privacy",
            ],
        },
        "hotelnewsresource.com": {
            "block": [
                r"/directory/",
                r"/events/",
                r"/advertise/",
                r"/Info-",
                r"/HNR-region-",
                r"/HNR-category-(?!.*Openings)",  # Block other categories
                r"/topics/",
                r"studio\.hotelnewsresource",
            ],
            "allow_patterns": [
                r"/article\d+\.html",  # Article pages
                r"/HNR-category-category-Openings",  # Entry page
            ],
        },
        "lodgingmagazine.com": {
            "block": [
                r"/tag/",
                r"/author/",
                r"/wp-admin/",
                r"/subscribe/",
            ],
            "allow_patterns": [
                r"/category/finance-development/",  # Our entry URLs
                r"/\d{4}/\d{2}/[a-z0-9-]+",  # Article URLs
            ],
        },
        # -------------------------------------------------------------------------
        # OTHER CHAINS
        # -------------------------------------------------------------------------
        "marriott.com": {
            "block": [
                r"/reservation/",
                r"/hotel-search/",
                r"/travel/",
            ]
        },
        "hilton.com": {
            "block": [
                r"/en/book/",
                r"/en/hotels/",
            ]
        },
    }

    # =========================================================================
    # METHODS
    # =========================================================================

    def __init__(self):
        """Initialize the URL filter with compiled regex patterns"""
        # Compile blocked patterns for performance
        self._blocked_compiled = [
            re.compile(pattern, re.IGNORECASE) for pattern in self.BLOCKED_PATTERNS
        ]

        # Compile priority patterns
        self._priority_compiled = [
            (re.compile(pattern, re.IGNORECASE), score)
            for pattern, score in self.PRIORITY_PATTERNS
        ]

        # Compile domain rules
        self._domain_rules_compiled = {}
        for domain, rules in self.DOMAIN_RULES.items():
            self._domain_rules_compiled[domain] = {
                "block": [re.compile(p, re.IGNORECASE) for p in rules.get("block", [])],
                "require": [
                    re.compile(p, re.IGNORECASE) for p in rules.get("require", [])
                ],
                "allow": [
                    re.compile(p, re.IGNORECASE)
                    for p in rules.get("allow_patterns", [])
                ],
            }

        # Track statistics
        self._stats = {
            "total_checked": 0,
            "blocked": 0,
            "allowed": 0,
            "prioritized": 0,
        }

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return ""

    def _check_blocked_patterns(self, url: str) -> Optional[str]:
        """
        Check if URL matches any blocked pattern.

        Returns: Matching pattern name if blocked, None if allowed
        """
        url_lower = url.lower()

        for pattern in self._blocked_compiled:
            if pattern.search(url_lower):
                return pattern.pattern

        return None

    def _check_domain_rules(self, url: str, domain: str) -> Tuple[bool, str]:
        """
        Check domain-specific rules.

        Returns: (should_block, reason)
        """
        # Find matching domain rules
        matching_domain = None
        for rule_domain in self._domain_rules_compiled:
            if rule_domain in domain:
                matching_domain = rule_domain
                break

        if not matching_domain:
            return False, "No domain rules"

        rules = self._domain_rules_compiled[matching_domain]
        url_lower = url.lower()

        # Check domain-specific blocks
        for pattern in rules["block"]:
            if pattern.search(url_lower):
                return True, f"Domain rule block: {pattern.pattern}"

        # Check required patterns (if any)
        if rules["require"]:
            has_required = any(p.search(url_lower) for p in rules["require"])
            if not has_required:
                return True, f"Missing required pattern for {matching_domain}"

        return False, "Domain rules passed"

    def _check_domain_allow_patterns(self, url: str, domain: str) -> bool:
        """
        Check if URL matches domain-specific allow patterns.

        Allow patterns OVERRIDE global blocks, letting specific URLs through.

        Returns: True if URL should be allowed (skip global blocks)
        """
        # Find matching domain rules
        matching_domain = None
        for rule_domain in self._domain_rules_compiled:
            if rule_domain in domain:
                matching_domain = rule_domain
                break

        if not matching_domain:
            return False

        rules = self._domain_rules_compiled[matching_domain]
        url_lower = url.lower()

        # Check allow patterns
        if rules.get("allow"):
            for pattern in rules["allow"]:
                if pattern.search(url_lower):
                    return True

        return False

    def _calculate_priority(self, url: str) -> int:
        """
        Calculate priority score for URL.

        Higher score = more valuable to scrape
        """
        url_lower = url.lower()
        max_priority = 0

        for pattern, score in self._priority_compiled:
            if pattern.search(url_lower):
                max_priority = max(max_priority, score)

        return max_priority

    def _is_same_domain(self, url: str, base_domain: str) -> bool:
        """Check if URL is on the same domain (or subdomain)"""
        url_domain = self._get_domain(url)

        # Exact match
        if url_domain == base_domain:
            return True

        # Subdomain match
        if url_domain.endswith("." + base_domain):
            return True

        # Base domain match (e.g., www.example.com matches example.com)
        base_without_www = base_domain.replace("www.", "")
        url_without_www = url_domain.replace("www.", "")

        return url_without_www == base_without_www

    def should_scrape(
        self, url: str, base_url: Optional[str] = None
    ) -> URLFilterResult:
        """
        Determine if a URL should be scraped.

        Args:
            url: The URL to check
            base_url: The source URL (for domain comparison)

        Returns:
            URLFilterResult with decision and reason
        """
        self._stats["total_checked"] += 1

        # Basic validation
        if not url or not url.startswith("http"):
            self._stats["blocked"] += 1
            return URLFilterResult(
                url=url, should_scrape=False, reason="Invalid URL (not http/https)"
            )

        # Check domain-specific ALLOW patterns FIRST (override global blocks)
        domain = self._get_domain(url)
        if self._check_domain_allow_patterns(url, domain):
            # URL matches a domain allow pattern - skip global blocks
            self._stats["allowed"] += 1
            priority = self._calculate_priority(url)
            return URLFilterResult(
                url=url,
                should_scrape=True,
                reason=f"Domain allow pattern matched (priority: {priority})",
                priority=priority,
            )

        # Check blocked patterns
        blocked_pattern = self._check_blocked_patterns(url)
        if blocked_pattern:
            self._stats["blocked"] += 1
            return URLFilterResult(
                url=url,
                should_scrape=False,
                reason=f"Blocked pattern: {blocked_pattern}",
            )

        # Check domain-specific block rules
        domain_blocked, domain_reason = self._check_domain_rules(url, domain)
        if domain_blocked:
            self._stats["blocked"] += 1
            return URLFilterResult(url=url, should_scrape=False, reason=domain_reason)

        # Check if same domain as base (for deep crawling)
        if base_url:
            base_domain = self._get_domain(base_url)
            if not self._is_same_domain(url, base_domain):
                # Allow external links only if they match priority patterns
                priority = self._calculate_priority(url)
                if priority < 8:  # Only high-priority external links
                    self._stats["blocked"] += 1
                    return URLFilterResult(
                        url=url,
                        should_scrape=False,
                        reason=f"External domain with low priority ({priority})",
                    )

        # Calculate priority score
        priority = self._calculate_priority(url)

        if priority > 0:
            self._stats["prioritized"] += 1

        self._stats["allowed"] += 1
        return URLFilterResult(
            url=url,
            should_scrape=True,
            reason=f"Passed all filters (priority: {priority})",
            priority=priority,
        )

    def filter_urls(
        self, urls: List[str], base_url: Optional[str] = None, max_urls: int = 100
    ) -> List[str]:
        """
        Filter a list of URLs, returning only valid ones.

        Args:
            urls: List of URLs to filter
            base_url: The source URL for domain comparison
            max_urls: Maximum URLs to return

        Returns:
            Filtered list of URLs, sorted by priority
        """
        results = []
        seen = set()

        for url in urls:
            # Skip duplicates
            url_normalized = url.lower().rstrip("/")
            if url_normalized in seen:
                continue
            seen.add(url_normalized)

            # Check if should scrape
            result = self.should_scrape(url, base_url)
            if result.should_scrape:
                results.append((url, result.priority))

        # Sort by priority (highest first) and limit
        results.sort(key=lambda x: -x[1])

        return [url for url, _ in results[:max_urls]]

    def get_stats(self) -> Dict[str, int]:
        """Get filtering statistics"""
        return self._stats.copy()

    def reset_stats(self):
        """Reset statistics"""
        self._stats = {
            "total_checked": 0,
            "blocked": 0,
            "allowed": 0,
            "prioritized": 0,
        }


# =============================================================================
# QUICK TEST
# =============================================================================


def test_url_filter():
    """Test the URL filter with various URLs"""

    filter = URLFilter()

    test_urls = [
        # Should be BLOCKED
        (
            "https://www.hoteldive.com/selfservice/article-licensing/submit/?newspostUrl=...",
            False,
        ),
        ("https://www.hoteldive.com/signup/", False),
        ("https://www.hoteldive.com/contact/", False),
        ("https://www.facebook.com/sharer/sharer.php?u=...", False),
        ("https://www.linkedin.com/shareArticle?mini=true&url=...", False),
        ("https://www.hoteldive.com/privacy-policy/", False),
        ("https://www.hoteldive.com/careers/", False),
        ("https://www.hoteldive.com/topic/development/page/2/", False),
        ("https://www.hoteldive.com/what-we-are-reading/", False),
        # Should be ALLOWED
        (
            "https://www.hoteldive.com/news/ritz-carlton-hotel-development-indianapolis/809199/",
            True,
        ),
        ("https://www.hoteldive.com/news/ihg-six-senses-resort-utah/810467/", True),
        ("https://news.marriott.com/news/2024/01/hotel-opening/", True),
        ("https://www.hospitalitynet.org/news/4130608.html", True),
        ("https://press.fourseasons.com/news-release/2024/new-hotel/", True),
    ]

    print("=" * 70)
    print("URL FILTER TEST")
    print("=" * 70)

    passed = 0
    failed = 0

    for url, expected in test_urls:
        result = filter.should_scrape(url)
        status = "✅" if result.should_scrape == expected else "❌"

        if result.should_scrape == expected:
            passed += 1
        else:
            failed += 1

        print(f"\n{status} {url[:60]}...")
        print(f"   Expected: {'ALLOW' if expected else 'BLOCK'}")
        print(f"   Got: {'ALLOW' if result.should_scrape else 'BLOCK'}")
        print(f"   Reason: {result.reason}")

    print("\n" + "=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print(f"Stats: {filter.get_stats()}")
    print("=" * 70)


if __name__ == "__main__":
    test_url_filter()
