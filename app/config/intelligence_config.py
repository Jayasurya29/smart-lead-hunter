# -*- coding: utf-8 -*-
"""
INTELLIGENCE CONFIGURATION — Single Source of Truth
=====================================================
Every threshold, interval, and tuning constant for the intelligence
system lives HERE. No other file should hardcode these values.

Imported by:
  - app/tasks/autonomous_tasks.py       (scheduling intervals, score thresholds)
  - app/services/source_intelligence.py  (pattern classification, page budgets)
  - app/services/intelligent_pipeline.py (cache TTL, AI config, skip patterns)
  - app/services/smart_scraper.py        (skip patterns)
  - app/services/lead_factory.py         (score tier thresholds)
  - app/services/orchestrator.py         (score tier thresholds)
  - app/services/rescore.py              (score tier thresholds)
  - app/services/url_filter.py           (skip patterns)

Change a number HERE → changes everywhere.
"""

# ═══════════════════════════════════════════════════════════════
# SOURCE SCHEDULING — How often to scrape each source
# ═══════════════════════════════════════════════════════════════
# Used by: autonomous_tasks.py smart_scrape()

# Minimum runs before a source can graduate from learning phase
MIN_RUNS_TO_GRADUATE = 3

# Yield thresholds that determine source tier
PRODUCER_YIELD_THRESHOLD = 0.15  # 15%+ = proven producer
MODERATE_YIELD_THRESHOLD = 0.05  # 5-15% = decent producer

# Interval multipliers (applied to learned publish_frequency_days)
PRODUCER_FREQ_MULTIPLIER = 12  # pub_freq * 12 = hours between scrapes
MODERATE_FREQ_MULTIPLIER = 18  # pub_freq * 18 = hours between scrapes

# Minimum intervals (floor, even if multiplier gives lower)
MIN_PRODUCER_INTERVAL_HOURS = 24  # Never scrape a producer more than 1x/day
MIN_MODERATE_INTERVAL_HOURS = 48  # Never scrape moderate more than 1x/2days

# Fixed intervals for non-graduated or low-yield sources
LEARNING_INTERVAL_HOURS = 65  # 65h = Mon 10am catches Fri afternoon scrapes
LOW_YIELD_INTERVAL_HOURS = 168  # Weekly (yield > 0 but low after 3+ runs)
ZERO_YIELD_INTERVAL_HOURS = 336  # Biweekly (0% yield after 3+ runs)


# ═══════════════════════════════════════════════════════════════
# LEAD SCORE TIER THRESHOLDS
# ═══════════════════════════════════════════════════════════════
# Used by: rescore.py, lead_factory.py, orchestrator.py,
#          intelligent_pipeline.py, autonomous_tasks.py
#
# These define what score = HOT/WARM/COOL/COLD across the
# entire app. Change here → changes everywhere.

SCORE_HOT_THRESHOLD = 70  # Score >= 70 = HOT lead
SCORE_WARM_THRESHOLD = 50  # Score >= 50 = WARM lead
SCORE_COOL_THRESHOLD = 30  # Score >= 30 = COOL lead
# Score < 30  = COLD lead

# Minimum score to auto-enrich (autonomous_tasks picks leads above this)
SCORE_MIN_ENRICH = 40

# Minimum score for orchestrator to flag as "worth attention"
SCORE_MIN_ATTENTION = 40


# ═══════════════════════════════════════════════════════════════
# PATTERN CLASSIFICATION — Gold vs Junk URL patterns
# ═══════════════════════════════════════════════════════════════
# Used by: source_intelligence.py

GOLD_MIN_TESTED = 3  # Min tests before promoting to gold
GOLD_MIN_HIT_RATE = 0.20  # 20%+ hit rate = gold pattern
JUNK_MIN_TESTED = 5  # Min tests before demoting to junk
JUNK_MAX_HIT_RATE = 0.0  # 0% after 5+ tests = junk


# ═══════════════════════════════════════════════════════════════
# PAGE BUDGETS — How many pages to scrape per source
# ═══════════════════════════════════════════════════════════════
# Used by: source_intelligence.py get_scrape_settings()

MAX_PAGES_NEW_SOURCE = 20  # New sources (< MIN_RUNS_TO_GRADUATE runs)
MAX_PAGES_HIGH_YIELD = 20  # yield > PRODUCER_YIELD_THRESHOLD
MAX_PAGES_MEDIUM_YIELD = 15  # yield > MODERATE_YIELD_THRESHOLD
MAX_PAGES_LOW_YIELD = 8  # yield below moderate threshold


# ═══════════════════════════════════════════════════════════════
# EFFICIENCY SCORING — Weights for composite source score
# ═══════════════════════════════════════════════════════════════
# Used by: source_intelligence.py _recalculate_efficiency_score()

WEIGHT_YIELD_RATE = 0.35
WEIGHT_USA_RATE = 0.25
WEIGHT_AVG_QUALITY = 0.20
WEIGHT_RELIABILITY = 0.10
WEIGHT_SPEED = 0.10


# ═══════════════════════════════════════════════════════════════
# RATE LIMITING — Adaptive request timing
# ═══════════════════════════════════════════════════════════════
# Used by: source_intelligence.py

RATE_LIMIT_COOLDOWN_HOURS = 2  # Hours to wait after a 429
RATE_LIMIT_BASE_DELAY = 2.0  # Base delay in seconds
RATE_LIMIT_BACKOFF_FACTOR = 1.5  # Multiply delay after each 429
RATE_LIMIT_MAX_DELAY = 10.0  # Never wait more than 10s


# ═══════════════════════════════════════════════════════════════
# EXTRACTION PIPELINE — AI models and caching
# ═══════════════════════════════════════════════════════════════
# Used by: intelligent_pipeline.py PipelineConfig

CLASSIFIER_MODEL = "gemini-2.5-flash-lite"  # 4,000 RPM / Unlimited RPD
EXTRACTOR_MODEL = "gemini-2.5-flash"  # 1,000 RPM / 10,000 RPD
CLASSIFIER_CONTENT_LIMIT = 5000  # Chars for classification
EXTRACTOR_CONTENT_LIMIT = 20000  # Chars for extraction
CLASSIFICATION_CONFIDENCE = 0.45  # Min confidence to extract
QUALIFICATION_THRESHOLD = 30  # Min score to keep a lead
MIN_DELAY_SECONDS = 0.15  # Between API calls
MAX_CONCURRENT_REQUESTS = 20  # Parallel AI calls

# Redis extraction cache
REDIS_CACHE_TTL_HOURS = 168  # 7 days — covers weekly runs


# ═══════════════════════════════════════════════════════════════
# SOURCE HEALTH — When to deactivate sources
# ═══════════════════════════════════════════════════════════════
# Used by: autonomous_tasks.py daily_health_check()

MAX_CONSECUTIVE_FAILURES = 10  # Deactivate after 10 straight failures
MIN_EFFICIENCY_SCORE = 2.0  # Deactivate if score drops below this
MIN_RUNS_FOR_DEACTIVATION = 5  # Don't deactivate until 5+ runs

# History limits
MAX_RUN_HISTORY = 20  # Keep last 20 scrape runs per source


# ═══════════════════════════════════════════════════════════════
# SKIP URL PATTERNS — URLs to never scrape/follow
# ═══════════════════════════════════════════════════════════════
# Used by: intelligent_pipeline.py (QuickRejectFilter),
#          smart_scraper.py (SKIP_PATTERNS), url_filter.py
#
# ONE list. Every scraping component imports this.

SKIP_URL_PATTERNS = [
    # Auth & user pages
    "/login",
    "/signin",
    "/signup",
    "/register",
    "/logout",
    "/account",
    "/profile",
    "/settings",
    "/password",
    # Site infrastructure
    "/contact-us",
    "/contact",
    "/about-us",
    "/about$",
    "/privacy",
    "/terms",
    "/cookie",
    "/legal",
    "/sitemap",
    "/robots",
    "/feed",
    "/rss",
    "/wp-admin",
    "/wp-login",
    "/admin",
    "/_next/",
    "/_nuxt/",
    # Media files
    "/video-gallery",
    "/photo-gallery",
    "/gallery",
    "/podcast",
    r"\.pdf$",
    r"\.jpg$",
    r"\.png$",
    r"\.gif$",
    r"\.svg$",
    r"\.mp4$",
    r"\.mp3$",
    r"\.zip$",
    r"\.doc$",
    # Social media
    r"facebook\.com",
    r"twitter\.com",
    r"instagram\.com",
    r"linkedin\.com",
    r"youtube\.com",
    "mailto:",
    "tel:",
    "javascript:",
    # E-commerce
    "/cart",
    "/checkout",
    "/shop",
    "/store",
    "/subscribe",
    "/advertise",
    # Navigation / pagination
    "/search\\?",
    "/search$",
    "/tag/",
    "/tags/",
    "/category/",
    "/author/",
    r"/page/\d+$",
    "#",
    # Career pages
    "/careers",
    "/jobs",
]
