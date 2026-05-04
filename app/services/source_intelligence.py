"""
SOURCE INTELLIGENCE SERVICE
============================
Unified learning engine for Smart Lead Hunter.
Replaces fragile JSON file learnings with DB-backed adaptive intelligence.

Every scrape makes the system smarter:
- Learns which URL patterns produce leads (gold) vs waste time (junk)
- Tracks rate limits and adapts request timing per source
- Monitors response times and chooses optimal scrape engines
- Calculates efficiency scores that drive scheduling decisions
- Automatically adjusts page budgets based on yield history

Architecture:
    Source.source_intelligence (JSONB) ← single source of truth per source
    SourceIntelligence (this class)    ← reads/writes/learns/recommends

Usage:
    intel = SourceIntelligence(source)

    # Before scraping — get adaptive settings
    settings = intel.get_scrape_settings()
    # settings.delay_seconds, settings.max_pages, settings.junk_patterns, etc.

    # After scraping — record what happened
    intel.record_scrape_result(url, produced_lead=True, lead_quality=0.85)
    intel.record_rate_limit(status_code=429)
    intel.record_response_time(url, ms=450)

    # Persist to DB
    intel.save()

Author: Jay (J.A. Uniforms)
Last Updated: March 2026
"""

import re
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Tuple
from urllib.parse import urlparse

from app.config.intelligence_config import (
    GOLD_MIN_TESTED,
    GOLD_MIN_HIT_RATE,
    JUNK_MIN_TESTED,
    JUNK_MAX_HIT_RATE,
    MAX_PAGES_NEW_SOURCE,
    MAX_PAGES_HIGH_YIELD,
    MAX_PAGES_MEDIUM_YIELD,
    MAX_PAGES_LOW_YIELD,
    PRODUCER_YIELD_THRESHOLD,
    MODERATE_YIELD_THRESHOLD,
    RATE_LIMIT_COOLDOWN_HOURS,
    RATE_LIMIT_BACKOFF_FACTOR,
    RATE_LIMIT_MAX_DELAY,
    WEIGHT_YIELD_RATE,
    WEIGHT_USA_RATE,
    WEIGHT_AVG_QUALITY,
    WEIGHT_RELIABILITY,
    WEIGHT_SPEED,
    MAX_RUN_HISTORY,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════


@dataclass
class ScrapeSettings:
    """Adaptive settings the scraper should use for this source."""

    delay_seconds: float = 1.0  # Time between requests
    max_pages: int = 15  # Page budget for this scrape
    max_concurrent: int = 3  # Concurrent requests allowed
    use_playwright: bool = False  # Whether JS rendering is needed
    gold_patterns: List[str] = field(
        default_factory=list
    )  # URL patterns that produce leads
    junk_patterns: List[str] = field(
        default_factory=list
    )  # URL patterns to skip (FREE filter)
    should_skip: bool = False  # Skip this source entirely (rate limited / dead)
    skip_reason: str = ""  # Why we're skipping
    priority_score: float = 5.0  # 1-10, drives scheduling order


@dataclass
class PatternStats:
    """Statistics for a single URL pattern."""

    tested: int = 0
    leads: int = 0
    total_quality: float = 0.0
    usa_caribbean: int = 0
    last_tested: str = ""

    @property
    def hit_rate(self) -> float:
        return self.leads / max(self.tested, 1)

    @property
    def avg_quality(self) -> float:
        return self.total_quality / max(self.leads, 1)

    def to_dict(self) -> dict:
        return {
            "tested": self.tested,
            "leads": self.leads,
            "total_quality": round(self.total_quality, 3),
            "usa_caribbean": self.usa_caribbean,
            "hit_rate": round(self.hit_rate, 3),
            "last_tested": self.last_tested,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PatternStats":
        return cls(
            tested=data.get("tested", 0),
            leads=data.get("leads", 0),
            total_quality=data.get("total_quality", 0.0),
            usa_caribbean=data.get("usa_caribbean", 0),
            last_tested=data.get("last_tested", ""),
        )


# ═══════════════════════════════════════════════════════════════
# CORE SERVICE CLASS
# ═══════════════════════════════════════════════════════════════


class SourceIntelligence:
    """
    Adaptive intelligence engine for a single source.

    Reads from and writes to Source.source_intelligence (JSONB).
    Every method that modifies state requires a subsequent save() call.
    """

    def __init__(self, source):
        """
        Initialize intelligence for a source.

        Args:
            source: SQLAlchemy Source model instance
        """
        self.source = source
        self._data = dict(source.source_intelligence or {})

        # Ensure all sections exist
        self._data.setdefault(
            "patterns", {"gold": [], "junk": [], "maybe": [], "stats": {}}
        )
        self._data.setdefault(
            "rate_limits",
            {
                "max_rpm": 30,
                "delay_seconds": 1.0,
                "last_429_at": None,
                "total_429s": 0,
                "total_403s": 0,
                "backoff_multiplier": 1.0,
            },
        )
        self._data.setdefault(
            "behavior",
            {
                "avg_response_ms": 0,
                "response_times": [],  # Last 50 response times
                "best_engine": "httpx",
                "requires_playwright": False,
                "content_type": "unknown",  # listing, article, mixed
                "publish_frequency_days": 7,
                "avg_new_articles_per_week": 0,
            },
        )
        self._data.setdefault(
            "performance",
            {
                "total_urls_tested": 0,
                "urls_with_leads": 0,
                "lead_yield_rate": 0.0,
                "usa_caribbean_rate": 0.0,
                "avg_lead_quality": 0.0,
                "efficiency_score": 5.0,
            },
        )
        self._data.setdefault("history", [])  # Last N scrape runs

    # ─── PROPERTIES ───────────────────────────────────────────

    @property
    def patterns(self) -> dict:
        return self._data["patterns"]

    @property
    def rate_limits(self) -> dict:
        return self._data["rate_limits"]

    @property
    def behavior(self) -> dict:
        return self._data["behavior"]

    @property
    def performance(self) -> dict:
        return self._data["performance"]

    @property
    def history(self) -> list:
        return self._data["history"]

    @property
    def efficiency_score(self) -> float:
        return self.performance.get("efficiency_score", 5.0)

    @property
    def is_rate_limited(self) -> bool:
        """Check if we're currently in a rate limit cooldown."""
        last_429 = self.rate_limits.get("last_429_at")
        if not last_429:
            return False
        try:
            last_time = datetime.fromisoformat(last_429)
            cooldown = timedelta(hours=RATE_LIMIT_COOLDOWN_HOURS)
            return datetime.now() < last_time + cooldown
        except (ValueError, TypeError):
            return False

    # ─── PRE-SCRAPE: GET ADAPTIVE SETTINGS ────────────────────

    def get_scrape_settings(self) -> ScrapeSettings:
        """
        Get optimized scrape settings based on everything we've learned.

        Call this BEFORE scraping a source. The returned settings tell
        the scraper how to behave for maximum efficiency.
        """
        settings = ScrapeSettings()

        # 1. Check rate limit cooldown
        if self.is_rate_limited:
            settings.should_skip = True
            settings.skip_reason = (
                f"Rate limited — cooling down until {self._rate_limit_expires_at()}"
            )
            return settings

        # 2. Check if source is dead
        if (self.source.consecutive_failures or 0) >= 10:
            settings.should_skip = True
            settings.skip_reason = "Source appears dead (10+ consecutive failures)"
            return settings

        # 3. Set request delay (adaptive based on rate limit history)
        base_delay = self.rate_limits.get("delay_seconds", 1.0)
        multiplier = self.rate_limits.get("backoff_multiplier", 1.0)
        settings.delay_seconds = min(base_delay * multiplier, RATE_LIMIT_MAX_DELAY)

        # 4. Set page budget based on yield rate
        yield_rate = self.performance.get("lead_yield_rate", 0.0)
        total_tested = self.performance.get("total_urls_tested", 0)

        if total_tested < 10:
            settings.max_pages = MAX_PAGES_NEW_SOURCE
        elif yield_rate > PRODUCER_YIELD_THRESHOLD:
            settings.max_pages = MAX_PAGES_HIGH_YIELD
        elif yield_rate > MODERATE_YIELD_THRESHOLD:
            settings.max_pages = MAX_PAGES_MEDIUM_YIELD
        else:
            settings.max_pages = MAX_PAGES_LOW_YIELD

        # 5. Set engine preference
        settings.use_playwright = self.behavior.get("requires_playwright", False)

        # 6. Gold and junk patterns for URL filtering
        settings.gold_patterns = list(self.patterns.get("gold", []))
        settings.junk_patterns = list(self.patterns.get("junk", []))

        # 7. Priority score for scheduling
        settings.priority_score = self.efficiency_score

        return settings

    def should_follow_url(self, url: str) -> Tuple[bool, str]:
        """
        Decide whether to follow a URL found on a listing page.

        Returns:
            (should_follow, reason)
        """

        # Check junk patterns first (FREE reject)
        for junk in self.patterns.get("junk", []):
            if re.search(junk, url):
                return False, f"Matches junk pattern: {junk}"

        # Check gold patterns (always follow)
        for gold in self.patterns.get("gold", []):
            if re.search(gold, url):
                return True, f"Matches gold pattern: {gold}"

        # Unknown pattern — follow if within page budget
        return True, "Unknown pattern — following cautiously"

    # ─── POST-SCRAPE: RECORD & LEARN ─────────────────────────

    def record_url_result(
        self,
        url: str,
        produced_lead: bool,
        lead_count: int = 0,
        lead_quality: float = 0.0,
        lead_location: str = "",
        response_time_ms: int = 0,
    ):
        """
        Record the result of scraping a single URL.

        This is how the system learns — every URL result updates
        pattern statistics and triggers reclassification.
        """
        path_pattern = self._extract_path_pattern(url)
        now = datetime.now().isoformat()

        # Update pattern stats
        stats_dict = self.patterns.setdefault("stats", {})
        if path_pattern not in stats_dict:
            stats_dict[path_pattern] = {
                "tested": 0,
                "leads": 0,
                "total_quality": 0.0,
                "usa_caribbean": 0,
                "last_tested": "",
            }

        stats = stats_dict[path_pattern]
        stats["tested"] += 1
        stats["last_tested"] = now

        if produced_lead:
            stats["leads"] += lead_count or 1
            stats["total_quality"] += lead_quality
            if lead_location in ("USA", "Caribbean", "Florida"):
                stats["usa_caribbean"] += lead_count or 1

        # Update global performance counters
        perf = self.performance
        perf["total_urls_tested"] = perf.get("total_urls_tested", 0) + 1
        if produced_lead:
            perf["urls_with_leads"] = perf.get("urls_with_leads", 0) + 1

        # Record response time
        if response_time_ms > 0:
            times = self.behavior.setdefault("response_times", [])
            times.append(response_time_ms)
            # Keep last 50
            if len(times) > 50:
                self.behavior["response_times"] = times[-50:]
            self.behavior["avg_response_ms"] = sum(
                self.behavior["response_times"]
            ) / len(self.behavior["response_times"])

        # Reclassify pattern if enough data
        self._reclassify_pattern(path_pattern, stats)

    def record_scrape_run(
        self,
        pages_scraped: int,
        leads_found: int,
        leads_saved: int,
        duration_seconds: float,
        errors: int = 0,
        mode: str = "discovery",
        pages_relevant: int = 0,
        pages_classified: int = 0,
        leads_extracted: int = 0,
    ):
        """
        Record a complete scrape run for this source.

        Called once per source at the end of a scrape cycle.
        Updates history and recalculates efficiency.

        New 2026-05-04: pages_relevant / pages_classified / leads_extracted
        capture the funnel stages so we can distinguish "junk source"
        from "broken extractor" later. A source where
        pages_relevant > 0 but leads_extracted == 0 has the right
        content but the extractor is failing on it.
        """
        run = {
            "at": datetime.now().isoformat(),
            "mode": mode,
            "pages": pages_scraped,
            "pages_relevant": pages_relevant,
            "pages_classified": pages_classified,
            "leads_extracted": leads_extracted,
            "leads_found": leads_found,
            "leads_saved": leads_saved,
            "duration_s": round(duration_seconds, 1),
            "errors": errors,
            "yield_rate": round(leads_found / max(pages_scraped, 1), 3),
        }

        self.history.append(run)
        # Keep only last N runs
        if len(self.history) > MAX_RUN_HISTORY:
            self._data["history"] = self.history[-MAX_RUN_HISTORY:]

        # Recalculate everything
        self._recalculate_performance()
        self._recalculate_efficiency_score()
        self._update_publish_frequency()

    def record_rate_limit(self, status_code: int = 429):
        """
        Record a rate limit or block response.

        Adapts future request timing automatically.
        """
        now = datetime.now().isoformat()

        if status_code == 429:
            self.rate_limits["total_429s"] = self.rate_limits.get("total_429s", 0) + 1
            self.rate_limits["last_429_at"] = now

            # Increase backoff
            current = self.rate_limits.get("backoff_multiplier", 1.0)
            self.rate_limits["backoff_multiplier"] = min(
                current * RATE_LIMIT_BACKOFF_FACTOR, 5.0
            )

            # Increase base delay
            current_delay = self.rate_limits.get("delay_seconds", 1.0)
            self.rate_limits["delay_seconds"] = min(
                current_delay + 0.5, RATE_LIMIT_MAX_DELAY
            )

            logger.warning(
                f"⚠️ Rate limited by {self.source.name} — "
                f"delay now {self.rate_limits['delay_seconds']}s, "
                f"backoff {self.rate_limits['backoff_multiplier']}x"
            )

        elif status_code == 403:
            self.rate_limits["total_403s"] = self.rate_limits.get("total_403s", 0) + 1
            logger.warning(
                f"🚫 Blocked (403) by {self.source.name} — "
                f"total blocks: {self.rate_limits['total_403s']}"
            )

    def record_engine_result(self, engine: str, success: bool):
        """
        Record which scrape engine worked for this source.

        Over time, the system learns which engine is most reliable.
        """
        engine_stats = self.behavior.setdefault("engine_stats", {})
        if engine not in engine_stats:
            engine_stats[engine] = {"attempts": 0, "successes": 0}

        engine_stats[engine]["attempts"] += 1
        if success:
            engine_stats[engine]["successes"] += 1

        # Update best engine recommendation
        best_engine = "httpx"
        best_rate = 0.0
        for eng, stats in engine_stats.items():
            rate = stats["successes"] / max(stats["attempts"], 1)
            if rate > best_rate:
                best_rate = rate
                best_engine = eng

        self.behavior["best_engine"] = best_engine

        # If httpx fails consistently, recommend playwright
        httpx_stats = engine_stats.get("httpx", {})
        if httpx_stats.get("attempts", 0) >= 5:
            httpx_rate = httpx_stats["successes"] / httpx_stats["attempts"]
            if httpx_rate < 0.5:
                self.behavior["requires_playwright"] = True

    # ─── PATTERN ANALYSIS ─────────────────────────────────────

    def _reclassify_pattern(self, pattern: str, stats: dict):
        """
        Promote or demote a URL pattern based on accumulated evidence.

        Patterns flow: unknown → maybe → gold/junk
        """
        tested = stats.get("tested", 0)
        leads = stats.get("leads", 0)
        hit_rate = leads / max(tested, 1)

        gold = self.patterns.setdefault("gold", [])
        junk = self.patterns.setdefault("junk", [])
        maybe = self.patterns.setdefault("maybe", [])

        # Promote to gold if high hit rate with enough evidence
        if tested >= GOLD_MIN_TESTED and hit_rate >= GOLD_MIN_HIT_RATE:
            if pattern not in gold:
                gold.append(pattern)
                logger.info(
                    f"🥇 Pattern promoted to GOLD for {self.source.name}: "
                    f"{pattern} ({hit_rate:.0%} yield, {tested} tests)"
                )
            # Remove from other lists
            if pattern in junk:
                junk.remove(pattern)
            if pattern in maybe:
                maybe.remove(pattern)

        # Demote to junk if zero hits with enough evidence
        elif tested >= JUNK_MIN_TESTED and hit_rate <= JUNK_MAX_HIT_RATE:
            if pattern not in junk:
                junk.append(pattern)
                logger.info(
                    f"🗑️ Pattern demoted to JUNK for {self.source.name}: "
                    f"{pattern} (0% yield after {tested} tests)"
                )
            # Remove from other lists
            if pattern in gold:
                gold.remove(pattern)
            if pattern in maybe:
                maybe.remove(pattern)

        # Otherwise it's a maybe
        elif pattern not in gold and pattern not in junk and pattern not in maybe:
            maybe.append(pattern)

    def _extract_path_pattern(self, url: str) -> str:
        r"""
        Convert a URL path to a generalized regex pattern.

        /news/hotel-opens-miami/12345/ → /news/[^/]+/\d+/
        /2024/01/15/hotel-name/        → /\d{4}/\d{2}/\d{2}/[^/]+/
        /releases/marriott-news        → /releases/[^/]+
        """
        try:
            parsed = urlparse(url)
            path = parsed.path

            # Replace numeric segments
            pattern = re.sub(r"/\d+/", r"/\\d+/", path)
            pattern = re.sub(r"/\d+\.", r"/\\d+\\.", pattern)
            pattern = re.sub(r"\d{4}/\d{2}/\d{2}", r"\\d{4}/\\d{2}/\\d{2}", pattern)

            # Simplify slug segments but keep meaningful prefixes
            parts = pattern.split("/")
            new_parts = []
            for part in parts:
                if not part:
                    new_parts.append(part)
                elif part.startswith("\\d"):
                    new_parts.append(part)
                elif re.match(r"^[a-z]{2,20}$", part):
                    new_parts.append(part)  # Keep short category words
                elif len(part) > 30:
                    new_parts.append("[^/]+")
                else:
                    new_parts.append(part)

            return "/".join(new_parts)
        except Exception:
            return url

    # ─── PERFORMANCE CALCULATIONS ─────────────────────────────

    def _recalculate_performance(self):
        """Recalculate all performance metrics from pattern stats."""
        perf = self.performance
        total_tested = 0
        total_with_leads = 0
        total_quality = 0.0
        total_usa = 0
        total_leads = 0

        for pattern, stats in self.patterns.get("stats", {}).items():
            total_tested += stats.get("tested", 0)
            total_with_leads += stats.get("leads", 0)
            total_quality += stats.get("total_quality", 0.0)
            total_usa += stats.get("usa_caribbean", 0)
            total_leads += stats.get("leads", 0)

        perf["total_urls_tested"] = total_tested
        perf["urls_with_leads"] = total_with_leads
        perf["lead_yield_rate"] = total_with_leads / max(total_tested, 1)
        perf["usa_caribbean_rate"] = total_usa / max(total_leads, 1)
        perf["avg_lead_quality"] = total_quality / max(total_leads, 1)

    def _recalculate_efficiency_score(self):
        """
        Calculate composite efficiency score (1-10).

        This score drives scheduling decisions:
        - High score → scrape more frequently
        - Low score → scrape less, eventually deactivate
        """
        perf = self.performance
        yield_rate = perf.get("lead_yield_rate", 0.0)
        usa_rate = perf.get("usa_caribbean_rate", 0.0)
        avg_quality = perf.get("avg_lead_quality", 0.0)

        # Reliability: based on recent run history
        recent = self.history[-5:] if self.history else []
        if recent:
            error_rate = sum(r.get("errors", 0) for r in recent) / len(recent)
            reliability = max(0, 1.0 - error_rate)
        else:
            reliability = 0.5  # Unknown

        # Speed: normalize response time (faster = better)
        avg_ms = self.behavior.get("avg_response_ms", 1000)
        speed = max(0, min(1.0, 1.0 - (avg_ms / 5000)))  # 0-5000ms range

        # Weighted composite
        raw_score = (
            yield_rate * WEIGHT_YIELD_RATE
            + usa_rate * WEIGHT_USA_RATE
            + min(avg_quality, 1.0) * WEIGHT_AVG_QUALITY
            + reliability * WEIGHT_RELIABILITY
            + speed * WEIGHT_SPEED
        )

        # Scale to 1-10 (logistic curve for better distribution)
        score = 1 + 9 / (1 + math.exp(-10 * (raw_score - 0.15)))
        self.performance["efficiency_score"] = round(score, 1)

    def _update_publish_frequency(self):
        """
        Estimate how often this source publishes new content.

        Looks at how many new leads appear per run over time.
        """
        recent = self.history[-10:] if self.history else []
        if len(recent) < 2:
            return

        # Calculate average days between runs
        dates = []
        for run in recent:
            try:
                dates.append(datetime.fromisoformat(run["at"]))
            except (KeyError, ValueError):
                pass

        if len(dates) < 2:
            return

        total_days = (dates[-1] - dates[0]).total_seconds() / 86400
        avg_gap = total_days / (len(dates) - 1)

        # Average new leads per run
        avg_leads = sum(r.get("leads_saved", 0) for r in recent) / len(recent)

        # Estimate weekly article rate
        if avg_gap > 0:
            runs_per_week = 7 / avg_gap
            self.behavior["avg_new_articles_per_week"] = round(
                avg_leads * runs_per_week, 1
            )

        # Recommend scrape frequency
        if avg_leads > 2:
            self.behavior["publish_frequency_days"] = 2
        elif avg_leads > 0.5:
            self.behavior["publish_frequency_days"] = 4
        else:
            self.behavior["publish_frequency_days"] = 7

    # ─── PERSISTENCE ──────────────────────────────────────────

    def save(self):
        """Write intelligence back to the source model."""
        self.source.source_intelligence = dict(self._data)

    # ─── REPORTING ────────────────────────────────────────────

    def get_report(self) -> dict:
        """Get a summary report of this source's intelligence."""
        gold = self.patterns.get("gold", [])
        junk = self.patterns.get("junk", [])
        maybe = self.patterns.get("maybe", [])

        return {
            "name": self.source.name,
            "efficiency_score": self.efficiency_score,
            "performance": {
                "total_urls_tested": self.performance.get("total_urls_tested", 0),
                "lead_yield_rate": f"{self.performance.get('lead_yield_rate', 0):.1%}",
                "usa_caribbean_rate": f"{self.performance.get('usa_caribbean_rate', 0):.1%}",
                "avg_lead_quality": round(
                    self.performance.get("avg_lead_quality", 0), 2
                ),
            },
            "patterns": {
                "gold": len(gold),
                "junk": len(junk),
                "maybe": len(maybe),
            },
            "rate_limits": {
                "delay_seconds": self.rate_limits.get("delay_seconds", 1.0),
                "total_429s": self.rate_limits.get("total_429s", 0),
                "is_rate_limited": self.is_rate_limited,
            },
            "behavior": {
                "avg_response_ms": round(self.behavior.get("avg_response_ms", 0)),
                "best_engine": self.behavior.get("best_engine", "httpx"),
                "publish_frequency_days": self.behavior.get(
                    "publish_frequency_days", 7
                ),
            },
            "last_runs": self.history[-3:] if self.history else [],
        }

    def __repr__(self):
        return (
            f"<SourceIntelligence({self.source.name}, "
            f"score={self.efficiency_score}, "
            f"yield={self.performance.get('lead_yield_rate', 0):.1%})>"
        )


# ═══════════════════════════════════════════════════════════════
# HELPER: Rate limit aware delay
# ═══════════════════════════════════════════════════════════════


def _rate_limit_expires_at(self) -> str:
    """Human-readable time when rate limit cooldown expires."""
    last_429 = self.rate_limits.get("last_429_at")
    if not last_429:
        return "N/A"
    try:
        expires = datetime.fromisoformat(last_429) + timedelta(
            hours=RATE_LIMIT_COOLDOWN_HOURS
        )
        return expires.strftime("%I:%M %p")
    except (ValueError, TypeError):
        return "unknown"


# Attach to class
SourceIntelligence._rate_limit_expires_at = _rate_limit_expires_at
