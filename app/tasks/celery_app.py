"""
Celery Application Configuration
--------------------------------
Background task processing with Redis as broker

Features:
- Async task execution
- Scheduled tasks (beat scheduler)
- Task retry with exponential backoff
- Result backend for task status tracking

Usage:
    # Start worker
    celery -A app.tasks.celery_app worker --loglevel=info

    # Start beat scheduler (for periodic tasks)
    celery -A app.tasks.celery_app beat --loglevel=info

    # Start both (development only)
    celery -A app.tasks.celery_app worker --beat --loglevel=info
"""

from celery import Celery
from celery.schedules import crontab

from ..config import settings

# Create Celery app
celery_app = Celery(
    "smart_lead_hunter",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.tasks.scraping_tasks", "app.tasks.autonomous_tasks"],
)

# Celery configuration
celery_app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="America/New_York",  # Florida timezone
    enable_utc=True,
    # Task execution settings
    task_acks_late=True,  # Acknowledge after task completes
    task_reject_on_worker_lost=True,  # Requeue if worker dies
    worker_prefetch_multiplier=1,  # One task at a time per worker
    # Result settings
    result_expires=86400,  # Results expire after 24 hours
    # Retry settings
    task_default_retry_delay=60,  # 1 minute default retry delay
    task_max_retries=3,
    # Rate limiting (be nice to websites)
    task_annotations={
        "app.tasks.scraping_tasks.scrape_source": {
            "rate_limit": "10/m"  # Max 10 scrape tasks per minute
        },
        "app.tasks.scraping_tasks.scrape_single_url": {
            "rate_limit": "30/m"  # Max 30 URL scrapes per minute
        },
    },
    # Beat scheduler (periodic tasks)
    #
    # Schedule rationale (revised 2026-05-01)
    # ────────────────────────────────────────
    # Celery worker + beat run on Jay's local machine via start_slh.bat
    # (Mon-Fri, ~9 AM-5 PM). They do NOT run at night or on weekends —
    # the machine is off. Beat does NOT replay missed schedules either:
    # if the trigger time elapses while beat is down, that fire is lost.
    #
    # Stretched window: 9:30 AM - 4:30 PM Mon-Fri.
    # Tasks are spaced with 60-90 min gaps so Gemini / Serper / Geoapify
    # never hit back-to-back load. AI-heavy tasks (auto_enrich, smart_scrape,
    # auto_smart_fill) get the most breathing room — auto_enrich alone
    # burns ~50+ Gemini calls per run on contact research. Bunched
    # scheduling caused 429 RESOURCE_EXHAUSTED in the past.
    #
    # Daily flow:
    #   morning   → DB maintenance (cheap)
    #   late-AM   → smart_fill empty fields (so afternoon enrich works)
    #   noon      → first scrape + enrich pass
    #   afternoon → second scrape + enrich pass
    #   late-PM   → final scrape (lighter, end-of-day catch-up)
    beat_schedule={
        # Recompute timeline labels: 9:30 AM Mon-Fri.
        # DB-only — drains expired → existing, resurrects ghosts. Runs
        # FIRST so the rest of the day operates on a clean dataset.
        # Mon-Fri only (was "daily incl weekends" — but Celery doesn't
        # run weekends in this setup, so the daily schedule was a lie).
        # Monday's run sweeps any weekend drift in one pass.
        "recompute-timeline-labels": {
            "task": "recompute_timeline_labels",
            "schedule": crontab(hour=9, minute=30, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # Daily health check: 9:45 AM Mon-Fri.
        # DB-only — cleanup, deactivate dead sources, rescore up to 50
        # stale leads. No external API calls.
        "startup-health-check": {
            "task": "daily_health_check",
            "schedule": crontab(hour=9, minute=45, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # Auto Smart Fill: 10:30 AM Mon-Fri (NEW 2026-05-01).
        # Backfills opening_date / brand_tier / room_count /
        # management_company on top 10 highest-score leads where any
        # of these are empty. CRITICAL — auto_enrich filters by
        # opening_date (HOT/URGENT bucket), so leads with empty dates
        # never get contact-enriched. This task fills those fields
        # BEFORE the noon auto_enrich runs.
        "auto-smart-fill": {
            "task": "auto_smart_fill",
            "schedule": crontab(hour=10, minute=30, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Auto Full Refresh: 11:00 AM Mon-Fri (NEW 2026-05-04).
        # Re-checks 5 staleest leads (>14 days since last update) via
        # grounded research. Catches the Atlas-style failure mode where
        # a lead's opening_date drifts behind reality. Auto-transfers
        # leads that have actually opened to existing_hotels.
        # Slotted between auto_smart_fill (10:30) and smart_scrape #1
        # (11:30) — light load, doesn't compete for Gemini quota.
        "auto-full-refresh": {
            "task": "auto_full_refresh",
            "schedule": crontab(hour=11, minute=0, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Smart Scrape Round 1: 11:30 AM Mon-Fri.
        # Heavy: Serper + Gemini classification per source.
        "smart-scrape-am": {
            "task": "smart_scrape",
            "schedule": crontab(hour=11, minute=30, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Auto-Enrich Round 1: 1:00 PM Mon-Fri.
        # HEAVIEST task — 6-iteration contact research, ~50+ Gemini
        # calls + ~30 Serper calls for up to 5 leads. 90 min space
        # after smart_scrape so Gemini quota recovers.
        "auto-enrich-am": {
            "task": "auto_enrich",
            "schedule": crontab(hour=13, minute=0, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Smart Scrape Round 2: 2:30 PM Mon-Fri.
        "smart-scrape-mid": {
            "task": "smart_scrape",
            "schedule": crontab(hour=14, minute=30, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Auto-Enrich Round 2: 3:30 PM Mon-Fri.
        # Second contact-enrichment pass on whatever the morning scrape
        # discovered.
        "auto-enrich-pm": {
            "task": "auto_enrich",
            "schedule": crontab(hour=15, minute=30, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Smart Scrape Round 3: 4:30 PM Mon-Fri (last task of the day).
        # Lighter end-of-day pull on remaining due sources.
        "smart-scrape-pm": {
            "task": "smart_scrape",
            "schedule": crontab(hour=16, minute=30, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Weekly Discovery: Thursday 12:00 PM.
        # Finds new sources from the web. Slots between smart_scrape #1
        # (11:30) and auto_enrich #1 (13:00) on Thursday only.
        "weekly-discovery": {
            "task": "weekly_discovery",
            "schedule": crontab(hour=12, minute=0, day_of_week=4),
            "options": {"queue": "scraping"},
        },
    },
    # Task routing
    task_routes={
        "app.tasks.scraping_tasks.scrape_*": {"queue": "scraping"},
        "app.tasks.scraping_tasks.run_full_scrape": {"queue": "scraping"},
        "app.tasks.scraping_tasks.update_*": {"queue": "maintenance"},
        "app.tasks.scraping_tasks.check_*": {"queue": "maintenance"},
        "app.tasks.scraping_tasks.sync_*": {"queue": "crm"},
    },
    # Queue configuration
    task_queues={
        "scraping": {"exchange": "scraping", "routing_key": "scraping"},
        "maintenance": {"exchange": "maintenance", "routing_key": "maintenance"},
        "crm": {"exchange": "crm", "routing_key": "crm"},
    },
)


class BaseTask(celery_app.Task):
    """Base task with automatic retry on recoverable failures.

    L-12 FIX: Previously used autoretry_for=(Exception,) which catches
    EVERYTHING including SystemExit and KeyboardInterrupt, preventing
    clean worker shutdown (Ctrl+C wouldn't stop workers).

    Now limited to specific recoverable exceptions:
    - ConnectionError: DB/Redis/HTTP connection lost
    - TimeoutError: Network timeouts
    - OSError: File/network I/O errors
    - RuntimeError: Async loop issues, etc.

    Non-recoverable errors (SystemExit, KeyboardInterrupt, MemoryError,
    ValueError, TypeError) will NOT be retried — they'll fail immediately
    and get logged.
    """

    autoretry_for = (
        ConnectionError,  # DB/Redis/HTTP connection lost
        TimeoutError,  # Network timeouts
        OSError,  # File/network I/O errors
        RuntimeError,  # Async event loop issues
    )
    retry_backoff = True  # Exponential backoff
    retry_backoff_max = 600  # Max 10 minutes between retries
    retry_jitter = True  # Add randomness to prevent thundering herd

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Log task failures"""
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Task {self.name}[{task_id}] failed: {exc}")

    def on_success(self, retval, task_id, args, kwargs):
        """Log task success"""
        import logging

        logger = logging.getLogger(__name__)
        logger.info(f"Task {self.name}[{task_id}] completed successfully")


# Export for use in tasks
__all__ = ["celery_app", "BaseTask"]
