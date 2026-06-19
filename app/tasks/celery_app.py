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
    # Active window: 9:30 AM - 4:30 PM Mon-Fri (no task starts before
    # 9:30 to give the laptop time to wake; nothing scheduled after 4:30
    # so the final task completes before machine shutdown).
    #
    # Strict scrape → enrich → scrape → enrich pipeline. Every scrape is
    # followed by an enrich within 90 min so newly-discovered leads
    # never sit overnight. Tasks are spaced with 60-90 min gaps so
    # Gemini / Serper / Geoapify never hit back-to-back load.
    #
    # Daily flow (Mon, Tue, Wed, Fri):
    #   9:30-9:50  → DB maintenance burst (recompute, health, digest)
    #   9:50-10:30 → light Gemini (smart_fill, full_refresh)
    #   10:30-4:00 → 3 alternating (scrape, enrich) pairs
    #   4:00-4:30  → final enrich completes, day ends
    #
    # Thursday is different — Weekly Discovery replaces Smart Scrape #1
    # at 11:00. Newly-discovered sources get picked up by Smart Scrape
    # #2 at 1:00 PM the same afternoon, so the discovery → scrape →
    # enrich pipeline completes same-day.
    beat_schedule={
        # ── DB-only maintenance burst (9:30 - 9:50 AM) ──
        # Three lightweight tasks back-to-back. Each is DB-only with no
        # external API calls so they can pack tight.
        # Recompute timeline labels: 9:30 AM Mon-Fri.
        # FIRST task of the day — drains expired → existing_hotels,
        # resurrects ghosts. Runs before everything else so the rest of
        # the day operates on a clean dataset. Monday's run sweeps any
        # weekend drift in one pass.
        "recompute-timeline-labels": {
            "task": "recompute_timeline_labels",
            "schedule": crontab(hour=9, minute=30, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # Daily health check: 9:35 AM Mon-Fri.
        # Cleanup, deactivate dead sources, rescore up to 50 stale leads.
        # DB-only.
        "startup-health-check": {
            "task": "daily_health_check",
            "schedule": crontab(hour=9, minute=35, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # Pre-opening digest: 9:40 AM Mon-Fri (HV-2, 2026-05-06).
        # Email digest of URGENT/HOT leads crossing the 6-12 month
        # uniform procurement window. Runs AFTER recompute_timeline_labels
        # (9:30) so the digest reflects fresh data — no stale boundaries.
        "pre-opening-digest": {
            "task": "pre_opening_digest",
            "schedule": crontab(hour=9, minute=40, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # Hotel intelligence news scan: 10:00 AM Mon-Fri (added 2026-06-05).
        # Serper /news + flash classification + relationship triangulation.
        # Deliberately AFTER sync-inbox-contacts (9:45) so triangulation
        # runs against a freshly synced contacts table.
        "hotel-news-scan": {
            "task": "hotel_news_scan",
            "schedule": crontab(hour=10, minute=0, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # Inbox Contact Sync: 9:45 AM Mon-Fri (added 2026-05-14).
        # Syncs Gmail signatures → contacts table for all active JA mailboxes.
        # Incremental after first run via History API cursor.
        "sync-inbox-contacts": {
            "task": "sync_inbox_contacts",
            "schedule": crontab(hour=9, minute=45, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
        },
        # ── Light Gemini tasks (9:50 - 10:15 AM) ──
        # Cheap grounding calls before heavy scrape/enrich rounds.
        # Auto Smart Fill: 9:50 AM Mon-Fri.
        # Backfills opening_date / brand_tier / room_count /
        # management_company on top 10 highest-score leads where any
        # of these are empty. CRITICAL — auto_enrich filters by
        # opening_date (HOT/URGENT bucket), so leads with empty dates
        # never get contact-enriched without this prefill step.
        "auto-smart-fill": {
            "task": "auto_smart_fill",
            "schedule": crontab(hour=9, minute=50, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Auto Full Refresh: 10:15 AM Mon-Fri.
        # Re-checks 5 staleest leads (>14 days since last update) via
        # grounded research. Catches the Atlas-style failure mode where
        # a lead's opening_date drifts behind reality. Auto-transfers
        # leads that have actually opened to existing_hotels. Light
        # load (~5 Gemini calls), doesn't compete with the heavy tasks.
        "auto-full-refresh": {
            "task": "auto_full_refresh",
            "schedule": crontab(hour=10, minute=15, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # ── Pipeline pairs (10:30 AM - 4:30 PM) ──
        # Strict alternation: scrape → enrich → scrape → enrich → scrape
        # → enrich. Every scrape is followed by an enrich within 90 min
        # so leads never sit overnight unenriched.
        # Smart Scrape #1: 10:30 AM Mon, Tue, Wed, Fri (NOT Thursday).
        # On Thursday this slot is taken by Weekly Discovery at 11:00.
        # Heavy: Serper + Gemini classification per source.
        "smart-scrape-am": {
            "task": "smart_scrape",
            "schedule": crontab(hour=10, minute=30, day_of_week="1,2,3,5"),
            "options": {"queue": "scraping"},
        },
        # Weekly Discovery: Thursday 11:00 AM.
        # Finds new sources from the web. Replaces Smart Scrape #1 on
        # Thursday — newly-discovered sources are then picked up by
        # Smart Scrape #2 at 1:00 PM the same afternoon, completing
        # the discover → scrape → enrich pipeline same-day.
        "weekly-discovery": {
            "task": "weekly_discovery",
            "schedule": crontab(hour=11, minute=0, day_of_week=4),
            "options": {"queue": "scraping"},
        },
        # Auto Enrich #1: 12:00 PM Mon-Fri.
        # HEAVIEST task — 6-iteration contact research, ~50+ Gemini calls
        # + ~30 Serper calls for up to 5 leads. 90 min gap after smart
        # scrape so Gemini quota recovers. Processes leads found by
        # Smart Scrape #1 (or Discovery on Thursday).
        "auto-enrich-am": {
            "task": "auto_enrich",
            "schedule": crontab(hour=12, minute=0, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Smart Scrape #2: 1:00 PM Mon-Fri.
        # Second scrape pass. On Thursday this is where Discovery's new
        # sources first get exercised.
        "smart-scrape-mid": {
            "task": "smart_scrape",
            "schedule": crontab(hour=13, minute=0, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Auto Enrich #2: 2:30 PM Mon-Fri.
        # Second contact-enrichment pass on whatever Smart Scrape #2
        # discovered.
        "auto-enrich-pm": {
            "task": "auto_enrich",
            "schedule": crontab(hour=14, minute=30, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Smart Scrape #3: 3:00 PM Mon-Fri.
        # Third (final) scrape of the day.
        "smart-scrape-pm": {
            "task": "smart_scrape",
            "schedule": crontab(hour=15, minute=0, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Auto Enrich #3: 4:00 PM Mon-Fri (added 2026-05-08).
        # Final enrichment of the day — processes anything Smart Scrape
        # #3 discovered so no leads sit overnight unenriched. Finishes
        # by 4:30 PM (within the 4:30 cutoff window).
        "auto-enrich-eod": {
            "task": "auto_enrich",
            "schedule": crontab(hour=16, minute=0, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        # Rescue Junk: 4:30 PM Mon-Fri (maintenance queue, no contention
        # with the 4:00 enrich on the scraping queue). Second look at
        # LLM-junked contacts that gained an org/title/LinkedIn during
        # the day via Deep Enrich or manual edits. One-shot per row.
        "rescue-junk-contacts": {
            "task": "rescue_junk_contacts",
            "schedule": crontab(hour=16, minute=30, day_of_week="1-5"),
            "options": {"queue": "maintenance"},
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
