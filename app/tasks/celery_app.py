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
    include=["app.tasks.scraping_tasks"],
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
    beat_schedule={
        # Daily full scrape at 6 AM Eastern
        "daily-full-scrape": {
            "task": "app.tasks.scraping_tasks.run_full_scrape",
            "schedule": crontab(hour=6, minute=0),
            "options": {"queue": "scraping"},
        },
        # Check high-priority sources every 6 hours
        "high-priority-scrape": {
            "task": "app.tasks.scraping_tasks.scrape_high_priority_sources",
            "schedule": crontab(hour="*/6", minute=30),
            "options": {"queue": "scraping"},
        },
        # Weekly embedding update (Sundays at 2 AM)
        "weekly-embedding-update": {
            "task": "app.tasks.scraping_tasks.update_all_embeddings",
            "schedule": crontab(hour=2, minute=0, day_of_week=0),
            "options": {"queue": "maintenance"},
        },
        # Daily duplicate cleanup at 3 AM
        "daily-duplicate-check": {
            "task": "app.tasks.scraping_tasks.check_duplicates",
            "schedule": crontab(hour=3, minute=0),
            "options": {"queue": "maintenance"},
        },
        # Sync approved leads to Insightly every hour
        "hourly-insightly-sync": {
            "task": "app.tasks.scraping_tasks.sync_approved_to_insightly",
            "schedule": crontab(minute=15),  # At :15 past every hour
            "options": {"queue": "crm"},
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
