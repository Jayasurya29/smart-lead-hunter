"""
Smart Lead Hunter - Tasks Package
---------------------------------
Celery background tasks for automated scraping and processing

Modules:
- celery_app: Celery configuration and beat schedule
- scraping_tasks: Web scraping and lead processing tasks

Usage:
    # Start Celery worker
    celery -A app.tasks.celery_app worker --loglevel=info

    # Start beat scheduler
    celery -A app.tasks.celery_app beat --loglevel=info

    # Trigger task manually
    from app.tasks.scraping_tasks import run_full_scrape
    run_full_scrape.delay()
"""

from .celery_app import celery_app, BaseTask
from .scraping_tasks import (
    scrape_single_url,
    scrape_source,
    run_full_scrape,
    scrape_high_priority_sources,
    update_all_embeddings,
    check_duplicates,
    sync_approved_to_insightly,
    convert_lead_to_insightly,
    health_check,
)

__all__ = [
    # Celery app
    "celery_app",
    "BaseTask",
    # Scraping tasks
    "scrape_single_url",
    "scrape_source",
    "run_full_scrape",
    "scrape_high_priority_sources",
    # Maintenance tasks
    "update_all_embeddings",
    "check_duplicates",
    # CRM tasks
    "sync_approved_to_insightly",
    "convert_lead_to_insightly",
    # Utility
    "health_check",
]
