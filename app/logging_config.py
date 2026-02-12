"""
SMART LEAD HUNTER — Logging Configuration
Fix: R7 (disk fills with logs — add log rotation)

Usage in main.py (add near top):
    from app.logging_config import setup_logging
    setup_logging()
"""

import logging
import logging.handlers
import os
from pathlib import Path


def setup_logging(
    log_dir: str = "logs",
    log_level: str | None = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB per file
    backup_count: int = 5,  # Keep 5 rotated files (50 MB total max)
):
    """
    Configure logging with rotation.

    Creates two handlers:
    - Console: INFO+ to stdout
    - File: DEBUG+ to logs/smart_lead_hunter.log with rotation

    Args:
        log_dir: Directory for log files
        log_level: Override log level (default: from ENVIRONMENT)
        max_bytes: Max size per log file before rotation
        backup_count: Number of rotated files to keep
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # Determine level from environment
    if log_level is None:
        env = os.getenv("ENVIRONMENT", "development")
        log_level = "DEBUG" if env == "development" else "INFO"

    level = getattr(logging, log_level.upper(), logging.INFO)

    # Format
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # capture everything, handlers filter

    # Clear existing handlers (avoid duplicates on reload)
    root.handlers.clear()

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(fmt)
    root.addHandler(console)

    # Rotating file handler
    log_file = os.path.join(log_dir, "smart_lead_hunter.log")
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    # Separate error log (errors only, for quick triage)
    error_file = os.path.join(log_dir, "errors.log")
    error_handler = logging.handlers.RotatingFileHandler(
        error_file,
        maxBytes=max_bytes,
        backupCount=3,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(fmt)
    root.addHandler(error_handler)

    # Quiet noisy third-party loggers
    for name in ["httpx", "httpcore", "urllib3", "asyncio", "playwright"]:
        logging.getLogger(name).setLevel(logging.WARNING)

    logging.info(f"Logging initialized: level={log_level}, file={log_file}")
