"""app/services/mailbox_discovery.py

Lists all active @jauniforms.com mailboxes via the Google Admin Directory API.
Used by the Celery beat task to discover which mailboxes to sync without
hardcoding the list.

Credentials: credentials/slh-contact-sync.json (service account with
domain-wide delegation). Requires the Admin SDK Directory API scope:
  https://www.googleapis.com/auth/admin.directory.user.readonly

The impersonated admin account must have read access to the user directory.
"""

from __future__ import annotations
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CREDENTIALS_PATH = PROJECT_ROOT / "credentials" / "slh-contact-sync.json"

DOMAIN = "jauniforms.com"

# Admin account to impersonate — must have Directory read permissions.
# Override via DOMAIN_ADMIN_EMAIL env var at runtime.

load_dotenv()
DOMAIN_ADMIN_EMAIL = os.getenv("DOMAIN_ADMIN_EMAIL", "ugarcia@jauniforms.com")

DIRECTORY_SCOPES = [
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
]

# Mailboxes to always exclude from syncing (shared inboxes, role accounts, etc.)
EXCLUDED_MAILBOXES: set[str] = set()


def _build_directory_client():
    """Build an authenticated Admin SDK Directory API client."""
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(
            f"Service account credentials not found: {CREDENTIALS_PATH}"
        )
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH),
        scopes=DIRECTORY_SCOPES,
    )
    delegated = creds.with_subject(DOMAIN_ADMIN_EMAIL)
    return build("admin", "directory_v1", credentials=delegated, cache_discovery=False)


def list_active_mailboxes(
    domain: str = DOMAIN,
    exclude: Optional[set[str]] = None,
    include_suspended: bool = False,
) -> list[str]:
    """Return a sorted list of active @jauniforms.com email addresses.

    Args:
        domain: Google Workspace domain to query.
        exclude: Additional mailboxes to skip (merged with EXCLUDED_MAILBOXES).
        include_suspended: If True, include suspended accounts. Default False.

    Returns:
        Sorted list of email strings, e.g. ["jdoe@jauniforms.com", ...]

    Raises:
        FileNotFoundError: Credentials file missing.
        HttpError: Directory API call failed (auth, quota, etc.)
    """
    skip = EXCLUDED_MAILBOXES | (exclude or set())
    service = _build_directory_client()

    mailboxes: list[str] = []
    page_token: Optional[str] = None

    while True:
        try:
            resp = (
                service.users()
                .list(
                    domain=domain,
                    maxResults=500,
                    orderBy="email",
                    pageToken=page_token,
                    projection="basic",  # name + email + suspended flag only
                )
                .execute()
            )
        except HttpError as e:
            logger.error(f"Directory API error listing users for {domain}: {e}")
            raise

        for user in resp.get("users", []):
            email = (user.get("primaryEmail") or "").lower().strip()
            if not email:
                continue
            if email in skip:
                logger.debug(f"mailbox_discovery: skipping excluded {email}")
                continue
            if not include_suspended and user.get("suspended", False):
                logger.debug(f"mailbox_discovery: skipping suspended {email}")
                continue
            mailboxes.append(email)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info(
        f"mailbox_discovery: found {len(mailboxes)} active mailboxes in {domain}"
    )
    return sorted(mailboxes)


@lru_cache(maxsize=1)
def _cached_mailboxes() -> tuple[str, ...]:
    """Cached mailbox list — refreshed at process start only.

    Use list_active_mailboxes() directly in the Celery task to get a fresh
    list each run. This cache is only for lightweight lookups (e.g. UI).
    """
    return tuple(list_active_mailboxes())


def get_mailboxes_cached() -> list[str]:
    """Return cached mailbox list. Stale until process restart."""
    return list(_cached_mailboxes())


def invalidate_mailbox_cache() -> None:
    """Force next call to get_mailboxes_cached() to re-query Directory API."""
    _cached_mailboxes.cache_clear()
