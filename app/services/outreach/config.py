"""LangChain LLM clients built from SLH's existing Vertex AI settings.

PitchIQ's original config hard-coded a project string and relied on
Application Default Credentials. SLH already authenticates to Vertex AI
via a service account JSON file (path lives in `vertex_key_path`).
We reuse the same file here so:
  - One auth method across the codebase (no `gcloud auth ADC` needed)
  - Same project/location config
  - Whatever credential SLH was already using for Smart Fill / contact
    enrichment also drives outreach
"""

from __future__ import annotations

import os
import logging
from functools import lru_cache
from pathlib import Path

from app.config import settings

logger = logging.getLogger(__name__)


SERPER_API_KEY = os.getenv("SERPER_API_KEY") or getattr(
    settings, "serper_api_key", None
)

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY") or getattr(
    settings, "apollo_api_key", None
)


def _vertex_project() -> str:
    return (
        getattr(settings, "vertex_project_id", None)
        or os.getenv("VERTEX_PROJECT_ID", "")
        or os.getenv("GOOGLE_CLOUD_PROJECT", "")
    )


def _vertex_location() -> str:
    return getattr(settings, "vertex_location", None) or os.getenv(
        "VERTEX_LOCATION", "us-central1"
    )


def _vertex_key_path() -> str:
    return getattr(settings, "vertex_key_path", None) or os.getenv(
        "VERTEX_KEY_PATH", "vertex-key.json"
    )


@lru_cache(maxsize=1)
def _load_service_account_credentials():
    """Load the service-account JSON file SLH already uses for Vertex AI.

    Mirrors the resolution logic from app/services/ai_client.py: try the
    raw path, then fall back to project-root + path. If the file is
    missing we return None — ChatVertexAI will then fall back to ADC
    (which is also fine if the dev has run `gcloud auth application-
    default login`).
    """
    key_path = Path(_vertex_key_path())
    if not key_path.is_file():
        key_path = (
            Path(__file__).resolve().parent.parent.parent.parent / _vertex_key_path()
        )
    if not key_path.is_file():
        logger.warning(
            f"Vertex AI key not found at {_vertex_key_path()} — outreach "
            f"agents will fall back to Application Default Credentials. "
            f"Run `gcloud auth application-default login` if this fails."
        )
        return None
    try:
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(
            str(key_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        logger.info(f"Outreach: loaded Vertex AI service account from {key_path}")
        return creds
    except Exception as e:
        logger.error(f"Failed to load Vertex AI service account: {e}")
        return None


@lru_cache(maxsize=1)
def get_llm():
    """Main writing/analysis model (Gemini 2.5 Flash)."""
    from langchain_google_vertexai import ChatVertexAI

    project = _vertex_project()
    location = _vertex_location()
    if not project:
        raise RuntimeError(
            "Outreach agents require Vertex AI configured. Set "
            "VERTEX_PROJECT_ID in .env (or settings.vertex_project_id)."
        )
    creds = _load_service_account_credentials()
    kwargs = dict(
        model="gemini-2.5-flash",
        project=project,
        location=location,
        temperature=0.4,
        # 8192 — the Researcher synthesis returns a deeply-structured
        # JSON object (hotel_intel + contact_intel + 3 conversation_hooks
        # + recent_news + signals + pain_points). 2048 was getting
        # truncated mid-output, leaving the parser with malformed JSON.
        max_output_tokens=8192,
    )
    if creds is not None:
        kwargs["credentials"] = creds
    return ChatVertexAI(**kwargs)


@lru_cache(maxsize=1)
def get_llm_lite():
    """Cheaper model for the Critic + cleanup tasks (Gemini 2.5 Flash Lite)."""
    from langchain_google_vertexai import ChatVertexAI

    project = _vertex_project()
    location = _vertex_location()
    if not project:
        raise RuntimeError(
            "Outreach agents require Vertex AI configured. Set "
            "VERTEX_PROJECT_ID in .env (or settings.vertex_project_id)."
        )
    creds = _load_service_account_credentials()
    kwargs = dict(
        model="gemini-2.5-flash-lite",
        project=project,
        location=location,
        temperature=0.2,
        max_output_tokens=2048,
    )
    if creds is not None:
        kwargs["credentials"] = creds
    return ChatVertexAI(**kwargs)


# Backwards-compat aliases so the ported agents can `from .config import llm`
# without changes. Lazy properties via module-level __getattr__.
def __getattr__(name):
    if name == "llm":
        return get_llm()
    if name == "llm_lite":
        return get_llm_lite()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
