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


# ─────────────────────────────────────────────────────────────────────────────
# Per-agent LLM factories
#
# Different agents need different temperatures to balance creativity vs
# faithfulness. Hallucination is the #1 risk in fact-extraction agents
# (Researcher, Analyst), so we run them at near-zero temp. The Writer
# needs some creativity to vary phrasing, so it sits at 0.4. The Critic
# is a strict yes/no judge → 0.0. Keeps things deterministic where it
# matters and creative where it helps.
#
# All factories are LRU-cached so we only pay the credential / network
# setup cost once per process.
# ─────────────────────────────────────────────────────────────────────────────


def _build_llm(model: str, temperature: float, max_tokens: int):
    """Internal — builds a ChatVertexAI with the given config."""
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
        model=model,
        project=project,
        location=location,
        temperature=temperature,
        max_output_tokens=max_tokens,
    )
    if creds is not None:
        kwargs["credentials"] = creds
    return ChatVertexAI(**kwargs)


@lru_cache(maxsize=1)
def get_researcher_llm():
    """Researcher synthesis — fact extraction, near-zero creativity.
    Hallucinated facts here poison every downstream agent."""
    return _build_llm("gemini-2.5-flash", temperature=0.1, max_tokens=8192)


@lru_cache(maxsize=1)
def get_analyst_llm():
    """Analyst scoring + value props — should be deterministic.
    The base score is given; we just want consistent adjustments.
    8192 tokens — value_props with 6 categorized props + fit_breakdown
    rationale + primary_angle. 4096 was still getting truncated mid-
    rationale, which failed JSON parse and silently dropped the score
    back to default."""
    return _build_llm("gemini-2.5-flash", temperature=0.1, max_tokens=8192)


@lru_cache(maxsize=1)
def get_writer_llm():
    """Writer email + LinkedIn — needs some creativity for varied phrasing.
    But still capped at 0.4 — beyond that the model starts inventing facts.
    4096 tokens — combined email body + LinkedIn message + tone field
    plus Critic-feedback regeneration on retry runs."""
    return _build_llm("gemini-2.5-flash", temperature=0.4, max_tokens=4096)


@lru_cache(maxsize=1)
def get_critic_llm():
    """Critic — strict rubric judge, fully deterministic."""
    return _build_llm("gemini-2.5-flash-lite", temperature=0.0, max_tokens=2048)


@lru_cache(maxsize=1)
def get_validator_llm():
    """Validator (between Researcher and Analyst) — checks claims against
    source. Must be deterministic and cheap (Flash Lite)."""
    return _build_llm("gemini-2.5-flash-lite", temperature=0.0, max_tokens=4096)


# ─────────────────────────────────────────────────────────────────────────────
# Backwards-compat — older agents called get_llm() / get_llm_lite() before
# we split into per-agent factories. Keep these as aliases pointing at the
# closest match so the ported PitchIQ code that hasn't been refactored yet
# (e.g., the /sequence endpoint's inline LLM call) still works.
# ─────────────────────────────────────────────────────────────────────────────


def get_llm():
    """Deprecated alias — defaults to the writer LLM (creative, 0.4)."""
    return get_writer_llm()


def get_llm_lite():
    """Deprecated alias — defaults to the critic LLM (deterministic, 0.0)."""
    return get_critic_llm()


# Backwards-compat aliases so the ported agents can `from .config import llm`
# without changes. Lazy properties via module-level __getattr__.
def __getattr__(name):
    if name == "llm":
        return get_llm()
    if name == "llm_lite":
        return get_llm_lite()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
