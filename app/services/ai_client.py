"""
SMART LEAD HUNTER — AI Client (Provider-Agnostic)
===================================================
Central module for ALL AI/LLM calls.
Supports multiple providers — switch by changing .env:

    AI_PROVIDER=vertex_ai      → Google Gemini via Vertex AI ($300 credits)
    AI_PROVIDER=openai         → OpenAI (GPT-4o, etc.)
    AI_PROVIDER=anthropic      → Anthropic (Claude)
    AI_PROVIDER=ollama         → Local Ollama (free, unlimited)

Change provider or model in .env — zero code changes needed.

Usage:
    from app.services.ai_client import ai_generate, get_ai_url, get_ai_headers

    # Simple — uses whatever provider is configured
    text = await ai_generate(client, "What is 2+2?")

    # With specific model override
    text = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")

    # Low-level access for custom requests
    url = get_ai_url()
    headers = get_ai_headers()
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── State ────────────────────────────────────────────────────────────────────
_initialized = False
_provider = None  # "vertex_ai", "openai", "anthropic", "ollama"
_creds = None  # Vertex AI credentials


# ─── Config ───────────────────────────────────────────────────────────────────


def _get_config() -> dict:
    """Read AI config from app settings / environment."""
    try:
        from app.config import settings

        return {
            "provider": os.getenv(
                "AI_PROVIDER", getattr(settings, "ai_provider", "vertex_ai")
            ),
            "model": getattr(
                settings, "gemini_model", os.getenv("AI_MODEL", "gemini-2.5-flash")
            ),
            "model_lite": getattr(
                settings,
                "gemini_model_lite",
                os.getenv("AI_MODEL_LITE", "gemini-2.5-flash-lite"),
            ),
            # Vertex AI
            "vertex_project_id": getattr(
                settings, "vertex_project_id", os.getenv("VERTEX_PROJECT_ID", "")
            ),
            "vertex_location": getattr(
                settings, "vertex_location", os.getenv("VERTEX_LOCATION", "us-central1")
            ),
            "vertex_key_path": getattr(
                settings,
                "vertex_key_path",
                os.getenv("VERTEX_KEY_PATH", "vertex-key.json"),
            ),
            # OpenAI
            "openai_api_key": os.getenv("OPENAI_API_KEY", ""),
            "openai_base_url": os.getenv(
                "OPENAI_BASE_URL", "https://api.openai.com/v1"
            ),
            # Anthropic
            "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY", ""),
            # Ollama
            "ollama_url": getattr(
                settings,
                "ollama_url",
                os.getenv("OLLAMA_URL", "http://localhost:11434"),
            ),
        }
    except Exception:
        # Fallback if settings not available (scripts, tests)
        return {
            "provider": os.getenv("AI_PROVIDER", "vertex_ai"),
            "model": os.getenv("AI_MODEL", "gemini-2.5-flash"),
            "model_lite": os.getenv("AI_MODEL_LITE", "gemini-2.5-flash-lite"),
            "vertex_project_id": os.getenv("VERTEX_PROJECT_ID", ""),
            "vertex_location": os.getenv("VERTEX_LOCATION", "us-central1"),
            "vertex_key_path": os.getenv("VERTEX_KEY_PATH", "vertex-key.json"),
            "openai_api_key": os.getenv("OPENAI_API_KEY", ""),
            "openai_base_url": os.getenv(
                "OPENAI_BASE_URL", "https://api.openai.com/v1"
            ),
            "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY", ""),
            "ollama_url": os.getenv("OLLAMA_URL", "http://localhost:11434"),
        }


# ─── Initialization ──────────────────────────────────────────────────────────


def _init():
    """Initialize the configured AI provider. Called once on first use."""
    global _initialized, _provider, _creds
    _initialized = True

    config = _get_config()
    _provider = config["provider"]

    if _provider == "vertex_ai":
        _init_vertex(config)
    elif _provider == "openai":
        if not config["openai_api_key"]:
            logger.error("AI_PROVIDER=openai but OPENAI_API_KEY not set in .env")
        else:
            logger.info(f"OpenAI initialized: model={config['model']}")
    elif _provider == "anthropic":
        if not config["anthropic_api_key"]:
            logger.error("AI_PROVIDER=anthropic but ANTHROPIC_API_KEY not set in .env")
        else:
            logger.info(f"Anthropic initialized: model={config['model']}")
    elif _provider == "ollama":
        logger.info(
            f"Ollama initialized: url={config['ollama_url']}, model={config['model']}"
        )
    else:
        logger.error(
            f"Unknown AI_PROVIDER: {_provider}. Valid: vertex_ai, openai, anthropic, ollama"
        )


def _init_vertex(config: dict):
    """Initialize Vertex AI credentials."""
    global _creds

    key_path = Path(config["vertex_key_path"])
    if not key_path.is_file():
        key_path = (
            Path(__file__).resolve().parent.parent.parent / config["vertex_key_path"]
        )

    if not key_path.is_file():
        logger.error(
            f"Vertex AI key not found at {config['vertex_key_path']}! "
            f"AI features will not work. Place the service account JSON in project root."
        )
        return

    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request

        _creds = service_account.Credentials.from_service_account_file(
            str(key_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        _creds.refresh(Request())
        logger.info(
            f"Vertex AI initialized: project={config['vertex_project_id']}, "
            f"location={config['vertex_location']}, model={config['model']}"
        )
    except Exception as e:
        logger.error(f"Failed to init Vertex AI: {e}")
        _creds = None


def _ensure_init():
    """Ensure provider is initialized."""
    if not _initialized:
        _init()


# ─── URL + Headers (for files that make custom requests) ─────────────────────


def get_ai_url(model: str = None) -> str:
    """Get the AI API endpoint URL for the configured provider."""
    _ensure_init()
    config = _get_config()
    model = model or config["model"]

    if _provider == "vertex_ai":
        if _creds is None:
            raise RuntimeError(
                "Vertex AI not configured. Place vertex-key.json in project root."
            )
        project = config["vertex_project_id"]
        location = config["vertex_location"]
        return (
            f"https://{location}-aiplatform.googleapis.com/v1/"
            f"projects/{project}/locations/{location}/"
            f"publishers/google/models/{model}:generateContent"
        )

    elif _provider == "openai":
        return f"{config['openai_base_url']}/chat/completions"

    elif _provider == "anthropic":
        return "https://api.anthropic.com/v1/messages"

    elif _provider == "ollama":
        return f"{config['ollama_url']}/api/generate"

    raise RuntimeError(f"Unknown AI_PROVIDER: {_provider}")


def get_ai_headers() -> dict:
    """Get auth headers for the configured provider."""
    _ensure_init()
    config = _get_config()

    if _provider == "vertex_ai":
        if _creds is None:
            raise RuntimeError("Vertex AI not configured.")
        from google.auth.transport.requests import Request

        if _creds.expired:
            _creds.refresh(Request())
        return {
            "Authorization": f"Bearer {_creds.token}",
            "Content-Type": "application/json",
        }

    elif _provider == "openai":
        return {
            "Authorization": f"Bearer {config['openai_api_key']}",
            "Content-Type": "application/json",
        }

    elif _provider == "anthropic":
        return {
            "x-api-key": config["anthropic_api_key"],
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }

    elif _provider == "ollama":
        return {"Content-Type": "application/json"}

    raise RuntimeError(f"Unknown AI_PROVIDER: {_provider}")


# ─── Aliases (backward compat — code that imports gemini_client still works) ──

get_gemini_url = get_ai_url
get_gemini_headers = get_ai_headers


def get_default_model() -> str:
    """Get the default model name from config."""
    return _get_config()["model"]


def get_lite_model() -> str:
    """Get the lite/fast model name from config."""
    return _get_config()["model_lite"]


# ─── Main generate function ──────────────────────────────────────────────────


async def ai_generate(
    client,
    prompt: str,
    model: str = None,
    temperature: float = 0.1,
    max_tokens: int = 8192,
    timeout: float = 60,
) -> str | None:
    """
    Generate text from ANY configured AI provider.
    Switch provider in .env — this function handles the rest.

    Args:
        client: httpx.AsyncClient instance
        prompt: The text prompt
        model: Model override (defaults to AI_MODEL from .env)
        temperature: 0.0 = deterministic, 1.0 = creative
        max_tokens: Max output tokens
        timeout: Request timeout in seconds

    Returns generated text, or None on failure.
    """
    _ensure_init()
    config = _get_config()
    model = model or config["model"]

    try:
        if _provider == "vertex_ai":
            return await _generate_vertex(
                client, prompt, model, temperature, max_tokens, timeout
            )
        elif _provider == "openai":
            return await _generate_openai(
                client, prompt, model, temperature, max_tokens, timeout, config
            )
        elif _provider == "anthropic":
            return await _generate_anthropic(
                client, prompt, model, temperature, max_tokens, timeout, config
            )
        elif _provider == "ollama":
            return await _generate_ollama(
                client, prompt, model, temperature, timeout, config
            )
        else:
            logger.error(f"Unknown provider: {_provider}")
            return None
    except Exception as e:
        logger.error(f"AI generate failed ({_provider}/{model}): {e}")
        return None


# ─── Provider implementations ────────────────────────────────────────────────


async def _generate_vertex(client, prompt, model, temperature, max_tokens, timeout):
    """Google Gemini via Vertex AI."""
    url = get_ai_url(model)
    headers = get_ai_headers()
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    resp = await client.post(url, json=body, headers=headers, timeout=timeout)
    data = resp.json()

    if resp.status_code != 200:
        error_msg = data.get("error", {}).get("message", "Unknown error")
        logger.error(f"Vertex AI error ({resp.status_code}): {error_msg}")
        return None

    candidates = data.get("candidates", [])
    if not candidates:
        return None
    parts = candidates[0].get("content", {}).get("parts", [])
    if not parts:
        return None
    return parts[0].get("text", "").strip()


async def _generate_openai(
    client, prompt, model, temperature, max_tokens, timeout, config
):
    """OpenAI / GPT models."""
    url = f"{config['openai_base_url']}/chat/completions"
    headers = {
        "Authorization": f"Bearer {config['openai_api_key']}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    resp = await client.post(url, json=body, headers=headers, timeout=timeout)
    data = resp.json()

    if resp.status_code != 200:
        logger.error(f"OpenAI error ({resp.status_code}): {data}")
        return None

    return data["choices"][0]["message"]["content"].strip()


async def _generate_anthropic(
    client, prompt, model, temperature, max_tokens, timeout, config
):
    """Anthropic / Claude models."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": config["anthropic_api_key"],
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
    }

    resp = await client.post(url, json=body, headers=headers, timeout=timeout)
    data = resp.json()

    if resp.status_code != 200:
        logger.error(f"Anthropic error ({resp.status_code}): {data}")
        return None

    return data["content"][0]["text"].strip()


async def _generate_ollama(client, prompt, model, temperature, timeout, config):
    """Local Ollama models."""
    url = f"{config['ollama_url']}/api/generate"
    body = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }

    resp = await client.post(url, json=body, timeout=timeout)
    data = resp.json()

    if resp.status_code != 200:
        logger.error(f"Ollama error ({resp.status_code}): {data}")
        return None

    return data.get("response", "").strip()


# ─── Status ───────────────────────────────────────────────────────────────────


def get_provider() -> str:
    """Get current AI provider name."""
    _ensure_init()
    return _provider or "not configured"


def is_vertex_ai() -> bool:
    """Check if using Vertex AI."""
    _ensure_init()
    return _provider == "vertex_ai" and _creds is not None
