"""
Backward compatibility — all code imports from gemini_client.
Everything is actually in ai_client.py (provider-agnostic).
"""

from app.services.ai_client import (  # noqa: F401
    ai_generate as gemini_generate,
    get_ai_url as get_gemini_url,
    get_ai_headers as get_gemini_headers,
    get_default_model,
    get_lite_model,
    get_provider,
    is_vertex_ai,
)
