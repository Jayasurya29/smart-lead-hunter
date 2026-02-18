"""
SMART LEAD HUNTER — API Key Authentication Middleware
Fix: SH4 (no authentication on endpoints)

Usage in main.py:
    from app.middleware.auth import api_key_auth

    # Protect individual endpoints:
    @app.get("/api/leads", dependencies=[Depends(api_key_auth)])

    # Or protect all /api/ routes via middleware (add to main.py startup):
    from app.middleware.auth import APIKeyMiddleware
    app.add_middleware(APIKeyMiddleware, exclude_paths=["/health", "/dashboard", "/docs", "/openapi.json"])

Environment:
    API_AUTH_KEY=your-secret-key-here   # Add to .env
"""

import os
import secrets
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

# ─── Simple Dependency (per-endpoint) ───

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _get_valid_api_key() -> str:
    """Get the configured API key from environment."""
    key = os.getenv("API_AUTH_KEY", "")
    if not key:
        raise RuntimeError(
            "API_AUTH_KEY not set in environment. Add API_AUTH_KEY=<your-key> to .env"
        )
    return key


async def api_key_auth(api_key: str = Security(_api_key_header)) -> str:
    """
    FastAPI dependency — validates X-API-Key header.

    Usage:
        @app.get("/api/leads", dependencies=[Depends(api_key_auth)])
    """
    valid_key = _get_valid_api_key()

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    # Constant-time comparison to prevent timing attacks
    if not secrets.compare_digest(api_key, valid_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )

    return api_key


# ─── Middleware (all /api/ routes at once) ───


class APIKeyMiddleware(BaseHTTPMiddleware):
    """
    Middleware that protects all /api/ routes with API key auth.
    Dashboard, health, and docs are excluded by default.

    Usage:
        app.add_middleware(
            APIKeyMiddleware,
            exclude_paths=["/health", "/dashboard", "/docs", "/openapi.json"]
        )
    """

    def __init__(self, app, exclude_paths: list[str] | None = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or [
            "/health",
            "/dashboard",
            "/docs",
            "/redoc",
            "/openapi.json",
        ]

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip auth for excluded paths and static files
        if any(path.startswith(p) for p in self.exclude_paths):
            return await call_next(request)

        # Skip auth for non-API routes (HTML pages, SSE)
        if not path.startswith("/api/"):
            return await call_next(request)

        # Validate API key
        api_key = request.headers.get("X-API-Key", "")
        try:
            valid_key = _get_valid_api_key()
        except RuntimeError:
            # If API_AUTH_KEY not set, allow through (dev mode)
            return await call_next(request)

        if not api_key or not secrets.compare_digest(api_key, valid_key):
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing X-API-Key header"},
            )

        return await call_next(request)
